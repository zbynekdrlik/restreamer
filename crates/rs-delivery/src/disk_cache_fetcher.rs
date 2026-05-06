//! `DiskCacheFetcher` — `ChunkFetcher` backed by the per-event `DiskCache`.
//!
//! Replaces the direct `S3Fetcher` used by the producer-consumer pipeline.
//! `fetch_chunk_with_meta` triggers a background fetch into the disk cache
//! (deduplicated, bandwidth-managed) and waits for the chunk to land on
//! local SSD before returning the bytes. The bandwidth-managed downloader
//! also pre-fetches `[id+1, id+window-1]` so the producer keeps reading
//! from disk at line speed even when S3 has transient failures.
//!
//! Issue #174: this is the integration point that decouples upstream S3
//! ingress from the downstream RTMP push hot path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::disk_cache::{ChunkAvailability, DiskCache};
use crate::endpoint_task::ChunkFetcher;

pub struct DiskCacheFetcher {
    cache: Arc<DiskCache>,
    alias: String,
    /// `{cache_dir}/{event_id}/`.
    event_dir: PathBuf,
    /// Endpoint window length in chunks. Used for prefetch-ahead and the
    /// position-registry registration.
    window_chunks: i64,
    /// `wait_for_chunk_with_timeout` deadline: how long the producer
    /// waits for a single chunk before returning Err. The producer's
    /// existing backoff loop turns the Err into a retry.
    stall_timeout_secs: u64,
}

impl DiskCacheFetcher {
    pub fn new(
        cache: Arc<DiskCache>,
        alias: String,
        start_chunk_id: i64,
        window_chunks: i64,
        stall_timeout_secs: u64,
    ) -> Self {
        let event_dir = cache.event_dir();
        // Register synchronously: a same-tick `advance` from the producer
        // would otherwise silently no-op on an unknown alias and the
        // EvictionTask could delete chunks this endpoint still needs
        // (#174 review finding 1).
        cache
            .position_registry
            .register(alias.clone(), window_chunks);
        // Seed initial position so the first eviction sweep already
        // protects this endpoint's window.
        cache.position_registry.advance(&alias, start_chunk_id);
        Self {
            cache,
            alias,
            event_dir,
            window_chunks,
            stall_timeout_secs,
        }
    }
}

impl ChunkFetcher for DiskCacheFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        // Prefetch upcoming chunks. Fire-and-forget so the producer is
        // not blocked. Each request_chunk is deduplicated by the
        // DownloadService -- six endpoints requesting the same chunk
        // result in one S3 GET.
        for ahead in 1..=self.window_chunks {
            let svc = Arc::clone(&self.cache.download_service);
            tokio::spawn(async move { svc.request_chunk(chunk_id + ahead).await });
        }

        // Update position registry so eviction protects this endpoint's window.
        self.cache.position_registry.advance(&self.alias, chunk_id);

        // Trigger the targeted fetch and wait for terminal state.
        self.cache.download_service.request_chunk(chunk_id).await;
        let state = self
            .cache
            .registry
            .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(self.stall_timeout_secs))
            .await
            .map_err(|e| format!("disk_cache stall on chunk {chunk_id}: {e}"))?;

        match state {
            ChunkAvailability::Available { .. } => {
                let path = self.event_dir.join(format!("{chunk_id}.bin"));
                let data = tokio::fs::read(&path)
                    .await
                    .map_err(|e| format!("disk read {}: {e}", path.display()))?;
                let duration_ms = self
                    .cache
                    .download_service
                    .get_duration(chunk_id)
                    .await
                    .unwrap_or(0);
                Ok(Some((data, duration_ms)))
            }
            ChunkAvailability::NotFound => Ok(None),
            ChunkAvailability::Evicted => {
                // The chunk used to exist on disk and was swept. The
                // producer treats `None` as "not on S3", which triggers
                // its skip-ahead probe loop. That's the right recovery
                // because eviction only happens for chunks outside any
                // endpoint's window.
                Ok(None)
            }
            ChunkAvailability::InFlight => Err(format!(
                "disk_cache: chunk {chunk_id} stuck InFlight after timeout"
            )),
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        // Producer's skip-ahead probe: fire a fetch and report whether
        // the chunk lands within a short window. The producer is
        // looking for the next available chunk, so a short timeout
        // (5s) keeps the probe loop responsive without waiting for a
        // full S3 retry cycle.
        let svc = Arc::clone(&self.cache.download_service);
        let cid = chunk_id;
        tokio::spawn(async move { svc.request_chunk(cid).await });
        match self
            .cache
            .registry
            .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(5))
            .await
        {
            Ok(ChunkAvailability::Available { .. }) => Ok(Some(
                self.cache
                    .download_service
                    .get_duration(chunk_id)
                    .await
                    .unwrap_or(0),
            )),
            Ok(ChunkAvailability::NotFound) | Ok(ChunkAvailability::Evicted) => Ok(None),
            Ok(ChunkAvailability::InFlight) | Err(_) => Ok(None),
        }
    }
}
