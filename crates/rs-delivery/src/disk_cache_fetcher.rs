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
        // (#174 review finding 1). Single alias clone for register;
        // advance takes a borrow.
        let alias_for_register = alias.clone();
        let positions = &cache.position_registry;
        positions.register(alias_for_register, window_chunks);
        positions.advance(&alias, start_chunk_id);
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
        // Prefetch the upcoming window in ONE spawned task that loops
        // (instead of N spawns per fetch). The DownloadService's own
        // semaphore + dedup keeps the actual S3 concurrency bounded;
        // collapsing the spawn ladder cuts ~window-1 spawn calls per
        // chunk per endpoint (#174 review-of-review #3).
        let prefetch_window = self.window_chunks;
        let prefetch_svc = Arc::clone(&self.cache.download_service);
        tokio::spawn(async move {
            for ahead in 1..=prefetch_window {
                prefetch_svc.request_chunk(chunk_id + ahead).await;
            }
        });

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
                // Single ENOENT retry: an EvictionTask sweep can race
                // a reader between the registry mark_available and the
                // tokio::fs::read (#174 review finding 3). One retry
                // covers the race; if the chunk truly vanished, fall
                // through to the producer's outer retry/backoff.
                let data = match tokio::fs::read(&path).await {
                    Ok(d) => d,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        self.cache.download_service.request_chunk(chunk_id).await;
                        let _ = self
                            .cache
                            .registry
                            .wait_for_chunk_with_timeout(
                                chunk_id,
                                Duration::from_secs(self.stall_timeout_secs),
                            )
                            .await
                            .map_err(|e| format!("disk_cache enoent retry stall: {e}"))?;
                        tokio::fs::read(&path)
                            .await
                            .map_err(|e| format!("disk read {} (retry): {e}", path.display()))?
                    }
                    Err(e) => return Err(format!("disk read {}: {e}", path.display())),
                };
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
        // Producer's skip-ahead probe: HEAD-only, no body download.
        // 5s client-side timeout in case the S3 HEAD wedges the
        // connection (#174 review-of-review #2). Transient errors are
        // surfaced as Err so the producer's outer backoff handles them
        // instead of silently advancing past chunks (#174 review-of-
        // review #4).
        let probe = self.cache.download_service.head_duration(chunk_id);
        match tokio::time::timeout(Duration::from_secs(5), probe).await {
            Ok(Ok(Some(ms))) => Ok(Some(ms)),
            Ok(Ok(None)) => Ok(None),
            Ok(Err(e)) => Err(format!("disk_cache HEAD probe error: {e}")),
            Err(_) => Err(format!("disk_cache HEAD probe timeout on chunk {chunk_id}")),
        }
    }
}
