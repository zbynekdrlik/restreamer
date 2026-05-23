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
    /// VPS audit ring for outage-forensics events (stall-timeout,
    /// reader-recovered, prefill-started). `None` outside production.
    audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    /// True after a stall-timeout, until the next successful `Available`
    /// fetch — that transition emits `DiskCacheReaderRecovered` so the
    /// audit timeline brackets each outage window. `&self` fetch path, so
    /// an atomic (not Cell).
    was_stalled: std::sync::atomic::AtomicBool,
    /// Rate-limits the `DiskCacheStallTimeout` emit (a sustained outage
    /// would otherwise emit one row per stall_timeout window).
    stall_rl: rs_core::audit::RateLimiter,
}

impl DiskCacheFetcher {
    pub fn new(
        cache: Arc<DiskCache>,
        alias: String,
        start_chunk_id: i64,
        window_chunks: i64,
        stall_timeout_secs: u64,
        audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
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
            audit_ring,
            was_stalled: std::sync::atomic::AtomicBool::new(false),
            stall_rl: rs_core::audit::RateLimiter::new(),
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
        let state = match self
            .cache
            .registry
            .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(self.stall_timeout_secs))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                // Outage forensics: the cache window emptied (S3 outage
                // longer than the window). Audit-only — do NOT abort; the
                // producer's outer backoff retries and rescue covers the
                // gap. The next successful Available fetch emits the paired
                // DiskCacheReaderRecovered to bracket the outage window.
                self.was_stalled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                if let Some(ring) = &self.audit_ring {
                    if self
                        .stall_rl
                        .allow(rs_core::audit::Action::DiskCacheStallTimeout, &self.alias)
                    {
                        ring.push_parts(crate::audit_ring::RingRowParts {
                            severity: rs_core::audit::Severity::Error,
                            source: rs_core::audit::Source::Vps,
                            endpoint: Some(self.alias.clone()),
                            action: rs_core::audit::Action::DiskCacheStallTimeout,
                            detail: serde_json::json!({
                                "chunk_id": chunk_id,
                                "timeout_secs": self.stall_timeout_secs,
                            }),
                        });
                    }
                }
                return Err(format!("disk_cache stall on chunk {chunk_id}: {e}"));
            }
        };

        match state {
            ChunkAvailability::Available { .. } => {
                // Recovered after a stall: emit the paired ReaderRecovered
                // exactly once per outage so the audit timeline brackets the
                // gap. `swap` is the atomic test-and-clear.
                if self
                    .was_stalled
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    if let Some(ring) = &self.audit_ring {
                        ring.push_parts(crate::audit_ring::RingRowParts {
                            severity: rs_core::audit::Severity::Info,
                            source: rs_core::audit::Source::Vps,
                            endpoint: Some(self.alias.clone()),
                            action: rs_core::audit::Action::DiskCacheReaderRecovered,
                            detail: serde_json::json!({ "chunk_id": chunk_id }),
                        });
                    }
                }
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
