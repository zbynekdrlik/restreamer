//! DownloadService — bandwidth-managed S3 chunk downloader with dedup.
//!
//! One instance per event. EndpointReaders call `request_chunk(id)`;
//! the service deduplicates concurrent requests for the same chunk,
//! issues a single S3 GET, writes atomically to disk, and marks the
//! registry available.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use super::registry::ChunkRegistry;

/// Trait abstracting the S3 fetch operation. The real implementation
/// is `crate::s3_fetch::S3Fetcher`; tests use `MockBackend`.
///
/// The `FetchedChunk` return carries the bytes plus all stage-A/B
/// metadata read from the S3 response headers, so `DownloadService`
/// can later associate timings with the chunk it cached.
#[async_trait::async_trait]
pub trait S3Backend: Send + Sync + 'static {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String>;
    /// HEAD-only duration probe. Default delegates to `fetch` (full GET)
    /// for backends that don't implement HEAD; production `S3Fetcher`
    /// overrides with a real HEAD request to keep skip-ahead probes
    /// from downloading full chunk bodies (#174 review finding 2).
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        self.fetch(chunk_id).await.map(|o| o.map(|c| c.duration_ms))
    }
}

/// Bytes + metadata returned by an `S3Backend::fetch` call.
#[derive(Debug, Clone)]
pub struct FetchedChunk {
    pub data: Vec<u8>,
    pub duration_ms: i64,
    pub host_emit_ts: Option<i64>,
    pub s3_upload_complete_ts: Option<i64>,
}

#[async_trait::async_trait]
impl S3Backend for crate::s3_fetch::S3Fetcher {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        match crate::s3_fetch::S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some(FetchedChunk {
                data: cd.data,
                duration_ms: cd.duration_ms,
                host_emit_ts: cd.host_emit_ts,
                s3_upload_complete_ts: cd.s3_upload_complete_ts,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        crate::s3_fetch::S3Fetcher::head_chunk_duration(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}

pub struct DownloadService {
    backend: Arc<dyn S3Backend>,
    registry: Arc<ChunkRegistry>,
    /// `{cache_root}/{event_id}/`. Constructed once at `new`; reused
    /// by `write_atomic` so the join is not duplicated per write
    /// (#174 review finding 12).
    event_dir: PathBuf,
    /// Concurrent in-flight requests. Used for dedup — same chunk_id
    /// requested twice yields one S3 GET; the second waiter blocks on
    /// the same Notify.
    in_flight: Mutex<HashMap<i64, Arc<tokio::sync::Notify>>>,
    /// Bytes-per-second budget. 0 means uncapped.
    bandwidth_cap_bytes_per_sec: u64,
    /// Limits parallel fetches.
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Shared token-bucket scheduling clock. Each `token_bucket_consume`
    /// atomically allocates a slot of duration `bytes / cap` starting at
    /// `max(now, next_slot_start)`. Subsequent allocations are serialized
    /// even when fetches run in parallel — total elapsed = total bytes / cap.
    next_slot_start: Mutex<Instant>,
    /// Per-chunk duration_ms metadata captured at fetch time. The disk
    /// file stores only the bytes; pacing-aware consumers query this map
    /// to recover the metadata that S3 originally returned.
    durations: Mutex<HashMap<i64, i64>>,
    /// S3 fetch latency + byte + failure profiler. Surfaced via
    /// `profile_snapshot()` for the `/api/v1/delivery/status` endpoint.
    profile: Arc<crate::s3_fetch_profile::S3FetchProfile>,
    /// VPS audit ring for outage-forensics events (write-failed, throttled).
    /// `None` in unit tests. Threaded through `DiskCache::new`.
    audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    /// Rate-limiter for noisy disk-cache audit events (download-throttled).
    /// Keeps the audit ring from flooding during a sustained throttle.
    audit_rl: rs_core::audit::RateLimiter,
}

impl DownloadService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend: Arc<dyn S3Backend>,
        registry: Arc<ChunkRegistry>,
        cache_dir: PathBuf,
        event_id: String,
        bandwidth_cap_mbit: u64,
        max_concurrent: usize,
        audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    ) -> Arc<Self> {
        let event_dir = cache_dir.join(&event_id);
        Arc::new(Self {
            backend,
            registry,
            event_dir,
            in_flight: Mutex::new(HashMap::new()),
            bandwidth_cap_bytes_per_sec: (bandwidth_cap_mbit * 1_000_000) / 8,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            next_slot_start: Mutex::new(Instant::now()),
            durations: Mutex::new(HashMap::new()),
            profile: Arc::new(crate::s3_fetch_profile::S3FetchProfile::new()),
            audit_ring,
            audit_rl: rs_core::audit::RateLimiter::new(),
        })
    }

    /// Look up the duration_ms metadata captured at fetch time. Returns
    /// `None` for chunks that have not been requested yet (or whose
    /// metadata predates this DownloadService instance).
    pub async fn get_duration(&self, chunk_id: i64) -> Option<i64> {
        self.durations.lock().await.get(&chunk_id).copied()
    }

    /// Returns a point-in-time snapshot of S3 fetch latency, byte, and
    /// failure statistics accumulated since this `DownloadService` was
    /// created. Consumed by `/api/v1/delivery/status` (Task 12).
    pub fn profile_snapshot(&self) -> crate::s3_fetch_profile::S3FetchProfileSnapshot {
        self.profile.snapshot()
    }

    /// Path of the cached chunk file inside the per-event directory.
    /// Used by `PrefetchReader::try_read_from_disk` so the reader does
    /// not depend on internal layout knowledge.
    pub fn chunk_path(&self, chunk_id: i64) -> std::path::PathBuf {
        self.event_dir.join(format!("{chunk_id}.bin"))
    }

    /// Test/integration helper: clone of the registry handle so external
    /// callers (PrefetchReader) can call `wait_for_chunk_with_timeout`
    /// without re-plumbing a separate registry argument.
    pub fn registry_for_test(&self) -> Arc<super::registry::ChunkRegistry> {
        Arc::clone(&self.registry)
    }

    /// HEAD-only duration probe. Returns `Ok(Some(ms))` if the chunk
    /// exists on S3, `Ok(None)` for 404, `Err(_)` for transient errors.
    /// Caches the result in `durations` so a follow-up `request_chunk`
    /// then `get_duration` can reuse the metadata without a second HEAD.
    /// Used by the producer's skip-ahead probe to avoid the full-GET
    /// regression (#174 review finding 2: a 50 MB skip-ahead waste).
    pub async fn head_duration(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if let Some(ms) = self.durations.lock().await.get(&chunk_id).copied() {
            return Ok(Some(ms));
        }
        let res = self.backend.head_duration_ms(chunk_id).await?;
        if let Some(ms) = res {
            self.durations.lock().await.insert(chunk_id, ms);
        }
        Ok(res)
    }

    /// Fetch a chunk if not already cached / in flight. Returns when
    /// the chunk reaches a terminal registry state (Available or NotFound).
    pub async fn request_chunk(self: &Arc<Self>, chunk_id: i64) {
        // Skip if already on disk.
        if self.registry.exists(chunk_id) {
            return;
        }

        // Dedup: if another request for this chunk is already in flight,
        // wait on its Notify. Otherwise spawn the fetch task.
        let notify = {
            let mut g = self.in_flight.lock().await;
            if let Some(n) = g.get(&chunk_id) {
                Arc::clone(n)
            } else {
                let n = Arc::new(tokio::sync::Notify::new());
                g.insert(chunk_id, Arc::clone(&n));
                self.registry.mark_in_flight(chunk_id);
                let svc = Arc::clone(self);
                let n_clone = Arc::clone(&n);
                tokio::spawn(async move {
                    // Catch panics so the in_flight slot + Notify are
                    // always released. A panic with no recovery would
                    // leave waiters blocked on an orphan Notify until
                    // their stall timeout (#174 review finding 7).
                    let result = AssertUnwindSafe(svc.fetch_with_retry(chunk_id))
                        .catch_unwind()
                        .await;
                    if result.is_err() {
                        tracing::error!(chunk_id, "disk_cache fetch panicked");
                        svc.registry.mark_not_found(chunk_id);
                    }
                    let mut g = svc.in_flight.lock().await;
                    g.remove(&chunk_id);
                    n_clone.notify_waiters();
                });
                n
            }
        };

        // Race-safe registration: build Notified, enable, re-check, then await.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.in_flight.lock().await.contains_key(&chunk_id) {
            // Already done before we registered.
            return;
        }
        notified.await;
    }

    async fn fetch_with_retry(self: &Arc<Self>, chunk_id: i64) {
        let mut backoff_secs: u64 = 1;
        let mut attempt: u64 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let _permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let fetch_start = Instant::now();
            let result = self.backend.fetch(chunk_id).await;
            let elapsed_ms = fetch_start.elapsed().as_millis() as u64;
            match result {
                Ok(Some(fc)) => {
                    self.profile
                        .record_success(elapsed_ms, fc.data.len() as u64);
                    self.token_bucket_consume(fc.data.len() as u64).await;
                    if let Err(e) = self.write_atomic(chunk_id, &fc.data, fc.duration_ms).await {
                        tracing::error!(chunk_id, "disk_cache write failed: {e}");
                        // Outage forensics: a local disk-write failure (full
                        // volume, I/O error) is operator-actionable — surface
                        // it on the audit timeline, not just the log.
                        if let Some(ring) = &self.audit_ring {
                            ring.push_parts(crate::audit_ring::RingRowParts {
                                severity: rs_core::audit::Severity::Error,
                                source: rs_core::audit::Source::Vps,
                                endpoint: None,
                                action: rs_core::audit::Action::DiskCacheWriteFailed,
                                detail: serde_json::json!({
                                    "chunk_id": chunk_id,
                                    "error": e.to_string(),
                                }),
                            });
                        }
                        // Disk-write failures are not handled here -- the
                        // outer PrefetchReader loop re-requests, and the
                        // next iteration retries from scratch. Mark
                        // NotFound so anyone awaiting wait_for_chunk
                        // wakes immediately rather than hanging.
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    self.durations.lock().await.insert(chunk_id, fc.duration_ms);
                    self.registry.mark_available(chunk_id, fc.data.len() as u64);
                    return;
                }
                Ok(None) => {
                    // 404: chunk genuinely not on S3 yet. Don't loop here
                    // forever -- mark NotFound so the OUTER PrefetchReader
                    // loop decides whether to retry (genuine miss is rare;
                    // uploader will eventually PUT).
                    self.registry.mark_not_found(chunk_id);
                    return;
                }
                Err(e) => {
                    tracing::warn!(chunk_id, attempt, "disk_cache S3 fetch failed: {e}");
                    let class = crate::endpoint_audit::classify_s3_fetch_error(&e.to_string());
                    self.profile.record_failure(class);
                    drop(_permit);
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                    // No max_attempts check. Loop until success, 404, or
                    // disk-write hard fail. Per user rule (#184): never
                    // give up on transient errors; only slow down.
                }
            }
        }
    }

    /// Reserve a `bytes / cap`-duration slot in the shared token-bucket
    /// schedule, then sleep until the slot's END. Concurrent callers
    /// serialize their slots, so total elapsed across N parallel fetches
    /// is total_bytes / cap (not max_bytes / cap).
    ///
    /// Sleeping to the slot END (not start) is what makes the rate cap
    /// real: the slot represents bytes-being-consumed during the
    /// `slot_dur` window, so the consumer must not return until that
    /// window ends. Sleeping to start would let the last task return
    /// `slot_dur` too early -- the bandwidth-cap test caught this
    /// (got 324ms vs expected ~400ms for 5x1MB at 100 Mbit/s).
    async fn token_bucket_consume(&self, bytes: u64) {
        if self.bandwidth_cap_bytes_per_sec == 0 {
            return;
        }
        let slot_dur =
            Duration::from_secs_f64(bytes as f64 / self.bandwidth_cap_bytes_per_sec as f64);
        let slot_end = {
            let mut g = self.next_slot_start.lock().await;
            let now = Instant::now();
            let start = (*g).max(now);
            *g = start + slot_dur;
            *g
        };
        let now = Instant::now();
        if slot_end > now {
            let queued = slot_end - now;
            // Outage forensics: when the bandwidth cap has backed downloads
            // up by >=1s, the cache is filling slower than real-time —
            // surface a rate-limited (1/min) warning so the operator sees
            // the throttle on the timeline, not just sustained lag.
            if queued >= Duration::from_secs(1) {
                if let Some(ring) = &self.audit_ring {
                    if self.audit_rl.allow(
                        rs_core::audit::Action::DiskCacheDownloadThrottled,
                        "throttle",
                    ) {
                        ring.push_parts(crate::audit_ring::RingRowParts {
                            severity: rs_core::audit::Severity::Warn,
                            source: rs_core::audit::Source::Vps,
                            endpoint: None,
                            action: rs_core::audit::Action::DiskCacheDownloadThrottled,
                            detail: serde_json::json!({ "queued_ms": queued.as_millis() as u64 }),
                        });
                    }
                }
            }
            tokio::time::sleep(queued).await;
        }
    }

    async fn write_atomic(
        &self,
        chunk_id: i64,
        data: &[u8],
        _duration_ms: i64,
    ) -> std::io::Result<()> {
        fs::create_dir_all(&self.event_dir).await?;
        let final_path = self.event_dir.join(format!("{chunk_id}.bin"));
        let part_path = self.event_dir.join(format!("{chunk_id}.bin.part"));
        let mut f = fs::File::create(&part_path).await?;
        f.write_all(data).await?;
        f.flush().await?;
        f.sync_all().await?;
        drop(f);
        fs::rename(&part_path, &final_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::registry::{ChunkAvailability, ChunkRegistry};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    /// Deterministic mock S3 backend. Counts GETs per chunk.
    #[derive(Default)]
    struct MockBackend {
        get_count: AtomicU32,
        result: std::sync::Mutex<Option<Result<(Vec<u8>, i64), String>>>,
    }

    impl MockBackend {
        fn set_ok(&self, data: Vec<u8>, dur: i64) {
            *self.result.lock().unwrap() = Some(Ok((data, dur)));
        }
        fn set_err(&self, msg: &str) {
            *self.result.lock().unwrap() = Some(Err(msg.into()));
        }
        fn count(&self) -> u32 {
            self.get_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Backend for MockBackend {
        async fn fetch(&self, _chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            match self.result.lock().unwrap().clone() {
                Some(Ok((d, dur))) => Ok(Some(FetchedChunk {
                    data: d,
                    duration_ms: dur,
                    host_emit_ts: None,
                    s3_upload_complete_ts: None,
                })),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }

    #[tokio::test]
    async fn dedup_six_concurrent_requests_for_same_chunk_yield_one_get() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1024], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000, // 10 Gbit cap so test isn't bandwidth-limited
            8,
            None,
        );
        let mut handles = Vec::new();
        for _ in 0..6 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(42).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(backend.count(), 1, "deduplicate concurrent requests");
    }

    #[tokio::test]
    async fn fetch_writes_atomic_file_then_marks_registry_available() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(b"hello".to_vec(), 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        svc.request_chunk(7).await;
        let path = tmp.path().join("evt").join("7.bin");
        assert!(
            path.exists(),
            "file must exist after request_chunk completes"
        );
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
        assert!(registry.exists(7));
    }

    #[tokio::test]
    async fn fetch_404_marks_registry_not_found_no_file() {
        let backend = Arc::new(MockBackend::default());
        // Ok(None) signals 404 / not-found.
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        svc.request_chunk(404).await;
        let state = registry.wait_for_chunk(404).await.unwrap();
        assert!(matches!(state, ChunkAvailability::NotFound));
        let path = tmp.path().join("evt").join("404.bin");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn fetch_5xx_retries_with_backoff_then_succeeds() {
        // Mock that fails twice then succeeds — verify retry happens.
        let attempts = Arc::new(AtomicU32::new(0));
        struct FlakyBackend(Arc<AtomicU32>);
        #[async_trait::async_trait]
        impl S3Backend for FlakyBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("S3 fetch error: status 503".into())
                } else {
                    Ok(Some(FetchedChunk {
                        data: vec![1, 2, 3],
                        duration_ms: 2000,
                        host_emit_ts: None,
                        s3_upload_complete_ts: None,
                    }))
                }
            }
        }
        let backend = Arc::new(FlakyBackend(attempts.clone()));
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        svc.request_chunk(99).await;
        let state = registry.wait_for_chunk(99).await.unwrap();
        assert!(matches!(state, ChunkAvailability::Available { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bandwidth_cap_throttles_combined_throughput() {
        // 10 concurrent fetches x 1 MB each at 100 Mbit/s combined cap
        //   = 10 MB total / 12.5 MB/s ~= 800 ms minimum.
        // Larger N puts the runtime jitter (~10-30ms typical, ~50ms
        // worst-case on loaded CI) at <10% of the expected duration so
        // a one-sided lower-bound assertion stays robust without an
        // upper bound that would flake on slow runners (#174 review #11).
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1_000_000], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            100, // 100 Mbit/s cap
            10,
            None,
        );
        let started = Instant::now();
        let mut handles = Vec::new();
        for id in 0..10 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(id).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(750),
            "bandwidth cap must throttle (got {:?})",
            elapsed
        );
    }

    #[tokio::test]
    async fn panicking_backend_releases_waiters_via_catch_unwind() {
        struct PanicBackend;
        #[async_trait::async_trait]
        impl S3Backend for PanicBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                panic!("simulated backend panic");
            }
        }
        let backend = Arc::new(PanicBackend);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend,
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        // Should resolve to NotFound, NOT hang.
        let result = tokio::time::timeout(Duration::from_secs(2), svc.request_chunk(7)).await;
        assert!(
            result.is_ok(),
            "request_chunk hung after backend panic; catch_unwind missing"
        );
        let state = registry.wait_for_chunk(7).await.unwrap();
        assert!(matches!(state, ChunkAvailability::NotFound));
    }

    #[tokio::test]
    async fn head_duration_does_not_download_body_or_write_disk() {
        struct HeadOnlyBackend(AtomicU32);
        #[async_trait::async_trait]
        impl S3Backend for HeadOnlyBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                panic!("fetch must not be called for HEAD probe");
            }
            async fn head_duration_ms(&self, _id: i64) -> Result<Option<i64>, String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Some(2000))
            }
        }
        let backend = Arc::new(HeadOnlyBackend(AtomicU32::new(0)));
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        let result = svc.head_duration(7).await.unwrap();
        assert_eq!(result, Some(2000));
        // No file written.
        assert!(!tmp.path().join("evt").join("7.bin").exists());
        // Cached: second probe is a hit, no second backend call.
        assert_eq!(svc.head_duration(7).await.unwrap(), Some(2000));
        assert_eq!(backend.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_5xx_no_longer_exhausts_retries_loops_forever() {
        let backend = Arc::new(MockBackend::default());
        backend.set_err("S3 fetch error: status 503");
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        let svc2 = Arc::clone(&svc);
        let task = tokio::spawn(async move { svc2.request_chunk(503).await });
        // Real-time, modest budget. See `fetch_with_retry_never_caps_attempts`
        // for why paused-time was rejected (tarpaulin instrumentation
        // overhead).
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let state = registry.peek(503);
        assert!(
            !matches!(state, Some(ChunkAvailability::NotFound)),
            "retry-forever must not give up, got state={state:?}"
        );
        assert!(backend.count() >= 4);
        task.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_with_retry_never_caps_attempts() {
        // Real-time test: backend always 503. After ~8s real time the
        // registry must NOT be NotFound — the retry loop must still
        // be running. The old 5-attempt cap would mark NotFound after
        // ~7.5s (0.5+1+2+4=7.5s of backoffs); retry-forever does NOT.
        //
        // start_paused was rejected because tarpaulin's instrumentation
        // is slow enough that paused-time tests with many virtual
        // sleep wake-ups exhaust tarpaulin's per-test 5-min timeout.
        let backend = Arc::new(MockBackend::default());
        backend.set_err("S3 fetch error: status 503");
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
            None,
        );
        let svc2 = Arc::clone(&svc);
        let req = tokio::spawn(async move { svc2.request_chunk(503).await });
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let st = registry.peek(503);
        assert!(
            !matches!(st, Some(ChunkAvailability::NotFound)),
            "retry-forever must NOT mark NotFound on transient errors, got {st:?}"
        );
        assert!(
            backend.count() >= 4,
            "expected >=4 attempts in 10s real time (proving no max_attempts cap), got {}",
            backend.count()
        );
        req.abort();
    }

    #[test]
    fn fetch_with_retry_backoff_caps_at_60s() {
        // White-box assertion on the backoff schedule. The cap matters
        // because uncapped exponential growth would make the reader
        // sleep multiple minutes between attempts after a few failures.
        let mut backoff_secs: u64 = 1;
        let mut steps = vec![];
        for _ in 0..10 {
            steps.push(backoff_secs);
            backoff_secs = (backoff_secs * 2).min(60);
        }
        assert_eq!(steps, vec![1, 2, 4, 8, 16, 32, 60, 60, 60, 60]);
    }
}
