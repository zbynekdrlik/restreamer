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
#[async_trait::async_trait]
pub trait S3Backend: Send + Sync + 'static {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String>;
    /// HEAD-only duration probe. Default delegates to `fetch` (full GET)
    /// for backends that don't implement HEAD; production `S3Fetcher`
    /// overrides with a real HEAD request to keep skip-ahead probes
    /// from downloading full chunk bodies (#174 review finding 2).
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        self.fetch(chunk_id).await.map(|o| o.map(|(_, d)| d))
    }
}

#[async_trait::async_trait]
impl S3Backend for crate::s3_fetch::S3Fetcher {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        match crate::s3_fetch::S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some((cd.data, cd.duration_ms))),
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
}

impl DownloadService {
    pub fn new(
        backend: Arc<dyn S3Backend>,
        registry: Arc<ChunkRegistry>,
        cache_dir: PathBuf,
        event_id: String,
        bandwidth_cap_mbit: u64,
        max_concurrent: usize,
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
        })
    }

    /// Look up the duration_ms metadata captured at fetch time. Returns
    /// `None` for chunks that have not been requested yet (or whose
    /// metadata predates this DownloadService instance).
    pub async fn get_duration(&self, chunk_id: i64) -> Option<i64> {
        self.durations.lock().await.get(&chunk_id).copied()
    }

    /// HEAD-only duration probe. Returns `Ok(Some(ms))` if the chunk
    /// exists on S3, `Ok(None)` for 404, `Err(_)` for transient errors.
    /// Caches the result in `durations` so a follow-up `request_chunk`
    /// + `get_duration` can reuse the metadata without a second HEAD.
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
        let mut backoff = Duration::from_millis(500);
        let max_attempts = 5;
        for attempt in 1..=max_attempts {
            // Concurrency gate.
            let _permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            match self.backend.fetch(chunk_id).await {
                Ok(Some((data, duration_ms))) => {
                    self.token_bucket_consume(data.len() as u64).await;
                    if let Err(e) = self.write_atomic(chunk_id, &data, duration_ms).await {
                        tracing::error!(chunk_id, "disk_cache write failed: {e}");
                        // Mark NotFound so waiters wake — better to surface a
                        // visible failure than to leave the slot InFlight and
                        // block the EndpointReader forever on this chunk.
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    self.durations.lock().await.insert(chunk_id, duration_ms);
                    self.registry.mark_available(chunk_id, data.len() as u64);
                    return;
                }
                Ok(None) => {
                    self.registry.mark_not_found(chunk_id);
                    return;
                }
                Err(e) => {
                    tracing::warn!(chunk_id, attempt, "disk_cache S3 fetch failed: {e}");
                    if attempt >= max_attempts {
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    drop(_permit);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
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
            tokio::time::sleep(slot_end - now).await;
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
        async fn fetch(&self, _chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            match self.result.lock().unwrap().clone() {
                Some(Ok((d, dur))) => Ok(Some((d, dur))),
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
            async fn fetch(&self, _id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("S3 fetch error: status 503".into())
                } else {
                    Ok(Some((vec![1, 2, 3], 2000)))
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
        );
        svc.request_chunk(99).await;
        let state = registry.wait_for_chunk(99).await.unwrap();
        assert!(matches!(state, ChunkAvailability::Available { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bandwidth_cap_throttles_combined_throughput() {
        // 5 concurrent fetches x 1 MB each at 100 Mbit/s combined cap
        //   = 5 MB total / 12.5 MB/s ~= 400 ms minimum.
        // Use 1 MB body to keep math obvious.
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
            5,
        );
        let started = Instant::now();
        let mut handles = Vec::new();
        for id in 0..5 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(id).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(350),
            "bandwidth cap must throttle (got {:?})",
            elapsed
        );
    }

    #[tokio::test]
    async fn panicking_backend_releases_waiters_via_catch_unwind() {
        struct PanicBackend;
        #[async_trait::async_trait]
        impl S3Backend for PanicBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
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
            async fn fetch(&self, _id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
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
        );
        let result = svc.head_duration(7).await.unwrap();
        assert_eq!(result, Some(2000));
        // No file written.
        assert!(!tmp.path().join("evt").join("7.bin").exists());
        // Cached: second probe is a hit, no second backend call.
        assert_eq!(svc.head_duration(7).await.unwrap(), Some(2000));
        assert_eq!(backend.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_5xx_exhausts_retries_then_marks_not_found() {
        // Always-failing backend triggers all 5 attempts; final state must be
        // NotFound so EndpointReader unblocks instead of stalling forever.
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
        );
        // Speed test up: don't actually wait the 500+1000+2000+4000ms. Pause
        // tokio time and let the retry loop sleep through paused time. The
        // backend is sync-instant; the only real waits are the backoff sleeps.
        // Note: cannot use tokio::time::pause without start_paused — and the
        // test runtime hasn't started paused. Simpler: tolerate ~7.5s test
        // runtime in CI. The plan's retry interval (500ms doubling, capped
        // at 30s) yields 0.5+1+2+4 = 7.5s for 4 backoffs after 5 failed
        // attempts. Acceptable for one slow test.
        svc.request_chunk(503).await;
        let state = registry.wait_for_chunk(503).await.unwrap();
        assert!(matches!(state, ChunkAvailability::NotFound));
        assert_eq!(backend.count(), 5, "all 5 retries must have happened");
    }
}
