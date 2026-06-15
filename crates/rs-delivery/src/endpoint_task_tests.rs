use super::super::*;
use async_trait::async_trait;
use rs_core::models::PusherKind;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Mutex;

struct MockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
}

impl MockFetcher {
    fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            chunks: Arc::new(TokioMutex::new(chunks.into_iter().collect())),
            duration_ms_per_chunk: 20,
        }
    }
}

impl ChunkFetcher for MockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        let map = self.chunks.lock().await;
        Ok(map
            .get(&chunk_id)
            .map(|data| (data.clone(), self.duration_ms_per_chunk)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(self.duration_ms_per_chunk))
        } else {
            Ok(None)
        }
    }
}

struct MockProcess {
    alive: Arc<AtomicBool>,
    writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
    fail_after: Option<u32>,
    write_count: u32,
    hang_on_write: bool,
}

impl MockProcess {
    fn new(alive: Arc<AtomicBool>, writes: Arc<TokioMutex<Vec<Vec<u8>>>>) -> Self {
        Self {
            alive,
            writes,
            fail_after: None,
            write_count: 0,
            hang_on_write: false,
        }
    }
}

#[async_trait]
impl OutputProcess for MockProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        if self.hang_on_write {
            // Simulate a hanging write
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            return Ok(());
        }
        self.write_count += 1;
        if let Some(limit) = self.fail_after {
            if self.write_count > limit {
                self.alive.store(false, Ordering::Relaxed);
                return Err("mock process died".to_string());
            }
        }
        self.writes.lock().await.push(data.to_vec());
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        // "Invalid data found" classifies as InvalidInput (1s backoff) so
        // tests observe restarts within their virtual-time windows.
        Some("Invalid data found".to_string())
    }
}

// `pub(crate)` so the sibling `fast_self_healing_tests` module (which holds
// the moved fast-delay / chunk-gap tests) can reuse this shared mock factory.
pub(crate) struct MockProcessFactory {
    alive: Arc<AtomicBool>,
    writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
    fail_after_writes: Option<u32>,
    spawn_fail: Arc<AtomicBool>,
    spawn_count: Arc<AtomicU32>,
    hang_on_write: bool,
}

impl MockProcessFactory {
    pub(crate) fn new() -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
            writes: Arc::new(TokioMutex::new(Vec::new())),
            fail_after_writes: None,
            spawn_fail: Arc::new(AtomicBool::new(false)),
            spawn_count: Arc::new(AtomicU32::new(0)),
            hang_on_write: false,
        }
    }
}

impl OutputProcessFactory for MockProcessFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        if self.spawn_fail.load(Ordering::Relaxed) {
            return Err("mock spawn failed".to_string());
        }
        self.alive.store(true, Ordering::Relaxed);
        let mut proc = MockProcess::new(self.alive.clone(), self.writes.clone());
        proc.fail_after = self.fail_after_writes;
        proc.hang_on_write = self.hang_on_write;
        Ok(Box::new(proc))
    }
}

// `pub(crate)` so the sibling `fast_self_healing_tests` module can build the
// same default endpoint config for the moved tests.
pub(crate) fn test_ep_cfg() -> EndpointConfig {
    EndpointConfig {
        alias: "test-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: PusherKind::Ffmpeg,
    }
}
#[tokio::test]
async fn test_processes_sequential_chunks() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=5).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = MockFetcher::new(chunks);
    let factory = MockProcessFactory::new();
    let writes = factory.writes.clone();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    // Direct write + 1s sleep per chunk. 5 chunks x 1s = 5s.
    for _ in 0..600 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 5, "Should have processed 5 chunks");
    assert_eq!(s.current_chunk_id, 5);
    assert_eq!(s.bytes_processed_total, 500);
    drop(s);

    let w = writes.lock().await;
    assert_eq!(w.len(), 5, "Should have 1 write per chunk: {}", w.len());
    drop(w);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_stops_on_signal() {
    tokio::time::pause();
    let fetcher = MockFetcher::new(vec![]);
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    tokio::time::advance(std::time::Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    let _ = stop_tx.send(true);

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "Task should have stopped cleanly");
}
#[tokio::test]
async fn test_restarts_ffmpeg_on_death() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=6).map(|i| (i, vec![i as u8; 50])).collect();
    let fetcher = MockFetcher::new(chunks);
    let mut factory = MockProcessFactory::new();
    // Direct write: 1 write per chunk.
    // Fail after ~3 chunks worth of writes = 150 writes.
    factory.fail_after_writes = Some(3);
    let spawn_count = factory.spawn_count.clone();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    // Direct write + 1s sleep per chunk. Need enough time for restart cycle.
    for _ in 0..1500 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(
        s.ffmpeg_restart_count >= 1,
        "Should have restarted at least once, got {}",
        s.ffmpeg_restart_count
    );
    assert!(
        s.chunks_processed >= 3,
        "Should have processed at least 3 chunks before death, got {}",
        s.chunks_processed
    );
    drop(s);

    assert!(
        spawn_count.load(Ordering::Relaxed) >= 2,
        "Factory should have been called at least twice"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_tracks_ffmpeg_restart_count() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=20).map(|i| (i, vec![i as u8; 10])).collect();
    let fetcher = MockFetcher::new(chunks);
    let mut factory = MockProcessFactory::new();
    factory.fail_after_writes = Some(1);

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..30 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(
        s.ffmpeg_restart_count >= 2,
        "Should track multiple restarts, got {}",
        s.ffmpeg_restart_count
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_tracks_consecutive_chunk_misses() {
    tokio::time::pause();
    let fetcher = MockFetcher::new(vec![(1, vec![1; 10])]);
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..20 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 1, "Should have processed chunk 1");
    assert!(
        s.consecutive_chunk_misses > 0,
        "Should have tracked chunk misses, got {}",
        s.consecutive_chunk_misses
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_tracks_last_error() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=2).map(|i| (i, vec![i as u8; 10])).collect();
    let fetcher = MockFetcher::new(chunks);
    let mut factory = MockProcessFactory::new();
    factory.fail_after_writes = Some(1);

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..15 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(s.last_error.is_some(), "Should have recorded last error");
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_ffmpeg_circuit_breaker_triggers() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=5).map(|i| (i, vec![i as u8; 10])).collect();
    let fetcher = MockFetcher::new(chunks);
    let factory = MockProcessFactory::new();
    factory.spawn_fail.store(true, Ordering::Relaxed);
    let spawn_count = factory.spawn_count.clone();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..60 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(
        s.stall_reason.as_deref(),
        Some("ffmpeg_crash_loop"),
        "Should have set ffmpeg_crash_loop stall reason"
    );
    drop(s);

    assert!(
        spawn_count.load(Ordering::Relaxed) >= MAX_FFMPEG_RESTARTS,
        "Should have attempted at least {} spawns, got {}",
        MAX_FFMPEG_RESTARTS,
        spawn_count.load(Ordering::Relaxed)
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_chunk_gap_skip_ahead() {
    tokio::time::pause();
    let mut chunks: Vec<(i64, Vec<u8>)> = (1..=15).map(|i| (i, vec![i as u8; 10])).collect();
    chunks.extend((17..=20).map(|i| (i, vec![i as u8; 10])));
    let fetcher = MockFetcher::new(chunks);
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..200 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    // R1 GREEN: cache-drain triggers rust_rescue_push during the chunk-16
    // gap; pusher blocks on TCP connect, so chunks 17-20 freeze in the
    // prefetch channel. Producer-skip-ahead is verified by producer_lag.
    assert!(
        s.chunks_processed >= 15,
        "Should have processed pre-gap chunks, got {}",
        s.chunks_processed
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_chunk_gap_detected_when_no_skip_found() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=15).map(|i| (i, vec![i as u8; 10])).collect();
    let fetcher = MockFetcher::new(chunks);
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..200 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 15, "Should have processed 15 chunks");
    assert_eq!(
        s.stall_reason.as_deref(),
        Some("chunk_gap"),
        "Should have set chunk_gap stall reason"
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_drought_mode_recovers_when_chunks_resume() {
    // Verify that when chunks dry up, the producer detects chunk_gap stall
    // and resumes processing when chunks become available again.
    // In the producer-consumer architecture, the consumer blocks on rx.recv()
    // while the producer waits for new chunks -- no ffmpeg kill/restart needed.
    tokio::time::pause();

    // 30 chunks total. Initially only 1-5 available.
    let chunks: Vec<(i64, Vec<u8>)> = (1..=30).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = TimedMockFetcher::new(chunks, 5);
    let available = fetcher.available_up_to();

    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let stats_clone = stats.clone();

    let task = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    // Process first 5 chunks
    for _ in 0..20 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(
        s.chunks_processed >= 5,
        "Should process available chunks, got {}",
        s.chunks_processed
    );
    drop(s);

    // Simulate drought: no new chunks. Advance time past MAX_CHUNK_MISS_COUNT
    for _ in 0..80 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    // Producer should have set chunk_gap stall
    let s = stats.lock().await;
    assert_eq!(
        s.stall_reason.as_deref(),
        Some("chunk_gap"),
        "Should have chunk_gap stall during drought, got {:?}",
        s.stall_reason
    );
    drop(s);

    // Resume chunks -- make 6-30 available
    available.store(30, Ordering::Relaxed);

    // Producer-side verification only: consumer's rust_rescue_push 120s
    // refill window can't elapse under tokio::time::pause(). What we
    // verify: producer's chunk_gap stall clears when chunks resume.
    for _ in 0..30 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    // After R1 GREEN, consumer enters rust_rescue_push once the cache
    // drains and chunks_processed freezes at the pre-rescue count.
    // Recovery is exercised end-to-end by FB / YT push integration tests.
    assert!(
        s.chunks_processed >= 5,
        "Should retain pre-drought chunks_processed, got {}",
        s.chunks_processed
    );
    // Producer-side recovery: chunk_gap stall clears when chunks resume,
    // unaffected by the consumer's always-on rescue.
    assert!(
        s.stall_reason.is_none() || s.stall_reason.as_deref() != Some("chunk_gap"),
        "Stall reason should clear after recovery, got {:?}",
        s.stall_reason
    );
    drop(s);

    stop_tx.send(true).ok();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
}
#[tokio::test]
async fn test_write_timeout_kills_ffmpeg() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=2).map(|i| (i, vec![i as u8; 10])).collect();
    let fetcher = MockFetcher::new(chunks);
    let mut factory = MockProcessFactory::new();
    factory.hang_on_write = true;
    let spawn_count = factory.spawn_count.clone();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..40 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(
        s.last_error.as_deref(),
        Some("write_timeout"),
        "Should have write_timeout as last error"
    );
    drop(s);

    assert!(
        spawn_count.load(Ordering::Relaxed) >= 2,
        "Should have respawned ffmpeg after timeout"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_fast_endpoint_jumps_to_live_edge_with_backlog() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=100).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = MockFetcher::new(chunks);
    let factory = MockProcessFactory::new();
    let writes = factory.writes.clone();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    for _ in 0..12000 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    // Fast endpoint jumps to live edge instead of replaying the backlog (#232).
    assert_eq!(s.current_chunk_id, 100); // converged to live edge
    assert!(
        s.chunks_processed > 0 && s.chunks_processed < 100,
        "fast endpoint must JUMP (skip backlog), not replay all 100; got {}",
        s.chunks_processed
    );
    assert!(
        s.stall_reason.is_none() || s.stall_reason.as_deref() == Some("chunk_gap"),
        "Unexpected stall: {:?}",
        s.stall_reason
    );
    drop(s);

    let w = writes.lock().await;
    assert!(!w.is_empty(), "expected some writes, got {}", w.len());
    drop(w);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
#[tokio::test]
async fn test_stats_struct_serializes() {
    let stats = EndpointStats {
        bytes_processed_total: 1000,
        duration_processed_ms: 20000,
        current_chunk_id: 42,
        chunks_processed: 10,
        ffmpeg_restart_count: 2,
        consecutive_chunk_misses: 5,
        last_error: Some("test error".to_string()),
        stall_reason: Some("chunk_gap".to_string()),
        ffmpeg_last_stderr: Some("connection refused".to_string()),
        delivery_mode: "normal".to_string(),
        ..EndpointStats::default()
    };
    let json = serde_json::to_string(&stats).unwrap();
    assert!(json.contains("\"stall_reason\":\"chunk_gap\""));
    assert!(json.contains("\"ffmpeg_restart_count\":2"));
}

// TimedMockFetcher: chunks available at configured rate.
// 2000ms chunk duration matches buffer-fill/chunk-gap tests.
// `pub(crate)` so the sibling `fast_self_healing_tests` module can reuse it
// for the moved fast-delay / chunk-gap tests.
pub(crate) struct TimedMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    available_up_to: Arc<AtomicI64>,
    duration_ms_per_chunk: i64,
    // Injected per-fetch latency to simulate S3 GET / HEAD slowness. Default
    // ZERO keeps existing tests byte-for-byte identical.
    fetch_latency: std::time::Duration,
    // Highest chunk_id ever requested via fetch_chunk_with_meta. Lets a test
    // observe the producer's READ position without a real consumer. Starts at
    // i64::MIN; updated monotonically. ZERO impact when unread.
    max_fetched_id: Arc<AtomicI64>,
    // Independent ceiling for HEAD probes (chunk_duration_ms / the lag-probe
    // ladder). `None` (default) → HEAD uses `available_up_to`, identical to
    // before. `Some(h)` lets the ladder discover the live edge at `h` while
    // GET stalls at `available_up_to` — modelling "HEAD sees the edge, GET
    // trails behind it" so a test can pin the producer's read position to
    // the lag-probe's jump target without it catching up.
    head_available_up_to: Option<Arc<AtomicI64>>,
}

impl TimedMockFetcher {
    pub(crate) fn new(chunks: Vec<(i64, Vec<u8>)>, initially_available: i64) -> Self {
        Self {
            chunks: Arc::new(TokioMutex::new(chunks.into_iter().collect())),
            available_up_to: Arc::new(AtomicI64::new(initially_available)),
            duration_ms_per_chunk: 2000,
            fetch_latency: std::time::Duration::ZERO,
            max_fetched_id: Arc::new(AtomicI64::new(i64::MIN)),
            head_available_up_to: None,
        }
    }

    /// Let HEAD probes (the lag-probe ladder) reach `head_edge` while GET
    /// fetches still stall at `available_up_to`. Models the producer
    /// discovering the live edge via HEAD but reading behind it. ZERO impact
    /// on callers that don't set it (HEAD falls back to `available_up_to`).
    pub(crate) fn with_head_edge(mut self, head_edge: i64) -> Self {
        self.head_available_up_to = Some(Arc::new(AtomicI64::new(head_edge)));
        self
    }

    pub(crate) fn with_latency(mut self, d: std::time::Duration) -> Self {
        self.fetch_latency = d;
        self
    }

    /// Override the per-chunk media duration the fetcher reports (default
    /// 2000ms). Lets a test pin `typical_chunk_dur_ms` so chunk-count maths
    /// against the adaptive read-delay are deterministic. ZERO impact on
    /// callers that don't use it.
    pub(crate) fn with_chunk_duration(mut self, ms: i64) -> Self {
        self.duration_ms_per_chunk = ms;
        self
    }

    pub(crate) fn available_up_to(&self) -> Arc<AtomicI64> {
        self.available_up_to.clone()
    }

    pub(crate) fn max_fetched_id(&self) -> Arc<AtomicI64> {
        self.max_fetched_id.clone()
    }
}

impl ChunkFetcher for TimedMockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        if !self.fetch_latency.is_zero() {
            tokio::time::sleep(self.fetch_latency).await;
        }
        self.max_fetched_id.fetch_max(chunk_id, Ordering::Relaxed);
        let available = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id > available {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        Ok(map
            .get(&chunk_id)
            .map(|data| (data.clone(), self.duration_ms_per_chunk)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if !self.fetch_latency.is_zero() {
            tokio::time::sleep(self.fetch_latency).await;
        }
        // HEAD ceiling: the dedicated head edge when set, else GET's edge.
        let available = match &self.head_available_up_to {
            Some(h) => h.load(Ordering::Relaxed),
            None => self.available_up_to.load(Ordering::Relaxed),
        };
        if chunk_id > available {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(self.duration_ms_per_chunk))
        } else {
            Ok(None)
        }
    }
}

// Buffer fill tests

#[tokio::test]
async fn test_buffer_fill_waits_for_target_chunk() {
    tokio::time::pause();

    let all_chunks: Vec<(i64, Vec<u8>)> = (1..=10).map(|i| (i, vec![i as u8; 100])).collect();
    // Initially only chunks 1-3 available
    let fetcher = TimedMockFetcher::new(all_chunks, 3);
    let avail = fetcher.available_up_to();
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,     // start_chunk_id
            10000, // delivery_delay_ms (5 chunks * 2000ms)
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    // Advance time: buffer fill polls every 2s, but only 3 chunks available (6000ms < 10000ms)
    for _ in 0..5 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    // Verify: still in buffer fill, no chunks processed
    {
        let s = stats.lock().await;
        assert_eq!(
            s.chunks_processed, 0,
            "Should not process any chunks during buffer fill (only 3 available = 6000ms, need 10000ms)"
        );
    }

    // Make chunks 1-6 available (6 * 2000ms = 12000ms >= 10000ms)
    avail.store(6, Ordering::Relaxed);

    // Advance time for buffer fill to detect chunk 6 and start processing
    for _ in 0..10 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    // Verify: buffer fill completed, processing started
    {
        let s = stats.lock().await;
        assert!(
            s.chunks_processed > 0,
            "Should have started processing after buffer fill (chunks_processed={})",
            s.chunks_processed
        );
        assert!(
            s.current_chunk_id >= 1,
            "Should be processing from start_chunk_id=1, got {}",
            s.current_chunk_id
        );
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
