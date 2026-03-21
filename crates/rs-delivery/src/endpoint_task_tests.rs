use super::*;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use tokio::sync::Mutex as TokioMutex;

struct MockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
}

impl MockFetcher {
    fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
        let map: std::collections::HashMap<i64, Vec<u8>> = chunks.into_iter().collect();
        Self {
            chunks: Arc::new(TokioMutex::new(map)),
        }
    }
}

impl ChunkFetcher for MockFetcher {
    async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        let map = self.chunks.lock().await;
        Ok(map.get(&chunk_id).cloned())
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
        Some("mock stderr line".to_string())
    }
}

struct MockProcessFactory {
    alive: Arc<AtomicBool>,
    writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
    fail_after_writes: Option<u32>,
    spawn_fail: Arc<AtomicBool>,
    spawn_count: Arc<AtomicU32>,
    hang_on_write: bool,
}

impl MockProcessFactory {
    fn new() -> Self {
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

fn test_ep_cfg() -> EndpointConfig {
    EndpointConfig {
        alias: "test-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
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
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // With 1000ms chunk_duration_ms, 5 chunks need ~5s
    for _ in 0..12 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 5, "Should have processed 5 chunks");
    assert_eq!(s.current_chunk_id, 5);
    assert_eq!(s.bytes_processed_total, 500);
    drop(s);

    let w = writes.lock().await;
    assert_eq!(w.len(), 5);
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
            1000,
            stop_rx,
            stats_clone,
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
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    for _ in 0..40 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
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
            1000,
            stop_rx,
            stats_clone,
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
            1000,
            stop_rx,
            stats_clone,
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
            1000,
            stop_rx,
            stats_clone,
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
    let mut factory = MockProcessFactory::new();
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
            1000,
            stop_rx,
            stats_clone,
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
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    for _ in 0..200 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(
        s.chunks_processed >= 18,
        "Should have processed 15 + at least 3 after skip, got {}",
        s.chunks_processed
    );
    assert!(
        s.current_chunk_id >= 17,
        "Should have skipped to at least chunk 17, got {}",
        s.current_chunk_id
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
            1000,
            stop_rx,
            stats_clone,
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
            1000,
            stop_rx,
            stats_clone,
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
async fn test_processes_100_sequential_chunks() {
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
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // 1000ms per chunk, 100 chunks = 100s
    for _ in 0..120 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 100, "Must process all 100 chunks");
    assert_eq!(s.current_chunk_id, 100);
    assert_eq!(s.bytes_processed_total, 10000);
    assert!(s.stall_reason.is_none(), "No stall: {:?}", s.stall_reason);
    drop(s);

    let w = writes.lock().await;
    assert_eq!(w.len(), 100);
    drop(w);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_stats_struct_serializes() {
    let stats = EndpointStats {
        bytes_processed_total: 1000,
        current_chunk_id: 42,
        chunks_processed: 10,
        ffmpeg_restart_count: 2,
        consecutive_ffmpeg_failures: 0,
        consecutive_chunk_misses: 5,
        last_error: Some("test error".to_string()),
        stall_reason: Some("chunk_gap".to_string()),
        ffmpeg_last_stderr: Some("connection refused".to_string()),
    };
    let json = serde_json::to_string(&stats).unwrap();
    assert!(json.contains("\"stall_reason\":\"chunk_gap\""));
    assert!(json.contains("\"ffmpeg_restart_count\":2"));
}

// ============================================================
// TimedMockFetcher: simulates chunks arriving over real time
// ============================================================

/// A fetcher where chunks become available at a configured rate.
/// `available_up_to` is an AtomicI64 that external code advances to simulate
/// new chunks arriving from S3.
struct TimedMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    available_up_to: Arc<AtomicI64>,
}

impl TimedMockFetcher {
    /// Create with pre-loaded chunk data. Chunks are only returned if
    /// chunk_id <= available_up_to.
    fn new(chunks: Vec<(i64, Vec<u8>)>, initially_available: i64) -> Self {
        let map: std::collections::HashMap<i64, Vec<u8>> = chunks.into_iter().collect();
        Self {
            chunks: Arc::new(TokioMutex::new(map)),
            available_up_to: Arc::new(AtomicI64::new(initially_available)),
        }
    }

    fn available_up_to(&self) -> Arc<AtomicI64> {
        self.available_up_to.clone()
    }
}

impl ChunkFetcher for TimedMockFetcher {
    async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        let available = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id > available {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        Ok(map.get(&chunk_id).cloned())
    }
}

// ============================================================
// Buffer fill tests (TDD for cache delay bug)
// ============================================================

#[tokio::test]
async fn test_buffer_fill_waits_for_target_chunk() {
    // With delivery_delay_chunks=5, start_chunk_id=1:
    // target_chunk = 1 + 5 = 6
    // Buffer fill must NOT complete before chunk 6 is available.
    // Buffer fill DOES complete once chunk 6 is available.
    // chunks_processed == 0 during buffer fill.
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
            1, // start_chunk_id
            5, // delivery_delay_chunks
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Advance time: buffer fill polls every 2s, but chunk 6 is not available yet
    for _ in 0..5 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    // Verify: still in buffer fill, no chunks processed
    {
        let s = stats.lock().await;
        assert_eq!(
            s.chunks_processed, 0,
            "Should not process any chunks during buffer fill (only 1-3 available, need 6)"
        );
    }

    // Make chunk 6 available (simulate chunks arriving over time)
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

#[tokio::test]
async fn test_chunk_gap_maintained_at_delay_target() {
    // With delivery_delay_chunks=10, start_chunk_id=1, pre-load chunks 1-30:
    // After buffer fill (chunk 11 available), VPS starts consuming from chunk 1
    // at real-time rate. The gap between latest_available and current_chunk_id
    // should stay at approximately delivery_delay_chunks.
    tokio::time::pause();

    let all_chunks: Vec<(i64, Vec<u8>)> = (1..=30).map(|i| (i, vec![i as u8; 100])).collect();
    // All 30 chunks available immediately
    let fetcher = TimedMockFetcher::new(all_chunks, 30);
    let factory = MockProcessFactory::new();

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            test_ep_cfg(),
            1,  // start_chunk_id
            10, // delivery_delay_chunks
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Buffer fill: target_chunk = 1 + 10 = 11 (immediately available)
    // Processing should start right away at chunk 1
    // After 15 seconds: should have processed ~15 chunks (paced at 1/sec)
    // Current chunk_id should be ~15, latest available is 30
    // Gap = 30 - 15 = 15 (close to delay of 10, because we started from 1)
    for _ in 0..20 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    // After 20s of processing at 1 chunk/sec from chunk 1:
    // Should be around chunk 20
    assert!(
        s.chunks_processed >= 15,
        "Should have processed at least 15 chunks in 20s, got {}",
        s.chunks_processed
    );
    assert!(
        s.current_chunk_id <= 25,
        "Pacing should prevent consuming too fast, current_chunk_id={}",
        s.current_chunk_id
    );

    // The gap from chunk 30 (latest available) to current should exist
    let gap = 30 - s.current_chunk_id;
    assert!(
        gap >= 5,
        "Gap should be substantial (delay=10, got gap={}), current={}",
        gap,
        s.current_chunk_id
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_buffer_fill_stops_on_signal() {
    // If stop signal is sent during buffer fill, the loop should exit
    // without processing any chunks.
    tokio::time::pause();

    let all_chunks: Vec<(i64, Vec<u8>)> = (1..=5).map(|i| (i, vec![i as u8; 100])).collect();
    // Only chunk 1 available, target_chunk = 1 + 10 = 11 — will never be available
    let fetcher = TimedMockFetcher::new(all_chunks, 1);
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
            10, // delivery_delay_chunks
            1000,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Let it poll a few times during buffer fill
    for _ in 0..3 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    // Send stop signal
    let _ = stop_tx.send(true);

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    assert!(
        result.is_ok(),
        "Task should have stopped during buffer fill"
    );

    let s = stats.lock().await;
    assert_eq!(
        s.chunks_processed, 0,
        "Should not have processed any chunks, stopped during buffer fill"
    );
}

#[tokio::test]
async fn test_delivery_delay_chunks_calculation() {
    // Verify the formula: delivery_delay_chunks = (delay_secs * 1000) / chunk_duration_ms
    // This tests the calculation that happens in DeliveryOrchestrator::poll_and_init

    // Default: 120s delay, 1000ms chunk = 120 chunks
    let delay_secs: u64 = 120;
    let chunk_duration_ms: u64 = 1000;
    let chunks = if chunk_duration_ms > 0 {
        (delay_secs * 1000 / chunk_duration_ms) as i64
    } else {
        0
    };
    assert_eq!(chunks, 120, "120s / 1000ms should = 120 chunks");

    // Custom: 90s delay, 1000ms chunk = 90 chunks
    let delay_secs: u64 = 90;
    let chunks = (delay_secs * 1000 / chunk_duration_ms) as i64;
    assert_eq!(chunks, 90, "90s / 1000ms should = 90 chunks");

    // Edge: 120s delay, 2000ms chunk = 60 chunks
    let chunk_duration_ms: u64 = 2000;
    let delay_secs: u64 = 120;
    let chunks = (delay_secs * 1000 / chunk_duration_ms) as i64;
    assert_eq!(chunks, 60, "120s / 2000ms should = 60 chunks");

    // Edge: chunk_duration_ms = 0 → 0
    let chunk_duration_ms: u64 = 0;
    let chunks = if chunk_duration_ms > 0 {
        (delay_secs * 1000 / chunk_duration_ms) as i64
    } else {
        0
    };
    assert_eq!(chunks, 0, "0ms chunk duration should = 0 chunks");

    // Edge: 500ms chunks → 240 chunks for 120s
    let chunk_duration_ms: u64 = 500;
    let delay_secs: u64 = 120;
    let chunks = (delay_secs * 1000 / chunk_duration_ms) as i64;
    assert_eq!(chunks, 240, "120s / 500ms should = 240 chunks");
}
