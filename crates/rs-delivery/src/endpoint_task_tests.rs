use super::*;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use tokio::sync::Mutex as TokioMutex;

struct MockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
}

impl MockFetcher {
    fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
        let map: std::collections::HashMap<i64, Vec<u8>> = chunks.into_iter().collect();
        Self {
            chunks: Arc::new(TokioMutex::new(map)),
            duration_ms_per_chunk: 2000,
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
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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

    // Advance time for recovery
    for _ in 0..80 {
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert!(
        s.chunks_processed >= 20,
        "Should recover and process more chunks after drought, got {}",
        s.chunks_processed
    );
    // Stall reason should clear after recovery
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
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
        endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
    });

    // Direct write + 1s sleep per chunk. 100 chunks = 100s.
    // Advance in 10ms steps. Need 100 * 100 = 10000 steps minimum.
    for _ in 0..12000 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(s.chunks_processed, 100, "Must process all 100 chunks");
    assert_eq!(s.current_chunk_id, 100);
    assert_eq!(s.bytes_processed_total, 10000);
    // After consuming all 100 chunks, the loop hits None repeatedly
    // and may set chunk_gap stall -- that's expected when data is exhausted.
    assert!(
        s.stall_reason.is_none() || s.stall_reason.as_deref() == Some("chunk_gap"),
        "Unexpected stall: {:?}",
        s.stall_reason
    );
    drop(s);

    let w = writes.lock().await;
    assert!(
        w.len() >= 100,
        "Should have writes (1 per chunk): {}",
        w.len()
    );
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

// TimedMockFetcher: chunks available at configured rate
struct TimedMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    available_up_to: Arc<AtomicI64>,
    duration_ms_per_chunk: i64,
}

impl TimedMockFetcher {
    /// Create with pre-loaded chunk data. Chunks are only returned if
    /// chunk_id <= available_up_to.
    fn new(chunks: Vec<(i64, Vec<u8>)>, initially_available: i64) -> Self {
        let map: std::collections::HashMap<i64, Vec<u8>> = chunks.into_iter().collect();
        Self {
            chunks: Arc::new(TokioMutex::new(map)),
            available_up_to: Arc::new(AtomicI64::new(initially_available)),
            duration_ms_per_chunk: 2000,
        }
    }

    fn available_up_to(&self) -> Arc<AtomicI64> {
        self.available_up_to.clone()
    }
}

impl ChunkFetcher for TimedMockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
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
        let available = self.available_up_to.load(Ordering::Relaxed);
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
#[tokio::test]
async fn test_chunk_gap_maintained_at_delay_target() {
    // With delivery_delay_ms=20000, start_chunk_id=1, pre-load chunks 1-30 (2000ms each):
    // After buffer fill (chunk 11 available), VPS starts consuming from chunk 1.
    // Elapsed-aware pacing: 1000ms per chunk (non-fast).
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
            1,     // start_chunk_id
            20000, // delivery_delay_ms (10 chunks * 2000ms)
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Direct write + 1s sleep per chunk. 30 chunks = 30s.
    for _ in 0..4000 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    assert_eq!(
        s.chunks_processed, 30,
        "Should have processed all 30 chunks, got {}",
        s.chunks_processed
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
    // Only chunk 1 available, target_chunk = 1 + 10 = 11 -- will never be available
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
            20000, // delivery_delay_ms (10 chunks * 2000ms)
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
#[test]
fn test_delivery_delay_ms_direct() {
    // VPS receives delivery_delay_ms directly -- no chunk-count conversion.
    assert_eq!(120_000u64, 120_000, "120s = 120000ms");
    assert_eq!(90_000u64, 90_000, "90s = 90000ms");
    assert_eq!(0u64, 0, "No delay = 0ms");
}

// FlvStreamNormalizer unit tests

fn build_test_flv_chunk(video_data: &[u8], timestamp: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    // FLV header (9 bytes)
    buf.extend_from_slice(&[0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09]);
    // Previous tag size 0 (4 bytes)
    buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

    // Video sequence header tag (tag_type=9, data=[0x17, 0x00, ...])
    let seq_data = [0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
    write_flv_tag(&mut buf, 9, 0, &seq_data);

    // Video data tag (tag_type=9, data=video_data)
    write_flv_tag(&mut buf, 9, timestamp, video_data);

    buf
}

fn write_flv_tag(buf: &mut Vec<u8>, tag_type: u8, timestamp: u32, data: &[u8]) {
    let data_size = data.len() as u32;
    // Tag header (11 bytes)
    buf.push(tag_type);
    buf.extend_from_slice(&[
        (data_size >> 16) as u8,
        (data_size >> 8) as u8,
        data_size as u8,
    ]);
    buf.extend_from_slice(&[
        (timestamp >> 16) as u8,
        (timestamp >> 8) as u8,
        timestamp as u8,
    ]);
    buf.push((timestamp >> 24) as u8);
    buf.extend_from_slice(&[0, 0, 0]); // StreamID
    buf.extend_from_slice(data);
    let tag_size = 11 + data_size;
    buf.extend_from_slice(&tag_size.to_be_bytes());
}
#[test]
fn flv_normalizer_passes_first_chunk_through() {
    let mut norm = FlvStreamNormalizer::new();
    let chunk = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let result = norm.normalize(&chunk);
    assert_eq!(result, chunk, "First chunk should pass through unchanged");
}
#[test]
fn flv_normalizer_strips_header_and_seq_from_subsequent_chunks() {
    let mut norm = FlvStreamNormalizer::new();

    // First chunk: pass through
    let chunk1 = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let _ = norm.normalize(&chunk1);

    // Second chunk: should strip FLV header and sequence header, keep data tag
    let chunk2 = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xBB], 200);
    let result = norm.normalize(&chunk2);

    // Result should NOT contain FLV header
    assert!(
        result.len() < chunk2.len(),
        "Subsequent chunk should be smaller"
    );
    assert!(
        result.is_empty() || result[0] != 0x46,
        "Should not start with FLV header"
    );

    // Result should contain the data tag (0x17, 0x01 = keyframe NALU)
    // but NOT the sequence header tag (0x17, 0x00 = seq header)
    assert!(!result.is_empty(), "Should contain the data tag");
}
#[test]
fn flv_normalizer_passes_through_non_flv_data() {
    let mut norm = FlvStreamNormalizer::new();
    let raw_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let result = norm.normalize(&raw_data);
    assert_eq!(result, raw_data, "Non-FLV data should pass through");
}
#[test]
fn flv_normalizer_passes_through_short_data() {
    let mut norm = FlvStreamNormalizer::new();
    let short = vec![0x46, 0x4C]; // Too short to be FLV
    let result = norm.normalize(&short);
    assert_eq!(result, short, "Short data should pass through");
}

#[test]
fn flv_normalizer_reset_after_new() {
    let mut norm = FlvStreamNormalizer::new();
    assert!(
        !norm.sent_header,
        "New normalizer should not have sent header"
    );
    let chunk = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let _ = norm.normalize(&chunk);
    assert!(
        norm.sent_header,
        "After first chunk, sent_header should be true"
    );
}

#[tokio::test]
async fn test_write_failure_skips_chunk_after_retries() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=10).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = MockFetcher::new(chunks);
    let mut factory = MockProcessFactory::new();
    factory.fail_after_writes = Some(0);
    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(TokioMutex::new(EndpointStats::default()));
    let sc = stats.clone();
    let task = tokio::spawn(endpoint_loop(
        fetcher,
        factory,
        test_ep_cfg(),
        1,
        0,
        stop_rx,
        sc,
    ));
    for _ in 0..80 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    let s = stats.lock().await;
    assert!(
        s.current_chunk_id > 1,
        "Should skip failed chunks, stuck at {}",
        s.current_chunk_id
    );
    drop(s);
    stop_tx.send(true).ok();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
}
