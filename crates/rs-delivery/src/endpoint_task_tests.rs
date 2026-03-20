use super::*;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
