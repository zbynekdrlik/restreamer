//! Regression tests for the ffmpeg-restart-loop bug.
//!
//! When ffmpeg dies after running successfully (e.g., the destination RTMP
//! server rejects the stream key after accepting some data), the consumer
//! must back off exponentially before respawning. The original code only
//! backed off on **spawn** failures (`consecutive_ffmpeg_failures`), not
//! on death-after-running, so a stale Facebook stream key would cause
//! ~1 ffmpeg restart per minute for the lifetime of the stream — observed
//! 524 restarts in a 9.5h overnight test.

use super::*;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

// Minimal duplicated test helpers (kept small for the file-size budget).

struct BackoffMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
}

impl ChunkFetcher for BackoffMockFetcher {
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

/// A process that dies after the first write — simulates a destination
/// RTMP server that closes the connection (e.g., stale stream key).
struct DyingMockProcess {
    alive: Arc<AtomicBool>,
    has_written: bool,
}

#[async_trait]
impl OutputProcess for DyingMockProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        if self.has_written {
            self.alive.store(false, Ordering::Relaxed);
            return Err("destination closed".to_string());
        }
        self.has_written = true;
        // Process accepts the first write, then dies on the next is_alive
        // check.
        self.alive.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        Some("destination closed".to_string())
    }
}

/// Factory that records every spawn timestamp (mock-time) so the test can
/// verify the gaps grow exponentially.
struct RecordingFactory {
    spawn_times_ms: Arc<StdMutex<Vec<u64>>>,
    anchor: tokio::time::Instant,
    spawn_count: Arc<AtomicU32>,
}

impl OutputProcessFactory for RecordingFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        let elapsed_ms = self.anchor.elapsed().as_millis() as u64;
        self.spawn_times_ms.lock().unwrap().push(elapsed_ms);
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(DyingMockProcess {
            alive: Arc::new(AtomicBool::new(true)),
            has_written: false,
        }))
    }
}

fn backoff_test_ep_cfg() -> EndpointConfig {
    EndpointConfig {
        alias: "backoff-test".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
    }
}

/// Reproduces the FB stale-key restart loop. Without exponential backoff
/// on death-after-running, the consumer respawns ffmpeg every ~1 second
/// forever. With proper backoff (1s -> 2s -> 4s -> 8s -> ... capped at
/// 60s), spawns are rate-limited so only ~6-8 occur in 60 seconds.
#[tokio::test]
async fn test_consumer_backs_off_exponentially_on_repeated_deaths() {
    tokio::time::pause();

    // Plenty of chunks so the producer never starves.
    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=1000).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = BackoffMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    let spawn_times_ms: Arc<StdMutex<Vec<u64>>> = Arc::new(StdMutex::new(Vec::new()));
    let spawn_count = Arc::new(AtomicU32::new(0));
    let factory = RecordingFactory {
        spawn_times_ms: spawn_times_ms.clone(),
        anchor: tokio::time::Instant::now(),
        spawn_count: spawn_count.clone(),
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            backoff_test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Advance 120 seconds of mock time. With proper backoff (1+2+4+8+16+
    // 32+60+60 = 183s, the 60s cap kicks in early), we should see ~7 spawns
    // in 120s. Without backoff (current bug), we'd see ~120 spawns.
    for _ in 0..1200 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let total_spawns = spawn_count.load(Ordering::Relaxed);
    let times = spawn_times_ms.lock().unwrap().clone();

    // Hard upper bound: with proper backoff, no more than 10 spawns in 120s.
    // Without backoff, this fails dramatically (typically 60-120 spawns).
    assert!(
        total_spawns <= 10,
        "Consumer made too many spawns in 120s: {total_spawns} (expected <= 10 with \
         exponential backoff). Spawn timestamps (ms): {times:?}"
    );

    // Sanity: backoff is at least growing. Check the gap between the 2nd
    // and 5th spawn is bigger than the gap between the 1st and 2nd.
    if times.len() >= 5 {
        let early_gap = times[1].saturating_sub(times[0]);
        let later_gap = times[4].saturating_sub(times[3]);
        assert!(
            later_gap > early_gap,
            "Backoff is not growing: early gap {early_gap}ms, later gap {later_gap}ms. \
             All times: {times:?}"
        );
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

/// Each ffmpeg death must produce a row in the per-endpoint restart_history
/// ring buffer. The ring is capped at RESTART_HISTORY_CAP — past that point
/// the oldest record is dropped.
#[tokio::test]
async fn test_restart_audit_log_records_each_death() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=1000).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = BackoffMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    let factory = RecordingFactory {
        spawn_times_ms: Arc::new(StdMutex::new(Vec::new())),
        anchor: tokio::time::Instant::now(),
        spawn_count: Arc::new(AtomicU32::new(0)),
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            backoff_test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Run long enough to record several deaths.
    for _ in 0..600 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    let history = s.restart_history.clone();
    drop(s);

    assert!(
        !history.is_empty(),
        "restart_history is empty — audit log not wired up"
    );

    // Verify the structure of the first record.
    let first = &history[0];
    assert!(
        first.timestamp_ms > 0,
        "restart record missing timestamp_ms"
    );
    assert!(
        first.backoff_secs > 0,
        "restart record missing backoff_secs"
    );
    assert!(
        !first.reason.is_empty(),
        "restart record missing reason classification"
    );

    // Backoff in the records should be growing (exponential).
    if history.len() >= 3 {
        assert!(
            history[2].backoff_secs >= history[0].backoff_secs,
            "backoff is not growing across records: {:?}",
            history.iter().map(|r| r.backoff_secs).collect::<Vec<_>>()
        );
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

/// The restart_history ring buffer must be bounded — old records get
/// evicted past RESTART_HISTORY_CAP.
#[tokio::test]
async fn test_restart_audit_log_is_bounded() {
    let mut s = EndpointStats::default();
    for i in 0..(RESTART_HISTORY_CAP + 50) {
        if s.restart_history.len() >= RESTART_HISTORY_CAP {
            s.restart_history.pop_front();
        }
        s.restart_history.push_back(FfmpegRestartRecord {
            timestamp_ms: i as i64,
            chunk_id: i as i64,
            lifetime_secs: 0,
            reason: "test".to_string(),
            stderr_tail: None,
            backoff_secs: 1,
        });
    }
    assert_eq!(s.restart_history.len(), RESTART_HISTORY_CAP);
    // Oldest 50 entries dropped — first remaining record should have
    // chunk_id == 50.
    assert_eq!(s.restart_history[0].chunk_id, 50);
}
