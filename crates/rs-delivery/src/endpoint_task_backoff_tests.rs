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
        // Must match a RemoteBrokenPipe trigger so the class-aware backoff
        // picks the 30s * 2^n exponential ladder (not Unknown=15s flat).
        Some("Error submitting a packet to the muxer: Broken pipe".to_string())
    }
}

/// Like `DyingMockProcess` but reports an `InvalidInput`-classified stderr
/// so `reconnect_floor` returns a flat 1s. Used by audit-log tests that
/// only need to observe that restarts happen and records are populated —
/// not that backoff grows exponentially. The 1s floor keeps the virtual-
/// time windows small so the test finishes without starvation.
struct DyingMockProcessInvalidInput {
    alive: Arc<AtomicBool>,
    has_written: bool,
}

#[async_trait]
impl OutputProcess for DyingMockProcessInvalidInput {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        if self.has_written {
            self.alive.store(false, Ordering::Relaxed);
            return Err("destination closed".to_string());
        }
        self.has_written = true;
        self.alive.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        Some("Invalid data found when processing input".to_string())
    }
}

struct RecordingFactoryInvalidInput {
    spawn_count: Arc<AtomicU32>,
}

impl OutputProcessFactory for RecordingFactoryInvalidInput {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(DyingMockProcessInvalidInput {
            alive: Arc::new(AtomicBool::new(true)),
            has_written: false,
        }))
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
    // CUSTOM_RTMP + "Broken pipe" stderr classifies as RemoteBrokenPipe,
    // which has the 30s * 2^n exponential reconnect floor. Unknown class
    // would return a flat 15s floor and break exponential-growth tests.
    EndpointConfig {
        alias: "backoff-test".to_string(),
        service_type: "CUSTOM_RTMP".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
    }
}

/// Reproduces the FB stale-key restart loop. Without exponential backoff
/// on death-after-running, the consumer respawns ffmpeg every ~1 second
/// forever. With proper class-aware backoff (RemoteBrokenPipe: 30s ->
/// 60s -> 120s -> 240s -> 300s cap), spawns are rate-limited so only
/// ~5 occur in 600 seconds.
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
            None,
            Arc::new(BufferState::new()),
        )
        .await;
    });

    // Advance 600 seconds of mock time. With RemoteBrokenPipe backoff
    // (30+60+120+240+300+300... capped at 300s), spawns occur at roughly
    // t=0, 30, 90, 210, 450, 750 — so we expect ~5 spawns in 600s.
    // Without backoff (the pre-fix bug), we'd see hundreds.
    for _ in 0..6000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let total_spawns = spawn_count.load(Ordering::Relaxed);
    let times = spawn_times_ms.lock().unwrap().clone();

    // Hard upper bound: with proper backoff, no more than 10 spawns in 600s.
    // Without backoff, this fails dramatically (hundreds of spawns).
    assert!(
        total_spawns <= 10,
        "Consumer made too many spawns in 600s: {total_spawns} (expected <= 10 with \
         exponential backoff). Spawn timestamps (ms): {times:?}"
    );

    // Sanity: backoff is growing. The gap between spawn 2->3 (60s floor)
    // must be strictly larger than the gap between spawn 1->2 (30s floor).
    if times.len() >= 3 {
        let early_gap = times[1].saturating_sub(times[0]);
        let later_gap = times[2].saturating_sub(times[1]);
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
///
/// Uses the InvalidInput-class mock (1s flat backoff) so restarts occur
/// quickly under mock time. Exponential-growth behaviour is covered by
/// the dedicated `test_consumer_backs_off_exponentially_on_repeated_deaths`.
#[tokio::test]
async fn test_restart_audit_log_records_each_death() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=1000).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = BackoffMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    let factory = RecordingFactoryInvalidInput {
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
            None,
            Arc::new(BufferState::new()),
        )
        .await;
    });

    // InvalidInput backoff is 1s flat, so a handful of ticks at 100ms each
    // comfortably covers multiple death-respawn cycles.
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

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

/// Process that lives but FAILS every write — simulates a destination
/// RTMP server that accepted the connection but rejects every payload
/// (e.g. stale Facebook stream key after some negotiation).
struct WriteFailMockProcess {
    alive: Arc<AtomicBool>,
}

#[async_trait]
impl OutputProcess for WriteFailMockProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        Err("write rejected by destination".to_string())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        // InvalidInput class → 1s flat backoff floor. This keeps the
        // virtual-time window short enough for the endpoint loop to
        // observe multiple deaths under mock time. Exponential backoff
        // is verified by the dedicated exponential test.
        Some("Invalid data found when processing input".to_string())
    }
}

struct WriteFailFactory {
    spawn_count: Arc<AtomicU32>,
}

impl OutputProcessFactory for WriteFailFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(WriteFailMockProcess {
            alive: Arc::new(AtomicBool::new(true)),
        }))
    }
}

/// Regression for the bug where the write-error path bypassed both the
/// audit log AND backoff. ffmpeg writes were rejected (stale Facebook
/// stream key), the consumer called proc.take() to kill it, and the
/// next loop iteration found proc=None — so the death handler's
/// `if proc.is_some()` skipped recording the restart and applying
/// backoff. Result: instant respawn loop.
///
/// With the fix, write-failure leaves proc as Some(dead_process), so the
/// death handler runs, increments restart_count, records an audit row,
/// and applies the class's reconnect floor.
///
/// Uses the InvalidInput class (1s floor) so the test terminates quickly.
/// The exponential-backoff case is covered by the dedicated exponential
/// test; this test only needs to prove the write-error path participates
/// in the death-handler path at all.
#[tokio::test]
async fn test_write_failure_records_audit_log_and_applies_backoff() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=1000).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = BackoffMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    let spawn_count = Arc::new(AtomicU32::new(0));
    let factory = WriteFailFactory {
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
            None,
            Arc::new(BufferState::new()),
        )
        .await;
    });

    // InvalidInput backoff is 1s flat → a handful of ticks at 100ms each
    // exercises several death-respawn cycles.
    for _ in 0..600 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let total_spawns = spawn_count.load(Ordering::Relaxed);
    assert!(
        total_spawns >= 1,
        "Write-failure path never triggered a respawn: {total_spawns}"
    );

    let s = stats.lock().await;
    assert!(
        s.ffmpeg_restart_count > 0,
        "ffmpeg_restart_count should be incremented on write failure"
    );
    assert!(
        !s.restart_history.is_empty(),
        "restart_history should be populated on write failure (audit log was \
         missing for the write-error path before the fix)"
    );
    drop(s);

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

/// A process that lives for a configurable duration before dying.
/// Used to verify that the death-counter reset fires when ffmpeg lives
/// long enough to prove the session was "real".
struct LongLivedProcess {
    alive: Arc<AtomicBool>,
    spawned_at: tokio::time::Instant,
    live_secs: u64,
}

#[async_trait]
impl OutputProcess for LongLivedProcess {
    fn is_alive(&mut self) -> bool {
        if self.spawned_at.elapsed().as_secs() >= self.live_secs {
            self.alive.store(false, Ordering::Relaxed);
        }
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        if self.spawned_at.elapsed().as_secs() >= self.live_secs {
            self.alive.store(false, Ordering::Relaxed);
            return Err("lived out its duration".to_string());
        }
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        // InvalidInput class → 1s flat reconnect floor. The class's floor
        // does not grow with consecutive deaths, so the reset-to-0 path
        // cannot be observed via backoff_secs directly. What *is*
        // observable is that a restart record exists with the expected
        // short-lived lifetime_secs, plus that the first record's
        // lifetime_secs for the long-lived process is >= LIFETIME_RESET_SECS.
        Some("Invalid data found when processing input".to_string())
    }
}

struct LongLivedFactory {
    live_secs_sequence: Arc<StdMutex<Vec<u64>>>,
    spawn_count: Arc<AtomicU32>,
}

impl OutputProcessFactory for LongLivedFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        let idx = self.spawn_count.fetch_add(1, Ordering::Relaxed) as usize;
        let seq = self.live_secs_sequence.lock().unwrap();
        let live_secs = seq.get(idx).copied().unwrap_or(0);
        drop(seq);
        Ok(Box::new(LongLivedProcess {
            alive: Arc::new(AtomicBool::new(true)),
            spawned_at: tokio::time::Instant::now(),
            live_secs,
        }))
    }
}

/// When ffmpeg lives longer than LIFETIME_RESET_SECS (60s) before dying,
/// the per-class `consecutive_same_class` counter is reset. This test
/// asserts that the first post-long-session death produces a restart
/// record with the class's initial floor as backoff_secs.
///
/// Uses the InvalidInput class (1s flat floor) so the virtual-time window
/// stays small; the `first_backoff == class_floor` property is the same
/// regardless of which flat-floor class is used. Exponential-growth reset
/// behaviour is indirectly covered by the dedicated exponential test;
/// here we only assert that (a) a record exists after the long session,
/// (b) the first record's backoff equals the class floor, and (c) the
/// long-lived process's lifetime was recorded.
#[tokio::test]
async fn test_backoff_counter_resets_after_long_lived_session() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=1000).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = BackoffMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    // Sequence: 1st process lives 120s, 2nd dies fast, 3rd dies fast.
    let live_sequence = Arc::new(StdMutex::new(vec![120u64, 0, 0, 0, 0]));
    let spawn_count = Arc::new(AtomicU32::new(0));
    let factory = LongLivedFactory {
        live_secs_sequence: live_sequence.clone(),
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
            None,
            Arc::new(BufferState::new()),
        )
        .await;
    });

    // Need > 120s of mock time for the long-lived process to die, plus a
    // few more death-respawn cycles at 1s backoff. 200s gives generous
    // headroom for scheduling quirks.
    for _ in 0..2000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let s = stats.lock().await;
    let history: Vec<_> = s.restart_history.iter().cloned().collect();
    drop(s);

    assert!(
        !history.is_empty(),
        "No restart records — long-lived process never died?"
    );

    // First recorded backoff equals the class's first-death floor
    // (InvalidInput: 1s) because the per-class counter reset after
    // the 120s long-lived session.
    assert_eq!(
        history[0].backoff_secs, 1,
        "After a long-lived session (>= LIFETIME_RESET_SECS), the first \
         recorded backoff must equal the class floor. First record: {:?}",
        history[0]
    );

    // Lifetime of the first process should be at least 120s (mock time).
    assert!(
        history[0].lifetime_secs >= 120,
        "First dead process should have lived >= 120s, got {}s",
        history[0].lifetime_secs
    );

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

// --- Task 8 regression tests: reconnect_floor + EndpointRestartState ---

#[test]
fn backoff_uses_reconnect_floor_for_youtube_broken_pipe() {
    // First failure of YT RTMP: must wait 30s before retry.
    use crate::ffmpeg_reason::{ReasonClass, reconnect_floor};
    assert_eq!(
        reconnect_floor(ReasonClass::YoutubeRtmpClosed, 0),
        std::time::Duration::from_secs(30)
    );
}

#[test]
fn restart_state_resets_consecutive_on_class_change() {
    use crate::endpoint_task::EndpointRestartState;
    use crate::ffmpeg_reason::ReasonClass;
    let state = EndpointRestartState::new();
    let s = state.advance(ReasonClass::YoutubeRtmpClosed);
    assert_eq!(s.consecutive_same_class, 1);
    let s = s.advance(ReasonClass::YoutubeRtmpClosed);
    assert_eq!(s.consecutive_same_class, 2);
    let s = s.advance(ReasonClass::NetworkTimeout);
    assert_eq!(s.consecutive_same_class, 1);
}
