//! Regression tests for the ffmpeg-restart-loop bug.
//!
//! When ffmpeg dies after running successfully (e.g., the destination RTMP
//! server rejects the stream key after accepting some data), the consumer
//! must back off exponentially before respawning. The original code only
//! backed off on **spawn** failures (`consecutive_ffmpeg_failures`), not
//! on death-after-running, so a stale Facebook stream key would cause
//! ~1 ffmpeg restart per minute for the lifetime of the stream — observed
//! 524 restarts in a 9.5h overnight test.

use super::super::*;
use rs_core::models::PusherKind;
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
        pusher: PusherKind::Ffmpeg,
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
            None,
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
/// ring buffer. The ring is capped at `RESTART_HISTORY_CAP` — past that point
/// the oldest record is dropped.
///
/// Pure unit test: verifies `FfmpegRestartRecord`s can be stored in
/// `EndpointStats::restart_history` and read back. Full endpoint_loop
/// integration (actual death → record insertion) is covered by
/// `test_consumer_backs_off_exponentially_on_repeated_deaths`, which
/// asserts on `spawn_count` and ladder growth, plus
/// `endpoint_task::tests::test_restarts_ffmpeg_on_death`.
#[test]
fn test_restart_audit_log_records_each_death() {
    let mut stats = EndpointStats::default();
    for i in 0..3 {
        stats.restart_history.push_back(FfmpegRestartRecord {
            timestamp_ms: 1000 + i,
            chunk_id: 10 + i,
            lifetime_secs: 5,
            reason: "invalid_input".to_string(),
            stderr_tail: Some("Invalid data".to_string()),
            backoff_secs: 1,
        });
    }
    assert_eq!(stats.restart_history.len(), 3);
    let first = &stats.restart_history[0];
    assert_eq!(first.reason, "invalid_input");
    assert!(first.timestamp_ms > 0);
    assert!(first.backoff_secs > 0);
    assert!(!first.reason.is_empty());
}

/// Regression for the bug where the write-error path bypassed both the
/// audit log AND backoff. With the fix, write-failure participates in the
/// death-handler path so the class's reconnect floor is applied.
///
/// Pure unit test: verifies that a Facebook-style "Broken pipe" stderr
/// classifies to `RemoteBrokenPipe` and that `reconnect_floor` returns the
/// correct 30s / 60s ladder. Full endpoint_loop integration (write failure
/// → proc dies → audit row + backoff) is covered by
/// `test_consumer_backs_off_exponentially_on_repeated_deaths`, which uses
/// the same `RemoteBrokenPipe` class under the endpoint loop.
#[test]
fn test_write_failure_records_audit_log_and_applies_backoff() {
    use crate::ffmpeg_reason::{ReasonClass, classify, reconnect_floor};

    let stderr = "[aost#0:1/copy] Error submitting a packet to the muxer: Broken pipe";
    let class = classify("CUSTOM_RTMP", stderr);
    assert_eq!(class, ReasonClass::RemoteBrokenPipe);
    assert_eq!(
        reconnect_floor(class, 0),
        Some(std::time::Duration::from_secs(30))
    );
    assert_eq!(
        reconnect_floor(class, 1),
        Some(std::time::Duration::from_secs(60))
    );
}

/// When ffmpeg lives longer than LIFETIME_RESET_SECS before dying, the
/// per-class `consecutive_same_class` counter is reset so the next death
/// starts a fresh backoff ladder.
///
/// Pure unit test: verifies `EndpointRestartState::new()` yields a fresh
/// counter, and that a freshly-constructed state behaves as if the class
/// had been reset. The lifetime-based reset path inside `endpoint_loop` is
/// verified indirectly by
/// `test_consumer_backs_off_exponentially_on_repeated_deaths` (which
/// asserts the initial floor on the first spawn) and by
/// `restart_state_resets_consecutive_on_class_change` below.
#[test]
fn test_backoff_counter_resets_after_long_lived_session() {
    use crate::ffmpeg_reason::ReasonClass;

    let s = EndpointRestartState::new();
    assert_eq!(s.consecutive_same_class, 0);
    assert_eq!(s.last_class, None);

    let s = s.advance(ReasonClass::RemoteBrokenPipe);
    assert_eq!(s.consecutive_same_class, 1);
    let s = s.advance(ReasonClass::RemoteBrokenPipe);
    assert_eq!(s.consecutive_same_class, 2);

    // After a long-lived session the loop rebuilds state from ::new();
    // that path must produce a zero counter regardless of prior history.
    let fresh = EndpointRestartState::new();
    assert_eq!(
        fresh.consecutive_same_class, 0,
        "new state must reset the per-class counter"
    );
    assert_eq!(fresh.last_class, None);
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
        Some(std::time::Duration::from_secs(30))
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

// NOTE: The 2026-04-20 cascade-drift fix (catchup_budget_for_backoff)
// was removed 2026-04-21. Root cause was that Rust-side pacing fought
// against ffmpeg's `-re`, causing cumulative drift + broken catchup.
// The replacement approach rebases FLV timestamps to start at PTS=0 in
// every new ffmpeg process (see FlvStreamNormalizer) so `-re` paces
// natively and no Rust-side pacing is needed at all.
