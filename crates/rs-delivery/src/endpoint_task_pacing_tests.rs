//! Regression tests for the cache-collapse-on-ffmpeg-restart bug.
//!
//! These tests live in a separate file because the main endpoint_task_tests.rs
//! is already at the project's 1000-line per-file cap.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::Mutex as TokioMutex;

// Minimal duplicated test helpers. Kept intentionally small so the primary
// endpoint_task_tests.rs remains the single source for test infrastructure;
// these pacing regression tests only need the handful of pieces below.

struct PacingMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
}

impl ChunkFetcher for PacingMockFetcher {
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

struct PacingMockProcess {
    alive: Arc<AtomicBool>,
    fail_after: Option<u32>,
    write_count: u32,
}

#[async_trait]
impl OutputProcess for PacingMockProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        self.write_count += 1;
        if let Some(limit) = self.fail_after {
            if self.write_count > limit {
                self.alive.store(false, Ordering::Relaxed);
                return Err("mock process died".to_string());
            }
        }
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        Some("mock".to_string())
    }
}

struct PacingMockProcessFactory {
    alive: Arc<AtomicBool>,
    fail_after_writes: Option<u32>,
    spawn_count: Arc<AtomicU32>,
}

impl OutputProcessFactory for PacingMockProcessFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        self.alive.store(true, Ordering::Relaxed);
        Ok(Box::new(PacingMockProcess {
            alive: self.alive.clone(),
            fail_after: self.fail_after_writes,
            write_count: 0,
        }))
    }
}

fn pacing_test_ep_cfg() -> EndpointConfig {
    EndpointConfig {
        alias: "test-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
    }
}

/// Regression test for the cache-collapse-on-ffmpeg-restart bug.
///
/// Before the fix, consumer_task wrote chunks to ffmpeg as fast as they
/// arrived from the channel. On ffmpeg restart, the fresh ffmpeg process
/// saw absolute FLV timestamps deep in the past and drained stdin as fast
/// as possible to "catch up", burning through the pre-fetch buffer in
/// seconds and collapsing the configured cache delay to ~0s.
///
/// This test asserts that with a mock process that accepts writes
/// instantly, the consumer still paces chunk delivery to real time via
/// its internal pacing anchor.
#[tokio::test]
async fn test_consumer_paces_chunk_delivery_to_real_time() {
    tokio::time::pause();

    // 50 chunks at 2000ms each. A naive consumer would burn through them
    // in microseconds against a fast mock process; a paced consumer sleeps.
    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=50).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = PacingMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };
    let factory = PacingMockProcessFactory {
        alive: Arc::new(AtomicBool::new(true)),
        fail_after_writes: None,
        spawn_count: Arc::new(AtomicU32::new(0)),
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            pacing_test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Advance 10 seconds of mock time. With real-time pacing the consumer
    // should deliver at most ~5 chunks (10000ms / 2000ms per chunk). A
    // naive consumer (no Rust pacing) would deliver all 50 instantly.
    for _ in 0..100 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let processed = stats.lock().await.chunks_processed;

    // Upper bound: real-time pacing limits delivery to (wall_elapsed /
    // chunk_duration) + 1 for the initial unpaced write + slack.
    assert!(
        processed <= 7,
        "Consumer ran faster than real time: processed {processed} chunks \
         in 10s of mock time (should pace to ~5 chunks max)"
    );

    // Lower bound: consumer must make progress.
    assert!(
        processed >= 3,
        "Consumer not making progress: processed only {processed} chunks \
         in 10s of mock time"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}

/// Regression test: pacing anchor is preserved across ffmpeg restarts,
/// so the cache delay survives restarts without collapsing.
#[tokio::test]
async fn test_consumer_pacing_anchor_survives_ffmpeg_restart() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=20).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = PacingMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    // Factory configured to fail the first ffmpeg after 3 writes,
    // triggering a restart mid-stream.
    let factory = PacingMockProcessFactory {
        alive: Arc::new(AtomicBool::new(true)),
        fail_after_writes: Some(3),
        spawn_count: Arc::new(AtomicU32::new(0)),
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            pacing_test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Advance 30 seconds of mock time. With pacing preserved across
    // restart, expect ~15 chunks (30000ms / 2000ms) at most.
    for _ in 0..300 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let (processed, restarts) = {
        let s = stats.lock().await;
        (s.chunks_processed, s.ffmpeg_restart_count)
    };

    assert!(
        restarts >= 1,
        "Expected at least one ffmpeg restart, got {restarts}"
    );

    // With pacing preserved across restart: ~15 chunks in 30s (loose upper
    // bound for restart backoff timing).
    assert!(
        processed <= 18,
        "Consumer delivered too many chunks ({processed}) in 30s — pacing \
         anchor was likely reset on ffmpeg restart, which would collapse \
         the cache delay in production"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}

/// Regression test for the "cache drifts when ffmpeg fails writes" bug.
///
/// When ffmpeg write fails repeatedly (e.g., stale Facebook stream keys),
/// each failed chunk is lost. Before this fix, `delivered_ms` only advanced
/// on SUCCESSFUL writes, so failures silently let the pacing budget fall
/// behind wall clock and the consumer raced through the buffer. This test
/// reproduces that scenario: a factory that spawns ffmpegs which always
/// fail after 1 write, causing constant restarts. Despite every write
/// failing, the consumer must still pace by real time.
#[tokio::test]
async fn test_consumer_paces_even_when_writes_fail() {
    tokio::time::pause();

    let chunks: std::collections::HashMap<i64, Vec<u8>> =
        (1..=100).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = PacingMockFetcher {
        chunks: Arc::new(TokioMutex::new(chunks)),
        duration_ms_per_chunk: 2000,
    };

    // Each spawned ffmpeg fails on the first write, triggering an immediate
    // restart. This simulates a stream key that's rejected by the upstream
    // server (Facebook/YouTube) every time.
    let spawn_count = Arc::new(AtomicU32::new(0));
    let factory = PacingMockProcessFactory {
        alive: Arc::new(AtomicBool::new(true)),
        fail_after_writes: Some(0),
        spawn_count: spawn_count.clone(),
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            pacing_test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
        )
        .await;
    });

    // Advance 40 seconds of mock time. Each chunk is 2000ms, so even with
    // every write failing the consumer should not advance current_chunk_id
    // by more than ~20-21 chunks (plus a few for restart-backoff-induced
    // slack).
    for _ in 0..400 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }

    let current = stats.lock().await.current_chunk_id;

    // current_chunk_id is only updated on MAX_WRITE_FAILURES (3) skip.
    // Each skip consumes 3 chunks, so we expect ~(40s * 1 chunk/2s) / 3 ≈ 6
    // skip events. But the important assertion is that the consumer has not
    // raced to the end of the 100 chunks — without the fix, it would have
    // drained the buffer almost immediately. Allow up to 25 chunks as an
    // upper bound (covers restart backoff, race conditions, slack).
    assert!(
        current <= 25,
        "Consumer raced past real-time pacing during write failures: \
         current_chunk_id={current} after 40s of mock time (should be \
         ≤ ~20 with real-time pacing). This means delivered_ms is not \
         being advanced on failed writes and the pacing budget is leaking."
    );

    // Sanity: at least some chunks should have been consumed.
    assert!(
        current >= 3,
        "Consumer made no progress: current_chunk_id={current} — \
         pacing may be stuck or test setup is broken"
    );

    // Sanity: restarts are happening (proves we're exercising the failure path).
    let final_spawn_count = spawn_count.load(Ordering::Relaxed);
    assert!(
        final_spawn_count >= 3,
        "Expected multiple spawn attempts due to write failures, got {final_spawn_count}"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
}
