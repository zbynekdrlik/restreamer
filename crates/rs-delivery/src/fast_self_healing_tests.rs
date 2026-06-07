//! Fast-delay self-healing producer tests, split out of
//! `endpoint_task_tests.rs` so both test files stay under the 1000-line CI
//! cap. These exercise the producer's adaptive read-delay / live-edge
//! lag-probe behaviour (#232) plus the buffer-fill stop/pacing paths.
//!
//! Shared mock helpers (`TimedMockFetcher`, `MockProcessFactory`,
//! `test_ep_cfg`) live in the sibling `tests` module and are reused here via
//! `super::tests::…`. This is a pure move of the tests — no assertion change.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{Mutex, mpsc, watch};

use crate::buffer_state::BufferState;
use crate::endpoint_producer::producer_task;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{PREFETCH_BUFFER_SIZE, PrefetchedChunk, endpoint_loop};

use super::tests::{MockProcessFactory, TimedMockFetcher, test_ep_cfg};

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
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    // Buffer fill needs 10 chunks (20000ms / 2000ms) which are already
    // available. Consumer pacing sleeps ~2000ms per chunk. 30 chunks require
    // ~60s of wall-clock advancement for pacing. Each iteration advances
    // 100ms, so we need at least 600 iterations; use 2000 for slack.
    for _ in 0..2000 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
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
            None,
            Arc::new(BufferState::new()),
            None,
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

#[tokio::test(start_paused = true)]
async fn test_fast_endpoint_producer_trails_live_edge() {
    // A FAST endpoint must build a buffer: even with the live edge far ahead
    // and S3 fetches incurring latency (the upload-spike scenario), the
    // adaptive controller keeps the read pointer BEHIND the edge. The
    // live-edge lag-probe jumps to `edge - delay_chunks` with delay_chunks
    // >= 1 (the controller floor), so the producer leaves a buffer instead of
    // re-pinning to the absolute edge.
    //
    // Old behaviour (delivery_delay_ms == 0 → delivery_delay_chunks == 0)
    // pinned the read pointer to the live edge, so any S3 latency spike
    // instantly starved the push. This test drives `producer_task` directly
    // and asserts the producer's highest requested chunk_id stays strictly
    // below the live edge after the lag-probe fires (a buffer was built) and
    // that it JUMPED forward (did not replay the whole backlog one-by-one).
    let live_edge: i64 = 30_000;
    // All chunks exist and are available from t=0; the producer is "behind"
    // the live edge purely because it reads at 1x + the injected latency.
    let chunks: Vec<(i64, Vec<u8>)> = (1..=live_edge).map(|i| (i, vec![0u8; 4])).collect();
    // 50ms per fetch simulates real S3 GET/HEAD latency. Each fetch advances
    // virtual time, so the producer reads at a bounded rate well below the
    // far-ahead live edge.
    let fetcher =
        TimedMockFetcher::new(chunks, live_edge).with_latency(std::time::Duration::from_millis(50));
    let max_fetched = fetcher.max_fetched_id();

    let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let buffer_state = Arc::new(BufferState::new());

    // Draining consumer: pull chunks as fast as they arrive so the producer
    // never blocks on the bounded channel.
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let producer = tokio::spawn(producer_task(
        fetcher,
        tx,
        1,    // start_chunk_id
        0,    // delivery_delay_ms (fast endpoints pass 0)
        true, // is_fast → adaptive controller engaged
        stop_rx,
        stats.clone(),
        "fast-ep".to_string(),
        buffer_state,
        None,
    ));

    // Drive virtual time. With 50ms/fetch latency, ~12s of advance lets the
    // producer read > LAG_PROBE_INTERVAL_ITERS (30) chunks, fire the lag-probe
    // once, jump forward by the ladder window (~8k chunks), and keep reading.
    // It stays far below the 30_000 live edge so the trailing assertion holds
    // deterministically (it cannot collapse onto the edge in this window).
    for _ in 0..1200 {
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }

    let observed = max_fetched.load(Ordering::Relaxed);
    assert!(
        observed > 1,
        "fast endpoint must JUMP forward toward live (skip backlog), got {observed}"
    );
    assert!(
        observed < live_edge,
        "fast endpoint must read BEHIND the live edge (built a buffer); \
         observed read position {observed} reached the edge {live_edge}"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), producer).await;
    drain.abort();
}

#[tokio::test]
async fn test_controller_grow_makes_lag_jump_trail_edge() {
    // Focused integration of the two pieces the producer wires together:
    // `FastDelayController` (read-delay) + `producer_lag::detect_lag_and_jump`
    // (live-edge jump target). After the controller GROWS on starvation, the
    // jump target must trail the live edge by the grown delay — proving the
    // fast endpoint keeps a real buffer rather than re-pinning to the edge.
    //
    // This is the assertion the full-producer test above cannot make sharply:
    // at the bare floor (~2 chunks) the inter-probe catch-up can momentarily
    // reach the edge; only AFTER a grow is the gap unambiguous. Old behaviour
    // (delivery_delay_chunks == 0) would jump to the absolute edge here.
    let now = std::time::Instant::now();
    let mut ctrl = crate::fast_delay::FastDelayController::new(now);

    let current: i64 = 100;
    // Edge chosen so BOTH the floor-delay and grown-delay probe ladders land
    // their highest existing rung on exactly the same chunk (8292), isolating
    // the delay's effect on the gap. The exponential ladder breaks at the
    // first missing probe, so the block must be contiguous up to the edge.
    //   floor (offset seed 4):  104,108,116,132,164,228,356,612,1124,2148,4196,8292 → last=8292 (12-rung cap)
    //   grown (offset seed 64): 164,228,356,612,1124,2148,4196,8292,16484(miss)      → last=8292
    let edge: i64 = 8292;
    let chunks: Vec<(i64, Vec<u8>)> = (1..=edge).map(|i| (i, vec![0u8; 1])).collect();
    let fetcher = TimedMockFetcher::new(chunks, edge); // 2000ms chunks

    // Floor delay (5s / 2s chunks = 2 chunks): jump target trails the
    // highest-found chunk (the live edge) by exactly 2.
    let floor_chunks = ctrl.delay_chunks(2000);
    assert_eq!(floor_chunks, 2, "floor 5s / 2s chunks = 2 chunks");
    let floor_target = crate::producer_lag::detect_lag_and_jump(&fetcher, current, floor_chunks)
        .await
        .expect("chunks exist far ahead → a jump target");
    assert_eq!(floor_target, edge - 2, "floor target trails the edge by 2");

    // Starvation: a 60s deficit grows the controller to 65s (deficit + margin).
    let grown = ctrl.on_starvation(60, now);
    assert_eq!(grown, Some((5, 65)), "grow to deficit(60)+margin(5)=65s");
    let grown_chunks = ctrl.delay_chunks(2000); // 65000 / 2000 = 32 chunks
    assert_eq!(grown_chunks, 32);

    let grown_target = crate::producer_lag::detect_lag_and_jump(&fetcher, current, grown_chunks)
        .await
        .expect("chunks exist far ahead → a jump target");
    assert_eq!(
        grown_target,
        edge - 32,
        "grown target trails the edge by 32"
    );

    // After growth the producer reads FURTHER behind the SAME detected edge
    // chunk (8292) → a strictly larger buffer. Both delays found 8292, so the
    // gap-behind-edge difference equals the chunk-delay difference (32-2=30):
    // detect_lag_and_jump subtracts delivery_delay_chunks from the highest
    // found chunk.
    assert!(
        grown_target < floor_target,
        "grown delay must trail the edge further: grown_target={grown_target} \
         floor_target={floor_target}"
    );
    assert!(
        grown_target < edge,
        "grown jump target must be behind the edge"
    );
    assert_eq!(
        floor_target - grown_target,
        grown_chunks - floor_chunks,
        "extra buffer = extra read-delay chunks"
    );
}

// ---------------------------------------------------------------------------
// REGRESSION GATE (Task 6): fast endpoint survives an upload gap.
//
// The core guarantee that shipped CI-green with NO test: when a fast endpoint
// starves (producer stops delivering chunks), the keepalive loop must
//   1. NEVER close the connection (no teardown on starvation),
//   2. keep pushing frames during the gap — first FREEZE (last chunk bytes),
//      then RESCUE (default FLV) once the gap exceeds FAST_KEEPALIVE_RESCUE_
//      AFTER_SECS (10s), and
//   3. resume the real chunk the moment one arrives.
//
// This locks `keepalive_until_chunk`'s freeze->rescue transition and its
// never-close contract against regression. Without this test, upload jitter
// against a fast endpoint had zero coverage.
// ---------------------------------------------------------------------------
mod fast_upload_gap_regression {
    // Reach `keepalive_until_chunk` (private fn in `endpoint_task`) and the
    // `Pushable` trait (in `endpoint_task::consumer_helpers`). This module is
    // nested as endpoint_task::test_root::fast_self_healing_tests::
    // fast_upload_gap_regression, so:
    //   super (fast_self_healing_tests) -> super (test_root) -> super (endpoint_task)
    use super::super::super::consumer_helpers::Pushable;
    use super::super::super::keepalive_until_chunk;

    use crate::endpoint_task::PrefetchedChunk;
    use crate::rescue_default::DEFAULT_RESCUE_FLV;
    use rs_rtmp_push::PushError;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};

    /// Test pusher that records every push's byte-length, models ~1x pacing
    /// with a 200ms sleep, and NEVER returns an error (so any close() would be
    /// caused by starvation logic, not by us injecting failures).
    struct RecordingPusher {
        pushes: Arc<Mutex<Vec<usize>>>,
        closed: Arc<AtomicBool>,
    }

    impl Pushable for RecordingPusher {
        async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
            self.pushes.lock().unwrap().push(data.len());
            // Model the real pusher's ~1x self-pacing so each keepalive tick
            // advances virtual time by a frame interval. Deterministic under
            // `start_paused = true` — advanced explicitly below, never waited
            // on the wall clock.
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        }

        async fn close(&mut self) {
            // Starvation must NEVER reach here. If it does, the assertion on
            // `closed` below fails and the regression is caught.
            self.closed.store(true, Ordering::SeqCst);
        }

        fn reconnect_count(&self) -> u32 {
            0
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fast_endpoint_survives_upload_gap_without_closing() {
        // Distinct freeze length (4242) so freeze pushes are identifiable and
        // never collide with the rescue FLV length.
        const FREEZE_LEN: usize = 4242;
        assert_ne!(
            FREEZE_LEN,
            DEFAULT_RESCUE_FLV.len(),
            "freeze length must differ from rescue length so the two are distinguishable"
        );

        let pushes = Arc::new(Mutex::new(Vec::<usize>::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let mut pusher = RecordingPusher {
            pushes: Arc::clone(&pushes),
            closed: Arc::clone(&closed),
        };

        // Producer gap: channel created, nothing sent yet.
        let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(10);
        let (_stop_tx, mut stop_rx) = watch::channel(false);

        // A populated last-chunk → freeze is available for gap < 10s.
        let last: Option<Arc<Vec<u8>>> = Some(Arc::new(vec![0u8; FREEZE_LEN]));
        let audit_ring: Option<Arc<crate::audit_ring::AuditRing>> = None;

        // Drive `keepalive_until_chunk` in a task so we can advance virtual
        // time around it. The pusher (moved in) records into the `pushes` Arc,
        // which the test still owns a clone of, so the recorded data stays
        // readable throughout.
        let stats: crate::endpoint_stats::Stats = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::endpoint_stats::EndpointStats::default(),
        ));
        let stats_task = stats.clone();
        let task = tokio::spawn(async move {
            keepalive_until_chunk(
                &mut pusher,
                &mut rx,
                &last,
                "fast-test",
                &audit_ring,
                &mut stop_rx,
                &stats_task,
            )
            .await
        });

        // Phase 1 — FREEZE window (gap < 10s). Advance ~3s in small steps,
        // yielding between each so the spawned keepalive task makes progress
        // (each push sleeps 200ms of virtual time). This keeps the timing
        // deterministic with no wall-clock waits.
        advance_in_steps(Duration::from_millis(200), 16).await; // ~3.2s

        {
            let recorded = pushes.lock().unwrap();
            assert!(
                !recorded.is_empty(),
                "keepalive must emit frames during the gap; recorded none after ~3s"
            );
            assert!(
                recorded.contains(&FREEZE_LEN),
                "during the freeze window keepalive must push the last chunk bytes \
                 (len {FREEZE_LEN}); recorded lengths: {recorded:?}"
            );
        }

        // Phase 2 — cross the 10s rescue threshold. Advance another ~9s so the
        // cumulative gap exceeds FAST_KEEPALIVE_RESCUE_AFTER_SECS (10s) and the
        // content switches to the default rescue FLV.
        advance_in_steps(Duration::from_millis(200), 50).await; // +~10s → ~13s total

        {
            let recorded = pushes.lock().unwrap();
            let rescue_len = DEFAULT_RESCUE_FLV.len();
            assert!(
                recorded.contains(&rescue_len),
                "after 10s gap keepalive must switch to the rescue FLV \
                 (len {rescue_len}); recorded lengths: {recorded:?}"
            );
        }

        // The connection must NEVER have been closed by starvation.
        assert!(
            !closed.load(Ordering::SeqCst),
            "keepalive must NOT close the connection during a producer gap"
        );

        // Phase 3 — a real chunk arrives. keepalive must return it (resume).
        tx.send(PrefetchedChunk {
            chunk_id: 99,
            data: vec![1, 2, 3],
            duration_ms: 2000,
        })
        .await
        .expect("send real chunk");
        // Let the rx.recv() arm win.
        advance_in_steps(Duration::from_millis(50), 4).await;

        let returned = task.await.expect("keepalive task panicked");
        let chunk = returned.expect("keepalive must return the resumed real chunk, got None");
        assert_eq!(
            chunk.chunk_id, 99,
            "keepalive must resume the real chunk (id 99) once one arrives"
        );

        // Final guarantees recap:
        // - never closed: asserted above.
        // - frames emitted during gap: asserted above (non-empty).
        // - freeze AND rescue both observed: asserted above.
        // - real chunk resumed: chunk_id == 99 asserted above.
        assert!(
            !closed.load(Ordering::SeqCst),
            "connection still must not be closed after the chunk resumed"
        );
    }

    /// Advance virtual time in `count` steps of `step`, yielding to the
    /// runtime between each so the spawned keepalive task is polled and its
    /// internal 200ms sleeps fire deterministically. Avoids the "advance past
    /// everything in one jump" pitfall where the task never gets scheduled
    /// between ticks.
    async fn advance_in_steps(step: Duration, count: u32) {
        for _ in 0..count {
            tokio::time::advance(step).await;
            tokio::task::yield_now().await;
        }
    }
}
