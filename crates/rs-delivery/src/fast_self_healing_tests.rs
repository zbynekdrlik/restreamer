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

#[tokio::test(start_paused = true)]
async fn trickle_starvation_grows_fast_read_delay() {
    // Simulate the consumer having measured a 30s keepalive gap (trickle
    // starvation: chunks arrive late-but-arrive, so the producer's 40-miss
    // probe cycle NEVER fires). The producer must consume that gap on its
    // next successful fetch and GROW the adaptive read-delay by it, so the
    // live-edge lag-probe jump trails the edge by ~gap+margin (35 chunks at
    // 1000ms chunks) instead of the 5s/5-chunk floor.
    //
    // Determinism — separating HEAD (lag-probe) from GET (read) ceilings:
    //   * `head_edge` (HEAD) = 20_511: the live edge the lag-probe ladder
    //     discovers. From current=31 the FLOOR-delay (5-chunk) ladder tops out
    //     at exactly this rung; the GROWN-delay (35-chunk) ladder tops out at
    //     chunk 17_951.
    //   * `available_up_to` (GET) = 17_916: GET stalls here, so once the
    //     producer jumps it CANNOT catch up and the read position freezes at
    //     the jump target (the value under test).
    //
    // Without the fix the controller stays at the floor → jump target
    //   17_951... no: floor ladder tops at 20_511 → jump to 20_511 - 5 = 20_506.
    //   GET 20_506 > 17_916 → None, but `max_fetched_id` still records 20_506,
    //   so the read pointer trails the edge by only 5 → FAILS `>= 30` (RED).
    // With the fix the 30s gap grows the controller to 35 chunks → grown ladder
    //   tops at 17_951 → jump to 17_951 - 35 = 17_916 → GET 17_916 succeeds,
    //   17_917 > avail → None, `max_fetched_id` = 17_917 → trail
    //   20_511 - 17_917 = 2_594 (>= 30) → PASS (GREEN).
    const HEAD_EDGE: i64 = 20_511;
    const GET_EDGE: i64 = 17_916; // grown jump target — pins the read position
    let chunks: Vec<(i64, Vec<u8>)> = (1..=HEAD_EDGE).map(|i| (i, vec![0u8; 1])).collect();
    // 1000ms chunks so `typical_chunk_dur_ms` stays at 1000 (EWMA seed) and
    // the delay→chunks maths is 1:1 (35s = 35 chunks, 5s = 5 chunks). 5ms
    // per fetch keeps the producer reading at a bounded rate under paused time.
    let fetcher = TimedMockFetcher::new(chunks, GET_EDGE)
        .with_head_edge(HEAD_EDGE)
        .with_chunk_duration(1000)
        .with_latency(std::time::Duration::from_millis(5));
    let max_fetched = fetcher.max_fetched_id();

    let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let buffer_state = Arc::new(BufferState::new());

    // The consumer-side keepalive measured a 30s starvation gap (trickle).
    buffer_state
        .starvation_gap_ms
        .store(30_000, Ordering::Relaxed);

    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let bs = buffer_state.clone();
    let producer = tokio::spawn(producer_task(
        fetcher,
        tx,
        1,    // start_chunk_id
        0,    // delivery_delay_ms (fast endpoints pass 0)
        true, // is_fast → adaptive controller engaged
        stop_rx,
        stats.clone(),
        "fast-ep".to_string(),
        bs,
        None,
    ));

    // Drive virtual time: ~30 reads at 5ms latency fire the lag-probe once,
    // which jumps the read pointer to `head_edge - delay_chunks`. GET then
    // stalls at GET_EDGE so the position freezes — the floor-vs-grown gap is
    // decisive and stable for the rest of the window.
    for _ in 0..400 {
        tokio::time::advance(std::time::Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
    }

    let observed = max_fetched.load(Ordering::Relaxed);
    // The producer must have consumed the gap (swapped to 0) on its first
    // successful fetch.
    assert_eq!(
        buffer_state.starvation_gap_ms.load(Ordering::Relaxed),
        0,
        "producer must consume the keepalive starvation gap (swap to 0)"
    );
    assert!(
        observed < HEAD_EDGE,
        "read pointer must trail the live edge, got {observed} vs edge {HEAD_EDGE}"
    );
    assert!(
        HEAD_EDGE - observed >= 30,
        "trickle gap (30s) must grow the read-delay so the producer trails the \
         live edge by >= 30 chunks; got trail {} (read_pos {observed}, edge {HEAD_EDGE}). \
         Floor-only behaviour trails by ~5 → this is the RED assertion.",
        HEAD_EDGE - observed
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
// REGRESSION GATE: fast-endpoint keepalive is FREEZE-ONLY (codec-homogeneous).
//
// The core guarantee: when a fast endpoint starves (producer stops delivering
// chunks), the keepalive loop must
//   1. NEVER close the connection (no teardown on starvation),
//   2. keep pushing frames during the gap — but ONLY the last delivered chunk
//      (freeze frame), NEVER the default rescue FLV, no matter how long the
//      gap lasts, and
//   3. resume the real chunk the moment one arrives.
//
// Why freeze-only: the RTMP pusher de-duplicates AVC sequence headers per
// session. Pushing the rescue clip (different SPS/PPS) onto a LIVE session
// makes YouTube decode the real stream with the wrong codec config -> solid
// green video for the entire session (2026-06-11 streampp incident,
// KS-PP-TEST). The keepalive must therefore push ONLY codec-homogeneous bytes
// (the last real chunk). If no chunk has been delivered yet there is nothing
// codec-safe to push: keepalive must push NOTHING and just wait (covered by
// `keepalive_without_first_chunk_pushes_nothing` below).
//
// This locks `keepalive_until_chunk`'s freeze-only contract and its
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
        // Shared buffer state so keepalive can record the measured starvation
        // gap for the producer's adaptive delay controller (trickle-grow fix).
        let buffer_state = Arc::new(crate::buffer_state::BufferState::new());
        let buffer_state_task = buffer_state.clone();
        let task = tokio::spawn(async move {
            keepalive_until_chunk(
                &mut pusher,
                &mut rx,
                &last,
                "fast-test",
                &audit_ring,
                &mut stop_rx,
                &stats_task,
                &buffer_state_task,
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
                "during the gap keepalive must push the last chunk bytes \
                 (len {FREEZE_LEN}); recorded lengths: {recorded:?}"
            );
            // FREEZE-ONLY invariant: every push so far is the freeze chunk.
            assert!(
                recorded.iter().all(|&l| l == FREEZE_LEN),
                "keepalive must push ONLY the freeze chunk (len {FREEZE_LEN}); \
                 found foreign byte-lengths (codec-corruption regression: green \
                 video): {recorded:?}"
            );
        }

        // Phase 2 — keep the gap open WELL past the old 10s rescue threshold.
        // The keepalive must STILL push only the freeze chunk: a long gap is a
        // frozen picture, never the codec-foreign rescue clip.
        advance_in_steps(Duration::from_millis(200), 50).await; // +~10s → ~13s total

        {
            let recorded = pushes.lock().unwrap();
            let rescue_len = DEFAULT_RESCUE_FLV.len();
            // RED on current code: after 10s the freeze→rescue switch pushes
            // DEFAULT_RESCUE_FLV (rescue_len) onto the live session. Freeze-only
            // forbids any codec-foreign bytes for the WHOLE gap.
            assert!(
                !recorded.contains(&rescue_len),
                "keepalive must NEVER push the rescue FLV (len {rescue_len}) on a \
                 live session — it corrupts the codec config (green video). \
                 recorded lengths: {recorded:?}"
            );
            assert!(
                recorded.iter().all(|&l| l == FREEZE_LEN),
                "keepalive must push ONLY the freeze chunk (len {FREEZE_LEN}) for \
                 the entire gap, no matter how long; recorded lengths: {recorded:?}"
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

        // Trickle-grow fix: on chunk resume keepalive must record the TRUE
        // starvation gap (~13s here) into BufferState so the producer's
        // adaptive read-delay controller grows by it. The gap is in ms and
        // the window above advanced well past 10s before the chunk arrived.
        assert!(
            buffer_state.starvation_gap_ms.load(Ordering::SeqCst) >= 10_000,
            "keepalive must record the measured starvation gap (>= 10s) for the \
             producer's delay controller; got {}ms",
            buffer_state.starvation_gap_ms.load(Ordering::SeqCst)
        );

        // Final guarantees recap:
        // - never closed: asserted above.
        // - frames emitted during gap: asserted above (non-empty).
        // - ONLY freeze observed, never rescue: asserted above.
        // - real chunk resumed: chunk_id == 99 asserted above.
        assert!(
            !closed.load(Ordering::SeqCst),
            "connection still must not be closed after the chunk resumed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keepalive_without_first_chunk_pushes_nothing() {
        // No real chunk has been delivered yet on this session
        // (`last_chunk_bytes == None`). There is NOTHING codec-safe to push —
        // pushing the rescue clip here is exactly the bug that locks YouTube
        // onto the wrong SPS/PPS (green video). Keepalive must push NOTHING and
        // simply wait for the first real chunk, then return it.
        let pushes = Arc::new(Mutex::new(Vec::<usize>::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let mut pusher = RecordingPusher {
            pushes: Arc::clone(&pushes),
            closed: Arc::clone(&closed),
        };

        let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(10);
        let (_stop_tx, mut stop_rx) = watch::channel(false);

        // The crux: no chunk delivered yet.
        let none: Option<Arc<Vec<u8>>> = None;
        let audit_ring: Option<Arc<crate::audit_ring::AuditRing>> = None;

        let stats: crate::endpoint_stats::Stats = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::endpoint_stats::EndpointStats::default(),
        ));
        let stats_task = stats.clone();
        let buffer_state = Arc::new(crate::buffer_state::BufferState::new());
        let buffer_state_task = buffer_state.clone();
        let task = tokio::spawn(async move {
            keepalive_until_chunk(
                &mut pusher,
                &mut rx,
                &none,
                "fast-test-nofirst",
                &audit_ring,
                &mut stop_rx,
                &stats_task,
                &buffer_state_task,
            )
            .await
        });

        // Hold the gap open well past the old 10s rescue threshold. With no
        // first chunk, NOTHING may be pushed during this entire window.
        advance_in_steps(Duration::from_millis(200), 80).await; // ~16s

        {
            let recorded = pushes.lock().unwrap();
            // RED on pre-fix code: with last_chunk_bytes == None the old
            // mode logic selected rescue from gap 0 and pushed
            // DEFAULT_RESCUE_FLV repeatedly. Freeze-only forbids any push here.
            assert!(
                recorded.is_empty(),
                "keepalive must push NOTHING before the first real chunk \
                 (no codec-safe bytes exist yet); recorded lengths: {recorded:?}"
            );
        }

        // First real chunk arrives — keepalive must return it.
        tx.send(PrefetchedChunk {
            chunk_id: 7,
            data: vec![9, 9, 9],
            duration_ms: 2000,
        })
        .await
        .expect("send first chunk");
        advance_in_steps(Duration::from_millis(50), 4).await;

        let returned = task.await.expect("keepalive task panicked");
        let chunk = returned.expect("keepalive must return the first real chunk, got None");
        assert_eq!(
            chunk.chunk_id, 7,
            "keepalive must return the first real chunk (id 7) once one arrives"
        );

        // Still NOTHING pushed even after the chunk arrived — keepalive was a
        // pure wait, no codec-foreign bytes on the wire.
        assert!(
            pushes.lock().unwrap().is_empty(),
            "keepalive must never push when no chunk was ever delivered"
        );

        // Never closed the connection while waiting.
        assert!(
            !closed.load(Ordering::SeqCst),
            "keepalive must NOT close the connection while waiting for the first chunk"
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
