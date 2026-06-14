//! C4 (#239) — REAL runtime behavioral rescue tests.
//!
//! Replaces the R1/R2/R3 source-grep tests (`read_to_string` + `.contains`)
//! that asserted the SHAPE of the source instead of its BEHAVIOUR. Those
//! tests passed while #251 shipped a broken rescue path — exactly the failure
//! mode the operator hit (CI green, rescue dead). These tests instead RUN the
//! trigger and assert observable behaviour.
//!
//! Group B (this file) drives the REAL `endpoint_loop` (producer + consumer
//! pipeline) with a draining `ChunkFetcher` and asserts that, for BOTH a fast
//! and a non-fast endpoint, a sustained drain ACTIVATES rescue: the
//! `RescueActivated` audit row is emitted and `delivery_mode` flips to
//! `"rescue"`. Covers C1 (#251 fast escalation) and C2 (#253 error-shaped
//! drain opens the gate). The drain is exercised as BOTH a clean-404
//! (`Ok(None)`) drain and an error (`Err`) drain.
//!
//! `run_outage_rescue`/the rescue loop dial a real RTMP server
//! (127.0.0.1:1935, refused), so the rescue clip bytes are not observable
//! here — the byte-level "rescue clip actually pushed" assertion lives in the
//! `rescue_push_actually_pushes_rescue_clip_bytes` test (Group A), which drives
//! the shared `rust_rescue_push_with_pusher` loop with a recording `Pushable`.
//! What IS observable end-to-end is the ACTIVATION: the `RescueActivated`
//! audit row + `delivery_mode == "rescue"`. We assert exactly that.
//!
//! All time-based waits (the 8s `RESCUE_STALL_THRESHOLD_SECS`, producer
//! miss/error flips) are fast-forwarded with `tokio::time` virtual time — no
//! real sleeps.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use rs_core::audit::Action;
use rs_core::models::PusherKind;
use rs_rtmp_push::PushError;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::endpoint_loop;
use crate::pushable::Pushable;
use crate::rescue_default::DEFAULT_RESCUE_FLV;

use super::tests::MockProcessFactory;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Draining fetcher: serves chunks `[1, available]` then drains. The drain is
/// EITHER a clean 404 (`Ok(None)` forever, models S3-reachable-but-empty) OR
/// an `Err` forever (models a wedged S3 / connection-reset outage — the #253
/// path). `chunk_duration_ms` (the skip-ahead/lag probe) mirrors the same
/// drain so the producer cannot probe its way out of the outage.
struct DrainingFetcher {
    available_up_to: i64,
    duration_ms: i64,
    /// When true the drain surfaces as `Err`; when false as `Ok(None)`.
    err_drain: bool,
}

impl crate::endpoint_task::ChunkFetcher for DrainingFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some((vec![0u8; 64], self.duration_ms)));
        }
        if self.err_drain {
            Err(format!("draining-fetcher forced error on chunk {chunk_id}"))
        } else {
            Ok(None)
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some(self.duration_ms));
        }
        if self.err_drain {
            Err(format!("draining-fetcher forced error on probe {chunk_id}"))
        } else {
            Ok(None)
        }
    }
}

fn ep_cfg(alias: &str, is_fast: bool, pusher: PusherKind) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher,
    }
}

/// True iff the ring recorded at least one `RescueActivated` row.
fn rescue_activated(ring: &Arc<AuditRing>) -> bool {
    let (rows, _) = ring.since(0);
    rows.iter().any(|r| r.action == Action::RescueActivated)
}

/// Advance virtual time in `count` steps of `step`, yielding between each so
/// spawned tasks make progress (their internal sleeps fire deterministically).
async fn advance_in_steps(step: Duration, count: u32) {
    for _ in 0..count {
        tokio::time::advance(step).await;
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Group A — the shared rescue push loop actually pushes the rescue clip.
//
// This is the assertion the source-grep R1 could never make: that real rescue
// bytes flow on the wire. `rust_rescue_push_with_pusher` is the single loop
// every endpoint type (fast + non-fast, after #251) funnels through, so
// proving the bytes here proves them for all of them. A recording `Pushable`
// captures every payload; the production path constructs the concrete
// `RtmpPusher` in the (untested-here) thin wrapper. Standing up an in-process
// RTMP server to observe bytes end-to-end would be >300 LoC of protocol
// plumbing — the same scope-creep R1/R2/R3 punted to source greps; the
// injectable `Pushable` seam (#239) is the architecture-clean alternative.
// ---------------------------------------------------------------------------

/// A recording `Pushable`: captures every pushed payload's exact bytes and
/// never errors, so any rescue-clip push is observable. Models ~1x self-pacing
/// (200ms virtual sleep) so it advances virtual time like the real pusher.
struct RecordingPusher {
    pushes: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Pushable for RecordingPusher {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.pushes.lock().await.push(data.to_vec());
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
    async fn close(&mut self) {}
    fn reconnect_count(&self) -> u32 {
        0
    }
}

#[tokio::test(start_paused = true)]
async fn rescue_push_actually_pushes_rescue_clip_bytes() {
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    // Outage: producer stalled, so the refill-exit (120s active) never fires.
    let buffer_state = Arc::new(BufferState::new());
    buffer_state.producer_active.store(false, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());
    let stats_task = stats.clone();
    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-bytes-test",
            flv,
            bs_task,
            stats_task,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Let the loop push several rescue blobs (each ~200ms paced).
    advance_in_steps(Duration::from_millis(200), 20).await;

    {
        let recorded = pushes.lock().await;
        assert!(
            !recorded.is_empty(),
            "rescue loop must push the rescue clip during an outage; pushed nothing"
        );
        assert!(
            recorded.iter().all(|p| p.as_slice() == DEFAULT_RESCUE_FLV),
            "every rescue push must be the DEFAULT_RESCUE_FLV blob (len {}); got lengths {:?}",
            DEFAULT_RESCUE_FLV.len(),
            recorded.iter().map(|p| p.len()).collect::<Vec<_>>()
        );
    }
    {
        let s = stats.lock().await;
        assert_eq!(
            s.delivery_mode, "rescue",
            "delivery_mode must read 'rescue' while the producer is stalled, got {:?}",
            s.delivery_mode
        );
    }

    let _ = stop_tx.send(true);
    advance_in_steps(Duration::from_millis(50), 4).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test(start_paused = true)]
async fn rescue_push_resumes_normal_when_producer_recovers() {
    // Refill recovery: once the producer is active for RESCUE_REFILL_TARGET_SECS
    // continuous wall-seconds, the loop exits with `false` (not stop) — the
    // recovery path that lets normal delivery resume (C3 completes via the
    // producer respawn that flips producer_active back to true).
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    let buffer_state = Arc::new(BufferState::new());
    // Producer ACTIVE from the start (source already back): the loop should
    // count continuous-active wall-seconds and exit on refill.
    buffer_state.producer_active.store(true, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-recover-test",
            flv,
            bs_task,
            stats,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Advance well past RESCUE_REFILL_TARGET_SECS (120s) of continuous active.
    advance_in_steps(Duration::from_millis(500), 320).await; // ~160s

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("rescue loop must exit once the producer has been active long enough")
        .expect("rescue task panicked");
    assert!(
        !result,
        "rescue loop must return false (refilled, resume normal), not true (stop)"
    );
}

// ---------------------------------------------------------------------------
// Group B — end-to-end drain activates rescue through the real endpoint_loop.
// ---------------------------------------------------------------------------

/// Drive `endpoint_loop` with a draining fetcher until rescue activates (or a
/// generous virtual-time budget elapses), then stop and join. Returns the
/// audit ring + stats for assertions.
async fn run_until_rescue_or_timeout(
    cfg: EndpointConfig,
    fetcher: DrainingFetcher,
    factory: MockProcessFactory,
) -> (Arc<AuditRing>, Stats) {
    tokio::time::pause();
    let ring = AuditRing::new(500);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let buffer_state = Arc::new(BufferState::new());
    let (stop_tx, stop_rx) = watch::channel(false);

    let stats_clone = stats.clone();
    let ring_clone = ring.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            cfg,
            1,
            0, // delivery_delay_ms == 0 → no warmup, straight to the pipeline
            stop_rx,
            stats_clone,
            None,
            buffer_state,
            Some(ring_clone),
        )
        .await;
    });

    // Budget: serve a few chunks, drain, let the producer flip producer_active
    // (>=3 misses/errors), let the 8s rescue threshold elapse, let
    // run_outage_rescue fire. ~200s of 100ms steps is ample.
    for _ in 0..2000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        if rescue_activated(&ring) {
            break;
        }
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    (ring, stats)
}

#[tokio::test]
async fn nonfast_clean404_drain_activates_rescue() {
    // Non-fast ffmpeg endpoint: 3 chunks then a clean-404 drain. Producer flips
    // producer_active=false after >=3 misses; the consumer's 8s rescue arm
    // then fires run_outage_rescue. (Baseline cache-drain path — already works
    // on current dev; this locks it against regression.)
    let cfg = ep_cfg("nonfast-404", false, PusherKind::Ffmpeg);
    let fetcher = DrainingFetcher {
        available_up_to: 3,
        duration_ms: 2000,
        err_drain: false,
    };
    let (ring, stats) = run_until_rescue_or_timeout(cfg, fetcher, MockProcessFactory::new()).await;

    assert!(
        rescue_activated(&ring),
        "non-fast clean-404 drain must emit RescueActivated"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "rescue",
        "non-fast clean-404 drain must put delivery_mode in 'rescue', got {:?}",
        s.delivery_mode
    );
}

#[tokio::test]
async fn nonfast_err_drain_activates_rescue() {
    // C2 (#253): the drain surfaces as Err forever (wedged S3). Pre-fix the
    // producer's Err arm never touched producer_active, so the 8s rescue gate
    // (`!producer_active`) stayed shut and rescue NEVER fired → dark. RED
    // before C2; GREEN after the Err arm flips producer_active=false.
    let cfg = ep_cfg("nonfast-err", false, PusherKind::Ffmpeg);
    let fetcher = DrainingFetcher {
        available_up_to: 3,
        duration_ms: 2000,
        err_drain: true,
    };
    let (ring, stats) = run_until_rescue_or_timeout(cfg, fetcher, MockProcessFactory::new()).await;

    assert!(
        rescue_activated(&ring),
        "C2: non-fast ERROR-shaped drain must emit RescueActivated (producer must flip \
         producer_active=false on sustained fetch errors, not only on clean 404s)"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "rescue",
        "C2: error-drain must put delivery_mode in 'rescue', got {:?}",
        s.delivery_mode
    );
}

#[tokio::test]
async fn fast_drain_escalates_to_rescue() {
    // C1 (#251): a FAST + rust endpoint on a sustained drain. Pre-fix the fast
    // branch had ONLY the freeze-only keepalive and NO run_outage_rescue arm,
    // so it froze/went dark forever and NEVER emitted RescueActivated. RED
    // before C1; GREEN after keepalive escalates to run_outage_rescue once
    // starved >= 8s AND producer_active==false.
    let cfg = ep_cfg("fast-rust", true, PusherKind::Rust);
    let fetcher = DrainingFetcher {
        available_up_to: 3,
        duration_ms: 2000,
        err_drain: false,
    };
    // Fast+rust uses the Rust pusher (no ffmpeg), so the factory is unused on
    // the hot path; supply one anyway for the signature.
    let (ring, stats) = run_until_rescue_or_timeout(cfg, fetcher, MockProcessFactory::new()).await;

    assert!(
        rescue_activated(&ring),
        "C1: fast endpoint must escalate a sustained drain to run_outage_rescue \
         (RescueActivated), not freeze/go dark forever"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "rescue",
        "C1: fast escalation must put delivery_mode in 'rescue', got {:?}",
        s.delivery_mode
    );
}

#[tokio::test]
async fn fast_err_drain_escalates_to_rescue() {
    // C1 + C2 together on a FAST endpoint: the drain is error-shaped. The
    // producer must flip producer_active=false on the Err arm (C2) AND the
    // fast keepalive must escalate to rescue (C1). Both fixes required.
    let cfg = ep_cfg("fast-rust-err", true, PusherKind::Rust);
    let fetcher = DrainingFetcher {
        available_up_to: 3,
        duration_ms: 2000,
        err_drain: true,
    };
    let (ring, _stats) = run_until_rescue_or_timeout(cfg, fetcher, MockProcessFactory::new()).await;

    assert!(
        rescue_activated(&ring),
        "C1+C2: fast endpoint with an ERROR-shaped drain must escalate to \
         run_outage_rescue (RescueActivated)"
    );
}
