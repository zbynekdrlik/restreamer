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
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use rs_core::audit::Action;
use rs_core::models::PusherKind;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::endpoint_loop;

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
    async fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> Result<Option<(Vec<u8>, i64)>, String> {
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
