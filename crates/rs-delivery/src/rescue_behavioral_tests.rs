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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rs_ffmpeg::ServiceType;
use tokio::sync::{Mutex, watch};

use rs_core::audit::Action;
use rs_core::models::PusherKind;
use rs_rtmp_push::PushError;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats, initial_endpoint_stats};
use crate::endpoint_task::{OutputProcess, OutputProcessFactory, endpoint_loop};
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

// ---------------------------------------------------------------------------
// Group C — C3 producer-respawn arm (#237 / #251). The DrainingFetcher tests
// above never reach the `result = &mut producer` respawn arm of
// `endpoint_loop`: their producer never FINISHES, it just drains and the
// consumer enters rescue. The respawn arm only fires when the producer task
// itself ENDS (panic) while the consumer is still alive and no stop was
// signalled. These tests drive a real producer PANIC through the real
// `endpoint_loop` so the respawn arm + `on_producer_finished` resume math are
// covered behaviourally:
//
//   * `producer_panic_respawns_resumes_and_never_duplicates` — panic mid-
//     delivery with chunks still queued (slow consumer), guarding FIX 1
//     (duplicate-chunks) and the resume_from = max(delivered, sent) + 1
//     expression, plus the recovery + `ProducerRespawned` audit.
//   * `producer_panic_before_any_delivery_resumes_at_start` — panic on the
//     very first fetch before anything is delivered, guarding FIX 3 (first-
//     death off-by-one: resume AT start_chunk_id, not start_chunk_id + 1).
// ---------------------------------------------------------------------------

/// Encode a chunk_id as the first 8 bytes (LE) of an 8-byte payload. The
/// payload is intentionally < 13 bytes and NOT an "FLV" stream, so the
/// consumer's `FlvStreamNormalizer::normalize` passes it through VERBATIM
/// (see flv_normalizer: `data.len() < 13 || != b"FLV"` → `data.to_vec()`),
/// letting the recording process decode the exact delivered chunk_id back out.
fn encode_chunk_id(id: i64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

fn decode_chunk_id(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 {
        let mut a = [0u8; 8];
        a.copy_from_slice(bytes);
        Some(i64::from_le_bytes(a))
    } else {
        None
    }
}

/// A `ChunkFetcher` that serves `encode_chunk_id`-tagged chunks for
/// `[1, available_up_to]`, panics EXACTLY ONCE when chunk `panic_at` is first
/// fetched, then (because `endpoint_loop` re-clones the `Arc<F>` for the
/// respawn) keeps serving the SAME window so the consumer recovers.
///
/// The panic-once guard is an `Arc<AtomicBool>` so it survives the Arc-clone
/// the respawn performs. At panic time it snapshots the at-panic
/// `current_chunk_id` (last DELIVERED) and `highest_sent_chunk_id` (last
/// QUEUED) from the SAME `Stats`/`BufferState` the endpoint uses, and records
/// the first chunk_id the RESPAWNED producer asks for (`resume_seen`) — the
/// three values the resume assertions need.
struct PanicOnceFetcher {
    available_up_to: i64,
    duration_ms: i64,
    panic_at: i64,
    /// Set true (CAS) the moment the panic fires; survives the respawn clone.
    panicked: Arc<AtomicBool>,
    /// First chunk_id fetched AFTER the panic = the respawn resume point.
    resume_seen: Arc<AtomicI64>,
    /// `current_chunk_id` (last delivered) snapshotted at panic time.
    delivered_at_panic: Arc<AtomicI64>,
    /// `highest_sent_chunk_id` (last queued) snapshotted at panic time.
    sent_at_panic: Arc<AtomicI64>,
    /// Same handles the endpoint uses, so the snapshots are the live values.
    stats: Stats,
    buffer_state: Arc<BufferState>,
}

impl crate::endpoint_task::ChunkFetcher for PanicOnceFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        // Fire the panic exactly once, on the first fetch of `panic_at`.
        if chunk_id == self.panic_at
            && self
                .panicked
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            // Snapshot the at-panic delivered + queued positions from the live
            // endpoint state so the resume assertions compare against reality.
            let delivered = self.stats.lock().await.current_chunk_id;
            let sent = self
                .buffer_state
                .highest_sent_chunk_id
                .load(Ordering::Relaxed);
            self.delivered_at_panic.store(delivered, Ordering::Relaxed);
            self.sent_at_panic.store(sent, Ordering::Relaxed);
            panic!("PanicOnceFetcher forced producer panic at chunk {chunk_id}");
        }

        // Once the panic has fired, the next fetch is the RESPAWNED producer's
        // first request — record it as the resume point (record once).
        if self.panicked.load(Ordering::Acquire) {
            let _ = self.resume_seen.compare_exchange(
                i64::MIN,
                chunk_id,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }

        if chunk_id <= self.available_up_to {
            Ok(Some((encode_chunk_id(chunk_id), self.duration_ms)))
        } else {
            Ok(None)
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if chunk_id <= self.available_up_to {
            Ok(Some(self.duration_ms))
        } else {
            Ok(None)
        }
    }
}

/// A recording `OutputProcess` (ffmpeg-path seam) that decodes each write's
/// chunk_id back out (see `encode_chunk_id`) and appends it to a shared log,
/// so the test can assert the DELIVERED chunk_id sequence is strictly
/// monotonic with no repeats across the respawn boundary (the FIX 1 guard).
/// Paces ~1x with a per-write virtual sleep so the producer races AHEAD of the
/// consumer and the prefetch channel holds queued-but-undelivered chunks at
/// panic time — which is what makes the no-repeat assertion RED-able.
struct RecordingProcess {
    alive: Arc<AtomicBool>,
    delivered: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl OutputProcess for RecordingProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        if let Some(id) = decode_chunk_id(data) {
            self.delivered.lock().await.push(id);
        }
        // ~1x pacing so the producer gets ahead → channel stays non-empty.
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }

    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }

    fn last_stderr_line(&self) -> Option<String> {
        None
    }
}

struct RecordingProcessFactory {
    delivered: Arc<Mutex<Vec<i64>>>,
}

impl OutputProcessFactory for RecordingProcessFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        Ok(Box::new(RecordingProcess {
            alive: Arc::new(AtomicBool::new(true)),
            delivered: self.delivered.clone(),
        }))
    }
}

/// True iff the ring recorded at least one `ProducerRespawned` row.
fn producer_respawned(ring: &Arc<AuditRing>) -> bool {
    let (rows, _) = ring.since(0);
    rows.iter().any(|r| r.action == Action::ProducerRespawned)
}

#[tokio::test(start_paused = true)]
async fn producer_panic_respawns_resumes_and_never_duplicates() {
    // Non-fast ffmpeg endpoint: the OutputProcess seam lets us record the
    // delivered chunk_id sequence. The endpoint type does not change the
    // respawn arm — any producer panic with a live consumer reaches it.
    let cfg = ep_cfg("c3-respawn", false, PusherKind::Ffmpeg);

    let start_chunk_id: i64 = 1;
    // Seed stats exactly like production (`initial_endpoint_stats` sets
    // current_chunk_id = start_chunk_id) so the FIX-3 base case is faithful.
    let stats: Stats = Arc::new(Mutex::new(initial_endpoint_stats(
        start_chunk_id,
        "normal".to_string(),
    )));
    let buffer_state = Arc::new(BufferState::new());
    let ring = AuditRing::new(500);
    let (stop_tx, stop_rx) = watch::channel(false);

    // Panic deep enough into the window that the slow consumer is still many
    // chunks behind the producer when the panic fires (queued-but-undelivered
    // chunks in the channel — the scenario FIX 1 exists for).
    let panic_at: i64 = 25;
    let resume_seen = Arc::new(AtomicI64::new(i64::MIN));
    let delivered_at_panic = Arc::new(AtomicI64::new(i64::MIN));
    let sent_at_panic = Arc::new(AtomicI64::new(i64::MIN));
    let fetcher = PanicOnceFetcher {
        available_up_to: 60,
        duration_ms: 2000,
        panic_at,
        panicked: Arc::new(AtomicBool::new(false)),
        resume_seen: resume_seen.clone(),
        delivered_at_panic: delivered_at_panic.clone(),
        sent_at_panic: sent_at_panic.clone(),
        stats: stats.clone(),
        buffer_state: buffer_state.clone(),
    };

    let delivered = Arc::new(Mutex::new(Vec::<i64>::new()));
    let factory = RecordingProcessFactory {
        delivered: delivered.clone(),
    };

    let stats_task = stats.clone();
    let bs_task = buffer_state.clone();
    let ring_task = ring.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            cfg,
            start_chunk_id,
            0, // no warmup
            stop_rx,
            stats_task,
            None,
            bs_task,
            Some(ring_task),
        )
        .await;
    });

    // Drive virtual time: deliver pre-panic chunks, hit the panic, ride the 2s
    // respawn backoff, refill past the panic point and resume normal delivery.
    for _ in 0..2000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        // Stop once recovery is clearly past the panic point.
        if stats.lock().await.current_chunk_id >= panic_at + 5 {
            break;
        }
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

    // (a) the respawn arm ran and emitted the audit row.
    assert!(
        producer_respawned(&ring),
        "C3: a producer panic with a live consumer must emit ProducerRespawned"
    );

    let delivered_base = delivered_at_panic.load(Ordering::Relaxed);
    let sent_base = sent_at_panic.load(Ordering::Relaxed);
    let resume = resume_seen.load(Ordering::Relaxed);

    // Prove the test actually exercised the queued-chunk scenario: the producer
    // must have been AHEAD of the consumer at panic time (channel non-empty),
    // otherwise the no-repeat assertion below would be vacuous w.r.t. FIX 1.
    assert!(
        sent_base > delivered_base,
        "test setup invalid: producer must be ahead of consumer at panic \
         (sent={sent_base} delivered={delivered_base}); FIX-1 guard would be vacuous"
    );

    // (b) resume_from correctness — exactly the on_producer_finished
    // expression: resume AT max(last_delivered, last_queued) + 1. With FIX 1
    // this is `sent_base + 1` (don't re-fetch queued chunks); without FIX 1 the
    // old code resumed at `delivered_base + 1`, re-fetching the queued window.
    assert_eq!(
        resume,
        delivered_base.max(sent_base) + 1,
        "C3 FIX 1/3: respawn must resume at max(delivered={delivered_base}, \
         sent={sent_base}) + 1, got {resume}"
    );

    // FIX 1 regression: the delivered chunk_id sequence must be strictly
    // monotonic with NO repeats across the respawn boundary. Without FIX 1 the
    // respawned producer re-fetches the still-queued chunks and the consumer
    // delivers them twice → a non-monotonic / repeating sequence.
    let seq = delivered.lock().await.clone();
    assert!(
        seq.len() >= 2,
        "expected several delivered chunks, got {seq:?}"
    );
    for w in seq.windows(2) {
        assert!(
            w[1] > w[0],
            "C3 FIX 1: delivered chunk_ids must be strictly increasing with no \
             repeats across the respawn boundary; got ...{w:?}... in {seq:?}"
        );
    }

    // (c) delivery resumed past the panic point and never fell into rescue.
    let s = stats.lock().await;
    assert!(
        s.current_chunk_id >= panic_at,
        "C3: delivery must resume past the panic point (chunk {panic_at}), \
         current_chunk_id={}",
        s.current_chunk_id
    );
    assert!(
        seq.iter().any(|&id| id >= panic_at),
        "C3: a chunk at/after the panic point ({panic_at}) must be delivered \
         after respawn; delivered={seq:?}"
    );
    assert_eq!(
        s.delivery_mode, "normal",
        "C3: a respawn that refills quickly must keep delivery_mode normal \
         (never starve into rescue), got {:?}",
        s.delivery_mode
    );
}

#[tokio::test(start_paused = true)]
async fn producer_panic_before_any_delivery_resumes_at_start() {
    // FIX 3 — first-death off-by-one. Panic on the VERY FIRST fetch
    // (panic_at == start_chunk_id), before any chunk is delivered. `stats` is
    // seeded like production with current_chunk_id = start_chunk_id, so the old
    // `current_chunk_id.max(start-1)+1` math would resume at start_chunk_id + 1
    // and SKIP start_chunk_id itself. The fix resumes AT start_chunk_id.
    let cfg = ep_cfg("c3-first-death", false, PusherKind::Ffmpeg);

    let start_chunk_id: i64 = 1;
    let stats: Stats = Arc::new(Mutex::new(initial_endpoint_stats(
        start_chunk_id,
        "normal".to_string(),
    )));
    let buffer_state = Arc::new(BufferState::new());
    let ring = AuditRing::new(500);
    let (stop_tx, stop_rx) = watch::channel(false);

    let resume_seen = Arc::new(AtomicI64::new(i64::MIN));
    let fetcher = PanicOnceFetcher {
        available_up_to: 30,
        duration_ms: 2000,
        panic_at: start_chunk_id, // panic before sending/delivering anything
        panicked: Arc::new(AtomicBool::new(false)),
        resume_seen: resume_seen.clone(),
        delivered_at_panic: Arc::new(AtomicI64::new(i64::MIN)),
        sent_at_panic: Arc::new(AtomicI64::new(i64::MIN)),
        stats: stats.clone(),
        buffer_state: buffer_state.clone(),
    };

    let delivered = Arc::new(Mutex::new(Vec::<i64>::new()));
    let factory = RecordingProcessFactory {
        delivered: delivered.clone(),
    };

    let stats_task = stats.clone();
    let bs_task = buffer_state.clone();
    let ring_task = ring.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            factory,
            cfg,
            start_chunk_id,
            0,
            stop_rx,
            stats_task,
            None,
            bs_task,
            Some(ring_task),
        )
        .await;
    });

    // Drive past the panic + 2s respawn backoff + first post-respawn deliveries.
    for _ in 0..1500 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        // Stop once a couple of chunks have been delivered post-respawn.
        if delivered.lock().await.len() >= 2 {
            break;
        }
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

    assert!(
        producer_respawned(&ring),
        "FIX 3: a first-fetch panic with a live consumer must emit ProducerRespawned"
    );
    assert_eq!(
        resume_seen.load(Ordering::Relaxed),
        start_chunk_id,
        "FIX 3: with nothing delivered yet, respawn must resume AT start_chunk_id \
         ({start_chunk_id}), not start_chunk_id + 1 (which would skip it)"
    );
    // The very first delivered chunk must be start_chunk_id (never skipped).
    let seq = delivered.lock().await.clone();
    assert_eq!(
        seq.first().copied(),
        Some(start_chunk_id),
        "FIX 3: the first delivered chunk after respawn must be start_chunk_id \
         ({start_chunk_id}); delivered={seq:?}"
    );
}
