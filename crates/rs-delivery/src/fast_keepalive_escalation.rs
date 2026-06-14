//! Fast-endpoint keepalive + outage escalation, extracted from
//! `endpoint_task.rs` to keep that file under the 1000-line file-size gate
//! (CI `file-size` job). Included via `#[path]` as `mod
//! fast_keepalive_escalation` inside `endpoint_task.rs`, and its two public
//! items (`KeepaliveOutcome`, `keepalive_until_chunk`) are re-exported at the
//! `endpoint_task` level so the existing call site in `consumer_task` AND the
//! tests (`fast_self_healing_tests`, which reach them via
//! `super::super::super::{KeepaliveOutcome, keepalive_until_chunk}`) keep
//! compiling unchanged. This is a pure move — no behaviour change.

use std::sync::atomic::Ordering as AtomicOrdering;

use super::consumer_helpers;
use super::{PrefetchedChunk, Stats};

/// Outcome of a fast-endpoint keepalive window (`keepalive_until_chunk`).
///
/// Before #251 the keepalive only distinguished "a chunk arrived" (`Some`)
/// from "stop / channel closed" (`None`). On a SUSTAINED outage (producer
/// stalled, `producer_active==false`) the freeze-only keepalive would push a
/// frozen frame forever — or push NOTHING (dark) when no chunk had been
/// delivered yet. C1 (#251) adds escalation: once the gap exceeds
/// `RESCUE_STALL_THRESHOLD_SECS` AND the producer has signalled stalled, the
/// keepalive returns `EscalateToRescue` so the fast consumer enters the SAME
/// `run_outage_rescue` (fresh RTMP session + rescue clip) the non-fast path
/// uses. Short gaps and trickle jitter (`producer_active==true`) NEVER
/// escalate — they stay freeze-only, preserving the #249 codec-homogeneity
/// guarantee (rescue clip on a live session = green video).
pub(crate) enum KeepaliveOutcome {
    /// A real chunk arrived — resume normal/fast delivery with it.
    Chunk(PrefetchedChunk),
    /// Stop signal fired (or the channel closed) — consumer should break.
    Stop,
    /// Sustained outage detected (gap >= threshold AND producer stalled) —
    /// consumer should escalate to `run_outage_rescue` on a fresh session.
    EscalateToRescue,
}

/// Keep the existing rust session alive during a fast-endpoint producer gap.
/// Returns `KeepaliveOutcome::Chunk` when a real chunk arrives,
/// `KeepaliveOutcome::Stop` on stop/closed channel, or
/// `KeepaliveOutcome::EscalateToRescue` when the gap becomes a SUSTAINED
/// outage (gap >= `RESCUE_STALL_THRESHOLD_SECS` AND `producer_active==false`)
/// — see C1 (#251). NEVER closes the connection on starvation — a push error
/// just backs off briefly and the pusher lazy-reconnects on the next push.
///
/// FREEZE-ONLY: a keepalive tick may push ONLY the last delivered chunk
/// (same codec as the live stream). It must NEVER push the rescue clip —
/// the RTMP pusher de-duplicates AVC sequence headers per session, so a
/// codec-foreign rescue blob on a LIVE session makes YouTube decode the
/// real stream with the wrong SPS/PPS (solid green video, 2026-06-11
/// streampp KS-PP-TEST). If no chunk has been delivered yet there is
/// nothing codec-safe to push: this function just WAITS for the first
/// chunk (no pushes), identical to the pre-keepalive behaviour at session
/// start. On a sustained outage the keepalive does NOT splice rescue into
/// the live session — it returns `EscalateToRescue` so the caller drops the
/// session and reconnects FRESH for the rescue clip (the #249-safe path).
///
/// Escalation is gated on `producer_active==false`: short gaps and trickle
/// jitter (chunks arriving late but the producer still alive) keep
/// `producer_active==true` and stay freeze-only forever, no matter how long.
/// Only a genuine producer stall (the producer signalled `false` after >=3
/// 404s or >=3 fetch errors) escalates.
///
/// Sets `stats.delivery_mode = "rescue"` on entry and resets it to
/// `"normal"` on the chunk/stop exit paths so the dashboard correctly
/// reflects the keepalive gap instead of showing stale `"normal"` state.
/// On `EscalateToRescue` it leaves `delivery_mode = "rescue"` because
/// `run_outage_rescue` owns it from there.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn keepalive_until_chunk<P: consumer_helpers::Pushable>(
    pusher: &mut P,
    rx: &mut tokio::sync::mpsc::Receiver<PrefetchedChunk>,
    last_chunk_bytes: &Option<std::sync::Arc<Vec<u8>>>,
    alias: &str,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    stats: &Stats,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
) -> KeepaliveOutcome {
    // ONLY codec-homogeneous bytes (the last real chunk). `None` => no chunk
    // delivered yet => pure-wait, never push. Borrows `last_chunk_bytes` (an
    // immutable `&` param) for the whole fn; nothing mutates it here.
    let freeze: Option<&[u8]> = crate::fast_keepalive::keepalive_bytes(last_chunk_bytes);
    // `tokio::time::Instant` (not `std::time::Instant`) so the gap clock shares
    // the loop's `tokio::time::sleep` time source: identical to the real clock
    // in prod, but advances under `start_paused` so the gap is deterministically
    // testable without wall-clock waits.
    let started = tokio::time::Instant::now();
    crate::fast_delay_audit::emit_keepalive_started(
        audit_ring,
        alias,
        if freeze.is_some() { "freeze" } else { "wait" },
    );
    // Surface keepalive gap on the dashboard: set delivery_mode to "rescue"
    // so the UI doesn't falsely show "normal" during the starvation window.
    // Lock is released immediately (not held across any .await).
    {
        let mut s = stats.lock().await;
        s.delivery_mode = "rescue".to_string();
    }
    // C1 (#251): is this gap now a SUSTAINED outage that must escalate to a
    // fresh-session rescue? Gated on `producer_active==false` so trickle
    // jitter (producer alive) stays freeze-only forever (#249 protection).
    let should_escalate = |bs: &std::sync::Arc<crate::buffer_state::BufferState>| -> bool {
        started.elapsed()
            >= std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS)
            && !bs.producer_active.load(AtomicOrdering::Relaxed)
    };
    // Periodic re-check tick for the escalation gate. Far below the 8s
    // threshold so escalation fires within ~1s of the threshold being
    // crossed, and short enough that the `wait`-mode (no freeze) branch
    // still observes the outage promptly. Uses `tokio::time::sleep` so it
    // advances under `start_paused`.
    const ESCALATION_POLL: std::time::Duration = std::time::Duration::from_secs(1);
    // Shared exit bookkeeping. `resume` => record the TRUE gap for the
    // producer's adaptive controller and return the chunk; otherwise (stop) =>
    // return Stop. Both emit keepalive-ended and reset delivery_mode.
    // `escalate` => leave delivery_mode "rescue" (run_outage_rescue owns it).
    macro_rules! finish {
        (resume $maybe:expr) => {{
            match $maybe {
                Some(c) => {
                    consumer_helpers::record_starvation_gap(buffer_state, started);
                    finish!(@end_normal);
                    return KeepaliveOutcome::Chunk(c);
                }
                // Channel closed with no chunk: treat as stop (caller breaks).
                None => { finish!(@end_normal); return KeepaliveOutcome::Stop; }
            }
        }};
        (stop) => {{
            finish!(@end_normal);
            return KeepaliveOutcome::Stop;
        }};
        (escalate) => {{
            crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, started.elapsed().as_secs());
            tracing::warn!(alias = %alias, "keepalive: sustained outage (producer stalled past threshold); escalating to fresh-session rescue");
            return KeepaliveOutcome::EscalateToRescue;
        }};
        (@end_normal) => {{
            crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, started.elapsed().as_secs());
            let mut s = stats.lock().await;
            s.delivery_mode = "normal".to_string();
        }};
    }
    match freeze {
        Some(bytes) => loop {
            if should_escalate(buffer_state) {
                finish!(escalate);
            }
            tokio::select! {
                maybe = rx.recv() => finish!(resume maybe),
                res = pusher.push_flv_bytes(bytes) => {
                    if let Err(e) = res {
                        tracing::warn!(alias = %alias, "keepalive push error: {e}; will reconnect on next push");
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    // push_flv_bytes self-paces ~1x; loop to push the next tick.
                }
                _ = tokio::time::sleep(ESCALATION_POLL) => {
                    // Re-evaluate the escalation gate at the top of the loop.
                }
                _ = stop_rx.changed() => { if *stop_rx.borrow() { finish!(stop); } }
            }
        },
        // No chunk delivered yet: nothing codec-safe to push. Pure wait for the
        // first real chunk (or stop), with the same outage-escalation gate so a
        // session that starves BEFORE its first chunk still reaches rescue
        // instead of going dark forever — no push arm, so no codec-foreign bytes.
        None => loop {
            if should_escalate(buffer_state) {
                finish!(escalate);
            }
            tokio::select! {
                maybe = rx.recv() => finish!(resume maybe),
                _ = tokio::time::sleep(ESCALATION_POLL) => {
                    // Re-evaluate the escalation gate at the top of the loop.
                }
                _ = stop_rx.changed() => { if *stop_rx.borrow() { finish!(stop); } }
            }
        },
    }
}
