//! Live-edge lag detection for the producer hot loop.
//!
//! Periodically probes far ahead via HEAD-only S3 calls. If chunks exist
//! beyond the current read position, jumps the read pointer to
//! `(latest_known - delivery_delay_chunks)`. Without this, any one-time
//! slowdown (slow start, transient stall, OBS pause) accumulates lag
//! forever — producer reads at 1x and never catches up.
//!
//! Two endpoint classes, two jump targets (operator-locked behavior):
//! - **Fast endpoints** (`is_fast`, e.g. the control/monitor stream): jump
//!   target is `latest_known - delivery_delay_chunks`, where the delay is the
//!   adaptive controller's RATCHETED target rather than a configured constant
//!   (#232/#294). After an outage they MUST skip the gap to stay near-live;
//!   falling behind unbounded is what killed them repeatedly. Because the
//!   ratcheted delay can grow large, the ladder starts at a small constant
//!   first rung so it stays armed at any target (#294 — see
//!   `LAG_PROBE_FAST_FIRST_RUNG`), and a binary refine pins the edge exactly.
//!   Pre-#232 a fast endpoint had `delivery_delay_chunks == 0` and jumped to
//!   the live edge itself; it now always trails by its ratcheted buffer.
//! - **Delayed endpoints** (`delivery_delay_ms > 0`, main YT/FB): jump target
//!   is `latest_known - delivery_delay_chunks`, holding a constant gap behind
//!   live equal to the configured delay. Unchanged behavior — deliberately so:
//!   correcting their drift would mean skipping content on the main outputs.
//!
//! RTMP push itself is always strictly 1× — this only moves the READ pointer
//! (which chunk to fetch next); it never speeds up delivery.
//!
//! Observed in prod (event 9289 on 2026-05-07): VPS read pointer 70+
//! min behind live edge, OBS-stop didn't drain cache because fresh
//! chunks kept arriving faster than reader consumed.

use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_task::ChunkFetcher;

/// Trigger lag-probe every N successful fetches.
/// At default chunk size ~2s, 30 fetches = ~60s detection latency.
/// At 5s chunks the latency grows to 150s — still well within the
/// delivery_delay budget. Tunable; tests assert exact-match behavior.
pub(crate) const LAG_PROBE_INTERVAL_ITERS: u32 = 30;

/// Max exponential-probe ladder steps: 12 = up to 4096× delivery_delay
/// search window. Each rung is HEAD-only (no body download).
const LAG_PROBE_LADDER_MAX: u32 = 12;

/// First ladder rung (chunks ahead) for FAST endpoints (#294).
///
/// Delayed endpoints start the ladder at `2 * delivery_delay_chunks`, which
/// makes any drift smaller than `2 *` the delay invisible — the first probe
/// lands past the live edge and the ladder breaks. That blind band used to be
/// harmless for fast endpoints because the adaptive controller's healthy-shrink
/// continuously walked `delivery_delay_chunks` back toward the floor, shrinking
/// the first rung until it re-entered the existing chunks. #294 removed the
/// shrink (it re-starved the endpoint and caused the repeated stuttering), so
/// the ladder lost its only re-arm mechanism and a ratcheted fast endpoint
/// could drift up to 2× its held target with no correction.
///
/// Fast endpoints therefore start at a small constant rung instead, so the
/// ladder is ALWAYS armed regardless of how large the ratcheted target grew.
const LAG_PROBE_FAST_FIRST_RUNG: i64 = 2;

/// Ladder steps for FAST endpoints (#294).
///
/// Reach is `first_rung * 2^(steps - 1)`. The delayed-endpoint ladder starts at
/// `2 * delivery_delay_chunks`, so its reach SCALES with the configured delay;
/// pinning the fast first rung to a small constant would otherwise shrink a
/// ratcheted fast endpoint's reach dramatically (at a 32-chunk target, from
/// 131072 chunks down to 4096). More steps restore it: `2 * 2^15` = 65536
/// chunks ≈ 36 h at 2 s chunks — far beyond any real outage (the worst observed
/// in prod was ~70 min ≈ 2100 chunks) while staying HEAD-only and running at
/// most once per `LAG_PROBE_INTERVAL_ITERS` fetches.
const LAG_PROBE_FAST_LADDER_MAX: u32 = 16;

/// Safety cap on the binary-refine loop (see `pin_live_edge`). The bracket it
/// narrows is at most `LAG_PROBE_LADDER_MAX` doublings wide, so it converges in
/// well under this many probes; the cap only bounds a pathological fetcher.
const LAG_REFINE_MAX_PROBES: u32 = 16;

/// Binary-refine the live edge inside the bracket `(lo, hi)`, where `lo` is a
/// chunk PROVEN to exist and `hi` a chunk PROVEN to be missing. Returns the
/// highest chunk proven to exist.
///
/// The exponential ladder alone only locates the edge to within a factor of 2
/// (it reports the last power-of-two rung that hit), which for a ratcheted fast
/// endpoint means a drift correction of as little as one chunk per probe cycle
/// — far too slow to matter. Pinning the edge exactly makes the correction land
/// the endpoint precisely at its ratcheted target in a single cycle.
///
/// A probe error stops the refine and keeps the last proven-existing value:
/// under-estimating the edge is always the SAFE direction (it leaves the
/// endpoint further behind live, never closer).
async fn pin_live_edge<F: ChunkFetcher>(fetcher: &F, lo: i64, hi: i64) -> i64 {
    let (mut lo, mut hi) = (lo, hi);
    let mut probes = 0;
    while hi - lo > 1 && probes < LAG_REFINE_MAX_PROBES {
        probes += 1;
        let mid = lo + (hi - lo) / 2;
        match fetcher.chunk_duration_ms(mid).await {
            Ok(Some(_)) => lo = mid,
            Ok(None) => hi = mid,
            Err(_) => break,
        }
    }
    lo
}

/// Exponential-probe ladder for the highest known-existing chunk_id ahead
/// of `current`. Returns `Some(new_id)` to jump to, or `None` if no skip
/// needed. Cost: O(log lag) probes when lag is large, 1 probe when not.
///
/// `delivery_delay_chunks == 0` means "jump target = live edge" (fast
/// endpoints). `> 0` means "jump target = live edge - delay" (delayed
/// endpoints). Negative is nonsensical → `None`.
///
/// `is_fast` selects the fast-endpoint behaviour (#294): a small constant first
/// rung so the ladder is always armed, plus a binary refine that pins the live
/// edge exactly. Delayed endpoints are deliberately left byte-for-byte
/// unchanged — correcting their drift would mean SKIPPING content on the main
/// YouTube/Facebook outputs (a visible jump-cut) to shave latency the operator
/// has already chosen to buy. Skipping to stay near-live is the fast endpoint's
/// whole documented purpose; it is not the delayed endpoints'.
///
/// ## Buffer invariant (#294 — operator-locked)
///
/// The returned target is `max_id - delivery_delay_chunks`, where `max_id` is a
/// chunk PROVEN to exist. The true live edge `E` is therefore `>= max_id`, so
/// after the jump the endpoint sits `E - (max_id - delay) >= delay` chunks
/// behind live. **A jump can never leave the endpoint closer to the live edge
/// than the ratcheted target** — it only removes accidental drift ABOVE it. And
/// when no forward progress is available the answer is `None`, never a
/// one-chunk nudge: nudging would shave the buffer every probe cycle and walk
/// the endpoint back to the fragile live edge, which is precisely the shrink
/// behaviour #294 removed.
pub(crate) async fn detect_lag_and_jump<F: ChunkFetcher>(
    fetcher: &F,
    current: i64,
    delivery_delay_chunks: i64,
    is_fast: bool,
) -> Option<i64> {
    if delivery_delay_chunks < 0 {
        return None;
    }
    // The ladder must always step strictly forward. For a delayed endpoint with
    // a zero delay the natural start `delivery_delay_chunks * 2` is 0, which
    // would probe `current` forever and never advance — clamp to +2.
    let mut probe_offset: i64 = if is_fast {
        LAG_PROBE_FAST_FIRST_RUNG
    } else {
        delivery_delay_chunks.saturating_mul(2).max(2)
    };
    let ladder_max = if is_fast {
        LAG_PROBE_FAST_LADDER_MAX
    } else {
        LAG_PROBE_LADDER_MAX
    };
    let mut last_existing: Option<i64> = None;
    let mut first_missing: Option<i64> = None;
    for _ in 0..ladder_max {
        let probe_id = current + probe_offset;
        match fetcher.chunk_duration_ms(probe_id).await {
            Ok(Some(_)) => {
                last_existing = Some(probe_id);
                probe_offset = probe_offset.saturating_mul(2);
            }
            Ok(None) => {
                first_missing = Some(probe_id);
                break;
            }
            Err(_) => break,
        }
    }
    let mut max_id = last_existing?;
    // Fast endpoints: pin the edge exactly so the correction lands ON the
    // ratcheted target rather than crawling toward it one chunk per cycle.
    if is_fast && let Some(miss) = first_missing {
        max_id = pin_live_edge(fetcher, max_id, miss).await;
    }
    let target = max_id - delivery_delay_chunks;
    // Only a genuine forward catch-up counts. For delayed endpoints this is
    // unconditionally true whenever the ladder hit at all (their first rung is
    // `2 * delay`, so `max_id >= current + 2*delay` ⇒ `target >= current +
    // delay > current`), which is why their behaviour is unchanged.
    (target > current).then_some(target)
}

/// First ladder rung (chunks ahead) for the #296 refill-deficit probe.
const REFILL_PROBE_FIRST_RUNG: i64 = 1;

/// Measure how far a BUFFERED endpoint's cushion sits BELOW its configured
/// target, VPS-side, via HEAD-only S3 probes (#296). Returns the deficit in
/// CHUNKS when the cushion is below `target_chunks`, or `None` when it is at /
/// above target (the steady state), when the target is non-positive (fast
/// endpoints), or when the live edge cannot be located (probe error / no chunk
/// ahead of `current` — the safe answer is "don't throttle").
///
/// `chunk_delay_secs` is computed HOST-side (`rs-api::delivery_status`) and the
/// VPS binary cannot see it, so the deficit MUST be derived here from what the
/// VPS does know: the read pointer `current` and the live edge it probes for.
///
/// This is a MEASUREMENT ONLY — unlike `detect_lag_and_jump` it never moves the
/// read pointer. The delayed-endpoint jump geometry (`2 * delay` first rung,
/// intentional blind band, no content-skipping on the main outputs) is left
/// byte-for-byte unchanged; this probe is purely additive.
///
/// Cost: 1 probe in the steady state (the chunk at `current + target` exists →
/// cushion >= target → `None`), `O(log cushion)` probes only when genuinely
/// below target.
pub(crate) async fn detect_refill_deficit<F: ChunkFetcher>(
    fetcher: &F,
    current: i64,
    target_chunks: i64,
) -> Option<u64> {
    if target_chunks <= 0 {
        return None;
    }
    // Steady-state fast path: if the chunk at the target depth already exists,
    // the cushion is at or above target — nothing to refill (1 HEAD probe).
    // A probe error is the SAFE answer "don't throttle".
    match fetcher.chunk_duration_ms(current + target_chunks).await {
        Ok(Some(_)) => return None,
        Err(_) => return None,
        Ok(None) => {} // below target — locate the live edge to size the deficit
    }
    // Ascending small-rung ladder to bracket the live edge, strictly BELOW the
    // target depth (we already proved `current + target` is missing). Each rung
    // is HEAD-only. `first_missing` seeds to the proven-missing target chunk so
    // the refine has an upper bound even if the ladder breaks on its first rung.
    let mut probe_offset: i64 = REFILL_PROBE_FIRST_RUNG;
    let mut last_existing: Option<i64> = None;
    let mut first_missing: i64 = current + target_chunks;
    while probe_offset < target_chunks {
        let probe_id = current + probe_offset;
        match fetcher.chunk_duration_ms(probe_id).await {
            Ok(Some(_)) => {
                last_existing = Some(probe_id);
                probe_offset = probe_offset.saturating_mul(2);
            }
            Ok(None) => {
                first_missing = probe_id;
                break;
            }
            Err(_) => break,
        }
    }
    // No existing chunk found ahead of `current` (producer sitting at the live
    // edge with no headroom to measure) → cannot size the deficit → don't
    // throttle. Rebuild starts as soon as the source is >= 1 chunk ahead.
    let max_id = pin_live_edge(fetcher, last_existing?, first_missing).await;
    let deficit = crate::refill::refill_deficit_chunks(max_id, current, target_chunks);
    (deficit > 0).then_some(deficit)
}

/// Non-fast producer wrapper (#296): every `LAG_PROBE_INTERVAL_ITERS` fetches,
/// re-measure the below-target cushion deficit and publish it on the shared
/// `BufferState` so the consumer can throttle (or stop throttling). Emits the
/// refill-started / refill-ended audit transition. `refilling` is the
/// producer-local edge tracker for that transition. No-op for fast endpoints
/// (they use the #294 read-delay ratchet, not this) — the caller gates on
/// `!is_fast`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn maybe_update_refill_deficit<F: ChunkFetcher>(
    fetcher: &F,
    current: i64,
    target_chunks: i64,
    typical_chunk_dur_ms: u64,
    iters: &mut u32,
    buffer_state: &Arc<BufferState>,
    refilling: &mut bool,
    alias: &str,
    audit_ring: &Option<Arc<AuditRing>>,
) {
    *iters += 1;
    if *iters < LAG_PROBE_INTERVAL_ITERS {
        return;
    }
    *iters = 0;

    let deficit_secs = match detect_refill_deficit(fetcher, current, target_chunks).await {
        // A genuine sub-target deficit: convert chunks → seconds, flooring to 1
        // so a small-but-real deficit still signals the consumer to throttle.
        Some(chunks) if chunks > 0 => (chunks.saturating_mul(typical_chunk_dur_ms) / 1000).max(1),
        _ => 0,
    };
    buffer_state
        .refill_deficit_secs
        .store(deficit_secs, AtomicOrdering::Relaxed);

    if deficit_secs > 0 && !*refilling {
        *refilling = true;
        crate::refill_audit::emit_refill_started(audit_ring, alias, deficit_secs);
        tracing::info!(
            alias = %alias,
            deficit_secs,
            "Refill: cushion below target, delivering slower to rebuild"
        );
    } else if deficit_secs == 0 && *refilling {
        *refilling = false;
        crate::refill_audit::emit_refill_ended(audit_ring, alias);
        tracing::info!(alias = %alias, "Refill: cushion back at target, resuming 1x");
    }
}

/// Clear the refill deficit when the producer goes stalled (outage) so the
/// consumer stops throttling — there is no source to rebuild from, and the
/// rescue machinery owns the drain from here. Closes the refill-audit pair if
/// one was open. No-op if not currently refilling and already clear.
pub(crate) fn clear_refill_deficit(
    buffer_state: &Arc<BufferState>,
    refilling: &mut bool,
    alias: &str,
    audit_ring: &Option<Arc<AuditRing>>,
) {
    buffer_state
        .refill_deficit_secs
        .store(0, AtomicOrdering::Relaxed);
    if *refilling {
        *refilling = false;
        crate::refill_audit::emit_refill_ended(audit_ring, alias);
    }
}

/// Convenience wrapper called once per successful fetch in producer_task.
/// Bumps the counter; every `LAG_PROBE_INTERVAL_ITERS` invocations it
/// runs the ladder probe and (if lag detected) updates `chunk_id`.
///
/// Fast endpoints probe too (they must jump to the live edge after an outage),
/// so there is no short-circuit here; the jump target is fully determined by
/// `delivery_delay_chunks` and `is_fast`. The former `delivery_delay_ms`
/// parameter was already unused and is dropped (#294).
pub(crate) async fn maybe_jump<F: ChunkFetcher>(
    fetcher: &F,
    chunk_id: &mut i64,
    delivery_delay_chunks: i64,
    iters: &mut u32,
    alias: &str,
    is_fast: bool,
) {
    *iters += 1;
    if *iters < LAG_PROBE_INTERVAL_ITERS {
        return;
    }
    *iters = 0;
    if let Some(new_id) =
        detect_lag_and_jump(fetcher, *chunk_id, delivery_delay_chunks, is_fast).await
    {
        tracing::warn!(
            alias = %alias,
            from = *chunk_id,
            to = new_id,
            jump = new_id - *chunk_id,
            "Producer: live-edge lag detected, skipping ahead"
        );
        *chunk_id = new_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint_task::ChunkFetcher;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Minimal fake fetcher: chunks 1..=highest_existing exist with 2s duration.
    struct MockFetcher {
        highest_existing: i64,
        probe_count: AtomicU32,
    }

    impl ChunkFetcher for MockFetcher {
        async fn fetch_chunk_with_meta(
            &self,
            _chunk_id: i64,
        ) -> Result<Option<(Vec<u8>, i64)>, String> {
            unreachable!("lag-detect uses HEAD only")
        }

        async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
            self.probe_count.fetch_add(1, Ordering::SeqCst);
            if chunk_id <= self.highest_existing {
                Ok(Some(2000))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn detect_returns_none_when_no_lag() {
        // current=100, delay=60, no chunks exist beyond current+120.
        let f = MockFetcher {
            highest_existing: 100,
            probe_count: AtomicU32::new(0),
        };
        let r = detect_lag_and_jump(&f, 100, 60, false).await;
        assert_eq!(r, None);
        // First probe at current+120=220 returns None → break immediately.
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detect_returns_max_minus_delay_when_chunks_far_ahead() {
        // current=100, delay=60, chunks exist up to 5000.
        // Ladder: 220, 340, 580, 1060, 2020, 3940, 7780, ...
        // 7780 > 5000 → break. Last existing = 3940. Result = 3940-60 = 3880.
        let f = MockFetcher {
            highest_existing: 5000,
            probe_count: AtomicU32::new(0),
        };
        let r = detect_lag_and_jump(&f, 100, 60, false).await;
        assert_eq!(r, Some(3880));
    }

    #[tokio::test]
    async fn detect_returns_none_instead_of_nudging_forward_one_chunk() {
        // REPLACES `detect_floors_jump_target_to_current_plus_one`, which
        // asserted the old `.max(current + 1)` clamp — behaviour deliberately
        // removed by #294.
        //
        // The old clamp forced a jump of at least one chunk whenever ANY probe
        // hit, even when the computed target was at or behind the read pointer.
        // That was unreachable for delayed endpoints (their first rung is
        // `2 * delay`, so a hit always implies a genuinely forward target), and
        // the old test could only assert the clamp's arithmetic inline rather
        // than drive the function at all.
        //
        // Re-arming the fast-endpoint ladder makes the case REACHABLE: a
        // healthy fast endpoint now probes chunks that do exist just ahead. A
        // one-chunk nudge per probe cycle would shave the ratcheted buffer down
        // toward the fragile live edge — the exact shrink behaviour #294
        // removed. So the guard is now "jump only when it is genuinely
        // forward", and no-progress returns None.
        //
        // current=100, delay=15, live edge at 110 (10 chunks behind = INSIDE
        // the ratcheted target). Ladder hits at +2/+4/+8, misses at +16; the
        // refine pins the edge at 110; target = 110-15 = 95, which is behind
        // the read pointer ⇒ no jump at all.
        let f = MockFetcher {
            highest_existing: 110,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(
            detect_lag_and_jump(&f, 100, 15, true).await,
            None,
            "a target at/behind the read pointer must yield NO jump, never a \
             one-chunk nudge that nibbles the ratcheted buffer"
        );
    }

    #[tokio::test]
    async fn detect_respects_ladder_cap() {
        // With infinite chunks ahead, ladder must stop at LAG_PROBE_LADDER_MAX
        // probes regardless. current=0, delay=1, all chunks exist.
        let f = MockFetcher {
            highest_existing: i64::MAX,
            probe_count: AtomicU32::new(0),
        };
        let _ = detect_lag_and_jump(&f, 0, 1, false).await;
        assert_eq!(
            f.probe_count.load(Ordering::SeqCst),
            LAG_PROBE_LADDER_MAX,
            "ladder must cap at {LAG_PROBE_LADDER_MAX} probes"
        );
    }

    #[tokio::test]
    async fn detect_returns_none_when_delay_chunks_is_negative() {
        // Negative delivery_delay_chunks is nonsensical → no jump.
        let f = MockFetcher {
            highest_existing: 1_000_000,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_lag_and_jump(&f, 100, -1, false).await, None);
        // Early-return guard: no probes issued.
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn detect_fast_endpoint_jumps_to_live_edge() {
        // Fast endpoint (delivery_delay_chunks=0) behind a live edge.
        // current=100, highest=5000. probe_offset starts at
        // LAG_PROBE_FAST_FIRST_RUNG (2) and DOUBLES each rung, so the probe ids
        // are 102, 104, 108, 116, 132, 164, 228, 356, 612, 1124, 2148, 4196,
        // 8292(miss). The ladder brackets the edge in (4196, 8292], then the
        // #294 binary refine pins it EXACTLY at 5000.
        // delay_chunks=0 → target = 5000, which is > current → jump.
        //
        // Before #294 this asserted Some(4196) — the last power-of-two rung the
        // ladder happened to hit under a 12-rung cap. That was an artifact of
        // the probe geometry, not the live edge; the endpoint was left ~800
        // chunks short of live with no further correction until the next cycle.
        // Pinning the true edge is strictly more correct.
        let f = MockFetcher {
            highest_existing: 5000,
            probe_count: AtomicU32::new(0),
        };
        let r = detect_lag_and_jump(&f, 100, 0, true).await;
        assert_eq!(
            r,
            Some(5000),
            "fast endpoint must jump forward to the ACTUAL live edge"
        );
    }

    #[tokio::test]
    async fn detect_fast_endpoint_at_live_edge_does_not_jump() {
        // Fast endpoint already at the live edge: no chunks ahead.
        // current=100, highest=100. First probe at 102 (>100) → None → break,
        // last_existing=None → returns None (no jump).
        let f = MockFetcher {
            highest_existing: 100,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_lag_and_jump(&f, 100, 0, true).await, None);
        // Only the first probe (at 102) was issued before the ladder broke.
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fast_endpoint_corrects_blind_band_drift_without_lowering_the_buffer() {
        // #294 review finding 3 — RED repro of the LADDER BLIND BAND.
        //
        // The ladder's first rung sits at `2 * delivery_delay_chunks` ahead of
        // the read pointer. So a drift of between 1x and 2x the delay is
        // invisible: the first probe lands PAST the live edge, misses, and the
        // ladder breaks without ever detecting that we are behind.
        //
        // Before #294 this was masked: the healthy-shrink kept walking
        // `delivery_delay_chunks` back down toward the floor, which shrank the
        // first rung until it re-entered the existing chunks and re-armed the
        // ladder. #294 removed the shrink (it was the cause of the repeated
        // stuttering), so for a ratcheted fast endpoint the ladder now NEVER
        // re-arms and the delivered latency can sit anywhere in [1x, 2x) of the
        // held target indefinitely.
        //
        // A fast endpoint ratcheted to 15 chunks (~30s at 2s chunks) that has
        // drifted 16..29 chunks behind the live edge must be pulled back to its
        // target — and, per the operator's hard constraint ("nemozes skakat s
        // buffrom hore dole"), the correction must NEVER leave it closer to the
        // live edge than the ratcheted target.
        let delay = 15_i64;
        for lag in [16_i64, 20, 22, 29] {
            let live_edge = 100 + lag;
            let f = MockFetcher {
                highest_existing: live_edge,
                probe_count: AtomicU32::new(0),
            };
            let jumped = detect_lag_and_jump(&f, 100, delay, true)
                .await
                .unwrap_or_else(|| panic!("drift of {lag} chunks (blind band) was not corrected"));
            assert!(
                jumped > 100,
                "correction must move the read pointer FORWARD (lag {lag}, got {jumped})"
            );
            assert!(
                live_edge - jumped >= delay,
                "must never land closer to the live edge than the ratcheted target \
                 (lag {lag}: jumped to {jumped}, live edge {live_edge}, target {delay})"
            );
        }
    }

    #[tokio::test]
    async fn fast_endpoint_at_its_target_does_not_nibble_the_buffer() {
        // Safety guard for the blind-band fix: a HEALTHY fast endpoint sitting
        // exactly at its ratcheted target must not jump at all. Re-arming the
        // ladder means it now probes chunks that DO exist just ahead, so a
        // naive "always step forward at least one chunk" clamp would shave a
        // chunk off the buffer on every probe cycle and walk the endpoint back
        // to the fragile live edge — exactly the shrink behaviour #294 removed.
        let delay = 15_i64;
        // `lag` = how far the read pointer trails the live edge. At or inside
        // the ratcheted target (lag <= delay) there is nothing to correct.
        for lag in [0_i64, 1, 8, 14, 15] {
            let f = MockFetcher {
                highest_existing: 100 + lag,
                probe_count: AtomicU32::new(0),
            };
            assert_eq!(
                detect_lag_and_jump(&f, 100, delay, true).await,
                None,
                "a fast endpoint {lag} chunks behind live with a {delay}-chunk target \
                 must NOT jump — the buffer is never nibbled"
            );
        }
    }

    #[tokio::test]
    async fn refill_deficit_none_when_cushion_at_or_above_target() {
        // #296: cushion == target (live edge at current + target). The
        // fast-path probe at current+target EXISTS → no deficit, 1 HEAD probe.
        let f = MockFetcher {
            highest_existing: 100 + 150,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_refill_deficit(&f, 100, 150).await, None);
        assert_eq!(
            f.probe_count.load(Ordering::SeqCst),
            1,
            "healthy endpoint costs exactly one HEAD probe"
        );
        // Excess lag (cushion >> target) is maybe_jump's job, not refill's.
        let f2 = MockFetcher {
            highest_existing: 100 + 900,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_refill_deficit(&f2, 100, 150).await, None);
    }

    #[tokio::test]
    async fn refill_deficit_positive_when_below_target() {
        // #296: live edge at current+50, target 150 → cushion 50, deficit 100.
        let f = MockFetcher {
            highest_existing: 100 + 50,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(
            detect_refill_deficit(&f, 100, 150).await,
            Some(100),
            "must report the exact below-target deficit so the consumer can refill"
        );
    }

    #[tokio::test]
    async fn refill_deficit_pins_edge_exactly_across_a_range_of_drains() {
        // The measured deficit must be EXACT (binary-refine pins the live
        // edge), for cushions anywhere below target.
        let target = 150_i64;
        for cushion in [1_i64, 7, 40, 99, 149] {
            let f = MockFetcher {
                highest_existing: 100 + cushion,
                probe_count: AtomicU32::new(0),
            };
            assert_eq!(
                detect_refill_deficit(&f, 100, target).await,
                Some((target - cushion) as u64),
                "cushion {cushion} must yield deficit {}",
                target - cushion
            );
        }
    }

    #[tokio::test]
    async fn refill_deficit_none_for_nonpositive_target() {
        // Fast endpoints (delivery_delay 0 → target 0) never refill; guard
        // returns None without probing.
        let f = MockFetcher {
            highest_existing: 5000,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_refill_deficit(&f, 100, 0).await, None);
        assert_eq!(
            f.probe_count.load(Ordering::SeqCst),
            0,
            "non-positive target short-circuits with no probes"
        );
    }

    #[tokio::test]
    async fn refill_deficit_none_when_live_edge_not_locatable() {
        // Producer sitting AT the live edge with nothing ahead (current+1
        // missing): the deficit cannot be measured → None (safe: don't
        // throttle when we cannot see headroom to rebuild from).
        let f = MockFetcher {
            highest_existing: 100,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_refill_deficit(&f, 100, 150).await, None);
    }

    #[tokio::test]
    async fn maybe_jump_does_not_probe_until_interval_reached() {
        let f = MockFetcher {
            highest_existing: 1_000_000,
            probe_count: AtomicU32::new(0),
        };
        let mut chunk_id = 100;
        let mut iters = 0u32;
        // Call < LAG_PROBE_INTERVAL_ITERS times: no probe.
        for _ in 0..(LAG_PROBE_INTERVAL_ITERS - 1) {
            maybe_jump(&f, &mut chunk_id, 60, &mut iters, "test", false).await;
        }
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 0);
        // Hit the threshold: ladder runs.
        maybe_jump(&f, &mut chunk_id, 60, &mut iters, "test", false).await;
        assert!(f.probe_count.load(Ordering::SeqCst) > 0);
        // Counter resets to 0 after firing.
        assert_eq!(iters, 0);
    }

    #[tokio::test]
    async fn maybe_jump_fast_endpoint_jumps_to_live_edge_when_behind() {
        // Fast endpoint (delivery_delay_ms=0, delivery_delay_chunks=0) that has
        // fallen behind MUST probe and jump to the live edge. Previously this
        // path short-circuited on `delivery_delay_ms == 0` and the fast
        // endpoint never caught up → fell behind unbounded → died repeatedly.
        let f = MockFetcher {
            highest_existing: 5000,
            probe_count: AtomicU32::new(0),
        };
        let mut chunk_id = 100;
        // One call short of the interval: this call fires the probe.
        let mut iters = LAG_PROBE_INTERVAL_ITERS - 1;
        maybe_jump(&f, &mut chunk_id, 0, &mut iters, "test", true).await;
        assert!(
            f.probe_count.load(Ordering::SeqCst) > 0,
            "fast endpoint must probe for the live edge"
        );
        assert!(
            chunk_id > 100,
            "fast endpoint behind the live edge must jump FORWARD (was 100, now {chunk_id})"
        );
        assert_eq!(iters, 0, "interval counter resets after firing");
    }

    #[tokio::test]
    async fn maybe_jump_fast_endpoint_at_edge_does_not_jump() {
        // Fast endpoint already at the live edge: probe runs but finds no
        // chunks ahead, so the read pointer stays put.
        let f = MockFetcher {
            highest_existing: 100,
            probe_count: AtomicU32::new(0),
        };
        let mut chunk_id = 100;
        let mut iters = LAG_PROBE_INTERVAL_ITERS - 1;
        maybe_jump(&f, &mut chunk_id, 0, &mut iters, "test", true).await;
        // Probe issued (interval reached), but no forward jump.
        assert!(f.probe_count.load(Ordering::SeqCst) > 0);
        assert_eq!(chunk_id, 100, "fast endpoint at live edge stays put");
    }
}
