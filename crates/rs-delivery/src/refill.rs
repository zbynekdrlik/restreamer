//! Slow cushion-refill for BUFFERED (non-fast) endpoints (#296).
//!
//! ## The bug this fixes
//!
//! A buffered endpoint's cache cushion is a **time** cushion: it delivers
//! content that is `delivery_delay` seconds behind the live edge. Content is
//! produced at 1x realtime and consumed at 1x realtime, so once a source gap
//! (OBS restart, brief outage) lets the consumer eat into the cushion, the
//! cushion **never recovers** — transferring faster cannot rebuild it because
//! the missing content does not exist yet. Every subsequent outage then starts
//! with less protection than the one before (live event 9333, 2026-07-19:
//! YouTube endpoints fell 303s → 203s and stayed there for the rest of the
//! event).
//!
//! Fast endpoints got the equivalent recovery via the #294 read-delay ratchet;
//! buffered endpoints had no mechanism at all.
//!
//! ## The mechanism
//!
//! The only way to rebuild a time cushion is to consume slightly SLOWER than
//! realtime until the target is reached: while the delivered cushion is below
//! target, deliver at `REFILL_SPEED_FACTOR` (0.98x). Over wall time `T` the
//! source produces `T` seconds of media while the endpoint delivers `0.98·T`,
//! so the cushion grows by `0.02·T`. Rebuilding 100s takes ~1.4h — imperceptible
//! to a viewer (no visible stall, and no audio pitch shift: FLV chunk PTS are
//! untouched, only the wall-clock PACING of the RTMP push shifts, which the
//! YouTube/Facebook ingest buffer absorbs), unlike a visible rebuffer.
//!
//! ## Engineering forks (decided here, per the ticket leaving them to the
//! implementer)
//!
//! - **Flat vs deficit-proportional rate → FLAT.** A single fixed
//!   `REFILL_SPEED_FACTOR` is simplest, has zero overshoot risk (the deficit
//!   itself is the stop condition — throttle ceases the instant the cushion
//!   reaches target), and keeps the slowdown safely inside the
//!   "imperceptible" envelope the operator specified. The deficit magnitude
//!   only gates the throttle on/off; it never scales the rate.
//! - **Absolute per-chunk cap.** The extra per-chunk sleep is additionally
//!   capped at `REFILL_MAX_THROTTLE_MS`, far below both
//!   `rescue::RESCUE_STALL_THRESHOLD_SECS` (8s) and
//!   `endpoint_task::WRITE_TIMEOUT_SECS` (30s), so a pathological chunk
//!   duration can never turn the gentle refill into a stall that trips the
//!   rescue / write-timeout machinery.
//!
//! Pure helpers only — the async consumer-side throttle lives in
//! `endpoint_consumer_helpers::maybe_refill_throttle`, and the producer-side
//! deficit probe in `producer_lag::detect_refill_deficit`.
#![allow(dead_code)]

/// Delivery speed (fraction of realtime) while a buffered endpoint's cushion
/// is below target. 0.98 = 2% slower → rebuilds ~100s of cushion in ~1.4h,
/// absorbed by the ingest buffer with no viewer-visible effect.
pub const REFILL_SPEED_FACTOR: f64 = 0.98;

/// Absolute ceiling on the extra per-chunk sleep the refill throttle may add
/// (ms). Far below `RESCUE_STALL_THRESHOLD_SECS` (8_000ms) and
/// `WRITE_TIMEOUT_SECS` (30_000ms) so the refill can never look like a stall.
pub const REFILL_MAX_THROTTLE_MS: u64 = 500;

/// Deficit (in CHUNKS) of a buffered endpoint whose live edge is `max_id` and
/// whose read/deliver pointer is `current`, for a configured cushion of
/// `target_chunks`. The cushion is `max_id - current`; the deficit is how many
/// chunks that falls SHORT of the target. Returns 0 when the cushion is at or
/// above target (the steady state — nothing to refill) or when the inputs are
/// nonsensical (negative target, pointer ahead of the live edge).
pub fn refill_deficit_chunks(max_id: i64, current: i64, target_chunks: i64) -> u64 {
    if target_chunks <= 0 {
        return 0;
    }
    // Live edge behind the read pointer is a nonsensical / transient
    // (clock-race) reading — treat it as "cannot determine a deficit", never
    // as a full deficit that would wrongly throttle. `max_id == current` (fully
    // drained AT the live edge) is legitimate and DOES yield the full deficit.
    if max_id < current {
        return 0;
    }
    let cushion = max_id - current;
    (target_chunks - cushion).max(0) as u64
}

/// Extra wall-clock sleep (ms) to add after delivering one chunk of
/// `chunk_duration_ms` so the effective delivery rate becomes
/// `REFILL_SPEED_FACTOR`. Returns 0 when `deficit_secs == 0` (at/above target
/// — deliver at the normal 1x). The deficit magnitude ONLY gates on/off; the
/// rate is flat. The result is clamped to `REFILL_MAX_THROTTLE_MS` and the
/// chunk duration is clamped to a sane `[500, 5000]ms` window first so an
/// outlier duration can never produce a stall-sized sleep.
pub fn refill_throttle_ms(deficit_secs: u64, chunk_duration_ms: i64) -> u64 {
    if deficit_secs == 0 {
        return 0;
    }
    let dur = (chunk_duration_ms.max(0) as u64).clamp(500, 5000) as f64;
    // deliver `dur` ms of media over `dur / factor` ms of wall time → the extra
    // wall time per chunk is `dur * (1/factor - 1)`.
    let extra = (dur * (1.0 / REFILL_SPEED_FACTOR - 1.0)).round() as u64;
    extra.min(REFILL_MAX_THROTTLE_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deficit_chunks_zero_when_cushion_at_or_above_target() {
        // cushion == target → no deficit
        assert_eq!(refill_deficit_chunks(100 + 150, 100, 150), 0);
        // cushion > target (excess lag — maybe_jump's job, not refill's)
        assert_eq!(refill_deficit_chunks(100 + 300, 100, 150), 0);
        // pointer ahead of live edge (clock race) → clamped, no deficit
        assert_eq!(refill_deficit_chunks(90, 100, 150), 0);
    }

    #[test]
    fn deficit_chunks_positive_when_below_target() {
        // cushion 50, target 150 → deficit 100
        assert_eq!(refill_deficit_chunks(100 + 50, 100, 150), 100);
        // fully drained to the live edge → deficit == full target
        assert_eq!(refill_deficit_chunks(100, 100, 150), 150);
        // cushion 1 → deficit 149
        assert_eq!(refill_deficit_chunks(101, 100, 150), 149);
    }

    #[test]
    fn deficit_chunks_zero_when_target_nonpositive() {
        // fast endpoints (no delay) never have a refill target
        assert_eq!(refill_deficit_chunks(500, 100, 0), 0);
        assert_eq!(refill_deficit_chunks(500, 100, -5), 0);
    }

    #[test]
    fn throttle_zero_at_or_above_target() {
        // deficit 0 → deliver at normal 1x, no extra sleep
        assert_eq!(refill_throttle_ms(0, 2000), 0);
        assert_eq!(refill_throttle_ms(0, 1000), 0);
    }

    #[test]
    fn throttle_positive_and_small_when_below_target() {
        // 2000ms chunk at 0.98x → extra = 2000*(1/0.98 - 1) ≈ 41ms
        let t = refill_throttle_ms(50, 2000);
        assert!(t > 0, "below target must add SOME throttle");
        assert_eq!(t, 41, "0.98x of a 2000ms chunk adds ~41ms");
        // imperceptible: far below the chunk duration itself
        assert!(t < 2000);
    }

    #[test]
    fn throttle_flat_regardless_of_deficit_magnitude() {
        // FLAT rate: a 10s deficit and a 200s deficit throttle IDENTICALLY.
        assert_eq!(refill_throttle_ms(10, 2000), refill_throttle_ms(200, 2000));
    }

    #[test]
    fn throttle_respects_absolute_cap_on_pathological_chunk_duration() {
        // A huge chunk duration must never produce a stall-sized sleep.
        assert!(refill_throttle_ms(50, 5_000_000) <= REFILL_MAX_THROTTLE_MS);
        // And the cap is far below the 8s rescue-stall / 30s write-timeout gates.
        const { assert!(REFILL_MAX_THROTTLE_MS < 8_000) };
    }

    /// The core feedback loop: a buffered endpoint drained below target must
    /// climb BACK to target over time and then HOLD, without the per-chunk
    /// throttle ever reaching a stall-sized value. This is the #296 acceptance
    /// ("drain -> cushion climbs back toward target") as a deterministic
    /// simulation of the producer/consumer physics driven by the real
    /// `refill_throttle_ms`.
    #[test]
    fn refill_climbs_cushion_back_to_target_without_stall() {
        let target_secs = 300.0_f64;
        let chunk_ms = 2000_i64;
        // Start drained (the 9333 evidence: 303 → 203, a ~100s deficit).
        let mut cushion = 200.0_f64;
        let mut prev = cushion;
        let mut reached = false;

        for _ in 0..100_000 {
            // `ceil` mirrors the real chunk-quantized deficit: any positive
            // shortfall is still a deficit, so the refill runs until the
            // cushion actually reaches target (0 shortfall), then stops.
            let deficit = (target_secs - cushion).max(0.0).ceil() as u64;
            let throttle_ms = refill_throttle_ms(deficit, chunk_ms);
            // Never a stall-sized throttle.
            assert!(
                throttle_ms <= REFILL_MAX_THROTTLE_MS,
                "throttle {throttle_ms}ms must stay under the cap"
            );
            if deficit == 0 {
                // At/above target: throttle stops, cushion HOLDS (no shrink).
                assert_eq!(throttle_ms, 0, "no throttle once the cushion is refilled");
                reached = true;
                break;
            }
            // While below target the throttle must make progress.
            assert!(
                throttle_ms > 0,
                "below target must throttle to make progress"
            );
            // Over one delivered chunk of wall time `chunk_ms + throttle_ms`,
            // the source adds that many ms of media while the endpoint delivers
            // only `chunk_ms` → the cushion grows by `throttle_ms`.
            cushion += throttle_ms as f64 / 1000.0;
            assert!(
                cushion > prev,
                "cushion must climb monotonically while below target"
            );
            prev = cushion;
        }

        assert!(
            reached,
            "cushion must reach target within the iteration budget"
        );
        assert!(
            cushion >= target_secs,
            "final cushion {cushion} must be at least the target {target_secs}"
        );
    }
}
