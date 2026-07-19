//! Adaptive read-delay controller for fast endpoints.
//!
//! The sole consumer (`producer_task`) lives in the `rs-delivery` BINARY
//! (`main.rs`), while this module is also declared `pub(crate)` in the
//! library crate (`lib.rs`) for integration tests. In the library
//! compilation unit there is no consumer, so every item reads as dead code;
//! the module-level allow below suppresses that false positive. The bin
//! build uses every item, so this does not hide a genuine dead-code bug.
#![allow(dead_code)]
//!
//! A fast endpoint normally reads at the live edge (delay 0). That has zero
//! tolerance for local S3-upload latency spikes: when the live-edge chunk is
//! not yet in S3 the push starves and YouTube resets the idle connection.
//!
//! This controller makes the fast endpoint's read-delay ADAPTIVE: it grows
//! when the producer starves (so the live-edge lag-probe jumps to
//! `live_edge - delay` and leaves a buffer instead of yanking back to the
//! edge). It NEVER speeds up the push — it only changes which chunk the
//! producer reads next. See the design doc for the full rationale.
//!
//! ## RATCHET-UP ONLY — no shrink within a session (#294, operator decision
//! 2026-07-17)
//!
//! The controller previously ALSO shrank slowly back toward the floor while
//! healthy. On a live event that caused the exact failure the operator
//! reported: after a drain grew the buffer, the periodic shrink pulled it
//! back toward the fragile live edge, which re-starved, which grew it again
//! — the buffer bounced up and down for hours and the fast stream stuttered
//! REPEATEDLY (visible YouTube buffering). Bouncing the delay itself hurts
//! smoothness. The operator's model: a fast endpoint starts at the lowest
//! buffer; a single/occasional stutter is fine; on a real drain it raises
//! the buffer and then **HOLDS** it — a few early stutters while it climbs to
//! the size this event's jitter needs, then smooth for the rest of the round.
//! No shrink-back, no oscillation. The near-live minimum is re-established on
//! the NEXT session (a fresh event / VPS spin-up constructs a fresh
//! controller starting at the floor), not by ratcheting down mid-session.

use std::time::Instant;

/// Lowest fast-stream read-delay when healthy (seconds).
pub const FAST_DELAY_FLOOR_SECS: u64 = 5;
/// Maximum read-delay (seconds) = same safety as the normal stream.
pub const FAST_DELAY_CEILING_SECS: u64 = 120;
/// Headroom added above the observed deficit when growing (seconds).
pub const FAST_DELAY_MARGIN_SECS: u64 = 5;
/// Step size when shrinking back toward the floor (seconds).
pub const FAST_DELAY_SHRINK_STEP_SECS: u64 = 5;
/// Healthy window (seconds) with no starvation before one shrink step.
pub const FAST_HEALTHY_SHRINK_SECS: u64 = 180;

#[derive(Debug, Clone)]
pub struct FastDelayController {
    target_secs: u64,
    floor: u64,
    ceiling: u64,
    margin: u64,
    shrink_step: u64,
    healthy_shrink_secs: u64,
    /// Wall-clock of the last change. Retained for controller
    /// introspection; no longer gates anything (#294 removed the shrink).
    last_change: Instant,
}

impl FastDelayController {
    /// Production constructor: floor/ceiling/margin/step from the consts above.
    pub fn new(now: Instant) -> Self {
        Self::with_params(
            FAST_DELAY_FLOOR_SECS,
            FAST_DELAY_CEILING_SECS,
            FAST_DELAY_MARGIN_SECS,
            FAST_DELAY_SHRINK_STEP_SECS,
            FAST_HEALTHY_SHRINK_SECS,
            now,
        )
    }

    /// Test/explicit constructor.
    pub fn with_params(
        floor: u64,
        ceiling: u64,
        margin: u64,
        shrink_step: u64,
        healthy_shrink_secs: u64,
        now: Instant,
    ) -> Self {
        Self {
            target_secs: floor,
            floor,
            ceiling,
            margin,
            shrink_step,
            healthy_shrink_secs,
            last_change: now,
        }
    }

    pub fn target_secs(&self) -> u64 {
        self.target_secs
    }

    /// Producer starved: the chunk it needs is not in S3 yet. `deficit_secs`
    /// is how far the needed chunk trails the newest chunk available in S3
    /// (0 when unknown). Grows the target to `max(target, deficit + margin)`,
    /// clamped to the ceiling. Returns `Some((from, to))` when the target
    /// actually changed.
    pub fn on_starvation(&mut self, deficit_secs: u64, now: Instant) -> Option<(u64, u64)> {
        let want = deficit_secs
            .saturating_add(self.margin)
            .clamp(self.floor, self.ceiling);
        let next = self.target_secs.max(want);
        if next != self.target_secs {
            let from = self.target_secs;
            self.target_secs = next;
            self.last_change = now;
            Some((from, next))
        } else {
            None
        }
    }

    /// Called while chunks are flowing normally.
    ///
    /// RATCHET-UP ONLY (#294): this is now a NO-OP and never shrinks. Shrinking
    /// back toward the floor mid-session pulled the fast endpoint to the
    /// fragile live edge and re-starved it, producing the repeated stuttering
    /// / buffer-bouncing the operator observed on 2026-07-17. The buffer is
    /// held at whatever level a real drain required for this session; the
    /// near-live minimum is re-established only by a fresh session (new
    /// controller at the floor). Kept as a method so the call site is
    /// unchanged; always returns `None`.
    pub fn on_healthy(&mut self, _now: Instant) -> Option<(u64, u64)> {
        None
    }

    /// Current target expressed in chunks, for the live-edge lag-probe.
    /// Always >= 1 so a fast endpoint never re-pins to the absolute edge.
    pub fn delay_chunks(&self, typical_chunk_dur_ms: u64) -> i64 {
        let dur = typical_chunk_dur_ms.max(1);
        ((self.target_secs.saturating_mul(1000) / dur) as i64).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ctrl(now: Instant) -> FastDelayController {
        // floor 5, ceiling 120, margin 5, step 5, healthy-window 180
        FastDelayController::with_params(5, 120, 5, 5, 180, now)
    }

    #[test]
    fn starts_at_floor() {
        let now = Instant::now();
        assert_eq!(ctrl(now).target_secs(), 5);
    }

    #[test]
    fn grows_to_deficit_plus_margin() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 20s -> target 25s
        assert_eq!(c.on_starvation(20, now), Some((5, 25)));
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn grow_is_monotonic() {
        let now = Instant::now();
        let mut c = ctrl(now);
        c.on_starvation(20, now); // -> 25
        // smaller deficit does not lower the target
        assert_eq!(c.on_starvation(5, now), None);
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn grow_clamps_to_ceiling() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 200s + margin would be 205 -> clamp to 120
        assert_eq!(c.on_starvation(200, now), Some((5, 120)));
        assert_eq!(c.target_secs(), 120);
    }

    #[test]
    fn unknown_deficit_grows_by_margin_floor() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 0 -> want = max(floor, margin)=5 == floor -> no change at floor
        assert_eq!(c.on_starvation(0, now), None);
        // after a grow to 25, deficit-0 still cannot lower
        c.on_starvation(20, now);
        assert_eq!(c.on_starvation(0, now), None);
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn healthy_never_shrinks_ratchet_holds_for_whole_session() {
        // #294 regression (live event 2026-07-17): after a drain grew the
        // buffer, the periodic healthy-shrink pulled it back toward the
        // fragile live edge, which re-starved and re-grew it — the buffer
        // bounced up/down for hours and the fast stream stuttered
        // REPEATEDLY. Operator decision: ratchet UP only; once raised, HOLD
        // for the rest of the session, no matter how long it stays healthy.
        let base = Instant::now();
        let mut c = ctrl(base);
        c.on_starvation(40, base); // -> 45 at t=0
        // No shrink at any horizon: 3 min, 30 min, 3 hours.
        assert_eq!(c.on_healthy(base + Duration::from_secs(180)), None);
        assert_eq!(c.on_healthy(base + Duration::from_secs(1800)), None);
        assert_eq!(c.on_healthy(base + Duration::from_secs(10800)), None);
        assert_eq!(c.target_secs(), 45, "raised buffer must HOLD, never bounce");
    }

    #[test]
    fn ratchet_climbs_across_repeated_drains_and_holds() {
        // The expected session shape: a few early stutters climb the buffer
        // to what this event's jitter needs, then it holds there — later
        // healthy periods and smaller drains never move it. (Deficits mirror
        // the real 2026-07-17 freeze gaps: 4s, 18s, 29s.)
        let base = Instant::now();
        let mut c = ctrl(base);
        assert_eq!(c.on_starvation(4, base), Some((5, 9))); // 4 + margin 5
        assert_eq!(c.on_starvation(18, base), Some((9, 23)));
        assert_eq!(c.on_starvation(29, base), Some((23, 34)));
        // long healthy stretch: holds
        assert_eq!(c.on_healthy(base + Duration::from_secs(3600)), None);
        // a smaller later drain does not lower it
        assert_eq!(c.on_starvation(10, base + Duration::from_secs(3700)), None);
        assert_eq!(c.target_secs(), 34);
    }

    #[test]
    fn delay_chunks_uses_chunk_duration() {
        let now = Instant::now();
        let mut c = ctrl(now);
        c.on_starvation(20, now); // 25s
        // 2000ms chunks -> 25000/2000 = 12 chunks
        assert_eq!(c.delay_chunks(2000), 12);
        // 1000ms chunks -> 25 chunks
        assert_eq!(c.delay_chunks(1000), 25);
        // never below 1 even at floor with huge chunks
        let edge = ctrl(now);
        assert_eq!(edge.delay_chunks(60_000), 1);
    }
}
