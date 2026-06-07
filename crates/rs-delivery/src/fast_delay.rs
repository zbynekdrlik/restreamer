//! Adaptive read-delay controller for fast endpoints.
// Items are used by other modules wired up in a later task.
#![allow(dead_code)]
//!
//! A fast endpoint normally reads at the live edge (delay 0). That has zero
//! tolerance for local S3-upload latency spikes: when the live-edge chunk is
//! not yet in S3 the push starves and YouTube resets the idle connection.
//!
//! This controller makes the fast endpoint's read-delay ADAPTIVE: it grows
//! when the producer starves (so the live-edge lag-probe jumps to
//! `live_edge - delay` and leaves a buffer instead of yanking back to the
//! edge) and shrinks slowly when healthy (chasing the lowest working
//! latency). It NEVER speeds up the push — it only changes which chunk the
//! producer reads next. See the design doc for the full rationale.

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
    /// Wall-clock of the last grow OR shrink; gates the next shrink.
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

    /// Called while chunks are flowing normally. After `healthy_shrink_secs`
    /// with no change, shrink one step toward the floor. Returns
    /// `Some((from, to))` when the target changed.
    pub fn on_healthy(&mut self, now: Instant) -> Option<(u64, u64)> {
        if self.target_secs <= self.floor {
            return None;
        }
        if now.duration_since(self.last_change).as_secs() < self.healthy_shrink_secs {
            return None;
        }
        let from = self.target_secs;
        let next = from.saturating_sub(self.shrink_step).max(self.floor);
        self.target_secs = next;
        self.last_change = now;
        Some((from, next))
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
    fn grow_is_monotonic_until_shrink() {
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
    fn shrink_only_after_healthy_window() {
        let base = Instant::now();
        let mut c = ctrl(base);
        c.on_starvation(40, base); // -> 45 at t=0
        // before window: no shrink
        assert_eq!(c.on_healthy(base + Duration::from_secs(179)), None);
        // at window: one step down (45 -> 40)
        assert_eq!(
            c.on_healthy(base + Duration::from_secs(180)),
            Some((45, 40))
        );
        assert_eq!(c.target_secs(), 40);
    }

    #[test]
    fn shrink_floors_at_floor() {
        let base = Instant::now();
        let mut c = ctrl(base);
        c.on_starvation(2, base); // deficit2+margin5=7 -> 7
        assert_eq!(c.target_secs(), 7);
        let t = base + Duration::from_secs(180);
        assert_eq!(c.on_healthy(t), Some((7, 5))); // 7-5=2 -> max(floor)=5
        // already at floor -> no further shrink
        assert_eq!(c.on_healthy(t + Duration::from_secs(180)), None);
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
