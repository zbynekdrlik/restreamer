//! Absolute A/V invariant guard (#367) — defense in depth, NO baseline.
//!
//! INVARIANT: the A/V relationship is defined ONLY by the publisher's source
//! timestamps. Every stage (the stream.lan chunker, each VPS pusher) applies
//! ONE common transform (same origin, same base, same rate) to both tracks.
//! For the latest tag of each track this means:
//!
//! ```text
//! (a_out - a_in) == (v_out - v_in)        i.e. (a_out - v_out) == (a_in - v_in)
//! ```
//!
//! This guard checks exactly that, within [`AV_INVARIANT_TOLERANCE_MS`]. It
//! has NO baseline. The relative skew guards (#257/#354/#359, `SkewTracker`)
//! subtract a first-chunk baseline, so an offset present from chunk 0 (the
//! 2026-10-01 late-join offset) is invisible to them by construction. Here
//! it is a violation from the first evaluation. The relative guards stay;
//! they cover a different failure, a source-side DRIFT.
//!
//! Pure state, no I/O. The stage that owns a guard logs the edges and turns
//! them into `AvInvariantViolated` / `AvInvariantRestored` audit rows.

/// Allowed |delta| between the two tracks' transform offsets, in ms.
pub const AV_INVARIANT_TOLERANCE_MS: i64 = 50;

/// Whether a delta breaks the invariant (the tolerance itself is still OK).
fn outside_tolerance(delta_ms: i64) -> bool {
    delta_ms.abs() > AV_INVARIANT_TOLERANCE_MS
}

/// A measured violation: how far the stage moved audio relative to video.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AvInvariantViolation {
    /// Offset the stage applied to the latest AUDIO tag (`out - in`).
    pub a_rel_ms: i64,
    /// Offset the stage applied to the latest VIDEO tag (`out - in`).
    pub v_rel_ms: i64,
    /// `a_rel_ms - v_rel_ms`: positive = audio pushed later than video.
    pub delta_ms: i64,
}

/// An edge of the latched violation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvInvariantEvent {
    /// The invariant broke (first evaluation outside tolerance).
    Violated(AvInvariantViolation),
    /// The latch cleared. `delta_ms` is the delta of the latest samples at
    /// that moment (back within tolerance, or the last value seen before a
    /// reset).
    Restored { delta_ms: i64 },
}

/// Tracks the latest `(input, output)` ts of each track under the stage's
/// CURRENT transform and latches a violation edge-triggered.
#[derive(Debug, Default)]
pub struct AvInvariantGuard {
    /// Latest audio `(input_ts, output_ts)` under the current transform.
    audio: Option<(i64, i64)>,
    /// Latest video `(input_ts, output_ts)` under the current transform.
    video: Option<(i64, i64)>,
    /// Whether a violation is currently latched.
    latched: bool,
    /// Violation edges since creation (telemetry).
    violations: u32,
}

impl AvInvariantGuard {
    /// Record the latest AUDIO tag's input ts and the ts the stage emitted.
    pub fn observe_audio(&mut self, input_ts: i64, output_ts: i64) {
        self.audio = Some((input_ts, output_ts));
    }

    /// Record the latest VIDEO tag's input ts and the ts the stage emitted.
    pub fn observe_video(&mut self, input_ts: i64, output_ts: i64) {
        self.video = Some((input_ts, output_ts));
    }

    /// The delta of the latest samples, if both tracks were seen under the
    /// current transform.
    fn delta(&self) -> Option<AvInvariantViolation> {
        let (a_in, a_out) = self.audio?;
        let (v_in, v_out) = self.video?;
        let a_rel_ms = a_out - a_in;
        let v_rel_ms = v_out - v_in;
        Some(AvInvariantViolation {
            a_rel_ms,
            v_rel_ms,
            delta_ms: a_rel_ms - v_rel_ms,
        })
    }

    /// The current violation, if both tracks were seen and the delta is
    /// outside tolerance.
    pub fn check(&self) -> Option<AvInvariantViolation> {
        self.delta().filter(|d| outside_tolerance(d.delta_ms))
    }

    /// Evaluate at a chunk boundary. Returns an edge (`Violated` when the
    /// invariant first breaks, `Restored` when it holds again) or `None`.
    /// Without samples of both tracks nothing can be decided: no edge.
    pub fn evaluate(&mut self) -> Option<AvInvariantEvent> {
        let current = self.delta()?;
        let violated = outside_tolerance(current.delta_ms);
        match (self.latched, violated) {
            (false, true) => {
                self.latched = true;
                self.violations = self.violations.saturating_add(1);
                Some(AvInvariantEvent::Violated(current))
            }
            (true, false) => {
                self.latched = false;
                Some(AvInvariantEvent::Restored {
                    delta_ms: current.delta_ms,
                })
            }
            _ => None,
        }
    }

    /// The stage's transform legitimately changed (session re-anchor,
    /// reconnect, re-anchor): forget the samples so an old-transform sample of
    /// one track is never paired with a new-transform sample of the other.
    /// The latch is kept: the next evaluation with fresh samples decides.
    pub fn begin_new_transform(&mut self) {
        self.audio = None;
        self.video = None;
    }

    /// Full reset (new session). Returns `Restored` if a violation was
    /// latched, so the owner can close the operator episode.
    pub fn reset(&mut self) -> Option<AvInvariantEvent> {
        let delta_ms = self.delta().map_or(0, |d| d.delta_ms);
        self.begin_new_transform();
        if std::mem::take(&mut self.latched) {
            Some(AvInvariantEvent::Restored { delta_ms })
        } else {
            None
        }
    }

    /// Whether a violation is currently latched.
    pub fn is_violated(&self) -> bool {
        self.latched
    }

    /// The latched violation's current delta, if latched and measurable.
    pub fn latched_delta_ms(&self) -> Option<i64> {
        if self.latched {
            self.delta().map(|d| d.delta_ms)
        } else {
            None
        }
    }

    /// Violation edges since creation.
    pub fn violation_count(&self) -> u32 {
        self.violations
    }
}
