//! Per-track source-ts history for the chunker and what a new source ts
//! means (#367).
//!
//! Since #367 the chunker stamps both tracks `src - session_origin`, so the
//! publisher's source timestamps drive everything. A real backward jump means
//! a new publisher reused the stream identifier, and it re-anchors the
//! session for BOTH tracks (a flush, then drop until the next keyframe). One
//! odd timestamp must not cost that:
//! - a tiny backward step (jitter, <= `TINY_BACKWARD_MS`) is stamped at the
//!   track's last ts instead;
//! - a lone forward glitch is only recognisable from its successor, which
//!   walks back below it but stays on the timeline from before the glitch.
//!   That is `AfterGlitch`: the glitch is dropped from the history and no
//!   re-anchor happens. The pusher clamps the glitch itself on the wire
//!   (`rs_rtmp_push`'s outlier isolation).

/// Backward steps up to this are timestamp jitter, not a new publisher.
pub(crate) const TINY_BACKWARD_MS: u32 = 1_000;

/// A forward step larger than this, walked back by the very next tag, was a
/// single corrupt timestamp (same bound as the pusher's
/// `MAX_TAG_TS_JUMP_MS`).
pub(crate) const GLITCH_JUMP_MS: u32 = 30_000;

/// What a track's new source ts means relative to its history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SrcStep {
    /// On the timeline: stamp it as is.
    Continue,
    /// A tiny backward step (jitter): stamp it at the track's last ts `to`.
    ClampTiny { to: u32 },
    /// The PREVIOUS tag was a lone forward glitch; this one is back on the
    /// timeline from before it.
    AfterGlitch { glitch: u32 },
    /// A real backward jump from `prev`: a new publisher on the identifier.
    NewTimeline { prev: u32 },
}

/// The last two accepted source ts of one track.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SrcTrack {
    last: Option<u32>,
    before_last: Option<u32>,
}

impl SrcTrack {
    /// Classify a new source ts against this track's history.
    pub(crate) fn classify(&self, ts: u32) -> SrcStep {
        let Some(last) = self.last else {
            return SrcStep::Continue;
        };
        if ts >= last {
            return SrcStep::Continue;
        }
        if let Some(before) = self.before_last {
            if ts >= before && last - before > GLITCH_JUMP_MS {
                return SrcStep::AfterGlitch { glitch: last };
            }
        }
        if last - ts <= TINY_BACKWARD_MS {
            return SrcStep::ClampTiny { to: last };
        }
        SrcStep::NewTimeline { prev: last }
    }

    /// Record an accepted tag's source ts according to its step.
    pub(crate) fn record(&mut self, ts: u32, step: SrcStep) {
        match step {
            SrcStep::Continue => {
                self.before_last = self.last;
                self.last = Some(ts);
            }
            // The history keeps its maximum: the clamped tag adds nothing.
            SrcStep::ClampTiny { .. } => {}
            // Drop the glitch; the pre-glitch ts stays the one before.
            SrcStep::AfterGlitch { .. } => self.last = Some(ts),
            SrcStep::NewTimeline { .. } => {
                self.before_last = None;
                self.last = Some(ts);
            }
        }
    }

    /// Forget the history (new session).
    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(points: &[u32]) -> SrcTrack {
        let mut t = SrcTrack::default();
        for &p in points {
            let step = t.classify(p);
            t.record(p, step);
        }
        t
    }

    #[test]
    fn forward_and_equal_steps_continue() {
        let t = track(&[100, 140]);
        assert_eq!(t.classify(140), SrcStep::Continue);
        assert_eq!(t.classify(180), SrcStep::Continue);
        assert_eq!(SrcTrack::default().classify(5), SrcStep::Continue);
    }

    #[test]
    fn tiny_backward_step_clamps_to_last_and_keeps_history() {
        let mut t = track(&[1_480, 1_520]);
        assert_eq!(t.classify(1_519), SrcStep::ClampTiny { to: 1_520 });
        assert_eq!(
            t.classify(1_520 - TINY_BACKWARD_MS),
            SrcStep::ClampTiny { to: 1_520 },
            "the tolerance is inclusive"
        );
        t.record(1_519, SrcStep::ClampTiny { to: 1_520 });
        assert_eq!(
            t.classify(1_519),
            SrcStep::ClampTiny { to: 1_520 },
            "the history kept 1_520"
        );
    }

    #[test]
    fn a_large_backward_jump_is_a_new_timeline() {
        let mut t = track(&[601_960, 602_000]);
        assert_eq!(
            t.classify(602_000 - TINY_BACKWARD_MS - 1),
            SrcStep::NewTimeline { prev: 602_000 }
        );
        assert_eq!(t.classify(0), SrcStep::NewTimeline { prev: 602_000 });
        t.record(0, SrcStep::NewTimeline { prev: 602_000 });
        assert_eq!(t.classify(40), SrcStep::Continue);
    }

    #[test]
    fn the_successor_of_a_lone_forward_glitch_is_after_glitch() {
        let mut t = track(&[360, 400 + 40_000]);
        assert_eq!(t.classify(440), SrcStep::AfterGlitch { glitch: 40_400 });
        t.record(440, SrcStep::AfterGlitch { glitch: 40_400 });
        assert_eq!(t.classify(480), SrcStep::Continue, "back on the timeline");
    }

    #[test]
    fn a_forward_step_within_the_glitch_bound_is_not_a_glitch() {
        // 360 -> 360 + GLITCH_JUMP_MS is a (large) normal step; walking back
        // more than the jitter tolerance from it is a new timeline.
        let t = track(&[360, 360 + GLITCH_JUMP_MS]);
        assert_eq!(t.classify(400), SrcStep::NewTimeline { prev: 30_360 });
    }

    #[test]
    fn going_below_the_pre_glitch_ts_is_a_new_timeline() {
        let t = track(&[360, 400 + 40_000]);
        assert_eq!(t.classify(100), SrcStep::NewTimeline { prev: 40_400 });
    }

    #[test]
    fn clear_forgets_everything() {
        let mut t = track(&[1_000, 2_000]);
        t.clear();
        assert_eq!(t.classify(0), SrcStep::Continue);
    }
}
