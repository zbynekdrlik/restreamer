//! Per-track source-ts history for the chunker and what a new source ts
//! means (#367).
//!
//! Since #367 the chunker stamps both tracks `src - session_origin`, so the
//! publisher's source timestamps drive everything. A real backward jump means
//! a new publisher reused the stream identifier, and it re-anchors the
//! session for BOTH tracks (a flush, then a new shared origin). One odd
//! timestamp must not cost that:
//! - a tiny backward step (jitter, <= `TINY_BACKWARD_MS`) is stamped at the
//!   track's last ts instead;
//! - a lone forward glitch is only recognisable from its successor, which
//!   walks back below it but stays on the timeline from before the glitch.
//!   That is `AfterGlitch`: the glitch is dropped from the history and no
//!   re-anchor happens. The pusher clamps the glitch itself on the wire
//!   (`rs_rtmp_push`'s outlier isolation);
//! - a far backward step (`FarBackward`) is only a CANDIDATE new timeline:
//!   a lone LOW glitch looks the same. The chunker holds that tag and lets
//!   the next one decide (`flv_chunker_ingest`);
//! - a far forward step (`FarForward`) is written and recorded as is (its
//!   successor tells whether it was a lone glitch), but it never stretches
//!   the chunk's content duration.
//!
//! Known limit: TWO consecutive forward-glitched tags leave the second one
//! as `before_last`, so the walk back is `FarBackward`, not `AfterGlitch`;
//! the next tag then confirms it as a new timeline (one re-anchor). A
//! bounded "pre-glitch anchor" could fix it, but it must expire, or a later
//! real new publisher above that anchor would be missed. The pusher's
//! outlier rules still keep the wire clean.

/// A media track of the chunker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Track {
    Video,
    Audio,
}

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
    /// A forward step larger than `GLITCH_JUMP_MS`: stamped and recorded as
    /// is, but it does not extend the chunk's content duration.
    FarForward,
    /// A tiny backward step (jitter): stamp it at the track's last ts `to`.
    ClampTiny { to: u32 },
    /// The PREVIOUS tag was a lone forward glitch; this one is back on the
    /// timeline from before it.
    AfterGlitch { glitch: u32 },
    /// A far backward step from `prev`: a new publisher on the identifier,
    /// or a lone low glitch. The chunker never records it: it holds the tag,
    /// and records `Continue` (after the re-anchor cleared the history) or
    /// `ClampTiny` once the next tag decided. `record` treats it as the
    /// start of a new timeline.
    FarBackward { prev: u32 },
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
            if ts - last > GLITCH_JUMP_MS {
                return SrcStep::FarForward;
            }
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
        SrcStep::FarBackward { prev: last }
    }

    /// Record an accepted tag's source ts according to its step.
    pub(crate) fn record(&mut self, ts: u32, step: SrcStep) {
        match step {
            SrcStep::Continue | SrcStep::FarForward => {
                self.before_last = self.last;
                self.last = Some(ts);
            }
            // The history keeps its maximum: the clamped tag adds nothing.
            SrcStep::ClampTiny { .. } => {}
            // Drop the glitch; the pre-glitch ts stays the one before.
            SrcStep::AfterGlitch { .. } => self.last = Some(ts),
            SrcStep::FarBackward { .. } => {
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
    fn a_large_backward_jump_is_far_backward_and_records_a_new_timeline() {
        let mut t = track(&[601_960, 602_000]);
        assert_eq!(
            t.classify(602_000 - TINY_BACKWARD_MS - 1),
            SrcStep::FarBackward { prev: 602_000 }
        );
        assert_eq!(t.classify(0), SrcStep::FarBackward { prev: 602_000 });
        t.record(0, SrcStep::FarBackward { prev: 602_000 });
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
        // more than the jitter tolerance from it is a far backward step.
        let t = track(&[360, 360 + GLITCH_JUMP_MS]);
        assert_eq!(t.classify(400), SrcStep::FarBackward { prev: 30_360 });
    }

    #[test]
    fn a_forward_step_past_the_glitch_bound_is_far_forward() {
        let mut t = track(&[360]);
        assert_eq!(
            t.classify(360 + GLITCH_JUMP_MS),
            SrcStep::Continue,
            "the bound is inclusive"
        );
        assert_eq!(t.classify(361 + GLITCH_JUMP_MS), SrcStep::FarForward);
        t.record(40_400, SrcStep::FarForward);
        assert_eq!(
            t.classify(440),
            SrcStep::AfterGlitch { glitch: 40_400 },
            "recorded like a normal step, so its successor can tell a lone glitch"
        );
    }

    #[test]
    fn going_below_the_pre_glitch_ts_is_far_backward() {
        let t = track(&[360, 400 + 40_000]);
        assert_eq!(t.classify(100), SrcStep::FarBackward { prev: 40_400 });
    }

    #[test]
    fn clear_forgets_everything() {
        let mut t = track(&[1_000, 2_000]);
        t.clear();
        assert_eq!(t.classify(0), SrcStep::Continue);
    }
}
