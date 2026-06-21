//! Cross-track A/V-skew detection + bounded recovery decision (issue #257).
//!
//! ## Why this exists — defense-in-depth behind the chunker shared-epoch fix
//!
//! The 2026-06-19 live incident (event 9316) propagated a ~25.5 s
//! audio-behind-video skew verbatim to every endpoint. The root cause was
//! source-side (the chunker stamped audio and video on independent epochs on
//! an OBS republish) and is fixed by #255 (chunker shared session epoch). This
//! module is the CONSUMER-side safety net + observability so a desync can never
//! again be silent or permanent — it defends against any FUTURE producer
//! regression, the rescue→resume / endpoint-reconnect skew trigger that #255
//! does NOT cover, and the rescue-clip→live-resume codec-foreign boundary
//! (#249).
//!
//! ## Why CONTENT-PTS, not container-output-ts
//!
//! The pusher re-anchors audio and video on independent per-track timelines
//! (`pusher.rs`). A guard based on the wire `output_ts` of each track
//! (`max_audio_output_ts` vs `max_video_output_ts`) is BLIND to the exact
//! desync the operator sees: the per-track re-anchor realigns the
//! *container timestamps* (so `a_out ≈ v_out`, the "normal" −60..−100 ms band
//! reported on all 11 endpoints during the incident) while the actual
//! picture/sound stays offset. The 2026-06-19 telemetry proved this directly.
//!
//! So the skew metric is computed from the **input** FLV tag timestamps — the
//! chunker-stamped PTS the pusher receives BEFORE its per-track re-anchor
//! rewrites them — measured as each track's progress from a common session
//! origin. On a healthy shared-epoch source audio-PTS and video-PTS advance
//! together → `av_skew_ms ≈ 0`. When the chunker propagates a desync, audio's
//! input PTS lags video's by ~25.5 s → `av_skew_ms ≈ 25500` even though the
//! wire output timestamps look aligned.

/// Hard A/V-skew guard threshold (ms). When the content-PTS skew between the
/// two tracks exceeds this for `SKEW_DEBOUNCE_CHUNKS` consecutive chunks, the
/// pusher trips `PushError::AvSkewExceeded`, forcing a clean reconnect so both
/// tracks re-anchor from a common session start.
///
/// 4000 ms is well above any benign per-chunk straddle (chunks flush at video
/// keyframes; an audio frame straddling that boundary lands in whichever chunk
/// is open, a sub-100 ms effect) yet far below the 25_500 ms incident skew.
pub const MAX_AV_SKEW_MS: i64 = 4_000;

/// Consecutive-chunk debounce before a skew trips recovery. A single chunk
/// can carry a transient straddle at a keyframe boundary or right after a
/// re-anchor; requiring the skew to PERSIST across several chunks rejects
/// those transients and only acts on a real, sustained desync.
pub const SKEW_DEBOUNCE_CHUNKS: u32 = 3;

/// Minimum wall-clock gap (ms) between two skew-triggered recoveries. A
/// persistent upstream skew that survives the reconnect must NOT cause the
/// pusher to thrash (drop+reconnect every chunk). The rate limit caps recovery
/// attempts; between attempts the pusher keeps delivering (strict 1×, never a
/// speed-up — recovery is only ever a clean reconnect + re-anchor).
pub const SKEW_RECOVERY_MIN_INTERVAL_MS: u64 = 60_000;

/// Per-track input-PTS progress accumulator.
///
/// `origin` pins the first non-seq-header input timestamp seen on the track in
/// the current session; `max_progress` is the largest `input_ts - origin`
/// observed. Comparing the two tracks' `max_progress` yields the content-PTS
/// skew that survives the per-track output re-anchor.
#[derive(Default, Clone, Copy)]
pub struct TrackProgress {
    origin: Option<u32>,
    max_progress: i64,
}

impl TrackProgress {
    /// Observe one input tag timestamp. Anchors the origin on the first call,
    /// then advances `max_progress` to the largest delta-from-origin seen.
    pub fn observe(&mut self, input_ts: u32) {
        let origin = *self.origin.get_or_insert(input_ts);
        let progress = (input_ts as i64) - (origin as i64);
        if progress > self.max_progress {
            self.max_progress = progress;
        }
    }

    /// Reset on a fresh session / re-anchor so the next tag re-pins the origin.
    pub fn reset(&mut self) {
        self.origin = None;
        self.max_progress = 0;
    }

    pub fn max_progress(&self) -> i64 {
        self.max_progress
    }
}

/// Cross-track skew tracker. Accumulates per-track input-PTS progress, exposes
/// the signed `av_skew_ms` (positive = audio behind video, the incident
/// direction), debounces over consecutive chunks, and rate-limits recovery.
#[derive(Default)]
pub struct SkewTracker {
    audio: TrackProgress,
    video: TrackProgress,
    /// Consecutive chunks whose end-of-chunk `|av_skew_ms|` exceeded the
    /// threshold. Reset to 0 the moment a chunk comes back under threshold.
    consecutive_over: u32,
    /// Last computed signed skew (video_progress − audio_progress), surfaced
    /// to telemetry.
    last_skew_ms: i64,
    /// Number of times this tracker has tripped recovery (for telemetry /
    /// alerting + the reconnect-thrash rate limit).
    trip_count: u32,
}

impl SkewTracker {
    /// Observe one input AUDIO tag timestamp (pre-re-anchor, content PTS).
    pub fn observe_audio(&mut self, input_ts: u32) {
        self.audio.observe(input_ts);
    }

    /// Observe one input VIDEO tag timestamp (pre-re-anchor, content PTS).
    pub fn observe_video(&mut self, input_ts: u32) {
        self.video.observe(input_ts);
    }

    /// Reset BOTH tracks' progress on a fresh RTMP session / symmetric
    /// re-anchor so skew is measured from the new common origin. The debounce
    /// counter is also cleared — a re-anchor establishes a clean baseline.
    pub fn reset_tracks(&mut self) {
        self.audio.reset();
        self.video.reset();
        self.consecutive_over = 0;
    }

    /// Signed content-PTS skew: `video_progress − audio_progress`. Positive
    /// means audio is BEHIND video (the 2026-06-19 incident direction).
    pub fn current_skew_ms(&self) -> i64 {
        self.video.max_progress() - self.audio.max_progress()
    }

    /// Last skew computed at a `chunk_done` boundary (telemetry surface).
    pub fn last_skew_ms(&self) -> i64 {
        self.last_skew_ms
    }

    pub fn trip_count(&self) -> u32 {
        self.trip_count
    }

    /// Call at the END of each chunk. Updates the debounce counter from the
    /// current skew and returns the decision for this chunk boundary.
    ///
    /// `now_ms` is a monotonic wall-clock in ms (the pusher passes
    /// `anchor.elapsed()`); it gates the reconnect-thrash rate limit.
    pub fn evaluate_chunk(&mut self, now_ms: u64) -> SkewDecision {
        let skew = self.current_skew_ms();
        self.last_skew_ms = skew;
        if skew.abs() > MAX_AV_SKEW_MS {
            self.consecutive_over = self.consecutive_over.saturating_add(1);
        } else {
            self.consecutive_over = 0;
        }
        self.decide(now_ms)
    }
}

/// What the pusher should do at a chunk boundary given the skew state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SkewDecision {
    /// Skew within bounds (or debounce/rate-limit not yet satisfied) — keep
    /// pushing at strict real-time.
    Continue,
    /// Sustained skew over threshold — the pusher must return
    /// `PushError::AvSkewExceeded` so the consumer force-closes and reconnects,
    /// re-anchoring BOTH tracks from a common session start.
    TripRecovery,
}

impl SkewTracker {
    /// Decide whether a sustained over-threshold skew should trip recovery,
    /// honoring the debounce and the reconnect-thrash rate limit.
    ///
    /// RED state (issue #257): the threshold guard is NOT yet wired — the
    /// pusher propagates whatever skew the source carries, exactly the
    /// 2026-06-19 silent-desync behavior. Always returns `Continue`.
    fn decide(&mut self, _now_ms: u64) -> SkewDecision {
        SkewDecision::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the per-track input-PTS sequences for ONE chunk where audio lags
    /// video by `audio_lag_ms` on the chunker's shared epoch.
    ///
    /// Video frames advance at ~33 ms (30 fps); audio at ~21 ms (AAC). Both
    /// start from the same wall reference EXCEPT audio's PTS is offset earlier
    /// by `audio_lag_ms`, exactly the 2026-06-19 incident shape (audio behind
    /// video). The pusher's per-track output re-anchor would mask this on the
    /// wire, but the INPUT PTS deltas carry the true content skew.
    fn chunk_input_pts(video_start: u32, audio_start: u32, span_ms: u32) -> (Vec<u32>, Vec<u32>) {
        let video: Vec<u32> = (0..=span_ms / 33).map(|i| video_start + i * 33).collect();
        let audio: Vec<u32> = (0..=span_ms / 21).map(|i| audio_start + i * 21).collect();
        (video, audio)
    }

    /// Issue #257 detection RED→GREEN. Two synthetic chunks where audio lags
    /// video by 25,500 ms (the incident skew). The content-PTS skew metric
    /// reproduces that 25,500 ms wire skew; the guard MUST trip recovery once
    /// the debounce is satisfied.
    ///
    /// RED (no guard wired): `evaluate_chunk` returns `Continue` forever — the
    /// pusher propagates the desync silently, exactly the 2026-06-19 behavior.
    /// GREEN: after `SKEW_DEBOUNCE_CHUNKS` consecutive over-threshold chunks
    /// the tracker returns `TripRecovery`.
    #[test]
    fn audio_lagging_video_by_25500ms_trips_recovery_after_debounce() {
        let mut tracker = SkewTracker::default();
        // Video runs from PTS 0; audio runs 25_500 ms BEHIND (its content for
        // the same wall instant is stamped 25_500 ms earlier).
        const LAG_MS: u32 = 25_500;

        let mut last_decision = SkewDecision::Continue;
        // Feed several chunks. Video PTS keeps climbing; audio PTS climbs the
        // same way but starts 25_500 ms behind, so the per-track progress
        // delta stays ~25_500 ms every chunk.
        for chunk in 0..SKEW_DEBOUNCE_CHUNKS as u32 {
            let video_start = chunk * 2_000;
            // audio_start mirrors video_start but the audio ORIGIN was pinned
            // 25_500 ms earlier, so its progress lags by LAG_MS.
            let audio_start = chunk * 2_000;
            let (video, audio) = chunk_input_pts(video_start + LAG_MS, audio_start, 2_000);
            for v in &video {
                tracker.observe_video(*v);
            }
            for a in &audio {
                tracker.observe_audio(*a);
            }
            // The content-PTS skew the tracker computes must reproduce the
            // 25,500 ms wire skew (within one inter-frame interval).
            let skew = tracker.current_skew_ms();
            assert!(
                (skew - LAG_MS as i64).abs() <= 66,
                "content-PTS skew must reproduce the {LAG_MS} ms incident skew, got {skew}"
            );
            last_decision = tracker.evaluate_chunk((chunk as u64 + 1) * 2_000);
        }

        assert_eq!(
            last_decision,
            SkewDecision::TripRecovery,
            "sustained 25,500 ms audio-behind-video skew MUST trip bounded recovery \
             after {SKEW_DEBOUNCE_CHUNKS} consecutive over-threshold chunks"
        );
        assert!(
            tracker.last_skew_ms().abs() > MAX_AV_SKEW_MS,
            "last_skew_ms must record the over-threshold value for telemetry"
        );
        assert_eq!(tracker.trip_count(), 1, "exactly one recovery tripped");
    }

    /// A healthy shared-epoch source (audio and video advance together) must
    /// NEVER trip — the guard is silent in steady state.
    #[test]
    fn aligned_av_never_trips() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..10u32 {
            let start = chunk * 2_000;
            let (video, audio) = chunk_input_pts(start, start, 2_000);
            for v in &video {
                tracker.observe_video(*v);
            }
            for a in &audio {
                tracker.observe_audio(*a);
            }
            let skew = tracker.current_skew_ms();
            assert!(
                skew.abs() <= MAX_AV_SKEW_MS,
                "aligned A/V must stay under threshold, got {skew}"
            );
            assert_eq!(
                tracker.evaluate_chunk((chunk as u64 + 1) * 2_000),
                SkewDecision::Continue,
                "aligned A/V must never trip recovery"
            );
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// A transient single-chunk over-threshold spike must NOT trip — only a
    /// skew sustained across the debounce window does.
    #[test]
    fn single_chunk_spike_does_not_trip_below_debounce() {
        let mut tracker = SkewTracker::default();
        // One over-threshold chunk only.
        tracker.observe_video(10_000);
        tracker.observe_audio(0);
        assert_eq!(
            tracker.evaluate_chunk(2_000),
            SkewDecision::Continue,
            "a single over-threshold chunk must not trip (debounce = {SKEW_DEBOUNCE_CHUNKS})"
        );
    }

    /// Reset clears both tracks AND the debounce so skew is re-measured from a
    /// fresh common origin after a reconnect / symmetric re-anchor.
    #[test]
    fn reset_tracks_clears_progress_and_debounce() {
        let mut tracker = SkewTracker::default();
        tracker.observe_video(30_000);
        tracker.observe_audio(0);
        let _ = tracker.evaluate_chunk(1_000);
        assert!(tracker.current_skew_ms() > MAX_AV_SKEW_MS);
        tracker.reset_tracks();
        assert_eq!(
            tracker.current_skew_ms(),
            0,
            "reset must clear both tracks' progress to a fresh common origin"
        );
    }

    /// Recovery is rate-limited: a skew that survives the reconnect must NOT
    /// thrash. After one trip, a second trip within
    /// `SKEW_RECOVERY_MIN_INTERVAL_MS` is suppressed.
    #[test]
    fn recovery_is_rate_limited_to_avoid_reconnect_thrash() {
        let mut tracker = SkewTracker::default();
        // Drive a persistent over-threshold skew to the first trip.
        for chunk in 0..SKEW_DEBOUNCE_CHUNKS as u32 {
            tracker.observe_video(30_000 + chunk * 2_000);
            tracker.observe_audio(chunk * 2_000);
            let _ = tracker.evaluate_chunk((chunk as u64 + 1) * 1_000);
        }
        assert_eq!(tracker.trip_count(), 1, "first sustained skew trips once");

        // Immediately after the trip the tracker would normally reset on the
        // reconnect, but if the skew SURVIVES (reset not called) a second
        // over-threshold window within the min interval must be suppressed.
        for chunk in SKEW_DEBOUNCE_CHUNKS as u32..(2 * SKEW_DEBOUNCE_CHUNKS as u32) {
            tracker.observe_video(30_000 + chunk * 2_000);
            tracker.observe_audio(chunk * 2_000);
            let decision = tracker.evaluate_chunk((chunk as u64 + 1) * 1_000);
            assert_eq!(
                decision,
                SkewDecision::Continue,
                "a second trip within the min recovery interval must be rate-limited"
            );
        }
        assert_eq!(
            tracker.trip_count(),
            1,
            "rate limit keeps trip_count at 1 within the min interval"
        );
    }
}
