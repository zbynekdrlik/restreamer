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

/// Cross-track skew tracker. Measures the content-PTS skew between audio and
/// video using a SINGLE shared origin (post-#255 the chunker stamps both tracks
/// on one shared epoch, so their input PTS live on the same clock — content
/// that is coincident has equal audio and video PTS). Exposes the signed
/// `av_skew_ms` (positive = audio behind video, the incident direction),
/// debounces over consecutive chunks, and rate-limits recovery.
///
/// Why a SHARED origin, not per-track origins: per-track origins each cancel
/// their own track's first-tag value, which would also cancel the very
/// inter-track offset we are trying to detect (audio's first tag landing
/// 25.5 s behind video's would just become each track's "0"). Anchoring BOTH
/// tracks to one shared origin keeps the offset visible: `video_max_abs −
/// audio_max_abs` is the live content skew on the shared epoch.
#[derive(Default)]
pub struct SkewTracker {
    /// First input timestamp seen on EITHER track this session — the shared
    /// epoch reference. Both tracks measure absolute progress from here.
    shared_origin: Option<u32>,
    /// Largest absolute AUDIO input ts (relative to `shared_origin`) seen.
    audio_max_abs: i64,
    /// Largest absolute VIDEO input ts (relative to `shared_origin`) seen.
    video_max_abs: i64,
    /// Whether at least one AUDIO tag has been observed this session.
    audio_seen: bool,
    /// Whether at least one VIDEO tag has been observed this session.
    video_seen: bool,
    /// Consecutive chunks whose end-of-chunk `|av_skew_ms|` exceeded the
    /// threshold. Reset to 0 the moment a chunk comes back under threshold.
    consecutive_over: u32,
    /// Last computed signed skew (video_progress − audio_progress), surfaced
    /// to telemetry.
    last_skew_ms: i64,
    /// Number of times this tracker has tripped recovery (for telemetry /
    /// alerting + the reconnect-thrash rate limit).
    trip_count: u32,
    /// Monotonic wall-clock (ms, from the pusher's pacing anchor) of the most
    /// recent trip. `None` until the first trip. Gates the
    /// `SKEW_RECOVERY_MIN_INTERVAL_MS` rate limit so a skew that survives the
    /// reconnect can't thrash the session.
    last_trip_ms: Option<u64>,
}

impl SkewTracker {
    /// Pin the shared epoch on the first input tag of EITHER track, then return
    /// the input ts relative to it.
    fn rel(&mut self, input_ts: u32) -> i64 {
        let origin = *self.shared_origin.get_or_insert(input_ts);
        (input_ts as i64) - (origin as i64)
    }

    /// Observe one input AUDIO tag timestamp (pre-re-anchor, content PTS).
    pub fn observe_audio(&mut self, input_ts: u32) {
        self.audio_seen = true;
        let rel = self.rel(input_ts);
        if rel > self.audio_max_abs {
            self.audio_max_abs = rel;
        }
    }

    /// Observe one input VIDEO tag timestamp (pre-re-anchor, content PTS).
    pub fn observe_video(&mut self, input_ts: u32) {
        self.video_seen = true;
        let rel = self.rel(input_ts);
        if rel > self.video_max_abs {
            self.video_max_abs = rel;
        }
    }

    /// Reset BOTH tracks' progress on a fresh RTMP session / symmetric
    /// re-anchor so skew is measured from the new common origin. The debounce
    /// counter is also cleared — a re-anchor establishes a clean baseline.
    pub fn reset_tracks(&mut self) {
        self.shared_origin = None;
        self.audio_max_abs = 0;
        self.video_max_abs = 0;
        self.audio_seen = false;
        self.video_seen = false;
        self.consecutive_over = 0;
    }

    /// `true` once BOTH tracks have produced at least one tag this session.
    /// An A/V skew is only meaningful when both tracks are present — an
    /// audio-only or video-only stream has nothing to compare and must never
    /// trip (the other track's `max_abs` stays 0 and would otherwise read as a
    /// huge spurious skew once the present track advances past the threshold).
    fn both_tracks_seen(&self) -> bool {
        self.audio_seen && self.video_seen
    }

    /// Signed content-PTS skew on the shared epoch:
    /// `video_max_abs − audio_max_abs`. Positive means audio is BEHIND video
    /// (the 2026-06-19 incident direction).
    pub fn current_skew_ms(&self) -> i64 {
        self.video_max_abs - self.audio_max_abs
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
        // Only a stream with BOTH tracks present can have a meaningful A/V
        // skew. For a one-track stream, keep the debounce at 0 so it can never
        // trip (and surface skew=last_skew_ms for telemetry continuity).
        if self.both_tracks_seen() && skew.abs() > MAX_AV_SKEW_MS {
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
    /// Trips `TripRecovery` only when ALL hold:
    ///   1. the skew has exceeded `MAX_AV_SKEW_MS` for at least
    ///      `SKEW_DEBOUNCE_CHUNKS` consecutive chunks (rejects transients), AND
    ///   2. at least `SKEW_RECOVERY_MIN_INTERVAL_MS` of wall-clock has elapsed
    ///      since the previous trip (prevents reconnect thrash if a skew
    ///      survives the reconnect).
    ///
    /// On a trip the debounce counter is reset (the reconnect establishes a
    /// fresh baseline) and `last_trip_ms` is stamped for the rate limit.
    fn decide(&mut self, now_ms: u64) -> SkewDecision {
        if self.consecutive_over < SKEW_DEBOUNCE_CHUNKS {
            return SkewDecision::Continue;
        }
        // Rate limit: suppress a second trip within the min interval.
        if let Some(prev) = self.last_trip_ms
            && now_ms.saturating_sub(prev) < SKEW_RECOVERY_MIN_INTERVAL_MS
        {
            return SkewDecision::Continue;
        }
        self.trip_count = self.trip_count.saturating_add(1);
        self.last_trip_ms = Some(now_ms);
        // Reset the debounce so the post-reconnect baseline must re-accumulate
        // before another trip (alongside the rate limit above).
        self.consecutive_over = 0;
        SkewDecision::TripRecovery
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

    /// After the min recovery interval has elapsed, a still-present skew is
    /// allowed to trip again (the rate limit is a floor, not a permanent
    /// silence).
    #[test]
    fn recovery_allowed_again_after_min_interval() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..SKEW_DEBOUNCE_CHUNKS as u32 {
            tracker.observe_video(30_000 + chunk * 2_000);
            tracker.observe_audio(chunk * 2_000);
            let _ = tracker.evaluate_chunk(1_000);
        }
        assert_eq!(tracker.trip_count(), 1);

        // Re-accumulate the debounce, now PAST the min interval.
        let base = SKEW_RECOVERY_MIN_INTERVAL_MS + 10_000;
        for chunk in SKEW_DEBOUNCE_CHUNKS as u32..(2 * SKEW_DEBOUNCE_CHUNKS as u32) {
            tracker.observe_video(30_000 + chunk * 2_000);
            tracker.observe_audio(chunk * 2_000);
            let _ = tracker.evaluate_chunk(base);
        }
        assert_eq!(
            tracker.trip_count(),
            2,
            "a still-present skew may trip again once the min interval has passed"
        );
    }

    /// An audio-ONLY stream (no video tags) must NEVER trip — there is no
    /// second track to compare against, so the growing audio progress must not
    /// read as a huge spurious skew.
    #[test]
    fn audio_only_stream_never_trips() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..10u32 {
            // Audio runs far past the threshold in absolute terms.
            tracker.observe_audio(chunk * 10_000);
            assert_eq!(
                tracker.evaluate_chunk((chunk as u64 + 1) * 1_000),
                SkewDecision::Continue,
                "audio-only stream has no A/V skew and must never trip"
            );
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// A video-ONLY stream must likewise never trip.
    #[test]
    fn video_only_stream_never_trips() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..10u32 {
            tracker.observe_video(chunk * 10_000);
            assert_eq!(
                tracker.evaluate_chunk((chunk as u64 + 1) * 1_000),
                SkewDecision::Continue,
                "video-only stream has no A/V skew and must never trip"
            );
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// The skew sign convention: audio behind video => POSITIVE skew. A
    /// reversed offset (video behind audio) is also detected (its absolute
    /// value crosses the threshold) but reads NEGATIVE for operator triage.
    #[test]
    fn skew_sign_audio_behind_is_positive_video_behind_is_negative() {
        // Audio behind video by 10 s.
        let mut t = SkewTracker::default();
        t.observe_video(10_000);
        t.observe_audio(0);
        assert!(
            t.current_skew_ms() > 0,
            "audio behind video must read positive, got {}",
            t.current_skew_ms()
        );

        // Video behind audio by 10 s.
        let mut t2 = SkewTracker::default();
        t2.observe_audio(10_000);
        t2.observe_video(0);
        assert!(
            t2.current_skew_ms() < 0,
            "video behind audio must read negative, got {}",
            t2.current_skew_ms()
        );
    }
}
