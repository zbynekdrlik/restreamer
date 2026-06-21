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
    /// Largest AUDIO input ts (relative to `shared_origin`) seen, or `None`
    /// until the first audio tag. MUST allow NEGATIVE values: when the shared
    /// origin is pinned by the OTHER track's first tag, this track's relative
    /// position can be negative (it started before the shared origin). The
    /// first observed value SEEDS the max — a default of 0 would wrongly clamp
    /// a genuinely-negative position to 0 and silently zero the inter-track
    /// offset (the bug that made `raw_skew_ms` read 0 for a real offset).
    audio_max_abs: Option<i64>,
    /// Largest VIDEO input ts (relative to `shared_origin`) seen, or `None`
    /// until the first video tag. Allows negatives for the same reason as
    /// `audio_max_abs`.
    video_max_abs: Option<i64>,
    /// Whether at least one AUDIO tag has been observed this session.
    audio_seen: bool,
    /// Whether at least one VIDEO tag has been observed this session.
    video_seen: bool,
    /// Consecutive chunks whose end-of-chunk `|av_skew_ms|` exceeded the
    /// threshold. Reset to 0 the moment a chunk comes back under threshold.
    consecutive_over: u32,
    /// Steady-state A/V offset captured on the first chunk where BOTH tracks
    /// are present. The skew that matters for recovery is the DEVIATION from
    /// this baseline, not the absolute offset: the chunker's audio (xiu-ts) and
    /// video (wall-clock) live in different time domains whose per-chunk RATE
    /// matches but whose absolute zero points can differ by a benign,
    /// CONSTANT startup gap (device/encoder init lag, silent pre-roll —
    /// `feedback_chunker_time_domains`). A guard on the ABSOLUTE offset would
    /// false-trip and kill a working stream's session on that benign gap. The
    /// 2026-06-19 incident skew, by contrast, APPEARED mid-stream (grew by
    /// ~25.5 s relative to a near-zero baseline on an OBS republish / reconnect)
    /// — a CHANGE, which is exactly what the baseline-relative metric detects.
    /// `None` until both tracks seen; cleared on `reset_tracks`.
    baseline_skew_ms: Option<i64>,
    /// Last computed baseline-relative skew (deviation from `baseline_skew_ms`),
    /// surfaced to telemetry. 0 until both tracks seen / baseline captured.
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
        // Seed on the first tag (even if negative), then keep the running max.
        self.audio_max_abs = Some(match self.audio_max_abs {
            Some(cur) => cur.max(rel),
            None => rel,
        });
    }

    /// Observe one input VIDEO tag timestamp (pre-re-anchor, content PTS).
    pub fn observe_video(&mut self, input_ts: u32) {
        self.video_seen = true;
        let rel = self.rel(input_ts);
        // Seed on the first tag (even if negative), then keep the running max.
        self.video_max_abs = Some(match self.video_max_abs {
            Some(cur) => cur.max(rel),
            None => rel,
        });
    }

    /// Reset BOTH tracks' progress on a fresh RTMP session / symmetric
    /// re-anchor so skew is measured from the new common origin. The debounce
    /// counter is also cleared — a re-anchor establishes a clean baseline.
    pub fn reset_tracks(&mut self) {
        self.shared_origin = None;
        self.audio_max_abs = None;
        self.video_max_abs = None;
        self.audio_seen = false;
        self.video_seen = false;
        self.consecutive_over = 0;
        // The baseline must re-establish from the post-reconnect / post-reanchor
        // first chunk — a stale baseline that survived the re-anchor would
        // measure deviation against the OLD steady state.
        self.baseline_skew_ms = None;
    }

    /// `true` once BOTH tracks have produced at least one tag this session.
    /// An A/V skew is only meaningful when both tracks are present — an
    /// audio-only or video-only stream has nothing to compare and must never
    /// trip (the other track's `max_abs` stays 0 and would otherwise read as a
    /// huge spurious skew once the present track advances past the threshold).
    fn both_tracks_seen(&self) -> bool {
        self.audio_seen && self.video_seen
    }

    /// Raw signed content-PTS offset on the shared epoch:
    /// `video_max_abs − audio_max_abs`. Positive means audio is BEHIND video
    /// (the 2026-06-19 incident direction). This is the ABSOLUTE offset; the
    /// guard and telemetry use the baseline-RELATIVE deviation
    /// (`current_skew_ms`) instead, so a benign constant startup domain gap
    /// doesn't read as a desync.
    pub fn raw_skew_ms(&self) -> i64 {
        // A track not yet seen contributes 0 (no position). Once seen, use its
        // true relative max (which may be negative when the shared origin was
        // pinned by the other track). The difference is the absolute
        // content-PTS offset between the two tracks on the shared epoch.
        self.video_max_abs.unwrap_or(0) - self.audio_max_abs.unwrap_or(0)
    }

    /// Baseline-relative content-PTS skew: the DEVIATION of the current raw
    /// offset from the steady-state baseline. 0 until both tracks are seen and
    /// the baseline is captured. This is what the guard trips on and what
    /// telemetry surfaces — a benign constant domain offset folds into the
    /// baseline and reads ~0; only a desync that APPEARS mid-stream (the
    /// incident signature) produces a non-zero deviation.
    pub fn current_skew_ms(&self) -> i64 {
        match self.baseline_skew_ms {
            Some(baseline) => self.raw_skew_ms() - baseline,
            None => 0,
        }
    }

    /// Last baseline-relative skew computed at a `chunk_done` boundary
    /// (telemetry surface). 0 until both tracks seen / baseline captured.
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
        // Capture the steady-state baseline on the first chunk where BOTH
        // tracks are present. Any benign constant domain offset present from
        // session start folds into this baseline, so the guard measures only
        // SUBSEQUENT deviation (the desync-appeared signature).
        if self.both_tracks_seen() && self.baseline_skew_ms.is_none() {
            self.baseline_skew_ms = Some(self.raw_skew_ms());
        }
        let skew = self.current_skew_ms();
        self.last_skew_ms = skew;
        // Only a stream with BOTH tracks present can have a meaningful A/V
        // skew. For a one-track stream the baseline is never captured and
        // current_skew_ms() returns 0, so the debounce stays at 0 and it can
        // never trip.
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

    /// Build the per-track input-PTS sequences for ONE chunk, given each
    /// track's start ts on the chunker's shared epoch. Video frames advance at
    /// ~33 ms (30 fps), audio at ~21 ms (AAC). The pusher's per-track output
    /// re-anchor would mask any offset on the wire, but the INPUT PTS deltas
    /// carry the true content skew.
    fn chunk_input_pts(video_start: u32, audio_start: u32, span_ms: u32) -> (Vec<u32>, Vec<u32>) {
        let video: Vec<u32> = (0..=span_ms / 33).map(|i| video_start + i * 33).collect();
        let audio: Vec<u32> = (0..=span_ms / 21).map(|i| audio_start + i * 21).collect();
        (video, audio)
    }

    /// Feed one chunk (video_start/audio_start on the shared epoch) and return
    /// the decision at its boundary.
    fn feed_chunk(
        t: &mut SkewTracker,
        video_start: u32,
        audio_start: u32,
        now_ms: u64,
    ) -> SkewDecision {
        let (video, audio) = chunk_input_pts(video_start, audio_start, 2_000);
        for v in &video {
            t.observe_video(*v);
        }
        for a in &audio {
            t.observe_audio(*a);
        }
        t.evaluate_chunk(now_ms)
    }

    /// Issue #257 detection RED→GREEN. The 2026-06-19 incident desync APPEARED
    /// mid-stream (audio fell ~25.5 s behind video on an OBS republish /
    /// reconnect), it was NOT present from t=0. So the stream starts ALIGNED
    /// (baseline ~0), then audio falls 25,500 ms behind — the baseline-relative
    /// skew jumps to ~25,500 ms and MUST trip recovery after the debounce.
    ///
    /// RED (no guard wired): `evaluate_chunk` returns `Continue` forever — the
    /// pusher propagates the desync silently. GREEN: after `SKEW_DEBOUNCE_CHUNKS`
    /// consecutive over-threshold chunks the tracker returns `TripRecovery`.
    #[test]
    fn audio_falling_behind_video_mid_stream_trips_recovery_after_debounce() {
        let mut tracker = SkewTracker::default();
        const LAG_MS: u32 = 25_500;

        // Phase 1: a few ALIGNED chunks establish a near-zero baseline.
        for chunk in 0..3u32 {
            let start = chunk * 2_000;
            assert_eq!(
                feed_chunk(&mut tracker, start, start, (chunk as u64 + 1) * 2_000),
                SkewDecision::Continue,
                "aligned warm-up must not trip"
            );
        }
        assert!(
            tracker.current_skew_ms().abs() <= 66,
            "baseline-relative skew must be ~0 while aligned, got {}",
            tracker.current_skew_ms()
        );

        // Phase 2: a republish makes video's epoch leap 25,500 ms AHEAD of
        // audio's (equivalently, audio falls 25,500 ms behind video). Audio
        // keeps advancing normally; video's PTS now runs LAG_MS ahead, so
        // video_max_abs pulls away from audio_max_abs by ~LAG_MS each chunk.
        // (Modelled as a video lead so both tracks' monotonic max keeps
        // growing — the GAP, not a downward step, is what the metric detects.)
        let mut last = SkewDecision::Continue;
        for chunk in 3..(3 + SKEW_DEBOUNCE_CHUNKS) {
            let video_start = chunk * 2_000 + LAG_MS;
            let audio_start = chunk * 2_000;
            last = feed_chunk(
                &mut tracker,
                video_start,
                audio_start,
                (chunk as u64 + 1) * 2_000,
            );
            let skew = tracker.current_skew_ms();
            assert!(
                (skew - LAG_MS as i64).abs() <= 66,
                "baseline-relative skew must reproduce the {LAG_MS} ms desync, got {skew}"
            );
        }

        assert_eq!(
            last,
            SkewDecision::TripRecovery,
            "a desync that APPEARS mid-stream and persists must trip bounded \
             recovery after {SKEW_DEBOUNCE_CHUNKS} consecutive over-threshold chunks"
        );
        assert!(
            tracker.last_skew_ms().abs() > MAX_AV_SKEW_MS,
            "last_skew_ms must record the over-threshold deviation for telemetry"
        );
        assert_eq!(tracker.trip_count(), 1, "exactly one recovery tripped");
    }

    /// THE false-positive guard (#257 review 🟡): a benign CONSTANT A/V domain
    /// offset present from session start (audio xiu-ts vs video wall-clock have
    /// different absolute zero points — startup/device init lag) must NEVER
    /// trip. The constant offset folds into the baseline; only a CHANGE trips.
    #[test]
    fn constant_startup_domain_offset_never_trips() {
        let mut tracker = SkewTracker::default();
        // Video's epoch sits 20,000 ms (>> threshold) AHEAD of audio's from the
        // very first chunk (a benign constant domain gap) and stays exactly
        // there for the whole stream. Audio runs from 0; video from CONST_OFFSET.
        const CONST_OFFSET: u32 = 20_000;
        for chunk in 0..15u32 {
            let video_start = chunk * 2_000 + CONST_OFFSET;
            let audio_start = chunk * 2_000;
            assert_eq!(
                feed_chunk(
                    &mut tracker,
                    video_start,
                    audio_start,
                    (chunk as u64 + 1) * 2_000
                ),
                SkewDecision::Continue,
                "a CONSTANT startup domain offset is benign and must never trip \
                 (it folds into the baseline)"
            );
            assert!(
                tracker.current_skew_ms().abs() <= 66,
                "baseline-relative skew must stay ~0 for a constant offset, got {}",
                tracker.current_skew_ms()
            );
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// A healthy shared-epoch source (audio and video advance together from 0)
    /// must NEVER trip — the guard is silent in steady state.
    #[test]
    fn aligned_av_never_trips() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..10u32 {
            let start = chunk * 2_000;
            assert_eq!(
                feed_chunk(&mut tracker, start, start, (chunk as u64 + 1) * 2_000),
                SkewDecision::Continue,
                "aligned A/V must never trip recovery"
            );
            assert!(tracker.current_skew_ms().abs() <= MAX_AV_SKEW_MS);
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// A transient single-chunk over-threshold deviation must NOT trip — only a
    /// deviation sustained across the debounce window does.
    #[test]
    fn single_chunk_spike_does_not_trip_below_debounce() {
        let mut tracker = SkewTracker::default();
        // Chunk 0 aligned → baseline ~0.
        assert_eq!(
            feed_chunk(&mut tracker, 0, 0, 2_000),
            SkewDecision::Continue
        );
        // Chunk 1: one over-threshold deviation only.
        assert_eq!(
            feed_chunk(&mut tracker, 12_000, 2_000, 4_000),
            SkewDecision::Continue,
            "a single over-threshold chunk must not trip (debounce = {SKEW_DEBOUNCE_CHUNKS})"
        );
    }

    /// Reset clears both tracks, the debounce, AND the baseline so skew is
    /// re-measured from a fresh common origin after a reconnect / symmetric
    /// re-anchor.
    #[test]
    fn reset_tracks_clears_progress_baseline_and_debounce() {
        let mut tracker = SkewTracker::default();
        // Establish a baseline, then deviate.
        feed_chunk(&mut tracker, 0, 0, 2_000);
        feed_chunk(&mut tracker, 30_000, 2_000, 4_000);
        assert!(tracker.current_skew_ms().abs() > MAX_AV_SKEW_MS);
        tracker.reset_tracks();
        assert_eq!(
            tracker.raw_skew_ms(),
            0,
            "reset must clear both tracks' progress"
        );
        assert_eq!(
            tracker.current_skew_ms(),
            0,
            "reset must clear the baseline so deviation re-measures from 0"
        );
    }

    /// Recovery is rate-limited: a deviation that survives the reconnect must
    /// NOT thrash. After one trip, a second trip within
    /// `SKEW_RECOVERY_MIN_INTERVAL_MS` is suppressed.
    #[test]
    fn recovery_is_rate_limited_to_avoid_reconnect_thrash() {
        let mut tracker = SkewTracker::default();
        // Aligned baseline.
        feed_chunk(&mut tracker, 0, 0, 1_000);
        // Sustained deviation → first trip.
        let mut tripped_at = 0u32;
        for chunk in 1.. {
            let d = feed_chunk(
                &mut tracker,
                30_000 + chunk * 2_000,
                chunk * 2_000,
                (chunk as u64 + 1) * 1_000,
            );
            if d == SkewDecision::TripRecovery {
                tripped_at = chunk;
                break;
            }
            assert!(chunk < 10, "should have tripped by now");
        }
        assert_eq!(
            tracker.trip_count(),
            1,
            "first sustained deviation trips once"
        );

        // A second over-threshold window within the min interval is suppressed.
        for chunk in (tripped_at + 1)..(tripped_at + 1 + 2 * SKEW_DEBOUNCE_CHUNKS) {
            let d = feed_chunk(
                &mut tracker,
                30_000 + chunk * 2_000,
                chunk * 2_000,
                (chunk as u64 + 1) * 1_000,
            );
            assert_eq!(
                d,
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

    /// After the min recovery interval has elapsed, a still-present deviation is
    /// allowed to trip again (the rate limit is a floor, not a permanent
    /// silence).
    #[test]
    fn recovery_allowed_again_after_min_interval() {
        let mut tracker = SkewTracker::default();
        feed_chunk(&mut tracker, 0, 0, 1_000);
        let mut tripped_at = 0u32;
        for chunk in 1.. {
            if feed_chunk(&mut tracker, 30_000 + chunk * 2_000, chunk * 2_000, 1_000)
                == SkewDecision::TripRecovery
            {
                tripped_at = chunk;
                break;
            }
            assert!(chunk < 10);
        }
        assert_eq!(tracker.trip_count(), 1);

        // Re-accumulate the debounce, now PAST the min interval.
        let base = SKEW_RECOVERY_MIN_INTERVAL_MS + 10_000;
        let mut tripped_again = false;
        for chunk in (tripped_at + 1)..(tripped_at + 1 + 2 * SKEW_DEBOUNCE_CHUNKS) {
            if feed_chunk(&mut tracker, 30_000 + chunk * 2_000, chunk * 2_000, base)
                == SkewDecision::TripRecovery
            {
                tripped_again = true;
            }
        }
        assert!(
            tripped_again,
            "a still-present deviation may trip again once the min interval has passed"
        );
        assert_eq!(tracker.trip_count(), 2);
    }

    /// An audio-ONLY stream (no video tags) must NEVER trip — the baseline is
    /// never captured (both_tracks_seen false), so current_skew_ms() stays 0.
    #[test]
    fn audio_only_stream_never_trips() {
        let mut tracker = SkewTracker::default();
        for chunk in 0..10u32 {
            tracker.observe_audio(chunk * 10_000);
            assert_eq!(
                tracker.evaluate_chunk((chunk as u64 + 1) * 1_000),
                SkewDecision::Continue,
                "audio-only stream has no A/V skew and must never trip"
            );
            assert_eq!(
                tracker.current_skew_ms(),
                0,
                "one-track skew must read 0 (no baseline) for telemetry sanity"
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
            assert_eq!(tracker.current_skew_ms(), 0);
        }
        assert_eq!(tracker.trip_count(), 0);
    }

    /// Sign convention on the baseline-relative deviation: audio falling behind
    /// video (vs baseline) reads POSITIVE; video falling behind reads NEGATIVE.
    #[test]
    fn deviation_sign_audio_behind_is_positive_video_behind_is_negative() {
        // Establish aligned baseline, then audio falls behind → positive.
        let mut t = SkewTracker::default();
        t.observe_video(0);
        t.observe_audio(0);
        t.evaluate_chunk(1_000); // baseline = 0
        t.observe_video(10_000);
        t.observe_audio(0);
        t.evaluate_chunk(2_000);
        assert!(
            t.current_skew_ms() > 0,
            "audio falling behind video must read positive, got {}",
            t.current_skew_ms()
        );

        // Aligned baseline, then video falls behind → negative.
        let mut t2 = SkewTracker::default();
        t2.observe_video(0);
        t2.observe_audio(0);
        t2.evaluate_chunk(1_000); // baseline = 0
        t2.observe_audio(10_000);
        t2.observe_video(0);
        t2.evaluate_chunk(2_000);
        assert!(
            t2.current_skew_ms() < 0,
            "video falling behind audio must read negative, got {}",
            t2.current_skew_ms()
        );
    }

    /// The RAW absolute offset is still exposed for diagnostics even though the
    /// guard uses the baseline-relative deviation.
    #[test]
    fn raw_skew_exposes_absolute_offset() {
        let mut t = SkewTracker::default();
        t.observe_video(10_000);
        t.observe_audio(0);
        assert_eq!(
            t.raw_skew_ms(),
            10_000,
            "raw_skew_ms is the absolute video-minus-audio offset"
        );
        // current_skew_ms is 0 until a baseline exists.
        assert_eq!(t.current_skew_ms(), 0);
    }
}
