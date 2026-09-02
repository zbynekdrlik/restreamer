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

/// Issue #359 — a CONTINUOUS cross-track drift (audio xiu-ts and video
/// wall-clock advancing at slightly DIFFERENT RATES on our path) must NOT
/// death-loop the push. On the pre-fix guard every `AvSkewExceeded`
/// reconnect calls `reset_tracks()`, re-zeroes the baseline, and the SAME
/// drift re-accumulates past `MAX_AV_SKEW_MS` and trips again — forever
/// (7 kills / run in the #359 evidence, skew alternating sign
/// +7308/-8088/+8622/-10128 and GROWING). A reconnect cannot fix a rate
/// drift. The guard must CONVERGE: after detecting the non-converging
/// chain it stops thrashing (bounded trips), while a genuine STEP desync
/// still trips (proved separately).
///
/// This models the exact failure signature: a ~50 ms/s drift that crosses
/// the 4000 ms threshold in ~80 s (past the 60 s trip floor), then FLIPS
/// SIGN on each simulated reconnect (the alternating-sign signature). Over
/// 1200 s the pre-fix guard trips ~14 times; the fix bounds it.
///
/// RED (pre-fix): ~14 trips → the `<= 3` assert fails.
/// GREEN (post-fix): the non-convergence hold caps it at 2.
#[test]
fn continuous_drift_does_not_death_loop() {
    let mut tracker = SkewTracker::default();
    let chunk_ms: u64 = 2_000;
    let drift_per_chunk: i64 = 100; // 50 ms/s
    let mut now: u64 = 0;
    let mut base: i64 = 0; // shared-epoch base for this connection
    let mut i: i64 = 0; // chunk index within the current connection
    let mut sign: i64 = 1; // audio-behind (+) then flips on each reconnect
    let mut trips: u32 = 0;

    for _ in 0..600 {
        now += chunk_ms;
        let video_start = base + i * chunk_ms as i64;
        // audio lags (sign +1) or leads (sign -1) by i*drift, growing each
        // chunk — a continuous rate drift a reconnect cannot fix.
        let audio_start = video_start - sign * i * drift_per_chunk;
        let d = feed_chunk(&mut tracker, video_start as u32, audio_start as u32, now);
        if d == SkewDecision::TripRecovery {
            trips += 1;
            // Simulate the pusher's reconnect: reset_tracks re-anchors from
            // a fresh common epoch. Input PTS keep running (the chunker is
            // continuous across OUR reconnect); flip the drift sign to
            // reproduce the alternating-sign signature.
            tracker.reset_tracks();
            base = video_start + chunk_ms as i64;
            sign = -sign;
            i = 0;
        } else {
            i += 1;
        }
    }

    assert!(
        trips <= 3,
        "a continuous cross-clock drift must converge to a bounded number of \
         recovery trips, not death-loop; got {trips} trips over 1200 s"
    );
}

/// Drive a continuous drift across simulated reconnects (the #359 death-loop
/// shape): the per-chunk skew grows by `drift_per_chunk` and the sign flips
/// on each reconnect (alternating-sign signature). Returns `(now, skew)` for
/// every issued `TripRecovery`.
fn drive_drift(t: &mut SkewTracker, drift_per_chunk: i64, total_chunks: u32) -> Vec<(u64, i64)> {
    let chunk_ms: u64 = 2_000;
    let mut now: u64 = 0;
    let mut base: i64 = 0;
    let mut i: i64 = 0;
    let mut sign: i64 = 1;
    let mut trips = Vec::new();
    for _ in 0..total_chunks {
        now += chunk_ms;
        let video_start = base + i * chunk_ms as i64;
        let audio_start = video_start - sign * i * drift_per_chunk;
        if feed_chunk(t, video_start as u32, audio_start as u32, now) == SkewDecision::TripRecovery
        {
            trips.push((now, t.last_skew_ms()));
            t.reset_tracks();
            base = video_start + chunk_ms as i64;
            sign = -sign;
            i = 0;
        } else {
            i += 1;
        }
    }
    trips
}

/// Issue #359 GREEN — CONVERGENCE. The same continuous drift that
/// death-looped (13 trips) is now bounded: the guard trips once in Normal,
/// detects the chained non-convergence on the 2nd would-be trip, engages
/// `DriftHold`, and thereafter only the hard cap trips (rate-limited). The
/// surviving trip is a hard-cap (≥ 12 000 ms), spaced by the hold interval.
#[test]
fn continuous_drift_engages_hold_and_bounds_reconnects() {
    let mut t = SkewTracker::default();
    let trips = drive_drift(&mut t, 100, 600); // 50 ms/s over 1200 s
    assert_eq!(
        t.mode(),
        GuardMode::DriftHold,
        "a non-converging drift must engage the hold"
    );
    assert!(
        t.trip_count() <= 2,
        "the hold must bound reconnects (death-loop was 13); got {}",
        t.trip_count()
    );
    assert_eq!(trips.len() as u32, t.trip_count());
    if trips.len() == 2 {
        assert!(
            trips[1].1.abs() >= SKEW_HOLD_MAX_MS,
            "the hold-mode trip must be a hard-cap (>= {SKEW_HOLD_MAX_MS} ms), got {}",
            trips[1].1
        );
        assert!(
            trips[1].0 - trips[0].0 >= SKEW_HOLD_MIN_INTERVAL_MS,
            "hard-cap trips are rate-limited to the hold interval"
        );
    }
    // A slower drift ALSO converges (not rate-specific): it still engages the
    // hold and stays bounded (one Normal trip + at most one hard-cap within the
    // window), never the death-loop.
    let mut t2 = SkewTracker::default();
    let trips2 = drive_drift(&mut t2, 30, 600); // 15 ms/s
    assert_eq!(
        t2.mode(),
        GuardMode::DriftHold,
        "a slower non-converging drift must ALSO engage the hold"
    );
    assert!(
        trips2.len() <= 2,
        "a slower drift stays bounded (Normal trip + at most one hard cap); got {}",
        trips2.len()
    );
}

/// Issue #359 GREEN — a genuine STEP still trips exactly once, the reconnect
/// fixes it (converges), the streak decays, and full sensitivity returns.
/// The hold must NOT engage for a STEP (it converges in one reconnect).
#[test]
fn genuine_step_trips_once_recovers_and_decays() {
    let mut t = SkewTracker::default();
    let chunk_ms = 2_000u64;
    let mut now = 0u64;
    // Constant 1500 ms offset for 30 chunks: folds into baseline, 0 trips.
    for i in 0..30u32 {
        let v = i * chunk_ms as u32;
        now += chunk_ms;
        assert_eq!(
            feed_chunk(&mut t, v + 1_500, v, now),
            SkewDecision::Continue
        );
    }
    assert_eq!(t.trip_count(), 0);
    // A single-chunk STEP: video leaps 25 500 ms ahead of audio.
    let mut trip_now = 0u64;
    for i in 30..40u32 {
        let v = i * chunk_ms as u32 + 25_500;
        let a = i * chunk_ms as u32;
        now += chunk_ms;
        if feed_chunk(&mut t, v, a, now) == SkewDecision::TripRecovery {
            trip_now = now;
            break;
        }
    }
    assert_eq!(t.trip_count(), 1, "a genuine STEP must trip once");
    assert_eq!(
        t.mode(),
        GuardMode::Normal,
        "one STEP reconnect must NOT engage the hold"
    );
    assert_eq!(t.loop_streak(), 1);
    // The reconnect fixes the STEP: aligned from here. No re-trip; after the
    // loop window the streak decays back to 0 (full sensitivity restored).
    t.reset_tracks();
    let mut n = trip_now;
    for i in 0..400u32 {
        let base = 100_000 + i * chunk_ms as u32;
        n += chunk_ms;
        assert_eq!(feed_chunk(&mut t, base, base, n), SkewDecision::Continue);
    }
    assert_eq!(t.trip_count(), 1, "a converged STEP must not re-trip");
    assert_eq!(
        t.loop_streak(),
        0,
        "the streak decays after the loop window"
    );
    assert_eq!(t.mode(), GuardMode::Normal);
}
