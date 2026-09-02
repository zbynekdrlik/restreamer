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

/// (#359) A per-chunk baseline-relative skew CHANGE ≥ this classifies a trip as
/// a STEP (a sudden jump — a republish freezing an offset, which a reconnect CAN
/// fix), else a DRIFT (a gradual cross-clock rate drift, which a reconnect canNOT
/// fix). Half the trip threshold: worst observed drift <100 ms/chunk (>20×
/// margin), while any step ≥4000 ms, even split across two chunks, exceeds it.
pub const SKEW_STEP_JUMP_MS: i64 = 2_000;

/// (#359) Window (ms) within which a fresh trip counts as CHAINED to the
/// previous — the death-loop signal (a converged STEP never re-trips; the #359
/// re-trips were 61–442 s apart). Also the `DriftHold` decay window. 10 min.
///
/// COVERED RATE RANGE: this bounds the hold to drifts whose time-to-threshold
/// (≈ `MAX_AV_SKEW_MS / rate`) is under this window — i.e. drift rates ≳ 6.7 ms/s
/// (`4000 ms / 600 s`), which fully covers the observed #359 signature
/// (~20–26 ms/s). A much slower drift (< ~6.7 ms/s) re-trips > 10 min apart, so
/// it is never chained and still reconnects every time-to-threshold (a far
/// slower loop, ≤ ~6/h, not the observed ~59/h) — a distinct, longer-baseline
/// drift-rate estimator is the proper fix, tracked as a follow-up.
pub const SKEW_LOOP_WINDOW_MS: u64 = 600_000;

/// (#359) Consecutive CHAINED DRIFT-class trips that force `DriftHold`. 2 = one
/// honest reconnect (absorbs a benign post-reconnect transient); a 2nd gradual
/// trip proves the reconnect is not the remedy.
pub const SKEW_LOOP_MAX_DRIFT: u32 = 2;

/// (#359) Consecutive CHAINED STEP-class trips that force `DriftHold`. 3 = a
/// mechanism-agnostic backstop (three jump-shaped trips in 10 min with no
/// convergence is a loop regardless of the classifier); > DRIFT limit so genuine
/// repeated republishes each still get their reconnect.
pub const SKEW_LOOP_MAX_STEP: u32 = 3;

/// (#359) `DriftHold` hard-cap threshold (ms), 3× `MAX_AV_SKEW_MS`. The guard is
/// NOT disabled in hold — a skew this large still trips (the "never never-kill"
/// floor: a runaway drift or a genuinely large step is always eventually killed).
pub const SKEW_HOLD_MAX_MS: i64 = 12_000;

/// (#359) Minimum wall-clock (ms) between `DriftHold` hard-cap trips — bounds
/// skew reconnects to ≤6/h under a persistent drift (vs the ~61 s loop). 10 min.
pub const SKEW_HOLD_MIN_INTERVAL_MS: u64 = 600_000;

/// (#359) Which recovery regime the guard is in. A reconnect fixes a STEP but
/// never a continuous DRIFT, so once a chain of reconnects fails to converge the
/// guard moves to `DriftHold` and stops thrashing.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum GuardMode {
    /// Full sensitivity: trip at `MAX_AV_SKEW_MS` / `SKEW_RECOVERY_MIN_INTERVAL_MS`.
    #[default]
    Normal,
    /// Non-convergence hold: only the `SKEW_HOLD_MAX_MS` hard cap trips, ≤1 per
    /// `SKEW_HOLD_MIN_INTERVAL_MS`. Exits to `Normal` after `SKEW_LOOP_WINDOW_MS`
    /// with no trip AND no over-threshold chunk (source healed).
    DriftHold,
}

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
    /// (#359) Previous chunk's `current_skew_ms`, for the per-chunk delta that
    /// classifies a trip STEP vs DRIFT. CLEARED on `reset_tracks` — each
    /// connection re-derives its own classification from a fresh baseline.
    prev_skew_ms: Option<i64>,
    /// (#359) Running max over THIS connection of `|current_skew − prev_skew|`
    /// (the largest single-chunk jump). `≥ SKEW_STEP_JUMP_MS` ⇒ STEP, else
    /// DRIFT. CLEARED on `reset_tracks`.
    max_abs_step_ms: i64,
    /// (#359) Number of consecutive CHAINED trips (each within
    /// `SKEW_LOOP_WINDOW_MS` of its predecessor) — the death-loop counter. MUST
    /// SURVIVE `reset_tracks`: a reconnect clears the tracks, so the loop is
    /// only ever visible ACROSS reconnects; clearing this here would make the
    /// non-convergence undetectable (the exact #359 bug).
    loop_streak: u32,
    /// (#359) Current recovery regime. PERSISTS across `reset_tracks`.
    mode: GuardMode,
    /// (#359) Last `now_ms` at which `|current_skew|` exceeded `MAX_AV_SKEW_MS`.
    /// Drives the `DriftHold` decay/exit — once the source heals it stops
    /// updating and, after `SKEW_LOOP_WINDOW_MS`, the guard returns to `Normal`.
    /// PERSISTS across `reset_tracks`.
    last_over_threshold_ms: Option<u64>,
    /// (#359) `now_ms` the guard entered `DriftHold` (diagnostics/log only).
    /// PERSISTS across `reset_tracks`.
    hold_since_ms: Option<u64>,
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
        // (#359) The per-connection STEP/DRIFT classifier is re-derived from the
        // fresh baseline, so clear it here. `loop_streak`, `mode`,
        // `last_over_threshold_ms` and `hold_since_ms` DELIBERATELY persist — the
        // death-loop is only visible ACROSS reconnects, so the non-convergence
        // state must survive this reset.
        self.prev_skew_ms = None;
        self.max_abs_step_ms = 0;
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

    /// (#359) Current recovery regime. `DriftHold` means the guard detected a
    /// non-converging reconnect chain and stopped thrashing (only the hard cap
    /// trips now). Surfaced for logging / tests.
    pub fn mode(&self) -> GuardMode {
        self.mode
    }

    /// (#359) Consecutive chained-trip count — the death-loop counter. Surfaced
    /// for logging / tests.
    pub fn loop_streak(&self) -> u32 {
        self.loop_streak
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

        // (#359) Per-chunk skew CHANGE → STEP/DRIFT classification for THIS
        // connection. The first computed chunk contributes no delta; a STEP
        // leaves at least one chunk-to-chunk jump ≥ SKEW_STEP_JUMP_MS, a DRIFT
        // reaches the threshold with every per-chunk delta well below it.
        if let Some(prev) = self.prev_skew_ms {
            let delta = (skew - prev).abs();
            if delta > self.max_abs_step_ms {
                self.max_abs_step_ms = delta;
            }
        }
        self.prev_skew_ms = Some(skew);

        // (#359) Decay: once SKEW_LOOP_WINDOW_MS passes with neither a trip nor
        // an over-threshold chunk, the reconnect(s) genuinely converged (or the
        // source healed) — return to full sensitivity. Under a persistent drift
        // the over-threshold chunks keep updating last_over_threshold_ms, so
        // this never fires while the desync is live (DriftHold is the right
        // steady state).
        let last_activity = self.last_trip_ms.max(self.last_over_threshold_ms);
        if let Some(act) = last_activity
            && now_ms.saturating_sub(act) >= SKEW_LOOP_WINDOW_MS
        {
            self.loop_streak = 0;
            self.mode = GuardMode::Normal;
            self.hold_since_ms = None;
        }

        // Only a stream with BOTH tracks present can have a meaningful A/V skew.
        // For a one-track stream the baseline is never captured and
        // current_skew_ms() returns 0, so nothing below trips.
        let over_hard_threshold = self.both_tracks_seen() && skew.abs() > MAX_AV_SKEW_MS;
        if over_hard_threshold {
            self.last_over_threshold_ms = Some(now_ms);
        }

        // Debounce against the ACTIVE mode's threshold: MAX_AV_SKEW_MS in
        // Normal, the wider SKEW_HOLD_MAX_MS hard cap in DriftHold.
        let threshold = match self.mode {
            GuardMode::Normal => MAX_AV_SKEW_MS,
            GuardMode::DriftHold => SKEW_HOLD_MAX_MS,
        };
        if self.both_tracks_seen() && skew.abs() > threshold {
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
    /// honoring the debounce, the per-mode reconnect-thrash rate limit, and the
    /// #359 non-convergence hold.
    ///
    /// A candidate trip requires (as before) the skew over the ACTIVE mode's
    /// threshold for `SKEW_DEBOUNCE_CHUNKS` consecutive chunks AND at least the
    /// mode's rate-limit interval since the previous trip. Given a candidate:
    ///   - **Normal:** classify the trip STEP vs DRIFT (from
    ///     `max_abs_step_ms`), count the CHAINED streak (trips within
    ///     `SKEW_LOOP_WINDOW_MS` of each other), and if it reaches the class
    ///     limit (`SKEW_LOOP_MAX_DRIFT`/`SKEW_LOOP_MAX_STEP`) switch to
    ///     `DriftHold` and SUPPRESS this trip — a reconnect cannot fix a drift,
    ///     so we stop thrashing. Otherwise issue the reconnect (a STEP converges
    ///     in one, so it never chains to the limit).
    ///   - **DriftHold:** the guard is not disabled — the `SKEW_HOLD_MAX_MS`
    ///     hard cap (already applied as the debounce threshold) still trips,
    ///     rate-limited to one per `SKEW_HOLD_MIN_INTERVAL_MS`. Never "never
    ///     kill".
    ///
    /// On an issued trip the debounce is reset and `last_trip_ms` stamped.
    fn decide(&mut self, now_ms: u64) -> SkewDecision {
        if self.consecutive_over < SKEW_DEBOUNCE_CHUNKS {
            return SkewDecision::Continue;
        }
        let interval = match self.mode {
            GuardMode::Normal => SKEW_RECOVERY_MIN_INTERVAL_MS,
            GuardMode::DriftHold => SKEW_HOLD_MIN_INTERVAL_MS,
        };
        // Rate limit: suppress a trip within the active mode's min interval.
        // Evaluated BEFORE the chain accounting so one over-threshold episode
        // held back by the floor can never be double-counted into the streak.
        if let Some(prev) = self.last_trip_ms
            && now_ms.saturating_sub(prev) < interval
        {
            return SkewDecision::Continue;
        }

        match self.mode {
            GuardMode::DriftHold => {
                // Safety net: the hard cap still kills (the debounce already
                // required |skew| > SKEW_HOLD_MAX_MS). Stay in hold.
                let held_for_ms = self.hold_since_ms.map(|h| now_ms.saturating_sub(h));
                tracing::error!(
                    skew_ms = self.last_skew_ms,
                    hard_cap_ms = SKEW_HOLD_MAX_MS,
                    loop_streak = self.loop_streak,
                    held_for_ms,
                    trip_count = self.trip_count + 1,
                    "rtmp_push: A/V skew HARD-CAP trip in DriftHold -- reconnecting a non-converging drift (#359)"
                );
                self.issue_trip(now_ms);
                SkewDecision::TripRecovery
            }
            GuardMode::Normal => {
                let chained = self
                    .last_trip_ms
                    .map(|t| now_ms.saturating_sub(t) <= SKEW_LOOP_WINDOW_MS)
                    .unwrap_or(false);
                self.loop_streak = if chained {
                    self.loop_streak.saturating_add(1)
                } else {
                    1
                };
                let is_step = self.max_abs_step_ms >= SKEW_STEP_JUMP_MS;
                let limit = if is_step {
                    SKEW_LOOP_MAX_STEP
                } else {
                    SKEW_LOOP_MAX_DRIFT
                };
                if self.loop_streak >= limit {
                    // The reconnect chain is NOT converging — a reconnect cannot
                    // fix this class. Stop thrashing: hold reconnects behind the
                    // hard cap. The would-be trip is NOT issued.
                    self.mode = GuardMode::DriftHold;
                    self.hold_since_ms = Some(now_ms);
                    self.consecutive_over = 0;
                    tracing::error!(
                        skew_ms = self.last_skew_ms,
                        loop_streak = self.loop_streak,
                        kind = if is_step { "step" } else { "drift" },
                        hard_cap_ms = SKEW_HOLD_MAX_MS,
                        hold_interval_ms = SKEW_HOLD_MIN_INTERVAL_MS,
                        "rtmp_push: A/V skew guard -- reconnects did NOT converge; HOLDING reconnects, hard cap only (#359)"
                    );
                    SkewDecision::Continue
                } else {
                    self.issue_trip(now_ms);
                    SkewDecision::TripRecovery
                }
            }
        }
    }

    /// Stamp an issued recovery trip: bump the count, record the trip time for
    /// the rate limit, and reset the debounce so a fresh over-threshold window
    /// must re-accumulate before the next trip.
    fn issue_trip(&mut self, now_ms: u64) {
        self.trip_count = self.trip_count.saturating_add(1);
        self.last_trip_ms = Some(now_ms);
        self.consecutive_over = 0;
    }
}

#[cfg(test)]
#[path = "skew_tests.rs"]
mod tests;
