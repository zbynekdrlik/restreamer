//! Ingest-side A/V-skew monitor (#354).
//!
//! ## Why this exists — measure the desync at the SOURCE, before it ships
//!
//! The 2026-08-30 live incidents (events SNV-stream-2026-08-30 / 9359) fed the
//! ingest with video tens of seconds AHEAD of audio (an OBS-side pipeline
//! drift). The ONLY A/V-skew measurement that existed was CONSUMER-side
//! (`rs_rtmp_push::skew::SkewTracker` inside each VPS pusher): every endpoint
//! independently tripped `PushError::AvSkewExceeded` and looped
//! connect→skew-kill→reconnect (148 `endpoint_rtmp_push_died` rows in 25 min),
//! so the fault was only ever visible DOWNSTREAM, as a wall of push deaths,
//! never named at its SOURCE — and `Start Delivering` happily spun up a paid
//! VPS into a feed every endpoint would skew-kill.
//!
//! This monitor reuses the EXACT same `SkewTracker` measurement at the INGEST
//! (the chunker on stream.lan), so the ingest number and the VPS `av_skew_ms`
//! are the SAME number by construction (they observe the same chunker-stamped
//! content-PTS). That makes the source-desync distinguishable from the
//! chunker-internal re-anchor gap (#146/#255) in future incidents, and lets
//! the operator be warned + `Start Delivering` be gated BEFORE any push dies.
//!
//! ## Diagnostic ONLY — never a recovery actuator
//!
//! Unlike the pusher's tracker, this monitor NEVER acts on
//! `SkewDecision::TripRecovery`: the source is OBS, which is owned by a
//! separate project (camera-box) and must never be restarted/reconnected from
//! here. The monitor only MEASURES + LATCHES an operator-facing "sustained
//! over threshold" state; the remedy ("restart the stream in OBS") is the
//! operator's.

use rs_rtmp_push::{SKEW_DEBOUNCE_CHUNKS, SkewTracker};

/// A latch transition emitted at a chunk boundary.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SkewEvent {
    /// Skew crossed the operator threshold and stayed there for the debounce
    /// window — raise the banner + emit the audit row.
    Detected,
    /// Skew fell back under the threshold after having been `Detected` —
    /// clear the banner + emit the recovery audit row.
    Cleared,
}

/// Result of evaluating one chunk boundary: the current baseline-relative
/// skew (ms, signed; positive = audio behind video) and an optional latch
/// transition. `event == None` on every chunk that does not flip the latch.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SkewTransition {
    pub skew_ms: i64,
    pub event: Option<SkewEvent>,
}

/// Ingest-side A/V-skew monitor: a thin operator-facing wrapper over the
/// canonical `rs_rtmp_push::skew::SkewTracker`.
///
/// It feeds the SAME chunker-stamped input PTS the pusher's tracker consumes
/// (`observe_video`/`observe_audio`), advances the tracker at each chunk
/// boundary, reads its baseline-relative `last_skew_ms()`, and applies an
/// operator threshold with a `SKEW_DEBOUNCE_CHUNKS` sustain-latch. Because it
/// uses the tracker's baseline-relative deviation, a benign CONSTANT startup
/// domain gap (audio xiu-ts vs video wall-clock have different absolute zero
/// points) folds into the baseline and never trips — only a desync that
/// APPEARS/GROWS mid-stream does (the incident signature; #257 guard reused).
pub struct IngestSkewMonitor {
    tracker: SkewTracker,
    /// Operator alert threshold (ms). Deliberately BELOW the pusher's
    /// `MAX_AV_SKEW_MS` (4000) recovery kill so the operator is warned EARLY,
    /// before endpoints start skew-killing.
    threshold_ms: i64,
    /// Consecutive chunk boundaries whose `|skew|` exceeded `threshold_ms`.
    consecutive_over: u32,
    /// Latched "sustained over threshold" state surfaced to the dashboard +
    /// the Start-Delivering gate.
    active: bool,
    /// Last baseline-relative skew (ms) computed at a chunk boundary. Tracked
    /// on the monitor rather than read straight from `SkewTracker` because the
    /// tracker's own `last_skew_ms` is NOT cleared by `reset_tracks()` (it
    /// would report a stale value after a republish re-anchor); this one is
    /// zeroed in `reset()`.
    last_skew_ms: i64,
    /// Monotonic pseudo-clock (ms) fed to `SkewTracker::evaluate_chunk`; only
    /// gates the tracker's own (ignored) rate-limit, so a simple per-chunk
    /// increment is sufficient.
    chunk_clock_ms: u64,
}

impl IngestSkewMonitor {
    /// Create a monitor with the given operator alert threshold (ms).
    pub fn new(threshold_ms: i64) -> Self {
        Self {
            tracker: SkewTracker::default(),
            threshold_ms,
            consecutive_over: 0,
            active: false,
            last_skew_ms: 0,
            chunk_clock_ms: 0,
        }
    }

    /// Observe one input VIDEO tag timestamp (chunker-stamped content PTS).
    pub fn observe_video(&mut self, ts: u32) {
        self.tracker.observe_video(ts);
    }

    /// Observe one input AUDIO tag timestamp (chunker-stamped content PTS).
    pub fn observe_audio(&mut self, ts: u32) {
        self.tracker.observe_audio(ts);
    }

    /// The current baseline-relative skew (ms, signed). 0 until both tracks
    /// are present and a baseline is captured; re-zeroed on `reset()`.
    pub fn skew_ms(&self) -> i64 {
        self.last_skew_ms
    }

    /// Whether the sustained-over-threshold latch is currently set.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Reset on a fresh RTMP session / OBS republish re-anchor (mirrors the
    /// chunker's `start_new_session`): both tracks, the debounce, the baseline
    /// AND the operator latch are cleared, so skew re-measures from the new
    /// common origin and the banner clears.
    pub fn reset(&mut self) {
        self.tracker.reset_tracks();
        self.consecutive_over = 0;
        self.active = false;
        self.last_skew_ms = 0;
    }

    /// Evaluate at the END of one chunk. Advances the tracker, reads the
    /// baseline-relative skew, updates the debounce, and returns the current
    /// skew plus any latch transition.
    pub fn evaluate_chunk(&mut self) -> SkewTransition {
        // Advance the tracker so it captures/updates the baseline and its
        // `last_skew_ms`. The tracker's OWN recovery decision (its 4000 ms
        // MAX_AV_SKEW_MS trip) is deliberately DISCARDED here — this monitor
        // never actuates recovery (camera-box owns OBS); it only measures.
        self.chunk_clock_ms = self.chunk_clock_ms.saturating_add(1_000);
        let _ = self.tracker.evaluate_chunk(self.chunk_clock_ms);
        let skew = self.tracker.last_skew_ms();
        self.last_skew_ms = skew;

        let over = skew.abs() > self.threshold_ms;
        if over {
            self.consecutive_over = self.consecutive_over.saturating_add(1);
        } else {
            self.consecutive_over = 0;
        }

        // Latch transitions: DETECT after a sustained over-threshold window
        // (rejects transients), CLEAR the moment skew falls back under
        // threshold (a fixed source recovers fast — skew drops to ~0 the
        // instant OBS realigns / on session re-anchor).
        let event = if !self.active && self.consecutive_over >= SKEW_DEBOUNCE_CHUNKS {
            self.active = true;
            tracing::warn!(
                skew_ms = skew,
                threshold_ms = self.threshold_ms,
                "ingest A/V skew DETECTED — source (OBS) audio/video desynced"
            );
            Some(SkewEvent::Detected)
        } else if self.active && !over {
            self.active = false;
            tracing::info!(skew_ms = skew, "ingest A/V skew CLEARED — source realigned");
            Some(SkewEvent::Cleared)
        } else {
            None
        };

        SkewTransition {
            skew_ms: skew,
            event,
        }
    }
}

#[cfg(test)]
#[path = "ingest_skew_tests.rs"]
mod tests;
