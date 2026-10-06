//! Ingest frame-gap metric (#368): every dropout reaching the ingest is
//! MEASURED, with timestamps.
//!
//! Two incidents, detected per frame on the ingest runtime:
//! - **arrival gap**: no media frame (video or audio) arrived for
//!   [`ARRIVAL_GAP_THRESHOLD`] (300 ms) or more. Either our side stopped
//!   reading (an ingest stall: OBS's send queue fills) or the publisher
//!   stopped sending.
//! - **source-ts jump**: the publisher's video timestamp advanced by more than
//!   [`JUMP_FACTOR`] (1.5) measured frame intervals. OBS drops frames from its
//!   send queue when the connection stalls, and each dropped frame leaves a
//!   hole in the timestamps, so `round(delta / interval) - 1` is exactly the
//!   number of frames OBS dropped before sending.
//!
//! The frame interval is measured, never assumed: until [`WARMUP_DELTAS`]
//! deltas are known nothing is classified; then the estimate is the mean of
//! the last [`WINDOW`] deltas within [0.5, 1.5] x their median (one outlier
//! cannot skew it); once [`CUMULATIVE_MIN`] deltas have been accepted it is
//! the mean of every accepted delta since the subscription, which cancels
//! the publisher's ms rounding (29.97 fps alternates 33 / 34 ms). A forward
//! step over [`MAX_COUNTED_JUMP_MS`] (the chunker's `FarForward` bound) is a
//! timeline discontinuity, a backward step a restart: neither counts frames.
//!
//! Cost on the ingest thread: a few integer ops per frame, a 16-entry sort
//! per video frame, atomics for the counters, and an `audit::record`
//! (`try_send`) per row the throttle lets through. No lock, no await.

use std::fmt::Display;
use std::time::Duration;

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::audit_throttle::{Admission, AuditThrottle, Suppressed};
use rs_core::ingest_gaps::IngestGapCounters;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::warn;

/// An arrival gap at least this long is an incident.
pub(crate) const ARRIVAL_GAP_THRESHOLD: Duration = Duration::from_millis(300);
/// A video ts delta above this many frame intervals is a jump.
pub(crate) const JUMP_FACTOR: f64 = 1.5;
/// A forward step longer than this is a timeline discontinuity, not drops.
pub(crate) const MAX_COUNTED_JUMP_MS: u32 = 30_000;
/// Deltas needed before anything is classified.
pub(crate) const WARMUP_DELTAS: usize = 8;
/// Recent deltas kept for the median-based estimate.
pub(crate) const WINDOW: usize = 16;
/// Accepted deltas after which the cumulative mean is the estimate.
pub(crate) const CUMULATIVE_MIN: u64 = 32;
/// At most one `IngestFrameGap` row per kind per this interval.
pub(crate) const AUDIT_MIN_INTERVAL: Duration = Duration::from_secs(10);

/// A source-ts jump: OBS dropped `dropped` frames before sending.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SourceJump {
    pub(crate) from_ts: u32,
    pub(crate) to_ts: u32,
    pub(crate) delta_ms: u32,
    pub(crate) interval_ms: f64,
    pub(crate) dropped: u64,
}

/// What one video timestamp meant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum VideoStep {
    /// The first frame, a duplicate ts, a normal step, or a step during the
    /// warm-up.
    Normal,
    /// Frames are missing.
    Jump(SourceJump),
    /// Backward, or forward by more than [`MAX_COUNTED_JUMP_MS`]: a new
    /// timeline. Re-based, not counted.
    Discontinuity { from_ts: u32, to_ts: u32 },
}

/// The publisher's video frame interval, measured from its timestamps.
#[derive(Debug, Clone, Default)]
pub(crate) struct FrameInterval {
    window: [u32; WINDOW],
    len: usize,
    next: usize,
    cum_ms: u64,
    cum_n: u64,
}

impl FrameInterval {
    /// The current estimate in ms, `None` during the warm-up.
    pub(crate) fn estimate(&self) -> Option<f64> {
        let _ = (self.window, self.len, self.next, self.cum_ms, self.cum_n);
        None
    }

    /// Remember a delta. `counted`: it was classified normal against an
    /// estimate, so it joins the cumulative mean.
    fn accept(&mut self, delta: u32, counted: bool) {
        let _ = (delta, counted);
    }
}

/// Frames OBS dropped for a `delta_ms` step at `interval_ms` per frame.
pub(crate) fn dropped_frames(delta_ms: u32, interval_ms: f64) -> u64 {
    let _ = (delta_ms, interval_ms);
    0
}

/// Per-subscription gap detection. Pure: instants and timestamps in,
/// incidents out.
#[derive(Debug, Clone, Default)]
pub(crate) struct IngestGapTracker {
    last_arrival: Option<Instant>,
    last_video_ts: Option<u32>,
    interval: FrameInterval,
}

impl IngestGapTracker {
    /// A new subscription: the wait for it is no gap, and its publisher may
    /// run at another frame rate.
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    /// A media frame arrived at `now`: the gap since the previous one, if it
    /// reached [`ARRIVAL_GAP_THRESHOLD`].
    pub(crate) fn on_media(&mut self, now: Instant) -> Option<Duration> {
        let _ = (now, self.last_arrival);
        None
    }

    /// A video frame with source timestamp `ts` arrived.
    pub(crate) fn on_video(&mut self, ts: u32) -> VideoStep {
        let _ = (
            ts,
            self.last_video_ts,
            &mut self.interval,
            FrameInterval::accept,
            SourceJump {
                from_ts: 0,
                to_ts: 0,
                delta_ms: 0,
                interval_ms: 0.0,
                dropped: 0,
            },
            VideoStep::Discontinuity {
                from_ts: 0,
                to_ts: 0,
            },
        );
        VideoStep::Normal
    }

    /// The current frame-interval estimate in µs, 0 = not measured yet.
    pub(crate) fn interval_us(&self) -> u64 {
        self.interval
            .estimate()
            .map_or(0, |ms| (ms * 1_000.0).round() as u64)
    }
}

/// What one frame revealed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FrameGaps {
    pub(crate) arrival: Option<Duration>,
    pub(crate) video: VideoStep,
}

/// The tracker plus one audit throttle per incident kind.
#[derive(Debug)]
pub(crate) struct IngestGapMonitor {
    tracker: IngestGapTracker,
    arrival_audit: AuditThrottle,
    jump_audit: AuditThrottle,
}

impl Default for IngestGapMonitor {
    fn default() -> Self {
        Self {
            tracker: IngestGapTracker::default(),
            arrival_audit: AuditThrottle::new(AUDIT_MIN_INTERVAL),
            jump_audit: AuditThrottle::new(AUDIT_MIN_INTERVAL),
        }
    }
}

const ARRIVAL_GAP: &str = "arrival_gap";
const SOURCE_TS_JUMP: &str = "source_ts_jump";

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn held_json(held: Option<Suppressed>, unit: &str) -> Value {
    held.map_or(Value::Null, |h| h.to_json(unit))
}

/// The `IngestFrameGap` row (Warn, Inpoint) carrying `detail`.
pub(crate) fn ingest_frame_gap_row(detail: Value) -> AuditRow {
    AuditRow {
        severity: Severity::Warn,
        source: Source::Inpoint,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: Action::IngestFrameGap,
        detail,
        ts_override: None,
    }
}

impl IngestGapMonitor {
    /// A new subscription started (see [`IngestGapTracker::reset`]).
    pub(crate) fn reset_stream(&mut self) {
        self.tracker.reset();
    }

    /// One media frame (`video_ts` for video) arrived at `now`, `wall_ms`
    /// Unix-epoch ms. Counts the incidents into `counters` and returns the
    /// `IngestFrameGap` details the throttles let through, plus what the
    /// frame revealed (for the log line).
    pub(crate) fn on_frame<S: Display + ?Sized>(
        &mut self,
        now: Instant,
        wall_ms: i64,
        video_ts: Option<u32>,
        stream: &S,
        counters: &IngestGapCounters,
    ) -> (FrameGaps, Vec<Value>) {
        let gaps = FrameGaps {
            arrival: self.tracker.on_media(now),
            video: video_ts.map_or(VideoStep::Normal, |ts| self.tracker.on_video(ts)),
        };
        if video_ts.is_some() {
            counters.set_frame_interval_us(self.tracker.interval_us());
        }
        let mut rows = Vec::new();
        let at = now.into_std();
        if let Some(gap) = gaps.arrival {
            counters.record_arrival_gap(ms(gap), wall_ms);
            if let Admission::Emit { suppressed } = self.arrival_audit.admit(at, ms(gap)) {
                rows.push(json!({
                    "kind": ARRIVAL_GAP,
                    "gap_ms": ms(gap),
                    "threshold_ms": ms(ARRIVAL_GAP_THRESHOLD),
                    "resumed_at_ms": wall_ms,
                    "stream_identifier": stream.to_string(),
                    "held_back_before": held_json(suppressed, "ms"),
                }));
            }
        }
        if let VideoStep::Jump(j) = gaps.video {
            counters.record_source_jump(j.dropped, wall_ms);
            if let Admission::Emit { suppressed } = self.jump_audit.admit(at, j.dropped) {
                rows.push(json!({
                    "kind": SOURCE_TS_JUMP,
                    "from_ts": j.from_ts,
                    "to_ts": j.to_ts,
                    "delta_ms": j.delta_ms,
                    "frame_interval_ms": (j.interval_ms * 1_000.0).round() / 1_000.0,
                    "dropped_frames": j.dropped,
                    "at_ms": wall_ms,
                    "stream_identifier": stream.to_string(),
                    "held_back_before": held_json(suppressed, "frames"),
                }));
            }
        }
        rows.extend(self.flush(Some(at), stream));
        (gaps, rows)
    }

    /// Aggregate rows for what the throttles held back: the ones due at
    /// `now`, or every one when `now` is `None` (the stream ended).
    pub(crate) fn flush<S: Display + ?Sized>(
        &mut self,
        now: Option<std::time::Instant>,
        stream: &S,
    ) -> Vec<Value> {
        let mut rows = Vec::new();
        for (kind, unit, throttle) in [
            (ARRIVAL_GAP, "ms", &mut self.arrival_audit),
            (SOURCE_TS_JUMP, "frames", &mut self.jump_audit),
        ] {
            let held = match now {
                Some(now) => throttle.take_due(now),
                None => throttle.take_pending(),
            };
            if let Some(held) = held {
                rows.push(json!({
                    "kind": kind,
                    "aggregate": true,
                    "held_back": held.to_json(unit),
                    "stream_identifier": stream.to_string(),
                }));
            }
        }
        rows
    }
}

/// The log line of each incident `gaps` holds (#368: every dropout is in
/// restreamer.log with its time, even when its audit row is held back).
pub(crate) fn incident_messages<S: Display + ?Sized>(gaps: &FrameGaps, stream: &S) -> Vec<String> {
    let _ = (gaps, stream);
    Vec::new()
}

/// Write `rows` as `IngestFrameGap` audit rows (`try_send`, never waits).
pub(crate) fn record_rows(audit_tx: Option<&mpsc::Sender<AuditRow>>, rows: Vec<Value>) {
    if let Some(tx) = audit_tx {
        for detail in rows {
            rs_core::audit::record(tx, ingest_frame_gap_row(detail));
        }
    }
}

/// Log every incident of one frame and write the rows the throttles let
/// through.
pub(crate) fn publish<S: Display + ?Sized>(
    gaps: &FrameGaps,
    rows: Vec<Value>,
    audit_tx: Option<&mpsc::Sender<AuditRow>>,
    stream: &S,
) {
    for message in incident_messages(gaps, stream) {
        warn!("{message}");
    }
    record_rows(audit_tx, rows);
}

#[cfg(test)]
#[path = "ingest_gap_tests.rs"]
mod tests;
