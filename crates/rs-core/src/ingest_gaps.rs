//! Ingest frame-gap counters (#368), shared lock-free between the
//! `MediaReceiver` on the `restreamer-ingest` runtime (writer) and
//! `GET /api/v1/status` (reader, `inpoint.details.ingest_gaps`).
//!
//! Two incident kinds, both counted since the process started:
//! - **arrival gap**: no media frame arrived for 300 ms or more. Our side
//!   stopped reading (an ingest stall) or the publisher stopped sending.
//! - **source-ts jump**: the publisher's video timestamps jumped by more than
//!   1.5 frame intervals. OBS drops frames from its send queue when the
//!   connection stalls; each dropped frame leaves a hole in the timestamps,
//!   so the jump size says EXACTLY how many frames it dropped before sending.
//!
//! Every field is its own atomic, so a reader may see one incident's count
//! before its "last" fields. That is fine for a status display, and the
//! writer never waits for a reader.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// The writer side: one instance, behind an `Arc` in `InpointState`.
#[derive(Debug, Default)]
pub struct IngestGapCounters {
    arrival_gaps: AtomicU64,
    arrival_gap_max_ms: AtomicU64,
    arrival_gap_total_ms: AtomicU64,
    last_arrival_gap_ms: AtomicU64,
    last_arrival_gap_at_ms: AtomicI64,
    source_ts_jumps: AtomicU64,
    dropped_frames: AtomicU64,
    last_jump_dropped_frames: AtomicU64,
    last_jump_at_ms: AtomicI64,
    frame_interval_us: AtomicU64,
}

/// What `/api/v1/status` shows. `*_at_ms` are Unix-epoch milliseconds of
/// the moment the frames resumed, 0 = never.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestGapSnapshot {
    pub arrival_gaps: u64,
    pub arrival_gap_max_ms: u64,
    pub arrival_gap_total_ms: u64,
    pub last_arrival_gap_ms: u64,
    pub last_arrival_gap_at_ms: i64,
    pub source_ts_jumps: u64,
    pub dropped_frames: u64,
    pub last_jump_dropped_frames: u64,
    pub last_jump_at_ms: i64,
    /// The publisher's measured video frame interval (µs), 0 = not measured
    /// yet in the current stream.
    pub frame_interval_us: u64,
}

impl IngestGapCounters {
    /// Count one arrival gap of `gap_ms`, which ended at `at_ms`.
    pub fn record_arrival_gap(&self, gap_ms: u64, at_ms: i64) {
        self.arrival_gaps.fetch_add(1, Ordering::Relaxed);
        self.arrival_gap_max_ms.fetch_max(gap_ms, Ordering::Relaxed);
        self.arrival_gap_total_ms
            .fetch_add(gap_ms, Ordering::Relaxed);
        self.last_arrival_gap_ms.store(gap_ms, Ordering::Relaxed);
        self.last_arrival_gap_at_ms.store(at_ms, Ordering::Relaxed);
    }

    /// Count one source-ts jump that dropped `dropped` frames, seen at `at_ms`.
    pub fn record_source_jump(&self, dropped: u64, at_ms: i64) {
        self.source_ts_jumps.fetch_add(1, Ordering::Relaxed);
        self.dropped_frames.fetch_add(dropped, Ordering::Relaxed);
        self.last_jump_dropped_frames
            .store(dropped, Ordering::Relaxed);
        self.last_jump_at_ms.store(at_ms, Ordering::Relaxed);
    }

    /// The current stream's measured frame interval (µs), 0 = unknown.
    pub fn set_frame_interval_us(&self, us: u64) {
        self.frame_interval_us.store(us, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IngestGapSnapshot {
        IngestGapSnapshot {
            arrival_gaps: self.arrival_gaps.load(Ordering::Relaxed),
            arrival_gap_max_ms: self.arrival_gap_max_ms.load(Ordering::Relaxed),
            arrival_gap_total_ms: self.arrival_gap_total_ms.load(Ordering::Relaxed),
            last_arrival_gap_ms: self.last_arrival_gap_ms.load(Ordering::Relaxed),
            last_arrival_gap_at_ms: self.last_arrival_gap_at_ms.load(Ordering::Relaxed),
            source_ts_jumps: self.source_ts_jumps.load(Ordering::Relaxed),
            dropped_frames: self.dropped_frames.load(Ordering::Relaxed),
            last_jump_dropped_frames: self.last_jump_dropped_frames.load(Ordering::Relaxed),
            last_jump_at_ms: self.last_jump_at_ms.load(Ordering::Relaxed),
            frame_interval_us: self.frame_interval_us.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_both_kinds_and_keeps_the_last_incident() {
        let c = IngestGapCounters::default();
        assert_eq!(c.snapshot(), IngestGapSnapshot::default());

        c.record_arrival_gap(450, 1_000);
        c.record_arrival_gap(1_200, 2_000);
        c.record_arrival_gap(310, 3_000);
        c.record_source_jump(9, 2_500);
        c.record_source_jump(2, 4_000);
        c.set_frame_interval_us(33_367);

        assert_eq!(
            c.snapshot(),
            IngestGapSnapshot {
                arrival_gaps: 3,
                arrival_gap_max_ms: 1_200,
                arrival_gap_total_ms: 1_960,
                last_arrival_gap_ms: 310,
                last_arrival_gap_at_ms: 3_000,
                source_ts_jumps: 2,
                dropped_frames: 11,
                last_jump_dropped_frames: 2,
                last_jump_at_ms: 4_000,
                frame_interval_us: 33_367,
            }
        );
    }

    #[test]
    fn the_snapshot_serializes_with_stable_field_names() {
        let v = serde_json::to_value(IngestGapSnapshot {
            dropped_frames: 7,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(v["dropped_frames"], 7);
        assert_eq!(v["arrival_gaps"], 0);
        assert_eq!(v["frame_interval_us"], 0);
        assert_eq!(v.as_object().unwrap().len(), 10);
    }
}
