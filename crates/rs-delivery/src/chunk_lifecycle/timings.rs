//! Per-chunk lifecycle timestamps. See spec §3.1.

use std::time::{Duration, SystemTime};

/// Six pipeline stages captured per chunk (A..F) plus identifying metadata.
///
/// Stages A and B live on the host clock; stages C..F live on the VPS clock.
/// `worst_stage` excludes the cross-clock B->C gap because skew dominates
/// it and would falsely accuse the host->VPS hop on every chunk.
#[derive(Debug, Clone)]
pub struct ChunkLifecycleTimings {
    pub sequence_number: i64,
    pub event_id: i64,
    pub endpoint_alias: String,

    // Host clock
    /// Stage A: host wrote chunk to local FS (or, in practice, the
    /// uploader dequeued the chunk row immediately before the S3 PUT).
    pub host_emit_ts: Option<SystemTime>,
    /// Stage B: host received S3 200 OK on PUT.
    pub s3_upload_complete_ts: Option<SystemTime>,

    // VPS clock
    /// Stage C: VPS issued S3 GET.
    pub vps_fetch_start_ts: Option<SystemTime>,
    /// Stage D: VPS GET returned with chunk in memory.
    pub vps_fetch_done_ts: Option<SystemTime>,
    /// Stage E: pusher popped chunk from PrefetchQueue.
    pub pusher_request_ts: Option<SystemTime>,
    /// Stage F: pusher's first TCP write succeeded.
    pub wire_first_byte_ts: Option<SystemTime>,
}

impl ChunkLifecycleTimings {
    pub fn new(sequence_number: i64, event_id: i64, endpoint_alias: String) -> Self {
        Self {
            sequence_number,
            event_id,
            endpoint_alias,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
            vps_fetch_start_ts: None,
            vps_fetch_done_ts: None,
            pusher_request_ts: None,
            wire_first_byte_ts: None,
        }
    }

    fn gap(earlier: Option<SystemTime>, later: Option<SystemTime>) -> Duration {
        match (earlier, later) {
            (Some(a), Some(b)) => b.duration_since(a).unwrap_or(Duration::ZERO),
            _ => Duration::ZERO,
        }
    }

    pub fn gap_a_to_b(&self) -> Duration {
        Self::gap(self.host_emit_ts, self.s3_upload_complete_ts)
    }

    pub fn gap_b_to_c(&self) -> Duration {
        Self::gap(self.s3_upload_complete_ts, self.vps_fetch_start_ts)
    }

    pub fn gap_c_to_d(&self) -> Duration {
        Self::gap(self.vps_fetch_start_ts, self.vps_fetch_done_ts)
    }

    pub fn gap_d_to_e(&self) -> Duration {
        Self::gap(self.vps_fetch_done_ts, self.pusher_request_ts)
    }

    pub fn gap_e_to_f(&self) -> Duration {
        Self::gap(self.pusher_request_ts, self.wire_first_byte_ts)
    }

    /// Returns the (label, duration) of the slowest within-clock stage.
    /// B->C is excluded by design — see struct doc.
    pub fn worst_stage(&self) -> (&'static str, Duration) {
        let candidates: [(&'static str, Duration); 4] = [
            ("A->B", self.gap_a_to_b()),
            ("C->D", self.gap_c_to_d()),
            ("D->E", self.gap_d_to_e()),
            ("E->F", self.gap_e_to_f()),
        ];
        candidates
            .into_iter()
            .max_by_key(|(_, d)| *d)
            .unwrap_or(("none", Duration::ZERO))
    }

    /// True if either stage A or B is None — the chunk was uploaded by
    /// a pre-lifecycle host. Audit rows for partial chunks carry an
    /// `instrumented=false` flag so the dashboard can dim them.
    pub fn is_partial(&self) -> bool {
        self.host_emit_ts.is_none() || self.s3_upload_complete_ts.is_none()
    }
}
