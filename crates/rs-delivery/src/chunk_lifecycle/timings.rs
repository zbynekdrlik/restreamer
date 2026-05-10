//! Per-chunk lifecycle timestamps. See spec §3.1.
//! Implementation lives in Task 10 (this is the Task 9 stub).

#![allow(dead_code)]

use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub struct ChunkLifecycleTimings {
    pub sequence_number: i64,
    pub event_id: i64,
    pub endpoint_alias: String,
    pub host_emit_ts: Option<SystemTime>,
    pub s3_upload_complete_ts: Option<SystemTime>,
    pub vps_fetch_start_ts: Option<SystemTime>,
    pub vps_fetch_done_ts: Option<SystemTime>,
    pub pusher_request_ts: Option<SystemTime>,
    pub wire_first_byte_ts: Option<SystemTime>,
}

impl ChunkLifecycleTimings {
    pub fn new(_sequence_number: i64, _event_id: i64, _endpoint_alias: String) -> Self {
        unimplemented!("Task 10")
    }

    pub fn gap_a_to_b(&self) -> Duration {
        unimplemented!("Task 10")
    }
    pub fn gap_b_to_c(&self) -> Duration {
        unimplemented!("Task 10")
    }
    pub fn gap_c_to_d(&self) -> Duration {
        unimplemented!("Task 10")
    }
    pub fn gap_d_to_e(&self) -> Duration {
        unimplemented!("Task 10")
    }
    pub fn gap_e_to_f(&self) -> Duration {
        unimplemented!("Task 10")
    }

    pub fn worst_stage(&self) -> (&'static str, Duration) {
        unimplemented!("Task 10")
    }
    pub fn is_partial(&self) -> bool {
        unimplemented!("Task 10")
    }
}
