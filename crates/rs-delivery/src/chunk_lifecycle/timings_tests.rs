use super::timings::ChunkLifecycleTimings;
use std::time::{Duration, SystemTime};

fn ms_after(base: SystemTime, ms: u64) -> SystemTime {
    base + Duration::from_millis(ms)
}

#[test]
fn worst_stage_returns_largest_within_clock_gap() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(42, 9292, "Kiko".to_string());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(ms_after(base, 100)); // A->B = 100ms (host clock)
    t.vps_fetch_start_ts = Some(ms_after(base, 5000)); // B->C cross-clock; ignored
    t.vps_fetch_done_ts = Some(ms_after(base, 5800)); // C->D = 800ms
    t.pusher_request_ts = Some(ms_after(base, 5810)); // D->E = 10ms
    t.wire_first_byte_ts = Some(ms_after(base, 9810)); // E->F = 4000ms
    let (label, dur) = t.worst_stage();
    assert_eq!(label, "E->F");
    assert_eq!(dur, Duration::from_millis(4000));
}

#[test]
fn worst_stage_excludes_b_to_c_cross_clock() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(ms_after(base, 50));
    // B->C of 60s would dominate every other gap if not excluded.
    t.vps_fetch_start_ts = Some(ms_after(base, 60_050));
    t.vps_fetch_done_ts = Some(ms_after(base, 60_150)); // C->D = 100ms
    t.pusher_request_ts = Some(ms_after(base, 60_160)); // D->E = 10ms
    t.wire_first_byte_ts = Some(ms_after(base, 60_260)); // E->F = 100ms
    let (label, _) = t.worst_stage();
    assert!(
        label != "B->C",
        "B->C must be excluded because clock skew makes it noise"
    );
}

#[test]
fn gap_a_to_b_returns_zero_when_either_missing() {
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    assert_eq!(t.gap_a_to_b(), Duration::ZERO);
    t.host_emit_ts = Some(SystemTime::UNIX_EPOCH);
    assert_eq!(
        t.gap_a_to_b(),
        Duration::ZERO,
        "B missing -> ZERO, never panic"
    );
}

#[test]
fn is_partial_true_when_a_or_b_missing() {
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    assert!(t.is_partial(), "fresh struct: A and B None -> partial");
    t.host_emit_ts = Some(SystemTime::UNIX_EPOCH);
    assert!(t.is_partial(), "B still None -> partial");
    t.s3_upload_complete_ts = Some(SystemTime::UNIX_EPOCH);
    assert!(!t.is_partial(), "both A and B set -> not partial");
}

#[test]
fn gap_b_to_c_returns_zero_when_either_missing() {
    // B->C is cross-clock and excluded from worst_stage but is still
    // serialized to the audit row by Task 12. Confirm it follows the
    // same None-safety contract as gap_a_to_b.
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    assert_eq!(t.gap_b_to_c(), Duration::ZERO);
    t.s3_upload_complete_ts = Some(SystemTime::UNIX_EPOCH);
    assert_eq!(t.gap_b_to_c(), Duration::ZERO, "C missing -> ZERO");
}

#[test]
fn gap_returns_zero_on_negative_duration_due_to_skew() {
    // If `later` is BEFORE `earlier` (e.g. clock jump), gap math must
    // saturate to ZERO instead of underflowing.
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    t.vps_fetch_start_ts = Some(base + Duration::from_secs(10));
    t.vps_fetch_done_ts = Some(base); // earlier than start: regression
    assert_eq!(t.gap_c_to_d(), Duration::ZERO);
}
