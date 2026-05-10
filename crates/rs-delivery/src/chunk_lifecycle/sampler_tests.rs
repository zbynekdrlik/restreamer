use super::sampler::LifecycleSampler;
use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use rs_core::audit::{Action, Severity};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn fast_chunk(seq: i64) -> ChunkLifecycleTimings {
    // Steady-state chunk: every gap < 100ms, no breach.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(seq, 9292, "Kiko".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(base + Duration::from_millis(50));
    t.vps_fetch_start_ts = Some(base + Duration::from_millis(60));
    t.vps_fetch_done_ts = Some(base + Duration::from_millis(110));
    t.pusher_request_ts = Some(base + Duration::from_millis(120));
    t.wire_first_byte_ts = Some(base + Duration::from_millis(170));
    t
}

fn slow_chunk(seq: i64) -> ChunkLifecycleTimings {
    // Breach: E->F = 5000ms exceeds default 4000ms threshold.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(seq, 9292, "Kiko".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(base + Duration::from_millis(10));
    t.vps_fetch_start_ts = Some(base + Duration::from_millis(20));
    t.vps_fetch_done_ts = Some(base + Duration::from_millis(30));
    t.pusher_request_ts = Some(base + Duration::from_millis(40));
    t.wire_first_byte_ts = Some(base + Duration::from_millis(5_040));
    t
}

#[tokio::test]
async fn observe_emits_sample_every_nth_chunk() {
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(/* sample_every_n */ 5, /* breach_ms */ 4_000);
    for i in 0..10 {
        s.observe(&fast_chunk(i), &ring);
    }
    let rows = ring.as_ref().unwrap().since(0).0;
    let sample_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleSample)
        .collect();
    // pushed_count goes 1..10. With every_n=5, samples emit at counts 5 and 10.
    assert_eq!(sample_rows.len(), 2, "expected 2 samples for 10 chunks at N=5");
    // Severity must be Info — operators don't want sample rows paging on-call.
    assert_eq!(
        sample_rows[0].severity,
        Severity::Info,
        "lifecycle_sample must be Severity::Info"
    );
}

#[tokio::test]
async fn observe_emits_breach_when_any_stage_exceeds_threshold() {
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(30, 4_000);
    s.observe(&slow_chunk(1), &ring);
    let rows = ring.as_ref().unwrap().since(0).0;
    let breach_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleBreach)
        .collect();
    assert_eq!(breach_rows.len(), 1);
    // Severity MUST be Warn — operators page off Severity, not Action.
    assert_eq!(
        breach_rows[0].severity,
        Severity::Warn,
        "lifecycle_breach must be Severity::Warn"
    );
}

#[tokio::test]
async fn observe_sample_wins_over_breach_at_n_boundary() {
    // every_n=1 (every chunk samples), threshold=0 (every chunk would
    // also breach). The early-return after sample emission must
    // suppress the breach for that chunk. Catches a regression that
    // removes the `return` in observe.
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(1, 0);
    s.observe(&slow_chunk(1), &ring);
    let rows = ring.as_ref().unwrap().since(0).0;
    let samples = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleSample)
        .count();
    let breaches = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleBreach)
        .count();
    assert_eq!(samples, 1, "sample must fire");
    assert_eq!(
        breaches, 0,
        "breach MUST be suppressed when sample arm fires for the same chunk"
    );
}

#[tokio::test]
async fn breach_rate_limit_at_most_one_emit_per_5s() {
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..100 {
        s.observe(&slow_chunk(i), &ring);
    }
    let breaches = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleBreach)
        .count();
    // 100 breaches in <1s real time → only the first should emit; the
    // 5s rate-limit window blocks the rest. Allow ≤ 2 in case the test
    // crosses a window boundary on a slow CI runner.
    assert!(
        breaches >= 1 && breaches <= 2,
        "expected 1-2, got {breaches}"
    );
}

#[tokio::test]
async fn emit_predeath_dumps_last_5_chunks_in_one_row_no_rate_limit() {
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..7 {
        s.observe(&fast_chunk(i), &ring);
    }
    s.emit_predeath(&ring);
    s.emit_predeath(&ring); // second call must also emit (no rate-limit)
    let predeaths = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::EndpointLifecyclePredeath)
        .collect::<Vec<_>>();
    assert_eq!(predeaths.len(), 2);
    let first = &predeaths[0];
    let chunks = first
        .detail
        .get("chunks")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(chunks.len(), 5, "predeath dump must carry exactly 5 chunks");
}

#[tokio::test]
async fn observe_pushes_to_predeath_ring_capped_at_5() {
    let ring = Some(AuditRing::new(500));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..20 {
        s.observe(&fast_chunk(i), &ring);
    }
    assert_eq!(s.predeath_ring.len(), 5, "ring capped at 5");
    let last = s
        .predeath_ring
        .iter()
        .map(|t| t.sequence_number)
        .collect::<Vec<_>>();
    assert_eq!(
        last,
        vec![15, 16, 17, 18, 19],
        "ring keeps the most recent 5"
    );
}
