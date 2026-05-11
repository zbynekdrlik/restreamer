//! Audit-row emit helpers for the lifecycle module (#184).
//! Builds the `serde_json::Value` detail payloads and pushes via the
//! crate-local `AuditRing`. See spec §3.2.

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use rs_core::audit::{Action, Severity, Source};
use std::sync::Arc;

fn millis_since_epoch_or_zero(ts: Option<std::time::SystemTime>) -> i64 {
    ts.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn timings_to_json(t: &ChunkLifecycleTimings) -> serde_json::Value {
    serde_json::json!({
        "sequence_number": t.sequence_number,
        "event_id": t.event_id,
        "host_emit_ts_ms": millis_since_epoch_or_zero(t.host_emit_ts),
        "s3_upload_complete_ts_ms": millis_since_epoch_or_zero(t.s3_upload_complete_ts),
        "vps_fetch_start_ts_ms": millis_since_epoch_or_zero(t.vps_fetch_start_ts),
        "vps_fetch_done_ts_ms": millis_since_epoch_or_zero(t.vps_fetch_done_ts),
        "pusher_request_ts_ms": millis_since_epoch_or_zero(t.pusher_request_ts),
        "wire_first_byte_ts_ms": millis_since_epoch_or_zero(t.wire_first_byte_ts),
        "gap_a_to_b_ms": t.gap_a_to_b().as_millis() as i64,
        "gap_b_to_c_ms": t.gap_b_to_c().as_millis() as i64,
        "gap_c_to_d_ms": t.gap_c_to_d().as_millis() as i64,
        "gap_d_to_e_ms": t.gap_d_to_e().as_millis() as i64,
        "gap_e_to_f_ms": t.gap_e_to_f().as_millis() as i64,
        "instrumented": !t.is_partial(),
    })
}

pub fn emit_lifecycle_sample(ring: &Arc<AuditRing>, t: &ChunkLifecycleTimings) {
    let (worst_label, worst_dur) = t.worst_stage();
    let detail = serde_json::json!({
        "endpoint": t.endpoint_alias,
        "worst_stage": worst_label,
        "worst_stage_ms": worst_dur.as_millis() as i64,
        "chunk": timings_to_json(t),
    });
    ring.push(
        Severity::Info,
        Source::Vps,
        Some(t.endpoint_alias.clone()),
        Action::DiskCacheLifecycleSample,
        detail,
    );
}

pub fn emit_lifecycle_breach(ring: &Arc<AuditRing>, t: &ChunkLifecycleTimings) {
    let (worst_label, worst_dur) = t.worst_stage();
    let detail = serde_json::json!({
        "endpoint": t.endpoint_alias,
        "worst_stage": worst_label,
        "worst_stage_ms": worst_dur.as_millis() as i64,
        "chunk": timings_to_json(t),
    });
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(t.endpoint_alias.clone()),
        Action::DiskCacheLifecycleBreach,
        detail,
    );
}

pub fn emit_lifecycle_predeath(ring: &Arc<AuditRing>, ts: &[ChunkLifecycleTimings]) {
    let alias = ts
        .last()
        .map(|t| t.endpoint_alias.clone())
        .unwrap_or_default();
    let chunks: Vec<serde_json::Value> = ts.iter().map(timings_to_json).collect();
    let detail = serde_json::json!({
        "endpoint": alias,
        "chunks": chunks,
    });
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(alias),
        Action::EndpointLifecyclePredeath,
        detail,
    );
}
