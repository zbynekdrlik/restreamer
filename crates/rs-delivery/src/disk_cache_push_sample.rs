//! Rate-limited DiskCachePushSample audit emission for the consumer_task hot path.
//!
//! Extracted from endpoint_task to keep that file under the 1000-line gate.
//! Issue #176 -- Phase 1: wire push samples to the real production hot path.

use std::sync::Arc;

use crate::audit_ring::AuditRing;

/// Emit a DiskCachePushSample audit row for one successful chunk push.
/// Rate-limited via push_audit_rl keyed by (DiskCachePushSample, alias) so
/// the audit log gets ~1 row/min/endpoint instead of ~1/2s.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_disk_cache_push_sample(
    audit_ring: &Option<Arc<AuditRing>>,
    push_audit_rl: &rs_core::audit::RateLimiter,
    alias: &str,
    chunk_id: i64,
    chunk_duration_ms: i64,
    delivery_delay_ms: u64,
    event_start_at: std::time::Instant,
    last_push_at: &Option<std::time::Instant>,
    current_chunk_delay_secs: f64,
) {
    if !push_audit_rl.allow(rs_core::audit::Action::DiskCachePushSample, alias) {
        return;
    }
    let Some(ring) = audit_ring else { return };
    let now = std::time::Instant::now();
    let inter_chunk_gap_ms = match last_push_at {
        Some(t) => now.saturating_duration_since(*t).as_millis() as u64,
        None => 0,
    };
    let chunk_dur = chunk_duration_ms.max(0) as u64;
    let burst_factor = if inter_chunk_gap_ms == 0 || chunk_dur == 0 {
        0.0
    } else {
        chunk_dur as f64 / inter_chunk_gap_ms as f64
    };
    let expected_wallclock_ms = (chunk_id.max(0) as u64).saturating_mul(chunk_dur);
    let actual_wallclock_ms = now.saturating_duration_since(event_start_at).as_millis() as i64;
    let chunk_supply_lag_ms = actual_wallclock_ms.saturating_sub(expected_wallclock_ms as i64);
    let payload = serde_json::json!({
        "endpoint": alias,
        "chunk_id": chunk_id,
        "chunk_supply_lag_ms": chunk_supply_lag_ms,
        "inter_chunk_gap_ms": inter_chunk_gap_ms,
        "burst_factor": burst_factor,
        "delivery_delay_secs": delivery_delay_ms / 1000,
        "current_chunk_delay_secs": current_chunk_delay_secs,
    });
    ring.push(
        rs_core::audit::Severity::Warn,
        rs_core::audit::Source::Vps,
        Some(alias.to_string()),
        rs_core::audit::Action::DiskCachePushSample,
        payload,
    );
}
