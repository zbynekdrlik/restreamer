//! Rate-limited DiskCachePushSample audit emission for the consumer_task hot path.
//!
//! Extracted from endpoint_task to keep that file under the 1000-line gate.
//! Issue #176 -- Phase 1: wire push samples to the real production hot path.

use std::cell::Cell;
use std::sync::Arc;

use crate::audit_ring::AuditRing;

/// Static-per-task context for `emit_push_sample`. Built once at the top of
/// `consumer_task` and reused on every push. Holds the previous-push
/// timestamp internally via `Cell` so the call site is a single line.
pub(crate) struct PushSampleCtx<'a> {
    pub audit_ring: &'a Option<Arc<AuditRing>>,
    pub push_audit_rl: &'a rs_core::audit::RateLimiter,
    pub alias: &'a str,
    pub delivery_delay_ms: u64,
    pub event_start_at: std::time::Instant,
    last_push_at: Cell<Option<std::time::Instant>>,
}

impl<'a> PushSampleCtx<'a> {
    pub fn new(
        audit_ring: &'a Option<Arc<AuditRing>>,
        push_audit_rl: &'a rs_core::audit::RateLimiter,
        alias: &'a str,
        delivery_delay_ms: u64,
    ) -> Self {
        Self {
            audit_ring,
            push_audit_rl,
            alias,
            delivery_delay_ms,
            event_start_at: std::time::Instant::now(),
            last_push_at: Cell::new(None),
        }
    }
}

/// Emit a DiskCachePushSample audit row for one successful chunk push.
/// Rate-limited via push_audit_rl keyed by (DiskCachePushSample, alias) so
/// the audit log gets ~1 row/min/endpoint instead of ~1/2s.
///
/// `cumulative_pushed_secs` is the total media duration this endpoint has
/// pushed so far (≈ stream age), NOT behind-live lag. It was previously
/// emitted under the key `current_chunk_delay_secs`, which read like a
/// behind-live number (showing ~7800s = stream age) and misled operators.
/// The behind-live signal is `chunk_supply_lag_ms` / the dashboard's
/// per-endpoint `chunk_delay_secs`; this field is purely a progress counter.
pub(crate) fn emit_push_sample(
    ctx: &PushSampleCtx<'_>,
    chunk_id: i64,
    chunk_duration_ms: i64,
    cumulative_pushed_secs: f64,
) {
    let now = std::time::Instant::now();
    let prev = ctx.last_push_at.replace(Some(now));
    if !ctx
        .push_audit_rl
        .allow(rs_core::audit::Action::DiskCachePushSample, ctx.alias)
    {
        return;
    }
    let Some(ring) = ctx.audit_ring else { return };
    let inter_chunk_gap_ms = match prev {
        Some(t) => now.saturating_duration_since(t).as_millis() as u64,
        None => 0,
    };
    let chunk_dur = chunk_duration_ms.max(0) as u64;
    let burst_factor = if inter_chunk_gap_ms == 0 || chunk_dur == 0 {
        0.0
    } else {
        chunk_dur as f64 / inter_chunk_gap_ms as f64
    };
    let expected_wallclock_ms = (chunk_id.max(0) as u64).saturating_mul(chunk_dur);
    let actual_wallclock_ms = now
        .saturating_duration_since(ctx.event_start_at)
        .as_millis() as i64;
    let chunk_supply_lag_ms = actual_wallclock_ms.saturating_sub(expected_wallclock_ms as i64);
    let payload = serde_json::json!({
        "endpoint": ctx.alias,
        "chunk_id": chunk_id,
        "chunk_supply_lag_ms": chunk_supply_lag_ms,
        "inter_chunk_gap_ms": inter_chunk_gap_ms,
        "burst_factor": burst_factor,
        "delivery_delay_secs": ctx.delivery_delay_ms / 1000,
        "cumulative_pushed_secs": cumulative_pushed_secs,
    });
    ring.push(
        rs_core::audit::Severity::Warn,
        rs_core::audit::Source::Vps,
        Some(ctx.alias.to_string()),
        rs_core::audit::Action::DiskCachePushSample,
        payload,
    );
}
