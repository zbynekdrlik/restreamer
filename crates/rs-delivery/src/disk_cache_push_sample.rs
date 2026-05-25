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
    // Telemetry is only emitted when audit is configured (the VPS runtime
    // always sets it); preserves prior behavior of skipping the no-audit path.
    if ctx.audit_ring.is_none() {
        return;
    }
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
    // #224: pacing telemetry is a debug-only diagnostic, NOT an operator-facing
    // audit row. Emitting it to the audit ring flooded the activity feed
    // (197/200 warn rows during a live event), drowning real warn/error events.
    // Log at debug so it stays retrievable in VPS logs without polluting the feed.
    tracing::debug!(
        endpoint = %ctx.alias,
        chunk_id,
        chunk_supply_lag_ms,
        inter_chunk_gap_ms,
        burst_factor,
        delivery_delay_secs = ctx.delivery_delay_ms / 1000,
        cumulative_pushed_secs,
        "disk_cache_push_sample"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit_ring::AuditRing;

    /// Regression for #224: the operator activity feed was flooded with
    /// `disk_cache_push_sample severity=warn` rows (197/200 during a live
    /// event). These carry pure pacing telemetry, not actionable warnings, so
    /// `emit_push_sample` must NOT write an audit row -- it logs at debug
    /// instead. Asserts the audit ring stays empty after a sample call.
    #[test]
    fn emit_push_sample_does_not_write_audit_row() {
        let ring_opt: Option<Arc<AuditRing>> = Some(AuditRing::new(500));
        let rl = rs_core::audit::RateLimiter::new();
        let ctx = PushSampleCtx::new(&ring_opt, &rl, "YT NLCH 4K", 120_000);

        // First call passes the rate limiter; the old code pushed one Warn row.
        emit_push_sample(&ctx, 42, 6000, 252.0);

        let (rows, _) = ring_opt.as_ref().unwrap().since(0);
        assert_eq!(
            rows.len(),
            0,
            "disk_cache_push_sample must not emit an audit row (telemetry -> debug log only); found: {rows:?}"
        );
    }
}
