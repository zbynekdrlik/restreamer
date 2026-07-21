//! Audit emit helpers for the #296 buffered-endpoint slow-refill path.
//! Mirrors `fast_delay_audit.rs`: VPS-side events go through the `AuditRing`
//! (which the host mirrors into the shared audit log), so the operator sees
//! "protection degraded / recovering" without VPS log access.
#![allow(dead_code)]

use std::sync::Arc;

use rs_core::audit::{Action, Severity, Source};

use crate::audit_ring::{AuditRing, RingRowParts};

fn push(
    audit_ring: &Option<Arc<AuditRing>>,
    severity: Severity,
    action: Action,
    alias: &str,
    detail: serde_json::Value,
) {
    if let Some(ring) = audit_ring {
        ring.push_parts(RingRowParts {
            severity,
            source: Source::Vps,
            endpoint: Some(alias.to_string()),
            action,
            detail,
        });
    }
}

/// The buffered endpoint's cushion fell below target and the slow refill began.
pub fn emit_refill_started(audit_ring: &Option<Arc<AuditRing>>, alias: &str, deficit_secs: u64) {
    push(
        audit_ring,
        Severity::Warn,
        Action::BufferRefillStarted,
        alias,
        serde_json::json!({ "alias": alias, "deficit_secs": deficit_secs }),
    );
}

/// The slow refill ended — cushion back at target, or the endpoint stalled
/// into rescue (either way the throttle is no longer active).
pub fn emit_refill_ended(audit_ring: &Option<Arc<AuditRing>>, alias: &str) {
    push(
        audit_ring,
        Severity::Info,
        Action::BufferRefillEnded,
        alias,
        serde_json::json!({ "alias": alias }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_with_none_ring_is_noop() {
        // Must not panic when there is no audit ring (e.g. tests / no DB).
        emit_refill_started(&None, "ep", 100);
        emit_refill_ended(&None, "ep");
    }
}
