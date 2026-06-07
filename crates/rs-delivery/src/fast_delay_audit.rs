//! Audit emit helpers for the fast-endpoint self-healing path. Mirrors
//! `rescue_audit.rs`: VPS-side events go through the `AuditRing`.
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

pub fn emit_delay_grown(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    from_secs: u64,
    to_secs: u64,
    deficit_secs: u64,
) {
    push(
        audit_ring,
        Severity::Warn,
        Action::FastDelayGrown,
        alias,
        serde_json::json!({
            "alias": alias,
            "from_secs": from_secs,
            "to_secs": to_secs,
            "observed_deficit_secs": deficit_secs,
        }),
    );
}

pub fn emit_delay_shrank(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    from_secs: u64,
    to_secs: u64,
) {
    push(
        audit_ring,
        Severity::Info,
        Action::FastDelayShrank,
        alias,
        serde_json::json!({ "alias": alias, "from_secs": from_secs, "to_secs": to_secs }),
    );
}

pub fn emit_keepalive_started(audit_ring: &Option<Arc<AuditRing>>, alias: &str, mode: &str) {
    push(
        audit_ring,
        Severity::Warn,
        Action::FastKeepaliveStarted,
        alias,
        serde_json::json!({ "alias": alias, "mode": mode }),
    );
}

pub fn emit_keepalive_ended(audit_ring: &Option<Arc<AuditRing>>, alias: &str, gap_secs: u64) {
    push(
        audit_ring,
        Severity::Info,
        Action::FastKeepaliveEnded,
        alias,
        serde_json::json!({ "alias": alias, "gap_secs": gap_secs }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_with_none_ring_is_noop() {
        // Must not panic when there is no audit ring (e.g. tests / no DB).
        emit_delay_grown(&None, "ep", 5, 25, 20);
        emit_delay_shrank(&None, "ep", 25, 20);
        emit_keepalive_started(&None, "ep", "freeze");
        emit_keepalive_ended(&None, "ep", 12);
    }
}
