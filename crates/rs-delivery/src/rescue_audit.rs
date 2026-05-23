//! Builders for the rescue-mode audit rows. Pure functions so the
//! enter/recover semantics are unit-testable; the consumer task calls these
//! and pushes the result onto the VPS [`AuditRing`](crate::audit_ring::AuditRing).

use crate::audit_ring::RingRowParts;
use rs_core::audit::{Action, Severity, Source};

/// Row emitted when an endpoint enters rescue (chunk supply dried up).
pub fn rescue_activated_row(alias: &str, stalled_at_chunk_id: i64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Warn,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueActivated,
        detail: serde_json::json!({ "stalled_at_chunk_id": stalled_at_chunk_id }),
    }
}

/// Row emitted when an endpoint exits rescue back to live delivery.
pub fn rescue_recovered_row(alias: &str, gap_secs: u64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Info,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueRecovered,
        detail: serde_json::json!({ "gap_secs": gap_secs }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activated_row_is_warn_with_chunk() {
        let r = rescue_activated_row("yt-main", 4242);
        assert_eq!(r.action, Action::RescueActivated);
        assert_eq!(r.severity, Severity::Warn);
        assert_eq!(r.endpoint.as_deref(), Some("yt-main"));
        assert_eq!(r.detail["stalled_at_chunk_id"], 4242);
    }

    #[test]
    fn recovered_row_carries_gap_secs() {
        let r = rescue_recovered_row("yt-main", 137);
        assert_eq!(r.action, Action::RescueRecovered);
        assert_eq!(r.severity, Severity::Info);
        assert_eq!(r.detail["gap_secs"], 137);
    }
}
