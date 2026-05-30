//! Builders for the rescue-mode audit rows. Pure functions so the
//! enter/recover semantics are unit-testable; the consumer task calls these
//! and pushes the result onto the VPS [`AuditRing`](crate::audit_ring::AuditRing).

use crate::audit_ring::{AuditRing, RingRowParts};
use rs_core::audit::{Action, Severity, Source};
use std::sync::Arc;

/// Push a RescueActivated row if a ring is present (no-op otherwise).
pub fn emit_activated(ring: &Option<Arc<AuditRing>>, alias: &str, stalled_at_chunk_id: i64) {
    if let Some(r) = ring {
        r.push_parts(rescue_activated_row(alias, stalled_at_chunk_id));
    }
}

/// Push a RescueRecovered row if a ring is present (no-op otherwise).
pub fn emit_recovered(ring: &Option<Arc<AuditRing>>, alias: &str, gap_secs: u64) {
    if let Some(r) = ring {
        r.push_parts(rescue_recovered_row(alias, gap_secs));
    }
}

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

/// Row emitted when an operator-configured rescue URL is rejected because
/// it is not a `.flv` (legacy MP4 / MOV / etc). VPS falls back to the
/// embedded default rescue blob.
pub fn legacy_rejected_row(alias: &str, url: &str) -> RingRowParts {
    RingRowParts {
        severity: Severity::Warn,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueLegacyFormatRejected,
        detail: serde_json::json!({ "url": url }),
    }
}

/// Row emitted when the VPS fails to fetch the operator-configured rescue
/// FLV from S3. VPS falls back to the embedded default rescue blob.
pub fn custom_fetch_failed_row(alias: &str, url: &str, err: &str) -> RingRowParts {
    RingRowParts {
        severity: Severity::Warn,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueCustomFetchFailed,
        detail: serde_json::json!({ "url": url, "error": err }),
    }
}

/// Push a RescueLegacyFormatRejected row if a ring is present.
pub fn emit_legacy_rejected(ring: &Option<Arc<AuditRing>>, alias: &str, url: &str) {
    if let Some(r) = ring {
        r.push_parts(legacy_rejected_row(alias, url));
    }
}

/// Push a RescueCustomFetchFailed row if a ring is present.
pub fn emit_custom_fetch_failed(ring: &Option<Arc<AuditRing>>, alias: &str, url: &str, err: &str) {
    if let Some(r) = ring {
        r.push_parts(custom_fetch_failed_row(alias, url, err));
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

    #[test]
    fn legacy_rejected_row_is_warn_with_url() {
        let r = legacy_rejected_row("yt-main", "https://s3.example.com/rescue.mp4");
        assert_eq!(r.action, Action::RescueLegacyFormatRejected);
        assert_eq!(r.severity, Severity::Warn);
        assert_eq!(r.source, Source::Vps);
        assert_eq!(r.endpoint.as_deref(), Some("yt-main"));
        assert_eq!(r.detail["url"], "https://s3.example.com/rescue.mp4");
    }

    #[test]
    fn custom_fetch_failed_row_carries_url_and_error() {
        let r = custom_fetch_failed_row("yt-main", "https://s3.example.com/rescue.flv", "HTTP 403");
        assert_eq!(r.action, Action::RescueCustomFetchFailed);
        assert_eq!(r.severity, Severity::Warn);
        assert_eq!(r.source, Source::Vps);
        assert_eq!(r.detail["url"], "https://s3.example.com/rescue.flv");
        assert_eq!(r.detail["error"], "HTTP 403");
    }
}
