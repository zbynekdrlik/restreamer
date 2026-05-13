//! YT health extraction + audit emission helpers.

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::models::YoutubeHealth;
use rs_youtube::streams::LiveStream;
use tokio::sync::mpsc::Sender;

/// Build a `YoutubeHealth` snapshot from a single liveStream item.
/// Picks `configurationIssues[0].type` as the top issue (YT returns the
/// most-severe / most-recent issue first).
pub fn extract_top_issue(stream: &LiveStream) -> YoutubeHealth {
    let stream_status = stream.status.stream_status.clone();
    let (health_status, top_issue) = match stream.status.health_status.as_ref() {
        Some(h) => (
            h.status.clone(),
            h.configuration_issues.first().map(|c| c.issue_type.clone()),
        ),
        None => ("unknown".to_string(), None),
    };
    let (resolution, frame_rate) = match stream.cdn.as_ref() {
        Some(c) => (c.resolution.clone(), c.frame_rate.clone()),
        None => (None, None),
    };
    YoutubeHealth {
        stream_status,
        health_status,
        top_issue,
        resolution,
        frame_rate,
        age_secs: 0,
        error: None,
    }
}

/// Decide whether `YoutubeIssueChanged` should fire.
/// Returns `Some((Action, from, to))` when `prior != current`.
pub fn issue_changed_action(
    prior: Option<&str>,
    current: Option<&str>,
) -> Option<(Action, Option<String>, Option<String>)> {
    if prior == current {
        return None;
    }
    Some((
        Action::YoutubeIssueChanged,
        prior.map(|s| s.to_string()),
        current.map(|s| s.to_string()),
    ))
}

/// Emit one `YoutubeIssueChanged` row when `prior != current`.
/// Returns `true` iff a row was sent. Drops silently if the channel is
/// full (the audit ring is best-effort).
pub async fn record_and_maybe_emit(
    prior: Option<&str>,
    current: Option<&str>,
    endpoint_alias: &str,
    audit_tx: &Sender<AuditRow>,
) -> bool {
    let Some((action, from, to)) = issue_changed_action(prior, current) else {
        return false;
    };
    let row = AuditRow {
        severity: Severity::Info,
        source: Source::System,
        event_id: None,
        instance_id: None,
        endpoint: Some(endpoint_alias.to_string()),
        action,
        detail: serde_json::json!({ "from": from, "to": to }),
        ts_override: None,
    };
    audit_tx.send(row).await.is_ok()
}
