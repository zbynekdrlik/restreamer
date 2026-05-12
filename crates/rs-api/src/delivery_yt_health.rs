//! YT health extraction + audit emission helpers.

use rs_core::audit::Action;
use rs_core::models::YoutubeHealth;
use rs_youtube::streams::LiveStream;

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
