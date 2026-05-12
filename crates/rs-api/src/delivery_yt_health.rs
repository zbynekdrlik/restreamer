//! YT health extraction + audit emission helpers.
//! GREEN implementation lands in Task 12.

use rs_core::audit::Action;
use rs_core::models::YoutubeHealth;
use rs_youtube::streams::LiveStream;

/// Extract the top-priority `YoutubeHealth` from a single liveStream item.
pub fn extract_top_issue(stream: &LiveStream) -> YoutubeHealth {
    let _ = stream;
    unimplemented!("filled in by GREEN task")
}

/// Decide whether an `Action::YoutubeIssueChanged` row should be emitted
/// given prior + current top_issue values for an endpoint.
pub fn issue_changed_action(
    prior: Option<&str>,
    current: Option<&str>,
) -> Option<(Action, Option<String>, Option<String>)> {
    let _ = (prior, current);
    unimplemented!("filled in by GREEN task")
}
