//! What a control room does with the popups its tab opens (#399, #405).
//!
//! Pure decisions over Chrome's target list; the relay acts on them.

use serde_json::{Value, json};

use super::cdp::TargetInfo;

/// The tabs the room tab `room` opened (`window.open`, a `target=_blank`
/// link) that already carry an http(s) URL: the room shows them in its own
/// tab, since the viewer only ever sees that one.
pub fn popups_of(pages: &[TargetInfo], room: &str) -> Vec<TargetInfo> {
    pages
        .iter()
        .filter(|t| {
            t.opener_id.as_deref() == Some(room)
                && (t.url.starts_with("https://") || t.url.starts_with("http://"))
        })
        .cloned()
        .collect()
}

/// What the relay does with one popup of the tab it shows (#405).
#[derive(Debug, Clone, PartialEq)]
pub enum PopupMove {
    /// Show the popup itself until it closes. It reports back to its opener
    /// (a Google sign-in or "Overte svoju totožnosť" flow), so the tab that
    /// opened it must stay alive behind it.
    ShowInPlace(TargetInfo),
    /// Load its URL into the shown tab and close it (#399).
    MoveIntoTab(TargetInfo),
}

/// The popups of `shown`, each with what to do with it, leaving out the ids
/// in `skip` (popups the room is done with). A popup to show comes first:
/// moving another one into the shown tab before it would destroy its opener.
pub fn popup_moves(
    pages: &[TargetInfo],
    shown: &str,
    skip: &std::collections::HashSet<String>,
) -> Vec<PopupMove> {
    let (show, moved): (Vec<TargetInfo>, Vec<TargetInfo>) = popups_of(pages, shown)
        .into_iter()
        .filter(|p| !skip.contains(&p.target_id))
        .partition(|p| p.can_access_opener);
    show.into_iter()
        .map(PopupMove::ShowInPlace)
        .chain(moved.into_iter().map(PopupMove::MoveIntoTab))
        .collect()
}

/// Why the relay stops showing a popup.
#[derive(Debug, Clone, PartialEq)]
pub enum PopupEnd {
    /// It is gone: its opener closed it after the hand-back.
    Closed,
    /// It reached a room page itself (a channel switch): the tab that opened
    /// it takes that URL over.
    Landed(String),
}

/// Whether the popup `popup` is done, read from the tab list.
pub fn popup_end(pages: &[TargetInfo], popup: &str) -> Option<PopupEnd> {
    match pages.iter().find(|t| t.target_id == popup) {
        None => Some(PopupEnd::Closed),
        Some(t) if super::remember_url(&t.url) => Some(PopupEnd::Landed(t.url.clone())),
        Some(_) => None,
    }
}

/// Every tab `tab` opened, whatever its URL: closed together with `tab`, or
/// it would live on with no room that ever shows or closes it.
pub fn opened_by(pages: &[TargetInfo], tab: &str) -> Vec<String> {
    pages
        .iter()
        .filter(|t| t.opener_id.as_deref() == Some(tab))
        .map(|t| t.target_id.clone())
        .collect()
}

/// What the dashboard is told when the room switches away from a tab: a
/// file chooser of that tab is gone, and a file still being put into it may
/// not have arrived. Nothing when the whole relay is over (no viewer left).
pub fn switch_notices(relay_over: bool, chooser_pending: bool, file_in_flight: bool) -> Vec<Value> {
    let mut out = Vec::new();
    if relay_over {
        return out;
    }
    if chooser_pending {
        out.push(json!({ "t": "file-chooser-closed" }));
    }
    if file_in_flight {
        out.push(json!({
            "t": "file-error",
            "error": "okno sa počas vkladania prepnulo, vložte súbor znova"
        }));
    }
    out
}

/// The `{"t":"popup"}` message: whether the room shows a popup now, so the
/// dashboard offers the way back to the room.
pub fn popup_message(open: bool) -> Value {
    json!({ "t": "popup", "open": open })
}

#[cfg(test)]
#[path = "popups_tests.rs"]
mod tests;
