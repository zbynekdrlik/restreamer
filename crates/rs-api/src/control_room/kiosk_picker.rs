//! #414: a file for a room page, picked with the box's own dialog.
//!
//! On the box's screen (the kiosk window) a page's file chooser used to show
//! only the bar "Stránka chce súbor" with a second button the operators did
//! not find. For a viewer that is the box itself, the relay opens the
//! desktop app's native dialog at once instead
//! ([`rs_core::native_picker`]); the chosen file goes into the page's chooser
//! by its path, with no upload. A cancel shows the bar.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::Message as AxMessage;
use rs_core::native_picker::NativeFilePicker;
use serde_json::{Value, json};
use tokio::sync::mpsc::WeakUnboundedSender;

use super::cdp;
use super::relay::file_actions;

/// The box's own dialog, and whether it is open: one for the whole box,
/// whichever room or viewer asked.
#[derive(Clone)]
pub struct BoxDialog {
    pub picker: Arc<dyn NativeFilePicker>,
    pub open: Arc<AtomicBool>,
}

/// How long after the viewer's own click or key a page's file chooser counts
/// as theirs.
pub const OWN_INPUT_WINDOW: Duration = Duration::from_secs(3);

/// Whether a page's file chooser opens the box dialog: an `<input>` waits for
/// the file (`node`), and it followed this viewer's own click or key within
/// [`OWN_INPUT_WINDOW`] (`since_own_input`), not the page by itself and not
/// another viewer of the same tab.
pub fn opens_box_dialog(node: Option<i64>, since_own_input: Option<Duration>) -> bool {
    node.is_some() && since_own_input.is_some_and(|d| d <= OWN_INPUT_WINDOW)
}

/// A dashboard message that is the viewer's own click or key in the page.
pub fn is_own_press(v: &Value) -> bool {
    matches!(
        (v["t"].as_str(), v["type"].as_str()),
        (Some("m"), Some("mousePressed")) | (Some("k"), Some("keyDown" | "rawKeyDown"))
    )
}

/// Where a pick goes: the tab's pending chooser, the page (weakly, with the
/// relay's CDP request ids) and the viewer (weakly).
pub struct PickTarget {
    pub chooser: Arc<Mutex<Option<i64>>>,
    pub page: WeakUnboundedSender<String>,
    pub seq: Arc<AtomicU64>,
    pub viewer: WeakUnboundedSender<AxMessage>,
    pub tab: String,
}

/// Opens the box's dialog in its own task and answers the viewer when it
/// closed. The task holds the page and the viewer only weakly: a relay that
/// ended while the dialog was open lets its CDP session go at once (no
/// screencast or focus emulation kept on a room nobody views), and a late
/// pick then goes nowhere. `dialog.open` was set by the caller.
pub fn spawn_pick(dialog: BoxDialog, mode: String, to: PickTarget) {
    let PickTarget {
        chooser,
        page,
        seq,
        viewer,
        tab,
    } = to;
    tokio::spawn(async move {
        let call = |method: &str, params: Value| match page.upgrade() {
            Some(page) => {
                let id = cdp::next_id(&seq);
                let _ = page.send(cdp::request(id, method, params));
            }
            None => tracing::info!("control rooms: tab {tab}: left before the dialog closed"),
        };
        let reply =
            pick_for_page(dialog.picker, mode, chooser, call, dialog.open, tab.clone()).await;
        if let Some(viewer) = viewer.upgrade() {
            let _ = viewer.send(AxMessage::Text(reply.to_string().into()));
        }
    });
}

/// Opens the box's dialog for the page's pending chooser and returns the
/// dashboard message once it closed. `picking` was set by the caller and is
/// cleared when the dialog closes.
pub async fn pick_for_page(
    picker: Arc<dyn NativeFilePicker>,
    mode: String,
    chooser: Arc<Mutex<Option<i64>>>,
    call: impl Fn(&str, Value),
    picking: Arc<AtomicBool>,
    tab: String,
) -> Value {
    tracing::info!("control rooms: tab {tab}: the page wants a file; opening the box dialog");
    let picked = picker.pick_file().await;
    picking.store(false, Ordering::SeqCst);
    picked_reply(picked.as_deref(), &mode, &chooser, &call, &tab)
}

/// The dashboard message once the box's dialog closed. A chosen file goes
/// into the page's pending chooser; a cancel shows the bar, for a drop or
/// another try.
pub fn picked_reply(
    picked: Option<&Path>,
    mode: &str,
    chooser: &Mutex<Option<i64>>,
    call: &impl Fn(&str, Value),
    tab: &str,
) -> Value {
    let Some(path) = picked else {
        tracing::info!("control rooms: tab {tab}: the box dialog was cancelled; the bar shows");
        return json!({ "t": "file-chooser", "mode": mode });
    };
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let Some(node) = chooser.lock().unwrap_or_else(|p| p.into_inner()).take() else {
        tracing::warn!(
            "control rooms: tab {tab}: {name} was picked, but the page no longer waits for a file"
        );
        return json!({
            "t": "file-error",
            "error": "stránka už súbor nečaká (medzitým sa zmenila); klikni na nahranie znova",
        });
    };
    tracing::info!("control rooms: tab {tab}: {name} into the page by the box dialog");
    for (method, params) in file_actions(&path.to_string_lossy(), Some(node), 0.0, 0.0) {
        call(method, params);
    }
    json!({ "t": "file-done", "name": name, "how": "picker" })
}

#[cfg(test)]
#[path = "kiosk_picker_tests.rs"]
mod tests;
