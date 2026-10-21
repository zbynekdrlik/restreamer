//! #414: a file for a room page, picked with the box's own dialog.
//!
//! On the box's screen (the kiosk window) a page's file chooser used to show
//! only the bar "Stránka chce súbor" with a second button the operators did
//! not find. For a viewer that is the box itself, the relay opens the
//! desktop app's native dialog at once instead
//! ([`rs_core::native_picker`]); the chosen file goes into the page's chooser
//! by its path, with no upload. A cancel shows the bar.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rs_core::native_picker::NativeFilePicker;
use serde_json::{Value, json};

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
    let _ = since_own_input;
    node.is_some()
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
