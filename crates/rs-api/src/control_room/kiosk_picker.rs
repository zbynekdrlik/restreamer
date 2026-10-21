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

use rs_core::native_picker::NativeFilePicker;
use serde_json::{Value, json};

use super::relay::file_actions;

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
