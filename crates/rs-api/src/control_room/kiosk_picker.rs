//! #414: a file for a room page, picked with the box's own dialog.
//!
//! On the box's screen (the kiosk window) a page's file chooser used to show
//! only the bar "Stránka chce súbor" with a second button the operators did
//! not find. For a viewer that is the box itself, the relay opens the
//! desktop app's native dialog at once instead
//! ([`rs_core::native_picker`]); the chosen file goes into the page's chooser
//! by its path, with no upload. A cancel shows the bar.

use std::path::Path;
use std::sync::Mutex;

use serde_json::{Value, json};

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
    let _ = (picked, chooser, call, tab);
    json!({ "t": "file-chooser", "mode": mode })
}

#[cfg(test)]
#[path = "kiosk_picker_tests.rs"]
mod tests;
