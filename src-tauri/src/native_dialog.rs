//! The box's own "open file" dialog for the control rooms (#414).
//!
//! A page in a control room asked for a file ("Upload thumbnail") while the
//! room is shown in this very window. The relay opens this dialog at once and
//! puts the chosen file into the page by its path. On Linux it is rfd's GTK
//! dialog inside this process (no desktop portal: the kiosk's portal could not
//! attach its dialog to the window, stream-pp 2026-10-21).

use std::sync::Arc;

use rs_core::native_picker::{NativeFilePicker, Pick};
use tauri::{AppHandle, Manager};
use tauri_plugin_dialog::DialogExt;

/// The dialog's title, in the operators' language.
const TITLE: &str = "Vyber súbor pre stránku v miestnosti";

struct DialogPicker(AppHandle);

impl NativeFilePicker for DialogPicker {
    fn pick_file(&self) -> Pick {
        let app = self.0.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mut dialog = app.dialog().file().set_title(TITLE);
            // Over the dashboard window, which it belongs to.
            match app.get_webview_window("main") {
                Some(window) => dialog = dialog.set_parent(&window),
                None => tracing::warn!("box file dialog: no main window to put it over"),
            }
            dialog.pick_file(move |file| {
                let _ = tx.send(file);
            });
            let Some(file) = rx.await.ok().flatten() else {
                tracing::info!("box file dialog: cancelled");
                return None;
            };
            match file.into_path() {
                Ok(path) => {
                    tracing::info!("box file dialog: picked {}", path.display());
                    Some(path)
                }
                Err(e) => {
                    tracing::warn!("box file dialog: the pick is not a local file: {e}");
                    None
                }
            }
        })
    }
}

/// Makes the dialog available to the control rooms of this process.
pub fn install(app: &AppHandle) {
    if rs_core::native_picker::install(Arc::new(DialogPicker(app.clone()))) {
        tracing::info!("control rooms: the box's file dialog is installed (#414)");
    } else {
        tracing::warn!("control rooms: a box file dialog was already installed");
    }
}
