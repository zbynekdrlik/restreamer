//! The box's own "open file" dialog for the control rooms (#414).
//!
//! A page in a control room asked for a file ("Upload thumbnail") right after
//! the operator clicked in this very window. The relay opens this dialog and
//! puts the chosen file into the page by its path.
//!
//! On Linux it is a GTK dialog inside this process, transient for and modal
//! over the dashboard window, and kept above it: the kiosk (openbox, a
//! maximized window) must never raise the dashboard over an open dialog.
//! rfd's GTK backend (tauri-plugin-dialog) ignores the parent there, and the
//! desktop portal could not attach its dialog to the window at all
//! (stream-pp, 2026-10-21). Elsewhere tauri-plugin-dialog's native dialog
//! takes the window as its owner.

use std::path::PathBuf;
use std::sync::Arc;

use rs_core::native_picker::{NativeFilePicker, Pick};
use tauri::AppHandle;

/// The dialog's title, in the operators' language.
const TITLE: &str = "Vyber súbor pre stránku v miestnosti";

struct DialogPicker(AppHandle);

impl NativeFilePicker for DialogPicker {
    fn pick_file(&self) -> Pick {
        let app = self.0.clone();
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            show(&app, tx);
            match rx.await.ok().flatten() {
                Some(path) => {
                    tracing::info!("box file dialog: picked {}", path.display());
                    Some(path)
                }
                None => {
                    tracing::info!("box file dialog: cancelled");
                    None
                }
            }
        })
    }
}

/// Shows the dialog on the GTK main thread; `tx` gets the chosen file.
#[cfg(target_os = "linux")]
fn show(app: &AppHandle, tx: tokio::sync::oneshot::Sender<Option<PathBuf>>) {
    use gtk::prelude::*;
    use tauri::Manager;

    let handle = app.clone();
    let shown = app.run_on_main_thread(move || {
        let parent = match handle.get_webview_window("main").map(|w| w.gtk_window()) {
            Some(Ok(window)) => Some(window),
            Some(Err(e)) => {
                tracing::warn!("box file dialog: no GTK window to put it over: {e}");
                None
            }
            None => {
                tracing::warn!("box file dialog: no main window to put it over");
                None
            }
        };
        let dialog = gtk::FileChooserDialog::with_buttons(
            Some(TITLE),
            parent.as_ref(),
            gtk::FileChooserAction::Open,
            &[
                ("_Zrušiť", gtk::ResponseType::Cancel),
                ("_Vybrať", gtk::ResponseType::Accept),
            ],
        );
        dialog.set_modal(true);
        dialog.set_keep_above(true);
        dialog.set_default_response(gtk::ResponseType::Accept);
        let tx = std::sync::Mutex::new(Some(tx));
        dialog.connect_response(move |dialog, response| {
            let path = (response == gtk::ResponseType::Accept)
                .then(|| dialog.filename())
                .flatten();
            if let Some(tx) = tx.lock().unwrap_or_else(|p| p.into_inner()).take() {
                let _ = tx.send(path);
            }
            // SAFETY: the dialog is ours, made above, and nothing uses it
            // after this response.
            unsafe { dialog.destroy() };
        });
        dialog.show_all();
        dialog.present();
    });
    if let Err(e) = shown {
        // `tx` went with the closure: the pick reads as cancelled.
        tracing::error!("box file dialog: cannot reach the GTK main thread: {e}");
    }
}

/// Shows the dialog with the dashboard window as its owner; `tx` gets the
/// chosen file.
#[cfg(not(target_os = "linux"))]
fn show(app: &AppHandle, tx: tokio::sync::oneshot::Sender<Option<PathBuf>>) {
    use tauri::Manager;
    use tauri_plugin_dialog::DialogExt;

    let mut dialog = app.dialog().file().set_title(TITLE);
    match app.get_webview_window("main") {
        Some(window) => dialog = dialog.set_parent(&window),
        None => tracing::warn!("box file dialog: no main window to put it over"),
    }
    dialog.pick_file(move |file| {
        let path = file.and_then(|f| match f.into_path() {
            Ok(path) => Some(path),
            Err(e) => {
                tracing::warn!("box file dialog: the pick is not a local file: {e}");
                None
            }
        });
        let _ = tx.send(path);
    });
}

/// Makes the dialog available to the control rooms of this process.
pub fn install(app: &AppHandle) {
    if rs_core::native_picker::install(Arc::new(DialogPicker(app.clone()))) {
        tracing::info!("control rooms: the box's file dialog is installed (#414)");
    } else {
        tracing::warn!("control rooms: a box file dialog was already installed");
    }
}
