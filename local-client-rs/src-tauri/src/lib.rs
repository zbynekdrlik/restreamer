mod commands;
mod tray;
mod updater;

use tauri::{Manager, WindowEvent};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    if let Err(e) = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            // Focus existing window when a second instance is launched
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .setup(|app| {
            tray::setup_tray(app)?;
            updater::start_update_checker(app.handle());
            Ok(())
        })
        .on_window_event(|window, event| {
            // Hide window instead of closing - keeps tray app running
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_service_status,
            commands::get_service_url,
        ])
        .run(tauri::generate_context!())
    {
        eprintln!("Tauri application failed to start: {e}");
        std::process::exit(1);
    }
}
