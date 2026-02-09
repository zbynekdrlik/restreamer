mod commands;
mod tray;
mod updater;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    if let Err(e) = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .setup(|app| {
            tray::setup_tray(app)?;
            updater::start_update_checker(&app.handle());
            Ok(())
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
