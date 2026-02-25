mod commands;
mod state;
mod tray;
mod updater;

use std::path::PathBuf;
use std::sync::Arc;

use tauri::{Manager, WindowEvent};
use tokio::sync::{broadcast, oneshot};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::WsEvent;
use rs_runtime::{LogCaptureLayer, ServiceCore};

use crate::state::AppState;

/// Initialize tracing with log capture.
fn init_tracing(log_buffer: &LogBuffer) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(LogCaptureLayer::new(log_buffer.clone()))
        .init();
}

/// Load configuration from the default path.
fn load_config() -> anyhow::Result<Config> {
    let config_path = Config::default_path();
    if config_path.exists() {
        Config::load(&config_path).map_err(|e| anyhow::anyhow!("failed to load config: {e}"))
    } else {
        tracing::warn!(
            "Config file not found at {}, using defaults",
            config_path.display()
        );
        Ok(Config::default())
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Initialize logging early
    let log_buffer = LogBuffer::new(1000);
    init_tracing(&log_buffer);

    // Load configuration
    let config = match load_config() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to load config: {e}");
            eprintln!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    // Validate configuration
    if let Err(e) = config.validate() {
        tracing::error!("Invalid config: {e}");
        eprintln!("Invalid config: {e}");
        std::process::exit(1);
    }

    let config_path = Config::default_path();

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
        .setup(move |app| {
            let handle = app.handle().clone();

            // Create shutdown channel for the embedded service
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            // WebSocket broadcast channel
            let (ws_tx, _) = broadcast::channel::<WsEvent>(256);

            // Clone values for the async block
            let config_clone = config.clone();
            let config_path_clone = config_path.clone();
            let log_buffer_clone = log_buffer.clone();
            let ws_tx_clone = ws_tx.clone();

            // Start the embedded service in a background task
            tauri::async_runtime::spawn(async move {
                // Initialize database first to get the pool
                let data_dir = if cfg!(windows) {
                    PathBuf::from(r"C:\ProgramData\Restreamer")
                } else {
                    PathBuf::from("/var/lib/restreamer")
                };

                // Ensure data directory exists
                if let Err(e) = tokio::fs::create_dir_all(&data_dir).await {
                    tracing::error!("Failed to create data directory: {e}");
                    return;
                }

                let db_path = data_dir.join("restreamer.db");

                // Create database pool
                let pool = match db::create_pool(&db_path).await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("Failed to create database pool: {e}");
                        return;
                    }
                };

                // Run migrations
                if let Err(e) = db::run_migrations(&pool).await {
                    tracing::error!("Failed to run migrations: {e}");
                    return;
                }

                // Create app state with direct database access
                let app_state = AppState::new(
                    pool,
                    config_clone.clone(),
                    log_buffer_clone,
                    ws_tx_clone,
                    shutdown_tx,
                );

                // Store state in Tauri
                handle.manage(Arc::new(app_state));

                tracing::info!("Embedded service state initialized");

                // Start the service core
                let core = ServiceCore::new(config_clone, config_path_clone, LogBuffer::new(1000));

                if let Err(e) = core
                    .run_with_signal(async {
                        // Wait for shutdown signal from app
                        let _ = shutdown_rx.await;
                        tracing::info!("Shutdown signal received from app");
                    })
                    .await
                {
                    tracing::error!("Service error: {e}");
                }
            });

            // Set up system tray
            tray::setup_tray(app)?;

            // Start update checker
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
            commands::get_status,
            commands::get_chunk_stats,
            commands::get_streaming_event,
            commands::get_logs,
            commands::get_config,
        ])
        .run(tauri::generate_context!())
    {
        eprintln!("Tauri application failed to start: {e}");
        std::process::exit(1);
    }
}
