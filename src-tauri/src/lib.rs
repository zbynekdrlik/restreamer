mod commands;
mod state;
mod tray;
mod tray_icons;
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
use rs_core::models::{InpointState, WsEvent};
use rs_runtime::{LogCaptureLayer, ServiceCore};

use crate::state::AppState;

#[cfg(windows)]
use tokio::signal::windows::{ctrl_c, ctrl_break};

/// Data directory path (platform-specific).
fn data_dir() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"C:\ProgramData\Restreamer")
    } else {
        PathBuf::from("/var/lib/restreamer")
    }
}

/// Initialize tracing with log capture and non-blocking file logging.
///
/// Returns a guard that keeps the non-blocking file writer alive.
/// The guard MUST be held for the lifetime of the process — dropping it
/// stops the background writer thread and loses buffered log lines.
fn init_tracing(log_buffer: &LogBuffer) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Log file path
    let log_path = data_dir().join("restreamer.log");

    // Ensure directory exists
    let _ = std::fs::create_dir_all(data_dir());

    // Simple rotation: rename to .old if > 1MB
    if let Ok(meta) = std::fs::metadata(&log_path) {
        if meta.len() > 1_000_000 {
            let _ = std::fs::rename(&log_path, log_path.with_extension("log.old"));
        }
    }

    // File layer with non-blocking writer.
    // Previously used std::sync::Mutex<File> which blocked ALL tokio tasks
    // when the file write stalled (Windows Defender, disk flush, etc.).
    // tracing_appender::non_blocking writes on a dedicated background thread
    // so logging never blocks the calling async task.
    let (file_layer, guard) = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(file) => {
            let (non_blocking, guard) = tracing_appender::non_blocking(file);
            let layer = tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(false);
            (Some(layer), Some(guard))
        }
        Err(e) => {
            eprintln!("Failed to open log file {}: {e}", log_path.display());
            (None, None)
        }
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(LogCaptureLayer::new(log_buffer.clone()))
        .with(file_layer)
        .init();

    guard
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
    let _log_guard = init_tracing(&log_buffer);

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
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
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

            // Shared RTMP connection state
            let inpoint_state = InpointState::new();

            // Clone values for the async block
            let config_clone = config.clone();
            let config_path_clone = config_path.clone();
            let log_buffer_clone = log_buffer.clone();
            let ws_tx_clone = ws_tx.clone();
            let inpoint_state_clone = inpoint_state.clone();

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
                // Clone the pool to share with ServiceCore (avoids duplicate pool creation)
                let pool_for_service = pool.clone();

                let app_state = AppState::new(
                    pool,
                    config_clone.clone(),
                    log_buffer_clone,
                    ws_tx_clone,
                    shutdown_tx,
                    inpoint_state_clone,
                );

                // Store state in Tauri
                handle.manage(Arc::new(app_state));

                tracing::info!("Embedded service state initialized");

                // Start the service core with shared inpoint state AND shared pool
                // This prevents the duplicate pool bug that caused SQLite lock conflicts
                let core = ServiceCore::with_inpoint_state(
                    config_clone,
                    config_path_clone,
                    LogBuffer::new(1000),
                    inpoint_state,
                )
                .with_pool(pool_for_service);

                if let Err(e) = core
                    .run_with_signal(async {
                        // Wait for shutdown signal from app
                        let _ = shutdown_rx.await;
                        tracing::info!("Shutdown signal received from app");
                    })
                    .await
                {
                    // Log full error chain for debugging
                    tracing::error!("Service error: {e}");
                    tracing::error!("Error chain: {e:#}");
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

/// Run in headless mode without GUI (for CI/service deployments).
/// This starts the ServiceCore directly without Tauri.
pub fn run_headless() {
    // Initialize logging early
    let log_buffer = LogBuffer::new(1000);
    let _log_guard = init_tracing(&log_buffer);

    tracing::info!("Starting Restreamer in headless mode");

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

    // Run the service core in a tokio runtime
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let core = ServiceCore::new(config, config_path, log_buffer);

        // Handle shutdown signals
        #[cfg(windows)]
        {
            let mut ctrl_c_signal =
                ctrl_c().expect("Failed to register Ctrl+C handler");
            let mut ctrl_break_signal =
                ctrl_break().expect("Failed to register Ctrl+Break handler");

            if let Err(e) = core
                .run_with_signal(async {
                    tokio::select! {
                        _ = ctrl_c_signal.recv() => {
                            tracing::info!("Received Ctrl+C, shutting down...");
                        }
                        _ = ctrl_break_signal.recv() => {
                            tracing::info!("Received Ctrl+Break, shutting down...");
                        }
                    }
                })
                .await
            {
                tracing::error!("Service error: {e}");
                std::process::exit(1);
            }
        }

        #[cfg(not(windows))]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())
                .expect("Failed to register SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

            if let Err(e) = core
                .run_with_signal(async {
                    tokio::select! {
                        _ = sigterm.recv() => {
                            tracing::info!("Received SIGTERM, shutting down...");
                        }
                        _ = sigint.recv() => {
                            tracing::info!("Received SIGINT, shutting down...");
                        }
                    }
                })
                .await
            {
                tracing::error!("Service error: {e}");
                std::process::exit(1);
            }
        }

        tracing::info!("Restreamer headless mode shutdown complete");
    });
}
