use anyhow::Context;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use rs_core::config::Config;
use rs_core::log_buffer::LogBuffer;
use rs_runtime::{LogCaptureLayer, ServiceCore};

#[cfg(windows)]
fn main() -> anyhow::Result<()> {
    // On Windows, run as a Windows Service unless --console flag is passed
    if std::env::args().any(|a| a == "--console") {
        return run_console();
    }

    use windows_service::service_dispatcher;
    service_dispatcher::start("RestreamerService", ffi_service_main)
        .map_err(|e| anyhow::anyhow!("Failed to start service dispatcher: {e}"))
}

#[cfg(not(windows))]
fn main() -> anyhow::Result<()> {
    run_console()
}

// --- Windows Service support ---

#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, windows_service_main);

#[cfg(windows)]
fn windows_service_main(_arguments: Vec<std::ffi::OsString>) {
    if let Err(e) = run_windows_service() {
        eprintln!("Service error: {e}");
    }
}

#[cfg(windows)]
fn run_windows_service() -> anyhow::Result<()> {
    use std::time::Duration;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};

    // Create a channel to receive the stop signal from SCM
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stop_tx = std::sync::Mutex::new(Some(stop_tx));

    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Stop => {
                if let Some(tx) = stop_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };

    let status_handle = service_control_handler::register("RestreamerService", event_handler)?;

    // Initialize logging and load config
    let log_buffer = LogBuffer::new(1000);
    init_tracing(&log_buffer);

    let config_path = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(Config::default_path);

    let config = load_config(&config_path)?;
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid config: {e}"))?;

    // Report running to SCM
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    // Run the service until SCM sends stop
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = rt.block_on(async {
        ServiceCore::new(config, config_path, log_buffer)
            .run_with_signal(async {
                let _ = stop_rx.await;
            })
            .await
    });

    // Report stopped to SCM
    let _ = status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    });

    result
}

// --- Console mode (Linux, macOS, Windows with --console) ---

fn run_console() -> anyhow::Result<()> {
    let log_buffer = LogBuffer::new(1000);
    init_tracing(&log_buffer);

    let config_path = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(Config::default_path);

    let config = load_config(&config_path)?;
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid config: {e}"))?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            ServiceCore::new(config, config_path, log_buffer)
                .run()
                .await
        })
}

// --- Shared helpers ---

fn init_tracing(log_buffer: &LogBuffer) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(LogCaptureLayer::new(log_buffer.clone()))
        .init();
}

fn load_config(config_path: &std::path::Path) -> anyhow::Result<Config> {
    if config_path.exists() {
        Config::load(config_path).context("failed to load config")
    } else {
        tracing::error!(
            "Config file not found at {}. Create a config file or set environment variables.",
            config_path.display()
        );
        Ok(Config::default())
    }
}
