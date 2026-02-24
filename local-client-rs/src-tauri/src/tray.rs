use std::time::Duration;

use tauri::image::Image;
use tauri::menu::{MenuBuilder, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{App, AppHandle, Manager, Wry};

const SERVICE_URL: &str = "http://127.0.0.1:8910/api/v1";

/// Status polling interval (3 seconds).
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Embedded tray icon (32x32 PNG from bundle).
const TRAY_ICON_BYTES: &[u8] = include_bytes!("../icons/32x32.png");

pub fn setup_tray(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let version = env!("CARGO_PKG_VERSION");
    let icon = Image::from_bytes(TRAY_ICON_BYTES)?;

    let menu = build_menu(app, version, &TrayStatus::default())?;

    let _tray = TrayIconBuilder::with_id("restreamer")
        .icon(icon)
        .menu(&menu)
        .tooltip("Restreamer - Connecting...")
        .on_menu_event(move |app, event| handle_menu_event(app, event.id().as_ref()))
        .build(app)?;

    start_status_poller(app.handle().clone());

    Ok(())
}

struct TrayStatus {
    inpoint: String,
    buffer: String,
    endpoint: String,
    pending: u64,
    total_chunks: u64,
    event_name: String,
}

impl Default for TrayStatus {
    fn default() -> Self {
        Self {
            inpoint: "Connecting...".to_string(),
            buffer: "--:--:--".to_string(),
            endpoint: "Connecting...".to_string(),
            pending: 0,
            total_chunks: 0,
            event_name: String::new(),
        }
    }
}

fn build_menu(
    app: &impl Manager<Wry>,
    version: &str,
    status: &TrayStatus,
) -> Result<tauri::menu::Menu<Wry>, Box<dyn std::error::Error>> {
    let mut builder = MenuBuilder::new(app)
        .item(&MenuItem::new(
            app,
            format!("Restreamer v{version}"),
            false,
            None::<&str>,
        )?)
        .separator();

    // Show streaming event name if available
    if !status.event_name.is_empty() {
        builder = builder.item(&MenuItem::new(
            app,
            format!("Event: {}", status.event_name),
            false,
            None::<&str>,
        )?);
    }

    Ok(builder
        .item(&MenuItem::new(
            app,
            format!("Inpoint: {}", status.inpoint),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Buffer: {}", status.buffer),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Uploader: {}", status.endpoint),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Chunks: {} total, {} pending", status.total_chunks, status.pending),
            false,
            None::<&str>,
        )?)
        .separator()
        .item(&MenuItem::with_id(
            app,
            "open_dashboard",
            "Open Dashboard",
            true,
            None::<&str>,
        )?)
        .item(&MenuItem::with_id(
            app,
            "view_logs",
            "View Log",
            true,
            None::<&str>,
        )?)
        .separator()
        .item(&MenuItem::with_id(
            app,
            "restart_inpoint",
            "Restart Inpoint",
            true,
            None::<&str>,
        )?)
        .item(&MenuItem::with_id(
            app,
            "restart_endpoint",
            "Restart Endpoint",
            true,
            None::<&str>,
        )?)
        .item(&MenuItem::with_id(
            app,
            "delete_chunks",
            "Delete All Chunks",
            true,
            None::<&str>,
        )?)
        .separator()
        .item(&MenuItem::with_id(
            app,
            "check_updates",
            "Check for Updates...",
            true,
            None::<&str>,
        )?)
        .item(&MenuItem::with_id(
            app,
            "quit",
            "Quit",
            true,
            None::<&str>,
        )?)
        .build()?)
}

fn handle_menu_event(app: &AppHandle<Wry>, event_id: &str) {
    match event_id {
        "open_dashboard" => {
            if let Some(window) = app.get_webview_window("main") {
                if let Err(e) = window.show() {
                    tracing::warn!("Failed to show window: {e}");
                }
                if let Err(e) = window.set_focus() {
                    tracing::warn!("Failed to focus window: {e}");
                }
            }
        }
        "view_logs" => {
            // Open the Restreamer data directory in file explorer
            #[cfg(target_os = "windows")]
            {
                let log_dir = std::path::PathBuf::from(r"C:\ProgramData\Restreamer");
                if log_dir.exists() {
                    let _ = std::process::Command::new("explorer")
                        .arg(&log_dir)
                        .spawn();
                } else {
                    tracing::warn!("Log directory does not exist: {:?}", log_dir);
                }
            }
            #[cfg(not(target_os = "windows"))]
            {
                tracing::warn!("View logs not implemented for this platform");
            }
        }
        "restart_inpoint" => {
            tauri::async_runtime::spawn(async {
                let url = format!("{SERVICE_URL}/actions/restart-inpoint");
                if let Err(e) = reqwest::Client::new().post(&url).send().await {
                    tracing::warn!("Failed to restart inpoint: {e}");
                }
            });
        }
        "restart_endpoint" => {
            tauri::async_runtime::spawn(async {
                let url = format!("{SERVICE_URL}/actions/restart-endpoint");
                if let Err(e) = reqwest::Client::new().post(&url).send().await {
                    tracing::warn!("Failed to restart endpoint: {e}");
                }
            });
        }
        "delete_chunks" => {
            tauri::async_runtime::spawn(async {
                let url = format!("{SERVICE_URL}/chunks");
                if let Err(e) = reqwest::Client::new().delete(&url).send().await {
                    tracing::warn!("Failed to delete chunks: {e}");
                }
            });
        }
        "check_updates" => {
            let handle = app.clone();
            tauri::async_runtime::spawn(async move {
                crate::updater::manual_check(&handle).await;
            });
        }
        "quit" => {
            app.exit(0);
        }
        _ => {
            tracing::debug!("Unhandled menu event: {event_id}");
        }
    }
}

/// Format seconds as HH:MM:SS.
fn format_duration(secs: f64) -> String {
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

/// Poll the service status and update the single tray icon/menu dynamically.
fn start_status_poller(handle: AppHandle<Wry>) {
    tauri::async_runtime::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_default();
        let version = env!("CARGO_PKG_VERSION");

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            let status_url = format!("{SERVICE_URL}/status");
            let stats_url = format!("{SERVICE_URL}/chunks/stats");

            let (status_resp, stats_resp) = tokio::join!(
                client.get(&status_url).send(),
                client.get(&stats_url).send(),
            );

            let status_json: Option<serde_json::Value> = status_resp
                .ok()
                .and_then(|r| tauri::async_runtime::block_on(r.json()).ok());

            let stats_json: Option<serde_json::Value> = stats_resp
                .ok()
                .and_then(|r| tauri::async_runtime::block_on(r.json()).ok());

            let mut tray_status = TrayStatus::default();

            // Parse status response
            if let Some(val) = &status_json {
                // Get streaming event info
                if let Some(event) = val.get("streaming_event") {
                    if !event.is_null() {
                        tray_status.event_name = event
                            .get("short_description")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        let receiving = event
                            .get("receiving_activated")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        tray_status.inpoint = if receiving {
                            "Receiving".to_string()
                        } else {
                            "Paused".to_string()
                        };
                    } else {
                        tray_status.inpoint = "No Event".to_string();
                    }
                } else {
                    tray_status.inpoint = "Idle".to_string();
                }

                // Parse stats
                if let Some(stats) = &stats_json {
                    tray_status.buffer = stats
                        .get("buffer_duration_secs")
                        .and_then(|v| v.as_f64())
                        .map(format_duration)
                        .unwrap_or_else(|| "00:00:00".to_string());

                    tray_status.pending = stats
                        .get("pending_chunks")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    tray_status.total_chunks = stats
                        .get("total_chunks")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    tray_status.endpoint = if tray_status.pending > 0 {
                        "Uploading".to_string()
                    } else {
                        "Idle".to_string()
                    };
                }
            } else {
                tray_status.inpoint = "Disconnected".to_string();
                tray_status.endpoint = "Disconnected".to_string();
            }

            // Update the single tray icon
            if let Some(tray) = handle.tray_by_id("restreamer") {
                let tooltip = if !tray_status.event_name.is_empty() {
                    format!(
                        "Restreamer - {} | {} chunks",
                        tray_status.event_name, tray_status.total_chunks
                    )
                } else {
                    format!("Restreamer - {}", tray_status.inpoint)
                };
                let _ = tray.set_tooltip(Some(&tooltip));

                if let Ok(menu) = build_menu(&handle, version, &tray_status) {
                    let _ = tray.set_menu(Some(menu));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tray_icon_bytes_not_empty() {
        assert!(!TRAY_ICON_BYTES.is_empty());
    }

    #[test]
    fn tray_icon_bytes_valid_png() {
        // PNG magic bytes: 0x89 P N G
        assert_eq!(&TRAY_ICON_BYTES[..4], &[0x89, 0x50, 0x4E, 0x47]);
    }

    #[test]
    fn format_duration_zero() {
        assert_eq!(format_duration(0.0), "00:00:00");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(15.0), "00:00:15");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(125.0), "00:02:05");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(format_duration(3661.0), "01:01:01");
    }
}
