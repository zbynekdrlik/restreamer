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

    let menu = build_menu(app, version, "Waiting", "--:--:--", "Waiting", 0)?;

    let _tray = TrayIconBuilder::with_id("restreamer")
        .icon(icon)
        .menu(&menu)
        .tooltip("Restreamer")
        .on_menu_event(move |app, event| handle_menu_event(app, event.id().as_ref()))
        .build(app)?;

    start_status_poller(app.handle().clone());

    Ok(())
}

fn build_menu(
    app: &impl Manager<Wry>,
    version: &str,
    inpoint_status: &str,
    buffer: &str,
    endpoint_status: &str,
    pending: u64,
) -> Result<tauri::menu::Menu<Wry>, Box<dyn std::error::Error>> {
    Ok(MenuBuilder::new(app)
        .item(&MenuItem::new(
            app,
            format!("Restreamer v{version}"),
            false,
            None::<&str>,
        )?)
        .separator()
        .item(&MenuItem::new(
            app,
            format!("Inpoint: {inpoint_status}"),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Buffer: {buffer}"),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Endpoint: {endpoint_status}"),
            false,
            None::<&str>,
        )?)
        .item(&MenuItem::new(
            app,
            format!("Pending: {pending} chunks"),
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
        "open_dashboard" | "view_logs" => {
            if let Some(window) = app.get_webview_window("main") {
                if let Err(e) = window.show() {
                    tracing::warn!("Failed to show window: {e}");
                }
                if let Err(e) = window.set_focus() {
                    tracing::warn!("Failed to focus window: {e}");
                }
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

            // Determine inpoint state
            let (inpoint_status, buffer_text) = match &status_json {
                Some(val) => {
                    let has_event = val
                        .get("streaming_event")
                        .is_some_and(|v| !v.is_null());

                    let buffer = stats_json
                        .as_ref()
                        .and_then(|s| s.get("buffer_duration_secs"))
                        .and_then(|v| v.as_f64())
                        .map(format_duration)
                        .unwrap_or_else(|| "00:00:00".to_string());

                    if has_event {
                        ("Streaming", buffer)
                    } else {
                        ("Idle", buffer)
                    }
                }
                None => ("Disconnected", "--:--:--".to_string()),
            };

            // Determine endpoint state
            let (endpoint_status, pending_chunks) = match &status_json {
                Some(_) => {
                    let pending = stats_json
                        .as_ref()
                        .and_then(|s| s.get("pending_chunks"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    if pending > 0 {
                        ("Uploading", pending)
                    } else {
                        ("Idle", 0)
                    }
                }
                None => ("Disconnected", 0),
            };

            // Update the single tray icon
            if let Some(tray) = handle.tray_by_id("restreamer") {
                let _ = tray.set_tooltip(Some(&format!(
                    "Restreamer — Inpoint: {inpoint_status} | Endpoint: {endpoint_status}"
                )));

                if let Ok(menu) = build_menu(
                    &handle,
                    version,
                    inpoint_status,
                    &buffer_text,
                    endpoint_status,
                    pending_chunks,
                ) {
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
