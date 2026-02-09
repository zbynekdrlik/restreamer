use std::time::Duration;

use tauri::image::Image;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{App, AppHandle, Manager, Wry};

const SERVICE_URL: &str = "http://127.0.0.1:8910/api/v1";

/// Status polling interval (3 seconds).
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Generate a 32x32 RGBA icon with the given color.
fn make_icon(r: u8, g: u8, b: u8) -> Vec<u8> {
    let size = 32usize;
    let mut pixels = vec![0u8; size * size * 4];
    for y in 0..size {
        for x in 0..size {
            let idx = (y * size + x) * 4;
            let cx = (x as f64 - 16.0).abs();
            let cy = (y as f64 - 16.0).abs();
            let dist = (cx * cx + cy * cy).sqrt();
            if dist < 14.0 {
                pixels[idx] = r;
                pixels[idx + 1] = g;
                pixels[idx + 2] = b;
                pixels[idx + 3] = 255;
            }
        }
    }
    pixels
}

pub fn setup_tray(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let version = env!("CARGO_PKG_VERSION");

    // Inpoint tray icon
    let inpoint_menu = build_inpoint_menu(app, version, "Waiting", "--:--:--")?;
    let inpoint_icon_data = make_icon(255, 165, 0); // Orange = waiting
    let inpoint_icon = Image::new_owned(inpoint_icon_data, 32, 32);

    let _inpoint_tray = TrayIconBuilder::with_id("inpoint")
        .icon(inpoint_icon)
        .menu(&inpoint_menu)
        .tooltip("Restreamer Inpoint")
        .on_menu_event(move |app, event| handle_inpoint_event(app, event.id().as_ref()))
        .build(app)?;

    // Endpoint tray icon
    let endpoint_menu = build_endpoint_menu(app, "Waiting", 0)?;
    let endpoint_icon_data = make_icon(255, 165, 0); // Orange = waiting
    let endpoint_icon = Image::new_owned(endpoint_icon_data, 32, 32);

    let _endpoint_tray = TrayIconBuilder::with_id("endpoint")
        .icon(endpoint_icon)
        .menu(&endpoint_menu)
        .tooltip("Restreamer Endpoint")
        .on_menu_event(move |_app, event| handle_endpoint_event(_app, event.id().as_ref()))
        .build(app)?;

    // Start background status poller
    start_status_poller(app.handle().clone());

    Ok(())
}

fn build_inpoint_menu(
    app: &impl Manager<Wry>,
    version: &str,
    status: &str,
    buffer: &str,
) -> Result<Menu<Wry>, Box<dyn std::error::Error>> {
    Ok(Menu::with_items(
        app,
        &[
            &MenuItem::new(app, format!("Restreamer v{version}"), false, None::<&str>)?,
            &MenuItem::new(app, format!("Status: {status}"), false, None::<&str>)?,
            &MenuItem::new(app, format!("Buffer: {buffer}"), false, None::<&str>)?,
            &MenuItem::with_id(app, "open_dashboard", "Open Dashboard", true, None::<&str>)?,
            &MenuItem::with_id(app, "view_logs", "View Log", true, None::<&str>)?,
            &MenuItem::with_id(
                app,
                "restart_inpoint",
                "Restart Inpoint",
                true,
                None::<&str>,
            )?,
            &MenuItem::with_id(
                app,
                "delete_chunks",
                "Delete All Chunks",
                true,
                None::<&str>,
            )?,
            &MenuItem::with_id(
                app,
                "check_updates",
                "Check for Updates...",
                true,
                None::<&str>,
            )?,
            &MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?,
        ],
    )?)
}

fn build_endpoint_menu(
    app: &impl Manager<Wry>,
    status: &str,
    pending: u64,
) -> Result<Menu<Wry>, Box<dyn std::error::Error>> {
    Ok(Menu::with_items(
        app,
        &[
            &MenuItem::new(app, format!("Status: {status}"), false, None::<&str>)?,
            &MenuItem::new(
                app,
                format!("Pending: {pending} chunks"),
                false,
                None::<&str>,
            )?,
            &MenuItem::with_id(
                app,
                "restart_endpoint",
                "Restart Endpoint",
                true,
                None::<&str>,
            )?,
        ],
    )?)
}

fn handle_inpoint_event(app: &AppHandle<Wry>, event_id: &str) {
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

fn handle_endpoint_event(app: &AppHandle<Wry>, event_id: &str) {
    match event_id {
        "restart_endpoint" => {
            tauri::async_runtime::spawn(async {
                let url = format!("{SERVICE_URL}/actions/restart-endpoint");
                if let Err(e) = reqwest::Client::new().post(&url).send().await {
                    tracing::warn!("Failed to restart endpoint: {e}");
                }
            });
        }
        _ => {
            tracing::debug!("Unhandled endpoint menu event: {event_id}");
        }
    }
    // Suppress unused variable warning in release builds
    let _ = app;
}

/// Format seconds as HH:MM:SS.
fn format_duration(secs: f64) -> String {
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

/// Poll the service status and update tray icons/menus dynamically.
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
            let (inpoint_r, inpoint_g, inpoint_b, inpoint_status, buffer_text) =
                match &status_json {
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
                            (0u8, 200u8, 0u8, "Streaming", buffer)
                        } else {
                            (255, 165, 0, "Idle", buffer)
                        }
                    }
                    None => (255, 0, 0, "Disconnected", "--:--:--".to_string()),
                };

            // Determine endpoint state
            let (endpoint_r, endpoint_g, endpoint_b, endpoint_status, pending_chunks) =
                match &status_json {
                    Some(_) => {
                        let pending = stats_json
                            .as_ref()
                            .and_then(|s| s.get("pending_chunks"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);

                        if pending > 0 {
                            (0u8, 200u8, 0u8, "Uploading", pending)
                        } else {
                            (255, 165, 0, "Idle", 0)
                        }
                    }
                    None => (255, 0, 0, "Disconnected", 0),
                };

            // Update inpoint tray
            if let Some(tray) = handle.tray_by_id("inpoint") {
                let icon_data = make_icon(inpoint_r, inpoint_g, inpoint_b);
                let icon = Image::new_owned(icon_data, 32, 32);
                let _ = tray.set_icon(Some(icon));
                let _ = tray.set_tooltip(Some(&format!("Restreamer Inpoint — {inpoint_status}")));

                if let Ok(menu) =
                    build_inpoint_menu(&handle, version, inpoint_status, &buffer_text)
                {
                    let _ = tray.set_menu(Some(menu));
                }
            }

            // Update endpoint tray
            if let Some(tray) = handle.tray_by_id("endpoint") {
                let icon_data = make_icon(endpoint_r, endpoint_g, endpoint_b);
                let icon = Image::new_owned(icon_data, 32, 32);
                let _ = tray.set_icon(Some(icon));
                let _ = tray.set_tooltip(Some(&format!(
                    "Restreamer Endpoint — {endpoint_status}"
                )));

                if let Ok(menu) =
                    build_endpoint_menu(&handle, endpoint_status, pending_chunks)
                {
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
    fn make_icon_produces_correct_size() {
        let icon = make_icon(255, 0, 0);
        assert_eq!(icon.len(), 32 * 32 * 4);
    }

    #[test]
    fn make_icon_has_nonzero_alpha() {
        let icon = make_icon(0, 255, 0);
        let has_visible = icon.chunks(4).any(|pixel| pixel[3] > 0);
        assert!(has_visible);
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
