//! System tray with direct state access (no HTTP polling).
//!
//! Menu is built once at startup. Status items update in-place via
//! `MenuItem::set_text()` — this never closes an open popup menu.

use std::sync::Arc;
use std::time::Duration;

use tauri::image::Image;
use tauri::menu::{MenuBuilder, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{App, AppHandle, Manager, Wry};

use tauri_plugin_clipboard_manager::ClipboardExt;
use tauri_plugin_dialog::{DialogExt, MessageDialogButtons};

use crate::state::AppState;

/// Status polling interval (3 seconds).
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Embedded tray icon (32x32 PNG from bundle).
const TRAY_ICON_BYTES: &[u8] = include_bytes!("../icons/32x32.png");

/// Holds references to menu items whose text changes at runtime.
struct DynamicMenuItems {
    event: MenuItem<Wry>,
    inpoint: MenuItem<Wry>,
    buffer: MenuItem<Wry>,
    uploader: MenuItem<Wry>,
    chunks: MenuItem<Wry>,
}

pub fn setup_tray(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let version = env!("CARGO_PKG_VERSION");
    let icon = Image::from_bytes(TRAY_ICON_BYTES)?;

    // Static header
    let header = MenuItem::new(app, format!("Restreamer v{version}"), false, None::<&str>)?;

    // Dynamic status items (disabled — display only)
    let event_item = MenuItem::new(app, "No active event", false, None::<&str>)?;
    let inpoint_item = MenuItem::new(app, "Inpoint: Starting...", false, None::<&str>)?;
    let buffer_item = MenuItem::new(app, "Buffer: --:--:--", false, None::<&str>)?;
    let uploader_item = MenuItem::new(app, "Uploader: Starting...", false, None::<&str>)?;
    let chunks_item = MenuItem::new(app, "Chunks: 0 sent, 0 pending", false, None::<&str>)?;

    // Action items
    let open_dashboard =
        MenuItem::with_id(app, "open_dashboard", "Open Dashboard", true, None::<&str>)?;
    let copy_rtmp =
        MenuItem::with_id(app, "copy_rtmp_url", "Copy RTMP URL", true, None::<&str>)?;
    let copy_api =
        MenuItem::with_id(app, "copy_api_url", "Copy API URL", true, None::<&str>)?;
    let view_logs = MenuItem::with_id(app, "view_logs", "View Live Log", true, None::<&str>)?;
    let clear_chunks = MenuItem::with_id(
        app,
        "clear_pending_chunks",
        "Clear Pending Chunks...",
        true,
        None::<&str>,
    )?;
    let check_updates =
        MenuItem::with_id(app, "check_updates", "Check for Updates...", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;

    let menu = MenuBuilder::new(app)
        .item(&header)
        .separator()
        .item(&event_item)
        .item(&inpoint_item)
        .item(&buffer_item)
        .item(&uploader_item)
        .item(&chunks_item)
        .separator()
        .item(&open_dashboard)
        .item(&copy_rtmp)
        .item(&copy_api)
        .item(&view_logs)
        .item(&clear_chunks)
        .separator()
        .item(&check_updates)
        .item(&quit)
        .build()?;

    let _tray = TrayIconBuilder::with_id("restreamer")
        .icon(icon)
        .menu(&menu)
        .tooltip("Restreamer - Starting...")
        .on_menu_event(move |app, event| handle_menu_event(app, event.id().as_ref()))
        .build(app)?;

    let items = DynamicMenuItems {
        event: event_item,
        inpoint: inpoint_item,
        buffer: buffer_item,
        uploader: uploader_item,
        chunks: chunks_item,
    };

    start_status_updater(app.handle().clone(), items);

    Ok(())
}

/// Build a rich multi-line tooltip with detailed stats.
fn build_tooltip(status: &TrayStatus) -> String {
    let title = if !status.event_name.is_empty() {
        format!("Restreamer \u{2014} {}", status.event_name)
    } else {
        "Restreamer".to_string()
    };

    let state = match status.inpoint.as_str() {
        "Receiving" => format!("Receiving | Buffer: {}", status.buffer),
        "Paused" => "Idle".to_string(),
        "No Event" => "No active event".to_string(),
        "Error" => "Error".to_string(),
        _ => "Starting...".to_string(),
    };

    let chunks = format!(
        "Chunks: {} sent, {} pending",
        status.sent_chunks, status.pending
    );

    format!("{title}\n{state}\n{chunks}")
}

struct TrayStatus {
    inpoint: String,
    buffer: String,
    endpoint: String,
    pending: u64,
    sent_chunks: u64,
    event_name: String,
}

impl Default for TrayStatus {
    fn default() -> Self {
        Self {
            inpoint: "Starting...".to_string(),
            buffer: "--:--:--".to_string(),
            endpoint: "Starting...".to_string(),
            pending: 0,
            sent_chunks: 0,
            event_name: String::new(),
        }
    }
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
        "copy_rtmp_url" => {
            let url = "rtmp://localhost:1234/live";
            match app.clipboard().write_text(url) {
                Ok(()) => tracing::info!("Copied RTMP URL to clipboard: {url}"),
                Err(e) => tracing::error!("Failed to copy to clipboard: {e}"),
            }
        }
        "copy_api_url" => {
            let url = "http://127.0.0.1:8910/api/v1/status";
            match app.clipboard().write_text(url) {
                Ok(()) => tracing::info!("Copied API URL to clipboard: {url}"),
                Err(e) => tracing::error!("Failed to copy to clipboard: {e}"),
            }
        }
        "clear_pending_chunks" => {
            let handle = app.clone();
            tauri::async_runtime::spawn(async move {
                let stats = if let Some(state) = handle.try_state::<Arc<AppState>>() {
                    state.get_chunk_stats().await.ok()
                } else {
                    None
                };

                let (total, pending) = stats
                    .map(|s| (s.total_chunks, s.pending_chunks))
                    .unwrap_or((0, 0));

                let msg = format!(
                    "Delete all chunk records ({} total, {} unsent)?\n\n\
                     This resets stats to zero and cannot be undone.\n\
                     Use before starting a new live stream.",
                    total, pending
                );

                let confirmed = handle
                    .dialog()
                    .message(msg)
                    .title("Clear Pending Chunks")
                    .buttons(MessageDialogButtons::OkCancelCustom(
                        "Clear".to_string(),
                        "Cancel".to_string(),
                    ))
                    .blocking_show();

                if confirmed {
                    if let Some(state) = handle.try_state::<Arc<AppState>>() {
                        match state.clear_all_chunks().await {
                            Ok(count) => tracing::info!("Cleared {count} chunk records"),
                            Err(e) => tracing::error!("Failed to clear chunks: {e}"),
                        }
                    }
                }
            });
        }
        "view_logs" => {
            // Open a live-tailing log window
            #[cfg(target_os = "windows")]
            {
                let _ = std::process::Command::new("powershell.exe")
                    .args([
                        "-NoExit",
                        "-Command",
                        "Get-Content 'C:\\ProgramData\\Restreamer\\restreamer.log' -Tail 50 -Wait",
                    ])
                    .spawn();
            }
            #[cfg(not(target_os = "windows"))]
            {
                let _ = std::process::Command::new("x-terminal-emulator")
                    .args([
                        "-e", "tail", "-n", "50", "-f",
                        "/var/lib/restreamer/restreamer.log",
                    ])
                    .spawn();
            }
        }
        "check_updates" => {
            let handle = app.clone();
            tauri::async_runtime::spawn(async move {
                crate::updater::manual_check(&handle).await;
            });
        }
        "quit" => {
            // Trigger shutdown of embedded service
            let handle = app.clone();
            tauri::async_runtime::spawn(async move {
                if let Some(state) = handle.try_state::<Arc<AppState>>() {
                    state.shutdown().await;
                }
                // Give service time to shut down
                tokio::time::sleep(Duration::from_millis(500)).await;
                handle.exit(0);
            });
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

/// Poll the service status directly from AppState (no HTTP).
/// Updates menu item text in-place — never calls `set_menu()`.
fn start_status_updater(handle: AppHandle<Wry>, items: DynamicMenuItems) {
    tauri::async_runtime::spawn(async move {
        // Wait for state to be initialized
        tokio::time::sleep(Duration::from_secs(2)).await;

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            let mut status = TrayStatus::default();

            // Get state if available
            if let Some(state) = handle.try_state::<Arc<AppState>>() {
                // Get streaming event
                match state.get_streaming_event().await {
                    Ok(Some(event)) => {
                        status.event_name = event.name;

                        status.inpoint = if event.receiving_activated {
                            "Receiving".to_string()
                        } else {
                            "Paused".to_string()
                        };
                    }
                    Ok(None) => {
                        status.inpoint = "No Event".to_string();
                    }
                    Err(e) => {
                        tracing::debug!("Failed to get streaming event: {e}");
                        status.inpoint = "Error".to_string();
                    }
                }

                // Get chunk stats
                match state.get_chunk_stats().await {
                    Ok(stats) => {
                        status.buffer = format_duration(stats.buffer_duration_secs);
                        status.pending = stats.pending_chunks as u64;
                        status.sent_chunks = stats.sent_chunks as u64;

                        status.endpoint = if stats.pending_chunks > 0 {
                            "Uploading".to_string()
                        } else {
                            "Idle".to_string()
                        };
                    }
                    Err(e) => {
                        tracing::debug!("Failed to get chunk stats: {e}");
                    }
                }
            } else {
                status.inpoint = "Initializing...".to_string();
                status.endpoint = "Initializing...".to_string();
            }

            // Update menu items in-place (menu stays open!)
            let event_text = if status.event_name.is_empty() {
                "No active event".to_string()
            } else {
                format!("Event: {}", status.event_name)
            };
            let _ = items.event.set_text(&event_text);
            let _ = items.inpoint.set_text(format!("Inpoint: {}", status.inpoint));
            let _ = items.buffer.set_text(format!("Buffer: {}", status.buffer));
            let _ = items.uploader.set_text(format!("Uploader: {}", status.endpoint));
            let _ = items
                .chunks
                .set_text(format!("Chunks: {} sent, {} pending", status.sent_chunks, status.pending));

            // Tooltip always safe to update
            if let Some(tray) = handle.tray_by_id("restreamer") {
                let tooltip = build_tooltip(&status);
                let _ = tray.set_tooltip(Some(&tooltip));
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

    // --- Tooltip tests ---

    #[test]
    fn tooltip_streaming_with_stats() {
        let s = TrayStatus {
            inpoint: "Receiving".to_string(),
            buffer: "00:05:23".to_string(),
            endpoint: "Uploading".to_string(),
            pending: 3,
            sent_chunks: 42,
            event_name: "Sunday Service".to_string(),
        };
        let tip = build_tooltip(&s);
        assert!(tip.contains("Restreamer \u{2014} Sunday Service"));
        assert!(tip.contains("Receiving | Buffer: 00:05:23"));
        assert!(tip.contains("Chunks: 42 sent, 3 pending"));
    }

    #[test]
    fn tooltip_idle_no_event() {
        let s = TrayStatus {
            inpoint: "No Event".to_string(),
            ..TrayStatus::default()
        };
        let tip = build_tooltip(&s);
        assert!(tip.starts_with("Restreamer\n"));
        assert!(tip.contains("No active event"));
    }

    // --- Chunk display format tests ---

    #[test]
    fn chunk_display_format() {
        let s = TrayStatus {
            sent_chunks: 42,
            pending: 3,
            ..TrayStatus::default()
        };
        let tip = build_tooltip(&s);
        assert!(tip.contains("Chunks: 42 sent, 3 pending"));
    }

    #[test]
    fn chunk_display_zero() {
        let s = TrayStatus::default();
        let tip = build_tooltip(&s);
        assert!(tip.contains("Chunks: 0 sent, 0 pending"));
    }

    #[test]
    fn tooltip_no_event_name() {
        let tip = build_tooltip(&TrayStatus::default());
        assert!(tip.starts_with("Restreamer\n"));
    }

    #[test]
    fn tooltip_with_event_name() {
        let s = TrayStatus {
            event_name: "Wednesday Prayer".to_string(),
            ..TrayStatus::default()
        };
        let tip = build_tooltip(&s);
        assert!(tip.starts_with("Restreamer \u{2014} Wednesday Prayer\n"));
    }
}
