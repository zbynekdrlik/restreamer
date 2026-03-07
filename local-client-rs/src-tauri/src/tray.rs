//! System tray with direct state access (no HTTP polling).

use std::sync::Arc;
use std::time::Duration;

use tauri::image::Image;
use tauri::menu::{MenuBuilder, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{App, AppHandle, Manager, Wry};

use crate::state::AppState;

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
        .tooltip("Restreamer - Starting...")
        .on_menu_event(move |app, event| handle_menu_event(app, event.id().as_ref()))
        .build(app)?;

    start_status_poller(app.handle().clone());

    Ok(())
}

#[derive(PartialEq)]
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
            inpoint: "Starting...".to_string(),
            buffer: "--:--:--".to_string(),
            endpoint: "Starting...".to_string(),
            pending: 0,
            total_chunks: 0,
            event_name: String::new(),
        }
    }
}

/// Build the single status summary line for the menu.
fn status_line(status: &TrayStatus) -> String {
    match status.inpoint.as_str() {
        "Receiving" => {
            if !status.event_name.is_empty() {
                format!("\u{25CF} Streaming \u{2014} {}", status.event_name)
            } else {
                "\u{25CF} Streaming".to_string()
            }
        }
        "Paused" => {
            if !status.event_name.is_empty() {
                format!("\u{25CB} Idle \u{2014} {}", status.event_name)
            } else {
                "\u{25CB} Idle".to_string()
            }
        }
        "No Event" => "\u{25CB} No active event".to_string(),
        "Error" => "\u{26A0} Error".to_string(),
        _ => "\u{25CC} Starting...".to_string(),
    }
}

/// Build a rich multi-line tooltip with detailed stats.
fn build_tooltip(status: &TrayStatus) -> String {
    let title = if !status.event_name.is_empty() {
        format!("Restreamer \u{2014} {}", status.event_name)
    } else {
        "Restreamer".to_string()
    };

    let state = match status.inpoint.as_str() {
        "Receiving" => format!("\u{25CF} Receiving | Buffer: {}", status.buffer),
        "Paused" => "\u{25CB} Idle".to_string(),
        "No Event" => "\u{25CB} No active event".to_string(),
        "Error" => "\u{26A0} Error".to_string(),
        _ => "\u{25CC} Starting...".to_string(),
    };

    let chunks = format!(
        "Chunks: {} total, {} pending",
        status.total_chunks, status.pending
    );

    format!("{title}\n{state}\n{chunks}")
}

fn build_menu(
    app: &impl Manager<Wry>,
    version: &str,
    status: &TrayStatus,
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
            status_line(status),
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
            "View Logs",
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
                let log_dir = std::path::PathBuf::from("/var/lib/restreamer");
                if log_dir.exists() {
                    let _ = std::process::Command::new("xdg-open")
                        .arg(&log_dir)
                        .spawn();
                } else {
                    tracing::warn!("Log directory does not exist: {:?}", log_dir);
                }
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
fn start_status_poller(handle: AppHandle<Wry>) {
    tauri::async_runtime::spawn(async move {
        let version = env!("CARGO_PKG_VERSION");

        // Wait for state to be initialized
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut prev_status: Option<TrayStatus> = None;

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            let mut tray_status = TrayStatus::default();

            // Get state if available
            if let Some(state) = handle.try_state::<Arc<AppState>>() {
                // Get streaming event
                match state.get_streaming_event().await {
                    Ok(Some(event)) => {
                        tray_status.event_name = event
                            .short_description
                            .unwrap_or_default();

                        tray_status.inpoint = if event.receiving_activated {
                            "Receiving".to_string()
                        } else {
                            "Paused".to_string()
                        };
                    }
                    Ok(None) => {
                        tray_status.inpoint = "No Event".to_string();
                    }
                    Err(e) => {
                        tracing::debug!("Failed to get streaming event: {e}");
                        tray_status.inpoint = "Error".to_string();
                    }
                }

                // Get chunk stats
                match state.get_chunk_stats().await {
                    Ok(stats) => {
                        tray_status.buffer = format_duration(stats.buffer_duration_secs);
                        tray_status.pending = stats.pending_chunks as u64;
                        tray_status.total_chunks = stats.total_chunks as u64;

                        tray_status.endpoint = if stats.pending_chunks > 0 {
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
                tray_status.inpoint = "Initializing...".to_string();
                tray_status.endpoint = "Initializing...".to_string();
            }

            // Update the tray icon
            if let Some(tray) = handle.tray_by_id("restreamer") {
                // Always update tooltip (doesn't close the menu)
                let tooltip = build_tooltip(&tray_status);
                let _ = tray.set_tooltip(Some(&tooltip));

                // Only rebuild menu when status changes (avoids closing open menu)
                let should_rebuild = match &prev_status {
                    Some(prev) => *prev != tray_status,
                    None => true,
                };

                if should_rebuild {
                    if let Ok(menu) = build_menu(&handle, version, &tray_status) {
                        let _ = tray.set_menu(Some(menu));
                    }
                }

                prev_status = Some(tray_status);
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

    // --- Status line tests ---

    #[test]
    fn status_line_streaming_with_event() {
        let s = TrayStatus {
            inpoint: "Receiving".to_string(),
            event_name: "Sunday Service".to_string(),
            ..TrayStatus::default()
        };
        assert_eq!(status_line(&s), "\u{25CF} Streaming \u{2014} Sunday Service");
    }

    #[test]
    fn status_line_streaming_no_event() {
        let s = TrayStatus {
            inpoint: "Receiving".to_string(),
            event_name: String::new(),
            ..TrayStatus::default()
        };
        assert_eq!(status_line(&s), "\u{25CF} Streaming");
    }

    #[test]
    fn status_line_idle_with_event() {
        let s = TrayStatus {
            inpoint: "Paused".to_string(),
            event_name: "Wednesday Prayer".to_string(),
            ..TrayStatus::default()
        };
        assert_eq!(
            status_line(&s),
            "\u{25CB} Idle \u{2014} Wednesday Prayer"
        );
    }

    #[test]
    fn status_line_idle_no_event() {
        let s = TrayStatus {
            inpoint: "Paused".to_string(),
            event_name: String::new(),
            ..TrayStatus::default()
        };
        assert_eq!(status_line(&s), "\u{25CB} Idle");
    }

    #[test]
    fn status_line_no_event() {
        let s = TrayStatus {
            inpoint: "No Event".to_string(),
            ..TrayStatus::default()
        };
        assert_eq!(status_line(&s), "\u{25CB} No active event");
    }

    #[test]
    fn status_line_error() {
        let s = TrayStatus {
            inpoint: "Error".to_string(),
            ..TrayStatus::default()
        };
        assert_eq!(status_line(&s), "\u{26A0} Error");
    }

    #[test]
    fn status_line_initializing() {
        let s = TrayStatus::default();
        assert_eq!(status_line(&s), "\u{25CC} Starting...");
    }

    // --- Tooltip tests ---

    #[test]
    fn tooltip_streaming_with_stats() {
        let s = TrayStatus {
            inpoint: "Receiving".to_string(),
            buffer: "00:05:23".to_string(),
            endpoint: "Uploading".to_string(),
            pending: 3,
            total_chunks: 42,
            event_name: "Sunday Service".to_string(),
        };
        let tip = build_tooltip(&s);
        assert!(tip.contains("Restreamer \u{2014} Sunday Service"));
        assert!(tip.contains("\u{25CF} Receiving | Buffer: 00:05:23"));
        assert!(tip.contains("Chunks: 42 total, 3 pending"));
    }

    #[test]
    fn tooltip_idle_no_event() {
        let s = TrayStatus {
            inpoint: "No Event".to_string(),
            ..TrayStatus::default()
        };
        let tip = build_tooltip(&s);
        assert!(tip.starts_with("Restreamer\n"));
        assert!(tip.contains("\u{25CB} No active event"));
    }

    // --- PartialEq tests ---

    #[test]
    fn tray_status_eq_same() {
        let a = TrayStatus::default();
        let b = TrayStatus::default();
        assert_eq!(a, b);
    }

    #[test]
    fn tray_status_ne_different_inpoint() {
        let a = TrayStatus::default();
        let b = TrayStatus {
            inpoint: "Receiving".to_string(),
            ..TrayStatus::default()
        };
        assert_ne!(a, b);
    }

    #[test]
    fn tray_status_ne_different_chunks() {
        let a = TrayStatus::default();
        let b = TrayStatus {
            total_chunks: 10,
            ..TrayStatus::default()
        };
        assert_ne!(a, b);
    }
}
