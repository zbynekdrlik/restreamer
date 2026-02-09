use std::time::Duration;

use tauri::{AppHandle, Runtime};
use tauri_plugin_updater::UpdaterExt;

/// Check interval: every 6 hours.
const CHECK_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Start a background task that checks for updates periodically.
pub fn start_update_checker<R: Runtime>(app: &AppHandle<R>) {
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        // Initial delay before first check (30 seconds after startup)
        tokio::time::sleep(Duration::from_secs(30)).await;

        loop {
            check_for_update(&handle).await;
            tokio::time::sleep(CHECK_INTERVAL).await;
        }
    });
}

/// Check for an update and notify via tray tooltip if available.
async fn check_for_update<R: Runtime>(app: &AppHandle<R>) {
    let updater = match app.updater() {
        Ok(u) => u,
        Err(e) => {
            tracing::debug!("Updater not available: {e}");
            return;
        }
    };

    match updater.check().await {
        Ok(Some(update)) => {
            let version = update.version.clone();
            tracing::info!("Update available: v{version}");

            // Update the inpoint tray tooltip to show update badge
            if let Some(tray) = app.tray_by_id("inpoint") {
                let _ = tray.set_tooltip(Some(&format!(
                    "Restreamer Inpoint — Update v{version} available"
                )));
            }
        }
        Ok(None) => {
            tracing::debug!("No update available");
        }
        Err(e) => {
            tracing::debug!("Update check failed: {e}");
        }
    }
}

/// Trigger a manual update check (called from menu item).
pub async fn manual_check<R: Runtime>(app: &AppHandle<R>) {
    let updater = match app.updater() {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!("Updater not available: {e}");
            return;
        }
    };

    match updater.check().await {
        Ok(Some(update)) => {
            let version = update.version.clone();
            tracing::info!("Update v{version} found, downloading...");

            if let Err(e) = update
                .download_and_install(
                    |downloaded, total| {
                        tracing::debug!("Download progress: {downloaded}/{total:?}");
                    },
                    || {
                        tracing::info!("Download complete, installing...");
                    },
                )
                .await
            {
                tracing::error!("Update install failed: {e}");
            }
        }
        Ok(None) => {
            tracing::info!("Already on the latest version");
            if let Some(tray) = app.tray_by_id("inpoint") {
                let _ = tray.set_tooltip(Some("Restreamer Inpoint — Up to date"));
            }
        }
        Err(e) => {
            tracing::error!("Update check failed: {e}");
        }
    }
}
