//! Load the dashboard into the operator window once the embedded server
//! answers (#393).
//!
//! The window is visible from the start (it is the operator's always-present
//! window), but the embedded HTTP server binds only after the DB pool and the
//! migrations. On a cold start the webview's first load can hit a refused
//! connection and would sit on the error page forever, so this waits for the
//! port and then (re)loads the dashboard once.

use std::time::Duration;

use tauri::{AppHandle, Manager, Runtime, Url};

/// Where the window loads the dashboard (same as `tauri.conf.json`).
const DASHBOARD_URL: &str = "http://127.0.0.1:8910";
const DASHBOARD_ADDR: &str = "127.0.0.1:8910";

/// How long to wait for the embedded server before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Spawn the wait-then-load task for the `main` window.
pub fn load_when_ready<R: Runtime>(app: &AppHandle<R>) {
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let started = tokio::time::Instant::now();
        if !wait_until_accepting(DASHBOARD_ADDR, READY_TIMEOUT, POLL_INTERVAL).await {
            tracing::error!(
                "Dashboard server did not answer on {DASHBOARD_ADDR} within {}s; the window keeps its first load",
                READY_TIMEOUT.as_secs()
            );
            return;
        }
        tracing::info!(
            "Dashboard server answers after {} ms; loading it into the window",
            started.elapsed().as_millis()
        );
        let Some(window) = handle.get_webview_window("main") else {
            tracing::warn!("No 'main' window to load the dashboard into");
            return;
        };
        match Url::parse(DASHBOARD_URL) {
            Ok(url) => {
                if let Err(e) = window.navigate(url) {
                    tracing::error!("Failed to load the dashboard into the window: {e}");
                }
            }
            Err(e) => tracing::error!("Bad dashboard URL {DASHBOARD_URL}: {e}"),
        }
    });
}

/// Poll `addr` until a TCP connect succeeds (`true`) or `timeout` passes
/// (`false`).
async fn wait_until_accepting(addr: &str, timeout: Duration, interval: Duration) -> bool {
    let started = tokio::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        if started.elapsed() >= timeout {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A free loopback port with nothing listening on it.
    fn free_addr() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    }

    #[tokio::test]
    async fn waits_for_a_server_that_binds_late() {
        let addr = free_addr();
        let bind_addr = addr.clone();
        let server = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let l = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
            let _ = l.accept().await;
        });
        let started = std::time::Instant::now();
        assert!(
            wait_until_accepting(&addr, Duration::from_secs(10), Duration::from_millis(50)).await
        );
        assert!(
            started.elapsed() >= Duration::from_millis(250),
            "must not report ready before the server binds"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gives_up_when_nothing_ever_listens() {
        let addr = free_addr();
        let started = std::time::Instant::now();
        assert!(
            !wait_until_accepting(&addr, Duration::from_millis(400), Duration::from_millis(50))
                .await
        );
        assert!(started.elapsed() >= Duration::from_millis(400));
    }
}
