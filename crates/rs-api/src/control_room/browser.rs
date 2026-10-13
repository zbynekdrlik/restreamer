//! The control-room Chrome (#388): one process hosting every account profile,
//! started on demand and reachable over its debugging port on 127.0.0.1.
//!
//! - Windows: a real window parked off-screen (the operator never sees it;
//!   the dashboard shows it through the screencast).
//! - Linux: Chrome draws on its own Xvfb display, so nothing appears on the
//!   kiosk screen or in its panel.
//!
//! Chrome and Xvfb outlive a Restreamer restart on purpose: the next start
//! finds the debugging port answering and reuses them, logins included.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::Mutex;

use super::cdp::Cdp;

/// Debugging port of the control-room Chrome (127.0.0.1 only).
pub const CDP_PORT: u16 = 9231;
/// Xvfb display number on Linux.
pub const XVFB_DISPLAY: &str = ":93";
/// Size of the control-room browser window (and the screencast).
pub const WINDOW_W: u32 = 1600;
pub const WINDOW_H: u32 = 900;
/// Override for the Chrome binary.
pub const CHROME_ENV: &str = "RESTREAMER_CHROME";

/// Where to look for Chrome, in order. `env` is the `RESTREAMER_CHROME`
/// override, tried first.
pub fn chrome_candidates(windows: bool, env: Option<String>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = env
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect();
    let fixed: &[&str] = if windows {
        &[
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        ]
    } else {
        &[
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/opt/google/chrome/google-chrome",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ]
    };
    out.extend(fixed.iter().map(PathBuf::from));
    out
}

/// The first candidate that exists.
pub fn locate_chrome() -> Option<PathBuf> {
    chrome_candidates(cfg!(windows), std::env::var(CHROME_ENV).ok())
        .into_iter()
        .find(|p| p.is_file())
}

/// Arguments of the first launch (the process that owns the debugging port).
pub fn launch_args(user_data: &Path, port: u16, windows: bool) -> Vec<String> {
    let mut a = vec![
        format!("--user-data-dir={}", user_data.display()),
        format!("--remote-debugging-port={port}"),
        "--remote-debugging-address=127.0.0.1".into(),
        "--no-first-run".into(),
        "--no-default-browser-check".into(),
        "--disable-backgrounding-occluded-windows".into(),
        "--disable-renderer-backgrounding".into(),
        "--disable-background-timer-throttling".into(),
        format!("--window-size={WINDOW_W},{WINDOW_H}"),
    ];
    if windows {
        a.push("--window-position=-32000,-32000".into());
    } else {
        // The kiosk has no keyring; cookies must stay readable across starts.
        a.push("--password-store=basic".into());
        a.push("--window-position=0,0".into());
    }
    a.push("about:blank".into());
    a
}

/// Arguments that open `url` in `profile` inside the running process (Chrome
/// forwards a second invocation with the same user-data-dir to it).
pub fn open_args(user_data: &Path, profile: &str, url: &str, windows: bool) -> Vec<String> {
    let mut a = vec![
        format!("--user-data-dir={}", user_data.display()),
        format!("--profile-directory={profile}"),
    ];
    if !windows {
        a.push("--password-store=basic".into());
    }
    a.push(url.into());
    a
}

/// Supervises the control-room Chrome (and Xvfb on Linux).
pub struct Browser {
    pub cdp: Cdp,
    pub user_data: PathBuf,
    start_lock: Mutex<()>,
}

impl Browser {
    pub fn new(data_dir: &Path) -> Self {
        Self::at(data_dir, format!("127.0.0.1:{CDP_PORT}"))
    }

    /// A browser whose debugging port is `cdp_addr` (tests use a free port).
    pub fn at(data_dir: &Path, cdp_addr: String) -> Self {
        Self {
            cdp: Cdp::new(cdp_addr),
            user_data: data_dir.join("browser-profiles").join("control-rooms"),
            start_lock: Mutex::new(()),
        }
    }

    /// Start Chrome (and Xvfb) unless the debugging port already answers.
    pub async fn ensure_running(&self) -> Result<()> {
        let _guard = self.start_lock.lock().await;
        if self.cdp.browser_ws_url().await.is_ok() {
            return Ok(());
        }
        let chrome = locate_chrome().context(
            "Chrome not found: install Google Chrome or set RESTREAMER_CHROME to its path",
        )?;
        std::fs::create_dir_all(&self.user_data)
            .with_context(|| format!("create {}", self.user_data.display()))?;
        if !cfg!(windows) {
            start_xvfb().await?;
        }
        tracing::info!(
            "control rooms: starting {} (profiles in {}, debugging port {CDP_PORT})",
            chrome.display(),
            self.user_data.display()
        );
        let mut cmd = tokio::process::Command::new(&chrome);
        cmd.args(launch_args(&self.user_data, CDP_PORT, cfg!(windows)));
        detach(&mut cmd);
        cmd.spawn()
            .with_context(|| format!("spawn {}", chrome.display()))?;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if self.cdp.browser_ws_url().await.is_ok() {
                tracing::info!("control rooms: Chrome is up");
                return Ok(());
            }
        }
        bail!("Chrome started but its debugging port {CDP_PORT} never answered")
    }

    /// Open `url` in `profile` (a new tab of the running browser).
    pub async fn open(&self, profile: &str, url: &str) -> Result<()> {
        let chrome = locate_chrome().context("Chrome not found")?;
        let mut cmd = tokio::process::Command::new(&chrome);
        cmd.args(open_args(&self.user_data, profile, url, cfg!(windows)));
        detach(&mut cmd);
        let status = tokio::time::timeout(Duration::from_secs(15), cmd.status())
            .await
            .context("Chrome did not hand the tab over within 15 s")?
            .context("run Chrome")?;
        if !status.success() {
            bail!("Chrome exited with {status} while opening {url}");
        }
        Ok(())
    }
}

/// Chrome/Xvfb must not die with Restreamer, and on Linux must draw on the
/// hidden display.
fn detach(cmd: &mut tokio::process::Command) {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
    if !cfg!(windows) {
        cmd.env("DISPLAY", XVFB_DISPLAY);
    }
}

/// Start the hidden X display unless it already runs.
async fn start_xvfb() -> Result<()> {
    let lock = format!("/tmp/.X{}-lock", XVFB_DISPLAY.trim_start_matches(':'));
    if Path::new(&lock).exists() {
        return Ok(());
    }
    let mut cmd = tokio::process::Command::new("Xvfb");
    cmd.args([
        XVFB_DISPLAY,
        "-screen",
        "0",
        &format!("{WINDOW_W}x{WINDOW_H}x24"),
        "-nolisten",
        "tcp",
    ]);
    detach(&mut cmd);
    cmd.spawn()
        .context("spawn Xvfb (install the xvfb package)")?;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if Path::new(&lock).exists() {
            tracing::info!("control rooms: Xvfb {XVFB_DISPLAY} is up");
            return Ok(());
        }
    }
    bail!("Xvfb {XVFB_DISPLAY} did not come up")
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;
