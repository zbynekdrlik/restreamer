//! The control-room Chrome (#388): one process hosting every account profile,
//! started on demand and reachable over its debugging port on 127.0.0.1.
//!
//! - Windows: a real window parked off-screen (the operator never sees it;
//!   the dashboard shows it through the screencast).
//! - Linux: Chrome draws on its own Xvfb display, so nothing appears on the
//!   kiosk screen or in its panel.
//!
//! Chrome and Xvfb outlive a Restreamer restart on purpose (on Linux under
//! systemd they get a scope of their own, see [`scoped_command`]): the next start
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
/// systemd's transient-unit launcher (Linux).
pub const SYSTEMD_RUN: &str = "/usr/bin/systemd-run";

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
        // #407: hiding a room (`frozen` then `active`) must not close its
        // WebSockets; without this a freeze closes every one of the page.
        "--disable-back-forward-cache-for-web-sockets".into(),
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
    /// The Chrome binary; `None` = [`locate_chrome`].
    pub chrome_bin: Option<PathBuf>,
    pub xvfb_bin: PathBuf,
    /// The hidden X display Chrome draws on (Linux).
    pub display: String,
    /// `systemd-run`, when Chrome and Xvfb must get their own scope (Linux,
    /// Restreamer running as a systemd service); see [`scope_runner`].
    pub systemd_run: Option<PathBuf>,
    start_lock: Mutex<()>,
}

impl Browser {
    pub fn new(data_dir: &Path) -> Self {
        let mut b = Self::at(data_dir, format!("127.0.0.1:{CDP_PORT}"));
        b.systemd_run = scope_runner(
            std::env::var_os("INVOCATION_ID").is_some(),
            Path::new(SYSTEMD_RUN),
        );
        b
    }

    /// A browser whose debugging port is `cdp_addr` (tests use a free port).
    pub fn at(data_dir: &Path, cdp_addr: String) -> Self {
        Self {
            cdp: Cdp::new(cdp_addr),
            user_data: data_dir.join("browser-profiles").join("control-rooms"),
            chrome_bin: None,
            xvfb_bin: PathBuf::from("Xvfb"),
            display: XVFB_DISPLAY.into(),
            systemd_run: None,
            start_lock: Mutex::new(()),
        }
    }

    fn chrome(&self) -> Result<PathBuf> {
        self.chrome_bin
            .clone()
            .or_else(locate_chrome)
            .context("Chrome not found: install Google Chrome or set RESTREAMER_CHROME to its path")
    }

    /// The debugging port, from `cdp.addr` (`host:port`).
    pub fn port(&self) -> u16 {
        port_of(&self.cdp.addr)
    }

    /// DISPLAY for Chrome/Xvfb: the hidden display on Linux, none on Windows.
    fn display_env(&self) -> Option<&str> {
        (!cfg!(windows)).then_some(self.display.as_str())
    }

    /// Start Chrome (and Xvfb) unless the debugging port already answers.
    pub async fn ensure_running(&self) -> Result<()> {
        let _guard = self.start_lock.lock().await;
        if self.cdp.browser_ws_url().await.is_ok() {
            return Ok(());
        }
        let chrome = self.chrome()?;
        std::fs::create_dir_all(&self.user_data)
            .with_context(|| format!("create {}", self.user_data.display()))?;
        if let Some(display) = self.display_env() {
            start_xvfb(
                &self.xvfb_bin,
                display,
                self.systemd_run.as_deref(),
                &stderr_log(&self.user_data, "Xvfb"),
            )
            .await?;
        }
        tracing::info!(
            "control rooms: starting {} (profiles in {}, debugging port {})",
            chrome.display(),
            self.user_data.display(),
            self.port()
        );
        let args = launch_args(&self.user_data, self.port(), cfg!(windows));
        #[cfg(windows)]
        drop(
            super::spawn_windows::spawn(&chrome, &args)
                .with_context(|| format!("spawn {}", chrome.display()))?,
        );
        // Its own scope: a restart of the service must not kill it.
        #[cfg(not(windows))]
        let mut started = Started::spawn(
            self.systemd_run.as_deref(),
            Launch {
                what: "Chrome",
                props: CHROME_SCOPE_PROPS,
                program: chrome,
                args,
                display: self.display_env().map(str::to_string),
                log: stderr_log(&self.user_data, "Chrome"),
            },
        )?;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            #[cfg(not(windows))]
            started.fall_back_if_the_scope_failed()?;
            if self.cdp.browser_ws_url().await.is_ok() {
                tracing::info!("control rooms: Chrome is up");
                return Ok(());
            }
        }
        bail!(
            "Chrome started but its debugging port {} never answered",
            self.port()
        )
    }

    /// Open `url` in `profile` (a new tab of the running browser).
    pub async fn open(&self, profile: &str, url: &str) -> Result<()> {
        let chrome = self.chrome()?;
        let args = open_args(&self.user_data, profile, url, cfg!(windows));
        #[cfg(windows)]
        {
            let child = super::spawn_windows::spawn(&chrome, &args)
                .with_context(|| format!("run {}", chrome.display()))?;
            let code = tokio::task::spawn_blocking(move || child.wait(Duration::from_secs(15)))
                .await
                .context("wait for Chrome")?
                .context("wait for Chrome")?;
            handover_result(code, url)?;
        }
        #[cfg(not(windows))]
        {
            let mut cmd = tokio::process::Command::new(&chrome);
            cmd.args(args);
            detach(&mut cmd, self.display_env());
            let status = tokio::time::timeout(Duration::from_secs(15), cmd.status())
                .await
                .context("Chrome did not hand the tab over within 15 s")?
                .context("run Chrome")?;
            if !status.success() {
                bail!("Chrome exited with {status} while opening {url}");
            }
        }
        Ok(())
    }
}

/// The outcome of a Chrome handover run that exited with `code` (`None`:
/// still running when the wait ended).
pub fn handover_result(code: Option<u32>, url: &str) -> Result<()> {
    match code {
        None => bail!("Chrome did not hand the tab over within 15 s"),
        Some(0) => Ok(()),
        Some(c) => bail!("Chrome exited with code {c} while opening {url}"),
    }
}

/// The command line `CreateProcessW` gets for `program` + `args`, quoted the
/// way `CommandLineToArgvW` (and Chrome) splits it back.
pub fn windows_command_line(program: &str, args: &[String]) -> String {
    let mut out = String::new();
    for (i, arg) in std::iter::once(program)
        .chain(args.iter().map(String::as_str))
        .enumerate()
    {
        if i > 0 {
            out.push(' ');
        }
        push_windows_arg(&mut out, arg);
    }
    out
}

/// One argument: quoted when empty or holding a space, tab or quote; a
/// backslash run is doubled only where it precedes a quote.
fn push_windows_arg(out: &mut String, arg: &str) {
    let quote = arg.is_empty() || arg.contains([' ', '\t', '"']);
    if quote {
        out.push('"');
    }
    let mut backslashes = 0;
    for c in arg.chars() {
        if c == '\\' {
            backslashes += 1;
        } else {
            if c == '"' {
                out.extend(std::iter::repeat_n('\\', backslashes + 1));
            }
            backslashes = 0;
        }
        out.push(c);
    }
    if quote {
        out.extend(std::iter::repeat_n('\\', backslashes));
        out.push('"');
    }
}

/// `systemd-run` when Chrome and Xvfb need a scope of their own: Restreamer
/// runs as a systemd service (`INVOCATION_ID` is set) and `systemd_run` exists.
pub fn scope_runner(under_systemd: bool, systemd_run: &Path) -> Option<PathBuf> {
    (under_systemd && systemd_run.is_file()).then(|| systemd_run.to_path_buf())
}

/// Properties of Chrome's scope: a memory cap, and an OOM inside it kills a
/// renderer (a "tab crashed" page), never the whole scope.
pub const CHROME_SCOPE_PROPS: &[&str] = &["MemoryHigh=3G", "MemoryMax=3584M", "OOMPolicy=continue"];

/// The program and arguments that start `program args`, in a scope of its own
/// (with `props`) when `systemd_run` is set.
pub fn scoped_command(
    systemd_run: Option<&Path>,
    what: &str,
    program: &Path,
    args: &[String],
    props: &[&str],
) -> (PathBuf, Vec<String>) {
    let Some(runner) = systemd_run else {
        return (program.to_path_buf(), args.to_vec());
    };
    let mut out: Vec<String> = ["--user", "--scope", "--collect", "--quiet"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    for p in props {
        out.push("-p".into());
        out.push((*p).to_string());
    }
    out.push(format!("--description=Restreamer control rooms: {what}"));
    out.push("--".into());
    out.push(program.display().to_string());
    out.extend(args.iter().cloned());
    (runner.to_path_buf(), out)
}

/// The port of a `host:port` address; [`CDP_PORT`] when there is none.
pub fn port_of(addr: &str) -> u16 {
    addr.rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(CDP_PORT)
}

/// Chrome/Xvfb must not die with Restreamer, and on Linux draw on the hidden
/// display.
fn detach(cmd: &mut tokio::process::Command, display: Option<&str>) {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
    if let Some(d) = display {
        cmd.env("DISPLAY", d);
    }
}

/// How to start a Chrome or Xvfb: detached, with stderr to `log`.
struct Launch {
    what: &'static str,
    /// Scope properties (`systemd-run -p`).
    props: &'static [&'static str],
    program: PathBuf,
    args: Vec<String>,
    display: Option<String>,
    log: PathBuf,
}

impl Launch {
    /// Spawn it, in a systemd scope when `systemd_run` is set. `append` keeps
    /// what the log already holds, else it is truncated.
    fn spawn(&self, systemd_run: Option<&Path>, append: bool) -> Result<tokio::process::Child> {
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true);
        if append {
            opts.append(true);
        } else {
            opts.write(true).truncate(true);
        }
        let file = opts
            .open(&self.log)
            .with_context(|| format!("open {}", self.log.display()))?;
        let (program, args) = scoped_command(
            systemd_run,
            self.what,
            &self.program,
            &self.args,
            self.props,
        );
        let mut cmd = tokio::process::Command::new(&program);
        cmd.args(args);
        detach(&mut cmd, self.display.as_deref());
        cmd.stderr(file);
        cmd.spawn()
            .with_context(|| format!("spawn {}", program.display()))
    }
}

/// A Chrome or Xvfb just started.
struct Started {
    child: tokio::process::Child,
    scoped: bool,
    launch: Launch,
}

impl Started {
    /// Start `launch`, in its own scope when `systemd_run` is set.
    fn spawn(systemd_run: Option<&Path>, launch: Launch) -> Result<Self> {
        let child = launch.spawn(systemd_run, false)?;
        if systemd_run.is_some() {
            tracing::info!(
                "control rooms: {} starts in its own systemd scope",
                launch.what
            );
        }
        Ok(Self {
            child,
            scoped: systemd_run.is_some(),
            launch,
        })
    }

    /// systemd-run exits at once when it cannot create the scope (no user
    /// manager, no session bus): start the program directly then, so the
    /// rooms still work (they only lose surviving a service restart).
    fn fall_back_if_the_scope_failed(&mut self) -> Result<()> {
        if !self.scoped {
            return Ok(());
        }
        let Ok(Some(status)) = self.child.try_wait() else {
            return Ok(());
        };
        if status.success() {
            return Ok(());
        }
        let why = std::fs::read_to_string(&self.launch.log).unwrap_or_default();
        if !systemd_run_failed(&why) {
            // The program itself exited (a crash, an OOM kill inside its
            // capped scope): it stays scoped, the next open starts it again.
            tracing::warn!(
                "control rooms: {} exited in its systemd scope ({status}): {} (log {})",
                self.launch.what,
                why.trim().lines().last().unwrap_or("no output"),
                self.launch.log.display()
            );
            self.scoped = false;
            return Ok(());
        }
        tracing::warn!(
            "control rooms: {} in a systemd scope exited ({status}): {}; starting it without a scope (log {})",
            self.launch.what,
            why.trim().lines().last().unwrap_or("no output"),
            self.launch.log.display()
        );
        self.child = self.launch.spawn(None, true)?;
        self.scoped = false;
        Ok(())
    }
}

/// systemd-run's own messages when it cannot run a program in a scope
/// (src/run/run.c). Chrome's similar "Failed to connect to the bus" comes
/// with a `[pid:tid:...]` prefix and never matches.
const SYSTEMD_RUN_FAILURES: &[&str] = &[
    "Failed to connect to bus",
    "Failed to create bus connection",
    "Failed to start transient scope unit",
    "Failed to find executable",
];

/// Whether a scoped start's stderr shows that systemd-run itself failed.
fn systemd_run_failed(log: &str) -> bool {
    log.lines().any(|l| {
        SYSTEMD_RUN_FAILURES
            .iter()
            .any(|f| l.trim_start().starts_with(f))
    })
}

/// Where a started Chrome or Xvfb writes its stderr (`what` = "Chrome" /
/// "Xvfb"): next to the control-room profiles, truncated at each start.
pub fn stderr_log(user_data: &Path, what: &str) -> PathBuf {
    user_data.with_file_name(format!("{}.stderr.log", what.to_lowercase()))
}

/// The X server lock file of `display` (`:93` -> `/tmp/.X93-lock`).
pub fn xvfb_lock(display: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/.X{}-lock", display.trim_start_matches(':')))
}

/// Start the hidden X display unless it already runs.
async fn start_xvfb(bin: &Path, disp: &str, systemd_run: Option<&Path>, log: &Path) -> Result<()> {
    let lock = xvfb_lock(disp);
    if lock.exists() {
        return Ok(());
    }
    let args: Vec<String> = [
        disp,
        "-screen",
        "0",
        &format!("{WINDOW_W}x{WINDOW_H}x24"),
        "-nolisten",
        "tcp",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let launch = Launch {
        what: "Xvfb",
        props: &[],
        program: bin.to_path_buf(),
        args,
        display: None,
        log: log.to_path_buf(),
    };
    let mut started =
        Started::spawn(systemd_run, launch).context("spawn Xvfb (install the xvfb package)")?;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        started.fall_back_if_the_scope_failed()?;
        if lock.exists() {
            tracing::info!("control rooms: Xvfb {disp} is up");
            return Ok(());
        }
    }
    bail!("Xvfb {disp} did not come up")
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;
