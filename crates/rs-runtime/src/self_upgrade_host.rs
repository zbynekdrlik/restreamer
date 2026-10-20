//! The OS side of the #323 upgrade check: dpkg/apt, the restart of the
//! desktop user's unit, and the CLI entry points. Every decision lives in
//! the tested helpers of `self_upgrade.rs`; like the `*_windows.rs` glue,
//! this file is excluded from mutation testing (`.cargo/mutants.toml`).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use super::{
    DPKG_QUERY_FORMAT, Host, Installed, LATEST_RELEASE_URL, Outcome, STATUS_TIMEOUT, STATUS_URL,
    UPDATER_PUBKEY, Upgrader, app_process, apt_get_install, exe_replaced, parse_dpkg_query,
    passwd_user, run_command, verify_signature,
};

/// One request, the ~22 MB `.deb` included, over the venue's line.
const HTTP_TIMEOUT: Duration = Duration::from_secs(300);

/// `dpkg`, `apt-get` and `systemctl` on the box.
pub struct SystemHost;

impl Host for SystemHost {
    fn installed(&self) -> Result<Option<Installed>, String> {
        let output = Command::new("dpkg-query")
            .arg("-W")
            .arg(format!("-f={DPKG_QUERY_FORMAT}"))
            .arg("restreamer")
            .output()
            .map_err(|e| format!("dpkg-query: {e}"))?;
        // Exit 1 = the package is not installed.
        Ok(output
            .status
            .success()
            .then(|| parse_dpkg_query(&String::from_utf8_lossy(&output.stdout)))
            .flatten())
    }

    fn app_running(&self) -> bool {
        app_process(Path::new("/proc"), std::process::id()).is_some()
    }

    fn restart_pending(&self) -> bool {
        app_process(Path::new("/proc"), std::process::id()).is_some_and(|app| {
            std::fs::read_link(format!("/proc/{}/exe", app.pid))
                .is_ok_and(|target| exe_replaced(&target))
        })
    }

    fn fetch_dependencies(&self, deb: &Path) -> Result<(), String> {
        // The slow part (new Depends over the network), before the last
        // idle check; installs nothing. It takes the dpkg lock too: wait at
        // most 10 s whatever the box's apt config says (stream-pp sets 600).
        run_command(&mut apt_get_install(deb, true))
    }

    fn install(&self, deb: &Path) -> Result<(), String> {
        // A dpkg lock held by unattended-upgrades fails within 10 s
        // (`dpkg_locked`), and the check tries again at the next tick instead
        // of waiting here, between the idle check and the restart.
        run_command(&mut apt_get_install(deb, false))
    }

    fn restart_app(&self) -> Result<String, String> {
        let Some(app) = app_process(Path::new("/proc"), std::process::id()) else {
            return Ok("the app is not running; its next start runs the new version".into());
        };
        let passwd = std::fs::read_to_string("/etc/passwd")
            .map_err(|e| format!("reading /etc/passwd: {e}"))?;
        let user = passwd_user(&passwd, app.uid)
            .ok_or_else(|| format!("uid {} is not in /etc/passwd", app.uid))?;
        let machine = format!("{user}@");
        // The package ships the user unit, so reload before the restart.
        run_command(Command::new("systemctl").args(["--user", "-M", &machine, "daemon-reload"]))?;
        run_command(Command::new("systemctl").args([
            "--user",
            "-M",
            &machine,
            "restart",
            "restreamer",
        ]))?;
        Ok(format!("restarted the restreamer unit of {user}"))
    }
}

fn init_journal_log() {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .try_init();
}

/// A root-only directory for the download: systemd's `CacheDirectory=`, or
/// (a manual run) a new 0700 directory. Never a shared /tmp path, where
/// another user could plant a link under the package's name.
fn download_dir() -> Result<(PathBuf, bool), String> {
    if let Some(dir) = std::env::var_os("CACHE_DIRECTORY") {
        return Ok((PathBuf::from(dir), false));
    }
    let dir = std::env::temp_dir().join(format!("restreamer-upgrade-{}", std::process::id()));
    #[cfg(unix)]
    let builder = {
        let mut builder = std::fs::DirBuilder::new();
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder
    };
    #[cfg(not(unix))]
    let builder = std::fs::DirBuilder::new();
    builder
        .create(&dir)
        .map_err(|e| format!("creating {}: {e}", dir.display()))?;
    Ok((dir, true))
}

/// `Restreamer --upgrade-check`, run as root by `restreamer-upgrade.service`.
/// Returns the process exit code; every outcome is logged to the journal.
pub fn run_cli() -> i32 {
    init_journal_log();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("upgrade check: no runtime: {e}");
            return 1;
        }
    };
    let http = match reqwest::Client::builder()
        .user_agent(concat!("restreamer-upgrade/", env!("CARGO_PKG_VERSION")))
        .timeout(HTTP_TIMEOUT)
        .build()
    {
        Ok(http) => http,
        Err(e) => {
            tracing::error!("upgrade check: no HTTP client: {e}");
            return 1;
        }
    };
    let (download_dir, own_dir) = match download_dir() {
        Ok(dir) => dir,
        Err(e) => {
            tracing::error!("upgrade check: {e}");
            return 1;
        }
    };
    let upgrader = Upgrader {
        http,
        latest_url: LATEST_RELEASE_URL.into(),
        // The box asks anonymously (4 calls an hour); CI passes its token,
        // since a shared runner address can be over the anonymous limit.
        github_token: std::env::var("GITHUB_TOKEN").ok(),
        status_url: STATUS_URL.into(),
        status_timeout: STATUS_TIMEOUT,
        pubkey: UPDATER_PUBKEY.into(),
        download_dir: download_dir.clone(),
        host: SystemHost,
    };
    let outcome = runtime.block_on(upgrader.run_once());
    if own_dir {
        let _ = std::fs::remove_dir_all(&download_dir);
    }
    match outcome {
        Ok(Outcome::UpToDate { latest }) => {
            tracing::info!("up to date: nothing newer than the latest release {latest} to install");
            0
        }
        Ok(Outcome::Busy { latest, reason }) => {
            tracing::info!("{latest} is waiting for an idle box: {reason}");
            0
        }
        Ok(Outcome::LeftAlone {
            latest,
            installed,
            want,
        }) => {
            tracing::info!(
                "restreamer {installed} is marked {want:?} (apt-mark hold or a removal): \
                 left alone; the latest release is {latest}"
            );
            0
        }
        Ok(Outcome::Upgraded {
            latest,
            installed,
            restart,
        }) => {
            let what = if installed {
                "installed"
            } else {
                "already installed"
            };
            tracing::info!("upgraded to {latest} ({what}); {restart}");
            0
        }
        Err(e) => {
            tracing::error!("upgrade check failed: {e}");
            1
        }
    }
}

/// `Restreamer --verify-update <deb> <sig>`: release CI checks that the signed
/// `.deb` verifies with the key compiled into this very binary (#323).
pub fn verify_files_cli(deb: &Path, sig: &Path) -> i32 {
    let file_name = deb
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let verified = std::fs::read(deb)
        .map_err(|e| format!("reading it: {e}"))
        .and_then(|data| {
            let signature = std::fs::read_to_string(sig)
                .map_err(|e| format!("reading {}: {e}", sig.display()))?;
            verify_signature(&data, &signature, UPDATER_PUBKEY, &file_name)
        });
    match verified {
        Ok(()) => {
            println!("{} verifies with the updater key", deb.display());
            0
        }
        Err(e) => {
            eprintln!("{}: {e}", deb.display());
            1
        }
    }
}

/// `Restreamer --restart-app`: CI drives the root restart path the upgrade
/// uses (`systemctl --user -M <user>@ restart restreamer`) against a
/// stand-in unit (#323).
pub fn restart_app_cli() -> i32 {
    init_journal_log();
    match SystemHost.restart_app() {
        Ok(done) => {
            tracing::info!("{done}");
            0
        }
        Err(e) => {
            tracing::error!("restart failed: {e}");
            1
        }
    }
}
