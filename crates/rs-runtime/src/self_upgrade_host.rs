//! The OS side of the #323 upgrade check: `dpkg -i`, the restart of the
//! desktop user's unit, and the two CLI entry points. Every decision lives in
//! the tested helpers of `self_upgrade.rs`; like the `*_windows.rs` glue,
//! this file is excluded from mutation testing (`.cargo/mutants.toml`).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use semver::Version;

use super::{
    Host, LATEST_RELEASE_URL, Outcome, STATUS_URL, UPDATER_PUBKEY, Upgrader, app_owner_uid,
    passwd_user, verify_signature,
};

/// One request, the ~22 MB `.deb` included, over the venue's line.
const HTTP_TIMEOUT: Duration = Duration::from_secs(300);

/// `dpkg` and `systemctl` on the box.
pub struct SystemHost;

impl Host for SystemHost {
    fn install(&self, deb: &Path) -> Result<(), String> {
        run(Command::new("dpkg").arg("-i").arg(deb))
    }

    fn restart_app(&self) -> Result<String, String> {
        let Some(uid) = app_owner_uid(Path::new("/proc"), std::process::id()) else {
            return Ok("the app is not running; its next start runs the new version".into());
        };
        let passwd = std::fs::read_to_string("/etc/passwd")
            .map_err(|e| format!("reading /etc/passwd: {e}"))?;
        let user =
            passwd_user(&passwd, uid).ok_or_else(|| format!("uid {uid} is not in /etc/passwd"))?;
        let machine = format!("{user}@");
        // The package ships the user unit, so reload before the restart.
        run(Command::new("systemctl").args(["--user", "-M", &machine, "daemon-reload"]))?;
        run(Command::new("systemctl").args(["--user", "-M", &machine, "restart", "restreamer"]))?;
        Ok(format!("restarted the restreamer unit of {user}"))
    }
}

fn run(command: &mut Command) -> Result<(), String> {
    let output = command.output().map_err(|e| format!("{command:?}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{command:?} failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// `Restreamer --upgrade-check`, run as root by `restreamer-upgrade.service`.
/// Returns the process exit code; every outcome is logged to the journal.
pub fn run_cli() -> i32 {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .try_init();
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
    // The box asks anonymously (4 calls an hour); CI passes its token, since
    // a shared runner address can be over GitHub's anonymous rate limit.
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(value) = std::env::var("GITHUB_TOKEN")
        .ok()
        .and_then(|token| reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).ok())
    {
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let http = match reqwest::Client::builder()
        .user_agent(concat!("restreamer-upgrade/", env!("CARGO_PKG_VERSION")))
        .default_headers(headers)
        .timeout(HTTP_TIMEOUT)
        .build()
    {
        Ok(http) => http,
        Err(e) => {
            tracing::error!("upgrade check: no HTTP client: {e}");
            return 1;
        }
    };
    let current = Version::parse(env!("CARGO_PKG_VERSION")).expect("the crate version is semver");
    // systemd's CacheDirectory= (root only): never a world-writable /tmp,
    // where another user could plant a symlink under the package's name.
    let download_dir = std::env::var_os("CACHE_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let upgrader = Upgrader {
        http,
        latest_url: LATEST_RELEASE_URL.into(),
        status_url: STATUS_URL.into(),
        pubkey: UPDATER_PUBKEY.into(),
        current: current.clone(),
        download_dir,
        host: SystemHost,
    };
    match runtime.block_on(upgrader.run_once()) {
        Ok(Outcome::UpToDate { latest }) => {
            tracing::info!("up to date: {current} (latest release {latest})");
            0
        }
        Ok(Outcome::Busy { latest, reason }) => {
            tracing::info!("{latest} is out, not installed now: {reason}");
            0
        }
        Ok(Outcome::Installed { from, to, restart }) => {
            tracing::info!("upgraded {from} -> {to}; {restart}");
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
    let verified = std::fs::read(deb)
        .map_err(|e| format!("reading it: {e}"))
        .and_then(|data| {
            let signature = std::fs::read_to_string(sig)
                .map_err(|e| format!("reading {}: {e}", sig.display()))?;
            verify_signature(&data, &signature, UPDATER_PUBKEY)
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
