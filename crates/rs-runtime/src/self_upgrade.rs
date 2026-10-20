//! #323: unattended upgrade of the Linux box (stream-pp) from the GitHub
//! release.
//!
//! stream.lan gets every release from CI's deploy job. stream-pp is not
//! reachable from CI, and installing a `.deb` needs root, so the `.deb` ships
//! a root systemd timer (`restreamer-upgrade.timer`) that runs
//! `Restreamer --upgrade-check` every 15 min. One check:
//! 1. reads the latest release from the GitHub API;
//! 2. compares it with what is INSTALLED (dpkg), not with its own compiled
//!    version: a half-finished install (dpkg left it unconfigured) is retried,
//!    and a GUI still running a binary dpkg replaced gets its pending restart;
//! 3. stops while the box is busy (an RTMP publisher connected, or an event
//!    receiving or delivering): the app restart would cut the stream. It looks
//!    again right before the install and right before the restart;
//! 4. downloads the `.deb` and its `.sig`, and verifies the minisign signature
//!    against [`UPDATER_PUBKEY`] AND that the signature was made for exactly
//!    that file name (a renamed old package cannot pose as a newer one);
//! 5. installs it and restarts the desktop user's `restreamer` unit
//!    (`self_upgrade_host.rs`, the OS calls).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use base64::Engine as _;
use minisign_verify::{PublicKey, Signature};
use semver::Version;
use serde::Deserialize;
use serde_json::Value;

#[path = "self_upgrade_host.rs"]
mod host;
pub use host::{SystemHost, restart_app_cli, run_cli, verify_files_cli};

/// The updater public key, base64 of the minisign public key file. It must
/// equal `plugins.updater.pubkey` in `src-tauri/tauri.conf.json` (a test
/// pins it); the release signs with the matching private key.
pub const UPDATER_PUBKEY: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDhEMTI3M0YyQzg4OTRCOEQKUldTTlM0bkk4bk1TamJxT3lzbjNGL1hMQ25xbW9ieUJ0bG1Yck5IMG4vT09qSUM4RXp2Rzl2eE4K";

/// The latest published release of this repo.
pub const LATEST_RELEASE_URL: &str =
    "https://api.github.com/repos/zbynekdrlik/restreamer/releases/latest";

/// The app's own status API on the box. It requires `api.bind` to answer on
/// 127.0.0.1 (the default, or 0.0.0.0) and `api.port` 8910.
pub const STATUS_URL: &str = "http://127.0.0.1:8910/api/v1/status";

/// Release tags are `restreamer-v<semver>`.
const TAG_PREFIX: &str = "restreamer-v";

/// Command-line flags of the root CLI modes: a process carrying one is never
/// the operator's GUI.
pub const CLI_FLAGS: [&str; 3] = ["--upgrade-check", "--verify-update", "--restart-app"];

/// The part of a GitHub release this reads.
#[derive(Debug, Deserialize)]
pub struct Release {
    pub tag_name: String,
    pub assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
}

/// The `.deb` of a release and its signature.
#[derive(Debug, PartialEq, Eq)]
pub struct Package {
    pub version: Version,
    pub deb_name: String,
    pub deb_url: String,
    pub sig_url: String,
}

/// The version in a `restreamer-v<semver>` tag.
pub fn release_version(tag: &str) -> Option<Version> {
    Version::parse(tag.strip_prefix(TAG_PREFIX)?).ok()
}

/// The `.deb` file name of a version.
pub fn deb_name(version: &Version) -> String {
    format!("Restreamer_{version}_amd64.deb")
}

/// The release's `Restreamer_<version>_amd64.deb` and `.deb.sig` assets.
pub fn deb_package(release: &Release) -> Result<Package, String> {
    let version = release_version(&release.tag_name).ok_or_else(|| {
        format!(
            "release tag {:?} is not {TAG_PREFIX}<semver>",
            release.tag_name
        )
    })?;
    let deb_name = deb_name(&version);
    let sig_name = format!("{deb_name}.sig");
    let url_of = |name: &str| {
        release
            .assets
            .iter()
            .find(|asset| asset.name == name)
            .map(|asset| asset.browser_download_url.clone())
            .ok_or_else(|| format!("release {} has no asset {name}", release.tag_name))
    };
    Ok(Package {
        deb_url: url_of(&deb_name)?,
        sig_url: url_of(&sig_name)?,
        deb_name,
        version,
    })
}

/// Why the box must not restart now, or `None` when it is idle. An error
/// means the status cannot be read the way this expects.
pub fn busy_reason(status: &Value) -> Result<Option<String>, String> {
    let connected = status
        .pointer("/inpoint/details/rtmp_connected")
        .and_then(Value::as_bool)
        .ok_or("the status has no inpoint.details.rtmp_connected")?;
    if connected {
        return Ok(Some("an RTMP publisher is connected".into()));
    }
    let Some(event) = status
        .get("streaming_event")
        .filter(|event| !event.is_null())
    else {
        return Ok(None);
    };
    for flag in ["receiving_activated", "delivering_activated"] {
        if event.get(flag).and_then(Value::as_bool) == Some(true) {
            let name = event.get("name").and_then(Value::as_str).unwrap_or("?");
            return Ok(Some(format!("event {name:?} has {flag}")));
        }
    }
    Ok(None)
}

/// Verifies `data` against a Tauri updater signature (base64 of the minisign
/// signature file) with `pubkey` (base64 of the minisign public key file),
/// the way `tauri-plugin-updater` does, and requires the signed (trusted)
/// comment to name `file_name`: the Tauri signer writes `file:<name>` there.
pub fn verify_signature(
    data: &[u8],
    signature: &str,
    pubkey: &str,
    file_name: &str,
) -> Result<(), String> {
    let public_key = PublicKey::decode(&base64_text(pubkey)?)
        .map_err(|e| format!("bad updater public key: {e}"))?;
    let signature = Signature::decode(&base64_text(signature)?)
        .map_err(|e| format!("bad signature file: {e}"))?;
    public_key
        .verify(data, &signature, true)
        .map_err(|e| format!("the signature does not verify: {e}"))?;
    let signed_for = format!("file:{file_name}");
    if signature
        .trusted_comment()
        .split('\t')
        .any(|field| field == signed_for)
    {
        Ok(())
    } else {
        Err(format!(
            "the signature was made for another file ({:?}), not {file_name}",
            signature.trusted_comment()
        ))
    }
}

fn base64_text(encoded: &str) -> Result<String, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|e| format!("not base64: {e}"))?;
    String::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))
}

/// The installed `restreamer` package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installed {
    /// dpkg's status is `ii`: unpacked AND configured.
    pub configured: bool,
    pub version: Version,
}

/// Parses `dpkg-query -W -f='${db:Status-Abbrev}${Version}' restreamer`
/// (`ii 0.29.53`; `iU 0.29.53` is unpacked but not configured).
pub fn parse_dpkg_query(output: &str) -> Option<Installed> {
    let mut fields = output.split_whitespace();
    let status = fields.next()?;
    let version = Version::parse(fields.next()?).ok()?;
    Some(Installed {
        configured: status == "ii",
        version,
    })
}

/// Whether `latest` still has to be installed: it is not installed, older,
/// or dpkg did not finish configuring it.
pub fn install_needed(installed: Option<&Installed>, latest: &Version) -> bool {
    !installed.is_some_and(|installed| installed.configured && installed.version >= *latest)
}

/// Whether a process's `/proc/<pid>/exe` link target says its binary was
/// replaced on disk since it started (dpkg installed a new one).
pub fn exe_replaced(exe_target: &Path) -> bool {
    exe_target.to_string_lossy().ends_with(" (deleted)")
}

/// The operator's GUI process.
#[derive(Debug, PartialEq, Eq)]
pub struct AppProcess {
    pub pid: u32,
    pub uid: u32,
}

/// The GUI process in `proc_root`: named `Restreamer`, not `own_pid`, not
/// root, and not running one of the root CLI modes ([`CLI_FLAGS`]).
pub fn app_process(proc_root: &Path, own_pid: u32) -> Option<AppProcess> {
    std::fs::read_dir(proc_root)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            (pid != own_pid).then(|| (pid, entry.path()))
        })
        .filter(|(_, dir)| {
            std::fs::read_to_string(dir.join("comm")).is_ok_and(|comm| comm.trim() == "Restreamer")
        })
        .filter(|(_, dir)| {
            let cmdline = std::fs::read(dir.join("cmdline")).unwrap_or_default();
            !cmdline
                .split(|byte| *byte == 0)
                .any(|arg| CLI_FLAGS.iter().any(|flag| arg == flag.as_bytes()))
        })
        .find_map(|(pid, dir)| {
            let uid = status_uid(&std::fs::read_to_string(dir.join("status")).ok()?)?;
            (uid != 0).then_some(AppProcess { pid, uid })
        })
}

/// The uid on the `Uid:` line of a `/proc/<pid>/status` file.
pub fn status_uid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// The user name of `uid` in an `/etc/passwd` file.
pub fn passwd_user(passwd: &str, uid: u32) -> Option<String> {
    passwd.lines().find_map(|line| {
        let mut fields = line.split(':');
        let name = fields.next()?;
        (fields.nth(1)?.parse::<u32>().ok()? == uid).then(|| name.to_owned())
    })
}

/// Runs `command`; an exit code other than 0 is an error carrying its stderr.
pub fn run_command(command: &mut Command) -> Result<(), String> {
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

/// Writes `bytes` to a NEW file at `path`, replacing a stale one: never
/// through an existing file or a link someone placed there.
pub fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("removing the stale {}: {e}", path.display())),
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(|e| format!("writing {}: {e}", path.display()))
}

/// The OS side of an upgrade.
pub trait Host {
    /// The installed `restreamer` package, `None` when it is not installed.
    fn installed(&self) -> Result<Option<Installed>, String>;
    /// Whether the GUI still runs a binary that an install replaced.
    fn restart_pending(&self) -> bool;
    /// Installs the `.deb` at `deb`, with its dependencies.
    fn install(&self, deb: &Path) -> Result<(), String>;
    /// Restarts the running GUI on the new version; returns what it did.
    fn restart_app(&self) -> Result<String, String>;
}

/// What one check did.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The latest release is installed and running.
    UpToDate { latest: Version },
    /// Something is to do, but the box streams.
    Busy { latest: Version, reason: String },
    /// Installed; the box turned busy before the restart. A later check
    /// restarts (the GUI runs a replaced binary).
    RestartPending { latest: Version, reason: String },
    /// Installed (`installed`, else only the pending restart) and restarted.
    Upgraded {
        latest: Version,
        installed: bool,
        restart: String,
    },
}

/// One upgrade check, with every endpoint a parameter so a test can point
/// it at a local server.
pub struct Upgrader<H> {
    pub http: reqwest::Client,
    pub latest_url: String,
    /// Sent only to `latest_url` (CI; the box asks anonymously).
    pub github_token: Option<String>,
    pub status_url: String,
    pub pubkey: String,
    pub download_dir: PathBuf,
    pub host: H,
}

impl<H: Host> Upgrader<H> {
    pub async fn run_once(&self) -> Result<Outcome, String> {
        let mut request = self.http.get(&self.latest_url);
        if let Some(token) = &self.github_token {
            request = request.bearer_auth(token);
        }
        let release: Release = send(request, &self.latest_url)
            .await?
            .json()
            .await
            .map_err(|e| {
                format!(
                    "the latest release from {} is not readable: {e}",
                    self.latest_url
                )
            })?;
        let package = deb_package(&release)?;
        let latest = package.version.clone();
        let install = install_needed(self.host.installed()?.as_ref(), &latest);
        if !install && !self.host.restart_pending() {
            return Ok(Outcome::UpToDate { latest });
        }
        if let Some(reason) = self.busy().await? {
            return Ok(Outcome::Busy { latest, reason });
        }

        if install {
            let deb = self.bytes(&package.deb_url).await?;
            let signature = String::from_utf8(self.bytes(&package.sig_url).await?)
                .map_err(|e| format!("{} is not text: {e}", package.sig_url))?;
            verify_signature(&deb, &signature, &self.pubkey, &package.deb_name)
                .map_err(|e| format!("{}: {e}", package.deb_name))?;
            // The download took a while: look again right before the install.
            if let Some(reason) = self.busy().await? {
                return Ok(Outcome::Busy { latest, reason });
            }
            let path = self.download_dir.join(&package.deb_name);
            write_new(&path, &deb)?;
            let installed = self.host.install(&path);
            // Installed or failed, the copy goes.
            let _ = std::fs::remove_file(&path);
            installed?;
        }

        if let Some(reason) = self.busy().await? {
            return Ok(Outcome::RestartPending { latest, reason });
        }
        let restart = self.host.restart_app()?;
        Ok(Outcome::Upgraded {
            latest,
            installed: install,
            restart,
        })
    }

    /// Why the box is busy, or `None`. Nothing listening on the status port
    /// means the app is down, so nothing can stream: idle (the release may
    /// be the fix for a crash loop).
    async fn busy(&self) -> Result<Option<String>, String> {
        let response = match self.http.get(&self.status_url).send().await {
            Err(e) if e.is_connect() => return Ok(None),
            sent => sent
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| format!("GET {}: {e}", self.status_url))?,
        };
        let status: Value = response
            .json()
            .await
            .map_err(|e| format!("the app status is not readable: {e}"))?;
        busy_reason(&status)
    }

    async fn bytes(&self, url: &str) -> Result<Vec<u8>, String> {
        send(self.http.get(url), url)
            .await?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| format!("GET {url}: {e}"))
    }
}

async fn send(request: reqwest::RequestBuilder, url: &str) -> Result<reqwest::Response, String> {
    request
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| format!("GET {url}: {e}"))
}

#[cfg(test)]
#[path = "self_upgrade_tests.rs"]
mod tests;
