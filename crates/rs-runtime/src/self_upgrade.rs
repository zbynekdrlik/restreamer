//! #323: unattended upgrade of the Linux box (stream-pp) from the GitHub
//! release.
//!
//! stream.lan gets every release from CI's deploy job. stream-pp is not
//! reachable from CI, and installing a `.deb` needs root, so the `.deb` ships
//! a root systemd timer (`restreamer-upgrade.timer`) that runs
//! `Restreamer --upgrade-check` every 15 min. One check:
//! 1. reads the latest release from the GitHub API;
//! 2. stops unless its version is newer than this binary's;
//! 3. stops while the box is busy (an RTMP publisher connected, or an event
//!    receiving or delivering): the app restart would cut the stream;
//! 4. downloads the `.deb` and its `.sig` and verifies the minisign signature
//!    against [`UPDATER_PUBKEY`];
//! 5. installs it and restarts the desktop user's `restreamer` unit
//!    (`self_upgrade_host.rs`, the OS calls).

use std::path::{Path, PathBuf};

use base64::Engine as _;
use minisign_verify::{PublicKey, Signature};
use semver::Version;
use serde::Deserialize;
use serde_json::Value;

#[path = "self_upgrade_host.rs"]
mod host;
pub use host::{SystemHost, run_cli, verify_files_cli};

/// The updater public key, base64 of the minisign public key file. It must
/// equal `plugins.updater.pubkey` in `src-tauri/tauri.conf.json` (a test
/// pins it); the release signs with the matching private key.
pub const UPDATER_PUBKEY: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDhEMTI3M0YyQzg4OTRCOEQKUldTTlM0bkk4bk1TamJxT3lzbjNGL1hMQ25xbW9ieUJ0bG1Yck5IMG4vT09qSUM4RXp2Rzl2eE4K";

/// The latest published release of this repo.
pub const LATEST_RELEASE_URL: &str =
    "https://api.github.com/repos/zbynekdrlik/restreamer/releases/latest";

/// The app's own status API on the box.
pub const STATUS_URL: &str = "http://127.0.0.1:8910/api/v1/status";

/// Release tags are `restreamer-v<semver>`.
const TAG_PREFIX: &str = "restreamer-v";

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

/// The release's `Restreamer_<version>_amd64.deb` and `.deb.sig` assets.
pub fn deb_package(release: &Release) -> Result<Package, String> {
    let version = release_version(&release.tag_name).ok_or_else(|| {
        format!(
            "release tag {:?} is not {TAG_PREFIX}<semver>",
            release.tag_name
        )
    })?;
    let deb_name = format!("Restreamer_{version}_amd64.deb");
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
/// the way `tauri-plugin-updater` does.
pub fn verify_signature(data: &[u8], signature: &str, pubkey: &str) -> Result<(), String> {
    let public_key = PublicKey::decode(&base64_text(pubkey)?)
        .map_err(|e| format!("bad updater public key: {e}"))?;
    let signature = Signature::decode(&base64_text(signature)?)
        .map_err(|e| format!("bad signature file: {e}"))?;
    public_key
        .verify(data, &signature, true)
        .map_err(|e| format!("the signature does not verify: {e}"))
}

fn base64_text(encoded: &str) -> Result<String, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|e| format!("not base64: {e}"))?;
    String::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))
}

/// The OS side of an upgrade: installing the package and restarting the app.
pub trait Host {
    /// Installs the `.deb` at `deb`.
    fn install(&self, deb: &Path) -> Result<(), String>;
    /// Restarts the running app on the new version; returns what it did.
    fn restart_app(&self) -> Result<String, String>;
}

/// What one check did.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    UpToDate {
        latest: Version,
    },
    Busy {
        latest: Version,
        reason: String,
    },
    Installed {
        from: Version,
        to: Version,
        restart: String,
    },
}

/// One upgrade check, with every endpoint a parameter so a test can point
/// it at a local server.
pub struct Upgrader<H> {
    pub http: reqwest::Client,
    pub latest_url: String,
    pub status_url: String,
    pub pubkey: String,
    pub current: Version,
    pub download_dir: PathBuf,
    pub host: H,
}

impl<H: Host> Upgrader<H> {
    pub async fn run_once(&self) -> Result<Outcome, String> {
        let release: Release = self
            .get(&self.latest_url)
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
        if package.version <= self.current {
            return Ok(Outcome::UpToDate {
                latest: package.version,
            });
        }

        let status: Value = self
            .get(&self.status_url)
            .await?
            .json()
            .await
            .map_err(|e| format!("the app status is not readable: {e}"))?;
        if let Some(reason) = busy_reason(&status)? {
            return Ok(Outcome::Busy {
                latest: package.version,
                reason,
            });
        }

        let deb = self.bytes(&package.deb_url).await?;
        let signature = String::from_utf8(self.bytes(&package.sig_url).await?)
            .map_err(|e| format!("{} is not text: {e}", package.sig_url))?;
        verify_signature(&deb, &signature, &self.pubkey)
            .map_err(|e| format!("{}: {e}", package.deb_name))?;

        let path = self.download_dir.join(&package.deb_name);
        std::fs::write(&path, &deb).map_err(|e| format!("writing {}: {e}", path.display()))?;
        let installed = self.host.install(&path);
        // The package is installed (or failed); either way the copy goes.
        let _ = std::fs::remove_file(&path);
        installed?;
        let restart = self.host.restart_app()?;
        Ok(Outcome::Installed {
            from: self.current.clone(),
            to: package.version,
            restart,
        })
    }

    async fn get(&self, url: &str) -> Result<reqwest::Response, String> {
        self.http
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("GET {url}: {e}"))
    }

    async fn bytes(&self, url: &str) -> Result<Vec<u8>, String> {
        self.get(url)
            .await?
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| format!("GET {url}: {e}"))
    }
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

/// The uid that runs the app's GUI: the owner of a `Restreamer` process in
/// `proc_root` other than `own_pid` (the upgrade check runs the same binary).
pub fn app_owner_uid(proc_root: &Path, own_pid: u32) -> Option<u32> {
    std::fs::read_dir(proc_root)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            (pid != own_pid).then_some(entry.path())
        })
        .find(|dir| {
            std::fs::read_to_string(dir.join("comm")).is_ok_and(|comm| comm.trim() == "Restreamer")
        })
        .and_then(|dir| status_uid(&std::fs::read_to_string(dir.join("status")).ok()?))
}

#[cfg(test)]
#[path = "self_upgrade_tests.rs"]
mod tests;
