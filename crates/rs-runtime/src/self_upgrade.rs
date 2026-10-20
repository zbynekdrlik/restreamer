//! #323: unattended upgrade of the Linux box (stream-pp) from the GitHub
//! release.
//!
//! stream.lan gets every release from CI's deploy job. stream-pp is not
//! reachable from CI, and installing a `.deb` needs root, so the `.deb` ships
//! a root systemd timer (`restreamer-upgrade.timer`) that runs
//! `Restreamer --upgrade-check` every 15 min. One check:
//! 1. reads the latest release from the GitHub API;
//! 2. compares it with what is INSTALLED (dpkg), not with its own compiled
//!    version: a half-finished install (dpkg left it unpacked) is retried,
//!    and a GUI still running a binary dpkg replaced gets its pending restart.
//!    An INTERRUPTED dpkg run ("you must manually run dpkg --configure -a")
//!    fails every check until unattended-upgrades or a person runs that;
//! 3. stops while the box is busy (an RTMP publisher connected, or an event
//!    receiving or delivering): the app restart would cut the stream. All the
//!    slow work (download, verify, fetching new dependencies) comes first; it
//!    looks again right before the install, waits at most 10 s for a dpkg lock
//!    another install holds (else it defers to the next check), and restarts
//!    right after: an old binary serving the new package's files is worse than
//!    a restart (it would upload the new bundled VPS binary as the old
//!    version). So between the last idle check and the restart there is at
//!    most ~10 s of lock wait plus the apt/dpkg run itself;
//! 4. downloads the `.deb` and its `.sig`, and verifies the minisign signature
//!    against [`UPDATER_PUBKEY`] AND that the signature was made for exactly
//!    that file name (a renamed old package cannot pose as a newer one);
//! 5. installs it and restarts the desktop user's `restreamer` unit
//!    (`self_upgrade_host.rs`, the OS calls).

use std::error::Error as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

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

/// The status API answers at once on a healthy box; a hung app is not idle.
pub const STATUS_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// dpkg's selection: `install`, or `hold` / `deinstall` / `purge` when
    /// the operator pinned the package or is removing it.
    pub want: String,
    /// dpkg finished configuring it (status `installed`, not `unpacked`).
    pub configured: bool,
    pub version: Version,
}

impl Installed {
    /// The operator pinned (`apt-mark hold`) or is removing the package: the
    /// upgrade check never touches it.
    pub fn left_alone(&self) -> bool {
        self.want != "install"
    }
}

/// The `dpkg-query -f` format [`parse_dpkg_query`] reads.
pub const DPKG_QUERY_FORMAT: &str = "${db:Status-Want} ${db:Status-Status} ${Version}";

/// Parses [`DPKG_QUERY_FORMAT`] output: `install installed 0.29.53`,
/// `hold installed 0.29.53`, `install unpacked 0.29.53`,
/// `deinstall config-files 0.29.53`, ...
pub fn parse_dpkg_query(output: &str) -> Option<Installed> {
    let mut fields = output.split_whitespace();
    let want = fields.next()?;
    let status = fields.next()?;
    let version = Version::parse(fields.next()?).ok()?;
    Some(Installed {
        want: want.to_owned(),
        configured: status == "installed",
        version,
    })
}

/// Whether `latest` has to be installed: nothing is installed, an older
/// version is, or `latest` itself is but dpkg did not finish configuring it.
/// A NEWER installed version is never downgraded.
pub fn install_needed(installed: Option<&Installed>, latest: &Version) -> bool {
    match installed {
        None => true,
        Some(installed) => {
            installed.version < *latest || (installed.version == *latest && !installed.configured)
        }
    }
}

/// Whether an apt-get failure is only an apt/dpkg lock held by another apt
/// run (unattended-upgrades, apt-daily): a deferral, not an error. Only the
/// stderr part counts: while apt WAITS for a lock it prints "Waiting for cache
/// lock: Could not get lock ..." on stdout, and a run that then got the lock
/// and failed for a real reason must stay an error. On stderr apt says "...
/// is another process using it?" when its wait for the dpkg frontend or
/// admin-directory lock runs out (each retry's "Could not get lock" only goes
/// to stdout as progress), and "Could not get lock ..." for the archives lock,
/// which it never waits for; "... are you root?" is a missing permission, a
/// real error. apt runs with `LC_ALL=C`
/// ([`apt_get_install`]), so these are the English texts.
pub fn dpkg_locked(error: &str) -> bool {
    let stderr = error.split(STDOUT_MARKER).next().unwrap_or(error);
    stderr.contains("Could not get lock") || stderr.contains("is another process using it?")
}

/// `apt-get install` of a local `.deb` (`download_only`: fetch what it needs,
/// install nothing), not `dpkg -i`: it installs a new Depends too.
/// - `-f` repairs a half-install (a hand `dpkg -i` with a missing Depends
///   leaves the package unpacked, and plain apt-get then refuses every later
///   install); it also completes ANY other broken package on the box (installs
///   its missing Depends), since such breakage blocked every upgrade;
/// - `--no-remove` makes apt abort rather than remove another package
///   unattended;
/// - `DPkg::Lock::Timeout=10` waits at most 10 s for a held dpkg lock,
///   whatever the box's apt config says (stream-pp sets 600);
/// - `LC_ALL=C` keeps the messages [`dpkg_locked`] reads in English.
pub fn apt_get_install(deb: &Path, download_only: bool) -> Command {
    let mut command = Command::new("apt-get");
    command
        .env("DEBIAN_FRONTEND", "noninteractive")
        .env("LC_ALL", "C")
        .args(["install", "-y", "-f", "--no-remove"])
        .args(["-o", "DPkg::Lock::Timeout=10"]);
    if download_only {
        command.arg("--download-only");
    }
    command.arg(deb);
    command
}

/// Whether a reqwest error is a refused connection: nothing listens there.
pub fn connection_refused(error: &reqwest::Error) -> bool {
    let mut source = error.source();
    while let Some(cause) = source {
        if cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
        {
            return true;
        }
        source = cause.source();
    }
    false
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

/// Runs `command`; an exit code other than 0 is an error carrying its stderr
/// and the end of its stdout ([`failure_text`]).
pub fn run_command(command: &mut Command) -> Result<(), String> {
    let output = command.output().map_err(|e| format!("{command:?}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{command:?} failed ({}): {}",
            output.status,
            failure_text(
                &String::from_utf8_lossy(&output.stdout),
                &String::from_utf8_lossy(&output.stderr)
            )
        ))
    }
}

/// Lines of stdout a failure keeps.
const STDOUT_TAIL_LINES: usize = 20;

/// Separates stderr from the stdout tail in a [`failure_text`].
pub const STDOUT_MARKER: &str = " | stdout: ";

/// stderr, then the last [`STDOUT_TAIL_LINES`] non-empty lines of stdout:
/// apt names the package it would remove or cannot satisfy only on stdout
/// ("The following packages will be REMOVED: ..."), while stderr says just
/// "E: Packages need to be removed but remove is disabled."
pub fn failure_text(stdout: &str, stderr: &str) -> String {
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let tail = &lines[lines.len().saturating_sub(STDOUT_TAIL_LINES)..];
    if tail.is_empty() {
        stderr.trim().to_owned()
    } else {
        format!("{}{STDOUT_MARKER}{}", stderr.trim(), tail.join(" / "))
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
    /// Whether the operator's GUI process runs.
    fn app_running(&self) -> bool;
    /// Whether the GUI still runs a binary that an install replaced (an
    /// earlier restart failed).
    fn restart_pending(&self) -> bool;
    /// Downloads what installing `deb` needs (new dependencies), installing
    /// nothing: the slow part, done before the last idle check.
    fn fetch_dependencies(&self, deb: &Path) -> Result<(), String>;
    /// Installs the `.deb` at `deb` without waiting long for a held dpkg lock
    /// (an error that [`dpkg_locked`] recognises).
    fn install(&self, deb: &Path) -> Result<(), String>;
    /// Restarts the running GUI on the new version; returns what it did.
    fn restart_app(&self) -> Result<String, String>;
}

/// What one check did.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The latest release is installed and running.
    UpToDate { latest: Version },
    /// The operator holds or is removing the package: nothing is touched.
    LeftAlone {
        latest: Version,
        installed: Version,
        want: String,
    },
    /// Something is to do, but the box streams.
    Busy { latest: Version, reason: String },
    /// Installed (`installed`, else only a pending restart) and restarted.
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
    /// Bound on one status request ([`STATUS_TIMEOUT`] on the box).
    pub status_timeout: Duration,
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
        let latest = release_version(&release.tag_name).ok_or_else(|| {
            format!(
                "release tag {:?} is not {TAG_PREFIX}<semver>",
                release.tag_name
            )
        })?;
        let installed = self.host.installed()?;
        if let Some(pinned) = installed
            .as_ref()
            .filter(|installed| installed.left_alone())
        {
            return Ok(Outcome::LeftAlone {
                latest,
                installed: pinned.version.clone(),
                want: pinned.want.clone(),
            });
        }
        let install = install_needed(installed.as_ref(), &latest);
        if !install && !self.host.restart_pending() {
            return Ok(Outcome::UpToDate { latest });
        }
        if let Some(reason) = self.busy().await? {
            return Ok(Outcome::Busy { latest, reason });
        }

        if install {
            // Only now: a release without its .deb/.sig must not turn the
            // checks of an up-to-date box into errors.
            let package = deb_package(&release)?;
            let deb = self.bytes(&package.deb_url).await?;
            let signature = String::from_utf8(self.bytes(&package.sig_url).await?)
                .map_err(|e| format!("{} is not text: {e}", package.sig_url))?;
            verify_signature(&deb, &signature, &self.pubkey, &package.deb_name)
                .map_err(|e| format!("{}: {e}", package.deb_name))?;
            let path = self.download_dir.join(&package.deb_name);
            write_new(&path, &deb)?;
            let installed = self.install_when_idle(&path, &latest).await;
            // Installed, deferred or failed, the copy goes.
            let _ = std::fs::remove_file(&path);
            if let Some(deferred) = installed? {
                return Ok(deferred);
            }
        }

        // Right away: until the restart, the old binary would serve the new
        // package's files (see the module doc).
        let restart = self.host.restart_app()?;
        Ok(Outcome::Upgraded {
            latest,
            installed: install,
            restart,
        })
    }

    /// Fetches the new dependencies, then installs only while the box is still
    /// idle; `Some(outcome)` when it deferred. A restart follows only an
    /// install that really finished (dpkg: `latest`, configured).
    async fn install_when_idle(
        &self,
        deb: &Path,
        latest: &Version,
    ) -> Result<Option<Outcome>, String> {
        // unattended-upgrades holds the lock for minutes, so this first apt
        // call is the one that usually meets it.
        match self.host.fetch_dependencies(deb) {
            Err(e) if dpkg_locked(&e) => return Ok(Some(locked(latest, &e))),
            fetched => fetched?,
        }
        if let Some(reason) = self.busy().await? {
            return Ok(Some(Outcome::Busy {
                latest: latest.clone(),
                reason,
            }));
        }
        match self.host.install(deb) {
            Err(e) if dpkg_locked(&e) => return Ok(Some(locked(latest, &e))),
            installed => installed?,
        }
        match self.host.installed()? {
            Some(now) if now.configured && now.version == *latest => Ok(None),
            now => Err(format!(
                "{latest} is not installed and configured after the install (dpkg: {now:?}); \
                 not restarting"
            )),
        }
    }

    /// Why the box is busy, or `None`. A refused connection while no GUI
    /// process runs means the app is down, so nothing can stream: idle (the
    /// release may be the fix for a crash loop). Any other failure, or a
    /// refusal while the GUI runs (another port or bind), is an error.
    async fn busy(&self) -> Result<Option<String>, String> {
        let sent = self
            .http
            .get(&self.status_url)
            .timeout(self.status_timeout)
            .send()
            .await;
        let response = match sent {
            Err(e) if connection_refused(&e) => {
                if !self.host.app_running() {
                    return Ok(None);
                }
                return Err(format!(
                    "the status port {} refused while the GUI runs: does api.bind/api.port \
                     answer on 127.0.0.1:8910?",
                    self.status_url
                ));
            }
            Err(e) if e.is_timeout() => {
                return Err(format!(
                    "no status from {} within {:?}",
                    self.status_url, self.status_timeout
                ));
            }
            sent => sent
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| {
                    // reqwest's own text leaves the cause out: add the chain.
                    let mut text = format!("GET {}: {e}", self.status_url);
                    let mut cause = e.source();
                    while let Some(inner) = cause {
                        text.push_str(&format!(": {inner}"));
                        cause = inner.source();
                    }
                    text
                })?,
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

/// The deferral for a dpkg lock another install holds.
fn locked(latest: &Version, error: &str) -> Outcome {
    Outcome::Busy {
        latest: latest.clone(),
        reason: format!("an apt/dpkg lock is held by another apt run: {error}"),
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
