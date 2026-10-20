//! Tests for `self_upgrade.rs` (#323).

use super::*;
use std::sync::Mutex;
use wiremock::matchers::{header, header_exists, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `Restreamer_0.29.54_amd64.deb` holding `PAYLOAD`, signed with a throwaway
/// key (`cargo tauri signer generate`, the private half discarded). `PUB_A` is
/// its public key, `PUB_B` another one. The signed comment names the file.
const PAYLOAD: &[u8] = b"restreamer self-upgrade test payload (#323)\n";
const SIGNED_DEB: &str = "Restreamer_0.29.54_amd64.deb";
const PUB_A: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDExRjc4MEQxMEExMjQyMUMKUldRY1FoSUswWUQzRVlURTczTVFHb3lCZkxEYW5scndKODdTNGpWU3djL3JNRjBCQkNna2VSUzUK";
const PUB_B: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDJEMjJDRUREMDI5RkRGMjcKUldRbjM1OEMzYzRpTFVia3NIQnRUb05zQ2I3SWhscHdpYUZXdTdTVnhjMmtoNCtMdlI0eGhQaE8K";
const SIG_A: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IHNpZ25hdHVyZSBmcm9tIHRhdXJpIHNlY3JldCBrZXkKUlVRY1FoSUswWUQzRVFyRWhad1prUVpkb1NTUWh4NGRCTWl4NEtWRXJoY2RoYXdmUVhnWGdqZGNzdFN1ai9yeklmQ1hiUk9PT3Zma1FCY0JvWlQ2Snp1S1JZTk8rVm9jWkEwPQp0cnVzdGVkIGNvbW1lbnQ6IHRpbWVzdGFtcDoxNzkyNDYxNjkyCWZpbGU6UmVzdHJlYW1lcl8wLjI5LjU0X2FtZDY0LmRlYgpXVEUrc2s2c2dqMkhraVM2N0d3UVdvblY5U0J5RUpLUzlaL1hmbkdoenduejViOU9UZTdhb0dMdGllR3NwanVsSHVMMEhqeEtIN3MzcmJ0a2xyNDFCQT09Cg==";
const TAMPERED: &[u8] = b"restreamer self-upgrade test payload (#323)!\n";

fn v(text: &str) -> Version {
    Version::parse(text).unwrap()
}

fn release(tag: &str, names: &[&str]) -> Release {
    Release {
        tag_name: tag.into(),
        assets: names
            .iter()
            .map(|name| Asset {
                name: (*name).into(),
                browser_download_url: format!("https://example.invalid/{name}"),
            })
            .collect(),
    }
}

// --- the pure helpers ----------------------------------------------------

#[test]
fn a_release_tag_carries_its_version() {
    assert_eq!(release_version("restreamer-v0.29.53"), Some(v("0.29.53")));
    assert_eq!(release_version("v0.29.53"), None, "the prefix is required");
    assert_eq!(release_version("restreamer-vnext"), None);
}

#[test]
fn the_package_is_the_deb_and_its_signature() {
    let package = deb_package(&release(
        "restreamer-v0.29.53",
        &[
            "Restreamer_0.29.53_x64-setup.exe",
            "Restreamer_0.29.53_amd64.deb",
            "Restreamer_0.29.53_amd64.deb.sig",
        ],
    ))
    .unwrap();
    assert_eq!(
        package,
        Package {
            version: v("0.29.53"),
            deb_name: "Restreamer_0.29.53_amd64.deb".into(),
            deb_url: "https://example.invalid/Restreamer_0.29.53_amd64.deb".into(),
            sig_url: "https://example.invalid/Restreamer_0.29.53_amd64.deb.sig".into(),
        }
    );
}

#[test]
fn a_release_without_the_signature_or_the_deb_is_refused() {
    let unsigned = deb_package(&release(
        "restreamer-v0.29.53",
        &["Restreamer_0.29.53_amd64.deb"],
    ));
    assert!(unsigned.unwrap_err().contains(".deb.sig"));
    let no_deb = deb_package(&release(
        "restreamer-v0.29.53",
        &["Restreamer_0.29.53_amd64.deb.sig"],
    ));
    assert!(no_deb.unwrap_err().contains("amd64.deb"));
    let bad_tag = deb_package(&release("latest", &[]));
    assert!(bad_tag.unwrap_err().contains("is not restreamer-v"));
}

fn status(connected: bool, event: Value) -> Value {
    serde_json::json!({
        "inpoint": {"state": "idle", "details": {"rtmp_connected": connected}},
        "streaming_event": event,
    })
}

fn idle() -> Value {
    status(false, Value::Null)
}

fn streaming() -> Value {
    status(true, Value::Null)
}

#[test]
fn an_idle_box_has_no_publisher_and_no_live_event() {
    assert_eq!(busy_reason(&idle()), Ok(None));
    let quiet_event = serde_json::json!({
        "name": "PP-live", "receiving_activated": false, "delivering_activated": false
    });
    assert_eq!(busy_reason(&status(false, quiet_event)), Ok(None));
}

#[test]
fn a_connected_publisher_or_a_live_event_is_busy() {
    assert_eq!(
        busy_reason(&streaming()),
        Ok(Some("an RTMP publisher is connected".into()))
    );
    for flag in ["receiving_activated", "delivering_activated"] {
        let mut event = serde_json::json!({
            "name": "PP-live", "receiving_activated": false, "delivering_activated": false
        });
        event[flag] = Value::Bool(true);
        assert_eq!(
            busy_reason(&status(false, event)),
            Ok(Some(format!("event \"PP-live\" has {flag}")))
        );
    }
}

#[test]
fn a_status_without_the_publisher_flag_is_an_error() {
    let err = busy_reason(&serde_json::json!({"inpoint": {}})).unwrap_err();
    assert!(err.contains("rtmp_connected"), "{err}");
}

#[test]
fn a_good_signature_for_that_file_verifies() {
    assert_eq!(verify_signature(PAYLOAD, SIG_A, PUB_A, SIGNED_DEB), Ok(()));
}

#[test]
fn another_key_or_other_bytes_fail_the_signature() {
    let wrong_key = verify_signature(PAYLOAD, SIG_A, PUB_B, SIGNED_DEB).unwrap_err();
    assert!(wrong_key.contains("does not verify"), "{wrong_key}");
    let tampered = verify_signature(TAMPERED, SIG_A, PUB_A, SIGNED_DEB).unwrap_err();
    assert!(tampered.contains("does not verify"), "{tampered}");
    assert!(
        verify_signature(PAYLOAD, "not base64 !", PUB_A, SIGNED_DEB)
            .unwrap_err()
            .contains("not base64")
    );
    assert!(
        verify_signature(PAYLOAD, SIG_A, "AAAA", SIGNED_DEB)
            .unwrap_err()
            .contains("bad updater public key")
    );
}

#[test]
fn a_signature_made_for_another_file_name_is_refused() {
    // A renamed old package must not pose as a newer release.
    let renamed =
        verify_signature(PAYLOAD, SIG_A, PUB_A, "Restreamer_99.0.0_amd64.deb").unwrap_err();
    assert!(renamed.contains("made for another file"), "{renamed}");
    // A name that only CONTAINS the signed one is not it.
    assert!(verify_signature(PAYLOAD, SIG_A, PUB_A, "Restreamer_0.29.5").is_err());
}

#[test]
fn the_compiled_key_is_the_one_tauri_conf_declares() {
    let conf: Value = serde_json::from_str(include_str!("../../../src-tauri/tauri.conf.json"))
        .expect("tauri.conf.json parses");
    assert_eq!(
        conf.pointer("/plugins/updater/pubkey")
            .and_then(Value::as_str),
        Some(UPDATER_PUBKEY)
    );
}

fn installed(status: &str, version: &str) -> Installed {
    parse_dpkg_query(&format!("{status} {version}")).unwrap()
}

#[test]
fn dpkg_query_gives_the_version_whether_it_is_configured_and_held() {
    assert_eq!(
        parse_dpkg_query("install installed 0.29.53"),
        Some(Installed {
            want: "install".into(),
            configured: true,
            version: v("0.29.53")
        })
    );
    assert_eq!(
        parse_dpkg_query("install unpacked 0.29.53"),
        Some(Installed {
            want: "install".into(),
            configured: false,
            version: v("0.29.53")
        })
    );
    assert_eq!(
        parse_dpkg_query("hold installed 0.29.53"),
        Some(Installed {
            want: "hold".into(),
            configured: true,
            version: v("0.29.53")
        })
    );
    let removing = parse_dpkg_query("deinstall config-files 0.29.53").unwrap();
    assert!(removing.left_alone() && !removing.configured);
    assert!(!installed("install installed", "0.29.53").left_alone());
    assert!(installed("purge config-files", "0.29.53").left_alone());
    assert_eq!(parse_dpkg_query(""), None);
    assert_eq!(parse_dpkg_query("install installed not-a-version"), None);
}

#[test]
fn only_a_configured_latest_or_newer_needs_no_install() {
    let latest = v("0.29.54");
    assert!(install_needed(None, &latest), "not installed");
    assert!(install_needed(
        Some(&installed("install installed", "0.29.53")),
        &latest
    ));
    assert!(
        install_needed(Some(&installed("install unpacked", "0.29.54")), &latest),
        "unpacked but never configured: retry"
    );
    assert!(!install_needed(
        Some(&installed("install installed", "0.29.54")),
        &latest
    ));
    assert!(!install_needed(
        Some(&installed("install installed", "0.29.60")),
        &latest
    ));
    assert!(
        !install_needed(Some(&installed("install unpacked", "0.29.60")), &latest),
        "a newer version is never downgraded"
    );
}

#[test]
fn a_deleted_exe_means_the_binary_was_replaced() {
    assert!(exe_replaced(Path::new("/usr/bin/Restreamer (deleted)")));
    assert!(!exe_replaced(Path::new("/usr/bin/Restreamer")));
}

#[test]
fn the_uid_comes_from_the_status_file() {
    let proc_status = "Name:\tRestreamer\nUmask:\t0002\nUid:\t1000\t1000\t1000\t1000\nGid:\t1000\n";
    assert_eq!(status_uid(proc_status), Some(1000));
    assert_eq!(status_uid("Name:\tx\n"), None);
}

#[test]
fn the_user_name_comes_from_passwd_by_uid() {
    let passwd = "root:x:0:0:root:/root:/bin/bash\n\
                  daemon:x:1:1000:daemon:/usr/sbin:/usr/sbin/nologin\n\
                  newlevel:x:1000:1000:New Level:/home/newlevel:/bin/bash\n";
    assert_eq!(passwd_user(passwd, 1000).as_deref(), Some("newlevel"));
    assert_eq!(passwd_user(passwd, 0).as_deref(), Some("root"));
    assert_eq!(passwd_user(passwd, 4242), None);
}

#[test]
fn the_gui_is_the_users_restreamer_not_a_root_cli_mode() {
    let root = tempfile::tempdir().unwrap();
    let process = |pid: &str, comm: &str, uid: u32, args: &[&str]| {
        let dir = root.path().join(pid);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("comm"), format!("{comm}\n")).unwrap();
        std::fs::write(
            dir.join("status"),
            format!("Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n"),
        )
        .unwrap();
        let mut cmdline = Vec::new();
        for arg in args {
            cmdline.extend_from_slice(arg.as_bytes());
            cmdline.push(0);
        }
        std::fs::write(dir.join("cmdline"), cmdline).unwrap();
    };
    process(
        "10",
        "Restreamer",
        0,
        &["/usr/bin/Restreamer", "--upgrade-check"],
    ); // itself
    process(
        "11",
        "Restreamer",
        1000,
        &["/usr/bin/Restreamer", "--verify-update", "a", "b"],
    );
    process(
        "12",
        "Restreamer",
        1000,
        &["/usr/bin/Restreamer", "--restart-app"],
    );
    process("13", "Restreamer", 0, &["/usr/bin/Restreamer"]); // root is never the GUI
    process("20", "obs", 1001, &["obs"]);
    std::fs::create_dir(root.path().join("self")).unwrap();
    assert_eq!(app_process(root.path(), 10), None, "no GUI runs");
    process("30", "Restreamer", 1000, &["/usr/bin/Restreamer"]);
    assert_eq!(
        app_process(root.path(), 10),
        Some(AppProcess { pid: 30, uid: 1000 })
    );
}

fn shell(script: &str) -> Command {
    if cfg!(windows) {
        let mut command = Command::new("cmd");
        command.args(["/C", script]);
        command
    } else {
        let mut command = Command::new("sh");
        command.args(["-c", script]);
        command
    }
}

#[test]
fn a_command_that_fails_is_an_error_with_its_stderr() {
    assert_eq!(run_command(&mut shell("exit 0")), Ok(()));
    let err = run_command(&mut shell(
        "echo REMOVED-other3 && echo apt-said-no 1>&2 && exit 3",
    ))
    .unwrap_err();
    assert!(err.contains("apt-said-no"), "{err}");
    assert!(err.contains("stdout: REMOVED-other3"), "{err}");
}

#[test]
fn a_failure_keeps_stderr_and_the_end_of_stdout() {
    let err = "E: Packages need to be removed but remove is disabled.";
    assert_eq!(failure_text("", err), err);
    assert_eq!(
        failure_text("\n  \n", err),
        err,
        "blank stdout adds nothing"
    );
    let stdout = (1..=30)
        .map(|n| format!("line {n}"))
        .chain([
            "The following packages will be REMOVED:".into(),
            "  other3".into(),
        ])
        .collect::<Vec<_>>()
        .join("\n");
    let text = failure_text(&stdout, err);
    assert!(text.starts_with(&format!("{err} | stdout: ")), "{text}");
    assert!(
        text.ends_with("The following packages will be REMOVED: /   other3"),
        "{text}"
    );
    assert!(!text.contains("line 12 "), "only the last 20 lines: {text}");
    assert!(
        text.contains("line 13 /"),
        "the last 20 lines are kept: {text}"
    );
}

#[test]
fn write_new_replaces_a_stale_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("Restreamer_0.29.54_amd64.deb");
    std::fs::write(&path, b"stale, longer than the new content").unwrap();
    write_new(&path, b"new").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
}

#[test]
fn write_new_stops_when_the_stale_entry_cannot_be_removed() {
    // A directory under the package's name: removing it as a file fails with
    // something other than NotFound, and nothing is written.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("Restreamer_0.29.54_amd64.deb");
    std::fs::create_dir(&path).unwrap();
    let err = write_new(&path, b"new").unwrap_err();
    assert!(err.contains("removing the stale"), "{err}");
}

// --- one whole check against a local server ------------------------------

/// dpkg, apt and systemctl in memory. A successful install makes the
/// package's version (from its file name) the configured installed one,
/// unless `install_unfinished`.
#[derive(Default)]
struct FakeHost {
    installed: Mutex<Option<Installed>>,
    app_running: bool,
    restart_pending: bool,
    fetch_error: Option<String>,
    install_error: Option<String>,
    install_unfinished: bool,
    fetches: Mutex<u32>,
    installs: Mutex<Vec<Vec<u8>>>,
    restarts: Mutex<u32>,
}

impl FakeHost {
    fn with(status: &str, version: &str) -> Self {
        Self {
            installed: Mutex::new(Some(installed(status, version))),
            ..Self::default()
        }
    }
}

impl Host for &FakeHost {
    fn installed(&self) -> Result<Option<Installed>, String> {
        Ok(self.installed.lock().unwrap().clone())
    }

    fn app_running(&self) -> bool {
        self.app_running
    }

    fn restart_pending(&self) -> bool {
        self.restart_pending
    }

    fn fetch_dependencies(&self, deb: &Path) -> Result<(), String> {
        assert!(deb.exists(), "the package is on disk before apt sees it");
        *self.fetches.lock().unwrap() += 1;
        match &self.fetch_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn install(&self, deb: &Path) -> Result<(), String> {
        let bytes = std::fs::read(deb).map_err(|e| e.to_string())?;
        self.installs.lock().unwrap().push(bytes);
        if let Some(error) = &self.install_error {
            return Err(error.clone());
        }
        let name = deb.file_name().unwrap().to_str().unwrap();
        let version = name
            .strip_prefix("Restreamer_")
            .and_then(|rest| rest.strip_suffix("_amd64.deb"))
            .unwrap();
        let state = if self.install_unfinished {
            "install unpacked"
        } else {
            "install installed"
        };
        *self.installed.lock().unwrap() = Some(installed(state, version));
        Ok(())
    }

    fn restart_app(&self) -> Result<String, String> {
        *self.restarts.lock().unwrap() += 1;
        Ok("restarted".into())
    }
}

/// A local GitHub + app. The release `latest` lists `<deb>` and `<deb>.sig`;
/// the `.deb` serves `deb` and must be fetched `deb_hits` times. The status
/// answers `statuses` in order, the last one from then on (none: 404).
async fn server(latest: &str, deb: &[u8], deb_hits: u64, statuses: &[Value]) -> MockServer {
    let server = MockServer::start().await;
    let name = deb_name(&v(latest));
    let release = serde_json::json!({
        "tag_name": format!("restreamer-v{latest}"),
        "assets": [
            {"name": name, "browser_download_url": format!("{}/dl/{name}", server.uri())},
            {"name": format!("{name}.sig"),
             "browser_download_url": format!("{}/dl/{name}.sig", server.uri())},
        ]
    });
    Mock::given(method("GET"))
        .and(path("/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(release))
        .mount(&server)
        .await;
    for (i, answer) in statuses.iter().enumerate() {
        let mock = Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer.clone()));
        let mock = if i + 1 < statuses.len() {
            mock.up_to_n_times(1)
        } else {
            mock
        };
        mock.mount(&server).await;
    }
    Mock::given(method("GET"))
        .and(path(format!("/dl/{name}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(deb.to_vec()))
        .expect(deb_hits)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{name}.sig")))
        .respond_with(ResponseTemplate::new(200).set_body_string(SIG_A))
        .mount(&server)
        .await;
    server
}

fn upgrader<'a>(server: &MockServer, dir: &Path, host: &'a FakeHost) -> Upgrader<&'a FakeHost> {
    Upgrader {
        http: reqwest::Client::new(),
        latest_url: format!("{}/latest", server.uri()),
        github_token: None,
        status_url: format!("{}/status", server.uri()),
        status_timeout: STATUS_TIMEOUT,
        pubkey: PUB_A.into(),
        download_dir: dir.to_path_buf(),
        host,
    }
}

fn upgraded(installed: bool) -> Result<Outcome, String> {
    Ok(Outcome::Upgraded {
        latest: v("0.29.54"),
        installed,
        restart: "restarted".into(),
    })
}

fn empty(dir: &Path) -> bool {
    std::fs::read_dir(dir).unwrap().next().is_none()
}

#[tokio::test]
async fn the_installed_latest_is_up_to_date_and_nothing_is_fetched() {
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    for host in [
        FakeHost::with("install installed", "0.29.54"),
        FakeHost::with("install installed", "0.29.60"),
    ] {
        let outcome = upgrader(&server, dir.path(), &host).run_once().await;
        assert_eq!(
            outcome,
            Ok(Outcome::UpToDate {
                latest: v("0.29.54")
            })
        );
        assert!(host.installs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_newer_release_waits_while_the_box_streams() {
    let server = server("0.29.54", PAYLOAD, 0, &[streaming()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::Busy {
            latest: v("0.29.54"),
            reason: "an RTMP publisher is connected".into()
        })
    );
    assert!(host.installs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_newer_signed_release_is_installed_and_the_app_restarted() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(outcome, upgraded(true));
    assert_eq!(*host.installs.lock().unwrap(), vec![PAYLOAD.to_vec()]);
    assert_eq!(*host.restarts.lock().unwrap(), 1);
    assert!(empty(dir.path()), "the downloaded package is removed");
}

#[tokio::test]
async fn an_unconfigured_install_of_the_latest_is_retried() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install unpacked", "0.29.54");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(outcome, upgraded(true));
    assert_eq!(host.installs.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_pending_restart_is_done_without_a_new_download() {
    let server = server("0.29.54", PAYLOAD, 0, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        restart_pending: true,
        ..FakeHost::with("install installed", "0.29.54")
    };
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(outcome, upgraded(false));
    assert!(host.installs.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 1);
}

#[tokio::test]
async fn a_pending_restart_waits_while_the_box_streams() {
    let server = server("0.29.54", PAYLOAD, 0, &[streaming()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        restart_pending: true,
        ..FakeHost::with("install installed", "0.29.54")
    };
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert!(matches!(outcome, Ok(Outcome::Busy { .. })), "{outcome:?}");
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_box_that_starts_streaming_during_the_download_is_not_touched() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle(), streaming()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert!(matches!(outcome, Ok(Outcome::Busy { .. })), "{outcome:?}");
    assert_eq!(
        *host.fetches.lock().unwrap(),
        1,
        "the dependencies come before the last idle check"
    );
    assert!(host.installs.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn the_restart_follows_the_install_at_once() {
    // An old binary serving the new package's files is worse than the
    // restart: no status read between the install and the restart.
    let server = server("0.29.54", PAYLOAD, 1, &[idle(), idle(), streaming()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(outcome, upgraded(true));
    assert_eq!(*host.restarts.lock().unwrap(), 1);
}

#[tokio::test]
async fn a_release_without_its_signature_leaves_an_up_to_date_box_alone() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": "restreamer-v0.29.54",
            "assets": [{"name": SIGNED_DEB, "browser_download_url": "https://example.invalid/x"}]
        })))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.54");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::UpToDate {
            latest: v("0.29.54")
        })
    );
}

#[tokio::test]
async fn a_held_package_is_never_touched() {
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("hold installed", "0.29.50");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::LeftAlone {
            latest: v("0.29.54"),
            installed: v("0.29.50"),
            want: "hold".into()
        })
    );
    assert!(host.installs.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_package_that_fails_its_signature_is_never_installed() {
    let server = server("0.29.54", TAMPERED, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("does not verify"), "{err}");
    assert!(host.installs.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn an_old_signed_package_renamed_as_a_newer_release_is_refused() {
    let server = server("0.29.99", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("made for another file"), "{err}");
    assert!(host.installs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failed_install_restarts_nothing_and_removes_the_download() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        install_error: Some("apt-get failed".into()),
        ..FakeHost::with("install installed", "0.29.53")
    };
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert_eq!(err, "apt-get failed");
    assert_eq!(*host.restarts.lock().unwrap(), 0);
    assert!(empty(dir.path()));
}

#[tokio::test]
async fn a_held_dpkg_lock_defers_the_install_to_the_next_check() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        install_error: Some(LOCK_HELD.into()),
        ..FakeHost::with("install installed", "0.29.53")
    };
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    match outcome {
        Ok(Outcome::Busy { latest, reason }) => {
            assert_eq!(latest, v("0.29.54"));
            assert!(reason.contains("dpkg lock"), "{reason}");
        }
        other => panic!("expected a deferral, got {other:?}"),
    }
    assert_eq!(*host.restarts.lock().unwrap(), 0);
    assert!(empty(dir.path()));
}

#[tokio::test]
async fn an_install_that_does_not_finish_is_not_restarted() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        install_unfinished: true,
        ..FakeHost::with("install installed", "0.29.53")
    };
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("not installed and configured"), "{err}");
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_package_being_removed_is_left_alone() {
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("deinstall config-files", "0.29.50");
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert!(
        matches!(&outcome, Ok(Outcome::LeftAlone { want, .. }) if want == "deinstall"),
        "{outcome:?}"
    );
    assert!(host.installs.lock().unwrap().is_empty());
}

/// What apt 3.2.0 writes to stderr after its lock wait (the `Could not get
/// lock` lines go to stdout as progress).
const LOCK_HELD: &str = "E: Unable to acquire the dpkg frontend lock \
                         (/var/lib/dpkg/lock-frontend), is another process using it?";

#[tokio::test]
async fn a_dependency_that_cannot_be_fetched_is_an_error() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        fetch_error: Some("E: Unable to locate package libfoo99".into()),
        ..FakeHost::with("install installed", "0.29.53")
    };
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert_eq!(err, "E: Unable to locate package libfoo99");
    assert!(host.installs.lock().unwrap().is_empty());
    assert!(empty(dir.path()));
}

#[test]
fn only_a_held_dpkg_lock_is_a_deferral() {
    assert!(dpkg_locked(LOCK_HELD));
    assert!(dpkg_locked(
        "E: Unable to lock the administration directory (/var/lib/dpkg/), is another \
         process using it?"
    ));
    // The archives lock, which apt never waits for (real apt 3.2.0 text).
    assert!(dpkg_locked(
        "E: Could not get lock /var/cache/apt/archives/lock. It is held by process 7 \
         (perl)\nE: Unable to lock directory /var/cache/apt/archives/"
    ));
    // A missing permission is a real error, never a reason to wait.
    assert!(!dpkg_locked(
        "E: Could not open lock file /var/lib/dpkg/lock-frontend - open (13: Permission \
         denied)\nE: Unable to acquire the dpkg frontend lock (/var/lib/dpkg/lock-frontend), \
         are you root?"
    ));
    assert!(!dpkg_locked(
        "E: Unable to correct problems, you have held broken packages."
    ));
}

#[tokio::test]
async fn a_held_dpkg_lock_while_fetching_dependencies_defers_too() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        fetch_error: Some(LOCK_HELD.into()),
        ..FakeHost::with("install installed", "0.29.53")
    };
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert!(
        matches!(&outcome, Ok(Outcome::Busy { reason, .. }) if reason.contains("dpkg lock")),
        "{outcome:?}"
    );
    assert_eq!(*host.fetches.lock().unwrap(), 1);
    assert!(host.installs.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 0);
    assert!(empty(dir.path()));
}

#[tokio::test]
async fn an_unreadable_or_failing_status_installs_nothing() {
    let unreadable = server("0.29.54", PAYLOAD, 0, &[serde_json::json!({"oops": 1})]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let err = upgrader(&unreadable, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("rtmp_connected"), "{err}");

    let failing = server("0.29.54", PAYLOAD, 0, &[]).await;
    Mock::given(method("GET"))
        .and(path("/status"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&failing)
        .await;
    let err = upgrader(&failing, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("500"), "{err}");
    assert!(host.installs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_status_that_does_not_answer_in_time_is_not_idle() {
    // Only "nothing listens" means the app is down; a hung app may stream.
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    Mock::given(method("GET"))
        .and(path("/status"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(idle())
                .set_delay(std::time::Duration::from_secs(5)),
        )
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let mut check = upgrader(&server, dir.path(), &host);
    check.status_timeout = std::time::Duration::from_millis(500);
    let err = check.run_once().await.unwrap_err();
    assert!(err.contains("no status from"), "{err}");
    assert!(host.installs.lock().unwrap().is_empty());
}

/// A port that refuses connections and stays reserved (bound, never
/// listening), so no parallel test can take it.
fn refusing_port() -> (tokio::net::TcpSocket, String) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("http://{}/status", socket.local_addr().unwrap());
    (socket, url)
}

#[tokio::test]
async fn an_app_that_is_down_counts_as_idle() {
    // A release can be the fix for a crash loop: a refused status port with
    // no GUI process means nothing streams.
    let (_reserved, refused_url) = refusing_port();
    let server = server("0.29.54", PAYLOAD, 1, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let mut check = upgrader(&server, dir.path(), &host);
    check.status_url = refused_url;
    assert_eq!(check.run_once().await, upgraded(true));
}

#[tokio::test]
async fn a_refused_status_while_the_gui_runs_is_not_idle() {
    // The GUI may listen on another port or address: never guess idle.
    let (_reserved, refused_url) = refusing_port();
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        app_running: true,
        ..FakeHost::with("install installed", "0.29.53")
    };
    let mut check = upgrader(&server, dir.path(), &host);
    check.status_url = refused_url;
    let err = check.run_once().await.unwrap_err();
    assert!(err.contains("refused while the GUI runs"), "{err}");
    assert!(host.installs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn any_other_status_failure_is_an_error_with_its_cause() {
    // A server that accepts and closes at once: neither refused nor a timeout.
    // It keeps accepting (a leaked thread), so a retry is never refused.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/status", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            drop(stream);
        }
    });
    let server = server("0.29.54", PAYLOAD, 0, &[]).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let mut check = upgrader(&server, dir.path(), &host);
    check.status_url = url.clone();
    let err = check.run_once().await.unwrap_err();
    // reqwest's own text leaves the cause out; the chain adds at least one
    // ": <cause>" (the URL itself has no ": ").
    let rest = err
        .strip_prefix(&format!("GET {url}: "))
        .unwrap_or_else(|| panic!("{err}"));
    assert!(rest.contains(": "), "no cause after reqwest's text: {err}");
    assert!(host.installs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_token_goes_to_the_release_api_only() {
    let server = server("0.29.54", PAYLOAD, 1, &[idle()]).await;
    // Anything but the release API that gets an Authorization header fails.
    Mock::given(header_exists("authorization"))
        .and(path_regex("^/(dl|status)"))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .mount(&server)
        .await;
    // And the release API answers only with the token.
    Mock::given(method("GET"))
        .and(path("/latest-with-token"))
        .and(header("authorization", "Bearer t0ken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tag_name": "restreamer-v0.29.54",
            "assets": [
                {"name": SIGNED_DEB,
                 "browser_download_url": format!("{}/dl/{SIGNED_DEB}", server.uri())},
                {"name": format!("{SIGNED_DEB}.sig"),
                 "browser_download_url": format!("{}/dl/{SIGNED_DEB}.sig", server.uri())},
            ]
        })))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::with("install installed", "0.29.53");
    let mut check = upgrader(&server, dir.path(), &host);
    check.latest_url = format!("{}/latest-with-token", server.uri());
    check.github_token = Some("t0ken".into());
    assert_eq!(check.run_once().await, upgraded(true));
}

// --- the packaging the timer depends on ----------------------------------

const LINUX_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src-tauri/linux");

fn linux_file(name: &str) -> String {
    std::fs::read_to_string(format!("{LINUX_DIR}/{name}"))
        .unwrap_or_else(|e| panic!("src-tauri/linux/{name}: {e}"))
}

#[test]
fn the_deb_ships_and_enables_the_upgrade_timer() {
    let service = linux_file("restreamer-upgrade.service");
    assert!(service.contains("ExecStart=/usr/bin/Restreamer --upgrade-check"));
    assert!(service.contains("CacheDirectory=restreamer-upgrade"));
    assert!(service.contains("TimeoutStartSec="));
    assert!(linux_file("restreamer-upgrade.timer").contains("OnUnitActiveSec="));
    assert!(linux_file("postinst").contains("systemctl enable --now restreamer-upgrade.timer"));
    assert!(linux_file("prerm").contains("systemctl disable --now restreamer-upgrade.timer"));

    let conf: Value =
        serde_json::from_str(include_str!("../../../src-tauri/tauri.linux.conf.json"))
            .expect("tauri.linux.conf.json parses");
    let deb = conf.pointer("/bundle/linux/deb").expect("deb section");
    for unit in ["restreamer-upgrade.service", "restreamer-upgrade.timer"] {
        assert_eq!(
            deb.pointer(&format!("/files/~1usr~1lib~1systemd~1system~1{unit}"))
                .and_then(Value::as_str),
            Some(format!("linux/{unit}").as_str()),
            "{unit} is installed as a system unit"
        );
    }
    assert_eq!(
        deb.get("postInstallScript").and_then(Value::as_str),
        Some("linux/postinst")
    );
    assert_eq!(
        deb.get("preRemoveScript").and_then(Value::as_str),
        Some("linux/prerm")
    );
}
