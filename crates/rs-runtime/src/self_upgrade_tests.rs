//! Tests for `self_upgrade.rs` (#323).

use super::*;
use std::sync::Mutex;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A payload signed with a throwaway key (`cargo tauri signer generate`, the
/// private half discarded); `PUB_A` is its public key, `PUB_B` another one.
const PAYLOAD: &[u8] = b"restreamer self-upgrade test payload (#323)\n";
const PUB_A: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDkzNUYzRDAxRjVBNEVFNDcKUldSSDdxVDFBVDFmazhPY1UwKzRkWXdzNEdPb0s0Y1lTTVFncGp6dHRZMGtXMGxUbEs5T3ZFQUwK";
const PUB_B: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDg4NUFGQTlBMzRGRjM3MUYKUldRZk4vODBtdnBhaUYySFNBWFhCV0NDNDQzbnJRTkV3UmZKZk1sRGViNmo4NFdzYVAyVjY0WGYK";
const SIG_A: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IHNpZ25hdHVyZSBmcm9tIHRhdXJpIHNlY3JldCBrZXkKUlVSSDdxVDFBVDFma3c5ODZucE03ckV2VjBjek1BQkZCWDRaeGJXK3d3ZU96WHQzYW1NdXpkbXVFb1EzWDRuK0RUMklLQWNxMDBlcG5qWmdvQkFCMitZdUZHQ2RyMnlPZUFRPQp0cnVzdGVkIGNvbW1lbnQ6IHRpbWVzdGFtcDoxNzkyNDU5OTU4CWZpbGU6cGF5bG9hZC5iaW4KQXlRdWdad3ZHYm5jaVlvZ3A1SzJycUdkMFZRREpHbWlDc3pUS1d4cXRCd0w4VndMVHZUN1ZOOVI1d0FqNVYwWExPZ1lvS0xuL1U2N2ZMektnSHZWQkE9PQo=";

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

#[test]
fn an_idle_box_has_no_publisher_and_no_live_event() {
    assert_eq!(busy_reason(&status(false, Value::Null)), Ok(None));
    let quiet_event = serde_json::json!({
        "name": "PP-live", "receiving_activated": false, "delivering_activated": false
    });
    assert_eq!(busy_reason(&status(false, quiet_event)), Ok(None));
}

#[test]
fn a_connected_publisher_or_a_live_event_is_busy() {
    assert_eq!(
        busy_reason(&status(true, Value::Null)),
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
fn a_good_signature_verifies() {
    assert_eq!(verify_signature(PAYLOAD, SIG_A, PUB_A), Ok(()));
}

#[test]
fn another_key_or_other_bytes_fail_the_signature() {
    let wrong_key = verify_signature(PAYLOAD, SIG_A, PUB_B).unwrap_err();
    assert!(wrong_key.contains("does not verify"), "{wrong_key}");
    let tampered = verify_signature(
        b"restreamer self-upgrade test payload (#323)!\n",
        SIG_A,
        PUB_A,
    )
    .unwrap_err();
    assert!(tampered.contains("does not verify"), "{tampered}");
    assert!(
        verify_signature(PAYLOAD, "not base64 !", PUB_A)
            .unwrap_err()
            .contains("not base64")
    );
    assert!(
        verify_signature(PAYLOAD, SIG_A, "AAAA")
            .unwrap_err()
            .contains("bad updater public key")
    );
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
fn the_app_owner_is_another_restreamer_process() {
    let root = tempfile::tempdir().unwrap();
    let process = |pid: &str, comm: &str, uid: u32| {
        let dir = root.path().join(pid);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("comm"), format!("{comm}\n")).unwrap();
        std::fs::write(
            dir.join("status"),
            format!("Uid:\t{uid}\t{uid}\t{uid}\t{uid}\n"),
        )
        .unwrap();
    };
    process("10", "Restreamer", 0); // the upgrade check itself
    process("20", "obs", 1001);
    std::fs::create_dir(root.path().join("self")).unwrap();
    assert_eq!(app_owner_uid(root.path(), 10), None, "only itself runs");
    process("30", "Restreamer", 1000);
    assert_eq!(app_owner_uid(root.path(), 10), Some(1000));
}

// --- one whole check against a local server ------------------------------

#[derive(Default)]
struct FakeHost {
    installed: Mutex<Vec<Vec<u8>>>,
    restarts: Mutex<u32>,
    fail_install: bool,
}

impl Host for &FakeHost {
    fn install(&self, deb: &Path) -> Result<(), String> {
        let bytes = std::fs::read(deb).map_err(|e| e.to_string())?;
        self.installed.lock().unwrap().push(bytes);
        if self.fail_install {
            return Err("dpkg failed".into());
        }
        Ok(())
    }

    fn restart_app(&self) -> Result<String, String> {
        *self.restarts.lock().unwrap() += 1;
        Ok("restarted".into())
    }
}

/// A local GitHub + app: release `latest` with the fixture `.deb` and `.sig`,
/// and `status` as the app's status. `deb_hits` = expected `.deb` downloads.
async fn server(latest: &str, status: Value, deb: &[u8], deb_hits: u64) -> MockServer {
    let server = MockServer::start().await;
    let deb_name = format!("Restreamer_{latest}_amd64.deb");
    let release = serde_json::json!({
        "tag_name": format!("restreamer-v{latest}"),
        "assets": [
            {"name": deb_name, "browser_download_url": format!("{}/dl/{deb_name}", server.uri())},
            {"name": format!("{deb_name}.sig"),
             "browser_download_url": format!("{}/dl/{deb_name}.sig", server.uri())},
        ]
    });
    Mock::given(method("GET"))
        .and(path("/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(release))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(status))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{deb_name}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(deb.to_vec()))
        .expect(deb_hits)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/dl/{deb_name}.sig")))
        .respond_with(ResponseTemplate::new(200).set_body_string(SIG_A))
        .mount(&server)
        .await;
    server
}

fn upgrader<'a>(server: &MockServer, dir: &Path, host: &'a FakeHost) -> Upgrader<&'a FakeHost> {
    Upgrader {
        http: reqwest::Client::new(),
        latest_url: format!("{}/latest", server.uri()),
        status_url: format!("{}/status", server.uri()),
        pubkey: PUB_A.into(),
        current: v("0.29.53"),
        download_dir: dir.to_path_buf(),
        host,
    }
}

#[tokio::test]
async fn the_same_version_is_up_to_date_and_nothing_is_downloaded() {
    let server = server("0.29.53", status(false, Value::Null), PAYLOAD, 0).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::UpToDate {
            latest: v("0.29.53")
        })
    );
    assert!(host.installed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_older_release_is_never_installed() {
    let server = server("0.29.52", status(false, Value::Null), PAYLOAD, 0).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::UpToDate {
            latest: v("0.29.52")
        })
    );
}

#[tokio::test]
async fn a_newer_release_waits_while_the_box_streams() {
    let server = server("0.29.54", status(true, Value::Null), PAYLOAD, 0).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::Busy {
            latest: v("0.29.54"),
            reason: "an RTMP publisher is connected".into()
        })
    );
    assert!(host.installed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_newer_signed_release_is_installed_and_the_app_restarted() {
    let server = server("0.29.54", status(false, Value::Null), PAYLOAD, 1).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let outcome = upgrader(&server, dir.path(), &host).run_once().await;
    assert_eq!(
        outcome,
        Ok(Outcome::Installed {
            from: v("0.29.53"),
            to: v("0.29.54"),
            restart: "restarted".into()
        })
    );
    assert_eq!(*host.installed.lock().unwrap(), vec![PAYLOAD.to_vec()]);
    assert_eq!(*host.restarts.lock().unwrap(), 1);
    assert!(
        std::fs::read_dir(dir.path()).unwrap().next().is_none(),
        "the downloaded package is removed"
    );
}

#[tokio::test]
async fn a_package_that_fails_its_signature_is_never_installed() {
    let tampered = b"restreamer self-upgrade test payload (#323)!\n";
    let server = server("0.29.54", status(false, Value::Null), tampered, 1).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("does not verify"), "{err}");
    assert!(host.installed.lock().unwrap().is_empty());
    assert_eq!(*host.restarts.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_failed_install_restarts_nothing_and_removes_the_download() {
    let server = server("0.29.54", status(false, Value::Null), PAYLOAD, 1).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost {
        fail_install: true,
        ..FakeHost::default()
    };
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert_eq!(err, "dpkg failed");
    assert_eq!(*host.restarts.lock().unwrap(), 0);
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn an_unreadable_status_installs_nothing() {
    let server = server("0.29.54", serde_json::json!({"oops": 1}), PAYLOAD, 0).await;
    let dir = tempfile::tempdir().unwrap();
    let host = FakeHost::default();
    let err = upgrader(&server, dir.path(), &host)
        .run_once()
        .await
        .unwrap_err();
    assert!(err.contains("rtmp_connected"), "{err}");
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
    assert!(linux_file("restreamer-upgrade.timer").contains("OnUnitActiveSec="));
    assert!(linux_file("postinst").contains("systemctl enable --now restreamer-upgrade.timer"));

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
