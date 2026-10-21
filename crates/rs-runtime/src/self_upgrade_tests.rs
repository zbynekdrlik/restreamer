//! Tests for `self_upgrade.rs` (#323).

use super::*;

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
fn a_lock_apt_only_waited_for_on_stdout_is_not_a_deferral() {
    // apt waited for the lock (stdout progress), got it, and then failed for
    // a real reason: that stays an error.
    let waited_then_failed = failure_text(
        "Waiting for cache lock: Could not get lock /var/lib/dpkg/lock-frontend. It is \
         held by process 9 (apt)...\nReading package lists...",
        "E: Unable to locate package libfoo99",
    );
    assert!(!dpkg_locked(&waited_then_failed), "{waited_then_failed}");
    // A lock on stderr is a deferral, with or without a stdout tail.
    let held = failure_text(
        "Reading package lists...",
        "E: Unable to acquire the dpkg frontend lock (/var/lib/dpkg/lock-frontend), is \
         another process using it?",
    );
    assert!(dpkg_locked(&held), "{held}");
}

#[test]
fn apt_installs_the_deb_repairing_without_removing_in_english() {
    let deb = Path::new("/var/cache/restreamer-upgrade/Restreamer_0.29.54_amd64.deb");
    let args = |command: &Command| -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    };
    let install = apt_get_install(deb, false);
    assert_eq!(install.get_program(), "apt-get");
    assert_eq!(
        args(&install),
        [
            "install",
            "-y",
            "-f",
            "--no-remove",
            "-o",
            "DPkg::Lock::Timeout=10",
            "/var/cache/restreamer-upgrade/Restreamer_0.29.54_amd64.deb"
        ]
    );
    let envs: Vec<(String, String)> = install
        .get_envs()
        .filter_map(|(key, value)| {
            Some((
                key.to_string_lossy().into_owned(),
                value?.to_string_lossy().into_owned(),
            ))
        })
        .collect();
    assert!(envs.contains(&("LC_ALL".into(), "C".into())), "{envs:?}");
    assert!(
        envs.contains(&("DEBIAN_FRONTEND".into(), "noninteractive".into())),
        "{envs:?}"
    );
    let fetch = args(&apt_get_install(deb, true));
    assert!(fetch.contains(&"--download-only".to_owned()), "{fetch:?}");
    assert_eq!(
        fetch.last().map(String::as_str),
        Some(deb.to_str().unwrap())
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

/// The journal line of a restart onto a package that was already installed
/// (a hand `dpkg -i` replaced the running binary) named the latest RELEASE
/// as an upgrade: "upgraded to 0.29.52 (already installed)" on stream-pp,
/// whose installed package was 0.29.54 (2026-10-21).
#[test]
fn a_restart_onto_the_installed_package_claims_no_upgrade() {
    let line = Outcome::Upgraded {
        latest: Version::new(0, 29, 52),
        installed: false,
        restart: "restarted the restreamer unit of newlevel".into(),
    }
    .summary();
    assert!(!line.contains("upgraded to"), "{line}");
    assert!(line.contains("the installed package"), "{line}");
    assert!(line.contains("latest release 0.29.52"), "{line}");
    assert!(
        line.ends_with("restarted the restreamer unit of newlevel"),
        "{line}"
    );
}

#[test]
fn every_outcome_line_names_what_happened() {
    let installed = Outcome::Upgraded {
        latest: Version::new(0, 29, 55),
        installed: true,
        restart: "restarted X".into(),
    };
    assert_eq!(installed.summary(), "installed 0.29.55; restarted X");
    let up = Outcome::UpToDate {
        latest: Version::new(0, 29, 55),
    };
    assert!(up.summary().starts_with("up to date"), "{}", up.summary());
    assert!(up.summary().contains("0.29.55"));
    let busy = Outcome::Busy {
        latest: Version::new(0, 29, 55),
        reason: "an event is delivering".into(),
    };
    assert_eq!(
        busy.summary(),
        "0.29.55 is waiting for an idle box: an event is delivering"
    );
    let held = Outcome::LeftAlone {
        latest: Version::new(0, 29, 55),
        installed: Version::new(0, 29, 50),
        want: "hold".into(),
    };
    let line = held.summary();
    assert!(
        line.contains("0.29.50") && line.contains("\"hold\"") && line.contains("0.29.55"),
        "{line}"
    );
}

#[path = "self_upgrade_check_tests.rs"]
mod check;
