//! One whole upgrade check against a local GitHub + app (#323). Child of
//! `self_upgrade_tests.rs` (`#[path]`, kept apart for the 1000-line cap): the
//! fixtures and helpers come from there.

use super::*;
use std::sync::Mutex;
use wiremock::matchers::{header, header_exists, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
