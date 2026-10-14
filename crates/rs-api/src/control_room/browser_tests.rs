use super::*;

#[test]
fn env_override_is_tried_first_and_empty_is_ignored() {
    let c = chrome_candidates(false, Some("/opt/my/chrome".into()));
    assert_eq!(c[0], PathBuf::from("/opt/my/chrome"));
    assert_eq!(c[1], PathBuf::from("/usr/bin/google-chrome-stable"));
    let c = chrome_candidates(false, Some(String::new()));
    assert_eq!(c[0], PathBuf::from("/usr/bin/google-chrome-stable"));
}

#[test]
fn windows_candidates_prefer_chrome_then_edge() {
    let c = chrome_candidates(true, None);
    assert!(
        c[0].to_string_lossy()
            .ends_with(r"Google\Chrome\Application\chrome.exe")
    );
    assert!(
        c.iter()
            .any(|p| p.to_string_lossy().ends_with("msedge.exe"))
    );
    assert!(!c.iter().any(|p| p.to_string_lossy().starts_with('/')));
}

#[test]
fn launch_args_pin_profile_dir_port_and_hidden_window() {
    let ud = Path::new("/data/browser-profiles/control-rooms");
    let win = launch_args(ud, 9231, true);
    assert!(win.contains(&"--user-data-dir=/data/browser-profiles/control-rooms".to_string()));
    assert!(win.contains(&"--remote-debugging-port=9231".to_string()));
    assert!(win.contains(&"--remote-debugging-address=127.0.0.1".to_string()));
    assert!(win.contains(&"--window-position=-32000,-32000".to_string()));
    assert!(win.contains(&"--disable-backgrounding-occluded-windows".to_string()));
    assert!(!win.contains(&"--password-store=basic".to_string()));
    assert_eq!(win.last().unwrap(), "about:blank");

    let lin = launch_args(ud, 9231, false);
    assert!(lin.contains(&"--password-store=basic".to_string()));
    assert!(
        !lin.iter()
            .any(|a| a.starts_with("--window-position=-32000"))
    );
}

#[test]
fn open_args_select_the_profile_and_end_with_the_url() {
    let ud = Path::new("/d/control-rooms");
    let a = open_args(ud, "acct-x-gmail-com", "https://studio.youtube.com/", false);
    assert_eq!(a[0], "--user-data-dir=/d/control-rooms");
    assert_eq!(a[1], "--profile-directory=acct-x-gmail-com");
    assert!(a.contains(&"--password-store=basic".to_string()));
    assert_eq!(a.last().unwrap(), "https://studio.youtube.com/");
    let w = open_args(
        ud,
        "dest-3",
        "https://www.facebook.com/live/producer/",
        true,
    );
    assert!(!w.contains(&"--password-store=basic".to_string()));
}

#[test]
fn browser_keeps_profiles_under_the_data_dir() {
    let b = Browser::new(Path::new("/data"));
    assert_eq!(
        b.user_data,
        Path::new("/data")
            .join("browser-profiles")
            .join("control-rooms")
    );
    assert_eq!(b.cdp.addr, format!("127.0.0.1:{CDP_PORT}"));
}

#[test]
fn handover_succeeds_only_on_exit_code_zero() {
    assert!(handover_result(Some(0), "https://a/").is_ok());
    let e = handover_result(Some(3), "https://a/")
        .unwrap_err()
        .to_string();
    assert!(e.contains("code 3") && e.contains("https://a/"), "{e}");
    let e = handover_result(None, "https://a/").unwrap_err().to_string();
    assert!(e.contains("within 15 s"), "{e}");
}

#[test]
fn windows_command_line_quotes_like_command_line_to_argv() {
    let args: Vec<String> = [
        "--user-data-dir=C:\\ProgramData\\Restreamer\\browser-profiles\\control-rooms",
        "",
        "a b",
        "say \"hi\"",
        "C:\\dir with space\\",
        "back\\\\\"slash",
        "tab\there",
        "https://studio.youtube.com/?a=1&b=2",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        windows_command_line(
            "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            &args
        ),
        concat!(
            "\"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe\" ",
            "--user-data-dir=C:\\ProgramData\\Restreamer\\browser-profiles\\control-rooms ",
            "\"\" ",
            "\"a b\" ",
            "\"say \\\"hi\\\"\" ",
            "\"C:\\dir with space\\\\\" ",
            "\"back\\\\\\\\\\\"slash\" ",
            "\"tab\there\" ",
            "https://studio.youtube.com/?a=1&b=2"
        )
    );
    assert_eq!(windows_command_line("chrome.exe", &[]), "chrome.exe");
}

#[test]
fn port_of_reads_the_debugging_port() {
    assert_eq!(port_of("127.0.0.1:9239"), 9239);
    assert_eq!(port_of("127.0.0.1"), CDP_PORT);
    assert_eq!(port_of("127.0.0.1:x"), CDP_PORT);
    assert_eq!(Browser::new(Path::new("/d")).port(), CDP_PORT);
}

#[test]
fn xvfb_lock_follows_the_display() {
    assert_eq!(xvfb_lock(":93"), PathBuf::from("/tmp/.X93-lock"));
    assert_eq!(xvfb_lock(":4711"), PathBuf::from("/tmp/.X4711-lock"));
}

/// `RESTREAMER_CHROME` is process-global: only this test sets it.
#[test]
fn locate_chrome_takes_the_env_override_when_it_exists() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("my-chrome");
    std::fs::write(&bin, b"").unwrap();
    // SAFETY: edition 2024 marks set_var unsafe; no other test reads or
    // writes RESTREAMER_CHROME.
    unsafe { std::env::set_var(CHROME_ENV, &bin) };
    let found = locate_chrome();
    unsafe { std::env::remove_var(CHROME_ENV) };
    assert_eq!(found, Some(bin));
}

#[test]
fn scope_runner_only_under_a_systemd_service_with_systemd_run() {
    let dir = tempfile::tempdir().unwrap();
    let sr = dir.path().join("systemd-run");
    std::fs::write(&sr, b"").unwrap();
    assert_eq!(scope_runner(true, &sr), Some(sr.clone()));
    assert_eq!(scope_runner(false, &sr), None, "not a systemd service");
    assert_eq!(
        scope_runner(true, &dir.path().join("missing")),
        None,
        "no systemd-run on the box"
    );
}

#[test]
fn scoped_command_wraps_in_a_collected_user_scope() {
    let args = vec!["--a=1".to_string(), "x y".to_string()];
    let (prog, out) = scoped_command(
        Some(Path::new("/usr/bin/systemd-run")),
        "Chrome",
        Path::new("/opt/chrome"),
        &args,
    );
    assert_eq!(prog, PathBuf::from("/usr/bin/systemd-run"));
    assert_eq!(
        out,
        [
            "--user",
            "--scope",
            "--collect",
            "--quiet",
            "--description=Restreamer control rooms: Chrome",
            "--",
            "/opt/chrome",
            "--a=1",
            "x y",
        ]
    );
    let (prog, out) = scoped_command(None, "Chrome", Path::new("/opt/chrome"), &args);
    assert_eq!(prog, PathBuf::from("/opt/chrome"));
    assert_eq!(out, args);
}

#[cfg(unix)]
mod spawn {
    use super::*;
    use crate::control_room::fake_chrome::{FakeChrome, fake_bin};

    fn free_addr() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    }

    /// A display number nobody else uses (the lock file lives in /tmp).
    fn test_display() -> String {
        format!(":{}", 40_000 + std::process::id() % 20_000)
    }

    #[tokio::test]
    async fn ensure_running_starts_xvfb_then_chrome_on_the_hidden_display() {
        let dir = tempfile::tempdir().unwrap();
        let display = test_display();
        let lock = xvfb_lock(&display);
        let _ = std::fs::remove_file(&lock);
        let addr = free_addr();
        let mut b = Browser::at(dir.path(), addr.clone());
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, None));
        b.xvfb_bin = fake_bin(dir.path(), "xvfb", 0, Some(&lock));
        b.display = display.clone();

        // Chrome's debugging port comes up once the fake chrome has run.
        let args = dir.path().join("chrome.args");
        let up = tokio::spawn(async move {
            while !args.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let l = tokio::net::TcpListener::bind(&addr).await.unwrap();
            FakeChrome::start_on(l).await
        });
        tokio::time::timeout(Duration::from_secs(20), b.ensure_running())
            .await
            .expect("ensure_running hung")
            .unwrap();
        // Bounded: if Chrome was never started, the helper would wait forever.
        let _chrome = tokio::time::timeout(Duration::from_secs(5), up)
            .await
            .expect("ensure_running returned without ever starting Chrome")
            .unwrap();
        let _ = std::fs::remove_file(&lock);

        let xargs = std::fs::read_to_string(dir.path().join("xvfb.args")).unwrap();
        assert!(
            xargs.starts_with(&format!("{display} -screen 0 1600x900x24")),
            "{xargs}"
        );
        let cargs = std::fs::read_to_string(dir.path().join("chrome.args")).unwrap();
        assert!(
            cargs.contains(&format!("--remote-debugging-port={}", b.port())),
            "{cargs}"
        );
        assert!(
            cargs.contains(&format!("--user-data-dir={}", b.user_data.display())),
            "{cargs}"
        );
        assert!(b.user_data.is_dir());
        let disp = std::fs::read_to_string(dir.path().join("chrome.display")).unwrap();
        assert_eq!(disp, display, "Chrome must draw on the hidden display");
    }

    #[tokio::test]
    async fn ensure_running_reuses_a_running_chrome() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        let mut b = Browser::at(dir.path(), chrome.addr.clone());
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, None));
        b.ensure_running().await.unwrap();
        assert!(!dir.path().join("chrome.args").exists(), "no second Chrome");
    }

    #[tokio::test]
    async fn open_hands_profile_and_url_over_and_reports_a_failed_handover() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = Browser::at(dir.path(), free_addr());
        b.display = ":4242".into();
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, None));
        b.open("dest-7", "https://studio.youtube.com/")
            .await
            .unwrap();
        let args = std::fs::read_to_string(dir.path().join("chrome.args")).unwrap();
        assert!(args.contains("--profile-directory=dest-7"), "{args}");
        assert!(
            args.trim_end().ends_with("https://studio.youtube.com/"),
            "{args}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("chrome.display")).unwrap(),
            ":4242"
        );

        b.chrome_bin = Some(fake_bin(dir.path(), "bad-chrome", 3, None));
        let err = b.open("dest-7", "https://x/").await.unwrap_err();
        assert!(err.to_string().contains("exited"), "{err}");
    }

    /// A `systemd-run` that logs its arguments and runs the command after `--`.
    fn fake_systemd_run(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join("systemd-run");
        std::fs::write(
            &p,
            format!(
                "#!/bin/sh\necho \"$*\" >> '{}'\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
                dir.join("systemd-run.args").display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p
    }

    /// stream-pp: `systemctl --user restart restreamer` killed the
    /// control-room Chrome and Xvfb with the service's cgroup (#388).
    #[tokio::test]
    async fn under_systemd_chrome_and_xvfb_start_in_their_own_scope() {
        let dir = tempfile::tempdir().unwrap();
        let display = format!(":{}", 60_000 + std::process::id() % 5_000);
        let lock = xvfb_lock(&display);
        let _ = std::fs::remove_file(&lock);
        let addr = free_addr();
        let mut b = Browser::at(dir.path(), addr.clone());
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, None));
        b.xvfb_bin = fake_bin(dir.path(), "xvfb", 0, Some(&lock));
        b.display = display.clone();
        b.systemd_run = Some(fake_systemd_run(dir.path()));

        let args = dir.path().join("chrome.args");
        let up = tokio::spawn(async move {
            while !args.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let l = tokio::net::TcpListener::bind(&addr).await.unwrap();
            FakeChrome::start_on(l).await
        });
        tokio::time::timeout(Duration::from_secs(20), b.ensure_running())
            .await
            .expect("ensure_running hung")
            .unwrap();
        let _chrome = tokio::time::timeout(Duration::from_secs(5), up)
            .await
            .expect("Chrome never started")
            .unwrap();
        let _ = std::fs::remove_file(&lock);
        // The handover run exits at once: it needs no scope.
        b.open("dest-7", "https://studio.youtube.com/")
            .await
            .unwrap();

        let runs = std::fs::read_to_string(dir.path().join("systemd-run.args"))
            .expect("nothing went through systemd-run");
        let runs: Vec<&str> = runs.lines().collect();
        assert_eq!(runs.len(), 2, "{runs:?}");
        let prefix = "--user --scope --collect --quiet --description=Restreamer control rooms:";
        assert!(
            runs[0].starts_with(&format!("{prefix} Xvfb -- {}", b.xvfb_bin.display())),
            "{runs:?}"
        );
        assert!(
            runs[1].starts_with(&format!(
                "{prefix} Chrome -- {}",
                b.chrome_bin.as_ref().unwrap().display()
            )),
            "{runs:?}"
        );
        // Through the scope, Chrome still draws on the hidden display.
        let disp = std::fs::read_to_string(dir.path().join("chrome.display")).unwrap();
        assert_eq!(disp, display);
        let xargs = std::fs::read_to_string(dir.path().join("xvfb.args")).unwrap();
        assert!(
            xargs.starts_with(&format!("{display} -screen 0")),
            "{xargs}"
        );
    }

    /// A `systemd-run` that cannot reach the user manager: it says why on
    /// stderr and exits 1 without running anything.
    fn failing_systemd_run(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join("systemd-run");
        std::fs::write(
            &p,
            format!(
                "#!/bin/sh\necho \"$*\" >> '{}'\necho 'Failed to connect to bus: No medium found' >&2\nexit 1\n",
                dir.join("systemd-run.args").display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p
    }

    /// Review of PR #397: a failing systemd-run was silent (stderr nulled),
    /// and Chrome / Xvfb then never started at all.
    #[tokio::test]
    async fn a_failing_systemd_run_falls_back_to_a_direct_start_and_says_why() {
        let dir = tempfile::tempdir().unwrap();
        let display = format!(":{}", 65_000 + std::process::id() % 400);
        let lock = xvfb_lock(&display);
        let _ = std::fs::remove_file(&lock);
        let addr = free_addr();
        let mut b = Browser::at(dir.path(), addr.clone());
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, None));
        b.xvfb_bin = fake_bin(dir.path(), "xvfb", 0, Some(&lock));
        b.display = display.clone();
        b.systemd_run = Some(failing_systemd_run(dir.path()));

        let args = dir.path().join("chrome.args");
        let up = tokio::spawn(async move {
            while !args.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let l = tokio::net::TcpListener::bind(&addr).await.unwrap();
            FakeChrome::start_on(l).await
        });
        let started = tokio::time::timeout(Duration::from_secs(30), b.ensure_running())
            .await
            .expect("ensure_running hung");
        let _ = std::fs::remove_file(&lock);
        started.unwrap();
        let _chrome = tokio::time::timeout(Duration::from_secs(5), up)
            .await
            .expect("Chrome never started")
            .unwrap();

        // Both were tried through systemd-run first, then started directly.
        let runs = std::fs::read_to_string(dir.path().join("systemd-run.args")).unwrap();
        assert_eq!(runs.lines().count(), 2, "{runs}");
        let xargs = std::fs::read_to_string(dir.path().join("xvfb.args")).unwrap();
        assert!(
            xargs.starts_with(&format!("{display} -screen 0")),
            "{xargs}"
        );
        let disp = std::fs::read_to_string(dir.path().join("chrome.display")).unwrap();
        assert_eq!(disp, display);
        // The reason is kept, not thrown away.
        for what in ["Chrome", "Xvfb"] {
            let log = std::fs::read_to_string(stderr_log(&b.user_data, what))
                .unwrap_or_else(|e| panic!("no {what} stderr log: {e}"));
            assert!(log.contains("Failed to connect to bus"), "{what}: {log}");
        }
    }
}
