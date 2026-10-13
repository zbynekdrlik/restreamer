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
        let _chrome = up.await.unwrap();
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
}
