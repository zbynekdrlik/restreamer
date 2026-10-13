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
