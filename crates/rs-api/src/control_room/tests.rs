use super::*;

fn ep(id: i64, alias: &str, service_type: &str) -> EndpointConfig {
    serde_json::from_value(json!({
        "id": id, "alias": alias, "service_type": service_type, "stream_key": "secret-key",
        "enabled": true, "position_last": 0, "delivered_bytes": 0, "is_fast": false,
        "created_at": "", "updated_at": ""
    }))
    .unwrap()
}

fn page(id: &str, ctx: &str) -> TargetInfo {
    TargetInfo {
        target_id: id.into(),
        kind: "page".into(),
        url: "https://x/".into(),
        title: String::new(),
        browser_context_id: Some(ctx.into()),
        opener_id: None,
        can_access_opener: false,
    }
}

fn accounts(entries: &[(i64, &str, Option<&str>)]) -> AccountsFile {
    let mut f = AccountsFile::default();
    for (id, acct, ch) in entries {
        f.endpoints.insert(
            id.to_string(),
            Account {
                account: Some((*acct).into()),
                channel: ch.map(str::to_string),
                channel_name: None,
                sure: true,
            },
        );
    }
    f
}

#[test]
fn every_destination_has_its_own_profile() {
    // YouTube: one Google profile holds one channel identity at a time.
    // Facebook: a shared account profile made one room's sign-in change every
    // FB room (stream-pp, 2026-10-14).
    let eps = vec![
        ep(4, "NLCH", "YT_RTMP"),
        ep(5, "FB-A", "FB"),
        ep(6, "FB-B", "FB"),
        ep(7, "FB-?", "FB"),
    ];
    let f = accounts(&[
        (4, "Milost.SNV@gmail.com", None),
        (5, "Milost.SNV@gmail.com", None),
        (6, "milost.snv@gmail.com", None),
    ]);
    let profiles: Vec<String> = destinations(&eps, &f)
        .into_iter()
        .map(|d| d.profile)
        .collect();
    assert_eq!(profiles, vec!["dest-4", "dest-5", "dest-6", "dest-7"]);
    assert_eq!(account_profile(" A.B@x.com "), "acct-a-b-x-com");
}

#[test]
fn clone_sources_are_same_account_same_service_profiles() {
    let eps = vec![
        ep(1, "NLCH", "YT_RTMP"),
        ep(2, "NLW", "YT_RTMP"),
        ep(3, "Kiko", "YT_RTMP"),
        ep(4, "FB-GoodFest", "FB"),
        ep(5, "FB-NLCH", "FB"),
    ];
    let f = accounts(&[
        (1, "a@x.com", None),
        (2, "A@X.com", None),
        (3, "k@x.com", None),
        (4, "a@x.com", None),
        (5, "a@x.com", None),
    ]);
    let d = destinations(&eps, &f);
    assert_eq!(
        clone_sources(&d[0], &eps, &f),
        vec!["acct-a-x-com", "dest-2"],
        "a YouTube room never clones a Facebook room"
    );
    assert_eq!(clone_sources(&d[2], &eps, &f), vec!["acct-k-x-com"]);
    assert_eq!(
        clone_sources(&d[3], &eps, &f),
        vec!["acct-a-x-com", "dest-5"],
        "a new FB room starts signed in like the account's other FB rooms"
    );
    let unknown = destinations(&[ep(9, "YT-?", "YT_RTMP")], &AccountsFile::default());
    assert!(clone_sources(&unknown[0], &eps, &f).is_empty());
}

#[test]
fn remember_url_keeps_only_room_pages() {
    for yes in [
        "https://studio.youtube.com/channel/UC1/livestreaming",
        "https://studio.youtube.com/video/abc/livestreaming",
        "https://www.facebook.com/live/producer/v2/?target_id=100050795320744",
        "https://www.facebook.com/live/producer/",
    ] {
        assert!(remember_url(yes), "{yes}");
    }
    for no in [
        "https://accounts.google.com/v3/signin/identifier",
        "https://www.youtube.com/channel_switcher",
        // #405: the hand-back page of a verification popup, never a room page.
        "https://studio.youtube.com/reauth?rapt=x",
        "https://www.facebook.com/login/",
        "https://www.facebook.com/checkpoint/",
        "http://studio.youtube.com/",
        "https://studio.youtube.com.evil.example/",
        "about:blank",
        "",
    ] {
        assert!(!remember_url(no), "{no}");
    }
}

#[test]
fn rooms_to_close_keeps_the_most_recently_viewed() {
    // `recent` is most recent first; a live room never viewed counts as oldest.
    assert_eq!(rooms_to_close(&[3, 1, 2], &[1, 2, 3, 4, 5], 4), vec![5]);
    assert_eq!(
        rooms_to_close(&[5, 4, 3, 2, 1], &[1, 2, 3, 4, 5], 4),
        vec![1]
    );
    assert!(rooms_to_close(&[1], &[1, 2, 3, 4], 4).is_empty());
    assert!(rooms_to_close(&[], &[], 4).is_empty());
    assert_eq!(rooms_to_close(&[2], &[1, 2, 3], 1), vec![1, 3]);
    assert_eq!(MAX_LIVE_ROOMS, 4);
}

#[tokio::test]
async fn a_room_reopens_at_its_last_page_until_its_account_changes() {
    let dir = tempfile::tempdir().unwrap();
    let map = TabMap {
        tab: [("T1".to_string(), 1)].into(),
        ..Default::default()
    };
    std::fs::write(
        dir.path().join("control-room-tabs.json"),
        serde_json::to_vec(&map).unwrap(),
    )
    .unwrap();
    // No Chrome on this port: everything below works from the files.
    let rooms =
        ControlRooms::with_browser(dir.path(), Browser::at(dir.path(), "127.0.0.1:9".into()));
    let page = "https://studio.youtube.com/channel/UC9/livestreaming";
    rooms.note_url("T1", "https://accounts.google.com/signin");
    rooms.note_url("T2", page);
    let d = rooms.status(&[ep(1, "YT A", "YT_RTMP")]).await;
    assert_eq!(
        d[0].url, "https://studio.youtube.com/",
        "nothing worth keeping yet"
    );
    rooms.note_url("T1", page);
    let d = rooms.status(&[ep(1, "YT A", "YT_RTMP")]).await;
    assert_eq!(d[0].url, page);
    // It survives a Restreamer restart.
    let again =
        ControlRooms::with_browser(dir.path(), Browser::at(dir.path(), "127.0.0.1:9".into()));
    assert_eq!(again.status(&[ep(1, "YT A", "YT_RTMP")]).await[0].url, page);
    // A new account (maybe a new channel) starts from the default again.
    again
        .set_account(
            1,
            Account {
                account: Some("a@x.com".into()),
                channel: Some("UC1".into()),
                sure: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        again.status(&[ep(1, "YT A", "YT_RTMP")]).await[0].url,
        "https://studio.youtube.com/channel/UC1/livestreaming"
    );
}

#[test]
fn clone_profile_copies_a_signed_in_profile_once_without_caches() {
    let dir = tempfile::tempdir().unwrap();
    let ud = dir.path();
    std::fs::create_dir_all(ud.join("acct-a/Network")).unwrap();
    std::fs::write(ud.join("acct-a/Network/Cookies"), b"cookie-db").unwrap();
    std::fs::write(ud.join("acct-a/Preferences"), b"{}").unwrap();
    std::fs::create_dir_all(ud.join("acct-a/Cache/Cache_Data")).unwrap();
    std::fs::write(ud.join("acct-a/Cache/Cache_Data/big"), b"x").unwrap();
    std::fs::write(ud.join("acct-a/LOCK"), b"").unwrap();

    let src = clone_profile(ud, "dest-1", &["acct-missing".into(), "acct-a".into()]).unwrap();
    assert_eq!(src.as_deref(), Some("acct-a"));
    assert_eq!(
        std::fs::read(ud.join("dest-1/Network/Cookies")).unwrap(),
        b"cookie-db"
    );
    assert!(ud.join("dest-1/Preferences").is_file());
    assert!(!ud.join("dest-1/Cache").exists(), "caches are not copied");
    assert!(!ud.join("dest-1/LOCK").exists(), "locks are not copied");

    // An existing profile is never overwritten.
    std::fs::write(ud.join("dest-1/Preferences"), b"mine").unwrap();
    assert_eq!(
        clone_profile(ud, "dest-1", &["acct-a".into()]).unwrap(),
        None
    );
    assert_eq!(
        std::fs::read(ud.join("dest-1/Preferences")).unwrap(),
        b"mine"
    );
    // No source on disk: nothing happens.
    assert_eq!(
        clone_profile(ud, "dest-2", &["acct-none".into()]).unwrap(),
        None
    );
    assert!(!ud.join("dest-2").exists());
}

#[test]
fn room_url_per_service() {
    assert_eq!(
        room_url("YT_RTMP", Some("UCabc")).unwrap(),
        "https://studio.youtube.com/channel/UCabc/livestreaming"
    );
    assert_eq!(
        room_url("YT_RTMP", Some(" ")).unwrap(),
        "https://studio.youtube.com/"
    );
    assert_eq!(
        room_url("FB", None).unwrap(),
        "https://www.facebook.com/live/producer/"
    );
    assert!(room_url("TEST_FILE", None).is_none());
    assert!(room_url("VIMEO", None).is_none());
}

#[test]
fn login_cookie_per_service() {
    assert_eq!(login_cookie("FB"), ("https://www.facebook.com/", "c_user"));
    assert_eq!(login_cookie("YT_RTMP"), ("https://www.youtube.com/", "SID"));
}

#[test]
fn destinations_keep_room_types_join_accounts_and_never_carry_the_key() {
    let eps = vec![
        ep(1, "YT NLCH 4K", "YT_RTMP"),
        ep(2, "e2e file", "TEST_FILE"),
        ep(3, "FB-GoodFest", "FB"),
    ];
    let f = accounts(&[(1, "a@x.com", Some("UC1")), (3, "b@x.com", None)]);
    let d = destinations(&eps, &f);
    assert_eq!(d.len(), 2);
    assert_eq!(d[0].alias, "YT NLCH 4K");
    assert_eq!(d[0].profile, "dest-1");
    assert_eq!(
        d[0].url,
        "https://studio.youtube.com/channel/UC1/livestreaming"
    );
    assert_eq!(d[0].account.as_deref(), Some("a@x.com"));
    assert_eq!(
        d[0].channel.as_deref(),
        Some("UC1"),
        "an account edit must be able to keep it"
    );
    assert_eq!(d[1].channel, None);
    assert!(d[0].sure);
    assert_eq!(d[1].profile, "dest-3");
    let text = serde_json::to_string(&d).unwrap();
    assert!(
        !text.contains("secret-key"),
        "a destination must never carry the stream key"
    );
}

#[test]
fn destination_without_mapping_gets_its_own_profile() {
    let d = destinations(&[ep(9, "FB-Zbynek", "FB")], &AccountsFile::default());
    assert_eq!(d[0].profile, "dest-9");
    assert_eq!(d[0].account, None);
    assert!(!d[0].sure);
}

#[test]
fn attach_tabs_uses_only_live_tabs_and_maps_profiles() {
    let mut d = destinations(
        &[ep(1, "A", "YT_RTMP"), ep(2, "B", "FB"), ep(3, "C", "FB")],
        &accounts(&[
            (1, "a@x.com", None),
            (2, "b@x.com", None),
            (3, "b@x.com", None),
        ]),
    );
    let mut map = TabMap::default();
    map.ctx.insert("C1".into(), "dest-1".into());
    map.ctx.insert("C2".into(), "acct-b-x-com".into());
    map.tab.insert("T1".into(), 1);
    map.tab.insert("T2".into(), 2);
    map.tab.insert("T-dead".into(), 3);
    let pages = vec![page("T1", "C1"), page("T2", "C2"), page("T9", "C9")];
    let pp = attach_tabs(&mut d, &map, &pages);
    assert_eq!(d[0].target_id.as_deref(), Some("T1"));
    assert_eq!(d[1].target_id.as_deref(), Some("T2"));
    assert_eq!(
        d[2].target_id, None,
        "C shares B's profile but its own tab is closed"
    );
    assert_eq!(pp.get("dest-1").map(String::as_str), Some("T1"));
    assert_eq!(pp.get("acct-b-x-com").map(String::as_str), Some("T2"));
}

#[test]
fn new_target_finds_the_room_tab_not_startup_tabs() {
    let mut blank = page("T3", "C2");
    blank.url = "about:blank".into();
    let mut newtab = page("T4", "C2");
    newtab.url = "chrome://newtab/".into();
    let before = vec![page("T1", "C1")];
    let now = vec![page("T1", "C1"), blank, newtab, page("T2", "C2")];
    assert_eq!(new_target(&before, &now).unwrap().target_id, "T2");
    assert!(new_target(&now, &now).is_none());
    assert!(
        new_target(&before, &now[..3]).is_none(),
        "only startup tabs so far: keep waiting"
    );
}

#[test]
fn accounts_file_round_trips_and_clears() {
    let dir = tempfile::tempdir().unwrap();
    let rooms = ControlRooms::new(dir.path());
    assert_eq!(rooms.accounts(), AccountsFile::default());
    let a = Account {
        account: Some("a@x.com".into()),
        channel: Some("UC1".into()),
        channel_name: Some("NEW LEVEL CHURCH".into()),
        sure: true,
    };
    rooms.set_account(5, a.clone()).unwrap();
    assert_eq!(rooms.accounts().endpoints.get("5"), Some(&a));
    assert!(dir.path().join("control-rooms.json").is_file());
    rooms.set_account(5, Account::default()).unwrap();
    assert!(rooms.accounts().endpoints.is_empty());
}

#[test]
fn unreadable_accounts_file_reads_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("control-rooms.json"), "{not json").unwrap();
    assert_eq!(
        ControlRooms::new(dir.path()).accounts(),
        AccountsFile::default()
    );
}

#[tokio::test]
async fn status_without_chrome_lists_destinations_closed() {
    let dir = tempfile::tempdir().unwrap();
    // Point at a port where nothing listens (Chrome "not running").
    let free = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let rooms = ControlRooms::with_browser(dir.path(), Browser::at(dir.path(), free));
    let d = rooms.status(&[ep(1, "A", "YT_RTMP")]).await;
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].target_id, None);
    assert_eq!(d[0].signed_in, None);
}

mod with_fake_chrome {
    use super::*;
    use crate::control_room::fake_chrome::{FakeChrome, page};

    fn rooms_at(dir: &std::path::Path, chrome: &FakeChrome) -> ControlRooms {
        ControlRooms::with_browser(dir, Browser::at(dir, chrome.addr.clone()))
    }

    #[tokio::test]
    async fn status_reports_open_tabs_and_sign_in_per_profile() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        chrome.set_targets(vec![
            page("T1", "C1", "https://studio.youtube.com/"),
            page("T3", "C3", "https://fb/"),
        ]);
        {
            let mut s = chrome.state.lock().unwrap();
            s.cookies
                .insert("T1".into(), vec!["SID".into(), "HSID".into()]);
            s.cookies.insert("T3".into(), vec!["datr".into()]);
        }
        let map = TabMap {
            ctx: [
                ("C1".to_string(), "dest-1".to_string()),
                ("C3".to_string(), "dest-3".to_string()),
            ]
            .into(),
            tab: [("T1".to_string(), 1), ("T3".to_string(), 3)].into(),
            ..Default::default()
        };
        std::fs::write(
            dir.path().join("control-room-tabs.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        let rooms = rooms_at(dir.path(), &chrome);
        rooms
            .set_account(
                1,
                Account {
                    account: Some("a@x.com".into()),
                    sure: true,
                    ..Default::default()
                },
            )
            .unwrap();
        rooms
            .set_account(
                3,
                Account {
                    account: Some("b@x.com".into()),
                    sure: true,
                    ..Default::default()
                },
            )
            .unwrap();
        rooms
            .set_account(
                4,
                Account {
                    account: Some("b@x.com".into()),
                    sure: true,
                    ..Default::default()
                },
            )
            .unwrap();

        let d = rooms
            .status(&[
                ep(1, "YT A", "YT_RTMP"),
                ep(3, "FB B", "FB"),
                ep(4, "FB B2", "FB"),
                ep(5, "YT none", "YT_RTMP"),
            ])
            .await;
        assert_eq!(d[0].target_id.as_deref(), Some("T1"));
        assert_eq!(
            d[0].signed_in,
            Some(true),
            "SID cookie = signed in to YouTube"
        );
        assert_eq!(d[1].target_id.as_deref(), Some("T3"));
        assert_eq!(
            d[1].signed_in,
            Some(false),
            "no c_user cookie = not signed in to FB"
        );
        assert_eq!(d[2].target_id, None, "its own FB profile, no tab open");
        assert_eq!(d[2].signed_in, None, "no tab of its own profile");
        assert_eq!(d[3].signed_in, None, "no tab of that profile");
        let cookie_reads = chrome
            .methods()
            .iter()
            .filter(|m| *m == "Network.getCookies")
            .count();
        assert_eq!(cookie_reads, 2, "one cookie read per (profile, service)");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn open_maps_the_new_tab_and_reuses_it() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        chrome.set_targets(vec![page("T0", "C0", "about:blank")]);
        let mut b = Browser::at(dir.path(), chrome.addr.clone());
        b.chrome_bin = Some(crate::control_room::fake_chrome::fake_bin(
            dir.path(),
            "chrome",
            0,
            None,
        ));
        let rooms = ControlRooms::with_browser(dir.path(), b);
        let dest = destinations(&[ep(7, "YT A", "YT_RTMP")], &AccountsFile::default()).remove(0);

        // The fake "chrome" hands the tab over: Chrome then lists it (plus a
        // startup new-tab page that must not be taken).
        let args = dir.path().join("chrome.args");
        let c2 = chrome.clone();
        tokio::spawn(async move {
            while !args.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            c2.set_targets(vec![
                page("T0", "C0", "about:blank"),
                page("NT", "C7", "chrome://newtab/"),
                page("T7", "C7", "https://studio.youtube.com/"),
            ]);
        });
        let id = tokio::time::timeout(std::time::Duration::from_secs(20), rooms.open(&dest, &[]))
            .await
            .expect("open hung")
            .unwrap();
        assert_eq!(id, "T7");
        let map: TabMap = serde_json::from_slice(
            &std::fs::read(dir.path().join("control-room-tabs.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(map.tab.get("T7"), Some(&7));
        assert_eq!(map.ctx.get("C7").map(String::as_str), Some("dest-7"));

        // A second open reuses the tab and does not run Chrome again.
        assert_eq!(rooms.open(&dest, &[]).await.unwrap(), "T7");
        let runs = std::fs::read_to_string(dir.path().join("chrome.args"))
            .unwrap()
            .lines()
            .count();
        assert_eq!(runs, 1);
    }

    /// Chrome starts in its last-used profile and creates that directory when
    /// it is missing. A YouTube room profile must be cloned before Chrome can
    /// do that, or the room opens signed out (stream-pp, 2026-10-14).
    #[cfg(unix)]
    #[tokio::test]
    async fn open_clones_the_profile_before_chrome_starts() {
        use crate::control_room::browser::xvfb_lock;
        use crate::control_room::fake_chrome::fake_bin;
        let dir = tempfile::tempdir().unwrap();
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        let mut b = Browser::at(dir.path(), addr.clone());
        let ud = b.user_data.clone();
        b.chrome_bin = Some(fake_bin(dir.path(), "chrome", 0, Some(&ud.join("dest-7"))));
        let display = format!(":{}", 20_000 + std::process::id() % 20_000);
        let lock = xvfb_lock(&display);
        let _ = std::fs::remove_file(&lock);
        b.xvfb_bin = fake_bin(dir.path(), "xvfb", 0, Some(&lock));
        b.display = display;
        std::fs::create_dir_all(ud.join("acct-a")).unwrap();
        std::fs::write(ud.join("acct-a").join("Cookies"), "signed-in").unwrap();
        let rooms = ControlRooms::with_browser(dir.path(), b);
        let dest = destinations(&[ep(7, "YT A", "YT_RTMP")], &AccountsFile::default()).remove(0);

        // The debugging port comes up once "chrome" first ran; the room tab
        // appears once it ran a second time (the handover).
        let args = dir.path().join("chrome.args");
        let helper = tokio::spawn(async move {
            while !args.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            let l = tokio::net::TcpListener::bind(&addr).await.unwrap();
            let chrome = FakeChrome::start_on(l).await;
            chrome.set_targets(vec![page("T0", "C0", "about:blank")]);
            while std::fs::read_to_string(&args)
                .unwrap_or_default()
                .lines()
                .count()
                < 2
            {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            chrome.set_targets(vec![
                page("T0", "C0", "about:blank"),
                page("T7", "C7", "https://studio.youtube.com/"),
            ]);
            chrome
        });
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            rooms.open(&dest, &["acct-a".to_string()]),
        )
        .await
        .expect("open hung")
        .unwrap();
        let _ = std::fs::remove_file(&lock);
        assert_eq!(id, "T7");
        assert_eq!(
            std::fs::read_to_string(ud.join("dest-7").join("Cookies"))
                .ok()
                .as_deref(),
            Some("signed-in"),
            "the room profile must be the clone of the signed-in profile"
        );
        helper.abort();
    }

    fn read_map(dir: &std::path::Path) -> TabMap {
        serde_json::from_slice(&std::fs::read(dir.join("control-room-tabs.json")).unwrap()).unwrap()
    }

    fn target_ids(chrome: &FakeChrome) -> Vec<String> {
        chrome
            .state
            .lock()
            .unwrap()
            .targets
            .iter()
            .map(|t| t["targetId"].as_str().unwrap().to_string())
            .collect()
    }

    /// Once "chrome" has run `runs` times (a handover), Chrome lists `targets`.
    fn after_handover(
        dir: &std::path::Path,
        chrome: &FakeChrome,
        runs: usize,
        targets: Vec<serde_json::Value>,
    ) {
        let args = dir.join("chrome.args");
        let c = chrome.clone();
        tokio::spawn(async move {
            while std::fs::read_to_string(&args)
                .unwrap_or_default()
                .lines()
                .count()
                < runs
            {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            c.set_targets(targets);
        });
    }

    /// Every live room tab costs ~500 MB of Chrome memory; stream-pp ran out
    /// of memory with 7 of them (2026-10-14: the OOM killer hit strih OBS and
    /// Restreamer). A room opened beyond the limit closes the least recently
    /// viewed one; that room reopens at its stored page later.
    #[cfg(unix)]
    #[tokio::test]
    async fn open_closes_the_least_recently_viewed_room_beyond_the_limit() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        let tab = |i: i64| {
            page(
                &format!("T{i}"),
                &format!("C{i}"),
                "https://studio.youtube.com/",
            )
        };
        // Room 1 has a popup open (#405): it must go with its room.
        let popup = serde_json::json!({"targetId": "P1", "type": "page",
            "url": "https://accounts.google.com/", "title": "p",
            "openerId": "T1", "canAccessOpener": true});
        let mut four: Vec<serde_json::Value> = (1..=4).map(tab).collect();
        four.push(popup.clone());
        chrome.set_targets(four);
        let map = TabMap {
            ctx: (1..=4)
                .map(|i| (format!("C{i}"), format!("dest-{i}")))
                .collect(),
            tab: (1..=4).map(|i| (format!("T{i}"), i)).collect(),
            recent: vec![4, 3, 2, 1],
            ..Default::default()
        };
        std::fs::write(
            dir.path().join("control-room-tabs.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        let mut b = Browser::at(dir.path(), chrome.addr.clone());
        b.chrome_bin = Some(crate::control_room::fake_chrome::fake_bin(
            dir.path(),
            "chrome",
            0,
            None,
        ));
        let rooms = ControlRooms::with_browser(dir.path(), b);
        let eps: Vec<EndpointConfig> = (1..=6)
            .map(|i| ep(i, &format!("YT {i}"), "YT_RTMP"))
            .collect();
        let dests = destinations(&eps, &AccountsFile::default());

        let mut five: Vec<serde_json::Value> = (1..=5).map(tab).collect();
        five.push(popup);
        after_handover(dir.path(), &chrome, 1, five);
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            rooms.open(&dests[4], &[]),
        )
        .await
        .expect("open hung")
        .unwrap();
        assert_eq!(id, "T5");
        assert_eq!(
            target_ids(&chrome),
            vec!["T2", "T3", "T4", "T5"],
            "room 1 was viewed longest ago, and its popup went with it"
        );
        let m = read_map(dir.path());
        assert!(!m.tab.contains_key("T1"), "{m:?}");
        assert_eq!(m.recent, vec![5, 4, 3, 2]);

        // Viewing room 2 again makes it the most recent: the next new room
        // closes room 3 instead.
        assert_eq!(rooms.open(&dests[1], &[]).await.unwrap(), "T2");
        after_handover(
            dir.path(),
            &chrome,
            2,
            [2, 3, 4, 5, 6].into_iter().map(tab).collect(),
        );
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            rooms.open(&dests[5], &[]),
        )
        .await
        .expect("open hung")
        .unwrap();
        assert_eq!(id, "T6");
        assert_eq!(target_ids(&chrome), vec!["T2", "T4", "T5", "T6"]);
        assert_eq!(read_map(dir.path()).recent, vec![6, 2, 5, 4]);
    }

    /// FB rooms shared one account profile, so one room's sign-in change
    /// changed them all (stream-pp, 2026-10-14). A room still open in the old
    /// shared profile is closed and reopened in its own, cloned profile.
    #[cfg(unix)]
    #[tokio::test]
    async fn open_moves_a_room_out_of_a_shared_profile() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        let producer = "https://www.facebook.com/live/producer/";
        chrome.set_targets(vec![page("T3", "CB", producer)]);
        let map = TabMap {
            ctx: [("CB".to_string(), "acct-b-x-com".to_string())].into(),
            tab: [("T3".to_string(), 3)].into(),
            ..Default::default()
        };
        std::fs::write(
            dir.path().join("control-room-tabs.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        let mut b = Browser::at(dir.path(), chrome.addr.clone());
        b.chrome_bin = Some(crate::control_room::fake_chrome::fake_bin(
            dir.path(),
            "chrome",
            0,
            None,
        ));
        let ud = b.user_data.clone();
        std::fs::create_dir_all(ud.join("acct-b-x-com")).unwrap();
        std::fs::write(ud.join("acct-b-x-com").join("Cookies"), "fb-signed-in").unwrap();
        let rooms = ControlRooms::with_browser(dir.path(), b);
        let f = accounts(&[(3, "b@x.com", None)]);
        let eps = vec![ep(3, "FB B", "FB")];
        let dest = destinations(&eps, &f).remove(0);
        assert_eq!(dest.profile, "dest-3");

        after_handover(dir.path(), &chrome, 1, vec![page("T3b", "C3", producer)]);
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            rooms.open(&dest, &clone_sources(&dest, &eps, &f)),
        )
        .await
        .expect("open hung")
        .unwrap();
        assert_eq!(id, "T3b");
        assert!(chrome.methods().iter().any(|m| m == "Target.closeTarget"));
        assert_eq!(
            std::fs::read_to_string(ud.join("dest-3").join("Cookies"))
                .ok()
                .as_deref(),
            Some("fb-signed-in")
        );
        let m = read_map(dir.path());
        assert_eq!(m.ctx.get("C3").map(String::as_str), Some("dest-3"));
        assert_eq!(m.tab.get("T3b"), Some(&3));
    }

    /// Review of PR #398: a room whose tab Chrome refused to close was
    /// dropped from the map anyway. It stayed open, uncounted and never
    /// closed again, so Chrome grew past the live-room limit.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_room_chrome_refused_to_close_stays_tracked_and_is_closed_later() {
        let dir = tempfile::tempdir().unwrap();
        let chrome = FakeChrome::start().await;
        let tab = |i: i64| {
            page(
                &format!("T{i}"),
                &format!("C{i}"),
                "https://studio.youtube.com/",
            )
        };
        chrome.set_targets((1..=4).map(tab).collect());
        chrome.state.lock().unwrap().refuse_close = vec!["T1".into()];
        let map = TabMap {
            ctx: (1..=6)
                .map(|i| (format!("C{i}"), format!("dest-{i}")))
                .collect(),
            tab: (1..=4).map(|i| (format!("T{i}"), i)).collect(),
            recent: vec![4, 3, 2, 1],
            ..Default::default()
        };
        std::fs::write(
            dir.path().join("control-room-tabs.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        let mut b = Browser::at(dir.path(), chrome.addr.clone());
        b.chrome_bin = Some(crate::control_room::fake_chrome::fake_bin(
            dir.path(),
            "chrome",
            0,
            None,
        ));
        let rooms = ControlRooms::with_browser(dir.path(), b);
        let eps: Vec<EndpointConfig> = (1..=6)
            .map(|i| ep(i, &format!("YT {i}"), "YT_RTMP"))
            .collect();
        let dests = destinations(&eps, &AccountsFile::default());

        after_handover(dir.path(), &chrome, 1, (1..=5).map(tab).collect());
        let open = |i: usize| {
            tokio::time::timeout(
                std::time::Duration::from_secs(20),
                rooms.open(&dests[i], &[]),
            )
        };
        assert_eq!(open(4).await.expect("open hung").unwrap(), "T5");
        assert_eq!(target_ids(&chrome), vec!["T1", "T2", "T3", "T4", "T5"]);
        let m = read_map(dir.path());
        assert_eq!(
            m.tab.get("T1"),
            Some(&1),
            "a tab still open stays tracked: {m:?}"
        );

        // Chrome closes it on the next try.
        chrome.state.lock().unwrap().refuse_close.clear();
        assert_eq!(open(3).await.expect("open hung").unwrap(), "T4");
        assert_eq!(target_ids(&chrome), vec!["T2", "T3", "T4", "T5"]);
        assert!(!read_map(dir.path()).tab.contains_key("T1"));
    }
}

/// #399: the box Chrome reads uploaded thumbnails from the data dir, next to its
/// profiles (the room WebSocket handler hands this path to the relay).
#[test]
fn uploads_live_in_the_browser_profiles_dir() {
    let dir = tempfile::tempdir().unwrap();
    let rooms = ControlRooms::new(dir.path());
    assert_eq!(
        rooms.uploads_dir(),
        dir.path().join("browser-profiles").join("uploads")
    );
}

/// #407: every room relay gets the SAME shown-tabs set, so a room one viewer
/// watches is never hidden by another viewer's relay.
#[test]
fn every_relay_shares_one_shown_tabs_set() {
    let dir = tempfile::tempdir().unwrap();
    let rooms = ControlRooms::new(dir.path());
    let _viewer = rooms.shown_tabs().show("T1");
    assert!(rooms.shown_tabs().is_shown("T1"));
}

/// #405 follow-up: a page saved by an older release that `remember_url`
/// rejects today (YT-BB-RTMP on stream-pp kept `studio.youtube.com/reauth`
/// from 0.29.47) is never reopened: the room opens at its default page.
#[tokio::test]
async fn a_saved_page_no_longer_worth_keeping_is_not_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let map = TabMap {
        url: [(1, "https://studio.youtube.com/reauth?rapt=x".to_string())].into(),
        ..Default::default()
    };
    std::fs::write(
        dir.path().join("control-room-tabs.json"),
        serde_json::to_vec(&map).unwrap(),
    )
    .unwrap();
    let rooms =
        ControlRooms::with_browser(dir.path(), Browser::at(dir.path(), "127.0.0.1:9".into()));
    let d = rooms.status(&[ep(1, "YT A", "YT_RTMP")]).await;
    assert_eq!(d[0].url, "https://studio.youtube.com/");
}
