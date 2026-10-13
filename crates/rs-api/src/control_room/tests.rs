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
fn youtube_profiles_are_per_destination_facebook_per_account() {
    // One Google profile holds one channel identity at a time (#388 finding).
    assert_eq!(
        profile_for("YT_RTMP", Some("Milost.SNV@gmail.com"), 4),
        "dest-4"
    );
    assert_eq!(
        profile_for("FB", Some("Milost.SNV@gmail.com"), 4),
        "acct-milost-snv-gmail-com"
    );
    assert_eq!(profile_for("FB", Some("  "), 4), "dest-4");
    assert_eq!(profile_for("FB", None, 11), "dest-11");
    assert_eq!(account_profile(" A.B@x.com "), "acct-a-b-x-com");
}

#[test]
fn clone_sources_are_same_account_youtube_profiles() {
    let eps = vec![
        ep(1, "NLCH", "YT_RTMP"),
        ep(2, "NLW", "YT_RTMP"),
        ep(3, "Kiko", "YT_RTMP"),
        ep(4, "FB-GoodFest", "FB"),
    ];
    let f = accounts(&[
        (1, "a@x.com", None),
        (2, "A@X.com", None),
        (3, "k@x.com", None),
        (4, "a@x.com", None),
    ]);
    let d = destinations(&eps, &f);
    assert_eq!(
        clone_sources(&d[0], &eps, &f),
        vec!["acct-a-x-com", "dest-2"]
    );
    assert_eq!(clone_sources(&d[2], &eps, &f), vec!["acct-k-x-com"]);
    assert!(
        clone_sources(&d[3], &eps, &f).is_empty(),
        "FB shares one profile, nothing to clone"
    );
    let unknown = destinations(&[ep(9, "YT-?", "YT_RTMP")], &AccountsFile::default());
    assert!(clone_sources(&unknown[0], &eps, &f).is_empty());
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
    assert!(d[0].sure);
    assert_eq!(d[1].profile, "acct-b-x-com");
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
