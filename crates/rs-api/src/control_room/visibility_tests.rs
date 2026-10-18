//! #407: who gets hidden, and how.

use super::*;
use crate::control_room::fake_chrome::FakeChrome;

fn tinfo(id: &str, url: &str, opener: Option<&str>) -> TargetInfo {
    TargetInfo {
        target_id: id.into(),
        kind: "page".into(),
        url: url.into(),
        title: id.into(),
        browser_context_id: None,
        opener_id: opener.map(str::to_string),
        can_access_opener: opener.is_some(),
    }
}

#[test]
fn a_tab_stays_shown_while_any_relay_shows_it() {
    let shown = ShownTabs::default();
    assert!(!shown.is_shown("T1"));
    let a = shown.show("T1");
    let b = shown.show("T1");
    let other = shown.show("T2");
    drop(a);
    assert!(shown.is_shown("T1"), "the second viewer still shows it");
    drop(b);
    assert!(!shown.is_shown("T1"));
    assert!(shown.is_shown("T2"));
    drop(other);
    assert!(shown.snapshot().is_empty());
}

#[test]
fn only_room_tabs_nobody_shows_are_hidden() {
    let pages = vec![
        tinfo("SHOWN", "https://studio.youtube.com/a", None),
        tinfo("ROOM", "https://studio.youtube.com/b", None),
        tinfo("FB", "http://www.facebook.com/live/producer", None),
        tinfo("POPUP", "https://accounts.google.com/x", Some("ROOM")),
        tinfo("BLANK", "about:blank", None),
        tinfo("NEWTAB", "chrome://newtab/", None),
    ];
    let shown = ShownTabs::default();
    let _g = shown.show("SHOWN");
    assert_eq!(to_hide(&pages, &shown.snapshot()), vec!["ROOM", "FB"]);
}

#[test]
fn the_visibility_is_read_from_the_evaluate_reply() {
    assert_eq!(
        visibility(&json!({"result": {"type": "string", "value": "visible"}})),
        Some("visible")
    );
    assert_eq!(visibility(&json!({"result": {"value": 3}})), None);
    assert_eq!(visibility(&json!({})), None);
}

/// The methods the fake Chrome got on `page/<tab>`, in order.
fn methods_on(c: &FakeChrome, tab: &str) -> Vec<String> {
    let path = format!("page/{tab}");
    c.state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(p, _)| *p == path)
        .map(|(_, r)| {
            let m = r["method"].as_str().unwrap_or("").to_string();
            match r["params"]["state"].as_str() {
                Some(state) => format!("{m} {state}"),
                None => m,
            }
        })
        .collect()
}

#[tokio::test]
async fn a_visible_tab_is_frozen_then_let_run_and_a_hidden_one_is_left_alone() {
    let chrome = FakeChrome::start().await;
    {
        let mut s = chrome.state.lock().unwrap();
        s.visibility.insert("V".into(), "visible".into());
        s.visibility.insert("H".into(), "hidden".into());
    }
    let cdp = Cdp::new(chrome.addr.clone());
    assert!(hide_if_visible(&cdp, "V").await.unwrap());
    assert_eq!(
        methods_on(&chrome, "V"),
        vec![
            "Runtime.evaluate",
            "Page.setWebLifecycleState frozen",
            "Page.setWebLifecycleState active"
        ]
    );
    assert!(!hide_if_visible(&cdp, "H").await.unwrap());
    assert_eq!(methods_on(&chrome, "H"), vec!["Runtime.evaluate"]);
    assert!(
        !hide_if_visible(&cdp, "UNKNOWN").await.unwrap(),
        "no visibility in the reply: never frozen"
    );
}

#[tokio::test]
async fn hide_others_hides_the_visible_rooms_nobody_shows() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![
        json!({"targetId": "T1", "type": "page", "url": "https://a/", "title": "a"}),
        json!({"targetId": "T2", "type": "page", "url": "https://b/", "title": "b"}),
        json!({"targetId": "T3", "type": "page", "url": "https://c/", "title": "c"}),
    ]);
    {
        let mut s = chrome.state.lock().unwrap();
        for t in ["T1", "T2", "T3"] {
            s.visibility.insert(t.into(), "visible".into());
        }
    }
    let shown = ShownTabs::default();
    let _viewer = shown.show("T1");
    hide_others(&Cdp::new(chrome.addr.clone()), &shown).await;
    assert!(
        methods_on(&chrome, "T1").is_empty(),
        "the shown room is untouched"
    );
    for t in ["T2", "T3"] {
        assert_eq!(
            methods_on(&chrome, t),
            vec![
                "Runtime.evaluate",
                "Page.setWebLifecycleState frozen",
                "Page.setWebLifecycleState active"
            ],
            "{t}"
        );
    }
}
