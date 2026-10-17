//! #405: a popup that can reach its opener is shown in place, not moved
//! into the room tab.

use serde_json::{Value, json};

use super::super::{PopupEnd, PopupMove, popup_end, popup_moves};
use super::{serve_relay, tinfo};
use crate::control_room::cdp::{Cdp, TargetInfo};

fn opener_popup(id: &str, url: &str, opener: &str) -> TargetInfo {
    TargetInfo {
        can_access_opener: true,
        ..tinfo(id, url, Some(opener))
    }
}

#[test]
fn a_popup_that_can_reach_its_opener_is_shown_in_place() {
    let auth = opener_popup(
        "P1",
        "https://accounts.google.com/v3/signin/challenge/pwd",
        "R",
    );
    let link = tinfo("P2", "https://www.youtube.com/account", Some("R"));
    let pages = vec![
        tinfo("R", "https://studio.youtube.com/", None),
        auth.clone(),
        link.clone(),
        opener_popup("P3", "https://x/", "OTHER"),
    ];
    assert_eq!(
        popup_moves(&pages, "R"),
        vec![PopupMove::ShowInPlace(auth), PopupMove::MoveIntoTab(link)]
    );
}

#[test]
fn a_shown_popup_ends_when_it_closes_or_lands_on_a_room_page() {
    let room = tinfo(
        "R",
        "https://studio.youtube.com/channel/UC1/livestreaming",
        None,
    );
    let reauth = opener_popup("P", "https://studio.youtube.com/reauth?rapt=x", "R");
    let challenge = opener_popup(
        "P",
        "https://accounts.google.com/v3/signin/challenge/dp",
        "R",
    );
    assert_eq!(popup_end(&[room.clone(), reauth], "P"), None);
    assert_eq!(popup_end(&[room.clone(), challenge], "P"), None);
    assert_eq!(
        popup_end(std::slice::from_ref(&room), "P"),
        Some(PopupEnd::Closed)
    );
    let landed = opener_popup(
        "P",
        "https://studio.youtube.com/channel/UC2/livestreaming",
        "R",
    );
    assert_eq!(
        popup_end(&[room, landed], "P"),
        Some(PopupEnd::Landed(
            "https://studio.youtube.com/channel/UC2/livestreaming".into()
        ))
    );
}

/// The CDP requests the fake Chrome got on `path` with `method`.
fn requests_on(
    c: &crate::control_room::fake_chrome::FakeChrome,
    path: &str,
    method: &str,
) -> Vec<Value> {
    c.state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(p, r)| p == path && r["method"] == method)
        .map(|(_, r)| r["params"].clone())
        .collect()
}

async fn wait_for(
    what: &str,
    c: &crate::control_room::fake_chrome::FakeChrome,
    done: impl Fn() -> bool,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}; requests: {:?}",
            c.methods()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn relay_shows_a_verification_popup_and_returns_to_the_room_when_it_closes() {
    use crate::control_room::fake_chrome::FakeChrome;
    let chrome = FakeChrome::start().await;
    let room = json!({"targetId": "T1", "type": "page",
                      "url": "https://studio.youtube.com/channel/UC1/livestreaming", "title": "r"});
    chrome.set_targets(vec![
        room.clone(),
        json!({"targetId": "P1", "type": "page",
               "url": "https://accounts.google.com/v3/signin/challenge/pwd",
               "title": "p", "openerId": "T1", "canAccessOpener": true}),
    ]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for("the popup was never shown", &chrome, || {
        !requests_on(&chrome, "page/P1", "Page.startScreencast").is_empty()
    })
    .await;
    // The Studio page that opened it stays where it is, and the popup lives.
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());
    assert!(requests_on(&chrome, "browser/b1", "Target.closeTarget").is_empty());

    // Studio got the proof and closed the popup: back to the room tab.
    let before = requests_on(&chrome, "page/T1", "Page.startScreencast").len();
    chrome.set_targets(vec![room]);
    wait_for("the relay never went back to the room tab", &chrome, || {
        requests_on(&chrome, "page/T1", "Page.startScreencast").len() > before
    })
    .await;
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());
    assert!(requests_on(&chrome, "browser/b1", "Target.closeTarget").is_empty());
}

#[tokio::test]
async fn a_shown_popup_that_lands_on_a_room_page_is_handed_to_the_room_tab() {
    use crate::control_room::fake_chrome::FakeChrome;
    let chrome = FakeChrome::start().await;
    let room = json!({"targetId": "T1", "type": "page",
                      "url": "https://studio.youtube.com/channel/UC1/livestreaming", "title": "r"});
    let popup = |url: &str| {
        json!({"targetId": "P1", "type": "page", "url": url, "title": "p",
               "openerId": "T1", "canAccessOpener": true})
    };
    chrome.set_targets(vec![
        room.clone(),
        popup("https://www.youtube.com/channel_switcher"),
    ]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for("the popup was never shown", &chrome, || {
        !requests_on(&chrome, "page/P1", "Page.startScreencast").is_empty()
    })
    .await;
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());

    let landed = "https://studio.youtube.com/channel/UC2/livestreaming";
    chrome.set_targets(vec![room, popup(landed)]);
    wait_for("the landed popup was never handed over", &chrome, || {
        !requests_on(&chrome, "page/T1", "Page.navigate").is_empty()
    })
    .await;
    assert_eq!(
        requests_on(&chrome, "page/T1", "Page.navigate"),
        vec![json!({"url": landed})]
    );
    assert_eq!(
        requests_on(&chrome, "browser/b1", "Target.closeTarget"),
        vec![json!({"targetId": "P1"})]
    );
}

#[tokio::test]
async fn a_popup_of_a_shown_popup_is_moved_into_the_shown_popup() {
    use crate::control_room::fake_chrome::FakeChrome;
    let chrome = FakeChrome::start().await;
    let room = json!({"targetId": "T1", "type": "page",
                      "url": "https://studio.youtube.com/channel/UC1/livestreaming", "title": "r"});
    let p1 = json!({"targetId": "P1", "type": "page",
                    "url": "https://accounts.google.com/v3/signin/challenge/pwd",
                    "title": "p", "openerId": "T1", "canAccessOpener": true});
    chrome.set_targets(vec![room.clone(), p1.clone()]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for("the popup was never shown", &chrome, || {
        !requests_on(&chrome, "page/P1", "Page.startScreencast").is_empty()
    })
    .await;

    // The shown popup opens one of its own: it stays inside the shown popup.
    let help = "https://support.google.com/accounts/answer/1";
    chrome.set_targets(vec![
        room,
        p1,
        json!({"targetId": "P2", "type": "page", "url": help, "title": "h",
               "openerId": "P1", "canAccessOpener": true}),
    ]);
    wait_for("the popup's own popup was never moved", &chrome, || {
        !requests_on(&chrome, "page/P1", "Page.navigate").is_empty()
    })
    .await;
    assert_eq!(
        requests_on(&chrome, "page/P1", "Page.navigate"),
        vec![json!({"url": help})]
    );
    assert_eq!(
        requests_on(&chrome, "browser/b1", "Target.closeTarget"),
        vec![json!({"targetId": "P2"})]
    );
    assert!(requests_on(&chrome, "page/P2", "Page.startScreencast").is_empty());
}
