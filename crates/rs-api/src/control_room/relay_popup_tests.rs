//! #405: a popup that can reach its opener is shown in place, not moved
//! into the room tab. The pure decisions are in `popups_tests.rs`.

use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

use super::serve_relay;
use crate::control_room::cdp::Cdp;
use crate::control_room::fake_chrome::FakeChrome;

const ROOM: &str = "https://studio.youtube.com/channel/UC1/livestreaming";
const CHALLENGE: &str = "https://accounts.google.com/v3/signin/challenge/pwd";

fn room() -> Value {
    json!({"targetId": "T1", "type": "page", "url": ROOM, "title": "r"})
}

/// A `window.open` popup of `opener`.
fn popup(id: &str, url: &str, opener: &str) -> Value {
    json!({"targetId": id, "type": "page", "url": url, "title": id,
           "openerId": opener, "canAccessOpener": true})
}

/// The CDP requests the fake Chrome got on `path` with `method`.
fn requests_on(c: &FakeChrome, path: &str, method: &str) -> Vec<Value> {
    c.state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(p, r)| p == path && r["method"] == method)
        .map(|(_, r)| r["params"].clone())
        .collect()
}

fn screencasts(c: &FakeChrome, tab: &str) -> usize {
    requests_on(c, &format!("page/{tab}"), "Page.startScreencast").len()
}

async fn wait_for(what: &str, c: &FakeChrome, done: impl Fn() -> bool) {
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

type ToRelay = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// A dashboard viewer: what it sends, and every text message it got.
async fn viewer(addr: std::net::SocketAddr) -> (ToRelay, Arc<Mutex<Vec<Value>>>) {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let (tx, mut rx) = ws.split();
    let got: Arc<Mutex<Vec<Value>>> = Arc::default();
    let sink = Arc::clone(&got);
    tokio::spawn(async move {
        while let Some(Ok(m)) = rx.next().await {
            if let Message::Text(t) = m
                && let Ok(v) = serde_json::from_str::<Value>(&t)
            {
                sink.lock().unwrap().push(v);
            }
        }
    });
    (tx, got)
}

async fn say(tx: &mut ToRelay, msg: Value) {
    tx.send(Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn relay_shows_a_verification_popup_and_returns_to_the_room_when_it_closes() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![room()]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (mut tx, got) = viewer(addr).await;
    wait_for("the room was never shown", &chrome, || {
        screencasts(&chrome, "T1") == 1
    })
    .await;
    // The viewer's size, before the popup opens (the screencast restarts).
    say(&mut tx, json!({"t": "size", "w": 800, "h": 600})).await;
    wait_for("the size never reached the room", &chrome, || {
        screencasts(&chrome, "T1") == 2
    })
    .await;

    chrome.set_targets(vec![room(), popup("P1", CHALLENGE, "T1")]);
    wait_for("the popup was never shown", &chrome, || {
        screencasts(&chrome, "P1") == 1
    })
    .await;
    // The Studio page that opened it stays where it is, and the popup lives.
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());
    assert!(requests_on(&chrome, "browser/b1", "Target.closeTarget").is_empty());
    // The popup is shown at the viewer's size.
    assert_eq!(
        requests_on(&chrome, "page/P1", "Emulation.setDeviceMetricsOverride")[0]["width"],
        800
    );
    {
        let m = got.lock().unwrap();
        assert!(m.contains(&json!({"t": "url", "url": CHALLENGE})), "{m:?}");
        assert!(m.contains(&json!({"t": "popup", "open": true})), "{m:?}");
    }

    // Studio got the proof and closed the popup. Like Chrome, the fake closes
    // the popup's socket at once, long before the next tab-list poll.
    chrome.set_targets(vec![room()]);
    wait_for("the relay never went back to the room tab", &chrome, || {
        screencasts(&chrome, "T1") == 3
    })
    .await;
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());
    assert_eq!(
        requests_on(&chrome, "page/T1", "Emulation.setDeviceMetricsOverride").len(),
        2,
        "the room tab gets the viewer's size again"
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let m = got.lock().unwrap().clone();
        let n = m.len();
        if n >= 2
            && m[n - 2] == json!({"t": "popup", "open": false})
            && m[n - 1] == json!({"t": "url", "url": ROOM})
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dashboard was not told it is back in the room: {m:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn the_viewer_can_leave_a_popup_that_never_closes() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![
        room(),
        popup("P1", "https://www.youtube.com/live_chat", "T1"),
    ]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (mut tx, got) = viewer(addr).await;
    wait_for("the popup was never shown", &chrome, || {
        screencasts(&chrome, "P1") == 1
    })
    .await;
    // In the room tab "close-popup" means nothing; in the popup it leaves it.
    say(&mut tx, json!({"t": "close-popup"})).await;
    wait_for("the viewer could not leave the popup", &chrome, || {
        screencasts(&chrome, "T1") == 2
    })
    .await;
    assert_eq!(
        requests_on(&chrome, "browser/b1", "Target.closeTarget"),
        vec![json!({"targetId": "P1"})]
    );
    assert!(
        got.lock()
            .unwrap()
            .contains(&json!({"t": "popup", "open": false}))
    );
    // A close-popup in the room tab does nothing.
    say(&mut tx, json!({"t": "close-popup"})).await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(screencasts(&chrome, "T1"), 2);
    assert_eq!(
        requests_on(&chrome, "browser/b1", "Target.closeTarget").len(),
        1
    );
}

#[tokio::test]
async fn a_shown_popup_that_lands_on_a_room_page_is_handed_to_the_room_tab() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![
        room(),
        popup("P1", "https://www.youtube.com/channel_switcher", "T1"),
    ]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_tx, got) = viewer(addr).await;
    wait_for("the popup was never shown", &chrome, || {
        screencasts(&chrome, "P1") == 1
    })
    .await;
    assert!(requests_on(&chrome, "page/T1", "Page.navigate").is_empty());

    let landed = "https://studio.youtube.com/channel/UC2/livestreaming";
    chrome.set_targets(vec![room(), popup("P1", landed, "T1")]);
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
    let m = got.lock().unwrap().clone();
    assert_eq!(
        m.last(),
        Some(&json!({"t": "url", "url": landed})),
        "the dashboard sees the page the room takes over: {m:?}"
    );
}

/// Chrome refuses to close a landed popup: the room takes its page over
/// once, and never shows it or hands it over again.
#[tokio::test]
async fn a_popup_chrome_will_not_close_is_handed_over_only_once() {
    let chrome = FakeChrome::start().await;
    let landed = "https://studio.youtube.com/channel/UC2/livestreaming";
    chrome.set_targets(vec![
        room(),
        popup("P1", "https://www.youtube.com/channel_switcher", "T1"),
    ]);
    chrome.state.lock().unwrap().refuse_close.push("P1".into());
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_tx, _got) = viewer(addr).await;
    wait_for("the popup was never shown", &chrome, || {
        screencasts(&chrome, "P1") == 1
    })
    .await;
    chrome.set_targets(vec![room(), popup("P1", landed, "T1")]);
    wait_for("the landed popup was never handed over", &chrome, || {
        !requests_on(&chrome, "page/T1", "Page.navigate").is_empty()
    })
    .await;
    // Several tab-list polls later: no show / hand-over loop.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert_eq!(requests_on(&chrome, "page/T1", "Page.navigate").len(), 1);
    assert_eq!(screencasts(&chrome, "P1"), 1);
    assert_eq!(screencasts(&chrome, "T1"), 2);
}

/// A popup whose socket cannot be opened counts as done: the room tab is
/// shown again, and the popup is not tried in a loop.
#[tokio::test]
async fn a_popup_that_cannot_be_attached_is_tried_once() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![room(), popup("P1", CHALLENGE, "T1")]);
    chrome.state.lock().unwrap().refuse_attach.push("P1".into());
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_tx, _got) = viewer(addr).await;
    wait_for("the room tab was never shown again", &chrome, || {
        screencasts(&chrome, "T1") == 2
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    let attaches = chrome.state.lock().unwrap().attaches.clone();
    assert_eq!(
        attaches.iter().filter(|a| *a == "P1").count(),
        1,
        "{attaches:?}"
    );
    assert_eq!(screencasts(&chrome, "T1"), 2);
}

/// A popup's own `window.open` popup is shown over it (Google's flow can
/// nest); when it closes, the first popup is shown again, not the room. A
/// popup's `noopener` tab is moved into the popup.
#[tokio::test]
async fn popups_of_a_shown_popup_are_shown_over_it_or_moved_into_it() {
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![room(), popup("P1", CHALLENGE, "T1")]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_tx, _got) = viewer(addr).await;
    wait_for("the popup was never shown", &chrome, || {
        screencasts(&chrome, "P1") == 1
    })
    .await;

    let inner = "https://accounts.google.com/v3/signin/challenge/dp";
    chrome.set_targets(vec![
        room(),
        popup("P1", CHALLENGE, "T1"),
        popup("P2", inner, "P1"),
    ]);
    wait_for("the inner popup was never shown", &chrome, || {
        screencasts(&chrome, "P2") == 1
    })
    .await;
    assert!(requests_on(&chrome, "page/P1", "Page.navigate").is_empty());

    chrome.set_targets(vec![room(), popup("P1", CHALLENGE, "T1")]);
    wait_for("the first popup was never shown again", &chrome, || {
        screencasts(&chrome, "P1") == 2
    })
    .await;
    assert_eq!(screencasts(&chrome, "T1"), 1, "not back to the room yet");

    let help = "https://support.google.com/accounts/answer/1";
    chrome.set_targets(vec![
        room(),
        popup("P1", CHALLENGE, "T1"),
        json!({"targetId": "P3", "type": "page", "url": help, "title": "h",
               "openerId": "P1", "canAccessOpener": false}),
    ]);
    wait_for("the noopener tab was never moved", &chrome, || {
        !requests_on(&chrome, "page/P1", "Page.navigate").is_empty()
    })
    .await;
    assert_eq!(
        requests_on(&chrome, "page/P1", "Page.navigate"),
        vec![json!({"url": help})]
    );
    assert_eq!(
        requests_on(&chrome, "browser/b1", "Target.closeTarget"),
        vec![json!({"targetId": "P3"})]
    );
    assert_eq!(screencasts(&chrome, "P3"), 0);
}

/// Chrome refuses to close a `noopener` tab: it is moved into the room tab
/// once, not on every poll.
#[tokio::test]
async fn a_noopener_tab_chrome_will_not_close_is_moved_only_once() {
    let chrome = FakeChrome::start().await;
    let account = "https://www.youtube.com/account";
    chrome.set_targets(vec![
        room(),
        json!({"targetId": "P1", "type": "page", "url": account, "title": "a",
               "openerId": "T1", "canAccessOpener": false}),
    ]);
    chrome.state.lock().unwrap().refuse_close.push("P1".into());
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_tx, _got) = viewer(addr).await;
    wait_for("the tab was never moved", &chrome, || {
        !requests_on(&chrome, "page/T1", "Page.navigate").is_empty()
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert_eq!(
        requests_on(&chrome, "page/T1", "Page.navigate"),
        vec![json!({"url": account})]
    );
}
