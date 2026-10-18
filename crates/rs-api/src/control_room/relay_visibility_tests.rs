//! #407: the room a viewer watches stays visible; every other room is
//! hidden, so the software compositor renders one room instead of all.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

use super::super::{ShownTabs, relay_room};
use crate::control_room::cdp::Cdp;
use crate::control_room::fake_chrome::FakeChrome;

fn tab(id: &str) -> Value {
    json!({"targetId": id, "type": "page", "url": format!("https://studio.youtube.com/{id}"), "title": id})
}

/// Rooms T1..T3, T2 and T3 visible, T4 hidden.
async fn chrome() -> FakeChrome {
    let c = FakeChrome::start().await;
    c.set_targets(vec![tab("T1"), tab("T2"), tab("T3"), tab("T4")]);
    {
        let mut s = c.state.lock().unwrap();
        for t in ["T1", "T2", "T3"] {
            s.visibility.insert(t.into(), "visible".into());
        }
        s.visibility.insert("T4".into(), "hidden".into());
    }
    c
}

/// Serve a relay of the target in the URL (`/ws/<id>`), every relay sharing
/// `shown`.
async fn serve(cdp: Cdp, shown: ShownTabs) -> std::net::SocketAddr {
    use axum::extract::{Path, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/ws/{target}",
        axum::routing::get(move |Path(target): Path<String>, ws: WebSocketUpgrade| {
            let (cdp, shown) = (cdp.clone(), shown.clone());
            async move {
                ws.on_upgrade(move |s| {
                    relay_room(
                        s,
                        cdp,
                        target,
                        "https://start/".into(),
                        Arc::new(|_| {}),
                        std::env::temp_dir().join("restreamer-room-uploads"),
                        shown,
                    )
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

type Viewer =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn view(addr: std::net::SocketAddr, target: &str) -> Viewer {
    tokio_tungstenite::connect_async(format!("ws://{addr}/ws/{target}"))
        .await
        .unwrap()
        .0
}

/// (index in the request log, method + lifecycle state) of every request on
/// `page/<tab>`.
fn log_on(c: &FakeChrome, tab: &str) -> Vec<(usize, String)> {
    let path = format!("page/{tab}");
    c.state
        .lock()
        .unwrap()
        .requests
        .iter()
        .enumerate()
        .filter(|(_, (p, _))| *p == path)
        .map(|(i, (_, r))| {
            let m = r["method"].as_str().unwrap_or("").to_string();
            let m = match r["params"]["state"].as_str() {
                Some(state) => format!("{m} {state}"),
                None => m,
            };
            (i, m)
        })
        .collect()
}

fn hides(c: &FakeChrome, tab: &str) -> Vec<String> {
    log_on(c, tab)
        .into_iter()
        .map(|(_, m)| m)
        .filter(|m| m.starts_with("Page.setWebLifecycleState"))
        .collect()
}

const HIDDEN: [&str; 2] = [
    "Page.setWebLifecycleState frozen",
    "Page.setWebLifecycleState active",
];

async fn wait_until(what: &str, c: &FakeChrome, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}; requests: {:?}",
            c.methods()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn showing_a_room_hides_every_other_visible_room_after_bringing_it_to_front() {
    let c = chrome().await;
    let addr = serve(Cdp::new(c.addr.clone()), ShownTabs::default()).await;
    let _viewer = view(addr, "T1").await;
    wait_until("T2 and T3 were never hidden", &c, || {
        hides(&c, "T2") == HIDDEN && hides(&c, "T3") == HIDDEN
    })
    .await;
    assert!(
        hides(&c, "T4").is_empty(),
        "a hidden room is not frozen again"
    );
    assert!(hides(&c, "T1").is_empty(), "the shown room stays visible");

    let shown = log_on(&c, "T1");
    let pos = |m: &str| shown.iter().find(|(_, x)| x == m).map(|(i, _)| *i);
    let front = pos("Page.bringToFront").expect("the shown room is brought to front");
    let focus = pos("Emulation.setFocusEmulationEnabled").expect("and kept visible");
    assert!(front < focus);
    let focus_params = c
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|(p, r)| p == "page/T1" && r["method"] == "Emulation.setFocusEmulationEnabled")
        .map(|(_, r)| r["params"].clone());
    assert_eq!(focus_params, Some(json!({"enabled": true})));
    // Activation re-shows the window it leaves: the others are hidden only
    // after the shown room's window was activated.
    let first_hide_check = log_on(&c, "T2").first().map(|(i, _)| *i).unwrap();
    assert!(front < first_hide_check);
}

#[tokio::test]
async fn a_room_another_viewer_watches_is_never_hidden() {
    let c = chrome().await;
    let shown = ShownTabs::default();
    let addr = serve(Cdp::new(c.addr.clone()), shown.clone()).await;
    let _on_t2 = view(addr, "T2").await;
    wait_until("the first viewer never got T2", &c, || shown.is_shown("T2")).await;
    let _on_t1 = view(addr, "T1").await;
    wait_until("T3 was never hidden", &c, || {
        hides(&c, "T3") == HIDDEN && shown.is_shown("T1")
    })
    .await;
    // Both passes are over (T3 hidden once: the second pass found it hidden).
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        hides(&c, "T2").is_empty(),
        "T2 is on the other viewer's screen"
    );
    assert_eq!(hides(&c, "T3"), HIDDEN);
}

#[tokio::test]
async fn the_room_a_viewer_leaves_is_hidden_unless_another_viewer_still_watches_it() {
    let c = chrome().await;
    let shown = ShownTabs::default();
    let addr = serve(Cdp::new(c.addr.clone()), shown.clone()).await;
    let mut first = view(addr, "T1").await;
    wait_until("T2 was never hidden", &c, || hides(&c, "T2") == HIDDEN).await;
    // The second viewer's pass finds T2 hidden and leaves it alone.
    let mut second = view(addr, "T1").await;
    wait_until("the second viewer never attached", &c, || {
        log_on(&c, "T1")
            .iter()
            .filter(|(_, m)| m == "Emulation.setFocusEmulationEnabled")
            .count()
            == 2
    })
    .await;

    first.close(None).await.unwrap();
    while first.next().await.is_some() {}
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        hides(&c, "T1").is_empty(),
        "the second viewer still watches T1"
    );

    second.send(Message::Close(None)).await.unwrap();
    while second.next().await.is_some() {}
    wait_until("the room nobody watches was never hidden", &c, || {
        hides(&c, "T1") == HIDDEN
    })
    .await;
}

#[tokio::test]
async fn the_other_rooms_wait_for_the_shown_room_to_be_in_front() {
    let c = chrome().await;
    let gate = Arc::new(tokio::sync::Notify::new());
    c.state.lock().unwrap().front_gate = Some(Arc::clone(&gate));
    let addr = serve(Cdp::new(c.addr.clone()), ShownTabs::default()).await;
    let _viewer = view(addr, "T1").await;
    wait_until("the room was never brought to front", &c, || {
        log_on(&c, "T1")
            .iter()
            .any(|(_, m)| m == "Page.bringToFront")
    })
    .await;
    // Every other reply of the attach went out; the bringToFront one did not.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        log_on(&c, "T2").is_empty(),
        "nothing is hidden before the shown room's window is active"
    );
    gate.notify_one();
    wait_until("T2 was never hidden", &c, || hides(&c, "T2") == HIDDEN).await;
}
