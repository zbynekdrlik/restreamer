use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::config::Config;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;
use tower::ServiceExt;

use crate::control_room::ControlRooms;
use crate::control_room::browser::Browser;
use crate::router::build_router;
use crate::state::AppState;

use super::*;

/// State whose control rooms live in `dir` and whose Chrome is "not running"
/// (its debugging port points at a free port).
async fn state(dir: &std::path::Path) -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let free = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    AppState::new_for_tests(pool, Config::for_testing(), ws_tx).with_control_rooms(Arc::new(
        ControlRooms::with_browser(dir, Browser::at(dir, free)),
    ))
}

async fn call(
    state: &AppState,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, String) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = build_router(state.clone())
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn without_an_active_event_every_room_endpoint_is_listed() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    db::create_endpoint_config(&s.pool, "YT NLCH 4K", "YT_RTMP", "k1", false)
        .await
        .unwrap();
    db::create_endpoint_config(&s.pool, "e2e file", "TEST_FILE", "k2", false)
        .await
        .unwrap();
    db::create_endpoint_config(&s.pool, "FB-GoodFest", "FB", "k3", false)
        .await
        .unwrap();
    let (status, body) = call(&s, "GET", "/api/v1/control-rooms", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["event"].is_null());
    let aliases: Vec<&str> = v["destinations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["alias"].as_str().unwrap())
        .collect();
    assert_eq!(aliases, vec!["YT NLCH 4K", "FB-GoodFest"]);
    assert!(
        !body.contains("k1") && !body.contains("k3"),
        "stream keys must never be served"
    );
}

#[tokio::test]
async fn with_an_active_event_only_its_endpoints_are_listed() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    let a = db::create_endpoint_config(&s.pool, "YT A", "YT_RTMP", "k1", false)
        .await
        .unwrap();
    db::create_endpoint_config(&s.pool, "YT B", "YT_RTMP", "k2", false)
        .await
        .unwrap();
    let ev = db::upsert_streaming_event(&s.pool, "Sunday").await.unwrap();
    db::attach_endpoint_to_event(&s.pool, ev, a).await.unwrap();
    let (status, body) = call(&s, "GET", "/api/v1/control-rooms", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["event"]["name"], "Sunday");
    assert_eq!(v["destinations"].as_array().unwrap().len(), 1);
    assert_eq!(v["destinations"][0]["alias"], "YT A");
    assert!(v["destinations"][0]["target_id"].is_null());
}

#[tokio::test]
async fn account_is_stored_box_locally_and_shown() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    let id = db::create_endpoint_config(&s.pool, "YT A", "YT_RTMP", "k1", false)
        .await
        .unwrap();
    let acct = serde_json::json!({"account": "a@x.com", "channel": "UC1", "sure": true});
    let (status, _) = call(
        &s,
        "PUT",
        &format!("/api/v1/control-rooms/{id}/account"),
        Some(acct),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let file = std::fs::read_to_string(dir.path().join("control-rooms.json")).unwrap();
    assert!(file.contains("a@x.com"));
    let (_, body) = call(&s, "GET", "/api/v1/control-rooms", None).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["destinations"][0]["account"], "a@x.com");
    assert_eq!(v["destinations"][0]["profile"], format!("dest-{id}"));
    assert_eq!(
        v["destinations"][0]["url"],
        "https://studio.youtube.com/channel/UC1/livestreaming"
    );

    let (status, _) = call(
        &s,
        "PUT",
        "/api/v1/control-rooms/999/account",
        Some(serde_json::json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn open_rejects_unknown_and_roomless_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    let f = db::create_endpoint_config(&s.pool, "e2e file", "TEST_FILE", "k", false)
        .await
        .unwrap();
    let (status, _) = call(&s, "POST", "/api/v1/control-rooms/999/open", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&s, "POST", &format!("/api/v1/control-rooms/{f}/open"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ws_without_chrome_is_unavailable_not_a_hang() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    let req = Request::builder()
        .uri("/api/v1/control-rooms/ws?target=T1")
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(Body::empty())
        .unwrap();
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        build_router(s).oneshot(req),
    )
    .await
    .expect("hung")
    .unwrap();
    // Without a real hyper connection axum may refuse the upgrade itself;
    // either way it must not be a 101 into a dead browser.
    assert_ne!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}

#[tokio::test]
async fn a_db_failure_is_a_500_not_an_empty_list() {
    let dir = tempfile::tempdir().unwrap();
    let s = state(dir.path()).await;
    s.pool.close().await;
    let (status, _) = call(&s, "GET", "/api/v1/control-rooms", None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn ws_relays_a_live_tab_and_refuses_an_unknown_one() {
    use crate::control_room::fake_chrome::{FakeChrome, page};
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let dir = tempfile::tempdir().unwrap();
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![page("T1", "C1", "https://studio.youtube.com/x")]);
    let s = state(dir.path())
        .await
        .with_control_rooms(Arc::new(ControlRooms::with_browser(
            dir.path(),
            Browser::at(dir.path(), chrome.addr.clone()),
        )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, build_router(s)).await.unwrap() });

    let unknown = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/api/v1/control-rooms/ws?target=nope"
    ))
    .await;
    assert!(unknown.is_err(), "an unknown tab must not be upgraded");

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/api/v1/control-rooms/ws?target=T1"))
            .await
            .unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(t) = first else {
        panic!("expected the url message")
    };
    let v: serde_json::Value = serde_json::from_str(&t).unwrap();
    assert_eq!(v["url"], "https://studio.youtube.com/x");
    let click = serde_json::json!({"t": "m", "type": "mousePressed", "x": 1, "y": 2, "button": "left", "clickCount": 1});
    ws.send(Message::Text(click.to_string().into()))
        .await
        .unwrap();
    let got = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if chrome
                .methods()
                .iter()
                .any(|m| m == "Input.dispatchMouseEvent")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        got.is_ok(),
        "the click never reached the tab: {:?}",
        chrome.methods()
    );
}

/// #399 review: a thumbnail near the 20 MB cap travels base64 (~27 MB) in ONE
/// WebSocket frame. The room route must accept a frame that big, or the whole
/// room connection drops with no file-error (tungstenite's frame default is 16 MiB).
#[tokio::test]
async fn the_room_route_takes_a_20_mb_file_in_one_frame() {
    use crate::control_room::fake_chrome::{FakeChrome, page};
    use base64::Engine;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let dir = tempfile::tempdir().unwrap();
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![page("T1", "C1", "https://studio.youtube.com/x")]);
    let s = state(dir.path())
        .await
        .with_control_rooms(Arc::new(ControlRooms::with_browser(
            dir.path(),
            Browser::at(dir.path(), chrome.addr.clone()),
        )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, build_router(s)).await.unwrap() });
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/api/v1/control-rooms/ws?target=T1"))
            .await
            .unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(vec![7u8; 20 * 1024 * 1024]);
    let msg = serde_json::json!({"t": "file", "name": "big.png", "b64": b64, "x": 1, "y": 2});
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .unwrap();
    let done = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(Ok(m)) = ws.next().await {
            if let Message::Text(t) = m {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v["t"] == "file-done" || v["t"] == "file-error" {
                    return Some(v);
                }
            }
        }
        None
    })
    .await
    .expect("no answer to the file within 30 s");
    let done = done.expect("the room connection dropped on a 20 MB file");
    assert_eq!(done["t"], "file-done", "{done}");
    let stored: Vec<_> = std::fs::read_dir(dir.path().join("browser-profiles").join("uploads"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].metadata().unwrap().len(), 20 * 1024 * 1024);
}

struct NoDialog;

impl rs_core::native_picker::NativeFilePicker for NoDialog {
    fn pick_file(&self) -> rs_core::native_picker::Pick {
        Box::pin(async { None })
    }
}

/// #414: the box's dialog opens only for a viewer on the box itself.
#[test]
fn only_a_viewer_on_the_box_gets_the_box_dialog() {
    let dialog: Arc<dyn rs_core::native_picker::NativeFilePicker> = Arc::new(NoDialog);
    assert!(viewer_picker(true, Some(Arc::clone(&dialog))).is_some());
    assert!(
        viewer_picker(false, Some(dialog)).is_none(),
        "a notebook or the tunnel must never open a dialog on the box's screen"
    );
    assert!(viewer_picker(true, None).is_none(), "no GUI, no dialog");
}
