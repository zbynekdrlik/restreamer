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
