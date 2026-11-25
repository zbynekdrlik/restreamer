//! Unit tests for `api.rs` (split out to keep it under the 1000-line cap).
use super::*;
use axum::body::Body;
use axum::http::Request;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

const TEST_TOKEN: &str = "test-secret-token";

async fn test_state() -> Arc<AppState> {
    let mut state = AppState::new().await;
    state.auth_token = RwLock::new(Some(TEST_TOKEN.to_string()));
    Arc::new(state)
}

#[tokio::test]
async fn health_endpoint() {
    let app = router(test_state().await);
    let req = Request::builder()
        .uri("/api/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn status_endpoint_requires_auth() {
    let app = router(test_state().await);
    // Request without auth header should be rejected
    let req = Request::builder()
        .uri("/api/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn status_endpoint_empty() {
    let app = router(test_state().await);
    let req = Request::builder()
        .uri("/api/status")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["endpoint_count"], 0);
}

#[tokio::test]
async fn status_response_includes_diagnostic_fields() {
    let app = router(test_state().await);
    let req = Request::builder()
        .uri("/api/status")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // When there are no endpoints, the array is empty but the shape is correct
    assert!(json["endpoints"].is_array());
}

#[tokio::test]
async fn stop_endpoint_empty() {
    let app = router(test_state().await);
    let req = Request::builder()
        .method("POST")
        .uri("/api/stop")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::from(r#"{"alias": null}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn add_endpoint_returns_conflict_before_init() {
    let app = router(test_state().await);
    let body = serde_json::json!({
        "endpoint": {
            "alias": "test-yt",
            "service_type": "YT_RTMP",
            "stream_key": "fake-key"
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/endpoints/add")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn remove_endpoint_not_found() {
    let app = router(test_state().await);
    let body = serde_json::json!({ "alias": "nonexistent" });
    let req = Request::builder()
        .method("POST")
        .uri("/api/endpoints/remove")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {TEST_TOKEN}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["removed"], false);
}

#[tokio::test]
async fn get_logs_requires_auth() {
    let state = Arc::new(AppState::new().await);
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/logs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_logs_returns_entries() {
    let state = Arc::new(AppState::new().await);
    state.log_buffer.push(rs_core::log_buffer::LogEntry {
        level: "WARN".into(),
        target: "rs_delivery::endpoint_task".into(),
        message: "ffmpeg died".into(),
    });
    *state.auth_token.write().await = Some("test-token".to_string());
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/logs?limit=10")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let logs: LogsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(logs.entries.len(), 1);
    assert!(logs.entries[0].message.contains("ffmpeg died"));
}

/// #346 — the delivery VPS binary was reported as serving an unauthenticated
/// HTTP API on a public IP. It does not: every control/read route is behind
/// `require_auth` (a bearer token minted per instance by `DeliveryOrchestrator`,
/// shipped in a 0600 cloud-init env file, fail-closed when unset). This test
/// ENUMERATES those routes and pins that a request with NO bearer token is
/// refused with 401 — so a future addition cannot silently land a control or
/// read route outside the gate. (A token IS set on `test_state`; the point is
/// that the absence of the *header* is what is rejected.)
#[tokio::test]
async fn every_protected_route_requires_auth() {
    let protected: &[(&str, &str)] = &[
        ("POST", "/api/init"),
        ("GET", "/api/status"),
        ("POST", "/api/stop"),
        ("POST", "/api/endpoints/add"),
        ("POST", "/api/endpoints/remove"),
        ("POST", "/api/endpoints/update_start"),
        ("GET", "/api/logs"),
        ("POST", "/api/source"),
        ("POST", "/api/endpoints/fast_delay"),
    ];
    let mut reachable = Vec::new();
    for (method, path) in protected {
        let app = router(test_state().await);
        let req = Request::builder()
            .method(*method)
            .uri(*path)
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        if resp.status() != StatusCode::UNAUTHORIZED {
            reachable.push(format!("{method} {path} -> {}", resp.status()));
        }
    }
    assert!(
        reachable.is_empty(),
        "#346: these delivery routes answered without a bearer token: {reachable:#?}"
    );
}

/// #346 — the two deliberately-public routes must stay reachable with no
/// token: the host skew probe hits `/clock` and the liveness check hits
/// `/api/health` before `/api/init` sets the token. Neither can control or
/// read delivery state, so they carry no severity. Pinning this stops a
/// well-meaning "gate everything" change from breaking the orchestrator's
/// readiness poll.
#[tokio::test]
async fn public_routes_need_no_auth() {
    for path in ["/api/health", "/clock"] {
        let app = router(test_state().await);
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "#346: {path} must stay public"
        );
    }
}
