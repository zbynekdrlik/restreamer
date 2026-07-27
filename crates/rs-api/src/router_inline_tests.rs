use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::WsEvent;
use tokio::sync::{broadcast, mpsc};
use tower::ServiceExt;

async fn test_state() -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    AppState::new_for_tests(pool, config, ws_tx)
}

#[tokio::test]
async fn health_returns_ok() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn status_returns_json() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn streaming_event_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/streaming-event")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(text, "null");
}

#[tokio::test]
async fn chunks_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/chunks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(text, "[]");
}

#[tokio::test]
async fn chunk_stats_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/chunks/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let stats: rs_core::models::ChunkStats = serde_json::from_slice(&body).unwrap();
    assert_eq!(stats.total_chunks, 0);
}

#[tokio::test]
async fn delete_streaming_event_no_content() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/streaming-event")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn config_returns_json_with_redacted_credentials() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let config: rs_core::config::Config = serde_json::from_slice(&body).unwrap();
    assert_eq!(config.client_uuid, "test-uuid-00000000");
    // S3 credentials must be redacted
    assert_eq!(config.s3.access_key_id, "***");
    assert_eq!(config.s3.secret_access_key, "***");
    // Non-sensitive S3 fields remain intact
    assert_eq!(config.s3.bucket, "test-bucket");
}

#[tokio::test]
async fn toggle_receiving_ok() {
    let state = test_state().await;

    // Create a streaming event first
    db::upsert_streaming_event(&state.pool, "evt-1")
        .await
        .unwrap();

    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/toggle-receiving")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn restart_inpoint_without_channel_returns_503() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/restart-inpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn restart_endpoint_without_channel_returns_503() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/restart-endpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn restart_inpoint_with_channel_returns_ok() {
    let mut state = test_state().await;
    let (tx, mut rx) = mpsc::channel(1);
    state.inpoint_restart_tx = Some(tx);
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/restart-inpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // Verify the signal was actually sent
    let msg = rx.try_recv();
    assert!(msg.is_ok());
}

#[tokio::test]
async fn restart_endpoint_with_channel_returns_ok() {
    let mut state = test_state().await;
    let (tx, mut rx) = mpsc::channel(1);
    state.endpoint_restart_tx = Some(tx);
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/restart-endpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let msg = rx.try_recv();
    assert!(msg.is_ok());
}

#[tokio::test]
async fn chunks_pagination_caps_limit() {
    let state = test_state().await;
    let app = build_router(state);

    // Request with excessively high limit — should still succeed (capped internally)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/chunks?limit=999999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn toggle_receiving_no_event_returns_404() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/toggle-receiving")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn toggle_delivering_no_event_returns_404() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/actions/toggle-delivering")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_config_updates_field() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "client_uuid": "updated-uuid-12345678"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let config: rs_core::config::Config = serde_json::from_slice(&body).unwrap();
    assert_eq!(config.client_uuid, "updated-uuid-12345678");
    // Credentials should be redacted
    assert_eq!(config.s3.access_key_id, "***");
}

#[tokio::test]
async fn patch_config_rejects_invalid() {
    let state = test_state().await;
    let app = build_router(state);

    // Empty client_uuid should fail validation
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "client_uuid": "" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_config_preserves_redacted_credentials() {
    let state = test_state().await;
    let app = build_router(state);

    // Send "***" for credentials — original values should be preserved (not saved as ***)
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "s3": {
                            "bucket": "test-bucket",
                            "region": "us-east-1",
                            "endpoint": "http://localhost:9000",
                            "access_key_id": "***",
                            "secret_access_key": "***"
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should succeed (the original creds pass validation)
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn patch_config_saves_to_disk() {
    let mut state = test_state().await;
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.json");

    // Save initial config so the file exists
    state.config.save(&config_path).unwrap();
    state.config_path = Some(config_path.clone());

    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "client_uuid": "saved-uuid-12345678"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify the file was written
    let saved = rs_core::config::Config::load(&config_path).unwrap();
    assert_eq!(saved.client_uuid, "saved-uuid-12345678");
}

#[tokio::test]
async fn get_logs_inpoint_returns_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/inpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let logs: handlers::LogsResponse = serde_json::from_slice(&body).unwrap();
    assert!(logs.entries.is_empty());
}

#[tokio::test]
async fn get_logs_endpoint_returns_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/endpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let logs: handlers::LogsResponse = serde_json::from_slice(&body).unwrap();
    assert!(logs.entries.is_empty());
}

#[tokio::test]
async fn get_logs_with_populated_buffer() {
    let mut state = test_state().await;
    let buffer = LogBuffer::new(100);
    buffer.push(rs_core::log_buffer::LogEntry {
        level: "INFO".into(),
        target: "rs_inpoint::rtmp".into(),
        message: "RTMP server started".into(),
    });
    buffer.push(rs_core::log_buffer::LogEntry {
        level: "WARN".into(),
        target: "rs_endpoint::s3".into(),
        message: "Upload retry".into(),
    });
    state.log_buffer = buffer;

    let app = build_router(state);

    // Check inpoint logs
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/inpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let logs: handlers::LogsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(logs.entries.len(), 1);
    assert!(logs.entries[0].message.contains("RTMP"));

    // Check endpoint logs
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/endpoint")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let logs: handlers::LogsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(logs.entries.len(), 1);
    assert!(logs.entries[0].message.contains("Upload"));
}

#[tokio::test]
async fn delivery_start_returns_503_without_hetzner_token() {
    let state = test_state().await;
    // Satisfy the RTMP-stable gate so we reach the Hetzner-missing check.
    *state.rtmp_stable_since.lock().await = Some(
        std::time::Instant::now()
            - std::time::Duration::from_secs(
                crate::delivery_handlers::RTMP_STABLE_REQUIRED_SECS + 5,
            ),
    );
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/delivery/start")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"event_id": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn delivery_status_returns_503_without_hetzner_token() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/delivery/status?event_id=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn delivery_stop_returns_503_without_hetzner_token() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/delivery/stop")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"event_id": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn youtube_status_returns_empty_array_without_tokens() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/youtube/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Legacy single-channel shape — CI gate consumer relies on this.
    // No refresh_token on the v25-seeded default row -> authenticated=false.
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["authenticated"], false);
    assert_eq!(v["stream_count"], 0);
}

#[tokio::test]
async fn youtube_oauth_seed_stores_tokens() {
    let state = test_state().await;
    let app = build_router(state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/youtube/oauth/seed")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "label": "default",
                        "refresh_token": "test-refresh",
                        "client_id": "test-client",
                        "client_secret": "test-secret"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify tokens stored via the label-aware API.
    let oauth = rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, "default")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(oauth.refresh_token, "test-refresh");
    assert_eq!(oauth.client_id, "test-client");
}

#[tokio::test]
async fn delivery_instances_list_returns_empty() {
    let state = test_state().await;
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/delivery/instances")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let instances: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert!(instances.is_empty());
}
