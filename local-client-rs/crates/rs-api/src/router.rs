use axum::Router;
use axum::http::{HeaderValue, Method, header};
use axum::routing::{delete, get, patch, post};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;
use crate::websocket;

/// Build the Axum router with all API routes.
pub fn build_router(state: AppState) -> Router {
    let api = Router::new()
        .route("/health", get(handlers::health))
        .route("/status", get(handlers::get_status))
        .route("/streaming-event", get(handlers::get_streaming_event))
        .route("/streaming-event", delete(handlers::delete_streaming_event))
        .route("/chunks", get(handlers::get_chunks))
        .route("/chunks/stats", get(handlers::get_chunk_stats))
        .route("/chunks", delete(handlers::delete_chunks))
        .route(
            "/actions/restart-inpoint",
            post(handlers::action_restart_inpoint),
        )
        .route(
            "/actions/restart-endpoint",
            post(handlers::action_restart_endpoint),
        )
        .route(
            "/actions/toggle-receiving",
            post(handlers::action_toggle_receiving),
        )
        .route(
            "/actions/toggle-delivering",
            post(handlers::action_toggle_delivering),
        )
        .route("/config", get(handlers::get_config))
        .route("/config", patch(handlers::patch_config))
        .route("/logs/inpoint", get(handlers::get_logs_inpoint))
        .route("/logs/endpoint", get(handlers::get_logs_endpoint))
        .route("/ws", get(websocket::ws_handler));

    let cors = CorsLayer::new()
        .allow_origin([
            "http://localhost:5173".parse::<HeaderValue>().unwrap(),
            "tauri://localhost".parse::<HeaderValue>().unwrap(),
            "https://tauri.localhost".parse::<HeaderValue>().unwrap(),
        ])
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::PATCH])
        .allow_headers([header::CONTENT_TYPE, header::ACCEPT]);

    Router::new()
        .nest("/api/v1", api)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
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
        AppState::new(pool, config, ws_tx)
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
        db::upsert_streaming_event(&state.pool, "evt-1", Some("Test"), "127.0.0.1")
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
                            "manager_url": "https://new-manager.example.com"
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
        assert_eq!(config.manager_url, "https://new-manager.example.com");
        // Other fields should be preserved from the original config
        assert_eq!(config.client_uuid, "test-uuid-00000000");
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
                            "manager_url": "https://saved-manager.example.com"
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
        assert_eq!(saved.manager_url, "https://saved-manager.example.com");
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
}
