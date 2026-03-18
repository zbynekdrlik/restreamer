use axum::Router;
use axum::http::{Method, header};
use axum::routing::{delete, get, patch, post, put};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;
use crate::websocket;
use crate::youtube;

/// Build the Axum router with all API routes.
pub fn build_router(state: AppState) -> Router {
    let api = Router::new()
        // Core status/health
        .route("/health", get(handlers::health))
        .route("/status", get(handlers::get_status))
        .route("/streaming-event", get(handlers::get_streaming_event))
        .route("/streaming-event", delete(handlers::delete_streaming_event))
        .route("/chunks", get(handlers::get_chunks))
        .route("/chunks/stats", get(handlers::get_chunk_stats))
        .route("/chunks", delete(handlers::delete_chunks))
        // Actions
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
        // Config
        .route("/config", get(handlers::get_config))
        .route("/config", patch(handlers::patch_config))
        // Logs
        .route("/logs/inpoint", get(handlers::get_logs_inpoint))
        .route("/logs/endpoint", get(handlers::get_logs_endpoint))
        // WebSocket
        .route("/ws", get(websocket::ws_handler))
        // Events CRUD
        .route("/events", get(handlers::list_events))
        .route("/events", post(handlers::create_event))
        .route("/events/{id}", get(handlers::get_event_by_id))
        .route("/events/{id}", delete(handlers::delete_event_by_id))
        .route("/events/{id}", patch(handlers::update_event))
        .route("/events/{id}/activate", post(handlers::activate_event))
        .route(
            "/events/{id}/start-delivering",
            post(handlers::start_delivering),
        )
        .route("/events/{id}/deactivate", post(handlers::deactivate_event))
        .route("/events/{id}/start-stream", post(handlers::start_stream))
        .route("/events/{id}/stop-stream", post(handlers::stop_stream))
        .route("/events/{id}/endpoints", get(handlers::get_event_endpoints))
        .route(
            "/events/{event_id}/endpoints/{endpoint_id}",
            post(handlers::attach_endpoint_to_event),
        )
        .route(
            "/events/{event_id}/endpoints/{endpoint_id}",
            delete(handlers::detach_endpoint_from_event),
        )
        // Endpoint Configs CRUD
        .route("/endpoints", get(handlers::list_endpoints))
        .route("/endpoints", post(handlers::create_endpoint))
        .route("/endpoints/{id}", get(handlers::get_endpoint_by_id))
        .route("/endpoints/{id}", put(handlers::update_endpoint))
        .route("/endpoints/{id}", delete(handlers::delete_endpoint))
        // Delivery orchestration
        .route("/delivery/start", post(handlers::delivery_start))
        .route("/delivery/status", get(handlers::delivery_status))
        .route(
            "/delivery/status/cached",
            get(handlers::delivery_status_cached),
        )
        .route("/delivery/stop", post(handlers::delivery_stop))
        .route(
            "/delivery/instances",
            get(handlers::list_delivery_instances),
        )
        // YouTube
        .route("/youtube/status", get(youtube::youtube_status))
        .route("/youtube/oauth/seed", post(youtube::youtube_oauth_seed))
        .route("/youtube/oauth/start", get(youtube::youtube_oauth_start))
        .route(
            "/youtube/oauth/callback",
            get(youtube::youtube_oauth_callback),
        );

    // Allow any origin so the dashboard is accessible from LAN devices
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::PATCH,
            Method::PUT,
        ])
        .allow_headers([header::CONTENT_TYPE, header::ACCEPT]);

    let mut router = Router::new()
        .nest("/api/v1", api)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    // Serve the WASM frontend from the www_dir if configured,
    // so LAN browsers can access the dashboard at http://<host>:8910/
    if let Some(www_dir) = &state.www_dir {
        use tower_http::services::{ServeDir, ServeFile};
        let index = www_dir.join("index.html");
        let serve = ServeDir::new(www_dir).fallback(ServeFile::new(index));
        router = router.fallback_service(serve);
    }

    router
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
    async fn start_stream_sets_both_flags() {
        let state = test_state().await;
        // Create an event
        db::create_streaming_event(&state.pool, "test-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;

        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/events/{event_id}/start-stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify DB state
        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert!(evt.receiving_activated);
        assert!(evt.delivering_activated);
    }

    #[tokio::test]
    async fn start_stream_conflict_with_active_event() {
        let state = test_state().await;
        // Create two events
        db::create_streaming_event(&state.pool, "event-1")
            .await
            .unwrap();
        db::create_streaming_event(&state.pool, "event-2")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let id1 = events[0].id;
        let id2 = events[1].id;

        // Start first event
        db::update_streaming_event_flags(&state.pool, id1, true, true)
            .await
            .unwrap();

        let app = build_router(state);

        // Try to start second event — should conflict
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/events/{id2}/start-stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn stop_stream_deactivates_both_flags() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "test-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;

        // Activate both flags
        db::update_streaming_event_flags(&state.pool, event_id, true, true)
            .await
            .unwrap();

        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/events/{event_id}/stop-stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify DB state — both flags should be false
        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!evt.receiving_activated);
        assert!(!evt.delivering_activated);
    }

    #[tokio::test]
    async fn start_stop_stream_full_cycle() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "cycle-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;
        let app = build_router(state.clone());

        // Start
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/events/{event_id}/start-stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert!(evt.receiving_activated);
        assert!(evt.delivering_activated);

        // Stop
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/events/{event_id}/stop-stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!evt.receiving_activated);
        assert!(!evt.delivering_activated);
    }

    #[tokio::test]
    async fn update_event_sets_cache_delay() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "delay-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;

        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&format!("/api/v1/events/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "cache_delay_secs": 300 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(evt.cache_delay_secs, Some(300));
    }

    #[tokio::test]
    async fn update_event_preserves_cache_delay_when_omitted() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "preserve-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;

        // First set a cache delay
        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&format!("/api/v1/events/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "cache_delay_secs": 180 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Now update only name (cache_delay_secs omitted)
        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&format!("/api/v1/events/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "name": "Renamed Event" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify cache_delay_secs was preserved
        let evt = db::get_streaming_event_by_id(&state.pool, event_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(evt.name, "Renamed Event");
        assert_eq!(evt.cache_delay_secs, Some(180));
    }

    #[tokio::test]
    async fn delivery_start_returns_503_without_hetzner_token() {
        let state = test_state().await;
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
    async fn youtube_status_returns_503_without_hetzner_token() {
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

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
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

        // Verify tokens stored
        let oauth = rs_core::db::get_youtube_oauth(&state.pool)
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
}

#[cfg(test)]
mod youtube_oauth_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rs_core::config::Config;
    use rs_core::db;
    use rs_core::models::WsEvent;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    fn yt_config() -> Config {
        let mut c = Config::for_testing();
        c.youtube.client_id = "yt-cid-for-test".into();
        c.youtube.client_secret = "yt-cs-for-test".into();
        c
    }

    #[tokio::test]
    async fn oauth_start_returns_url() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, yt_config(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/youtube/oauth/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let val: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let url = val["url"].as_str().unwrap();
        assert!(url.contains("yt-cid-for-test"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("access_type=offline"));
    }

    #[tokio::test]
    async fn oauth_start_no_creds_returns_400() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/youtube/oauth/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn oauth_callback_no_code_returns_400() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, yt_config(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/youtube/oauth/callback")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn oauth_callback_error_param_returns_html() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, yt_config(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/youtube/oauth/callback?error=access_denied")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("Authorization Failed"));
    }
}
