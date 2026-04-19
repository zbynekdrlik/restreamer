//! Additional router integration tests (stream control, events, delivery).
//! Split from router.rs to keep files under 1000 lines.

#[cfg(test)]
mod stream_tests {
    use crate::router::build_router;
    use crate::state::AppState;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rs_core::config::Config;
    use rs_core::db;
    use rs_core::models::WsEvent;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let config = Config::for_testing();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        AppState::new(pool, config, ws_tx)
    }

    #[tokio::test]
    async fn start_stream_sets_both_flags() {
        let state = test_state().await;
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
                    .uri(format!("/api/v1/events/{event_id}/start-stream"))
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
    }

    #[tokio::test]
    async fn start_stream_conflict_with_active_event() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "event-1")
            .await
            .unwrap();
        db::create_streaming_event(&state.pool, "event-2")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let id1 = events[0].id;
        let id2 = events[1].id;

        db::update_streaming_event_flags(&state.pool, id1, true, true)
            .await
            .unwrap();

        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/events/{id2}/start-stream"))
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

        db::update_streaming_event_flags(&state.pool, event_id, true, true)
            .await
            .unwrap();

        let app = build_router(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/events/{event_id}/stop-stream"))
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
    async fn start_stop_stream_full_cycle() {
        let state = test_state().await;
        db::create_streaming_event(&state.pool, "cycle-event")
            .await
            .unwrap();
        let events = db::list_streaming_events(&state.pool).await.unwrap();
        let event_id = events[0].id;
        let app = build_router(state.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/events/{event_id}/start-stream"))
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

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/events/{event_id}/stop-stream"))
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
                    .uri(format!("/api/v1/events/{event_id}"))
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

        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/events/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "cache_delay_secs": 180 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/events/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "name": "Renamed Event" }).to_string(),
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
        assert_eq!(evt.name, "Renamed Event");
        assert_eq!(evt.cache_delay_secs, Some(180));
    }
}

#[cfg(test)]
mod obs_tests {
    use crate::router::build_router;
    use crate::state::AppState;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rs_core::config::Config;
    use rs_core::db;
    use rs_core::models::WsEvent;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    #[tokio::test]
    async fn obs_status_returns_503_when_disabled() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/obs/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn obs_start_stream_returns_503_when_disabled() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/obs/start-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn obs_stop_stream_returns_503_when_disabled() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx);
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/obs/stop-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
mod youtube_oauth_tests {
    use crate::router::build_router;
    use crate::state::AppState;
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

    async fn test_state_with_yt() -> AppState {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        AppState::new(pool, yt_config(), ws_tx)
    }

    async fn test_state_no_yt() -> AppState {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        AppState::new(pool, Config::for_testing(), ws_tx)
    }

    #[tokio::test]
    async fn oauth_start_returns_url() {
        let state = test_state_with_yt().await;
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
        let state = test_state_no_yt().await;
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
        let state = test_state_with_yt().await;
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
        let state = test_state_with_yt().await;
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

#[cfg(test)]
mod audit_tests {
    use crate::router::build_router;
    use crate::state::AppState;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rs_core::config::Config;
    use rs_core::db;
    use rs_core::models::WsEvent;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let config = Config::for_testing();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        AppState::new(pool, config, ws_tx)
    }

    async fn body_to_bytes(body: Body) -> Vec<u8> {
        axum::body::to_bytes(body, 1024 * 1024)
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn audit_list_returns_empty_on_fresh_db() {
        let state = test_state().await;
        let app = build_router(state);

        let req = Request::builder()
            .uri("/api/v1/audit")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_to_bytes(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rows"].as_array().unwrap().len(), 0);
        assert_eq!(json["total"], 0);
    }

    #[tokio::test]
    async fn audit_list_returns_inserted_rows() {
        let state = test_state().await;
        sqlx::query("INSERT INTO audit_log (severity, source, action, detail) VALUES ('info','operator','event_started','{\"n\":1}')")
            .execute(&state.pool)
            .await
            .unwrap();

        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_to_bytes(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rows"].as_array().unwrap().len(), 1);
        assert_eq!(json["rows"][0]["action"], "event_started");
    }

    #[tokio::test]
    async fn audit_get_by_id_returns_row() {
        let state = test_state().await;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO audit_log (severity, source, action, detail) VALUES ('error','ffmpeg','endpoint_ffmpeg_died','{\"reason_class\":\"youtube_rtmp_closed\"}') RETURNING id"
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();

        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/audit/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_to_bytes(resp.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], id);
        assert_eq!(json["detail"]["reason_class"], "youtube_rtmp_closed");
    }

    #[tokio::test]
    async fn audit_get_by_id_returns_404_on_missing() {
        let state = test_state().await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/audit/9999999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
