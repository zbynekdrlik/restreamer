//! HTTP endpoints for upload telemetry (issue #118 + #65).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;
use std::time::Duration;

use rs_core::db;
use rs_core::models::UploadChunkRow;
use rs_endpoint::metrics::Snapshot;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct RecentQuery {
    limit: Option<i64>,
}

pub async fn get_uploads_stats(State(state): State<AppState>) -> Json<Snapshot> {
    // Permanent failure escalation window: count chunks marked permanent
    // whose first attempt fired in the last 5 minutes. Anything older is
    // historical and should not keep the dashboard strip red.
    let now_ms = chrono::Utc::now().timestamp_millis();
    let since_ms = now_ms - 5 * 60 * 1000;
    let permanent_recent = db::count_permanently_failed_since(&state.pool, since_ms)
        .await
        .unwrap_or(0)
        .max(0) as u32;
    state.upload_metrics.set_permanent_recent(permanent_recent);
    let snap = state.upload_metrics.snapshot(Duration::from_secs(60));
    Json(snap)
}

pub async fn get_recent_uploads(
    State(state): State<AppState>,
    Query(q): Query<RecentQuery>,
) -> Result<Json<Vec<UploadChunkRow>>, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let rows = db::list_recent_uploads(&state.pool, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Build a router with just the two upload endpoints against an in-memory state.
    async fn test_app() -> axum::Router {
        use rs_core::config::Config;
        use rs_core::models::WsEvent;
        use tokio::sync::broadcast;
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new_for_tests(pool, Config::for_testing(), ws_tx);
        axum::Router::new()
            .route(
                "/api/v1/uploads/stats",
                axum::routing::get(get_uploads_stats),
            )
            .route(
                "/api/v1/uploads/recent",
                axum::routing::get(get_recent_uploads),
            )
            .with_state(state)
    }

    #[tokio::test]
    async fn uploads_stats_returns_200_with_snapshot_shape() {
        let app = test_app().await;
        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("chunks_per_sec").is_some());
        assert!(v.get("median_ms").is_some());
        assert!(v.get("p95_ms").is_some());
        assert!(v.get("error_rate").is_some());
        assert!(v.get("in_flight").is_some());
        assert!(v.get("adaptive_target").is_some());
        // Issue #168: state classifier + server-rendered visuals must
        // be present in every snapshot so the leptos-ui can render
        // without re-implementing match arms client-side.
        assert!(v.get("permanent_recent").is_some());
        let state = v.get("state").expect("state field");
        assert!(state.get("kind").is_some(), "state must be a tagged enum");
        let render = v.get("render").expect("render field");
        assert!(render.get("class").is_some());
        assert!(render.get("label").is_some());
        assert!(render.get("tooltip").is_some());
    }

    #[tokio::test]
    async fn uploads_recent_empty_returns_empty_array() {
        let app = test_app().await;
        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn uploads_recent_clamps_high_limit() {
        let app = test_app().await;
        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/recent?limit=99999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn uploads_recent_returns_inserted_rows_not_empty_vec() {
        // Kills the mutant that returns Ok(Json::from(vec![])) regardless of DB.
        use rs_core::config::Config;
        use rs_core::models::WsEvent;
        use tokio::sync::broadcast;

        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        rs_core::db::upsert_client_profile(&pool, "test-uuid")
            .await
            .unwrap();
        let event_id = rs_core::db::upsert_streaming_event(&pool, "evt-ep")
            .await
            .unwrap();
        rs_core::db::insert_chunk(&pool, event_id, "/tmp/ep.bin", 100, "mep", 2000)
            .await
            .unwrap();

        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new_for_tests(pool, Config::for_testing(), ws_tx);
        let app = axum::Router::new()
            .route(
                "/api/v1/uploads/recent",
                axum::routing::get(get_recent_uploads),
            )
            .with_state(state);

        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = v.as_array().expect("response must be an array");
        assert_eq!(
            arr.len(),
            1,
            "must return the 1 inserted chunk, not an empty array"
        );
    }
}
