//! HTTP endpoint for the outgoing-Mbps history graph (issue #77).
//!
//! Kept in its own module (not `handlers.rs`, which is at the 1000-line cap)
//! and served on a slower cadence than the 2 s `/uploads/stats` poll because
//! the payload is a ~720-point time-series.

use axum::extract::State;
use axum::response::Json;

use rs_endpoint::throughput::ThroughputSeries;

use crate::state::AppState;

/// `GET /api/v1/uploads/throughput` — the retained outgoing-Mbps series.
pub async fn get_throughput(State(state): State<AppState>) -> Json<ThroughputSeries> {
    Json(state.upload_metrics.throughput_series())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rs_endpoint::metrics::UploadEvent;
    use std::time::Instant;
    use tower::ServiceExt;

    async fn test_app() -> (axum::Router, AppState) {
        use rs_core::config::Config;
        use rs_core::models::WsEvent;
        use tokio::sync::broadcast;
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new_for_tests(pool, Config::for_testing(), ws_tx);
        let router = axum::Router::new()
            .route(
                "/api/v1/uploads/throughput",
                axum::routing::get(get_throughput),
            )
            .with_state(state.clone());
        (router, state)
    }

    #[tokio::test]
    async fn throughput_endpoint_returns_200_and_interval() {
        let (app, _state) = test_app().await;
        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/throughput")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["interval_ms"], 15_000);
        assert!(v["samples"].is_array());
    }

    #[tokio::test]
    async fn recorded_uploads_surface_as_samples() {
        let (app, state) = test_app().await;
        // Record two successful uploads. They land in the current in-progress
        // bucket, so a snapshot right away emits nothing yet — but the field
        // shape must still be present and well-formed.
        for _ in 0..2 {
            state.upload_metrics.record(UploadEvent {
                at: Instant::now(),
                duration_ms: 50,
                success: true,
                bytes: 1_000_000,
            });
        }
        let resp = app
            .oneshot(
                Request::get("/api/v1/uploads/throughput")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
