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
    async fn recorded_upload_surfaces_as_a_finalized_sample() {
        let (app, state) = test_app().await;
        // Record a 1.875 MB success TWO intervals in the past so the
        // real-clock endpoint snapshot finalizes it into a completed bucket.
        let interval = rs_endpoint::throughput::SAMPLE_INTERVAL_MS;
        let past = chrono::Utc::now().timestamp_millis() - 2 * interval;
        state.upload_metrics.record_at(
            UploadEvent {
                at: Instant::now(),
                duration_ms: 50,
                success: true,
                bytes: 1_875_000,
            },
            past,
        );
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
        let samples = v["samples"].as_array().expect("samples array");
        assert!(!samples.is_empty(), "the past upload surfaces as a sample");
        // The bucket holding our 1.875 MB upload reads ~1 Mbps.
        let has_one_mbps = samples
            .iter()
            .any(|s| (s["mbps"].as_f64().unwrap_or(0.0) - 1.0).abs() < 1e-6);
        assert!(has_one_mbps, "the ~1 Mbps bucket is present: {samples:?}");
    }
}
