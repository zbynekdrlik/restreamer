//! GET /api/v1/diagnostics/pacing — drift telemetry time-series endpoint.
//!
//! Returns three `Vec<DriftSample>` series:
//! - `producer_rate`: ts-duration / wall-clock ratio per consecutive chunk pair
//! - `consumer_rate`: ffmpeg-media-time / wall-clock ratio per progress sample pair
//! - `clock_skew`: VPS clock offset in ms relative to the producer wall-clock

use axum::Json;
use axum::extract::{Query, State};
use rs_core::db::drift::DriftSample;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct PacingQuery {
    pub event_id: i64,
    pub since_ms: Option<i64>,
    pub endpoint_alias: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PacingResponse {
    pub producer_rate: Vec<DriftSample>,
    pub consumer_rate: Vec<DriftSample>,
    pub clock_skew: Vec<DriftSample>,
}

/// Query drift telemetry for a streaming event.
///
/// `endpoint_alias` is optional; when omitted, `consumer_rate` is empty (no
/// single endpoint is selected). Callers that want per-endpoint consumer-rate
/// data should supply the alias.
pub async fn get_pacing(
    State(state): State<AppState>,
    Query(q): Query<PacingQuery>,
) -> Result<Json<PacingResponse>, (axum::http::StatusCode, String)> {
    let since_ms = q.since_ms.unwrap_or(0);
    let endpoint_alias = q.endpoint_alias.as_deref().unwrap_or("");
    let pool = &state.pool;

    let producer_rate = rs_core::db::drift::list_chunk_producer_rate(pool, q.event_id, since_ms)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let consumer_rate = if endpoint_alias.is_empty() {
        Vec::new()
    } else {
        rs_core::db::drift::list_ffmpeg_consumer_rate(pool, q.event_id, endpoint_alias, since_ms)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    let clock_skew = rs_core::db::drift::list_clock_skew(pool, q.event_id, since_ms)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(PacingResponse {
        producer_rate,
        consumer_rate,
        clock_skew,
    }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rs_core::config::Config;
    use rs_core::db;
    use rs_core::models::WsEvent;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    use crate::router::build_router;
    use crate::state::AppState;

    async fn test_state() -> AppState {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let config = Config::for_testing();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        AppState::new_for_tests(pool, config, ws_tx)
    }

    #[tokio::test]
    async fn pacing_endpoint_returns_empty_for_unknown_event() {
        let state = test_state().await;
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/diagnostics/pacing?event_id=9999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp: super::PacingResponse = serde_json::from_slice(&body).unwrap();
        assert!(resp.producer_rate.is_empty());
        assert!(resp.consumer_rate.is_empty());
        assert!(resp.clock_skew.is_empty());
    }

    #[tokio::test]
    async fn pacing_endpoint_returns_populated_series_after_inserts() {
        let state = test_state().await;
        let pool = &state.pool.clone();

        // Create an event
        let event_id = db::upsert_streaming_event(pool, "evt-pacing-t7")
            .await
            .unwrap();

        // Insert two chunks with wall-clock timestamps so producer_rate has 1 sample
        db::drift::insert_chunk_with_walltime(pool, event_id, "/tmp/p7a", 1, "a", 1000, 1_000_000)
            .await
            .unwrap();
        db::drift::insert_chunk_with_walltime(pool, event_id, "/tmp/p7b", 1, "b", 1000, 1_001_000)
            .await
            .unwrap();

        // Insert a clock-skew sample
        db::drift::insert_clock_skew_sample(
            pool, event_id, 2_000_000, 1_999_900, 2_000_050, 2_000_100, 50, 200,
        )
        .await
        .unwrap();

        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/api/v1/diagnostics/pacing?event_id={event_id}&since_ms=0"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp: super::PacingResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            resp.producer_rate.len(),
            1,
            "expected 1 producer_rate sample"
        );
        assert_eq!(resp.clock_skew.len(), 1, "expected 1 clock_skew sample");
        // No endpoint_alias supplied → consumer_rate must be empty
        assert!(
            resp.consumer_rate.is_empty(),
            "consumer_rate should be empty without alias"
        );
    }

    #[tokio::test]
    async fn pacing_endpoint_returns_consumer_rate_when_alias_provided() {
        let state = test_state().await;
        let pool = &state.pool.clone();

        let event_id = db::upsert_streaming_event(pool, "evt-pacing-t7b")
            .await
            .unwrap();

        // Two ffmpeg progress samples → consumer_rate has 1 sample
        db::drift::insert_ffmpeg_progress_sample(pool, event_id, "YT_HLS", 1_000, 0, 1_000_000)
            .await
            .unwrap();
        db::drift::insert_ffmpeg_progress_sample(pool, event_id, "YT_HLS", 2_000, 990, 1_001_000)
            .await
            .unwrap();

        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/api/v1/diagnostics/pacing?event_id={event_id}&endpoint_alias=YT_HLS"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp: super::PacingResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            resp.consumer_rate.len(),
            1,
            "expected 1 consumer_rate sample"
        );
    }
}
