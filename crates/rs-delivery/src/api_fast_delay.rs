//! `POST /api/endpoints/fast_delay` (#437): the operator sets a fast
//! endpoint's read-delay target live, behind the bearer gate.
//!
//! Owner decision 25.11.2026: a live intervention (the 18:27 re-add of
//! KS-PP-TEST reset its adaptive buffer to the 9 s floor) must be correctable
//! during an event, back to a known good state. The target is clamped to
//! `[FAST_DELAY_FLOOR_SECS, FAST_DELAY_CEILING_SECS]`, published at once on
//! `/api/status`, adopted by the producer on its next fetch, and audited as
//! `fast_delay_set` (mirrored to the host audit log). Kept out of `api.rs`,
//! which is at the 1000-line cap.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct FastDelayRequest {
    pub alias: String,
    pub target_secs: u64,
}

/// 200 `{alias, from_secs, to_secs}`; 404 `not_found` for an unknown alias;
/// 409 `not_fast` for an endpoint without a read-delay target.
pub async fn set_fast_delay(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FastDelayRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let outcome = {
        let endpoints = state.endpoints.read().await;
        endpoints
            .get(&req.alias)
            .map(|h| h.request_fast_delay(req.target_secs))
    };
    let (from, to) = match outcome {
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "not_found" })),
            );
        }
        Some(None) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "not_fast" })),
            );
        }
        Some(Some(edge)) => edge,
    };
    // An unchanged target is no change: same answer, no audit row, no log.
    if from != to {
        crate::fast_delay_audit::emit_delay_set(
            &Some(Arc::clone(&state.audit_ring)),
            &req.alias,
            from,
            to,
            req.target_secs,
        );
        tracing::info!(
            alias = %req.alias,
            from_secs = from,
            to_secs = to,
            requested_secs = req.target_secs,
            "operator set the fast read-delay target (#437)"
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "alias": req.alias, "from_secs": from, "to_secs": to })),
    )
}

#[cfg(test)]
#[path = "api_fast_delay_tests.rs"]
mod tests;
