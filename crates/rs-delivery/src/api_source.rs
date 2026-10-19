//! `POST /api/source` (#408): the host announces that OBS stopped on purpose
//! and which chunk is the last of the source. The endpoints play their
//! buffer out to it and then stop sending (`source_end`). Kept out of
//! `api.rs`, which is at the 1000-line cap.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct SourceEndRequest {
    pub ended_at_chunk_id: i64,
}

/// 200 `{ended_at_chunk_id}`: every endpoint of the event will stop after
/// that chunk. 409 `no_event` before `/api/init` built the event's cache;
/// 409 `stale` when a later chunk was already fetched (OBS is live again).
pub async fn announce_source_end(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SourceEndRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(cache) = state.disk_cache.read().await.clone() else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "no_event" })),
        );
    };
    if !cache.source_end.announce(req.ended_at_chunk_id) {
        tracing::warn!(
            ended_at = req.ended_at_chunk_id,
            "source end refused: a later chunk was already fetched"
        );
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "stale" })),
        );
    }
    tracing::info!(
        ended_at = req.ended_at_chunk_id,
        "source ended on purpose: endpoints play out to this chunk"
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({ "ended_at_chunk_id": req.ended_at_chunk_id })),
    )
}

#[cfg(test)]
#[path = "api_source_tests.rs"]
mod tests;
