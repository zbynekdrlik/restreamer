//! S3-related API handlers: usage measurement and per-event chunk cleanup.
//!
//! Extracted from `handlers.rs` to keep that file under the project's
//! 1000-line per-file cap.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Serialize;
use tracing::error;

use rs_core::db;
use rs_endpoint::s3::S3Client;

use crate::state::AppState;

#[derive(Serialize)]
pub struct ClearChunksResponse {
    pub deleted: u64,
}

/// POST /events/{id}/clear-s3 — delete all S3 chunks for an event but
/// keep the event row in the DB. Used by the per-event "Clear S3 chunks"
/// dashboard button so the operator can free space without losing the
/// event configuration.
pub async fn clear_event_s3_chunks(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<ClearChunksResponse>, StatusCode> {
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    if event.receiving_activated || event.delivering_activated {
        return Err(StatusCode::CONFLICT);
    }

    let config = match state.config_live.read() {
        Ok(c) => c.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let s3_client = S3Client::new(&config.s3).map_err(|e| {
        error!("Failed to create S3 client for clear-s3 {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let deleted = s3_client
        .delete_event_chunks(&event.name)
        .await
        .map_err(|e| {
            error!(
                "Failed to clear S3 chunks for event {id} ({}): {e}",
                event.name
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(ClearChunksResponse { deleted }))
}

#[derive(Serialize)]
pub struct S3UsageEntry {
    pub event_name: String,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Serialize)]
pub struct S3UsageResponse {
    pub total_bytes: u64,
    pub total_objects: u64,
    pub by_event: Vec<S3UsageEntry>,
}

/// GET /s3/usage — total and per-event byte/object counts in the S3 bucket.
/// Used by the dashboard to show storage consumption per event.
pub async fn get_s3_usage(
    State(state): State<AppState>,
) -> Result<Json<S3UsageResponse>, StatusCode> {
    let config = match state.config_live.read() {
        Ok(c) => c.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let s3_client = S3Client::new(&config.s3).map_err(|e| {
        error!("Failed to create S3 client for usage query: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let prefixes = s3_client.list_event_prefixes().await.map_err(|e| {
        error!("Failed to list S3 prefixes: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut by_event: Vec<S3UsageEntry> = Vec::with_capacity(prefixes.len());
    let mut total_bytes: u64 = 0;
    let mut total_objects: u64 = 0;
    for prefix in &prefixes {
        let prefix_with_slash = format!("{prefix}/");
        let (bytes, objects) = s3_client
            .measure_prefix(&prefix_with_slash)
            .await
            .map_err(|e| {
                error!("Failed to measure prefix {prefix}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        total_bytes += bytes;
        total_objects += objects;
        by_event.push(S3UsageEntry {
            event_name: prefix.clone(),
            bytes,
            objects,
        });
    }
    by_event.sort_by(|a, b| b.bytes.cmp(&a.bytes));

    Ok(Json(S3UsageResponse {
        total_bytes,
        total_objects,
        by_event,
    }))
}
