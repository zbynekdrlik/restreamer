use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::error;

use rs_core::db;
use rs_core::models::{ChunkStats, ServiceStatus, StreamingEvent, WsEvent};

use crate::state::AppState;

pub async fn health() -> StatusCode {
    StatusCode::OK
}

pub async fn get_status(State(state): State<AppState>) -> Result<Json<ServiceStatus>, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(ServiceStatus {
        streaming_event: event,
        ..Default::default()
    }))
}

pub async fn get_streaming_event(
    State(state): State<AppState>,
) -> Result<Json<Option<StreamingEvent>>, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(event))
}

pub async fn delete_streaming_event(
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(event) = event {
        db::delete_streaming_event(&state.pool, event.id)
            .await
            .map_err(|e| {
                error!("Failed to delete streaming event {}: {e}", event.id);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct PaginationParams {
    #[serde(default)]
    pub offset: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Maximum allowed pagination limit to prevent excessive queries.
const MAX_PAGINATION_LIMIT: i64 = 500;

pub async fn get_chunks(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<PaginationParams>,
) -> Result<Json<Vec<rs_core::models::ChunkRecord>>, StatusCode> {
    let offset = params.offset.unwrap_or(0).max(0);
    let limit = params.limit.unwrap_or(50).min(MAX_PAGINATION_LIMIT);
    let chunks = db::get_chunks_paginated(&state.pool, offset, limit)
        .await
        .map_err(|e| {
            error!("Failed to get chunks: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(chunks))
}

pub async fn get_chunk_stats(
    State(state): State<AppState>,
) -> Result<Json<ChunkStats>, StatusCode> {
    let stats = db::get_chunk_stats(&state.pool).await.map_err(|e| {
        error!("Failed to get chunk stats: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(stats))
}

pub async fn delete_chunks(State(state): State<AppState>) -> Result<Json<u64>, StatusCode> {
    let deleted = db::delete_all_chunks(&state.pool).await.map_err(|e| {
        error!("Failed to delete chunks: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(deleted))
}

pub async fn action_restart_inpoint() -> StatusCode {
    // TODO: implement actual restart logic via service handle
    StatusCode::NOT_IMPLEMENTED
}

pub async fn action_restart_endpoint() -> StatusCode {
    // TODO: implement actual restart logic via service handle
    StatusCode::NOT_IMPLEMENTED
}

pub async fn action_toggle_receiving(
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let event = event.ok_or(StatusCode::NOT_FOUND)?;

    let new_receiving = !event.receiving_activated;
    db::update_streaming_event_flags(
        &state.pool,
        event.id,
        new_receiving,
        event.delivering_activated,
    )
    .await
    .map_err(|e| {
        error!("Failed to update receiving flag: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let _ = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "toggled_receiving".to_string(),
        identifier: event.identifier,
        receiving: new_receiving,
        delivering: event.delivering_activated,
    });

    Ok(StatusCode::OK)
}

pub async fn action_toggle_delivering(
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let event = event.ok_or(StatusCode::NOT_FOUND)?;

    let new_delivering = !event.delivering_activated;
    db::update_streaming_event_flags(
        &state.pool,
        event.id,
        event.receiving_activated,
        new_delivering,
    )
    .await
    .map_err(|e| {
        error!("Failed to update delivering flag: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let _ = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "toggled_delivering".to_string(),
        identifier: event.identifier,
        receiving: event.receiving_activated,
        delivering: new_delivering,
    });

    Ok(StatusCode::OK)
}

pub async fn get_config(State(state): State<AppState>) -> Json<rs_core::config::Config> {
    let mut config = (*state.config).clone();
    // Redact sensitive credentials before sending over the API
    config.s3.access_key_id = "***".to_string();
    config.s3.secret_access_key = "***".to_string();
    Json(config)
}
