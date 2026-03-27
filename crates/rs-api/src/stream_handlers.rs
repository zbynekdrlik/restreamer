//! Stream control handlers: start/stop stream, update event.
//! Split from handlers.rs to keep files under 1000 lines.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::error;

use rs_core::db;
use rs_core::models::{DeliveryEndpointMetrics, WsEvent};

use crate::state::AppState;

pub async fn start_stream(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    // Verify event exists
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Enforce single-event: check if any other event is active
    let all_events = db::list_streaming_events(&state.pool).await.map_err(|e| {
        error!("Failed to list events: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    for evt in &all_events {
        if evt.id != id && (evt.receiving_activated || evt.delivering_activated) {
            return Err(StatusCode::CONFLICT);
        }
    }

    // Set both flags
    db::update_streaming_event_flags(&state.pool, id, true, true)
        .await
        .map_err(|e| {
            error!("Failed to start stream for event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Clear stale chunks from prior sessions so buffer starts at 0%
    let deleted = db::delete_chunks_for_event(&state.pool, id)
        .await
        .unwrap_or(0);
    if deleted > 0 {
        tracing::info!("Cleared {deleted} stale chunks for event {id}");
    }

    // Broadcast WS event
    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "start_stream".to_string(),
        name: Some(event.name.clone()),
        receiving: true,
        delivering: true,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    // Broadcast activity feed event
    if let Err(e) = state.ws_tx.send(WsEvent::ActivityFeed {
        timestamp: chrono::Utc::now().to_rfc3339(),
        severity: "info".to_string(),
        message: format!("Stream started: {}", event.name),
        source: "system".to_string(),
    }) {
        tracing::debug!("No WS subscribers for ActivityFeed: {e}");
    }

    // Start delivery VPS if orchestrator is available
    if let Some(orch) = state.delivery_orchestrator.as_ref() {
        match orch.start_delivery(id).await {
            Ok(result) => {
                let (instance_id, event_name) = (result.instance_id, event.name.clone());
                let (auth_token, poll_handles, orch) = (
                    result.auth_token.clone(),
                    orch.poll_handles(),
                    Arc::clone(orch),
                );
                let handle = tokio::spawn(async move {
                    if let Err(e) = orch
                        .poll_and_init(instance_id, id, &event_name, &auth_token)
                        .await
                    {
                        tracing::error!(
                            "Background poll_and_init failed for instance {instance_id}: {e}"
                        );
                        if let Err(e) =
                            db::update_delivery_instance_status(orch.pool(), instance_id, "failed")
                                .await
                        {
                            tracing::error!("Failed to mark instance {instance_id} as failed: {e}");
                        }
                        orch.poll_handles().lock().await.remove(&instance_id);
                        return;
                    }

                    // Transition to health monitoring loop
                    tracing::info!(event_id = id, "Delivery health monitor started");
                    orch.monitor_delivery_health(id, instance_id).await;
                    orch.poll_handles().lock().await.remove(&instance_id);
                });
                poll_handles.lock().await.insert(instance_id, handle);

                if let Err(e) = state.ws_tx.send(WsEvent::ActivityFeed {
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    severity: "info".to_string(),
                    message: "Delivery VPS creation started".to_string(),
                    source: "delivery".to_string(),
                }) {
                    tracing::debug!("No WS subscribers for ActivityFeed: {e}");
                }

                // Send immediate placeholder DeliveryStatus + PipelineState
                // so the dashboard shows endpoint cards and cache bar right away
                let configured = db::get_event_endpoints(&state.pool, id)
                    .await
                    .unwrap_or_default();
                let placeholder_eps: Vec<DeliveryEndpointMetrics> = configured
                    .iter()
                    .map(|ep| DeliveryEndpointMetrics {
                        alias: ep.alias.clone(),
                        alive: false,
                        current_chunk_id: 0,
                        bytes_processed_total: 0,
                        chunks_processed: 0,
                        chunk_delay_secs: 0.0,
                        stall_reason: None,
                        ffmpeg_restart_count: 0,
                        last_error: None,
                        is_fast: ep.is_fast,
                    })
                    .collect();
                let _ = state.ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: result.name.clone(),
                    status: "creating".to_string(),
                    server_ip: None,
                    endpoint_count: placeholder_eps.len() as u32,
                    endpoints: placeholder_eps,
                });
                let target_delay = event
                    .cache_delay_secs
                    .map(|s| s as u64)
                    .unwrap_or(state.config.delivery.delivery_delay_secs);
                let _ = state.ws_tx.send(WsEvent::PipelineState {
                    state: "buffering".to_string(),
                    event_id: Some(id),
                    event_name: Some(event.name.clone()),
                    buffer_progress: 0.0,
                    target_delay_secs: target_delay,
                    current_delay_secs: 0.0,
                    session_start: Some(chrono::Utc::now().to_rfc3339()),
                    predicted: false,
                    local_buffer_chunks: 0,
                    s3_queue_chunks: 0,
                });
            }
            Err(e) => {
                error!("Failed to start delivery VPS: {e}");
                if let Err(e) = state.ws_tx.send(WsEvent::ActivityFeed {
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    severity: "error".to_string(),
                    message: format!("Delivery VPS start failed: {e}"),
                    source: "delivery".to_string(),
                }) {
                    tracing::debug!("No WS subscribers for ActivityFeed: {e}");
                }
                // Don't fail the whole start_stream — receiving still works
            }
        }
    }

    Ok(StatusCode::OK)
}

pub async fn stop_stream(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Deactivate both flags
    db::deactivate_event(&state.pool, id).await.map_err(|e| {
        error!("Failed to stop stream for event {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Stop delivery VPS if running
    if let Some(orch) = state.delivery_orchestrator.as_ref() {
        if let Err(e) = orch.stop_delivery(id).await {
            error!("Failed to stop delivery for event {id}: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Broadcast WS event
    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "stop_stream".to_string(),
        name: Some(event.name.clone()),
        receiving: false,
        delivering: false,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    // Broadcast activity feed
    if let Err(e) = state.ws_tx.send(WsEvent::ActivityFeed {
        timestamp: chrono::Utc::now().to_rfc3339(),
        severity: "info".to_string(),
        message: format!("Stream stopped: {}", event.name),
        source: "system".to_string(),
    }) {
        tracing::debug!("No WS subscribers for ActivityFeed: {e}");
    }

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct UpdateEventRequest {
    pub name: Option<String>,
    pub cache_delay_secs: Option<i64>,
}

pub async fn update_event(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Json(req): Json<UpdateEventRequest>,
) -> Result<StatusCode, StatusCode> {
    let existing = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let new_name = req.name.as_deref().unwrap_or(&existing.name);
    if new_name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Merge: preserve existing cache_delay_secs if not provided in request
    let new_delay = req.cache_delay_secs.or(existing.cache_delay_secs);

    db::update_streaming_event(&state.pool, id, new_name, new_delay)
        .await
        .map_err(|e| {
            error!("Failed to update event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Broadcast WS event
    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "updated".to_string(),
        name: Some(new_name.to_string()),
        receiving: existing.receiving_activated,
        delivering: existing.delivering_activated,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    Ok(StatusCode::OK)
}
