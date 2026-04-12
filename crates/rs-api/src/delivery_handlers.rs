//! Delivery HTTP handlers: start, stop, status, instances.
//! Split from handlers.rs to keep files under 1000 lines.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use rs_core::db;
use rs_core::models::DeliveryInstance;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct DeliveryStartRequest {
    pub event_id: i64,
}
#[derive(Serialize)]
pub struct DeliveryStartResponse {
    pub instance_id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub server_type: String,
    pub status: String,
}

pub async fn delivery_start(
    State(state): State<AppState>,
    Json(req): Json<DeliveryStartRequest>,
) -> Result<Json<DeliveryStartResponse>, StatusCode> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        error!("Delivery orchestrator not configured (missing Hetzner API token)");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let event_id = req.event_id;
    let result = orch.start_delivery(event_id).await.map_err(|e| {
        error!("Failed to start delivery: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Look up event details for poll_and_init
    let event = db::get_streaming_event_by_id(&state.pool, event_id)
        .await
        .map_err(|e| {
            error!("Failed to get event {event_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or_else(|| {
            error!("Event {event_id} not found");
            StatusCode::NOT_FOUND
        })?;

    // Spawn background task to poll Hetzner and init rs-delivery
    let (instance_id, event_name) = (result.instance_id, event.name.clone());
    let (auth_token, poll_handles, orch) = (
        result.auth_token.clone(),
        orch.poll_handles(),
        Arc::clone(orch),
    );
    let cached_delivery = state.cached_delivery.clone();
    let ws_tx_clone = state.ws_tx.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = orch
            .poll_and_init(instance_id, event_id, &event_name, &auth_token)
            .await
        {
            tracing::error!("Background poll_and_init failed for instance {instance_id}: {e}");
            if let Err(e) =
                db::update_delivery_instance_status(orch.pool(), instance_id, "failed").await
            {
                tracing::error!("Failed to mark instance {instance_id} as failed: {e}");
            }
            orch.poll_handles().lock().await.remove(&instance_id);
            return;
        }

        // Transition to health monitoring loop with auto-restart
        tracing::info!(event_id, "Delivery health monitor started");
        orch.monitor_delivery_health(event_id, instance_id, cached_delivery, ws_tx_clone)
            .await;
        orch.poll_handles().lock().await.remove(&instance_id);
    });
    poll_handles.lock().await.insert(instance_id, handle);

    Ok(Json(DeliveryStartResponse {
        instance_id: result.instance_id,
        hetzner_id: result.hetzner_id,
        name: result.name,
        server_type: result.server_type,
        status: result.status,
    }))
}

#[derive(Deserialize)]
pub struct DeliveryStatusQuery {
    pub event_id: i64,
}

#[derive(Serialize)]
pub struct DeliveryStatusResponse {
    pub instance: Option<DeliveryInstance>,
    pub server_ready: bool,
    pub server_ip: Option<String>,
    pub instance_status: Option<String>,
    pub endpoints_alive: bool,
    pub endpoint_details: Vec<DeliveryEndpointEntry>,
}
#[derive(Serialize)]
pub struct DeliveryEndpointEntry {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub stall_reason: Option<String>,
    pub ffmpeg_restart_count: u32,
    pub last_error: Option<String>,
    pub is_fast: bool,
    pub restart_history: Vec<crate::delivery::EndpointRestartRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescue_eta_secs: Option<u64>,
}

pub async fn delivery_status(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<DeliveryStatusQuery>,
) -> Result<Json<DeliveryStatusResponse>, StatusCode> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        error!("Delivery orchestrator not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let status = orch
        .get_delivery_status(query.event_id)
        .await
        .map_err(|e| {
            error!("Failed to get delivery status: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let server_ip = status.instance.as_ref().map(|i| i.ipv4.clone());
    let endpoint_details: Vec<DeliveryEndpointEntry> = status
        .endpoints
        .into_iter()
        .map(|ep| DeliveryEndpointEntry {
            alias: ep.alias,
            alive: ep.alive,
            current_chunk_id: ep.current_chunk_id,
            bytes_processed_total: ep.bytes_processed_total,
            chunks_processed: ep.chunks_processed,
            chunk_delay_secs: ep.chunk_delay_secs,
            stall_reason: ep.stall_reason,
            ffmpeg_restart_count: ep.ffmpeg_restart_count,
            last_error: ep.last_error,
            is_fast: ep.is_fast,
            restart_history: ep.restart_history,
            delivery_mode: ep.delivery_mode,
            rescue_eta_secs: ep.rescue_eta_secs,
        })
        .collect();
    let endpoints_alive =
        !endpoint_details.is_empty() && endpoint_details.iter().all(|ep| ep.alive);

    let instance_status = status.instance.as_ref().map(|i| i.status.clone());

    Ok(Json(DeliveryStatusResponse {
        instance: status.instance,
        server_ready: status.server_ready,
        server_ip,
        instance_status,
        endpoints_alive,
        endpoint_details,
    }))
}

#[derive(Deserialize)]
pub struct DeliveryStopRequest {
    pub event_id: i64,
}
pub async fn delivery_stop(
    State(state): State<AppState>,
    Json(req): Json<DeliveryStopRequest>,
) -> Result<StatusCode, StatusCode> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        error!("Delivery orchestrator not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    orch.stop_delivery(req.event_id).await.map_err(|e| {
        error!("Failed to stop delivery: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}

pub async fn delivery_status_cached(
    State(state): State<AppState>,
) -> Json<crate::state::CachedDeliveryStatus> {
    let cached = state
        .cached_delivery
        .read()
        .map(|c| c.clone())
        .unwrap_or_default();
    Json(cached)
}

pub async fn list_delivery_instances(
    State(state): State<AppState>,
) -> Result<Json<Vec<DeliveryInstance>>, StatusCode> {
    let instances = db::list_delivery_instances(&state.pool)
        .await
        .map_err(|e| {
            error!("Failed to list delivery instances: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(instances))
}

// --- Mid-stream endpoint add/remove handlers ---

#[derive(Deserialize)]
pub struct AddEndpointToDeliveryRequest {
    pub event_id: i64,
    pub endpoint_id: i64,
    #[serde(default)]
    pub start_position: crate::delivery_endpoints::StartPosition,
}

pub async fn delivery_add_endpoint(
    State(state): State<AppState>,
    Json(req): Json<AddEndpointToDeliveryRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Delivery orchestrator not configured".to_string(),
        )
    })?;

    crate::delivery_endpoints::add_endpoint_to_delivery(
        orch,
        &state.pool,
        &state.config,
        req.event_id,
        req.endpoint_id,
        req.start_position,
    )
    .await
    .map_err(|e| {
        error!("Failed to add endpoint to delivery: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct RemoveEndpointFromDeliveryRequest {
    pub event_id: i64,
    pub alias: String,
}

pub async fn delivery_remove_endpoint(
    State(state): State<AppState>,
    Json(req): Json<RemoveEndpointFromDeliveryRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Delivery orchestrator not configured".to_string(),
        )
    })?;

    crate::delivery_endpoints::remove_endpoint_from_delivery(
        orch,
        &state.pool,
        req.event_id,
        &req.alias,
    )
    .await
    .map_err(|e| {
        error!("Failed to remove endpoint from delivery: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct DeliveryLogsQuery {
    pub instance_id: i64,
}

#[derive(Serialize)]
pub struct DeliveryLogsResponse {
    pub instance_id: i64,
    pub restart_log: Vec<rs_core::db::DeliveryRestartRow>,
    pub captured_log: Option<String>,
}

/// GET /delivery/logs?instance_id=N — retrieve persisted delivery logs
/// and ffmpeg restart records for a (possibly deleted) VPS instance.
pub async fn delivery_logs(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<DeliveryLogsQuery>,
) -> Result<Json<DeliveryLogsResponse>, StatusCode> {
    let restart_log = rs_core::db::get_delivery_restart_log(&state.pool, query.instance_id)
        .await
        .map_err(|e| {
            error!("Failed to get restart log: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let captured_log = rs_core::db::get_delivery_log(&state.pool, query.instance_id)
        .await
        .map_err(|e| {
            error!("Failed to get delivery log: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(DeliveryLogsResponse {
        instance_id: query.instance_id,
        restart_log,
        captured_log,
    }))
}
