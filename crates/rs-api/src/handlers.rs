use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogEntry;
use rs_core::models::{
    ChunkStats, ComponentStatus, DeliveryInstance, EndpointConfig, ServiceStatus, StreamingEvent,
    WsEvent,
};

use crate::state::AppState;

const REDACTED: &str = "***";
const VALID_SERVICE_TYPES: &[&str] =
    &["YT_HLS", "FB", "YT_RTMP", "VIMEO", "INSTAGRAM", "TEST_FILE"];

pub async fn health() -> StatusCode {
    StatusCode::OK
}

pub async fn get_status(State(state): State<AppState>) -> Result<Json<ServiceStatus>, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let rtmp_connected = state.inpoint_state.is_connected();
    let inpoint = ComponentStatus {
        state: if rtmp_connected {
            "connected".into()
        } else {
            "disconnected".into()
        },
        details: serde_json::json!({ "rtmp_connected": rtmp_connected }),
    };

    Ok(Json(ServiceStatus {
        inpoint,
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
    let chunk_duration_ms = state.config.inpoint.chunk_duration_ms;
    let stats = db::get_chunk_stats(&state.pool, chunk_duration_ms)
        .await
        .map_err(|e| {
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

pub async fn action_restart_inpoint(
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    match &state.inpoint_restart_tx {
        Some(tx) => {
            tx.send(()).await.map_err(|_| {
                error!("Inpoint restart channel closed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            tracing::info!("Inpoint restart requested via API");
            Ok(StatusCode::OK)
        }
        None => Ok(StatusCode::SERVICE_UNAVAILABLE),
    }
}

pub async fn action_restart_endpoint(
    State(state): State<AppState>,
) -> Result<StatusCode, StatusCode> {
    match &state.endpoint_restart_tx {
        Some(tx) => {
            tx.send(()).await.map_err(|_| {
                error!("Endpoint restart channel closed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            tracing::info!("Endpoint restart requested via API");
            Ok(StatusCode::OK)
        }
        None => Ok(StatusCode::SERVICE_UNAVAILABLE),
    }
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
    db::set_receiving_activated(&state.pool, event.id, new_receiving)
        .await
        .map_err(|e| {
            error!("Failed to update receiving flag: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "toggled_receiving".to_string(),
        name: Some(event.name),
        receiving: new_receiving,
        delivering: event.delivering_activated,
    }) {
        tracing::debug!("No WS subscribers for StreamingEvent: {e}");
    }

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
    db::set_delivering_activated(&state.pool, event.id, new_delivering)
        .await
        .map_err(|e| {
            error!("Failed to update delivering flag: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "toggled_delivering".to_string(),
        name: Some(event.name),
        receiving: event.receiving_activated,
        delivering: new_delivering,
    }) {
        tracing::debug!("No WS subscribers for StreamingEvent: {e}");
    }

    Ok(StatusCode::OK)
}

pub async fn get_config(State(state): State<AppState>) -> Json<Config> {
    let config_arc = state
        .config_live
        .read()
        .map(|c| c.clone())
        .unwrap_or_else(|_| state.config.clone());
    let mut config = (*config_arc).clone();
    // Redact sensitive credentials before sending over the API
    config.s3.access_key_id = REDACTED.to_string();
    config.s3.secret_access_key = REDACTED.to_string();
    config.hetzner.api_token = REDACTED.to_string();
    config.youtube.client_secret = REDACTED.to_string();
    Json(config)
}

pub async fn patch_config(
    State(state): State<AppState>,
    Json(updates): Json<serde_json::Value>,
) -> Result<Json<Config>, StatusCode> {
    let current_config = state
        .config_live
        .read()
        .map(|c| c.clone())
        .unwrap_or_else(|_| state.config.clone());

    let current = serde_json::to_value(&*current_config).map_err(|e| {
        error!("Failed to serialize current config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let merged = merge_json(current, updates);

    let mut new_config: Config = serde_json::from_value(merged).map_err(|e| {
        tracing::warn!("Invalid config update: {e}");
        StatusCode::BAD_REQUEST
    })?;

    // Preserve redacted credentials — keep originals if placeholder sent back
    if new_config.s3.access_key_id == REDACTED {
        new_config.s3.access_key_id = current_config.s3.access_key_id.clone();
    }
    if new_config.s3.secret_access_key == REDACTED {
        new_config.s3.secret_access_key = current_config.s3.secret_access_key.clone();
    }
    if new_config.hetzner.api_token == REDACTED {
        new_config.hetzner.api_token = current_config.hetzner.api_token.clone();
    }
    if new_config.youtube.client_secret == REDACTED {
        new_config.youtube.client_secret = current_config.youtube.client_secret.clone();
    }

    new_config.validate().map_err(|e| {
        tracing::warn!("Config validation failed: {e}");
        StatusCode::BAD_REQUEST
    })?;

    if let Some(path) = &state.config_path {
        new_config.save(path).map_err(|e| {
            error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        tracing::info!("Config saved to {}", path.display());
    }

    if let Ok(mut live) = state.config_live.write() {
        *live = std::sync::Arc::new(new_config.clone());
    }

    new_config.s3.access_key_id = REDACTED.to_string();
    new_config.s3.secret_access_key = REDACTED.to_string();
    new_config.hetzner.api_token = REDACTED.to_string();
    new_config.youtube.client_secret = REDACTED.to_string();

    Ok(Json(new_config))
}

/// Recursively merge a JSON patch into a base object.
fn merge_json(base: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    match (base, patch) {
        (serde_json::Value::Object(mut base_map), serde_json::Value::Object(patch_map)) => {
            for (key, value) in patch_map {
                let existing = base_map.remove(&key).unwrap_or(serde_json::Value::Null);
                base_map.insert(key, merge_json(existing, value));
            }
            serde_json::Value::Object(base_map)
        }
        (_, patch) => patch,
    }
}

/// Maximum number of log entries returned per request.
const MAX_LOG_ENTRIES: usize = 200;

#[derive(Serialize, Deserialize)]
pub struct LogsResponse {
    pub entries: Vec<LogEntry>,
}

pub async fn get_logs_inpoint(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<LogQueryParams>,
) -> Json<LogsResponse> {
    let limit = params.limit.unwrap_or(100).min(MAX_LOG_ENTRIES);
    let entries = state.log_buffer.recent("rs_inpoint", limit);
    Json(LogsResponse { entries })
}

pub async fn get_logs_endpoint(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<LogQueryParams>,
) -> Json<LogsResponse> {
    let limit = params.limit.unwrap_or(100).min(MAX_LOG_ENTRIES);
    let entries = state.log_buffer.recent("rs_endpoint", limit);
    Json(LogsResponse { entries })
}

#[derive(Deserialize)]
pub struct LogQueryParams {
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn list_events(
    State(state): State<AppState>,
) -> Result<Json<Vec<StreamingEvent>>, StatusCode> {
    let events = db::list_streaming_events(&state.pool).await.map_err(|e| {
        error!("Failed to list events: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(events))
}

#[derive(Deserialize)]
pub struct CreateEventRequest {
    pub name: String,
}

pub async fn create_event(
    State(state): State<AppState>,
    Json(req): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    if req.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = db::create_streaming_event(&state.pool, &req.name)
        .await
        .map_err(|e| {
            error!("Failed to create event: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn get_event_by_id(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<StreamingEvent>, StatusCode> {
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(event))
}

pub async fn delete_event_by_id(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::delete_streaming_event(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to delete event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_endpoints(
    State(state): State<AppState>,
) -> Result<Json<Vec<EndpointConfig>>, StatusCode> {
    let endpoints = db::list_endpoint_configs(&state.pool).await.map_err(|e| {
        error!("Failed to list endpoints: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(endpoints))
}

#[derive(Deserialize)]
pub struct CreateEndpointRequest {
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    #[serde(default)]
    pub is_fast: Option<bool>,
}

pub async fn create_endpoint(
    State(state): State<AppState>,
    Json(req): Json<CreateEndpointRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    if req.alias.trim().is_empty() || req.alias.len() > 255 {
        tracing::warn!("Invalid alias: empty or too long (max 255 chars)");
        return Err(StatusCode::BAD_REQUEST);
    }

    if !VALID_SERVICE_TYPES.contains(&req.service_type.as_str()) {
        tracing::warn!("Invalid service_type: {}", req.service_type);
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = db::create_endpoint_config(
        &state.pool,
        &req.alias,
        &req.service_type,
        &req.stream_key,
        req.is_fast.unwrap_or(false),
    )
    .await
    .map_err(|e| {
        error!("Failed to create endpoint: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn get_endpoint_by_id(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<EndpointConfig>, StatusCode> {
    let endpoint = db::get_endpoint_config(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get endpoint {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(endpoint))
}

#[derive(Deserialize)]
pub struct UpdateEndpointRequest {
    pub alias: Option<String>,
    pub service_type: Option<String>,
    pub stream_key: Option<String>,
    pub enabled: Option<bool>,
    pub is_fast: Option<bool>,
}

pub async fn update_endpoint(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Json(req): Json<UpdateEndpointRequest>,
) -> Result<StatusCode, StatusCode> {
    if let Some(ref st) = req.service_type {
        if !VALID_SERVICE_TYPES.contains(&st.as_str()) {
            tracing::warn!("Invalid service_type in update: {st}");
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    if let Some(ref alias) = req.alias {
        if alias.trim().is_empty() || alias.len() > 255 {
            tracing::warn!("Invalid alias in update: empty or too long");
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    let existing = db::get_endpoint_config(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get endpoint {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    db::update_endpoint_config(
        &state.pool,
        id,
        req.alias.as_deref().unwrap_or(&existing.alias),
        req.service_type
            .as_deref()
            .unwrap_or(&existing.service_type),
        req.stream_key.as_deref().unwrap_or(&existing.stream_key),
        req.enabled.unwrap_or(existing.enabled),
        req.is_fast.unwrap_or(existing.is_fast),
    )
    .await
    .map_err(|e| {
        error!("Failed to update endpoint {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::OK)
}

pub async fn delete_endpoint(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::delete_endpoint_config(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to delete endpoint {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn attach_endpoint_to_event(
    State(state): State<AppState>,
    axum::extract::Path((event_id, endpoint_id)): axum::extract::Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::attach_endpoint_to_event(&state.pool, event_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to attach endpoint {endpoint_id} to event {event_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::CREATED)
}

pub async fn detach_endpoint_from_event(
    State(state): State<AppState>,
    axum::extract::Path((event_id, endpoint_id)): axum::extract::Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::detach_endpoint_from_event(&state.pool, event_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to detach endpoint {endpoint_id} from event {event_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_event_endpoints(
    State(state): State<AppState>,
    axum::extract::Path(event_id): axum::extract::Path<i64>,
) -> Result<Json<Vec<rs_core::models::EndpointConfig>>, StatusCode> {
    let links = db::get_event_endpoints(&state.pool, event_id)
        .await
        .map_err(|e| {
            error!("Failed to get endpoints for event {event_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(links))
}

pub async fn activate_event(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    // Verify event exists
    db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    db::set_receiving_activated(&state.pool, id, true)
        .await
        .map_err(|e| {
            error!("Failed to activate event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "activated".to_string(),
        name: None,
        receiving: true,
        delivering: false,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    Ok(StatusCode::OK)
}

pub async fn deactivate_event(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    db::deactivate_event(&state.pool, id).await.map_err(|e| {
        error!("Failed to deactivate event {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "deactivated".to_string(),
        name: None,
        receiving: false,
        delivering: false,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    Ok(StatusCode::OK)
}

pub async fn start_delivering(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    db::set_delivering_activated(&state.pool, id, true)
        .await
        .map_err(|e| {
            error!("Failed to start delivering for event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Err(e) = state.ws_tx.send(WsEvent::StreamingEvent {
        action: "delivering_started".to_string(),
        name: None,
        receiving: true,
        delivering: true,
    }) {
        tracing::debug!("No WS subscribers: {e}");
    }

    Ok(StatusCode::OK)
}

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
        }
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

#[derive(Serialize)]
pub struct YouTubeStatusResponse {
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub error: Option<String>,
}

pub async fn youtube_status(
    State(state): State<AppState>,
) -> Result<Json<YouTubeStatusResponse>, StatusCode> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        error!("Delivery orchestrator not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let status = orch.check_youtube_status().await;

    Ok(Json(YouTubeStatusResponse {
        authenticated: status.authenticated,
        stream_receiving: status.stream_receiving,
        error: status.error,
    }))
}

#[derive(Deserialize)]
pub struct YouTubeOAuthSeedRequest {
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
}

pub async fn youtube_oauth_seed(
    State(state): State<AppState>,
    Json(req): Json<YouTubeOAuthSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    db::upsert_youtube_oauth(
        &state.pool,
        "",
        &req.refresh_token,
        "https://oauth2.googleapis.com/token",
        &req.client_id,
        &req.client_secret,
        "https://www.googleapis.com/auth/youtube.readonly",
        None,
    )
    .await
    .map_err(|e| {
        error!("Failed to seed YouTube OAuth: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!("YouTube OAuth tokens seeded");
    Ok(StatusCode::OK)
}

#[derive(Serialize)]
pub struct YouTubeOAuthStartResponse {
    pub url: String,
}

pub async fn youtube_oauth_start(
    State(state): State<AppState>,
) -> Result<Json<YouTubeOAuthStartResponse>, StatusCode> {
    let yt_config = &state.config.youtube;
    if yt_config.client_id.is_empty() || yt_config.client_secret.is_empty() {
        error!("YouTube OAuth client_id or client_secret not configured");
        return Err(StatusCode::BAD_REQUEST);
    }

    let config = rs_youtube::YouTubeConfig {
        client_id: yt_config.client_id.clone(),
        client_secret: yt_config.client_secret.clone(),
    };
    let redirect_uri = "http://127.0.0.1:8910/api/v1/youtube/oauth/callback";
    let url = rs_youtube::oauth::authorization_url(&config, redirect_uri);

    Ok(Json(YouTubeOAuthStartResponse { url }))
}

#[derive(Deserialize)]
pub struct YouTubeOAuthCallbackParams {
    pub code: Option<String>,
    pub error: Option<String>,
}

pub async fn youtube_oauth_callback(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<YouTubeOAuthCallbackParams>,
) -> Result<axum::response::Html<String>, StatusCode> {
    if let Some(err) = params.error {
        let escaped = err
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        return Ok(axum::response::Html(format!(
            "<html><body><h1>YouTube Authorization Failed</h1><p>{escaped}</p></body></html>"
        )));
    }

    let code = params.code.ok_or_else(|| {
        error!("YouTube OAuth callback missing 'code' parameter");
        StatusCode::BAD_REQUEST
    })?;

    let yt_config = &state.config.youtube;
    let config = rs_youtube::YouTubeConfig {
        client_id: yt_config.client_id.clone(),
        client_secret: yt_config.client_secret.clone(),
    };
    let redirect_uri = "http://127.0.0.1:8910/api/v1/youtube/oauth/callback";

    let tokens = rs_youtube::oauth::exchange_code(&config, &code, redirect_uri)
        .await
        .map_err(|e| {
            error!("YouTube OAuth code exchange failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let expires_at = tokens
        .expires_in
        .map(|secs| (chrono::Utc::now() + chrono::Duration::seconds(secs as i64)).to_rfc3339());

    db::upsert_youtube_oauth(
        &state.pool,
        &tokens.access_token,
        tokens.refresh_token.as_deref().unwrap_or(""),
        "https://oauth2.googleapis.com/token",
        &yt_config.client_id,
        &yt_config.client_secret,
        "https://www.googleapis.com/auth/youtube.readonly",
        expires_at.as_deref(),
    )
    .await
    .map_err(|e| {
        error!("Failed to store YouTube OAuth tokens: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!("YouTube OAuth tokens stored successfully");

    Ok(axum::response::Html(
        "<html><body><h1>YouTube Authorized Successfully</h1>\
         <p>You can close this tab. The refresh token has been stored.</p></body></html>"
            .to_string(),
    ))
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
