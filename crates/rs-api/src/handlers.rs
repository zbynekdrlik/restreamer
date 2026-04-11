use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogEntry;
use rs_core::models::{
    ChunkStats, ComponentStatus, EndpointConfig, ServiceStatus, StreamingEvent, WsEvent,
};
use rs_endpoint::s3::S3Client;

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
    config.obs.ws_password = REDACTED.to_string();
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
    if new_config.obs.ws_password == REDACTED {
        new_config.obs.ws_password = current_config.obs.ws_password.clone();
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

    match state.config_live.write() {
        Ok(mut live) => {
            *live = std::sync::Arc::new(new_config.clone());
        }
        Err(e) => {
            error!("Config lock poisoned, runtime config diverges from saved file: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Restart OBS client if OBS config changed
    if new_config.obs.enabled != current_config.obs.enabled
        || new_config.obs.ws_url != current_config.obs.ws_url
        || new_config.obs.ws_password != current_config.obs.ws_password
    {
        state.restart_obs_client(&new_config.obs).await;
        tracing::info!("OBS client restarted due to config change");
    }

    new_config.s3.access_key_id = REDACTED.to_string();
    new_config.s3.secret_access_key = REDACTED.to_string();
    new_config.hetzner.api_token = REDACTED.to_string();
    new_config.youtube.client_secret = REDACTED.to_string();
    new_config.obs.ws_password = REDACTED.to_string();

    Ok(Json(new_config))
}

/// Maximum recursion depth for JSON merge to prevent stack overflow from malicious input.
const MAX_MERGE_DEPTH: usize = 10;

/// Recursively merge a JSON patch into a base object with depth limit.
fn merge_json(base: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    merge_json_inner(base, patch, 0)
}

fn merge_json_inner(
    base: serde_json::Value,
    patch: serde_json::Value,
    depth: usize,
) -> serde_json::Value {
    if depth >= MAX_MERGE_DEPTH {
        return patch;
    }
    match (base, patch) {
        (serde_json::Value::Object(mut base_map), serde_json::Value::Object(patch_map)) => {
            for (key, value) in patch_map {
                let existing = base_map.remove(&key).unwrap_or(serde_json::Value::Null);
                base_map.insert(key, merge_json_inner(existing, value, depth + 1));
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
    pub name: Option<String>,
    pub template_id: Option<i64>,
}

pub async fn create_event(
    State(state): State<AppState>,
    Json(req): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    match (req.template_id, req.name) {
        (Some(tid), _) => {
            let (id, name) = db::create_event_from_template(&state.pool, tid)
                .await
                .map_err(|e| {
                    error!("Failed to create event from template {tid}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({ "id": id, "name": name })),
            ))
        }
        (None, Some(name)) => {
            if name.trim().is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            let id = db::create_streaming_event(&state.pool, &name)
                .await
                .map_err(|e| {
                    error!("Failed to create event: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({ "id": id, "name": name })),
            ))
        }
        (None, None) => Err(StatusCode::BAD_REQUEST),
    }
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
    // Fetch event first — return 404 if not found
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Refuse to delete while streaming is active
    if event.receiving_activated || event.delivering_activated {
        tracing::warn!(
            "Refusing to delete event {id} ({}) — streaming is active",
            event.name
        );
        return Err(StatusCode::CONFLICT);
    }

    // Clean up S3 chunks before removing DB records. If config_live is
    // poisoned (another thread panicked while holding the lock), fall back
    // to the initial config snapshot — but log a warning so the underlying
    // panic isn't hidden.
    let config = match state.config_live.read() {
        Ok(c) => c.clone(),
        Err(poisoned) => {
            tracing::warn!(
                "config_live lock is poisoned (another thread panicked) — \
                 falling back to initial config snapshot for event {id} cleanup"
            );
            poisoned.into_inner().clone()
        }
    };

    let s3_client = S3Client::new(&config.s3).map_err(|e| {
        error!("Failed to create S3 client for event {id} cleanup: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Note: S3 deletion is not transactional. If delete_event_chunks fails
    // mid-loop (network error on object #5 of 10), the first 4 objects are
    // already gone. We still abort the DB delete, leaving the remaining S3
    // objects accessible-but-orphaned. Retrying the delete is safe because
    // the list-then-delete pattern cleans them up on the next attempt.
    s3_client
        .delete_event_chunks(&event.name)
        .await
        .map_err(|e| {
            error!(
                "Failed to delete S3 chunks for event {id} ({}): {e}",
                event.name
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Delete DB records (cascade deletes chunks, endpoint links, etc.)
    db::delete_streaming_event(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to delete event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::NO_CONTENT)
}

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

// Delivery handlers are in delivery_handlers.rs

// --- OBS WebSocket handlers ---

pub async fn obs_status(
    State(state): State<AppState>,
) -> Result<Json<crate::obs::ObsState>, StatusCode> {
    let guard = state.obs_client.read().await;
    match guard.as_ref() {
        Some(client) => Ok(Json(client.get_status().await)),
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

pub async fn obs_start_stream(
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, String)> {
    let guard = state.obs_client.read().await;
    match guard.as_ref() {
        Some(client) => {
            client
                .start_stream()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
            Ok(StatusCode::OK)
        }
        None => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "OBS integration not enabled".to_string(),
        )),
    }
}

pub async fn obs_stop_stream(
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, String)> {
    let guard = state.obs_client.read().await;
    match guard.as_ref() {
        Some(client) => {
            client
                .stop_stream()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
            Ok(StatusCode::OK)
        }
        None => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "OBS integration not enabled".to_string(),
        )),
    }
}

// Stream control handlers (start_stream, stop_stream, update_event) are in stream_handlers.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_json_simple_override() {
        let base = serde_json::json!({"a": 1, "b": 2});
        let patch = serde_json::json!({"b": 3});
        let result = merge_json(base, patch);
        assert_eq!(result, serde_json::json!({"a": 1, "b": 3}));
    }

    #[test]
    fn merge_json_nested() {
        let base = serde_json::json!({"s3": {"bucket": "old", "region": "us"}});
        let patch = serde_json::json!({"s3": {"bucket": "new"}});
        let result = merge_json(base, patch);
        assert_eq!(
            result,
            serde_json::json!({"s3": {"bucket": "new", "region": "us"}})
        );
    }

    #[test]
    fn merge_json_depth_limit_stops_recursion() {
        // Build a deeply nested JSON object exceeding MAX_MERGE_DEPTH
        let mut base = serde_json::json!("base_leaf");
        let mut patch = serde_json::json!("patch_leaf");
        for _ in 0..(MAX_MERGE_DEPTH + 5) {
            base = serde_json::json!({"nested": base});
            patch = serde_json::json!({"nested": patch});
        }
        // Should not stack overflow — at depth limit, patch replaces base wholesale
        let result = merge_json(base, patch.clone());
        // The result should be valid JSON (no stack overflow)
        assert!(result.is_object());
    }

    #[test]
    fn merge_json_adds_new_keys() {
        let base = serde_json::json!({"a": 1});
        let patch = serde_json::json!({"b": 2});
        let result = merge_json(base, patch);
        assert_eq!(result, serde_json::json!({"a": 1, "b": 2}));
    }

    #[test]
    fn merge_json_scalar_replaces_object() {
        let base = serde_json::json!({"a": {"nested": 1}});
        let patch = serde_json::json!({"a": "flat"});
        let result = merge_json(base, patch);
        assert_eq!(result, serde_json::json!({"a": "flat"}));
    }
}

// --- Test hooks for CI E2E ---

pub async fn test_s3_block(State(state): State<AppState>) -> StatusCode {
    state
        .s3_upload_blocked
        .store(true, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!("S3 uploads BLOCKED (test hook)");
    StatusCode::OK
}

pub async fn test_s3_unblock(State(state): State<AppState>) -> StatusCode {
    state
        .s3_upload_blocked
        .store(false, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!("S3 uploads UNBLOCKED (test hook)");
    StatusCode::OK
}
