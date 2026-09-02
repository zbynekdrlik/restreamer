use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use rs_core::config::Config;
use rs_core::config_redact;
use rs_core::db;
use rs_core::log_buffer::LogEntry;
use rs_core::models::{
    ChunkStats, ComponentStatus, EndpointConfig, ServiceStatus, StreamingEvent, WsEvent,
};
use rs_endpoint::s3::S3Client;

use crate::state::AppState;

const VALID_SERVICE_TYPES: &[&str] = &["FB", "YT_RTMP", "VIMEO", "INSTAGRAM", "TEST_FILE"];

pub async fn health() -> StatusCode {
    StatusCode::OK
}

pub async fn get_status(State(state): State<AppState>) -> Result<Json<ServiceStatus>, StatusCode> {
    let event = db::get_streaming_event(&state.pool).await.map_err(|e| {
        error!("Failed to get streaming event: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let rtmp_connected = state.inpoint_state.is_connected();
    // Compute how long the RTMP publisher has been "stable". Used by the
    // dashboard to gate the Start-Delivering button until the ingest has
    // been up for `RTMP_STABLE_REQUIRED_SECS` (15s). Zero when no
    // publisher is connected.
    let rtmp_stable_secs = state
        .rtmp_stable_since
        .lock()
        .await
        .map(|t| t.elapsed().as_secs())
        .unwrap_or(0);
    let inpoint = ComponentStatus {
        state: if rtmp_connected {
            "connected".into()
        } else {
            "disconnected".into()
        },
        details: serde_json::json!({
            "rtmp_connected": rtmp_connected,
            "rtmp_stable_secs": rtmp_stable_secs,
            // #354: live ingest A/V skew + latched banner flag (read through
            // the shared InpointState the chunker writes). The dashboard shows
            // the red "restart OBS" banner + gates the Start-Delivering button;
            // the server enforces the gate too.
            "ingest_skew_ms": state.inpoint_state.ingest_skew_ms(),
            "ingest_skew_active": state.inpoint_state.ingest_skew_active(),
        }),
    };

    // #228: derive endpoint + delivery summary from the cached delivery status
    // (refreshed every 2s by the broadcast loop). After an app restart
    // mid-event the loop repopulates the cache within one poll, so the summary
    // reflects true health instead of an empty (false-RED) Default.
    let (endpoint, delivery) = match state.cached_delivery.read() {
        Ok(c) => crate::status_summary::summarize_delivery(&c),
        Err(_) => (ComponentStatus::default(), ComponentStatus::default()),
    };

    // #231: expose the local chunk-store disk-pressure level so the dashboard
    // can show a dedicated banner (warn at 80%, critical at 90%). The disk
    // monitor publishes the level into this shared atomic every 10s.
    let disk_pressure = rs_endpoint::disk_pressure::DiskPressure::from_u8(
        state
            .disk_pressure_level
            .load(std::sync::atomic::Ordering::Relaxed),
    )
    .as_str()
    .to_string();

    // #278 + #84: read the LIVE config (config_live) once so a runtime config
    // patch is reflected in both the S3-region flag and the long-stream
    // threshold.
    let (s3_region_standard, long_stream_warn_secs) = state
        .config_live
        .read()
        .map(|c| (c.s3_region_is_standard(), c.delivery.long_stream_warn_secs))
        .unwrap_or((true, 0));

    // #84: warn when the current delivery has been running longer than the
    // operator threshold. Computed live (auto-clears when delivery stops)
    // through the shared rs-core helper so the Tauri IPC path matches exactly.
    let long_stream_warning = rs_core::long_stream::is_long_running_now(
        &state.pool,
        event.as_ref(),
        long_stream_warn_secs,
    )
    .await;

    Ok(Json(ServiceStatus {
        inpoint,
        endpoint,
        delivery,
        streaming_event: event,
        disk_pressure,
        s3_region_standard,
        long_stream_warning,
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

/// `GET /api/v1/config` — the config as the dashboard sees it, with every
/// credential masked.
///
/// Redaction is deny-by-default ([`rs_core::config_redact`], #336): a field is
/// masked because its NAME looks like a credential, not because someone
/// remembered to add it to a list. The previous hardcoded list silently leaked
/// two credentials that were added to `Config` after it was written.
pub async fn get_config(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let config_arc = state
        .config_live
        .read()
        .map(|c| c.clone())
        .unwrap_or_else(|_| state.config.clone());
    // Serializing `Config` cannot fail in practice (no non-string map keys, no
    // floats) — mapped to a 500 rather than unwrapped, because a panic in a
    // handler is never the better failure mode.
    let mut value = serde_json::to_value(&*config_arc).map_err(|e| {
        error!("Failed to serialize config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    config_redact::redact_secrets(&mut value);
    Ok(Json(value))
}

pub async fn patch_config(
    State(state): State<AppState>,
    Json(updates): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let current_config = state
        .config_live
        .read()
        .map(|c| c.clone())
        .unwrap_or_else(|_| state.config.clone());

    let current = serde_json::to_value(&*current_config).map_err(|e| {
        error!("Failed to serialize current config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Capture the list of top-level sections patched for the audit row.
    // We only record top-level keys (e.g. "obs", "s3") rather than deep
    // paths because the per-leaf diff is surfaced via WsEvent elsewhere.
    let patched_fields: Vec<String> = match &updates {
        serde_json::Value::Object(map) => map.keys().cloned().collect(),
        _ => Vec::new(),
    };

    let mut merged = merge_json(current.clone(), updates);

    // Two things at once (#336, #273): restore credentials the client echoed
    // back as the mask, so a dashboard round-trip cannot overwrite a real
    // credential with "***"; and refuse any change to `api.access`, because a
    // door that can be unlocked through the door it guards is not a lock.
    config_redact::sanitize_patch(&mut merged, &current);

    let new_config: Config = serde_json::from_value(merged).map_err(|e| {
        tracing::warn!("Invalid config update: {e}");
        StatusCode::BAD_REQUEST
    })?;

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

    let mut response = serde_json::to_value(&new_config).map_err(|e| {
        error!("Failed to serialize config: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    config_redact::redact_secrets(&mut response);

    // Audit: record config change. Only top-level section names are recorded,
    // never values, so the audit row carries no credential material.
    rs_core::audit::record(
        &state.audit_tx,
        rs_core::audit::AuditRow {
            severity: rs_core::audit::Severity::Info,
            source: rs_core::audit::Source::Operator,
            event_id: None,
            instance_id: None,
            endpoint: None,
            action: rs_core::audit::Action::ConfigChanged,
            detail: serde_json::json!({ "patched_fields": patched_fields }),
            ts_override: None,
        },
    );

    Ok(Json(response))
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
    // Serialize with other S3 mutation handlers (clear-s3, delete-event).
    // See AppState::s3_mutation_lock doc.
    let _guard = state.s3_mutation_lock.lock().await;

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
    //
    // Wrapped in a timeout so we can't hang a reverse proxy on a slow S3
    // endpoint (same bound as S3_OPERATION_TIMEOUT in s3_handlers.rs).
    let event_prefix = config.event_s3_prefix(&event.name);
    let delete_future = s3_client.delete_event_chunks(&event_prefix);
    match tokio::time::timeout(std::time::Duration::from_secs(180), delete_future).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error!(
                "Failed to delete S3 chunks for event {id} ({}): {e}",
                event.name
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Err(_) => {
            error!(
                "Timeout deleting S3 chunks for event {id} ({}) after 60s",
                event.name
            );
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    }

    // TOCTOU double-check: if streaming was started during the delete, log
    // a warning. The chunks are already gone — recovery is restart-stream.
    if let Ok(Some(post)) = db::get_streaming_event_by_id(&state.pool, id).await {
        if post.receiving_activated || post.delivering_activated {
            tracing::warn!(
                "delete_event_by_id for {id} ({}) raced against a start-stream — \
                 new chunks may have been deleted during the scan",
                event.name
            );
        }
    }

    // Delete DB records (cascade deletes chunks, endpoint links, etc.)
    db::delete_streaming_event(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to delete event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // #115: best-effort delete of the event's rescue video from S3. Reuses
    // the s3_client already built above for chunk cleanup. Never fails the
    // request -- a stray S3 object is tech debt, not a correctness problem.
    crate::rescue_video_cleanup::cleanup_orphaned_rescue_video(
        &state.pool,
        &s3_client,
        event.rescue_video_url.as_deref(),
        None,
        None,
        Some(id),
    )
    .await;

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

// Stream control handlers (start_stream, stop_stream, update_event) are in stream_handlers.rs

#[cfg(test)]
#[path = "handlers_tests.rs"]
mod tests;
