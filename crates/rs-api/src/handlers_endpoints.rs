use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;

use rs_core::config_redact::REDACTED;
use rs_core::db;
use rs_core::models::EndpointConfig;
use tracing::{error, info};

use crate::state::AppState;

const VALID_SERVICE_TYPES: &[&str] = &["FB", "YT_RTMP", "VIMEO", "INSTAGRAM", "TEST_FILE"];

/// The stream key an endpoint read route serves instead of the stored one
/// (#391). Those routes are internet-reachable behind Access and their bodies
/// land in logs and transcripts, so the key never leaves the box. `***` (the
/// config-redaction mask) plus the last 4 characters lets the operator tell
/// keys apart. A key shorter than `MIN_TAILED_KEY_CHARS` gets no tail, so the
/// tail never reveals a third or more of a key. Internal consumers (VPS init,
/// YT health matching, oauth-suggest) read the DB row, not this.
pub fn mask_stream_key(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() < MIN_TAILED_KEY_CHARS {
        return REDACTED.to_string();
    }
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{REDACTED}{tail}")
}

/// Shortest stream key whose masked form shows its last 4 characters.
const MIN_TAILED_KEY_CHARS: usize = 13;

/// An endpoint row as the read routes serve it: stream key masked.
pub fn masked_endpoint(mut ep: EndpointConfig) -> EndpointConfig {
    ep.stream_key = mask_stream_key(&ep.stream_key);
    ep
}

/// A PUT key that is empty or still carries the mask means "keep the stored
/// key": a client echoing a listed row back must never write `***abcd` over
/// the real key (#391). A real stream key never starts with the mask.
fn is_unchanged_key(key: &str) -> bool {
    let key = key.trim();
    key.is_empty() || key.starts_with(REDACTED)
}

pub async fn list_endpoints(
    State(state): State<AppState>,
) -> Result<Json<Vec<EndpointConfig>>, StatusCode> {
    let endpoints = db::list_endpoint_configs(&state.pool).await.map_err(|e| {
        error!("Failed to list endpoints: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(endpoints.into_iter().map(masked_endpoint).collect()))
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
    Ok(Json(masked_endpoint(endpoint)))
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

    let stream_key = match req.stream_key.as_deref() {
        Some(key) if !is_unchanged_key(key) => {
            info!("Endpoint {id}: stream key replaced");
            key
        }
        _ => &existing.stream_key,
    };

    db::update_endpoint_config(
        &state.pool,
        id,
        req.alias.as_deref().unwrap_or(&existing.alias),
        req.service_type
            .as_deref()
            .unwrap_or(&existing.service_type),
        stream_key,
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
    Ok(Json(links.into_iter().map(masked_endpoint).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_keys_get_no_tail() {
        assert_eq!(mask_stream_key(""), "***");
        assert_eq!(mask_stream_key("abcd"), "***");
        // Up to 12 characters a 4-character tail would reveal a third or more
        // of the key, so those get the bare mask.
        assert_eq!(mask_stream_key("123456789"), "***");
        assert_eq!(mask_stream_key("123456789abc"), "***");
    }

    #[test]
    fn long_keys_show_only_the_last_four_characters() {
        assert_eq!(mask_stream_key("123456789abcd"), "***abcd");
        assert_eq!(mask_stream_key("new-stream-key-mnop"), "***mnop");
    }

    #[test]
    fn the_tail_is_cut_on_char_boundaries() {
        assert_eq!(mask_stream_key("ľščťžýáíéúäôň-ďĺŕ"), "***-ďĺŕ");
    }

    #[test]
    fn empty_or_masked_keys_mean_unchanged() {
        assert!(is_unchanged_key(""));
        assert!(is_unchanged_key("   "));
        assert!(is_unchanged_key("***"));
        assert!(is_unchanged_key("***abcd"));
        assert!(is_unchanged_key("  ***abcd "));
        assert!(!is_unchanged_key("new-stream-key-mnop"));
        assert!(!is_unchanged_key("a***b"));
    }
}
