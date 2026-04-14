use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::error;

use rs_core::db;
use rs_core::models::{EndpointConfig, EventTemplate};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateTemplateRequest {
    pub name: String,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateTemplateRequest {
    pub name: Option<String>,
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

pub async fn list_templates(
    State(state): State<AppState>,
) -> Result<Json<Vec<EventTemplate>>, StatusCode> {
    let templates = db::list_templates(&state.pool).await.map_err(|e| {
        error!("Failed to list templates: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(templates))
}

pub async fn create_template(
    State(state): State<AppState>,
    Json(req): Json<CreateTemplateRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    if req.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = db::create_template(
        &state.pool,
        &req.name,
        req.cache_delay_secs,
        req.rescue_video_url,
    )
    .await
    .map_err(|e| {
        error!("Failed to create template: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn get_template(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<EventTemplate>, StatusCode> {
    let template = db::get_template_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(template))
}

pub async fn update_template(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<UpdateTemplateRequest>,
) -> Result<StatusCode, StatusCode> {
    let existing = db::get_template_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let new_name = req.name.as_deref().unwrap_or(&existing.name);
    let new_cache_delay = if req.cache_delay_secs.is_some() {
        req.cache_delay_secs
    } else {
        existing.cache_delay_secs
    };
    // For rescue_video_url, we merge: if the request provides it, use it
    // (empty string means "clear"); otherwise preserve existing.
    let new_rescue_url = req
        .rescue_video_url
        .clone()
        .or(existing.rescue_video_url.clone());

    db::update_template(&state.pool, id, new_name, new_cache_delay, new_rescue_url)
        .await
        .map_err(|e| {
            error!("Failed to update template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::OK)
}

pub async fn delete_template(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::delete_template(&state.pool, id).await.map_err(|e| {
        error!("Failed to delete template {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_template_endpoints(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<EndpointConfig>>, StatusCode> {
    let endpoints = db::get_template_endpoints(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get endpoints for template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(endpoints))
}

pub async fn attach_endpoint_to_template(
    State(state): State<AppState>,
    Path((template_id, endpoint_id)): Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::attach_endpoint_to_template(&state.pool, template_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to attach endpoint {endpoint_id} to template {template_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::CREATED)
}

pub async fn detach_endpoint_from_template(
    State(state): State<AppState>,
    Path((template_id, endpoint_id)): Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::detach_endpoint_from_template(&state.pool, template_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to detach endpoint {endpoint_id} from template {template_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}
