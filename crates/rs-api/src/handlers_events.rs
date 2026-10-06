use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;

use rs_core::db;
use rs_core::models::{StreamingEvent, WsEvent};
use rs_endpoint::s3::S3Client;
use tracing::error;

use crate::state::AppState;

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
