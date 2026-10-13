//! HTTP + WebSocket API of the control rooms (#388), behind the same Access
//! gate as the rest of `/api/v1`.
//!
//! - `GET  /control-rooms[?event_id=N]`: the destinations to show as tabs.
//!   With `event_id`, that event's endpoints; else the active event's; with
//!   no active event, every endpoint (so accounts can be signed in early).
//! - `POST /control-rooms/{endpoint_id}/open`: open (or reuse) the room.
//! - `PUT  /control-rooms/{endpoint_id}/account`: set its sign-in account.
//! - `GET  /control-rooms/ws?target=T`: the screencast relay of tab T.

use axum::Json;
use axum::extract::{Path, Query, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rs_core::db;
use rs_core::models::EndpointConfig;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{Account, Destination};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct RoomsQuery {
    pub event_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct EventRef {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct RoomsResponse {
    /// The event the tabs belong to; `None` = no active event (all endpoints).
    pub event: Option<EventRef>,
    pub destinations: Vec<Destination>,
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!("control rooms: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

/// The endpoints the tabs are for, and the event they belong to.
async fn scope(
    state: &AppState,
    event_id: Option<i64>,
) -> anyhow::Result<(Option<EventRef>, Vec<EndpointConfig>)> {
    let event = match event_id {
        Some(id) => db::get_streaming_event_by_id(&state.pool, id).await?,
        None => db::get_streaming_event(&state.pool)
            .await?
            .filter(|e| e.receiving_activated),
    };
    Ok(match event {
        Some(e) => (
            Some(EventRef {
                id: e.id,
                name: e.name.clone(),
            }),
            db::get_event_endpoints(&state.pool, e.id).await?,
        ),
        None => (None, db::list_endpoint_configs(&state.pool).await?),
    })
}

pub async fn list(State(state): State<AppState>, Query(q): Query<RoomsQuery>) -> Response {
    match scope(&state, q.event_id).await {
        Ok((event, endpoints)) => {
            let destinations = state.control_rooms.status(&endpoints).await;
            Json(RoomsResponse {
                event,
                destinations,
            })
            .into_response()
        }
        Err(e) => internal(e),
    }
}

pub async fn open(State(state): State<AppState>, Path(endpoint_id): Path<i64>) -> Response {
    let endpoint = match db::get_endpoint_config(&state.pool, endpoint_id).await {
        Ok(Some(e)) => e,
        Ok(None) => return (StatusCode::NOT_FOUND, "unknown endpoint").into_response(),
        Err(e) => return internal(e),
    };
    let accounts = state.control_rooms.accounts();
    let Some(dest) = super::destinations(std::slice::from_ref(&endpoint), &accounts).pop() else {
        return (
            StatusCode::BAD_REQUEST,
            "this endpoint type has no control room",
        )
            .into_response();
    };
    tracing::info!(
        "control rooms: opening {} ({}) in profile {}",
        dest.alias,
        dest.url,
        dest.profile
    );
    let sources = match db::list_endpoint_configs(&state.pool).await {
        Ok(all) => super::clone_sources(&dest, &all, &accounts),
        Err(e) => {
            tracing::warn!("control rooms: endpoint list failed, no profile cloning: {e}");
            vec![]
        }
    };
    match state.control_rooms.open(&dest, &sources).await {
        Ok(target_id) => Json(json!({ "target_id": target_id })).into_response(),
        Err(e) => {
            tracing::error!("control rooms: opening {} failed: {e:#}", dest.alias);
            (StatusCode::SERVICE_UNAVAILABLE, format!("{e:#}")).into_response()
        }
    }
}

pub async fn set_account(
    State(state): State<AppState>,
    Path(endpoint_id): Path<i64>,
    Json(account): Json<Account>,
) -> Response {
    match db::get_endpoint_config(&state.pool, endpoint_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "unknown endpoint").into_response(),
        Err(e) => return internal(e),
    }
    match state.control_rooms.set_account(endpoint_id, account) {
        Ok(()) => {
            tracing::info!("control rooms: account of endpoint {endpoint_id} updated");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => internal(format!("{e:#}")),
    }
}

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    pub target: String,
}

pub async fn ws(
    State(state): State<AppState>,
    Query(q): Query<WsQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let pages = match state.control_rooms.browser.cdp.pages().await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Chrome is not running: {e:#}"),
            )
                .into_response();
        }
    };
    let Some(page) = pages.into_iter().find(|p| p.target_id == q.target) else {
        return (StatusCode::NOT_FOUND, "no such tab").into_response();
    };
    let cdp = state.control_rooms.browser.cdp.clone();
    upgrade
        .max_message_size(1 << 20)
        .on_upgrade(move |socket| super::relay::relay(socket, cdp, page.target_id, page.url))
}

#[cfg(test)]
#[path = "handlers_tests.rs"]
mod tests;
