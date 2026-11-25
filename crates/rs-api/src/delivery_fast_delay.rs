//! `POST /api/v1/delivery/endpoints/fast-delay` (#437): the operator sets a
//! fast endpoint's read-delay target live during an event.
//!
//! The host forwards `{alias, target_secs}` to the event's delivery VPS
//! (`POST /api/endpoints/fast_delay`, bearer-gated), the same pattern as
//! `/delivery/endpoints/add`. The VPS clamps the target, the producer adopts
//! it on its next fetch, and the VPS audit row `fast_delay_set` is mirrored
//! into the host audit log. The VPS answer (`{alias, from_secs, to_secs}`)
//! and its refusals (404 `not_found`, 409 `not_fast`) pass through, so the
//! dashboard can show exactly what happened.

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use sqlx::SqlitePool;

use crate::delivery::is_delivery_active;
use crate::state::AppState;

/// The delivery VPS API port (`rs-delivery` binds `0.0.0.0:8000`).
pub const VPS_API_PORT: u16 = 8000;

#[derive(Debug, Deserialize)]
pub struct SetFastDelayRequest {
    pub event_id: i64,
    pub alias: String,
    pub target_secs: u64,
}

/// Forward the target to the event's running delivery VPS on `vps_port`.
/// `Err((status, message))` maps straight onto the HTTP answer.
pub async fn set_fast_delay(
    pool: &SqlitePool,
    req: &SetFastDelayRequest,
    vps_port: u16,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let instance = rs_core::db::get_delivery_instance_by_event(pool, req.event_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("no delivery instance for event {}", req.event_id),
            )
        })?;
    if !is_delivery_active(&instance.status) {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "delivery instance is '{}', not an active delivery",
                instance.status
            ),
        ));
    }
    let url = format!(
        "http://{}:{vps_port}/api/endpoints/fast_delay",
        instance.ipv4
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth(&instance.auth_token)
        .json(&serde_json::json!({ "alias": req.alias, "target_secs": req.target_secs }))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.without_url().to_string()))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        let code = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        return Err((code, text));
    }
    serde_json::from_str(&text).map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("VPS answered 200 with an unreadable body: {e}"),
        )
    })
}

pub async fn delivery_set_fast_delay(
    State(state): State<AppState>,
    Json(req): Json<SetFastDelayRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match set_fast_delay(&state.pool, &req, VPS_API_PORT).await {
        Ok(body) => {
            tracing::info!(
                event_id = req.event_id,
                alias = %req.alias,
                target_secs = req.target_secs,
                vps = %body,
                "operator set a fast endpoint's read-delay target (#437)"
            );
            Ok(Json(body))
        }
        Err((status, msg)) => {
            tracing::warn!(
                event_id = req.event_id,
                alias = %req.alias,
                %status,
                "setting the fast read-delay target failed: {msg}"
            );
            Err((status, msg))
        }
    }
}

#[cfg(test)]
#[path = "delivery_fast_delay_tests.rs"]
mod tests;
