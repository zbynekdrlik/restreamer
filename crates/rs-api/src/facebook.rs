//! Facebook-side handlers for the restreamer API.
//!
//! Currently exposes a single CI-only endpoint `POST /api/v1/facebook/config/seed`
//! used by the `e2e-fb-push-stream-lan` CI job to install a deterministic
//! FB endpoint row on stream.lan before each test run. This avoids manual
//! operator setup and keeps the test target idempotent across CI invocations.
//!
//! Unlike YouTube, FB has no OAuth/refresh-token concept on our side — the
//! stream key is a persistent secret tied to the dedicated test broadcast.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::{error, info};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct FacebookConfigSeedRequest {
    pub alias: String,
    pub stream_key: String,
}

pub async fn facebook_config_seed(
    State(state): State<AppState>,
    Json(req): Json<FacebookConfigSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    if req.stream_key.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if req.alias.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let rows = rs_core::db::v2::list_endpoint_configs(&state.pool)
        .await
        .map_err(|e| {
            error!("facebook seed: list_endpoint_configs failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(existing) = rows.iter().find(|e| e.alias == req.alias) {
        sqlx::query(
            "UPDATE endpoint_configs \
             SET stream_key = ?1, service_type = 'FB', pusher = 'rust', \
                 updated_at = datetime('now') \
             WHERE id = ?2",
        )
        .bind(&req.stream_key)
        .bind(existing.id)
        .execute(&state.pool)
        .await
        .map_err(|e| {
            error!("facebook seed: update failed for id={}: {e}", existing.id);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        info!(
            "facebook endpoint '{}' (id={}) updated with new stream key",
            req.alias, existing.id
        );
    } else {
        let id = rs_core::db::v2::create_endpoint_config(
            &state.pool,
            &req.alias,
            "FB",
            &req.stream_key,
            false,
        )
        .await
        .map_err(|e| {
            error!("facebook seed: create failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        info!("facebook endpoint '{}' created (id={})", req.alias, id);
    }

    Ok(StatusCode::OK)
}
