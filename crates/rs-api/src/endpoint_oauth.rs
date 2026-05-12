//! `POST /endpoints/{id}/link-oauth` handler.
//! Extracted from `handlers.rs` to keep that file under the 1000-line
//! workspace cap.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use tracing::error;

use crate::state::AppState;

#[derive(serde::Deserialize)]
pub struct LinkOauthBody {
    pub oauth_id: Option<i64>,
}

pub async fn link_endpoint_oauth(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Json(body): Json<LinkOauthBody>,
) -> Result<StatusCode, StatusCode> {
    // Validate endpoint exists -> 404, else FK violation becomes 500.
    let endpoint = rs_core::db::v2::get_endpoint_config(&state.pool, id)
        .await
        .map_err(|e| {
            error!("link_endpoint_oauth: endpoint lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or_else(|| {
            error!("link_endpoint_oauth: endpoint id={id} not found");
            StatusCode::NOT_FOUND
        })?;

    // Validate oauth_id exists when Some -> 404, prevents FK 500.
    if let Some(oauth_id) = body.oauth_id {
        let oauth = rs_core::db::youtube_oauth::get_oauth_by_id(&state.pool, oauth_id)
            .await
            .map_err(|e| {
                error!("link_endpoint_oauth: oauth lookup failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        if oauth.is_none() {
            error!("link_endpoint_oauth: oauth_id={oauth_id} not found");
            return Err(StatusCode::NOT_FOUND);
        }
    }

    let prior_oauth_id = endpoint.youtube_oauth_id;
    rs_core::db::v2::set_endpoint_youtube_oauth_id(&state.pool, id, body.oauth_id)
        .await
        .map_err(|e| {
            error!("link_endpoint_oauth failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Audit: linking an OAuth grant to an endpoint changes which channel's
    // quota is consumed. Operator-visible config change.
    rs_core::audit::record(
        &state.audit_tx,
        rs_core::audit::AuditRow {
            severity: rs_core::audit::Severity::Info,
            source: rs_core::audit::Source::Operator,
            event_id: None,
            instance_id: None,
            endpoint: Some(endpoint.alias.clone()),
            action: rs_core::audit::Action::ConfigChanged,
            detail: serde_json::json!({
                "kind": "endpoint_oauth_link",
                "endpoint_id": id,
                "endpoint_alias": endpoint.alias,
                "prior_oauth_id": prior_oauth_id,
                "new_oauth_id": body.oauth_id,
            }),
            ts_override: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}
