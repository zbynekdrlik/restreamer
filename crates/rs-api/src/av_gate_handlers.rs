//! HTTP surface of the A/V-gate session API (#357):
//!
//! - `POST /api/v1/av-gate/session` `{requester, title?}` -> 201
//!   `{session_id, broadcast_id}`
//! - `GET  /api/v1/av-gate/session/{id}` -> the [`SessionView`]
//! - `POST /api/v1/av-gate/session/{id}/stop` -> 202
//!
//! Auth, on top of the router-wide access gate: the request must come from the
//! LAN (a tunneled request is refused even with a valid Cloudflare Access
//! assertion) AND carry `Authorization: Bearer <token>`, where the token is the
//! content of `av_gate.api_token_file`. That file is never part of
//! `config.json`, so `GET`/`PATCH /api/v1/config` can neither read nor change
//! it, and a missing or short file switches the API off (503).
//!
//! [`SessionView`]: crate::av_gate::SessionView

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rs_core::db::av_gate as store;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::access::{Origin, classify};
use crate::av_gate::{SESSION_QUOTA_ESTIMATE, SessionState, SessionView, validate_request};
use crate::av_gate_driver::{CreateOutcome, spawn_create};
use crate::av_gate_rig::{manage_client, session_ctx};
use crate::state::AppState;

/// A shorter token file is treated as not provisioned.
pub const MIN_TOKEN_CHARS: usize = 32;

type Peer = Option<Extension<ConnectInfo<SocketAddr>>>;

#[derive(Debug, Deserialize)]
struct CreateRequest {
    requester: String,
    #[serde(default)]
    title: Option<String>,
}

/// The configured token, read per request (a rotated file applies at once).
/// The error names the problem, never the content.
pub(crate) async fn read_api_token(path: &Path) -> Result<String, String> {
    let raw = tokio::fs::read_to_string(path).await.map_err(|e| {
        format!(
            "av-gate token file {} unreadable: {}",
            path.display(),
            e.kind()
        )
    })?;
    let token = raw.trim_start_matches('\u{FEFF}').trim();
    if token.chars().count() < MIN_TOKEN_CHARS {
        return Err(format!(
            "av-gate token file {} holds fewer than {MIN_TOKEN_CHARS} characters",
            path.display()
        ));
    }
    Ok(token.to_string())
}

/// The `Authorization: Bearer` value, if any.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// Compare two tokens without an early exit on the first differing byte:
/// both are hashed to a fixed length and every byte is folded.
pub(crate) fn tokens_match(given: &str, expected: &str) -> bool {
    let a = Sha256::digest(given.as_bytes());
    let b = Sha256::digest(expected.as_bytes());
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn error(status: StatusCode, code: &str, detail: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": code, "detail": detail.into() })),
    )
        .into_response()
}

/// Why a request was refused before it reached the session API.
#[derive(Debug, PartialEq, Eq)]
enum Denied {
    NotLan,
    NotProvisioned(String),
    Unauthorized,
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        match self {
            Self::NotLan => error(
                StatusCode::FORBIDDEN,
                "lan_only",
                "the av-gate API is LAN-only",
            ),
            Self::NotProvisioned(e) => error(StatusCode::SERVICE_UNAVAILABLE, "not_provisioned", e),
            Self::Unauthorized => {
                let mut resp = error(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Authorization: Bearer <av-gate token> required",
                );
                resp.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    header::HeaderValue::from_static("Bearer"),
                );
                resp
            }
        }
    }
}

/// LAN origin + the bearer token.
async fn authorize(state: &AppState, peer: &Peer, headers: &HeaderMap) -> Result<(), Denied> {
    let addr = peer.as_ref().map(|Extension(ConnectInfo(a))| a);
    if classify(addr, headers) != Origin::Local {
        warn!("av-gate: refused a non-LAN request");
        return Err(Denied::NotLan);
    }
    let expected = read_api_token(Path::new(&state.config.av_gate.api_token_file))
        .await
        .map_err(Denied::NotProvisioned)?;
    match bearer(headers) {
        Some(given) if tokens_match(given, &expected) => Ok(()),
        _ => {
            warn!("av-gate: refused a request without a valid token");
            Err(Denied::Unauthorized)
        }
    }
}

/// `POST /api/v1/av-gate/session`
pub async fn create(
    State(state): State<AppState>,
    peer: Peer,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(denied) = authorize(&state, &peer, &headers).await {
        return denied.into_response();
    }
    let req: CreateRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, "bad_request", e.to_string()),
    };
    let session_id = uuid::Uuid::new_v4().to_string();
    let (requester, title) =
        match validate_request(&req.requester, req.title.as_deref(), &session_id) {
            Ok(v) => v,
            Err(e) => return error(StatusCode::BAD_REQUEST, "bad_request", e),
        };
    let yt = match manage_client(&state) {
        Ok(c) => Arc::new(c),
        Err(e) => return error(StatusCode::SERVICE_UNAVAILABLE, "not_provisioned", e),
    };
    let start = spawn_create(session_ctx(&state), yt, session_id, requester, title);
    let outcome = match start.await {
        Ok(outcome) => outcome,
        Err(e) => CreateOutcome::Internal(format!("the session start task failed: {e}")),
    };
    match outcome {
        CreateOutcome::Created {
            session_id,
            broadcast_id,
        } => (
            StatusCode::CREATED,
            Json(json!({ "session_id": session_id, "broadcast_id": broadcast_id })),
        )
            .into_response(),
        CreateOutcome::Busy(holder) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "busy", "holder": holder })),
        )
            .into_response(),
        CreateOutcome::NotReady => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "starting_up",
            "the boot reconcile of earlier sessions has not finished",
        ),
        CreateOutcome::CleanupPending(sessions) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "cleanup_pending", "sessions": sessions })),
        )
            .into_response(),
        CreateOutcome::QuotaExceeded { spent, budget } => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": "quota",
                "spent": spent,
                "estimate": SESSION_QUOTA_ESTIMATE,
                "budget": budget,
            })),
        )
            .into_response(),
        CreateOutcome::StartFailed { session_id, reason } => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "session_id": session_id,
                "state": SessionState::Failed,
                "reason": reason,
            })),
        )
            .into_response(),
        CreateOutcome::Internal(e) => error(StatusCode::INTERNAL_SERVER_ERROR, "internal", e),
    }
}

/// `GET /api/v1/av-gate/session/{id}`
pub async fn get(
    State(state): State<AppState>,
    peer: Peer,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    if let Err(denied) = authorize(&state, &peer, &headers).await {
        return denied.into_response();
    }
    match store::get(&state.pool, &id).await {
        Ok(Some(row)) => Json(SessionView::from(row)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "not_found", id),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    }
}

/// `POST /api/v1/av-gate/session/{id}/stop`. 202 when the stop was handed to
/// the running session; 200 when the session is already past it
/// (`processing`/`done`/`failed`), so a retried stop is harmless.
pub async fn stop(
    State(state): State<AppState>,
    peer: Peer,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    if let Err(denied) = authorize(&state, &peer, &headers).await {
        return denied.into_response();
    }
    let row = match store::get(&state.pool, &id).await {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", id),
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()),
    };
    let status = if state.av_gate.registry.request_stop(&id) {
        StatusCode::ACCEPTED
    } else if row.state == SessionState::Starting.as_str()
        || row.state == SessionState::Ready.as_str()
    {
        // Unfinished but not running here: the boot reconcile owns it.
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(json!({ "session_id": id, "state": row.state })),
    )
        .into_response()
}

#[cfg(test)]
#[path = "av_gate_handlers_tests.rs"]
mod tests;
