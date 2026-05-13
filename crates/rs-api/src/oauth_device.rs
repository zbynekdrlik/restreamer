//! Device Code Flow Axum handlers + background poller.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::state::AppState;

const GOOGLE_OAUTH_BASE: &str = "https://oauth2.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/youtube.readonly openid";
const LABEL_PATTERN: &str = "^[a-z0-9_]{1,32}$";

fn is_valid_label(s: &str) -> bool {
    let re = regex::Regex::new(LABEL_PATTERN).expect("static regex");
    re.is_match(s)
}

#[derive(Debug, Deserialize)]
pub struct DeviceStartBody {
    pub label: String,
}

#[derive(Debug, Serialize)]
pub struct DeviceStartResponse {
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: i64,
}

pub async fn device_start(
    State(state): State<AppState>,
    Json(body): Json<DeviceStartBody>,
) -> Result<Json<DeviceStartResponse>, StatusCode> {
    if !is_valid_label(&body.label) {
        error!("device_start: invalid label '{}'", body.label);
        return Err(StatusCode::BAD_REQUEST);
    }
    if let Some(existing) = rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, &body.label)
        .await
        .map_err(|e| {
            error!("device_start: oauth lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        && !existing.refresh_token.is_empty()
    {
        return Err(StatusCode::CONFLICT);
    }

    let device_cfg = &state.config.youtube.device_flow;
    if device_cfg.client_id.is_empty() || device_cfg.client_secret.is_empty() {
        error!("device_start: youtube.device_flow client_id/secret not configured");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let base = state
        .device_flow_api_base
        .clone()
        .unwrap_or_else(|| GOOGLE_OAUTH_BASE.to_string());

    let resp = rs_youtube::device_flow::request_device_code(&base, &device_cfg.client_id, SCOPE)
        .await
        .map_err(|e| {
            error!("device_start: device/code request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let now = Utc::now();
    let expires_at = (now + chrono::Duration::seconds(resp.expires_in)).to_rfc3339();
    let started_at = now.to_rfc3339();

    rs_core::db::oauth_device_grants::insert(
        &state.pool,
        &body.label,
        &resp.device_code,
        &resp.user_code,
        &resp.verification_url,
        resp.interval,
        &expires_at,
        &started_at,
    )
    .await
    .map_err(|e| {
        error!("device_start: insert grant failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    spawn_grant_poller(
        state.pool.clone(),
        state.audit_tx.clone(),
        base,
        device_cfg.client_id.clone(),
        device_cfg.client_secret.clone(),
        body.label.clone(),
        resp.device_code.clone(),
        resp.interval,
        expires_at.clone(),
    );

    info!("device_start: pending grant for label='{}'", body.label);
    Ok(Json(DeviceStartResponse {
        user_code: resp.user_code,
        verification_url: resp.verification_url,
        expires_in: resp.expires_in,
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_grant_poller(
    pool: sqlx::SqlitePool,
    audit_tx: tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
    api_base: String,
    client_id: String,
    client_secret: String,
    label: String,
    device_code: String,
    initial_interval: i64,
    expires_at: String,
) {
    tokio::spawn(async move {
        // Clamp interval to [1, 60] on entry; cap doublings to 120 below.
        // Without bounds a hostile/malformed Google interval could saturate
        // `u64` (sleep ~584B years), defeating the expiration check.
        let mut interval: u64 = initial_interval.clamp(1, 60) as u64;
        let exp = chrono::DateTime::parse_from_rfc3339(&expires_at)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| Utc::now() + chrono::Duration::seconds(900));
        loop {
            // Check expiry BEFORE sleeping so an oversized interval can't
            // stretch us past the deadline.
            if Utc::now() > exp {
                if let Err(e) =
                    rs_core::db::oauth_device_grants::update_status(&pool, &label, "expired", None)
                        .await
                {
                    warn!("device_poller: mark-expired UPDATE failed for '{label}': {e}");
                }
                warn!("device_poller: label='{label}' expired");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            if Utc::now() > exp {
                if let Err(e) =
                    rs_core::db::oauth_device_grants::update_status(&pool, &label, "expired", None)
                        .await
                {
                    warn!("device_poller: mark-expired UPDATE failed for '{label}': {e}");
                }
                warn!("device_poller: label='{label}' expired");
                return;
            }
            let resp = match rs_youtube::device_flow::poll_token(
                &api_base,
                &client_id,
                &client_secret,
                &device_code,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("device_poller: poll error for '{label}': {e}");
                    continue;
                }
            };
            use rs_youtube::device_flow::PollDecision::*;
            match rs_youtube::device_flow::poll_decision(&resp) {
                Continue => continue,
                DoubleInterval => {
                    // Cap at 120s — a slow_down chain shouldn't push us past
                    // the 15-min expiration window in a single sleep.
                    interval = interval.saturating_mul(2).min(120);
                    continue;
                }
                TerminalDenied => {
                    if let Err(e) = rs_core::db::oauth_device_grants::update_status(
                        &pool, &label, "denied", None,
                    )
                    .await
                    {
                        warn!("device_poller: mark-denied UPDATE failed for '{label}': {e}");
                    }
                    return;
                }
                TerminalExpired => {
                    if let Err(e) = rs_core::db::oauth_device_grants::update_status(
                        &pool, &label, "expired", None,
                    )
                    .await
                    {
                        warn!("device_poller: mark-expired UPDATE failed for '{label}': {e}");
                    }
                    return;
                }
                TerminalError(e) => {
                    if let Err(db_err) = rs_core::db::oauth_device_grants::update_status(
                        &pool,
                        &label,
                        "error",
                        Some(&e),
                    )
                    .await
                    {
                        warn!(
                            "device_poller: mark-error UPDATE failed for '{label}': {db_err} (original: {e})"
                        );
                    }
                    return;
                }
                TerminalGranted {
                    access_token,
                    refresh_token,
                    expires_in,
                    ..
                } => {
                    let new_expires = expires_in
                        .map(|s| (Utc::now() + chrono::Duration::seconds(s)).to_rfc3339());
                    if let Err(e) = rs_core::db::youtube_oauth::upsert_oauth_by_label(
                        &pool,
                        &label,
                        &access_token,
                        &refresh_token,
                        "https://oauth2.googleapis.com/token",
                        &client_id,
                        &client_secret,
                        SCOPE,
                        new_expires.as_deref(),
                    )
                    .await
                    {
                        error!("device_poller: upsert failed for '{label}': {e}");
                        if let Err(db_err) = rs_core::db::oauth_device_grants::update_status(
                            &pool,
                            &label,
                            "error",
                            Some(&format!("upsert: {e}")),
                        )
                        .await
                        {
                            warn!(
                                "device_poller: mark-error UPDATE failed for '{label}': {db_err} (original: {e})"
                            );
                        }
                        return;
                    }
                    let channel_id = rs_youtube::streams::list_streams_for_label(&pool, &label)
                        .await
                        .ok()
                        .and_then(|streams| {
                            streams.first().and_then(|s| s.snippet.channel_id.clone())
                        });
                    let connected_at = Utc::now().to_rfc3339();
                    if let Err(e) = sqlx::query(
                        "UPDATE youtube_oauth SET channel_id = ?1, connected_at = ?2 WHERE label = ?3",
                    )
                    .bind(&channel_id)
                    .bind(&connected_at)
                    .bind(&label)
                    .execute(&pool)
                    .await
                    {
                        warn!("device_poller: channel_id/connected_at UPDATE failed for '{label}': {e}");
                    }
                    if let Err(e) = rs_core::db::oauth_device_grants::delete(&pool, &label).await {
                        warn!("device_poller: delete grant row failed for '{label}': {e}");
                    }
                    let row = rs_core::audit::AuditRow {
                        severity: rs_core::audit::Severity::Info,
                        source: rs_core::audit::Source::Operator,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: rs_core::audit::Action::OAuthGranted,
                        detail: serde_json::json!({
                            "label": label,
                            "channel_id": channel_id,
                            "scopes": SCOPE,
                        }),
                        ts_override: None,
                    };
                    if let Err(e) = audit_tx.send(row).await {
                        warn!("device_poller: audit channel send failed for '{label}': {e}");
                    }
                    info!(
                        "device_poller: label='{label}' GRANTED (channel_id={:?})",
                        channel_id
                    );
                    return;
                }
            }
        }
    });
}

#[derive(Debug, Deserialize)]
pub struct DeviceStatusQuery {
    pub label: String,
}

#[derive(Debug, Serialize)]
pub struct DeviceStatusResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// On startup, scan `oauth_device_grants WHERE status='pending'`. For each
/// row whose `expires_at` is still in the future, spawn a poller. For each
/// row already expired, update its status to `expired`. Returns the number
/// of pollers actually spawned.
pub async fn resume_pending_grants(
    pool: &sqlx::SqlitePool,
    audit_tx: &tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
    api_base: &str,
    client_id: &str,
    client_secret: &str,
) -> sqlx::Result<usize> {
    let pending = rs_core::db::oauth_device_grants::list_pending(pool)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let mut resumed = 0usize;
    for g in pending {
        let exp = chrono::DateTime::parse_from_rfc3339(&g.expires_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        if Utc::now() > exp {
            let _ =
                rs_core::db::oauth_device_grants::update_status(pool, &g.label, "expired", None)
                    .await;
            continue;
        }
        spawn_grant_poller(
            pool.clone(),
            audit_tx.clone(),
            api_base.to_string(),
            client_id.to_string(),
            client_secret.to_string(),
            g.label,
            g.device_code,
            g.interval_secs,
            g.expires_at,
        );
        resumed += 1;
    }
    Ok(resumed)
}

pub async fn device_status(
    State(state): State<AppState>,
    Query(q): Query<DeviceStatusQuery>,
) -> Result<Json<DeviceStatusResponse>, StatusCode> {
    if !is_valid_label(&q.label) {
        return Err(StatusCode::BAD_REQUEST);
    }
    if let Some(o) = rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, &q.label)
        .await
        .map_err(|e| {
            error!("device_status: oauth lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        && !o.refresh_token.is_empty()
    {
        return Ok(Json(DeviceStatusResponse {
            status: "granted".into(),
            user_code: None,
            verification_url: None,
            channel_id: o.channel_id,
            connected_at: o.connected_at,
            error: None,
        }));
    }
    let g = rs_core::db::oauth_device_grants::get_by_label(&state.pool, &q.label)
        .await
        .map_err(|e| {
            error!("device_status: grant lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    match g {
        Some(g) => Ok(Json(DeviceStatusResponse {
            status: g.status,
            user_code: Some(g.user_code),
            verification_url: Some(g.verification_url),
            channel_id: None,
            connected_at: None,
            error: g.error,
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct TestGrantBody {
    pub label: String,
    pub channel_id: Option<String>,
}

/// Test fixture: pretend the operator just completed Device Flow for `label`.
/// Persists a fake refresh token and channel_id, deletes any pending grant,
/// emits the OAuthGranted audit row. Refuses unless the
/// `RESTREAMER_TEST_HOOKS=1` env var is set.
pub async fn test_grant_now(
    State(state): State<AppState>,
    Json(body): Json<TestGrantBody>,
) -> Result<StatusCode, StatusCode> {
    if std::env::var("RESTREAMER_TEST_HOOKS").as_deref() != Ok("1") {
        return Err(StatusCode::NOT_FOUND);
    }
    rs_core::db::youtube_oauth::upsert_oauth_by_label(
        &state.pool,
        &body.label,
        "test_AT",
        "test_RT",
        "https://oauth2.googleapis.com/token",
        "test_cid",
        "test_csec",
        SCOPE,
        Some(&(chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()),
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let now = chrono::Utc::now().to_rfc3339();
    let _ =
        sqlx::query("UPDATE youtube_oauth SET channel_id = ?1, connected_at = ?2 WHERE label = ?3")
            .bind(&body.channel_id)
            .bind(&now)
            .bind(&body.label)
            .execute(&state.pool)
            .await;
    let _ = rs_core::db::oauth_device_grants::delete(&state.pool, &body.label).await;
    let row = rs_core::audit::AuditRow {
        severity: rs_core::audit::Severity::Info,
        source: rs_core::audit::Source::Operator,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: rs_core::audit::Action::OAuthGranted,
        detail: serde_json::json!({"label": body.label, "channel_id": body.channel_id}),
        ts_override: None,
    };
    let _ = state.audit_tx.send(row).await;
    Ok(StatusCode::OK)
}
