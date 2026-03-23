use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use rs_core::db;

use crate::state::AppState;

#[derive(Serialize)]
pub struct YouTubeStatusResponse {
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub broadcast_testing: Option<bool>,
    pub broadcast_statuses: Vec<BroadcastStatusInfo>,
    pub stream_count: usize,
    pub streams: Vec<YouTubeStreamInfo>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct BroadcastStatusInfo {
    pub title: String,
    pub life_cycle_status: String,
}

#[derive(Serialize)]
pub struct YouTubeStreamInfo {
    pub title: String,
    pub stream_status: String,
    pub health_status: Option<String>,
    pub configuration_issues: Vec<String>,
}

pub async fn youtube_status(
    State(state): State<AppState>,
) -> Result<Json<YouTubeStatusResponse>, StatusCode> {
    let orch = state.delivery_orchestrator.as_ref().ok_or_else(|| {
        error!("Delivery orchestrator not configured");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    let status = orch.check_youtube_status().await;

    // Fetch broadcast lifecycle status (testing = video playing in preview)
    let (broadcast_testing, broadcast_statuses) = if status.authenticated
        && status.error.is_none()
    {
        match orch.get_broadcast_statuses().await {
            Ok(statuses) => {
                tracing::info!(
                    "Broadcast statuses: {:?}",
                    statuses
                );
                let testing = statuses.iter().any(|(_, s)| s == "testing");
                let infos = statuses
                    .into_iter()
                    .map(|(title, status)| BroadcastStatusInfo {
                        title,
                        life_cycle_status: status,
                    })
                    .collect();
                (Some(testing), infos)
            }
            Err(e) => {
                tracing::warn!("Failed to fetch broadcast statuses: {e}");
                (None, Vec::new())
            }
        }
    } else {
        (None, Vec::new())
    };

    // Fetch stream details for diagnostics
    let (stream_count, streams) = if status.authenticated && status.error.is_none() {
        match orch.list_youtube_streams().await {
            Ok(list) => {
                let count = list.len();
                let infos: Vec<YouTubeStreamInfo> = list
                    .into_iter()
                    .map(|s| {
                        let issues = s
                            .status
                            .health_status
                            .as_ref()
                            .map(|h| {
                                h.configuration_issues
                                    .iter()
                                    .map(|i| {
                                        format!("{}: {} ({})", i.issue_type, i.reason, i.severity)
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        YouTubeStreamInfo {
                            title: s.snippet.title,
                            stream_status: s.status.stream_status,
                            health_status: s.status.health_status.map(|h| h.status),
                            configuration_issues: issues,
                        }
                    })
                    .collect();
                (count, infos)
            }
            Err(_) => (0, Vec::new()),
        }
    } else {
        (0, Vec::new())
    };

    Ok(Json(YouTubeStatusResponse {
        authenticated: status.authenticated,
        stream_receiving: status.stream_receiving,
        broadcast_testing,
        broadcast_statuses,
        stream_count,
        streams,
        error: status.error,
    }))
}

#[derive(Deserialize)]
pub struct YouTubeOAuthSeedRequest {
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
}

pub async fn youtube_oauth_seed(
    State(state): State<AppState>,
    Json(req): Json<YouTubeOAuthSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    db::upsert_youtube_oauth(
        &state.pool,
        "",
        &req.refresh_token,
        "https://oauth2.googleapis.com/token",
        &req.client_id,
        &req.client_secret,
        "https://www.googleapis.com/auth/youtube.readonly",
        None,
    )
    .await
    .map_err(|e| {
        error!("Failed to seed YouTube OAuth: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!("YouTube OAuth tokens seeded");
    Ok(StatusCode::OK)
}

#[derive(Serialize)]
pub struct YouTubeOAuthStartResponse {
    pub url: String,
}

pub async fn youtube_oauth_start(
    State(state): State<AppState>,
) -> Result<Json<YouTubeOAuthStartResponse>, StatusCode> {
    let yt_config = &state.config.youtube;
    if yt_config.client_id.is_empty() || yt_config.client_secret.is_empty() {
        error!("YouTube OAuth client_id or client_secret not configured");
        return Err(StatusCode::BAD_REQUEST);
    }

    let config = rs_youtube::YouTubeConfig {
        client_id: yt_config.client_id.clone(),
        client_secret: yt_config.client_secret.clone(),
    };
    let redirect_uri = "http://127.0.0.1:8910/api/v1/youtube/oauth/callback";
    let url = rs_youtube::oauth::authorization_url(&config, redirect_uri);

    Ok(Json(YouTubeOAuthStartResponse { url }))
}

#[derive(Deserialize)]
pub struct YouTubeOAuthCallbackParams {
    pub code: Option<String>,
    pub error: Option<String>,
}

pub async fn youtube_oauth_callback(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<YouTubeOAuthCallbackParams>,
) -> Result<axum::response::Html<String>, StatusCode> {
    if let Some(err) = params.error {
        let escaped = err
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        return Ok(axum::response::Html(format!(
            "<html><body><h1>YouTube Authorization Failed</h1><p>{escaped}</p></body></html>"
        )));
    }

    let code = params.code.ok_or_else(|| {
        error!("YouTube OAuth callback missing 'code' parameter");
        StatusCode::BAD_REQUEST
    })?;

    let yt_config = &state.config.youtube;
    let config = rs_youtube::YouTubeConfig {
        client_id: yt_config.client_id.clone(),
        client_secret: yt_config.client_secret.clone(),
    };
    let redirect_uri = "http://127.0.0.1:8910/api/v1/youtube/oauth/callback";

    let tokens = rs_youtube::oauth::exchange_code(&config, &code, redirect_uri)
        .await
        .map_err(|e| {
            error!("YouTube OAuth code exchange failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let expires_at = tokens
        .expires_in
        .map(|secs| (chrono::Utc::now() + chrono::Duration::seconds(secs as i64)).to_rfc3339());

    db::upsert_youtube_oauth(
        &state.pool,
        &tokens.access_token,
        tokens.refresh_token.as_deref().unwrap_or(""),
        "https://oauth2.googleapis.com/token",
        &yt_config.client_id,
        &yt_config.client_secret,
        "https://www.googleapis.com/auth/youtube.readonly",
        expires_at.as_deref(),
    )
    .await
    .map_err(|e| {
        error!("Failed to store YouTube OAuth tokens: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!("YouTube OAuth tokens stored successfully");

    Ok(axum::response::Html(
        "<html><body><h1>YouTube Authorized Successfully</h1>\
         <p>You can close this tab. The refresh token has been stored.</p></body></html>"
            .to_string(),
    ))
}
