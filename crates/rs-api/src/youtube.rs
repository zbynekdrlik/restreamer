use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::state::AppState;

#[derive(Debug, Default, serde::Deserialize)]
pub struct OAuthStartQuery {
    #[serde(default)]
    pub label: Option<String>,
}

/// Whitelist labels to `[a-z0-9_]{1,32}`. Anything else falls back to
/// `default` to avoid SQL injection / path traversal via the query string.
pub fn parse_label_from_query(q: &OAuthStartQuery) -> String {
    let raw = q.label.as_deref().unwrap_or("");
    let ok = !raw.is_empty()
        && raw.len() <= 32
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok {
        raw.to_string()
    } else {
        "default".to_string()
    }
}


#[derive(Debug, serde::Serialize)]
pub struct YouTubeStatusPerChannel {
    pub label: String,
    pub channel_id: Option<String>,
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub error: Option<String>,
    pub connected_at: Option<String>,
}

pub async fn check_all_youtube_status(pool: &sqlx::SqlitePool) -> Vec<YouTubeStatusPerChannel> {
    let oauths = match rs_core::db::youtube_oauth::list_oauths(pool).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("list_oauths failed: {e}");
            return vec![];
        }
    };
    let mut out = Vec::with_capacity(oauths.len());
    for o in oauths {
        if o.refresh_token.is_empty() {
            out.push(YouTubeStatusPerChannel {
                label: o.label,
                channel_id: o.channel_id,
                authenticated: false,
                stream_receiving: None,
                error: None,
                connected_at: o.connected_at,
            });
            continue;
        }
        let receiving = rs_youtube::streams::list_streams_for_label(pool, &o.label)
            .await
            .ok()
            .map(|streams| streams.iter().any(|s| s.status.stream_status == "active"));
        out.push(YouTubeStatusPerChannel {
            label: o.label,
            channel_id: o.channel_id,
            authenticated: true,
            stream_receiving: receiving,
            error: None,
            connected_at: o.connected_at,
        });
    }
    out
}

pub async fn youtube_status(
    State(state): State<AppState>,
) -> Json<Vec<YouTubeStatusPerChannel>> {
    Json(check_all_youtube_status(&state.pool).await)
}

#[derive(Deserialize)]
pub struct YouTubeOAuthSeedRequest {
    pub label: String,
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
}

pub async fn youtube_oauth_seed(
    State(state): State<AppState>,
    Json(req): Json<YouTubeOAuthSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    rs_core::db::youtube_oauth::upsert_oauth_by_label(
        &state.pool,
        &req.label,
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
        error!("seed failed for label '{}': {e}", req.label);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    tracing::info!("youtube oauth seeded for label '{}'", req.label);
    Ok(StatusCode::OK)
}

#[derive(Serialize)]
pub struct YouTubeOAuthStartResponse {
    pub url: String,
}

pub async fn youtube_oauth_start(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<OAuthStartQuery>,
) -> Result<Json<YouTubeOAuthStartResponse>, StatusCode> {
    let yt_config = &state.config.youtube;
    if yt_config.client_id.is_empty() || yt_config.client_secret.is_empty() {
        error!("YouTube OAuth client_id or client_secret not configured");
        return Err(StatusCode::BAD_REQUEST);
    }

    let label = parse_label_from_query(&q);

    let config = rs_youtube::YouTubeConfig {
        client_id: yt_config.client_id.clone(),
        client_secret: yt_config.client_secret.clone(),
    };
    let redirect_uri = "http://127.0.0.1:8910/api/v1/youtube/oauth/callback";
    let base = rs_youtube::oauth::authorization_url(&config, redirect_uri);

    // Append `state=<label>` so the callback can recover which grant to upsert.
    // The `authorization_url` helper does not include `state` itself.
    // The label whitelist `[a-z0-9_]{1,32}` means no URL encoding is needed.
    let url = format!("{base}&state={label}");

    Ok(Json(YouTubeOAuthStartResponse { url }))
}

#[derive(Deserialize)]
pub struct YouTubeOAuthCallbackParams {
    pub code: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
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
        .map(|secs| (chrono::Utc::now() + chrono::Duration::seconds(secs)).to_rfc3339());

    let label = {
        let raw = params.state.as_deref().unwrap_or("");
        let ok = !raw.is_empty()
            && raw.len() <= 32
            && raw
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if ok {
            raw.to_string()
        } else {
            "default".to_string()
        }
    };

    rs_core::db::youtube_oauth::upsert_oauth_by_label(
        &state.pool,
        &label,
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

    tracing::info!(label = %label, "YouTube OAuth tokens stored successfully");

    Ok(axum::response::Html(
        "<html><body><h1>YouTube Authorized Successfully</h1>\
         <p>You can close this tab. The refresh token has been stored.</p></body></html>"
            .to_string(),
    ))
}

pub async fn list_oauths(
    State(state): State<AppState>,
) -> Result<Json<Vec<rs_core::models::YouTubeOAuth>>, StatusCode> {
    rs_core::db::youtube_oauth::list_oauths(&state.pool)
        .await
        .map(Json)
        .map_err(|e| {
            error!("list_oauths failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}
