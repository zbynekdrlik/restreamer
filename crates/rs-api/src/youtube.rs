use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::error;

use crate::state::AppState;

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
