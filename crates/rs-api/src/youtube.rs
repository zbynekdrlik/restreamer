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

#[derive(Debug, serde::Serialize)]
pub struct YouTubeStreamInfo {
    pub title: String,
    pub stream_status: String,
    pub health_status: Option<String>,
    pub configuration_issues: Vec<String>,
    pub cdn_resolution: Option<String>,
    pub cdn_frame_rate: Option<String>,
    pub cdn_ingestion_type: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct YouTubeStatusResponse {
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub stream_count: usize,
    pub streams: Vec<YouTubeStreamInfo>,
    pub error: Option<String>,
}

/// Legacy single-channel (`label = "default"`) status endpoint. CI gates +
/// dashboards consume this shape. Multi-channel listing lives at
/// `/youtube/oauths`. Internal refactor kept the shape stable across PR #197.
pub async fn youtube_status(State(state): State<AppState>) -> Json<YouTubeStatusResponse> {
    match rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, "default").await {
        Ok(Some(o)) if !o.refresh_token.is_empty() => {}
        _ => {
            return Json(YouTubeStatusResponse {
                authenticated: false,
                stream_receiving: None,
                stream_count: 0,
                streams: Vec::new(),
                error: None,
            });
        }
    }
    match rs_youtube::streams::list_streams_for_label(&state.pool, "default").await {
        Ok(list) => {
            let stream_receiving = Some(list.iter().any(|s| s.status.stream_status == "active"));
            let stream_count = list.len();
            let streams: Vec<YouTubeStreamInfo> = list
                .into_iter()
                .map(|s| {
                    let issues = s
                        .status
                        .health_status
                        .as_ref()
                        .map(|h| {
                            h.configuration_issues
                                .iter()
                                .map(|i| format!("{}: {} ({})", i.issue_type, i.reason, i.severity))
                                .collect()
                        })
                        .unwrap_or_default();
                    let cdn = s.cdn.as_ref();
                    YouTubeStreamInfo {
                        title: s.snippet.title,
                        stream_status: s.status.stream_status,
                        health_status: s.status.health_status.map(|h| h.status),
                        configuration_issues: issues,
                        cdn_resolution: cdn.and_then(|c| c.resolution.clone()),
                        cdn_frame_rate: cdn.and_then(|c| c.frame_rate.clone()),
                        cdn_ingestion_type: cdn.and_then(|c| c.ingestion_type.clone()),
                    }
                })
                .collect();
            Json(YouTubeStatusResponse {
                authenticated: true,
                stream_receiving,
                stream_count,
                streams,
                error: None,
            })
        }
        Err(e) => Json(YouTubeStatusResponse {
            authenticated: true,
            stream_receiving: None,
            stream_count: 0,
            streams: Vec::new(),
            error: Some(format!("{e}")),
        }),
    }
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
    let all = rs_core::db::youtube_oauth::list_oauths(&state.pool)
        .await
        .map_err(|e| {
            error!("list_oauths failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    // Filter out the migration-v25-seeded empty `default` placeholder so the
    // dashboard's Channels panel only shows actually-authorized grants.
    let authorized: Vec<_> = all
        .into_iter()
        .filter(|o| !o.refresh_token.is_empty())
        .collect();
    Ok(Json(authorized))
}
