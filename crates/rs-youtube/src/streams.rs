/// YouTube Live Streaming API queries.
use crate::{Result, YouTubeError};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Default YouTube Data API base URL. Overridable via the
/// `YOUTUBE_API_BASE` environment variable for tests (wiremock).
fn youtube_api_base() -> String {
    std::env::var("YOUTUBE_API_BASE")
        .unwrap_or_else(|_| "https://www.googleapis.com/youtube/v3".to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStream {
    pub id: String,
    pub snippet: StreamSnippet,
    pub status: StreamStatus,
    #[serde(default)]
    pub cdn: Option<StreamCdn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSnippet {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamStatus {
    pub stream_status: String,
    #[serde(default)]
    pub health_status: Option<HealthStatus>,
}

/// CDN configuration and actual stream details from YouTube.
/// If resolution/frameRate are populated, YouTube successfully decoded the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCdn {
    #[serde(default)]
    pub ingestion_type: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub frame_rate: Option<String>,
    #[serde(default)]
    pub ingestion_info: Option<IngestionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestionInfo {
    #[serde(default)]
    pub stream_name: Option<String>,
    #[serde(default)]
    pub ingestion_address: Option<String>,
    #[serde(default)]
    pub backup_ingestion_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub status: String,
    #[serde(default)]
    pub configuration_issues: Vec<ConfigurationIssue>,
    #[serde(default)]
    pub last_update_time_seconds: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigurationIssue {
    #[serde(rename = "type")]
    pub issue_type: String,
    pub severity: String,
    pub reason: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveBroadcast {
    pub id: String,
    pub snippet: BroadcastSnippet,
    pub status: BroadcastStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastSnippet {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastStatus {
    pub life_cycle_status: String,
}

/// List live streams (mine=True) to check stream health.
pub async fn list_live_streams(access_token: &str) -> Result<Vec<LiveStream>> {
    let client = Client::new();
    let resp = client
        .get(format!("{}/liveStreams", youtube_api_base()))
        .bearer_auth(access_token)
        .query(&[("part", "id,snippet,status,cdn"), ("mine", "true")])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(YouTubeError::Api {
            status,
            message: body,
        });
    }

    let body: ListResponse<LiveStream> = resp.json().await?;
    Ok(body.items)
}

/// Check if any live stream is actively receiving data.
pub async fn is_stream_receiving(access_token: &str) -> Result<bool> {
    let streams = list_live_streams(access_token).await?;
    Ok(streams.iter().any(|s| s.status.stream_status == "active"))
}

/// List all live broadcasts (mine=true, all types, all states).
pub async fn list_live_broadcasts(access_token: &str) -> Result<Vec<LiveBroadcast>> {
    let client = Client::new();
    // Try mine=true first to get all broadcasts for the authenticated user.
    // This returns broadcasts in all lifecycle states (ready, testing, live, complete).
    let resp = client
        .get(format!("{}/liveBroadcasts", youtube_api_base()))
        .bearer_auth(access_token)
        .query(&[("part", "id,snippet,status"), ("mine", "true")])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(YouTubeError::Api {
            status,
            message: body,
        });
    }

    let body: ListResponse<LiveBroadcast> = resp.json().await?;
    Ok(body.items)
}

/// Check if any broadcast is in "testing" state (video preview is playing).
/// This is the definitive check — "testing" means YouTube successfully decoded
/// the stream and the preview is rendering. If the broadcast stays in "ready"
/// despite streamStatus=="active", the stream data is invalid/unplayable.
pub async fn is_broadcast_testing(access_token: &str) -> Result<bool> {
    let broadcasts = list_live_broadcasts(access_token).await?;
    Ok(broadcasts
        .iter()
        .any(|b| b.status.life_cycle_status == "testing"))
}

/// Get the lifecycle status of all broadcasts for diagnostics.
pub async fn get_broadcast_statuses(access_token: &str) -> Result<Vec<(String, String)>> {
    let broadcasts = list_live_broadcasts(access_token).await?;
    Ok(broadcasts
        .into_iter()
        .map(|b| (b.snippet.title, b.status.life_cycle_status))
        .collect())
}

/// Refresh-if-needed wrapper around `list_live_streams`. Uses the OAuth
/// grant identified by `label` from `youtube_oauth`.
///
/// - If `expires_at` is in the past (or absent), refreshes via the
///   `token_uri` and persists the new access token + expiry.
/// - Always passes the resulting bearer to `liveStreams.list(mine=true)`.
pub async fn list_streams_for_label(
    pool: &sqlx::SqlitePool,
    label: &str,
) -> crate::Result<Vec<LiveStream>> {
    use rs_core::db::youtube_oauth as yo;

    let mut oauth = yo::get_oauth_by_label(pool, label)
        .await
        .map_err(|e| crate::YouTubeError::Db(e.to_string()))?
        .ok_or_else(|| crate::YouTubeError::OAuth(format!("no oauth grant for label '{label}'")))?;

    if crate::oauth::is_token_expired(oauth.expires_at.as_deref()) {
        let tokens = crate::OAuthTokens {
            access_token: oauth.access_token.clone(),
            refresh_token: oauth.refresh_token.clone(),
            token_uri: oauth.token_uri.clone(),
            client_id: oauth.client_id.clone(),
            client_secret: oauth.client_secret.clone(),
            scopes: oauth.scopes.clone(),
            expires_at: oauth.expires_at.clone(),
        };
        let refreshed = crate::oauth::refresh_access_token(&tokens).await?;
        let new_expires =
            chrono::Utc::now() + chrono::Duration::seconds(refreshed.expires_in.unwrap_or(3600));
        let new_expires_str = new_expires.to_rfc3339();
        yo::upsert_oauth_by_label(
            pool,
            label,
            &refreshed.access_token,
            &oauth.refresh_token,
            &oauth.token_uri,
            &oauth.client_id,
            &oauth.client_secret,
            &oauth.scopes,
            Some(&new_expires_str),
        )
        .await
        .map_err(|e| crate::YouTubeError::Db(e.to_string()))?;
        oauth.access_token = refreshed.access_token;
        oauth.expires_at = Some(new_expires_str);
    }

    list_live_streams(&oauth.access_token).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_stream_deserialize() {
        let json = serde_json::json!({
            "id": "stream-123",
            "snippet": {"title": "Test Stream"},
            "status": {
                "streamStatus": "active",
                "healthStatus": {"status": "good"}
            }
        });
        let stream: LiveStream = serde_json::from_value(json).unwrap();
        assert_eq!(stream.id, "stream-123");
        assert_eq!(stream.status.stream_status, "active");
        assert_eq!(stream.status.health_status.unwrap().status, "good");
    }

    #[test]
    fn live_broadcast_deserialize() {
        let json = serde_json::json!({
            "id": "broadcast-456",
            "snippet": {"title": "Sunday Service"},
            "status": {"lifeCycleStatus": "live"}
        });
        let broadcast: LiveBroadcast = serde_json::from_value(json).unwrap();
        assert_eq!(broadcast.id, "broadcast-456");
        assert_eq!(broadcast.status.life_cycle_status, "live");
    }

    #[test]
    fn list_response_deserialize() {
        let json = serde_json::json!({
            "items": [
                {
                    "id": "s1",
                    "snippet": {"title": "Stream 1"},
                    "status": {"streamStatus": "active"}
                },
                {
                    "id": "s2",
                    "snippet": {"title": "Stream 2"},
                    "status": {"streamStatus": "ready"}
                }
            ]
        });
        let resp: ListResponse<LiveStream> = serde_json::from_value(json).unwrap();
        assert_eq!(resp.items.len(), 2);
        assert_eq!(resp.items[0].status.stream_status, "active");
        assert_eq!(resp.items[1].status.stream_status, "ready");
    }
}
