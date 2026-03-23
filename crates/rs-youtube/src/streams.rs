/// YouTube Live Streaming API queries.
use crate::{Result, YouTubeError};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const YOUTUBE_API_BASE: &str = "https://www.googleapis.com/youtube/v3";

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
        .get(format!("{YOUTUBE_API_BASE}/liveStreams"))
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
        .get(format!("{YOUTUBE_API_BASE}/liveBroadcasts"))
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
