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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
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

    let body: ListResponse<LiveStream> = resp.json().await?;
    Ok(body.items)
}

/// Check if any live stream is actively receiving data.
pub async fn is_stream_receiving(access_token: &str) -> Result<bool> {
    let streams = list_live_streams(access_token).await?;
    Ok(streams.iter().any(|s| s.status.stream_status == "active"))
}

/// List active live broadcasts.
pub async fn list_live_broadcasts(access_token: &str) -> Result<Vec<LiveBroadcast>> {
    let client = Client::new();
    let resp = client
        .get(format!("{YOUTUBE_API_BASE}/liveBroadcasts"))
        .bearer_auth(access_token)
        .query(&[
            ("part", "id,snippet,status"),
            ("broadcastStatus", "active"),
            ("broadcastType", "all"),
        ])
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
