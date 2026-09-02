//! Graph API `live_videos` edge — response types + fetch.
//!
//! We query `GET /<page_id>/live_videos?fields=id,status,ingest_streams{stream_health,is_master}`
//! and inspect the currently-receiving `live_video` for the page. The
//! `stream_health` object (`video_bitrate`, `video_framerate`, `video_width`,
//! `video_height`, `audio_bitrate`) is FB's measured view of the ingest it is
//! decoding from us — a non-zero `video_bitrate` on a `LIVE` object is proof FB
//! is receiving. See developers.facebook.com live-video-input-stream reference.

use crate::{FacebookError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Default Graph API base URL. Overridable via `FB_GRAPH_API_BASE` for tests
/// (wiremock) — the same pattern `rs-youtube` uses with `YOUTUBE_API_BASE`.
fn graph_api_base() -> String {
    std::env::var("FB_GRAPH_API_BASE").unwrap_or_else(|_| "https://graph.facebook.com".to_string())
}

/// One `live_video` object as returned by the `live_videos` edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveVideo {
    pub id: String,
    /// `LIVE` | `LIVE_NOW` | `UNPUBLISHED` | `PROCESSING` | `LIVE_STOPPED` |
    /// `VOD` | `SCHEDULED_*`. Absent on some historical objects.
    #[serde(default)]
    pub status: Option<String>,
    /// The ingest input streams FB expands under `ingest_streams{...}`. FB wraps
    /// the edge in `{ "data": [ ... ] }`.
    #[serde(default)]
    pub ingest_streams: Option<IngestStreamsEdge>,
}

/// FB wraps an expanded edge in `{ "data": [ ... ] }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct IngestStreamsEdge {
    #[serde(default)]
    pub data: Vec<IngestStream>,
}

/// One input (ingest) stream of a live video.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestStream {
    /// `true` if this input is the one being served to viewers.
    #[serde(default)]
    pub is_master: Option<bool>,
    #[serde(default)]
    pub stream_health: Option<StreamHealth>,
}

/// FB's measured health of the ingest input (what it is decoding from us).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamHealth {
    #[serde(default)]
    pub video_bitrate: Option<f64>,
    #[serde(default)]
    pub video_framerate: Option<f64>,
    #[serde(default)]
    pub video_width: Option<f64>,
    #[serde(default)]
    pub video_height: Option<f64>,
    #[serde(default)]
    pub audio_bitrate: Option<f64>,
}

/// Top-level `{ "data": [ live_video, ... ], "paging": {...} }` response.
#[derive(Debug, Deserialize)]
struct LiveVideosResponse {
    #[serde(default)]
    data: Vec<LiveVideo>,
}

/// Graph API error envelope: `{ "error": { "message": ..., "code": ... } }`.
#[derive(Debug, Deserialize)]
struct GraphErrorEnvelope {
    error: GraphError,
}

#[derive(Debug, Deserialize)]
struct GraphError {
    message: String,
}

impl LiveVideo {
    /// The master ingest stream if flagged, else the first ingest stream.
    pub fn master_ingest(&self) -> Option<&IngestStream> {
        let streams = self.ingest_streams.as_ref()?;
        streams
            .data
            .iter()
            .find(|s| s.is_master == Some(true))
            .or_else(|| streams.data.first())
    }

    /// `true` when FB reports the object as currently live/receiving.
    pub fn is_live(&self) -> bool {
        matches!(self.status.as_deref(), Some("LIVE") | Some("LIVE_NOW"))
    }
}

impl StreamHealth {
    /// `"<width>x<height>"` when both dimensions are present.
    pub fn resolution(&self) -> Option<String> {
        match (self.video_width, self.video_height) {
            (Some(w), Some(h)) if w > 0.0 && h > 0.0 => Some(format!("{}x{}", w as i64, h as i64)),
            _ => None,
        }
    }
}

/// Fetch the `live_videos` edge for a page and return every returned object.
/// Caller (`classify_fb_health`) decides which one is the receiving broadcast.
///
/// A non-2xx response is surfaced as [`FacebookError::Api`] carrying the Graph
/// `error.message` when parseable (so the dashboard can map 190→oauth_invalid,
/// 200→permission, etc.), never swallowed.
pub async fn fetch_page_live_videos(
    access_token: &str,
    page_id: &str,
    api_version: &str,
) -> Result<Vec<LiveVideo>> {
    if access_token.trim().is_empty() {
        return Err(FacebookError::Other("empty access token".to_string()));
    }
    if page_id.trim().is_empty() {
        return Err(FacebookError::Other("empty page id".to_string()));
    }
    let url = format!(
        "{}/{}/{}/live_videos",
        graph_api_base(),
        api_version.trim_matches('/'),
        page_id
    );
    let resp = Client::new()
        .get(&url)
        .query(&[
            (
                "fields",
                "id,status,ingest_streams{stream_health,is_master}",
            ),
            ("access_token", access_token),
        ])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        let message = serde_json::from_str::<GraphErrorEnvelope>(&body)
            .map(|e| e.error.message)
            .unwrap_or(body);
        return Err(FacebookError::Api { status, message });
    }

    let parsed: LiveVideosResponse = resp.json().await?;
    Ok(parsed.data)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Recorded, sanitised Graph API JSON (no tokens) — the shape FB returns for
    // `GET /<page>/live_videos?fields=id,status,ingest_streams{stream_health,is_master}`.
    const RECEIVING_FIXTURE: &str = r#"{
      "data": [
        {
          "id": "1122334455",
          "status": "LIVE",
          "ingest_streams": {
            "data": [
              {
                "is_master": true,
                "stream_health": {
                  "video_bitrate": 2304481.75,
                  "video_framerate": 29.97,
                  "video_gop_size": 2000,
                  "video_width": 1920,
                  "video_height": 1080,
                  "audio_bitrate": 128000.0
                }
              }
            ]
          }
        }
      ],
      "paging": { "cursors": { "before": "x", "after": "y" } }
    }"#;

    const UNBOUND_FIXTURE: &str = r#"{ "data": [] }"#;

    const ERROR_FIXTURE: &str = r#"{
      "error": {
        "message": "Error validating access token: Session has expired.",
        "type": "OAuthException",
        "code": 190
      }
    }"#;

    #[test]
    fn deserialize_receiving_live_video_with_stream_health() {
        let resp: LiveVideosResponse = serde_json::from_str(RECEIVING_FIXTURE).unwrap();
        assert_eq!(resp.data.len(), 1);
        let lv = &resp.data[0];
        assert_eq!(lv.id, "1122334455");
        assert!(lv.is_live());
        let ingest = lv.master_ingest().expect("master ingest present");
        assert_eq!(ingest.is_master, Some(true));
        let health = ingest.stream_health.as_ref().unwrap();
        assert!(health.video_bitrate.unwrap() > 0.0);
        assert_eq!(health.video_framerate, Some(29.97));
        // `video_gop_size` is present in the wire JSON but not modeled — must be
        // ignored, never a deserialize failure.
        assert_eq!(health.resolution().as_deref(), Some("1920x1080"));
    }

    #[test]
    fn deserialize_empty_page_has_no_live_videos() {
        let resp: LiveVideosResponse = serde_json::from_str(UNBOUND_FIXTURE).unwrap();
        assert!(resp.data.is_empty());
    }

    #[test]
    fn deserialize_graph_error_envelope() {
        let env: GraphErrorEnvelope = serde_json::from_str(ERROR_FIXTURE).unwrap();
        assert!(env.error.message.contains("Session has expired"));
    }

    #[test]
    fn master_ingest_falls_back_to_first_when_no_master_flag() {
        let lv = LiveVideo {
            id: "1".into(),
            status: Some("LIVE".into()),
            ingest_streams: Some(IngestStreamsEdge {
                data: vec![
                    IngestStream {
                        is_master: None,
                        stream_health: Some(StreamHealth {
                            video_bitrate: Some(100.0),
                            video_framerate: Some(30.0),
                            video_width: None,
                            video_height: None,
                            audio_bitrate: None,
                        }),
                    },
                    IngestStream {
                        is_master: Some(false),
                        stream_health: None,
                    },
                ],
            }),
        };
        let m = lv.master_ingest().unwrap();
        assert_eq!(m.stream_health.as_ref().unwrap().video_bitrate, Some(100.0));
    }

    #[test]
    fn resolution_is_none_without_dimensions() {
        let h = StreamHealth {
            video_bitrate: Some(1.0),
            video_framerate: Some(30.0),
            video_width: None,
            video_height: None,
            audio_bitrate: None,
        };
        assert!(h.resolution().is_none());
    }

    #[tokio::test]
    async fn fetch_rejects_empty_credentials() {
        assert!(fetch_page_live_videos("", "123", "v21.0").await.is_err());
        assert!(fetch_page_live_videos("tok", "", "v21.0").await.is_err());
    }
}
