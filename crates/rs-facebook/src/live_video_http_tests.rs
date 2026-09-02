//! HTTP-path tests for `fetch_page_live_videos` using wiremock, exercising the
//! success parse and the Graph error-status mapping. Base URL is redirected via
//! `FB_GRAPH_API_BASE`. These tests set a process-global env var, so they are
//! serialized behind a mutex to avoid cross-test interference.

use crate::FacebookError;
use crate::live_video::fetch_page_live_videos;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Async mutex: its guard is safe to hold across `.await` (clippy
// `await_holding_lock` only fires on a std Mutex).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn fetch_parses_receiving_live_video() {
    let _guard = ENV_LOCK.lock().await;
    let server = MockServer::start().await;
    let body = serde_json::json!({
        "data": [{
            "id": "999",
            "status": "LIVE",
            "ingest_streams": { "data": [{
                "is_master": true,
                "stream_health": { "video_bitrate": 3500000.0, "video_framerate": 30.0,
                                   "video_width": 1280, "video_height": 720, "audio_bitrate": 128000.0 }
            }]}
        }]
    });
    Mock::given(method("GET"))
        .and(path("/v21.0/PAGE/live_videos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    unsafe { std::env::set_var("FB_GRAPH_API_BASE", server.uri()) };
    let videos = fetch_page_live_videos("tok", "PAGE", "v21.0")
        .await
        .unwrap();
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    assert_eq!(videos.len(), 1);
    assert!(videos[0].is_live());
    assert_eq!(
        videos[0]
            .master_ingest()
            .unwrap()
            .stream_health
            .as_ref()
            .unwrap()
            .video_bitrate,
        Some(3500000.0)
    );
}

#[tokio::test]
async fn fetch_maps_graph_error_status_and_message() {
    let _guard = ENV_LOCK.lock().await;
    let server = MockServer::start().await;
    let body = serde_json::json!({
        "error": { "message": "Error validating access token", "type": "OAuthException", "code": 190 }
    });
    Mock::given(method("GET"))
        .and(path("/v21.0/PAGE/live_videos"))
        .respond_with(ResponseTemplate::new(400).set_body_json(body))
        .mount(&server)
        .await;

    unsafe { std::env::set_var("FB_GRAPH_API_BASE", server.uri()) };
    let err = fetch_page_live_videos("tok", "PAGE", "v21.0")
        .await
        .unwrap_err();
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    match err {
        FacebookError::Api { status, message } => {
            assert_eq!(status, 400);
            assert!(message.contains("validating access token"));
        }
        other => panic!("expected Api error, got {other:?}"),
    }
}
