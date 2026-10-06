//! HTTP-path tests for `fetch_page_live_videos` using wiremock, exercising the
//! success parse, the Bearer-auth header, the Graph error-code mapping, and the
//! token-never-in-error guarantee. Base URL is redirected via
//! `FB_GRAPH_API_BASE`. These tests set a process-global env var, so they are
//! serialized behind an async mutex to avoid cross-test interference.

use crate::FacebookError;
use crate::live_video::fetch_page_live_videos;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Async mutex: its guard is safe to hold across `.await` (clippy
// `await_holding_lock` only fires on a std Mutex).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn fetch_parses_receiving_live_video_and_sends_bearer_token() {
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
        .and(path("/v21.0/163104934022649/live_videos"))
        // Proves the token travels in the Authorization header, NOT the URL.
        .and(header("authorization", "Bearer secret-tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    unsafe { std::env::set_var("FB_GRAPH_API_BASE", server.uri()) };
    let videos = fetch_page_live_videos("secret-tok", "163104934022649", "v21.0")
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
async fn fetch_maps_graph_error_code_and_message() {
    let _guard = ENV_LOCK.lock().await;
    let server = MockServer::start().await;
    // Graph returns HTTP 400 with error.code 190 for an expired/invalid token.
    let body = serde_json::json!({
        "error": { "message": "Error validating access token", "type": "OAuthException", "code": 190 }
    });
    Mock::given(method("GET"))
        .and(path("/v21.0/163104934022649/live_videos"))
        .respond_with(ResponseTemplate::new(400).set_body_json(body))
        .mount(&server)
        .await;

    unsafe { std::env::set_var("FB_GRAPH_API_BASE", server.uri()) };
    let err = fetch_page_live_videos("tok", "163104934022649", "v21.0")
        .await
        .unwrap_err();
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    match err {
        FacebookError::Api {
            status,
            code,
            message,
        } => {
            assert_eq!(status, 400);
            assert_eq!(code, Some(190));
            assert!(message.contains("validating access token"));
        }
        other => panic!("expected Api error, got {other:?}"),
    }
}

#[tokio::test]
async fn transport_error_never_contains_the_token() {
    let _guard = ENV_LOCK.lock().await;
    // Point at a closed port so the request fails at connect. The Display of the
    // resulting error must NOT contain the token (Bearer header + without_url).
    unsafe { std::env::set_var("FB_GRAPH_API_BASE", "http://127.0.0.1:1") };
    let err = fetch_page_live_videos("SUPER-SECRET-TOKEN", "163104934022649", "v21.0")
        .await
        .unwrap_err();
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    let rendered = format!("{err}");
    assert!(
        !rendered.contains("SUPER-SECRET-TOKEN"),
        "token leaked into error: {rendered}"
    );
    assert!(matches!(err, FacebookError::Http(_)));
}

#[tokio::test]
async fn fetch_rejects_non_digit_page_id() {
    let err = fetch_page_live_videos("tok", "PAGE", "v21.0")
        .await
        .unwrap_err();
    assert!(matches!(err, FacebookError::Other(_)));
}
