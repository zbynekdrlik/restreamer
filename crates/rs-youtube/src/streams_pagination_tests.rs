//! Tests for paginated `liveStreams.list` / `liveBroadcasts.list` (issue #200).
//!
//! Google's API caps a single response at `maxResults=50`. Accounts with
//! more than 50 streams require following `nextPageToken` to enumerate all
//! items; otherwise downstream callers see false-negative
//! `stream_not_in_mine_list`.

use crate::streams::{list_live_broadcasts, list_live_streams};
use std::sync::OnceLock;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serializes tests that mutate the process-global YOUTUBE_API_BASE env.
fn env_guard() -> &'static tokio::sync::Mutex<()> {
    static M: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    M.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn item(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "snippet": { "title": id, "channelId": "UCabc" },
        "status": { "streamStatus": "ready" },
    })
}

fn broadcast(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "snippet": { "title": id },
        "status": { "lifeCycleStatus": "ready" },
    })
}

#[tokio::test]
async fn list_live_streams_follows_next_page_token() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    // Page 1 (no pageToken in request) → 50 items + nextPageToken="PAGE2"
    let page1 = serde_json::json!({
        "items": (0..50).map(|i| item(&format!("s{i}"))).collect::<Vec<_>>(),
        "nextPageToken": "PAGE2",
    });
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(query_param("mine", "true"))
        .and(query_param("maxResults", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page1))
        .expect(1)
        .mount(&server)
        .await;

    // Page 2 (with pageToken=PAGE2) → 13 items + no nextPageToken
    let page2 = serde_json::json!({
        "items": (50..63).map(|i| item(&format!("s{i}"))).collect::<Vec<_>>(),
    });
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(query_param("mine", "true"))
        .and(query_param("pageToken", "PAGE2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page2))
        .expect(1)
        .mount(&server)
        .await;

    let streams = list_live_streams("TOK").await.unwrap();
    assert_eq!(
        streams.len(),
        63,
        "should accumulate items across both pages"
    );
    assert_eq!(streams[0].id, "s0");
    assert_eq!(streams[62].id, "s62");

    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

#[tokio::test]
async fn list_live_broadcasts_follows_next_page_token() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let page1 = serde_json::json!({
        "items": (0..50).map(|i| broadcast(&format!("b{i}"))).collect::<Vec<_>>(),
        "nextPageToken": "P2",
    });
    Mock::given(method("GET"))
        .and(path("/liveBroadcasts"))
        .and(query_param("mine", "true"))
        .and(query_param("maxResults", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page1))
        .expect(1)
        .mount(&server)
        .await;

    let page2 = serde_json::json!({
        "items": (50..55).map(|i| broadcast(&format!("b{i}"))).collect::<Vec<_>>(),
    });
    Mock::given(method("GET"))
        .and(path("/liveBroadcasts"))
        .and(query_param("mine", "true"))
        .and(query_param("pageToken", "P2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page2))
        .expect(1)
        .mount(&server)
        .await;

    let bs = list_live_broadcasts("TOK").await.unwrap();
    assert_eq!(bs.len(), 55);

    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

#[tokio::test]
async fn list_live_streams_caps_at_hard_limit() {
    // Hard cap is 500 items / 10 pages — beyond that we warn and stop
    // to avoid runaway quota burn on a misbehaving API.
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    // Every page returns 50 items + nextPageToken pointing at itself.
    // If the cap doesn't kick in, the test hangs / infinite-loops.
    let body = serde_json::json!({
        "items": (0..50).map(|i| item(&format!("loop{i}"))).collect::<Vec<_>>(),
        "nextPageToken": "LOOP",
    });
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&body))
        .mount(&server)
        .await;

    let streams = list_live_streams("TOK").await.unwrap();
    // 10 pages × 50 = 500 items max.
    assert_eq!(
        streams.len(),
        500,
        "hard cap should stop the loop at 500 items"
    );

    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}
