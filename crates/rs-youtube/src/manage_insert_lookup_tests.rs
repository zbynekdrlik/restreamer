//! When the post-503 lookup cannot decide whether the insert created the
//! broadcast, `insert_broadcast` must not insert again: a second insert could
//! leave an orphan broadcast on the channel (#435 review).

use super::*;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// A start the lookup cannot parse can never match, so a miss proves nothing.
#[tokio::test]
async fn an_insert_503_with_an_unparsable_start_is_not_inserted_again() {
    let s = server_with_token(3600, 1).await;
    insert_answers(&s, 503, 1).await;
    upcoming(&s, json!([])).await;
    match fast_client(&s).insert_broadcast("t", "not-a-time").await {
        Err(YouTubeError::Api { status, .. }) => assert_eq!(status, 503),
        other => panic!("expected the insert's 503, got {other:?}"),
    }
    s.verify().await;
}

/// The broadcast may sit on a page past the lookup's page cap.
#[tokio::test]
async fn an_insert_lookup_out_of_pages_is_not_inserted_again() {
    let s = server_with_token(3600, 1).await;
    insert_answers(&s, 503, 1).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "x", "snippet": {"title": "other", "scheduledStartTime": "2026-11-24T13:51:05Z"}}],
            "nextPageToken": "more"
        })))
        .expect(MAX_PAGES as u64)
        .mount(&s)
        .await;
    match fast_client(&s)
        .insert_broadcast("t", "2026-11-24T13:51:05Z")
        .await
    {
        Err(YouTubeError::Api { status, .. }) => assert_eq!(status, 503),
        other => panic!("expected the insert's 503, got {other:?}"),
    }
    s.verify().await;
}

/// An empty `nextPageToken` is the last page: the lookup has read everything,
/// found nothing, and the insert is sent again.
#[tokio::test]
async fn an_empty_next_page_token_ends_the_insert_lookup() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "bc-2"})))
        .expect(1)
        .with_priority(2)
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [], "nextPageToken": ""
        })))
        .expect(1)
        .mount(&s)
        .await;
    assert_eq!(
        fast_client(&s)
            .insert_broadcast("t", "2026-11-24T13:51:05Z")
            .await
            .unwrap(),
        "bc-2"
    );
    s.verify().await;
}
