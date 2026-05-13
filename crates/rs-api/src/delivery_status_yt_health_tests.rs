//! Integration test for `attach_yt_health`: given an endpoint linked to an
//! OAuth label and a wiremock'd YT API, the resulting
//! `DeliveryEndpointMetrics.youtube_health` is populated correctly.

use crate::delivery_status::attach_yt_health;
use crate::yt_health_test_env::env_guard;
use rs_core::db::youtube_oauth as yo;
use rs_core::db::{create_memory_pool, run_migrations, v2};
use rs_core::models::{DeliveryEndpointMetrics, EndpointConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn empty_metrics(alias: &str) -> DeliveryEndpointMetrics {
    DeliveryEndpointMetrics {
        alias: alias.into(),
        alive: true,
        current_chunk_id: 0,
        bytes_processed_total: 0,
        chunks_processed: 0,
        chunk_delay_secs: 0.0,
        stall_reason: None,
        ffmpeg_restart_count: 0,
        reconnect_count: 0,
        last_error: None,
        is_fast: false,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
    }
}

async fn pool_with_endpoint(
    label: &str,
    stream_key: &str,
    link: bool,
) -> (sqlx::SqlitePool, EndpointConfig) {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let oauth_id = yo::upsert_oauth_by_label(
        &pool,
        label,
        "TOK",
        "REFRESH",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "https://www.googleapis.com/auth/youtube.readonly",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", stream_key, false)
        .await
        .unwrap();
    if link {
        v2::set_endpoint_youtube_oauth_id(&pool, id, Some(oauth_id))
            .await
            .unwrap();
    }
    let ep = v2::get_endpoint_config(&pool, id).await.unwrap().unwrap();
    (pool, ep)
}

#[tokio::test]
async fn attach_yt_health_populates_for_linked_endpoint() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let (pool, ep) = pool_with_endpoint("bb", "KEY-BB", true).await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer TOK"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{
                "id":"abc",
                "snippet":{"title":"ytbb"},
                "status":{"streamStatus":"active",
                          "healthStatus":{"status":"bad",
                              "configurationIssues":[{
                                  "type":"videoIngestionStarved",
                                  "severity":"warning",
                                  "reason":"videoIngestionStarved"
                              }]}},
                "cdn":{"resolution":"1920x1080","frameRate":"30.0",
                       "ingestionInfo":{"streamName":"KEY-BB"}}
            }]}"#,
        ))
        .mount(&server)
        .await;

    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("must be populated");
    assert_eq!(h.health_status, "bad");
    assert_eq!(h.top_issue.as_deref(), Some("videoIngestionStarved"));
    assert_eq!(h.resolution.as_deref(), Some("1920x1080"));
    assert!(h.error.is_none());
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

#[tokio::test]
async fn attach_yt_health_no_op_when_unlinked() {
    let (pool, ep) = pool_with_endpoint("default", "K", false).await;
    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    assert!(
        m.youtube_health.is_none(),
        "no youtube_oauth_id => no probe"
    );
}

#[tokio::test]
async fn attach_yt_health_marks_error_on_oauth_invalid() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let token_uri = format!("{}/token", server.uri());
    let oauth_id = yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "OLD",
        "BADREFRESH",
        &token_uri,
        "cid",
        "csec",
        "scope",
        Some("2000-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let ep_id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", "K", false)
        .await
        .unwrap();
    v2::set_endpoint_youtube_oauth_id(&pool, ep_id, Some(oauth_id))
        .await
        .unwrap();
    let ep = v2::get_endpoint_config(&pool, ep_id)
        .await
        .unwrap()
        .unwrap();

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid_grant"}"#))
        .mount(&server)
        .await;

    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("error metric must be present");
    assert_eq!(h.error.as_deref(), Some("oauth_invalid"));
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

#[tokio::test]
async fn attach_yt_health_marks_unbound_when_key_not_in_list() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }
    let (pool, ep) = pool_with_endpoint("bb", "KEY-BB", true).await;
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{"id":"x","snippet":{"title":"other"},
                "status":{"streamStatus":"ready"},
                "cdn":{"ingestionInfo":{"streamName":"OTHER-KEY"}}}]}"#,
        ))
        .mount(&server)
        .await;
    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("unbound metric must be present");
    assert_eq!(h.stream_status, "unbound");
    assert_eq!(h.error.as_deref(), Some("stream_not_in_mine_list"));
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}
