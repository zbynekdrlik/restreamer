//! `attach_yt_health_cached` must hit the YT API at most once per 15 s
//! per endpoint id, even when called repeatedly.

use crate::delivery_status::attach_yt_health_cached;
use crate::yt_health_test_env::env_guard;
use rs_core::db::youtube_oauth as yo;
use rs_core::db::{create_memory_pool, run_migrations, v2};
use rs_core::models::DeliveryEndpointMetrics;
use wiremock::matchers::{method, path};
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

#[tokio::test]
async fn attach_yt_health_cached_calls_api_once_within_window() {
    let _g = env_guard().lock().await;
    crate::delivery_status::clear_yt_health_cache_for_test();
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let oauth_id = yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "TOK",
        "R",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "scope",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let ep_id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", "KEY-BB", false)
        .await
        .unwrap();
    v2::set_endpoint_youtube_oauth_id(&pool, ep_id, Some(oauth_id))
        .await
        .unwrap();
    let ep = v2::get_endpoint_config(&pool, ep_id)
        .await
        .unwrap()
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{"id":"x","snippet":{"title":"ytbb"},
                "status":{"streamStatus":"active",
                    "healthStatus":{"status":"good","configurationIssues":[]}},
                "cdn":{"ingestionInfo":{"streamName":"KEY-BB"}}}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    for _ in 0..3 {
        let mut m = empty_metrics(&ep.alias);
        attach_yt_health_cached(&pool, &ep, &mut m, None).await;
        assert!(m.youtube_health.is_some());
    }
    // wiremock's `.expect(1)` panics on Drop if count != 1.
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

/// Regression for the bug where `poll_delivery_metrics` passed `None` for
/// `audit_tx`, so `YoutubeIssueChanged` rows never fired in production.
/// This test pins the contract that when a `Some(tx)` is provided AND a
/// `top_issue` is observed on the slow path, an audit row is emitted.
#[tokio::test]
async fn attach_yt_health_cached_emits_audit_when_tx_provided() {
    use rs_core::audit::{Action, AuditRow};
    use tokio::sync::mpsc;

    let _g = env_guard().lock().await;
    crate::delivery_status::clear_yt_health_cache_for_test();
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let oauth_id = yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "TOK",
        "R",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "scope",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let ep_id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", "KEY-BB", false)
        .await
        .unwrap();
    v2::set_endpoint_youtube_oauth_id(&pool, ep_id, Some(oauth_id))
        .await
        .unwrap();
    let ep = v2::get_endpoint_config(&pool, ep_id)
        .await
        .unwrap()
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{"id":"x","snippet":{"title":"ytbb"},
                "status":{"streamStatus":"active",
                    "healthStatus":{"status":"bad","configurationIssues":[
                        {"type":"videoIngestionStarved","severity":"error","reason":"ingestion starved"}
                    ]}},
                "cdn":{"ingestionInfo":{"streamName":"KEY-BB"}}}]}"#,
        ))
        .mount(&server)
        .await;

    let (tx, mut rx) = mpsc::channel::<AuditRow>(8);
    let mut m = empty_metrics(&ep.alias);
    attach_yt_health_cached(&pool, &ep, &mut m, Some(&tx)).await;

    assert!(m.youtube_health.is_some(), "health must be attached");
    assert_eq!(
        m.youtube_health.as_ref().unwrap().top_issue.as_deref(),
        Some("videoIngestionStarved"),
        "mock must parse cleanly; if top_issue is None the wiremock body failed to deserialize"
    );
    let row = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("audit row must arrive within 5s")
        .expect("channel must yield row, not close");
    assert_eq!(row.action, Action::YoutubeIssueChanged);
    assert_eq!(row.endpoint.as_deref(), Some("ytbb"));

    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}
