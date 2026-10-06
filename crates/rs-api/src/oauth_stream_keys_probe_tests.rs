//! The probing half of the OAuth auto-suggest (#199): `build_map` against a
//! mocked YouTube `liveStreams.list`, and the TTL cache in front of it.
//! A child module, so it reaches the private `build_map` / `cached_map`.

use super::*;
use rs_core::db::youtube_oauth as yo;
use rs_core::db::{create_memory_pool, run_migrations};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const STREAM_A: &str = r#"{"items":[{"id":"s1","snippet":{"title":"t"},"status":{"streamStatus":"active"},"cdn":{"ingestionInfo":{"streamName":"key-a"}}}]}"#;

/// Two authorized grants with unexpired access tokens, so no token refresh.
async fn pool_with_two_grants() -> (sqlx::SqlitePool, i64) {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    for (label, token) in [("a", "TOK-A"), ("b", "TOK-B")] {
        yo::upsert_oauth_by_label(
            &pool,
            label,
            token,
            "refresh",
            "https://oauth2.googleapis.com/token",
            "cid",
            "csec",
            "https://www.googleapis.com/auth/youtube.readonly",
            Some("2099-01-01T00:00:00Z"),
        )
        .await
        .unwrap();
    }
    let id_a = yo::get_oauth_by_label(&pool, "a")
        .await
        .unwrap()
        .unwrap()
        .id;
    (pool, id_a)
}

async fn mock_youtube(expect_a: u64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer TOK-A"))
        .respond_with(ResponseTemplate::new(200).set_body_string(STREAM_A))
        .expect(expect_a)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer TOK-B"))
        .respond_with(ResponseTemplate::new(500).set_body_string("backend error"))
        .mount(&server)
        .await;
    server
}

/// Every authorized grant is probed: grant a's stream name is in the map,
/// and grant b's failed probe is OMITTED (never "owns nothing") and clears
/// `probed_ok`, so a "no owner" verdict stays honest.
#[tokio::test]
async fn build_map_lists_each_probed_grant_and_flags_a_failed_probe() {
    let _env = crate::yt_health_test_env::env_guard().lock().await;
    let server = mock_youtube(1).await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()) };
    let (pool, id_a) = pool_with_two_grants().await;

    let (map, probed_ok) = build_map(&pool).await;
    unsafe { std::env::remove_var("YOUTUBE_API_BASE") };

    assert_eq!(
        map,
        vec![OauthStreamKeys {
            oauth_id: id_a,
            stream_names: vec!["key-a".to_string()],
        }]
    );
    assert!(!probed_ok, "grant b's probe failed");
}

/// Inside the TTL a second lookup is served from the cache: one probe per
/// grant (`expect(1)`, checked when the mock server drops), same verdict.
#[tokio::test]
async fn cached_map_probes_once_within_the_ttl() {
    let _env = crate::yt_health_test_env::env_guard().lock().await;
    *cache_lock() = None;
    let server = mock_youtube(1).await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()) };
    let (pool, id_a) = pool_with_two_grants().await;

    let first = cached_map(&pool).await;
    let second = cached_map(&pool).await;
    unsafe { std::env::remove_var("YOUTUBE_API_BASE") };
    *cache_lock() = None;

    let expected = (
        vec![OauthStreamKeys {
            oauth_id: id_a,
            stream_names: vec!["key-a".to_string()],
        }],
        false,
    );
    assert_eq!(first, expected);
    assert_eq!(second, expected, "the cached map, not a re-probe");
}
