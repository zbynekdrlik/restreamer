//! Tests for `list_streams_for_label`: token refresh + correct bearer per label.

use crate::streams::list_streams_for_label;
use crate::test_env::env_guard;
use rs_core::db::youtube_oauth as yo;
use rs_core::db::{create_memory_pool, run_migrations};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn pool_with_label(
    label: &str,
    access_token: &str,
    expires_at: Option<&str>,
) -> sqlx::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(
        &pool,
        label,
        access_token,
        "refresh-token",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "https://www.googleapis.com/auth/youtube.readonly",
        expires_at,
    )
    .await
    .unwrap();
    pool
}

#[tokio::test]
async fn list_streams_for_label_sends_bearer_for_that_label() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }

    let pool = pool_with_label("bb", "TOK-BB", Some("2099-01-01T00:00:00Z")).await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(query_param("mine", "true"))
        .and(header("authorization", "Bearer TOK-BB"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"items":[]}"#))
        .expect(1)
        .mount(&server)
        .await;

    let streams = list_streams_for_label(&pool, "bb").await.unwrap();
    assert!(streams.is_empty());
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}

#[tokio::test]
async fn list_streams_for_label_refreshes_when_expired() {
    let _g = env_guard().lock().await;
    let server = MockServer::start().await;
    unsafe {
        std::env::set_var("YOUTUBE_API_BASE", server.uri());
    }
    let token_uri = format!("{}/token", server.uri());
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "OLD-TOK",
        "refresh-bb",
        &token_uri,
        "cid",
        "csec",
        "https://www.googleapis.com/auth/youtube.readonly",
        Some("2000-01-01T00:00:00Z"),
    )
    .await
    .unwrap();

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"NEW-TOK","expires_in":3600,"token_type":"Bearer"}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer NEW-TOK"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"items":[]}"#))
        .expect(1)
        .mount(&server)
        .await;

    let _ = list_streams_for_label(&pool, "bb").await.unwrap();

    let row = yo::get_oauth_by_label(&pool, "bb").await.unwrap().unwrap();
    assert_eq!(row.access_token, "NEW-TOK");
    unsafe {
        std::env::remove_var("YOUTUBE_API_BASE");
    }
}
