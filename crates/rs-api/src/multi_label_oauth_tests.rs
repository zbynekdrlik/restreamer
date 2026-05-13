//! `/youtube/oauth/seed` now requires `label`. `check_youtube_status` returns
//! a per-label array.

use crate::youtube::{YouTubeOAuthSeedRequest, YouTubeStatusPerChannel, youtube_oauth_seed};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::{create_memory_pool, run_migrations};

async fn state_with_pool() -> (crate::state::AppState, sqlx::SqlitePool) {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = tokio::sync::broadcast::channel(16);
    let state = crate::state::AppState::new_for_tests(
        pool.clone(),
        rs_core::config::Config::for_testing(),
        ws_tx,
    );
    (state, pool)
}

#[tokio::test]
async fn seed_with_label_persists_by_label() {
    let (state, pool) = state_with_pool().await;
    let r = youtube_oauth_seed(
        State(state),
        Json(YouTubeOAuthSeedRequest {
            label: "default".into(),
            refresh_token: "RT_DEFAULT".into(),
            client_id: "cid".into(),
            client_secret: "csec".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(r, StatusCode::OK);
    let row = rs_core::db::youtube_oauth::get_oauth_by_label(&pool, "default")
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.refresh_token, "RT_DEFAULT");
}

#[tokio::test]
async fn seed_rejects_missing_label() {
    // Body without label fails Json<YouTubeOAuthSeedRequest> deserialization.
    let body = serde_json::json!({
        "refresh_token": "X",
        "client_id": "cid",
        "client_secret": "csec",
    });
    let parsed: Result<YouTubeOAuthSeedRequest, _> = serde_json::from_value(body);
    assert!(parsed.is_err(), "label must be required");
}

#[tokio::test]
async fn youtube_status_returns_per_channel_array() {
    use rs_core::db::youtube_oauth as yo;
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(
        &pool,
        "default",
        "AT",
        "RT",
        "https://oauth2.googleapis.com/token",
        "c",
        "s",
        "scope",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "AT2",
        "RT2",
        "https://oauth2.googleapis.com/token",
        "c",
        "s",
        "scope",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let v: Vec<YouTubeStatusPerChannel> = crate::youtube::check_all_youtube_status(&pool).await;
    assert!(v.iter().any(|s| s.label == "default" && s.authenticated));
    assert!(v.iter().any(|s| s.label == "bb" && s.authenticated));
}
