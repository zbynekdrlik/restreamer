//! Tests for `POST /api/v1/facebook/config/seed`.
//!
//! The seed endpoint is CI-only. It upserts a single endpoint row keyed by
//! alias `"e2e fb"` so the `e2e-fb-push-stream-lan` CI job can deterministically
//! configure the rust pusher's test target on every run.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::{create_memory_pool, run_migrations};
use rs_core::models::PusherKind;

use crate::facebook::{FacebookConfigSeedRequest, facebook_config_seed};

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
async fn seed_creates_endpoint_when_absent() {
    let (state, pool) = state_with_pool().await;

    let resp = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "FB-PERSISTENT-KEY-001".to_string(),
        }),
    )
    .await
    .expect("seed handler must return Ok");

    assert_eq!(resp, StatusCode::OK);

    let rows = rs_core::db::v2::list_endpoint_configs(&pool)
        .await
        .expect("list endpoints");
    let fb = rows
        .iter()
        .find(|e| e.alias == "e2e fb")
        .expect("e2e fb endpoint row must exist after seed");

    assert_eq!(fb.service_type, "FB", "service_type must be FB");
    assert_eq!(fb.stream_key, "FB-PERSISTENT-KEY-001");
    assert_eq!(
        fb.pusher,
        PusherKind::Rust,
        "pusher must be Rust (PR #218 default)"
    );
}

#[tokio::test]
async fn seed_updates_existing_endpoint_stream_key() {
    let (state, pool) = state_with_pool().await;

    // First seed installs the row.
    facebook_config_seed(
        State(state.clone()),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "OLD-KEY".to_string(),
        }),
    )
    .await
    .expect("first seed must succeed");

    // Second seed with a different key must update, not duplicate.
    let resp = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "NEW-KEY".to_string(),
        }),
    )
    .await
    .expect("second seed must return Ok");

    assert_eq!(resp, StatusCode::OK);

    let rows = rs_core::db::v2::list_endpoint_configs(&pool)
        .await
        .expect("list endpoints");
    let fb: Vec<_> = rows.iter().filter(|e| e.alias == "e2e fb").collect();
    assert_eq!(fb.len(), 1, "must be exactly one 'e2e fb' row (idempotent)");
    assert_eq!(fb[0].stream_key, "NEW-KEY");
    assert_eq!(fb[0].service_type, "FB");
    assert_eq!(fb[0].pusher, PusherKind::Rust);
}

#[tokio::test]
async fn seed_rejects_empty_stream_key() {
    let (state, _pool) = state_with_pool().await;

    let err = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "".to_string(),
        }),
    )
    .await
    .expect_err("empty key must fail");

    assert_eq!(err, StatusCode::BAD_REQUEST);
}
