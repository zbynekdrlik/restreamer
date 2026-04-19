//! Integration tests for `add_endpoint_to_delivery` and
//! `remove_endpoint_from_delivery` guard clauses (issue #120).
//!
//! These tests kill two surviving-mutant classes per function:
//! - delete `!` in `if !is_delivery_active(...)` (guard inverts)
//! - replace function body with `Ok(())` (no-op)
//!
//! Both are killed by asserting the error MESSAGE contains the
//! guard's exact substring: "not in an active delivery state".
//! With either mutant applied, the message would not match.

use rs_api::delivery::DeliveryOrchestrator;
use rs_api::delivery_endpoints::{
    StartPosition, add_endpoint_to_delivery, remove_endpoint_from_delivery,
};
use rs_core::config::Config;
use rs_core::db;
use sqlx::SqlitePool;

/// Build an in-memory DB + orchestrator + config seeded with one
/// `endpoint_configs` row and one `delivery_instances` row whose
/// status is `status` and ipv4 points at an RFC 2606 .invalid host
/// (so the mutated `!`-deleted code path fails fast on DNS instead
/// of timing out).
async fn setup_with_status(status: &str) -> (DeliveryOrchestrator, SqlitePool, Config, i64, i64) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let endpoint_id: i64 = sqlx::query_scalar(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key)
         VALUES ('yt', 'YT_HLS', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let event_id: i64 = 42;

    let instance_id = db::create_delivery_instance(
        &pool,
        /* hetzner_id */ 1,
        /* name */ "test-instance",
        /* ipv4 */ "unreachable.invalid",
        /* server_type */ "cx22",
        Some(event_id),
        /* auth_token */ "test-token",
    )
    .await
    .unwrap();

    db::update_delivery_instance_status(&pool, instance_id, status)
        .await
        .unwrap();

    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    let orch = DeliveryOrchestrator::new(pool.clone(), config.clone()).unwrap();

    (orch, pool, config, event_id, endpoint_id)
}

#[tokio::test]
async fn add_endpoint_to_delivery_rejects_inactive_delivery() {
    let (orch, pool, config, event_id, endpoint_id) = setup_with_status("creating").await;
    let err = add_endpoint_to_delivery(
        &orch,
        &pool,
        &config,
        event_id,
        endpoint_id,
        StartPosition::Live,
    )
    .await
    .expect_err("creating state must be rejected by guard clause");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn remove_endpoint_from_delivery_rejects_inactive_delivery() {
    let (orch, pool, _config, event_id, _endpoint_id) = setup_with_status("creating").await;
    let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt")
        .await
        .expect_err("creating state must be rejected by guard clause");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error message: {err}"
    );
}
