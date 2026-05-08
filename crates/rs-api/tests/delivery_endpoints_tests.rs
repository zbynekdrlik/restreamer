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
         VALUES ('yt', 'YT_RTMP', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let event_id = db::create_streaming_event(&pool, "test-event")
        .await
        .unwrap();

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
    let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt", /*force*/ false)
        .await
        .expect_err("creating state must be rejected by guard clause");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn remove_endpoint_rejects_when_would_leave_zero_and_delivery_active() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "t").await.unwrap();
    sqlx::query("UPDATE streaming_events SET delivering_activated = 1 WHERE id = ?1")
        .bind(event_id)
        .execute(&pool)
        .await
        .unwrap();

    let instance_id = db::create_delivery_instance(
        &pool,
        /* hetzner_id */ 1,
        /* name */ "x",
        /* ipv4 */ "192.0.2.1",
        /* server_type */ "cx22",
        Some(event_id),
        /* auth_token */ "tok",
    )
    .await
    .unwrap();
    db::update_delivery_instance_status(&pool, instance_id, "delivering")
        .await
        .unwrap();

    // Seed delivery_endpoint_status with exactly 1 endpoint so removing it
    // would leave 0 endpoints under active delivery.
    sqlx::query(
        "INSERT INTO delivery_endpoint_status (instance_id, alias, alive, chunks_processed, current_chunk_id, bytes_processed_total)
         VALUES (?1, 'yt1', 1, 0, 0, 0)",
    )
    .bind(instance_id)
    .execute(&pool)
    .await
    .unwrap();

    let mut cfg = Config::for_testing();
    cfg.hetzner.api_token = "tok".into();
    let orch = DeliveryOrchestrator::new(pool.clone(), cfg).unwrap();

    let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt1", /*force*/ false)
        .await
        .expect_err("must reject last-endpoint removal under active delivery");
    assert!(
        err.to_string().contains("would_leave_zero_endpoints"),
        "unexpected error: {err}"
    );

    // force=true passes the guard. The HTTP call to the bogus ipv4 fails,
    // but crucially the guard is no longer the reason.
    let err2 = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt1", /*force*/ true)
        .await
        .expect_err("HTTP call to 192.0.2.1 must fail");
    assert!(
        !err2.to_string().contains("would_leave_zero_endpoints"),
        "force=true should bypass the guard, got: {err2}"
    );
}

#[tokio::test]
async fn start_position_live_resolves_to_latest_plus_one_strict_live_edge() {
    // #174 strict live-edge policy: mid-stream endpoint add with
    // StartPosition::Live must resolve to `latest_sent_seq + 1`, never to
    // a historical chunk. Buffering is the warmup loop's responsibility,
    // not compute_target_start_chunk's.
    use rs_api::delivery_endpoints::{StartPosition, resolve_start_chunk_id};

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "evt-live-test")
        .await
        .unwrap();

    // Insert 100 chunks of 2000 ms each, all sent=1.
    // Strict live-edge policy (#174): Live always returns latest_seq + 1
    // regardless of target_delay_ms. The warmup loop, not compute_target,
    // is what produces the delay buffer (from NEW chunks only).
    for seq in 1i64..=100 {
        sqlx::query(
            "INSERT INTO chunk_records
             (streaming_event_id, chunk_file_path, data_size, md5, sequence_number, duration_ms, sent)
             VALUES (?1, ?2, ?3, '', ?4, 2000, 1)",
        )
        .bind(event_id)
        .bind(format!("c{seq}.bin"))
        .bind(1024_i64)
        .bind(seq)
        .execute(&pool)
        .await
        .unwrap();
    }

    let live = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 120_000)
        .await
        .unwrap();
    assert_eq!(
        live, 101,
        "Live (strict): latest+1, expected 101 got {live}"
    );

    let beg = resolve_start_chunk_id(&pool, event_id, &StartPosition::Beginning, 120_000)
        .await
        .unwrap();
    assert_eq!(beg, 1, "Beginning must resolve to first sequence (1)");

    // target_delay_ms is now legacy and ignored by Live.
    let live_short = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 60_000)
        .await
        .unwrap();
    assert_eq!(
        live_short, 101,
        "Live ignores target_delay_ms: latest+1=101, got {live_short}"
    );
}
