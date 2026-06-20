//! Regression tests for #252 — crash-recovery boot reconciliation.
//!
//! After a stream.lan host/app crash, restarting Restreamer.exe must resume an
//! actively-delivering event WITHOUT any operator POST: re-establish delivery
//! management against the persisted VPS row, re-arm the health monitor, and
//! repopulate the in-memory `endpoint_fast_cache` / `resume_positions` from
//! persisted DB state so `is_fast` resolves correctly and each endpoint resumes
//! at its last-delivered chunk.
//!
//! Before the fix, `ServiceCore` only re-spawned RTMP ingest + S3 upload and the
//! sole boot-time delivery task was the read-only `delivery_broadcast_loop`; the
//! orchestrator's in-memory maps stayed empty until an operator clicked
//! "Start Delivering" again. These tests exercise
//! `DeliveryOrchestrator::reconcile_delivery_on_boot` — the boot path invoked
//! from `ServiceCore::run_with_signal` alongside `resume_pending_grants`.

use std::sync::Arc;

use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use sqlx::SqlitePool;
use tokio::sync::broadcast;

use crate::delivery::DeliveryOrchestrator;

async fn setup_pool() -> SqlitePool {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    pool
}

fn orch_with_unreachable_vps(pool: SqlitePool) -> Arc<DeliveryOrchestrator> {
    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    // Point the Hetzner client at an unroutable base URL: the boot path spawns
    // poll_and_init as a background task that WILL fail to reach the VPS, but
    // the synchronous reconciliation effects (poll_handles insert, fast-cache,
    // resume_positions) fire BEFORE that background task awaits the network.
    Arc::new(DeliveryOrchestrator::with_base_url(
        pool,
        config,
        "http://127.0.0.1:1",
    ))
}

/// Throwaway WS broadcast sender for the boot path (the health monitor takes
/// one to surface unreachable warnings; the test ignores the receiver).
fn test_ws_tx() -> broadcast::Sender<WsEvent> {
    broadcast::channel::<WsEvent>(16).0
}

/// Build a "was actively delivering at crash time" fixture:
/// - one streaming_event with delivering_activated = 1
/// - a delivery_instances row in a live state ("delivering")
/// - two endpoints attached (one fast, one non-fast)
/// - per-endpoint delivery_endpoint_status rows recording last-delivered chunk
async fn seed_delivering_fixture(pool: &SqlitePool) -> (i64, i64) {
    let event_id = db::create_streaming_event(pool, "crash-evt").await.unwrap();
    db::set_delivering_activated(pool, event_id, true)
        .await
        .unwrap();

    let fast_ep = db::create_endpoint_config(pool, "yt-fast", "YT_RTMP", "fk", true)
        .await
        .unwrap();
    let slow_ep = db::create_endpoint_config(pool, "fb-slow", "FB", "sk", false)
        .await
        .unwrap();
    db::attach_endpoint_to_event(pool, event_id, fast_ep)
        .await
        .unwrap();
    db::attach_endpoint_to_event(pool, event_id, slow_ep)
        .await
        .unwrap();

    let instance_id = db::create_delivery_instance(
        pool,
        4242,
        "crash-vps",
        "203.0.113.9",
        "cx22",
        Some(event_id),
        "vps-tok",
    )
    .await
    .unwrap();
    // Live state at crash time: the VPS was delivering.
    db::update_delivery_instance_status(pool, instance_id, "delivering")
        .await
        .unwrap();

    // Persisted per-endpoint progress (the resume positions).
    db::upsert_delivery_endpoint_status(pool, instance_id, "yt-fast", true, 50, 1500, 99)
        .await
        .unwrap();
    db::upsert_delivery_endpoint_status(pool, instance_id, "fb-slow", true, 40, 1480, 88)
        .await
        .unwrap();

    (event_id, instance_id)
}

#[tokio::test]
async fn boot_reconcile_reinits_delivering_event() {
    let pool = setup_pool().await;
    let (event_id, instance_id) = seed_delivering_fixture(&pool).await;

    let orch = orch_with_unreachable_vps(pool);

    // Fresh process boot: NO operator POST has run. Maps are empty.
    assert!(
        orch.poll_handles().lock().await.is_empty(),
        "precondition: no poll handle before boot reconciliation"
    );

    // The boot reconciliation that ServiceCore must perform.
    orch.reconcile_delivery_on_boot(test_ws_tx())
        .await
        .expect("boot reconciliation should succeed");

    // 1. Health monitor / poll_and_init re-armed: a JoinHandle is tracked for
    //    the persisted instance — exactly as the operator delivery_start path
    //    would have done.
    assert!(
        orch.poll_handles().lock().await.contains_key(&instance_id),
        "boot reconciliation must spawn a tracked task for the live instance"
    );

    // 2. endpoint_fast_cache repopulated from persisted config so is_fast
    //    resolves correctly (false before init — delivery_status.rs:208-212).
    let fast_cache = orch.endpoint_fast_cache_lock().await;
    let event_cache = fast_cache
        .get(&event_id)
        .expect("fast cache must contain the recovered event");
    assert_eq!(
        event_cache.get("yt-fast").copied(),
        Some(true),
        "fast endpoint must resolve is_fast=true after boot reconciliation"
    );
    assert_eq!(
        event_cache.get("fb-slow").copied(),
        Some(false),
        "non-fast endpoint must resolve is_fast=false after boot reconciliation"
    );
    drop(fast_cache);

    // 3. resume_positions re-seeded from the persisted per-endpoint
    //    current_chunk_id so each endpoint resumes at its last-delivered chunk
    //    instead of recomputing a fresh live edge.
    let resume = orch.resume_positions_snapshot(event_id).await;
    let resume = resume.expect("resume positions must be seeded for the recovered event");
    assert_eq!(
        resume.get("yt-fast").copied(),
        Some(1500),
        "fast endpoint must resume at its persisted current_chunk_id"
    );
    assert_eq!(
        resume.get("fb-slow").copied(),
        Some(1480),
        "non-fast endpoint must resume at its persisted current_chunk_id"
    );
}

#[tokio::test]
async fn boot_reconcile_skips_event_not_delivering() {
    let pool = setup_pool().await;
    let (event_id, _instance_id) = seed_delivering_fixture(&pool).await;
    // Operator had cleanly stopped delivery before the crash → must NOT resume.
    db::set_delivering_activated(&pool, event_id, false)
        .await
        .unwrap();

    let orch = orch_with_unreachable_vps(pool);
    orch.reconcile_delivery_on_boot(test_ws_tx())
        .await
        .expect("reconciliation is a no-op success when nothing was delivering");

    assert!(
        orch.poll_handles().lock().await.is_empty(),
        "no re-init when the event was not delivering at crash time"
    );
    assert!(
        orch.endpoint_fast_cache_lock()
            .await
            .get(&event_id)
            .is_none(),
        "fast cache must stay empty when nothing was delivering"
    );
    assert!(
        orch.resume_positions_snapshot(event_id).await.is_none(),
        "resume positions must stay empty when nothing was delivering"
    );
}

#[tokio::test]
async fn boot_reconcile_skips_dead_instance() {
    let pool = setup_pool().await;
    let (event_id, instance_id) = seed_delivering_fixture(&pool).await;
    // The VPS row is in a dead state (e.g. the crash also killed the VPS and a
    // prior run marked it failed). delivering_activated is still 1, but there is
    // no live instance to manage → must NOT spawn a handle against a dead VPS.
    db::update_delivery_instance_status(&pool, instance_id, "failed")
        .await
        .unwrap();

    let orch = orch_with_unreachable_vps(pool);
    orch.reconcile_delivery_on_boot(test_ws_tx())
        .await
        .expect("reconciliation is a no-op success when the instance is dead");

    assert!(
        orch.poll_handles().lock().await.is_empty(),
        "no re-init when the persisted instance is not in a live state"
    );
    assert!(
        orch.endpoint_fast_cache_lock()
            .await
            .get(&event_id)
            .is_none(),
        "fast cache must stay empty when the instance is dead"
    );
}
