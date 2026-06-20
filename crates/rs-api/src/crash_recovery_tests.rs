//! Regression tests for #252 — crash-recovery boot reconciliation.
//!
//! After a stream.lan host/app crash, restarting Restreamer.exe must resume an
//! actively-delivering event WITHOUT any operator POST: re-establish delivery
//! management against the persisted VPS row, re-arm the health monitor, and
//! repopulate the in-memory `endpoint_fast_cache` from persisted DB state so
//! `is_fast` resolves correctly. Recovery resumes at the LIVE EDGE (it does NOT
//! seed `resume_positions` / replay the backlog — strict 1x, see
//! feedback_rtmp_push_always_1x).
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
    // the synchronous reconciliation effects (poll_handles insert, fast-cache
    // repopulation) fire BEFORE that background task awaits the network.
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
/// - per-endpoint delivery_endpoint_status rows (used only to prove recovery
///   IGNORES them — it resumes at the live edge, not from these positions)
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

    // Persisted per-endpoint progress. Boot recovery deliberately does NOT use
    // these for resume (it resumes at the live edge — see the assertion in
    // boot_reconcile_reinits_delivering_event); they are seeded only to prove
    // recovery ignores them rather than replaying from them.
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

    // 3. resume_positions is NOT seeded: boot recovery resumes at the LIVE EDGE,
    //    not by replaying the persisted backlog. poll_and_init's resume branch
    //    starts a missing-position endpoint at MIN(sequence_number) (oldest
    //    chunk) and lacks the S3-existence guard — re-pushing hours of stale
    //    chunks violates the strict-1x rule and gets the stream killed by YT/FB.
    //    Leaving the map empty makes every endpoint take the tested live-edge
    //    path (compute_target_start_chunk + S3 advance), same as operator Start.
    assert!(
        orch.resume_positions_snapshot(event_id).await.is_none(),
        "boot recovery must resume at live edge — resume_positions must stay empty \
         (no backlog replay)"
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

#[tokio::test]
async fn boot_reconcile_does_not_overwrite_existing_poll_handle() {
    // Double-spawn race guard: if the operator pressed Start Delivering after
    // boot began, start_delivery reuses the same live instance_id and inserts
    // its own poll handle. Boot recovery must NOT overwrite it — overwriting
    // drops (detaches, does NOT abort) the operator's JoinHandle, leaving two
    // concurrent poll_and_init -> monitor loops + two /api/init POSTs for one
    // VPS, only one of which stop_delivery can later abort.
    let pool = setup_pool().await;
    let (_event_id, instance_id) = seed_delivering_fixture(&pool).await;
    let orch = orch_with_unreachable_vps(pool);

    // Simulate the operator path already owning a tracked task for this instance.
    let sentinel = tokio::spawn(async {
        // Long-lived; only aborted explicitly below.
        std::future::pending::<()>().await;
    });
    orch.poll_handles()
        .lock()
        .await
        .insert(instance_id, sentinel);

    orch.reconcile_delivery_on_boot(test_ws_tx())
        .await
        .expect("reconciliation is a no-op success when a handle already exists");

    // Exactly one handle for the instance, and it must NOT have been finished
    // (a spawned-then-overwritten replacement would leave the sentinel detached;
    // a replacement task against the unreachable VPS would still be the tracked
    // one). The guard returns BEFORE spawning, so the sentinel survives.
    let poll_handles = orch.poll_handles();
    {
        let handles = poll_handles.lock().await;
        assert_eq!(
            handles.len(),
            1,
            "boot recovery must not add a second handle for the same instance"
        );
        let h = handles
            .get(&instance_id)
            .expect("sentinel handle preserved");
        assert!(
            !h.is_finished(),
            "the operator's tracked task must be left running, not replaced/detached"
        );
    }

    poll_handles
        .lock()
        .await
        .remove(&instance_id)
        .unwrap()
        .abort();
}
