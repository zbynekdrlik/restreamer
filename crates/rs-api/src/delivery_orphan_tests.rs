//! #244 regression: an orphaned Hetzner VPS must be deleted, not just
//! relabelled in the DB.
//!
//! Two paths used to leak a running, billed VPS:
//!   1. `poll_and_init` failure (delivery_handlers.rs) marked the row "failed"
//!      and dropped the handle without ever calling `delete_server`.
//!   2. `start_delivery`'s stale-row cleanup (#165) relabelled a non-active
//!      leftover row straight to "deleted" — again without `delete_server` —
//!      so the old VPS vanished from the dashboard (deleted rows are filtered)
//!      while still running and billing.
//!
//! Both now funnel through `DeliveryOrchestrator::cleanup_orphan_delivery_vps`,
//! which deletes the VPS, marks the row "deleted", and audits a `vps_deleted`
//! row. These tests drive the stale-row path (the compounding site) through the
//! public `start_delivery` API against a mock Hetzner endpoint and assert the
//! delete actually fires.

use std::time::Duration;

use rs_core::config::Config;
use rs_core::db;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::delivery::DeliveryOrchestrator;

/// Seed a stale ("failed") delivery row for `event_id` backed by `hetzner_id`,
/// then call `start_delivery` and assert the orphaned VPS was DELETEd exactly
/// once. Without the fix, the stale-row cleanup relabels the row "deleted" but
/// never hits Hetzner — the mock's `.expect(1)` then fails on drop (RED).
#[tokio::test]
async fn start_delivery_deletes_orphaned_stale_vps() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/servers/4242"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "orphan-evt").await.unwrap();
    let stale_id = db::create_delivery_instance(
        &pool,
        4242,
        "rs-delivery-evt-old",
        "1.2.3.4",
        "cx23",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();
    // A non-active leftover row (not "creating"/active) hits the stale-row path.
    db::update_delivery_instance_status(&pool, stale_id, "failed")
        .await
        .unwrap();

    // with_base_url points the Hetzner client at the mock; the token is unused
    // by the mock so Config::for_testing()'s default is fine.
    let config = Config::for_testing();
    let orch = DeliveryOrchestrator::with_base_url(pool.clone(), config, &mock.uri());

    // The stale-row cleanup runs FIRST, before start_delivery reaches the
    // (unreachable in tests) S3 wipe / binary-lockstep abort. We only assert the
    // orphaned VPS was deleted; the later spawn is expected to error out. Bound
    // the call so an unmocked downstream can never stall the suite.
    let _ = tokio::time::timeout(Duration::from_secs(10), orch.start_delivery(event_id)).await;

    // The stale row is now marked deleted (both old and new code did this — the
    // distinguishing assertion is the mock's `.expect(1)` DELETE, verified on
    // MockServer drop).
    let row = db::get_delivery_instance(&pool, stale_id)
        .await
        .unwrap()
        .expect("stale row still present");
    assert_eq!(row.status, "deleted", "stale row must end up deleted");
}
