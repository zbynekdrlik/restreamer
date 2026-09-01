//! #352: integration tests for the async orphan-reaper sweep
//! (`DeliveryOrchestrator::reconcile_orphan_vps`) against a mock Hetzner
//! endpoint (same wiremock pattern as `delivery_orphan_tests.rs`).
//!
//! These prove the end-to-end behaviour the acceptance calls for: an orphaned
//! VPS (no DB row) past the delete grace is DELETE'd exactly once, a server
//! carrying a DIFFERENT client_uuid is NEVER deleted even when Hetzner returns
//! it (#137 defense in depth), and the orphan-count signal reflects money still
//! billing.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use rs_core::config::Config;
use rs_core::db;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::delivery::DeliveryOrchestrator;

const THIS_UUID: &str = "test-uuid-00000000";

/// A Hetzner `/servers` server object as the API returns it.
fn server_json(id: i64, created: &str, client_uuid: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": format!("rs-delivery-evt{id}"),
        "status": "running",
        "public_net": {"ipv4": {"ip": format!("1.2.3.{}", id % 250)}, "ipv6": {"ip": "::1"}},
        "server_type": {"name": "cpx32", "description": "CPX32"},
        "created": created,
        "labels": {"app": "restreamer", "client_uuid": client_uuid}
    })
}

/// Mount the paginated `GET /servers` list (page 1 → `servers`, page 2 → empty)
/// so `HetznerClient::list_servers` terminates.
async fn mount_list(mock: &MockServer, servers: Vec<serde_json::Value>) {
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "servers": servers })))
        .mount(mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "servers": [] })))
        .mount(mock)
        .await;
}

fn orch(pool: sqlx::SqlitePool, uri: &str) -> DeliveryOrchestrator {
    DeliveryOrchestrator::with_base_url(pool, Config::for_testing(), uri)
}

/// The evidenced money-leak: a labelled, rowless, old server is DELETE'd exactly
/// once; the orphan count ends at 0 (nothing still billing after deletion).
#[tokio::test]
async fn old_rowless_orphan_is_deleted_once() {
    let mock = MockServer::start().await;
    // Very old → well past the 3h delete grace.
    mount_list(
        &mock,
        vec![server_json(
            160756914,
            "2020-01-01T00:00:00+00:00",
            THIS_UUID,
        )],
    )
    .await;
    Mock::given(method("DELETE"))
        .and(path("/servers/160756914"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    // No delivery_instances row exists for hetzner_id 160756914 → orphan.

    let orch = orch(pool, &mock.uri());
    let count = AtomicU8::new(9);
    tokio::time::timeout(Duration::from_secs(10), orch.reconcile_orphan_vps(&count))
        .await
        .expect("sweep must not hang");

    assert_eq!(
        count.load(Ordering::Relaxed),
        0,
        "the sole orphan was deleted, so nothing is still billing"
    );
    // The DELETE .expect(1) is verified on MockServer drop.
}

/// #137 CRITICAL: even when Hetzner returns a server carrying a DIFFERENT
/// client_uuid, the reaper NEVER deletes it — the local label re-check in
/// `classify_orphan_vps` filters it out before any delete.
#[tokio::test]
async fn foreign_client_uuid_is_never_deleted() {
    let mock = MockServer::start().await;
    mount_list(
        &mock,
        vec![server_json(
            160624800,
            "2020-01-01T00:00:00+00:00",
            "f59e3ecd-other-install",
        )],
    )
    .await;
    // If the reaper ever tried to delete the foreign server, this expect(0)
    // fails on drop.
    Mock::given(method("DELETE"))
        .and(path("/servers/160624800"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let orch = orch(pool, &mock.uri());
    let count = AtomicU8::new(9);
    tokio::time::timeout(Duration::from_secs(10), orch.reconcile_orphan_vps(&count))
        .await
        .expect("sweep must not hang");

    assert_eq!(
        count.load(Ordering::Relaxed),
        0,
        "a foreign-uuid server is not our orphan — count must be 0"
    );
}

/// A server WITH a live DB row is never deleted (it is genuinely delivering),
/// even sitting next to a real orphan — only the rowless one is reaped.
#[tokio::test]
async fn tracked_server_is_never_deleted() {
    let mock = MockServer::start().await;
    mount_list(
        &mock,
        vec![
            server_json(777, "2020-01-01T00:00:00+00:00", THIS_UUID), // tracked
            server_json(888, "2020-01-01T00:00:00+00:00", THIS_UUID), // orphan
        ],
    )
    .await;
    Mock::given(method("DELETE"))
        .and(path("/servers/777"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/servers/888"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "evt").await.unwrap();
    // A live row backs hetzner_id 777 → tracked, must survive.
    db::create_delivery_instance(
        &pool,
        777,
        "rs-delivery-evt777",
        "1.2.3.9",
        "cpx32",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();

    let orch = orch(pool, &mock.uri());
    let count = AtomicU8::new(0);
    tokio::time::timeout(Duration::from_secs(10), orch.reconcile_orphan_vps(&count))
        .await
        .expect("sweep must not hang");

    assert_eq!(
        count.load(Ordering::Relaxed),
        0,
        "only the rowless VPS was reaped"
    );
}

/// An empty client_uuid fails the sweep closed — no list, no delete (#137).
#[tokio::test]
async fn empty_client_uuid_refuses_to_sweep() {
    let mock = MockServer::start().await;
    // Any GET/DELETE hitting the mock would be a bug — mount nothing and assert
    // the sweep made zero requests by leaving the mock empty (an unmatched
    // request 404s; list_servers would then error, but we must not even list).
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let mut config = Config::for_testing();
    config.client_uuid = String::new();
    let orch = DeliveryOrchestrator::with_base_url(pool, config, &mock.uri());
    let count = AtomicU8::new(5);
    tokio::time::timeout(Duration::from_secs(10), orch.reconcile_orphan_vps(&count))
        .await
        .expect("sweep must not hang");

    // Count is left untouched (still 5) — the sweep bailed before any work.
    assert_eq!(
        count.load(Ordering::Relaxed),
        5,
        "an empty client_uuid must abort before listing or deleting anything"
    );
    assert!(
        mock.received_requests().await.unwrap().is_empty(),
        "no Hetzner request may be made when client_uuid is empty"
    );
}
