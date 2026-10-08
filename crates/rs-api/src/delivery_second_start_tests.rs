//! #370: a second POST /delivery/start while the delivery is LIVE must be a
//! no-op. `start_delivery` hands back the existing instance, and the handler
//! used to spawn ANOTHER `poll_and_init` + health monitor for it (replacing
//! the live monitor's handle). When that second init failed, its cleanup
//! (`cleanup_orphan_delivery_vps`, "start_failed") deleted the LIVE VPS: a
//! double click on Start mid-event could take every endpoint off air.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;
use tower::ServiceExt;
use wiremock::MockServer;

use crate::delivery::DeliveryOrchestrator;
use crate::router::build_router;
use crate::state::AppState;

fn start_req(event_id: i64) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/delivery/start")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "event_id": event_id }).to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn a_second_start_on_a_live_delivery_spawns_no_second_init() {
    // Any Hetzner call (a second init polling the server, or deleting it)
    // would reach this mock; none is mounted, so the test needs no network.
    let hetzner = MockServer::start().await;

    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "live-evt").await.unwrap();
    let instance_id = db::create_delivery_instance(
        &pool,
        4242,
        "rs-delivery-evt-live",
        "1.2.3.4",
        "cx23",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();
    db::update_delivery_instance_status(&pool, instance_id, "delivering")
        .await
        .unwrap();

    let config = Config::for_testing();
    let orch = Arc::new(DeliveryOrchestrator::with_base_url(
        pool.clone(),
        config.clone(),
        &hetzner.uri(),
    ));
    // The live delivery's own monitor task, registered as the first start did.
    let live_monitor = tokio::spawn(std::future::pending::<()>());
    let live_monitor_id = live_monitor.id();
    orch.poll_handles()
        .lock()
        .await
        .insert(instance_id, live_monitor);

    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let mut state = AppState::new_for_tests(pool.clone(), config, ws_tx);
    state
        .rtmp_stable_since
        .set(Some(Instant::now() - Duration::from_secs(120)));
    state.delivery_orchestrator = Some(Arc::clone(&orch));
    let app = build_router(state);

    let resp = app.oneshot(start_req(event_id)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a second start answers with the live instance"
    );

    let handles = orch.poll_handles();
    let mut handles = handles.lock().await;
    let current = handles
        .remove(&instance_id)
        .expect("the live delivery's monitor handle must still be registered");
    let replaced = current.id() != live_monitor_id;
    current.abort();
    for (_, h) in handles.drain() {
        h.abort();
    }
    assert!(
        !replaced,
        "a second start spawned a second poll_and_init for the LIVE instance; \
         its failure path deletes the live VPS (#370)"
    );
    let row = db::get_delivery_instance(&pool, instance_id)
        .await
        .unwrap()
        .expect("instance row");
    assert_eq!(
        row.status, "delivering",
        "the live instance must stay delivering"
    );
}

/// The dashboard path: POST /events/{id}/start-stream on an event whose
/// delivery is already live must change nothing. It used to clear the event's
/// chunk rows (stale-chunk cleanup) and spawn a second poll_and_init whose
/// failure marked the LIVE instance "failed", so the next start deleted it
/// as a stale row.
#[tokio::test]
async fn a_second_start_stream_on_a_live_event_changes_nothing() {
    let hetzner = MockServer::start().await;
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "live-evt").await.unwrap();
    db::update_streaming_event_flags(&pool, event_id, true, true)
        .await
        .unwrap();
    db::insert_chunk(&pool, event_id, "/tmp/live-1.bin", 1024, "cafebabe", 1000)
        .await
        .unwrap();
    let instance_id = db::create_delivery_instance(
        &pool,
        4242,
        "rs-delivery-evt-live",
        "1.2.3.4",
        "cx23",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();
    db::update_delivery_instance_status(&pool, instance_id, "delivering")
        .await
        .unwrap();

    let config = Config::for_testing();
    let orch = Arc::new(DeliveryOrchestrator::with_base_url(
        pool.clone(),
        config.clone(),
        &hetzner.uri(),
    ));
    let live_monitor = tokio::spawn(std::future::pending::<()>());
    let live_monitor_id = live_monitor.id();
    orch.poll_handles()
        .lock()
        .await
        .insert(instance_id, live_monitor);

    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let mut state = AppState::new_for_tests(pool.clone(), config, ws_tx);
    state.delivery_orchestrator = Some(Arc::clone(&orch));
    let app = build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/events/{event_id}/start-stream"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let handles = orch.poll_handles();
    let mut handles = handles.lock().await;
    let current = handles
        .remove(&instance_id)
        .expect("the live delivery's monitor handle must still be registered");
    let replaced = current.id() != live_monitor_id;
    current.abort();
    for (_, h) in handles.drain() {
        h.abort();
    }
    let chunks = db::get_chunks_for_event(&pool, event_id).await.unwrap();
    assert!(
        !replaced,
        "a second start-stream spawned a second poll_and_init for the LIVE instance (#370)"
    );
    assert_eq!(
        chunks.len(),
        1,
        "a second start-stream must not clear the live event's chunks"
    );
}
