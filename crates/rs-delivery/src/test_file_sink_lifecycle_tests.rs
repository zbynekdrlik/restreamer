//! #192 — TEST_FILE loopback sink, driven through the REAL delivery handlers
//! and the REAL rescue loop (bin target).
//!
//! These tests run in the rs-delivery BIN test process, where other unit tests
//! assume `127.0.0.1:1935` is REFUSED (e.g. `rescue_endpoint_loop_tests`). So
//! every sink here binds an EPHEMERAL loopback port: `AppState::new_for_test()`
//! uses `127.0.0.1:0`. The production-port tests live in
//! `tests/test_file_sink_e2e.rs`, which runs as its own process.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use tokio::sync::{Mutex, RwLock, watch};
use tower::util::ServiceExt;

use crate::api::{
    EndpointConfig, UpdateStartRequest, reconcile_test_file_sink, router, update_start_handler,
};
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::rescue_segments::RescueClipSource;
use crate::rust_rescue_push::{RescuePushMode, rust_rescue_push_with_pusher};
use crate::test_file_sink::TestFileSinkSlot;
use crate::{AppState, EndpointHandle};

const TOKEN: &str = "test-file-sink-lifecycle-token";
const WAIT: Duration = Duration::from_secs(30);

fn endpoint_cfg(alias: &str, service_type: &str) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: service_type.to_string(),
        stream_key: format!("{alias}-key"),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: Default::default(),
    }
}

/// An authenticated test `AppState` (ephemeral-port sink slot) whose endpoint
/// map holds one no-op stub per `(alias, service_type)`. The sink is NOT
/// reconciled here; each test decides when.
async fn state_with(endpoints: &[(&str, &str)]) -> Arc<AppState> {
    let mut state = AppState::new_for_test();
    state.auth_token = RwLock::new(Some(TOKEN.to_string()));
    let state = Arc::new(state);
    {
        let mut map = state.endpoints.write().await;
        for (alias, service_type) in endpoints {
            map.insert(
                alias.to_string(),
                EndpointHandle::stub_with_config_for_test(endpoint_cfg(alias, service_type), 1),
            );
        }
    }
    state
}

async fn sink_addr(state: &AppState) -> Option<SocketAddr> {
    state.test_file_sink.status().await.map(|s| s.local_addr)
}

/// POST through the real router (auth middleware included). Returns the
/// status and the parsed JSON body.
async fn post(
    state: &Arc<AppState>,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router(Arc::clone(state)).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn sink_follows_the_test_file_endpoint_through_remove() {
    let state = state_with(&[("e2e rtmp", "YT_RTMP")]).await;
    reconcile_test_file_sink(&state, &[]).await;
    assert_eq!(
        sink_addr(&state).await,
        None,
        "a set with no TEST_FILE endpoint must not run the sink"
    );

    state.endpoints.write().await.insert(
        "e2e fast".to_string(),
        EndpointHandle::stub_with_config_for_test(endpoint_cfg("e2e fast", "TEST_FILE"), 1),
    );
    reconcile_test_file_sink(&state, &[]).await;
    let addr = sink_addr(&state)
        .await
        .expect("adding a TEST_FILE endpoint must start the sink");
    assert!(
        addr.ip().is_loopback(),
        "sink bound to loopback, got {addr}"
    );

    let (status, json) = post(
        &state,
        "/api/endpoints/remove",
        serde_json::json!({"alias": "e2e fast"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["removed"], true,
        "the TEST_FILE endpoint was removed: {json}"
    );
    assert_eq!(
        sink_addr(&state).await,
        None,
        "removing the last TEST_FILE endpoint must stop the sink"
    );
    assert!(
        state.endpoints.read().await.contains_key("e2e rtmp"),
        "the other endpoint is untouched"
    );
}

#[tokio::test]
async fn stopping_another_endpoint_keeps_the_same_sink_running() {
    let state = state_with(&[("e2e fast", "TEST_FILE"), ("e2e rtmp", "YT_RTMP")]).await;
    reconcile_test_file_sink(&state, &[]).await;
    let before = sink_addr(&state)
        .await
        .expect("TEST_FILE endpoint present -> sink running");

    let (status, _) = post(
        &state,
        "/api/stop",
        serde_json::json!({"alias": "e2e rtmp"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sink_addr(&state).await,
        Some(before),
        "the TEST_FILE endpoint is still configured: the SAME sink keeps running (no rebind)"
    );
}

/// init / add reconcile with the INCOMING service types before they spawn,
/// so a TEST_FILE endpoint's first push (its warmup rescue) never races the
/// sink's bind.
#[tokio::test]
async fn incoming_test_file_endpoint_starts_the_sink_before_it_spawns() {
    let state = state_with(&[]).await;
    reconcile_test_file_sink(&state, &["YT_RTMP"]).await;
    assert_eq!(
        sink_addr(&state).await,
        None,
        "an incoming non-TEST_FILE endpoint needs no sink"
    );
    reconcile_test_file_sink(&state, &["YT_RTMP", "TEST_FILE"]).await;
    assert!(
        sink_addr(&state).await.is_some(),
        "an incoming TEST_FILE endpoint must have its sink before it is even in the map"
    );
}

/// The real /api/endpoints/add handler refuses (409) before /api/init, and a
/// refused add must not start the sink: its pre-spawn reconcile runs only
/// after the DiskCache check passes.
#[tokio::test]
async fn add_before_init_is_refused_and_starts_no_sink() {
    let state = state_with(&[]).await;
    let (status, _) = post(
        &state,
        "/api/endpoints/add",
        serde_json::json!({"endpoint": {"alias": "e2e fast", "service_type": "TEST_FILE", "stream_key": "k"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "no /api/init yet");
    assert_eq!(
        sink_addr(&state).await,
        None,
        "a refused add must not start the sink"
    );
}

#[tokio::test]
async fn stop_all_endpoints_stops_the_sink() {
    let state = state_with(&[("e2e fast", "TEST_FILE"), ("e2e rtmp", "YT_RTMP")]).await;
    reconcile_test_file_sink(&state, &[]).await;
    assert!(
        sink_addr(&state).await.is_some(),
        "sink running before /api/stop"
    );

    let (status, json) = post(&state, "/api/stop", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        sink_addr(&state).await,
        None,
        "/api/stop (all) leaves no TEST_FILE endpoint, so the sink must stop"
    );
}

#[tokio::test]
async fn update_start_reconverges_the_sink() {
    // The TEST_FILE endpoint is configured but its sink is NOT running -- the
    // state a concurrent reconcile can leave while update_start briefly takes
    // the alias out of the map. update_start must re-converge.
    let state = state_with(&[("e2e fast", "TEST_FILE")]).await;
    assert_eq!(sink_addr(&state).await, None);

    let req = UpdateStartRequest {
        alias: "e2e fast".to_string(),
        new_start_chunk_id: 42,
    };
    let result = update_start_handler(axum::extract::State(state.clone()), axum::Json(req)).await;
    assert_eq!(result, Ok(StatusCode::OK));
    assert!(
        sink_addr(&state).await.is_some(),
        "after update_start the TEST_FILE endpoint must have a running sink"
    );
    let endpoints = state.endpoints.read().await;
    let handle = endpoints.get("e2e fast").expect("endpoint respawned");
    assert_eq!(handle.start_chunk_id(), 42);
    assert_eq!(
        handle.config().service_type,
        "TEST_FILE",
        "the respawn keeps the endpoint's real config"
    );
}

#[tokio::test]
async fn status_reports_the_running_sink() {
    let state = state_with(&[("e2e fast", "TEST_FILE")]).await;
    reconcile_test_file_sink(&state, &[]).await;
    let addr = sink_addr(&state).await.expect("sink running");

    let req = Request::builder()
        .uri("/api/status")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = router(Arc::clone(&state)).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        json["test_file_sink"]["local_addr"],
        addr.to_string(),
        "/api/status exposes the sink address: {json}"
    );
    assert_eq!(json["test_file_sink"]["counters"]["publishes"], 0);
}

/// (iv) The REAL outage-rescue loop pushes the production countdown clip
/// through a REAL `RtmpPusher` into the sink, and the push counts as a
/// successful push (`last_push_ok_unix_ms`) -- the exact signal the CI
/// crash-exhaustion gate reads for "rescue is live on EVERY endpoint".
#[tokio::test]
async fn rescue_loop_pushes_the_clip_into_the_sink() {
    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    slot.reconcile(async { true }).await;
    let addr = slot
        .status()
        .await
        .expect("the sink must be running")
        .local_addr;

    let pusher = RtmpPusher::new(
        format!("rtmp://{addr}/live/e2e-fast-key"),
        PusherConfig::default(),
    );
    let buffer_state = Arc::new(BufferState::default());
    // Producer stalled: rescue stays active (it never sees a refill).
    buffer_state.producer_active.store(false, Ordering::Relaxed);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let loop_stats = stats.clone();
    let rescue = tokio::spawn(async move {
        rust_rescue_push_with_pusher(
            pusher,
            "e2e fast",
            RescueClipSource::Countdown,
            buffer_state,
            loop_stats,
            &mut stop_rx,
            RescuePushMode::Outage,
        )
        .await
    });

    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let s = stats.lock().await.clone();
        if s.last_push_ok_unix_ms.is_some() {
            assert_eq!(s.delivery_mode, "rescue");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "rescue loop never completed a successful clip push into the sink within {WAIT:?} (last_error={:?})",
            s.last_error
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let counters = slot.status().await.expect("still running").counters;
    assert_eq!(
        counters.publishes, 1,
        "one rescue publish session: {counters:?}"
    );
    assert!(
        counters.tags_received > 0,
        "rescue clip tags reached the sink: {counters:?}"
    );
    assert!(
        counters.bytes_received > 0,
        "rescue clip bytes reached the sink: {counters:?}"
    );

    stop_tx.send(true).expect("stop the rescue loop");
    let stopped = tokio::time::timeout(Duration::from_secs(15), rescue)
        .await
        .expect("rescue loop must exit on stop")
        .expect("rescue task must not panic");
    assert!(stopped, "a stop signal returns true");
    slot.reconcile(async { false }).await;
}
