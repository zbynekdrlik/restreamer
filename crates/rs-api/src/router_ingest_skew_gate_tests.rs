//! Ingest A/V-skew gate tests for POST /delivery/start (#354).
// #354 — the ingest A/V-skew gate on POST /delivery/start: while the source
// (OBS) is desynced past the operator threshold, refuse to spin up a paid VPS
// (every endpoint would skew-kill in a loop) — UNLESS the operator explicitly
// forces it. The gate sits AFTER the rtmp-stable gate, so these tests make
// the ingest LOOK stable (rtmp_stable_since well past the threshold) and then
// toggle the ingest-skew latch.

use crate::router::build_router;
use crate::state::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::audit::AuditRow;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::{broadcast, mpsc};
use tower::ServiceExt;

async fn stable_state() -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let state = AppState::new_for_tests(pool, config, ws_tx);
    // Ingest has been publishing well past the rtmp-stable threshold, so
    // the rtmp gate passes and only the skew gate is under test.
    *state.rtmp_stable_since.lock().await =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(120));
    state
}

/// Same as `stable_state`, but wired with a live (non-dropped) audit channel
/// so a test can assert on rows the gate emits — `AppState::new_for_tests`
/// discards its receiver immediately, which is fine for the other tests here
/// (they only assert on the HTTP response) but not for the `force:true`
/// override, whose whole point is the audit trail it leaves (#354).
async fn stable_state_with_audit() -> (AppState, mpsc::Receiver<AuditRow>) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditRow>(8);
    let state = AppState::new(pool, config, ws_tx, audit_tx);
    *state.rtmp_stable_since.lock().await =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(120));
    (state, audit_rx)
}

async fn body_to_json(body: Body) -> serde_json::Value {
    let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn start_req(body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/delivery/start")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn start_delivery_rejects_when_ingest_skew_active() {
    let state = stable_state().await;
    state.inpoint_state.set_ingest_skew_active(true);
    state.inpoint_state.set_ingest_skew_ms(25_470);
    let app = build_router(state);

    let resp = app
        .oneshot(start_req(serde_json::json!({"event_id": 1})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json = body_to_json(resp.into_body()).await;
    assert_eq!(json["error"], "ingest_skew_too_high");
    assert_eq!(json["skew_ms"], 25_470);
    // The plain-language reason names OBS + the remedy.
    let reason = json["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("OBS"),
        "reason must name the source (OBS): {reason}"
    );
}

#[tokio::test]
async fn start_delivery_force_bypasses_ingest_skew_gate() {
    let (state, mut audit_rx) = stable_state_with_audit().await;
    state.inpoint_state.set_ingest_skew_active(true);
    state.inpoint_state.set_ingest_skew_ms(25_470);
    let app = build_router(state);

    // force:true must get PAST the skew gate. With no Hetzner token wired
    // in tests the next stop is the orchestrator's 503 — proving the gate
    // was bypassed (a non-BAD_REQUEST, non-skew response).
    let resp = app
        .oneshot(start_req(serde_json::json!({"event_id": 1, "force": true})))
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "force:true must bypass the ingest-skew gate"
    );
    let json = body_to_json(resp.into_body()).await;
    assert_ne!(
        json["error"], "ingest_skew_too_high",
        "force:true must not be blocked by the skew gate"
    );

    // The deliberate bypass must itself be audited (#354) -- an operator
    // overriding a "your source is desynced" gate is exactly the kind of
    // action that must leave a trail.
    let row = audit_rx
        .try_recv()
        .expect("force:true override must emit an audit row");
    assert_eq!(row.action, rs_core::audit::Action::IngestSkewDetected);
    assert_eq!(row.source, rs_core::audit::Source::Operator);
    assert_eq!(row.event_id, Some(1));
    assert_eq!(row.detail["skew_ms"], 25_470);
    assert_eq!(row.detail["state"], "override");
    assert!(
        row.detail.get("threshold_ms").is_some(),
        "override row must carry the threshold it bypassed"
    );
}

#[tokio::test]
async fn start_delivery_passes_gate_when_ingest_skew_clear() {
    let state = stable_state().await;
    // Skew latch clear (default) → the skew gate must not fire.
    let app = build_router(state);

    let resp = app
        .oneshot(start_req(serde_json::json!({"event_id": 1})))
        .await
        .unwrap();
    // Not blocked by either gate; falls through to the orchestrator
    // (503 hetzner_not_configured in tests).
    let status = resp.status();
    let json = body_to_json(resp.into_body()).await;
    assert_ne!(
        json["error"], "ingest_skew_too_high",
        "a clear ingest must not trip the skew gate (status was {status})"
    );
    assert_ne!(json["error"], "rtmp_not_stable");
}
