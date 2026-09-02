//! Ingest A/V-skew gate tests for the REAL production "Start Delivering"
//! path: `POST /events/{id}/start-stream` (#354).
//!
//! `router_ingest_skew_gate_tests.rs` tests the SAME gate logic on
//! `POST /delivery/start` -- but that HTTP handler is unreachable from the
//! current operator dashboard UI (the ControlBar's Start button calls THIS
//! endpoint, via `stream_handlers::start_stream`, which creates the VPS
//! directly through `DeliveryOrchestrator::start_delivery` rather than
//! going through the `/delivery/start` handler). These tests prove the gate
//! actually protects the path an operator's click really takes.

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

async fn state_with_event_and_audit() -> (AppState, i64, mpsc::Receiver<AuditRow>) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditRow>(16);
    let state = AppState::new(pool, config, ws_tx, audit_tx);
    db::create_streaming_event(&state.pool, "skew-gate-test")
        .await
        .unwrap();
    let events = db::list_streaming_events(&state.pool).await.unwrap();
    (state, events[0].id, audit_rx)
}

fn start_stream_req(event_id: i64, force: bool) -> Request<Body> {
    let uri = if force {
        format!("/api/v1/events/{event_id}/start-stream?force=true")
    } else {
        format!("/api/v1/events/{event_id}/start-stream")
    };
    Request::builder()
        .method("POST")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn start_stream_rejects_when_ingest_skew_active() {
    let (state, event_id, _audit_rx) = state_with_event_and_audit().await;
    state.inpoint_state.set_ingest_skew_active(true);
    state.inpoint_state.set_ingest_skew_ms(25_470);
    let app = build_router(state.clone());

    let resp = app
        .oneshot(start_stream_req(event_id, false))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "the REAL start-stream endpoint must be blocked by the ingest-skew gate"
    );

    // `start_stream` sets receiving/delivering_activated BEFORE the VPS-gate
    // check runs (matching the pre-existing Hetzner-not-configured
    // fallthrough, which also leaves the event activated with no VPS) --
    // assert that stays true so a future refactor can't silently change it.
    let evt = db::get_streaming_event_by_id(&state.pool, event_id)
        .await
        .unwrap()
        .unwrap();
    assert!(evt.receiving_activated);
}

#[tokio::test]
async fn start_stream_force_bypasses_ingest_skew_gate_and_audits_override() {
    let (state, event_id, mut audit_rx) = state_with_event_and_audit().await;
    state.inpoint_state.set_ingest_skew_active(true);
    state.inpoint_state.set_ingest_skew_ms(25_470);
    let app = build_router(state.clone());

    let resp = app.oneshot(start_stream_req(event_id, true)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "force=true must bypass the ingest-skew gate on the real start-stream path"
    );

    // Drain the EventStarted row (unrelated, fired earlier in the handler),
    // then find the override row.
    let mut found_override = false;
    while let Ok(row) = audit_rx.try_recv() {
        if row.action == rs_core::audit::Action::IngestSkewDetected {
            assert_eq!(row.source, rs_core::audit::Source::Operator);
            assert_eq!(row.detail["state"], "override");
            found_override = true;
        }
    }
    assert!(
        found_override,
        "force=true override must emit an IngestSkewDetected audit row"
    );
}

#[tokio::test]
async fn start_stream_passes_gate_when_ingest_skew_clear() {
    let (state, event_id, _audit_rx) = state_with_event_and_audit().await;
    // Skew latch clear (default) -> the gate must not fire.
    let app = build_router(state);

    let resp = app
        .oneshot(start_stream_req(event_id, false))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a clear ingest must not trip the skew gate on the real start-stream path"
    );
}
