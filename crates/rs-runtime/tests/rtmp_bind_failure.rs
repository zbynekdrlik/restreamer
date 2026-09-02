//! Integration test for #106: an RTMP port-bind failure must NOT break the
//! dashboard — it must be recorded on the shared `InpointState`, surfaced on
//! `/api/v1/status`, and broadcast as a `WsEvent::RtmpBindFailed`, while the
//! HTTP API stays fully reachable.
//!
//! The historical bug swallowed the bind error into `Ok(())` and silently
//! exited the inpoint loop, leaving the dashboard showing "everything fine".

use std::net::{SocketAddr, TcpListener};

use rs_api::state::AppState;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::{InpointState, WsEvent};
use rs_runtime::rtmp_bind::probe_and_record_bind;
use tokio::sync::broadcast;

/// Occupy a real port, probe it, and assert the failure is recorded + surfaced
/// on the API while the API stays reachable.
#[tokio::test]
async fn port_conflict_is_recorded_and_surfaced_on_status() {
    // Occupy a port the way the legacy Python restreamer's inpoint_service did.
    let hog = TcpListener::bind("127.0.0.1:0").expect("bind port hog");
    let port = hog.local_addr().unwrap().port();

    let inpoint_state = InpointState::new();
    let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);

    // The probe must detect the conflict, return false, and record it.
    let bindable = probe_and_record_bind("127.0.0.1", port, &inpoint_state, &ws_tx);
    assert!(
        !bindable,
        "probe must report the port as NOT bindable while the hog holds it"
    );

    let recorded = inpoint_state.bind_error();
    assert!(
        recorded
            .as_deref()
            .is_some_and(|m| m.contains(&port.to_string())),
        "bind_error must be recorded and name the port {port}, got: {recorded:?}"
    );

    // A RtmpBindFailed event must have been broadcast for live dashboards.
    match ws_rx.try_recv() {
        Ok(WsEvent::RtmpBindFailed { port: p, error }) => {
            assert_eq!(p, port);
            assert!(error.contains(&port.to_string()));
        }
        other => panic!("expected WsEvent::RtmpBindFailed, got: {other:?}"),
    }

    // The API — wired with the SAME InpointState — must stay reachable AND
    // report the bind error under inpoint.details.
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let api_ws_tx = ws_tx.clone();
    let state = AppState::new_for_tests(pool, Config::for_testing(), api_ws_tx)
        .with_inpoint_state(inpoint_state.clone());

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (actual_addr, _handle) = rs_api::serve(state, addr).await.unwrap();

    let body: serde_json::Value = reqwest::get(format!("http://{actual_addr}/api/v1/status"))
        .await
        .expect("API must stay reachable during a bind failure")
        .json()
        .await
        .unwrap();

    let surfaced = body["inpoint"]["details"]["rtmp_bind_error"]
        .as_str()
        .unwrap_or("");
    assert!(
        surfaced.contains(&port.to_string()),
        "/status inpoint.details.rtmp_bind_error must name port {port}, got: {}",
        body["inpoint"]["details"]["rtmp_bind_error"]
    );

    // Freeing the port must let a re-probe succeed and clear the banner.
    drop(hog);
    let bindable_again = probe_and_record_bind("127.0.0.1", port, &inpoint_state, &ws_tx);
    assert!(
        bindable_again,
        "probe must succeed once the port is free again"
    );
    assert!(
        inpoint_state.bind_error().is_none(),
        "bind_error must clear once the port is free (banner auto-clears)"
    );
}
