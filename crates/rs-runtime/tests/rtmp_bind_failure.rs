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

    // The recorded bind error must be SURFACED on /api/v1/status (the dashboard
    // banner's source), when the API is wired with the SAME InpointState. The
    // successful 200 with the field present is the real assertion — the ingest
    // loop and the API run as independent tasks, so the API serving the error
    // is what proves the "dashboard stays informative during a bind failure"
    // requirement, not merely that a separate server answers.
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let api_ws_tx = ws_tx.clone();
    let state = AppState::new_for_tests(pool, Config::for_testing(), api_ws_tx)
        .with_inpoint_state(inpoint_state.clone());

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (actual_addr, _handle) = rs_api::serve(state, addr).await.unwrap();

    let resp = reqwest::get(format!("http://{actual_addr}/api/v1/status"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "/status must answer 200");
    let body: serde_json::Value = resp.json().await.unwrap();

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

/// #106 (review finding 1): the post-probe TOCTOU case. A probe succeeds while
/// the port is free, then a process grabs it. The NEXT probe (the inpoint loop
/// runs one per iteration) must detect + record the conflict — proving the
/// failure is re-surfaced rather than silently lost. This is the probe-level
/// counterpart to `rtmp_server::run` now propagating xiu's bind error so the
/// loop actually reaches that next iteration.
#[tokio::test]
async fn post_probe_conflict_is_surfaced_on_the_next_probe() {
    // Find a free port (bind :0, read it, release). Accepted risk: a concurrent
    // test could grab this ephemeral port in the microsecond gap before the
    // probe below; if it ever flakes here, that is the cause, not a real bug.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let inpoint_state = InpointState::new();
    let (ws_tx, _rx) = broadcast::channel::<WsEvent>(16);

    // Iteration 1: port free -> probe succeeds, no error recorded.
    assert!(probe_and_record_bind(
        "127.0.0.1",
        port,
        &inpoint_state,
        &ws_tx
    ));
    assert!(inpoint_state.bind_error().is_none());

    // A process now grabs the port (the TOCTOU race / a later conflict).
    let hog = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    // Iteration 2: the next probe must surface the conflict.
    assert!(!probe_and_record_bind(
        "127.0.0.1",
        port,
        &inpoint_state,
        &ws_tx
    ));
    assert!(
        inpoint_state
            .bind_error()
            .as_deref()
            .is_some_and(|m| m.contains(&port.to_string())),
        "a conflict appearing AFTER a successful probe must be re-surfaced"
    );
    drop(hog);
}
