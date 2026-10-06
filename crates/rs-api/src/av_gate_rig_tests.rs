//! The production A/V-gate rig (#357) against the real DB, the real stream
//! handlers and a wiremock Hetzner.

use super::*;
use rs_core::config::Config;
use rs_core::db::av_gate::{self as store, AvGateSessionRow};
use rs_core::models::WsEvent;
use serde_json::json;
use tokio::sync::broadcast;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::delivery::DeliveryOrchestrator;

async fn state_with(config: Config) -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(64);
    AppState::new_for_tests(pool, config, ws_tx)
}

async fn state() -> AppState {
    state_with(Config::for_testing()).await
}

fn with_hetzner(mut state: AppState, uri: &str) -> AppState {
    state.delivery_orchestrator = Some(Arc::new(DeliveryOrchestrator::with_base_url(
        state.pool.clone(),
        (*state.config).clone(),
        uri,
    )));
    state
}

async fn event(state: &AppState, name: &str, delay: Option<i64>, active: bool) -> i64 {
    let id = db::create_streaming_event(&state.pool, name).await.unwrap();
    db::update_streaming_event(&state.pool, id, name, delay, None)
        .await
        .unwrap();
    if active {
        db::update_streaming_event_flags(&state.pool, id, true, true)
            .await
            .unwrap();
    }
    id
}

#[test]
fn delivery_phase_maps_every_instance_status() {
    for s in ["delivering", "running"] {
        assert_eq!(delivery_phase(Some(s)), RigDelivery::Delivering, "{s}");
    }
    for s in ["creating", "booting", "initializing"] {
        assert_eq!(delivery_phase(Some(s)), RigDelivery::Booting, "{s}");
    }
    for s in ["failed", "stopping", "stopped", "deleted", ""] {
        assert_eq!(delivery_phase(Some(s)), RigDelivery::NotRunning, "{s}");
    }
    assert_eq!(delivery_phase(None), RigDelivery::NotRunning);
}

#[test]
fn the_server_selector_is_scoped_to_this_box_and_event() {
    assert_eq!(
        server_selector("uuid-1", 9278),
        "app=restreamer,client_uuid=uuid-1,event_id=9278"
    );
}

#[tokio::test]
async fn resolve_event_takes_the_drain_from_the_event_or_the_default() {
    let s = state().await;
    let id = event(&s, "E2E-Test", Some(30), false).await;
    event(&s, "Sunday", None, false).await;
    let rig = AppRig::new(s.clone());
    assert_eq!(
        rig.resolve_event("E2E-Test").await,
        Ok(RigEvent {
            id,
            drain: Duration::from_secs(30)
        })
    );
    let other = event(&s, "Other", None, false).await;
    assert_eq!(
        rig.resolve_event("Other").await,
        Ok(RigEvent {
            id: other,
            drain: Duration::from_secs(s.config.delivery.delivery_delay_secs)
        })
    );
    assert_eq!(
        rig.resolve_event("Nope").await,
        Err("no event named \"Nope\"".to_string())
    );
}

#[tokio::test]
async fn resolve_event_refuses_while_another_event_is_live() {
    let s = state().await;
    event(&s, "E2E-Test", None, false).await;
    event(&s, "Sunday", None, true).await;
    let err = AppRig::new(s).resolve_event("E2E-Test").await.unwrap_err();
    assert!(
        err.contains("another event is active (\"Sunday\")"),
        "{err}"
    );
}

#[tokio::test]
async fn resolve_event_accepts_its_own_event_already_active() {
    let s = state().await;
    let id = event(&s, "E2E-Test", Some(5), true).await;
    assert_eq!(
        AppRig::new(s).resolve_event("E2E-Test").await.unwrap().id,
        id
    );
}

#[tokio::test]
async fn start_event_is_refused_without_a_hetzner_token() {
    let s = state().await;
    let id = event(&s, "E2E-Test", None, false).await;
    let err = AppRig::new(s.clone()).start_event(id).await.unwrap_err();
    assert!(
        matches!(&err, StartEventError::Refused(r) if r.contains("not configured")),
        "{err:?}"
    );
    let ev = db::get_streaming_event_by_id(&s.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(!ev.receiving_activated, "a refused start touches nothing");
}

#[tokio::test]
async fn start_event_is_refused_while_another_event_is_active() {
    let hetzner = MockServer::start().await;
    let s = with_hetzner(state().await, &hetzner.uri());
    let id = event(&s, "E2E-Test", None, false).await;
    event(&s, "Sunday", None, true).await;
    let err = AppRig::new(s).start_event(id).await.unwrap_err();
    assert_eq!(
        err,
        StartEventError::Refused("another event is active".to_string())
    );
}

#[tokio::test]
async fn start_event_of_a_missing_event_is_refused() {
    let hetzner = MockServer::start().await;
    let s = with_hetzner(state().await, &hetzner.uri());
    let err = AppRig::new(s).start_event(4242).await.unwrap_err();
    assert_eq!(
        err,
        StartEventError::Refused("event 4242 does not exist".to_string())
    );
}

#[tokio::test]
async fn start_event_reports_a_delivery_that_did_not_start() {
    // The S3 wipe that precedes every VPS start cannot reach this endpoint,
    // so `start_stream` activates the event, logs the VPS failure and
    // returns 200 with no delivery row.
    let mut config = Config::for_testing();
    config.s3.endpoint = "http://127.0.0.1:9".to_string();
    let hetzner = MockServer::start().await;
    let s = with_hetzner(state_with(config).await, &hetzner.uri());
    let id = event(&s, "E2E-Test", None, false).await;
    let err = AppRig::new(s.clone()).start_event(id).await.unwrap_err();
    assert!(
        matches!(&err, StartEventError::Failed(r) if r.contains("did not start")),
        "{err:?}"
    );
    let ev = db::get_streaming_event_by_id(&s.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(ev.delivering_activated, "Failed = something was started");
}

#[tokio::test]
async fn delivery_reads_the_event_instance_row() {
    let s = state().await;
    let id = event(&s, "E2E-Test", None, true).await;
    let rig = AppRig::new(s.clone());
    assert_eq!(rig.delivery(id).await, Ok(RigDelivery::NotRunning));
    let inst = db::create_delivery_instance(
        &s.pool,
        1,
        "rs-delivery-evt1",
        "1.2.3.4",
        "cpx22",
        Some(id),
        "t",
    )
    .await
    .unwrap();
    db::update_delivery_instance_status(&s.pool, inst, "booting")
        .await
        .unwrap();
    assert_eq!(rig.delivery(id).await, Ok(RigDelivery::Booting));
    db::update_delivery_instance_status(&s.pool, inst, "delivering")
        .await
        .unwrap();
    assert_eq!(rig.delivery(id).await, Ok(RigDelivery::Delivering));
}

#[tokio::test]
async fn stop_event_deactivates_the_event() {
    let s = state().await;
    let id = event(&s, "E2E-Test", None, true).await;
    AppRig::new(s.clone()).stop_event(id).await.unwrap();
    let ev = db::get_streaming_event_by_id(&s.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(!ev.receiving_activated && !ev.delivering_activated);
    assert_eq!(
        AppRig::new(s).stop_event(4242).await,
        Err("stopping event 4242 failed: HTTP 404".to_string())
    );
}

#[tokio::test]
async fn server_count_lists_this_box_and_event_only() {
    let hetzner = MockServer::start().await;
    let selector = server_selector("test-uuid-00000000", 7);
    let server = |id: i64| {
        json!({
            "id": id, "name": format!("rs-delivery-evt{id}"), "status": "running",
            "public_net": {"ipv4": {"ip": "1.2.3.4"}, "ipv6": {"ip": "::1"}},
            "server_type": {"name": "cpx22", "description": "CPX22"},
            "created": "2026-10-06T10:00:00+00:00", "labels": {}
        })
    };
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(query_param("label_selector", selector.as_str()))
        .and(query_param("page", "1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"servers": [server(1), server(2)]})),
        )
        .mount(&hetzner)
        .await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"servers": []})))
        .mount(&hetzner)
        .await;
    let s = with_hetzner(state().await, &hetzner.uri());
    assert_eq!(AppRig::new(s).server_count(7).await, Ok(2));
}

#[tokio::test]
async fn server_count_reports_a_hetzner_error_and_is_zero_without_hetzner() {
    let hetzner = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(500).set_body_string("down"))
        .mount(&hetzner)
        .await;
    let s = with_hetzner(state().await, &hetzner.uri());
    assert!(AppRig::new(s).server_count(7).await.is_err());
    assert_eq!(AppRig::new(state().await).server_count(7).await, Ok(0));
}

#[tokio::test]
async fn manage_client_needs_a_usable_oauth_file_and_device_flow_client() {
    let dir = tempfile::tempdir().unwrap();
    let oauth = dir.path().join("oauth.json");
    let mut config = Config::for_testing();
    config.av_gate.oauth_file = oauth.to_string_lossy().into_owned();
    let err = manage_client(&state_with(config.clone()).await)
        .err()
        .unwrap();
    assert!(err.contains("cannot read"), "{err}");

    std::fs::write(
        &oauth,
        r#"{"refresh_token":"rt","scope":"https://www.googleapis.com/auth/youtube"}"#,
    )
    .unwrap();
    let err = manage_client(&state_with(config.clone()).await)
        .err()
        .unwrap();
    assert!(err.contains("not configured"), "{err}");

    config.youtube.device_flow.client_id = "cid".to_string();
    config.youtube.device_flow.client_secret = "cs".to_string();
    let client = manage_client(&state_with(config).await).unwrap();
    assert_eq!(client.units_used(), 0);
}

#[tokio::test]
async fn session_ctx_uses_the_production_rig_and_the_config() {
    let mut config = Config::for_testing();
    config.av_gate.event_name = "Gate-Event".to_string();
    config.av_gate.stream_title = "gate stream".to_string();
    config.av_gate.daily_quota_budget = 1_234;
    config.av_gate.idle_timeout_secs = 99;
    let s = state_with(config).await;
    let ctx = session_ctx(&s);
    assert_eq!(ctx.event_name, "Gate-Event");
    assert_eq!(ctx.stream_title, "gate stream");
    assert_eq!(ctx.daily_quota_budget, 1_234);
    assert_eq!(ctx.timings, AvGateTimings::from_config(&s.config.av_gate));
    assert!(Arc::ptr_eq(&ctx.registry, &s.av_gate.registry));
    // The production rig answers from this state's DB.
    let id = event(&s, "Gate-Event", Some(3), false).await;
    assert_eq!(ctx.rig.resolve_event("Gate-Event").await.unwrap().id, id);
}

#[tokio::test]
async fn the_boot_reconcile_fails_a_session_left_starting() {
    let s = state().await;
    let row = AvGateSessionRow::new_starting("s1", "r", "t", "2026-10-06T10:00:00.000Z");
    store::save(&s.pool, &row).await.unwrap();
    reconcile_av_gate_on_boot(s.clone()).await;
    let row = store::get(&s.pool, "s1").await.unwrap().unwrap();
    assert_eq!(row.state, "failed");
    assert_eq!(
        row.reason.as_deref(),
        Some("Restreamer restarted during the session")
    );
}
