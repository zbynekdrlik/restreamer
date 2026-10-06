//! The A/V-gate HTTP surface (#357) through the real router: auth (LAN +
//! token), the 201/409/429/502/503 create outcomes, GET and stop.

use super::*;
use std::collections::VecDeque;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use rs_core::config::Config;
use rs_core::db::av_gate::AvGateSessionRow;
use rs_core::models::WsEvent;
use serde_json::Value;
use tokio::sync::broadcast;
use tower::ServiceExt;

use crate::av_gate::{AvGateTimings, RigDelivery, TestSeam};
use crate::av_gate_driver::tests::{FakeRig, YtState, fake_youtube, oauth_file, timings};
use crate::router::build_router;

const TOKEN: &str = "0123456789abcdef0123456789abcdef-gate";

struct Api {
    state: AppState,
    yt_state: Arc<YtState>,
    _server: wiremock::MockServer,
    _dir: tempfile::TempDir,
}

/// A state with a token file, an oauth file, the device-flow client and the
/// test seam (scripted rig + fake YouTube).
async fn api_with(rig: FakeRig, timings: AvGateTimings, access_mode: &str) -> Api {
    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("api-token");
    std::fs::write(&token_file, format!("\u{FEFF}{TOKEN}\r\n")).unwrap();
    let mut config = Config::for_testing();
    config.av_gate.api_token_file = token_file.to_string_lossy().into_owned();
    config.av_gate.oauth_file = oauth_file(&dir).to_string_lossy().into_owned();
    config.youtube.device_flow.client_id = "cid".to_string();
    config.youtube.device_flow.client_secret = "cs-fake".to_string();
    config.api.access.mode = access_mode.to_string();
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let state = AppState::new_for_tests(pool, config, ws_tx);
    state.av_gate.registry.mark_reconciled();
    let yt_state = Arc::new(YtState::default());
    let server = fake_youtube(Arc::clone(&yt_state)).await;
    *state.av_gate.seam.lock().unwrap() = Some(TestSeam {
        rig: Arc::new(rig),
        api_base: format!("{}/yt", server.uri()),
        token_uri: format!("{}/token", server.uri()),
        timings,
        quota_bucket: None,
    });
    Api {
        state,
        yt_state,
        _server: server,
        _dir: dir,
    }
}

async fn api() -> Api {
    api_with(FakeRig::default(), timings(), "enforce").await
}

fn request(method: &str, uri: &str, auth: Option<&str>, body: &str) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    b.body(Body::from(body.to_string())).unwrap()
}

fn authed(method: &str, uri: &str, body: &str) -> Request<Body> {
    request(method, uri, Some(&format!("Bearer {TOKEN}")), body)
}

async fn send(state: &AppState, req: Request<Body>) -> (StatusCode, Value) {
    let resp = build_router(state.clone()).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn wait_state(state: &AppState, id: &str, wanted: &str) -> Value {
    let wait = async {
        loop {
            let (_, v) = send(
                state,
                authed("GET", &format!("/api/v1/av-gate/session/{id}"), ""),
            )
            .await;
            if v["state"] == wanted {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(8), wait)
        .await
        .unwrap_or_else(|_| panic!("{id} never reached {wanted}"))
}

const CREATE: &str = r#"{"requester":"camera-box","title":"gate"}"#;

// ---- pure auth helpers ---------------------------------------------------------

#[test]
fn tokens_match_only_identical_tokens() {
    assert!(tokens_match(TOKEN, TOKEN));
    assert!(!tokens_match(TOKEN, &TOKEN[..TOKEN.len() - 1]));
    assert!(!tokens_match("", TOKEN));
    assert!(!tokens_match(&TOKEN.replace('0', "1"), TOKEN));
}

#[test]
fn bearer_reads_only_a_bearer_authorization() {
    let mut h = HeaderMap::new();
    assert_eq!(bearer(&h), None);
    h.insert(header::AUTHORIZATION, "Bearer  abc ".parse().unwrap());
    assert_eq!(bearer(&h), Some("abc"));
    h.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
    assert_eq!(bearer(&h), None);
}

#[tokio::test]
async fn read_api_token_trims_and_refuses_short_or_missing_files() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t");
    assert!(read_api_token(&p).await.unwrap_err().contains("unreadable"));
    std::fs::write(&p, format!("\u{FEFF} {TOKEN}\n")).unwrap();
    assert_eq!(read_api_token(&p).await.unwrap(), TOKEN);
    let short = "x".repeat(MIN_TOKEN_CHARS - 1);
    std::fs::write(&p, &short).unwrap();
    let err = read_api_token(&p).await.unwrap_err();
    assert!(err.contains("fewer than 32"), "{err}");
    assert!(!err.contains(&short), "the error must not quote the file");
    std::fs::write(&p, "y".repeat(MIN_TOKEN_CHARS)).unwrap();
    assert!(read_api_token(&p).await.is_ok());
}

// ---- auth through the router ----------------------------------------------------

#[tokio::test]
async fn every_route_rejects_a_request_without_the_token() {
    let a = api().await;
    for (method, uri) in [
        ("POST", "/api/v1/av-gate/session"),
        ("GET", "/api/v1/av-gate/session/x"),
        ("POST", "/api/v1/av-gate/session/x/stop"),
    ] {
        let resp = build_router(a.state.clone())
            .oneshot(request(method, uri, None, CREATE))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{method} {uri}");
        assert_eq!(resp.headers()[header::WWW_AUTHENTICATE], "Bearer");
        let (status, _) = send(
            &a.state,
            request(method, uri, Some("Bearer wrong-token"), CREATE),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {uri} wrong token"
        );
    }
    assert_eq!(
        a.yt_state.inserts.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn the_api_is_off_without_a_provisioned_token() {
    let a = api().await;
    std::fs::remove_file(&a.state.config.av_gate.api_token_file).unwrap();
    let (status, body) = send(&a.state, authed("GET", "/api/v1/av-gate/session/x", "")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "not_provisioned");
}

#[tokio::test]
async fn an_internet_request_is_refused_even_past_the_access_gate() {
    // `log_only` lets the router-wide gate pass everything; the av-gate's own
    // LAN check must still refuse.
    let a = api_with(FakeRig::default(), timings(), "log_only").await;
    let mut tunneled = authed("GET", "/api/v1/av-gate/session/x", "");
    tunneled
        .headers_mut()
        .insert("cf-connecting-ip", "203.0.113.7".parse().unwrap());
    let (status, body) = send(&a.state, tunneled).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], "lan_only");

    let mut public = authed("GET", "/api/v1/av-gate/session/x", "");
    public
        .extensions_mut()
        .insert(ConnectInfo("8.8.8.8:5000".parse::<SocketAddr>().unwrap()));
    let (status, body) = send(&a.state, public).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], "lan_only");

    let mut lan = authed("GET", "/api/v1/av-gate/session/x", "");
    lan.extensions_mut()
        .insert(ConnectInfo("10.77.9.5:5000".parse::<SocketAddr>().unwrap()));
    let (status, _) = send(&a.state, lan).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a LAN peer with the token gets through"
    );
}

// ---- create / get / stop ------------------------------------------------------------

#[tokio::test]
async fn create_get_stop_runs_a_session_to_done() {
    let a = api().await;
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["broadcast_id"], "bc-1");
    let id = body["session_id"].as_str().unwrap().to_string();

    let ready = wait_state(&a.state, &id, "ready").await;
    assert_eq!(ready["requester"], "camera-box");
    assert_eq!(ready["title"], "gate");
    assert!(ready["timestamps"]["ready"].is_string());

    let stop = format!("/api/v1/av-gate/session/{id}/stop");
    let (status, body) = send(&a.state, authed("POST", &stop, "")).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        body,
        serde_json::json!({"session_id": id, "state": "ready"})
    );

    let done = wait_state(&a.state, &id, "done").await;
    assert_eq!(done["vod_id"], "bc-1");
    let (status, body) = send(&a.state, authed("POST", &stop, "")).await;
    assert_eq!(status, StatusCode::OK, "a retried stop is harmless");
    assert_eq!(body["state"], "done");
}

#[tokio::test]
async fn a_second_create_gets_409_with_the_holder() {
    let a = api().await;
    let (status, first) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        &a.state,
        authed(
            "POST",
            "/api/v1/av-gate/session",
            r#"{"requester":"restreamer-ci"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "busy");
    assert_eq!(body["holder"]["session_id"], first["session_id"]);
    assert_eq!(body["holder"]["requester"], "camera-box");
}

#[tokio::test]
async fn a_bad_body_is_400() {
    let a = api().await;
    for body in [
        "not json",
        r#"{"title":"x"}"#,
        r#"{"requester":"  "}"#,
        &format!(r#"{{"requester":"r","title":"{}"}}"#, "t".repeat(101)),
    ] {
        let (status, v) = send(&a.state, authed("POST", "/api/v1/av-gate/session", body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(v["error"], "bad_request");
    }
}

#[tokio::test]
async fn create_without_a_usable_oauth_file_is_503() {
    let a = api().await;
    std::fs::remove_file(&a.state.config.av_gate.oauth_file).unwrap();
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "not_provisioned");
    assert_eq!(a.state.av_gate.registry.holder(), None);
}

#[tokio::test]
async fn a_failed_start_is_502_with_the_reason() {
    let a = api().await;
    a.yt_state
        .reusable
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["state"], "failed");
    assert!(
        body["reason"].as_str().unwrap().contains("not reusable"),
        "{body}"
    );
    let id = body["session_id"].as_str().unwrap();
    let (status, row) = send(
        &a.state,
        authed("GET", &format!("/api/v1/av-gate/session/{id}"), ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["state"], "failed");
}

#[tokio::test]
async fn an_exhausted_quota_is_429() {
    let a = api().await;
    let mut row = AvGateSessionRow::new_starting("old", "r", "t", &crate::av_gate_driver::now_ts());
    row.state = "done".to_string();
    row.quota_units = 3_900;
    store::save(&a.state.pool, &row).await.unwrap();
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        body,
        serde_json::json!({"error": "quota", "spent": 3900, "estimate": 400, "budget": 4000})
    );
}

#[tokio::test]
async fn stop_of_an_unknown_or_orphaned_session() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::Booting)]);
    let a = api_with(rig, timings(), "enforce").await;
    let (status, _) = send(
        &a.state,
        authed("POST", "/api/v1/av-gate/session/nope/stop", ""),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A `ready` row nobody runs (a crash before the boot reconcile ran).
    let mut row = AvGateSessionRow::new_starting("orphan", "r", "t", "2026-10-06T10:00:00.000Z");
    row.state = "ready".to_string();
    store::save(&a.state.pool, &row).await.unwrap();
    let (status, body) = send(
        &a.state,
        authed("POST", "/api/v1/av-gate/session/orphan/stop", ""),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["state"], "ready");
    row.state = "starting".to_string();
    store::save(&a.state.pool, &row).await.unwrap();
    let (status, _) = send(
        &a.state,
        authed("POST", "/api/v1/av-gate/session/orphan/stop", ""),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn get_of_an_unknown_session_is_404() {
    let a = api().await;
    let (status, body) = send(&a.state, authed("GET", "/api/v1/av-gate/session/nope", "")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
}

#[tokio::test]
async fn create_is_503_until_the_boot_reconcile_ran() {
    let a = api().await;
    let mut state = a.state.clone();
    state.av_gate = Arc::new(crate::av_gate::AvGateHub::default());
    *state.av_gate.seam.lock().unwrap() = a.state.av_gate.seam.lock().unwrap().clone();
    let (status, body) = send(&state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "starting_up");
}

#[tokio::test]
async fn create_is_409_while_a_failed_teardown_is_pending() {
    let a = api().await;
    let mut row = AvGateSessionRow::new_starting("stuck", "r", "t", "2026-10-06T10:00:00.000Z");
    row.state = "failed".to_string();
    row.cleanup_pending = true;
    store::save(&a.state.pool, &row).await.unwrap();
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        serde_json::json!({"error": "cleanup_pending", "sessions": ["stuck"]})
    );
    let (_, view) = send(&a.state, authed("GET", "/api/v1/av-gate/session/stuck", "")).await;
    assert_eq!(view["cleanup_pending"], true);
}

#[tokio::test]
async fn status_and_clear_cleanup_through_the_router() {
    let a = api().await;
    let (status, body) = send(&a.state, authed("GET", "/api/v1/av-gate/status", "")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        serde_json::json!({"reconciled": true, "holder": null, "cleanup_pending": []})
    );
    let mut row = AvGateSessionRow::new_starting("stuck", "r", "t", "2026-10-06T10:00:00.000Z");
    row.state = "failed".to_string();
    row.cleanup_pending = true;
    store::save(&a.state.pool, &row).await.unwrap();
    let (_, body) = send(&a.state, authed("GET", "/api/v1/av-gate/status", "")).await;
    assert_eq!(body["cleanup_pending"], serde_json::json!(["stuck"]));

    let clear = "/api/v1/av-gate/session/stuck/clear-cleanup";
    let (status, body) = send(&a.state, authed("POST", clear, "")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        serde_json::json!({"session_id": "stuck", "cleanup_pending": false})
    );
    let (status, body) = send(&a.state, authed("POST", clear, "")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "not_pending");
    let mut row = AvGateSessionRow::new_starting("busy", "r", "t", "2026-10-06T10:00:00.000Z");
    row.state = "failed".to_string();
    row.cleanup_pending = true;
    store::save(&a.state.pool, &row).await.unwrap();
    let round = a.state.av_gate.registry.cleanup_round.lock().await;
    let (status, body) = send(
        &a.state,
        authed("POST", "/api/v1/av-gate/session/busy/clear-cleanup", ""),
    )
    .await;
    drop(round);
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "retry_running");
    let (status, _) = send(
        &a.state,
        authed("POST", "/api/v1/av-gate/session/nope/clear-cleanup", ""),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    for (method, uri) in [("GET", "/api/v1/av-gate/status"), ("POST", clear)] {
        let (status, _) = send(&a.state, request(method, uri, None, "")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
}

#[tokio::test]
async fn a_low_project_bucket_is_429() {
    static LOW: std::sync::OnceLock<rs_youtube::quota::QuotaTracker> = std::sync::OnceLock::new();
    let a = api().await;
    a.state
        .av_gate
        .seam
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .quota_bucket = Some(LOW.get_or_init(|| rs_youtube::quota::QuotaTracker::new(399)));
    let (status, body) = send(&a.state, authed("POST", "/api/v1/av-gate/session", CREATE)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(
        body,
        serde_json::json!({"error": "project_quota", "remaining": 399, "estimate": 400})
    );
}

#[tokio::test]
async fn a_client_that_disconnects_mid_create_does_not_abort_the_start() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let rig = FakeRig::default();
    *rig.start_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let rig = Arc::new(rig);
    let a = api().await;
    {
        let mut seam = a.state.av_gate.seam.lock().unwrap();
        seam.as_mut().unwrap().rig = rig.clone();
    }
    let router = build_router(a.state.clone());
    let request_task =
        tokio::spawn(router.oneshot(authed("POST", "/api/v1/av-gate/session", CREATE)));
    let started = async {
        while !rig.called(&format!("start:{}", crate::av_gate_driver::tests::EVENT)) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap();
    // The client goes away: axum drops the handler future.
    request_task.abort();
    gate.notify_one();
    let holder = async {
        loop {
            if let Some(h) = a.state.av_gate.registry.holder() {
                return h.session_id;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let id = tokio::time::timeout(Duration::from_secs(5), holder)
        .await
        .unwrap();
    wait_state(&a.state, &id, "ready").await;
}
