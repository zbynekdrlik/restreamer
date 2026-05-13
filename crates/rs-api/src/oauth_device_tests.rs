//! Axum handlers for the Device Code Flow.

use crate::oauth_device::{DeviceStartBody, DeviceStartResponse, device_start};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::youtube_oauth as yo;
use rs_core::db::{create_memory_pool, run_migrations};

async fn make_state_with_device_config(api_base: &str) -> crate::state::AppState {
    use rs_core::config::{Config, DeviceFlowConfig};
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let mut config = Config::for_testing();
    config.youtube.device_flow = DeviceFlowConfig {
        client_id: "test-client".into(),
        client_secret: "test-secret".into(),
        daily_quota: 10_000,
    };
    let (ws_tx, _) = tokio::sync::broadcast::channel(16);
    crate::state::AppState::new_for_tests(pool, config, ws_tx)
        .with_device_flow_api_base(api_base.to_string())
}

#[tokio::test]
async fn device_start_rejects_invalid_label() {
    let state = make_state_with_device_config("http://unused").await;
    let r = device_start(
        State(state),
        Json(DeviceStartBody {
            label: "Bad Label!".into(),
        }),
    )
    .await;
    assert!(
        matches!(r, Err(StatusCode::BAD_REQUEST)),
        "invalid label must yield 400; got {:?}",
        r.err()
    );
}

#[tokio::test]
async fn device_start_409_when_label_already_authorized() {
    use wiremock::MockServer;
    let server = MockServer::start().await;
    let state = make_state_with_device_config(&server.uri()).await;
    yo::upsert_oauth_by_label(
        &state.pool,
        "bb",
        "AT",
        "RT",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "scope",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let r = device_start(State(state), Json(DeviceStartBody { label: "bb".into() })).await;
    assert!(matches!(r, Err(StatusCode::CONFLICT)));
}

#[tokio::test]
async fn device_start_happy_path_persists_grant() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/device/code"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"device_code":"DEV","user_code":"AB-CD-12","verification_url":"https://www.google.com/device","expires_in":1800,"interval":5}"#
        ))
        .mount(&server).await;
    let state = make_state_with_device_config(&server.uri()).await;
    let pool = state.pool.clone();
    let r = device_start(State(state), Json(DeviceStartBody { label: "bb".into() })).await;
    let Json(resp): Json<DeviceStartResponse> = r.expect("ok");
    assert_eq!(resp.user_code, "AB-CD-12");
    assert_eq!(resp.verification_url, "https://www.google.com/device");
    assert_eq!(resp.expires_in, 1800);
    use rs_core::db::oauth_device_grants as g;
    let got = g::get_by_label(&pool, "bb").await.unwrap().expect("row");
    assert_eq!(got.status, "pending");
    assert_eq!(got.user_code, "AB-CD-12");
}
