//! #437: the host forwards the operator's fast read-delay target to the
//! event's delivery VPS and passes its answer and refusals through.

use super::*;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn pool_with_instance(status: &str) -> (SqlitePool, i64) {
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    let event_id = rs_core::db::upsert_streaming_event(&pool, "fast-delay-test")
        .await
        .unwrap();
    let id = rs_core::db::create_delivery_instance(
        &pool,
        4370,
        "rs-delivery-evt437",
        "127.0.0.1",
        "cpx22",
        Some(event_id),
        "vps-token",
    )
    .await
    .unwrap();
    rs_core::db::update_delivery_instance_status(&pool, id, status)
        .await
        .unwrap();
    (pool, event_id)
}

fn request(event_id: i64) -> SetFastDelayRequest {
    SetFastDelayRequest {
        event_id,
        alias: "KS-PP-TEST".to_string(),
        target_secs: 30,
    }
}

#[tokio::test]
async fn the_target_reaches_the_vps_with_its_token_and_the_answer_comes_back() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/endpoints/fast_delay"))
        .and(header("authorization", "Bearer vps-token"))
        .and(body_json(
            serde_json::json!({ "alias": "KS-PP-TEST", "target_secs": 30 }),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "alias": "KS-PP-TEST", "from_secs": 9, "to_secs": 30 }),
        ))
        .expect(1)
        .mount(&vps)
        .await;
    let (pool, event_id) = pool_with_instance("delivering").await;

    let body = set_fast_delay(&pool, &request(event_id), vps.address().port())
        .await
        .expect("forwarded");
    assert_eq!(body["from_secs"], 9);
    assert_eq!(body["to_secs"], 30);
}

#[tokio::test]
async fn a_vps_refusal_passes_through_with_its_status() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/endpoints/fast_delay"))
        .respond_with(
            ResponseTemplate::new(409).set_body_json(serde_json::json!({ "error": "not_fast" })),
        )
        .mount(&vps)
        .await;
    let (pool, event_id) = pool_with_instance("delivering").await;

    let (status, msg) = set_fast_delay(&pool, &request(event_id), vps.address().port())
        .await
        .expect_err("refused");
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(msg.contains("not_fast"), "{msg}");
}

#[tokio::test]
async fn no_running_delivery_is_refused_without_calling_a_vps() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&vps)
        .await;
    let (pool, event_id) = pool_with_instance("stopping").await;
    let (status, _) = set_fast_delay(&pool, &request(event_id), vps.address().port())
        .await
        .expect_err("not active");
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = set_fast_delay(&pool, &request(event_id + 1), vps.address().port())
        .await
        .expect_err("no instance");
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_unreadable_vps_answer_is_a_bad_gateway() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&vps)
        .await;
    let (pool, event_id) = pool_with_instance("delivering").await;
    let (status, _) = set_fast_delay(&pool, &request(event_id), vps.address().port())
        .await
        .expect_err("unreadable");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn the_handler_answers_the_refusal_for_an_unknown_event() {
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = tokio::sync::broadcast::channel(4);
    let state = AppState::new_for_tests(pool, rs_core::config::Config::for_testing(), ws_tx);
    let err = delivery_set_fast_delay(State(state), Json(request(999)))
        .await
        .expect_err("no delivery");
    assert_eq!(err.0, StatusCode::NOT_FOUND);
}
