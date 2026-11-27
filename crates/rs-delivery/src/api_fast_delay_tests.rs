//! #437: `POST /api/endpoints/fast_delay` sets a fast endpoint's read-delay
//! target live: clamped, published on the endpoint, audited, auth-gated.

use super::*;
use axum::body::Body;
use axum::http::Request;
use rs_core::audit::Action;
use tokio::sync::RwLock;
use tower::ServiceExt;

const TOKEN: &str = "vps-token";

fn state_with(endpoints: Vec<(&str, bool)>) -> Arc<AppState> {
    let mut state = AppState::new_for_test();
    state.auth_token = RwLock::new(Some(TOKEN.to_string()));
    let mut map = std::collections::HashMap::new();
    for (alias, is_fast) in endpoints {
        let mut cfg = crate::EndpointHandle::stub_for_test(1).config().clone();
        cfg.alias = alias.to_string();
        cfg.is_fast = is_fast;
        map.insert(
            alias.to_string(),
            crate::EndpointHandle::stub_with_config_for_test(cfg, 1),
        );
    }
    state.endpoints = RwLock::new(map);
    Arc::new(state)
}

async fn post(
    state: Arc<AppState>,
    token: Option<&str>,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/api/endpoints/fast_delay")
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    let resp = crate::api::router(state).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn the_operator_sets_a_fast_endpoints_target_and_it_is_audited() {
    let state = state_with(vec![("KS-PP-TEST", true)]);
    let (status, body) = post(
        Arc::clone(&state),
        Some(TOKEN),
        serde_json::json!({ "alias": "KS-PP-TEST", "target_secs": 30 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        serde_json::json!({ "alias": "KS-PP-TEST", "from_secs": 5, "to_secs": 30 })
    );
    assert_eq!(
        state.endpoints.read().await["KS-PP-TEST"].fast_delay_target_secs(),
        Some(30),
        "the new target is published at once"
    );
    let (rows, _) = state.audit_ring.since(0);
    let set: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::FastDelaySet)
        .collect();
    assert_eq!(set.len(), 1, "{rows:?}");
    assert_eq!(set[0].endpoint.as_deref(), Some("KS-PP-TEST"));
    assert_eq!(set[0].detail["from_secs"], 5);
    assert_eq!(set[0].detail["to_secs"], 30);
    assert_eq!(set[0].detail["requested_secs"], 30);
    assert_eq!(set[0].detail["by"], "operator");
}

#[tokio::test]
async fn an_unchanged_target_answers_the_same_and_is_not_audited() {
    let state = state_with(vec![("fast", true)]);
    let body = serde_json::json!({ "alias": "fast", "target_secs": 30 });
    let (status, _) = post(Arc::clone(&state), Some(TOKEN), body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let (status, again) = post(Arc::clone(&state), Some(TOKEN), body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        again,
        serde_json::json!({ "alias": "fast", "from_secs": 30, "to_secs": 30 })
    );
    let (rows, _) = state.audit_ring.since(0);
    assert_eq!(
        rows.iter()
            .filter(|r| r.action == Action::FastDelaySet)
            .count(),
        1,
        "only the change is audited: {rows:?}"
    );
}

#[tokio::test]
async fn the_target_is_clamped_to_the_ceiling() {
    let state = state_with(vec![("fast", true)]);
    let (status, body) = post(
        state,
        Some(TOKEN),
        serde_json::json!({ "alias": "fast", "target_secs": 9999 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["to_secs"], crate::fast_delay::FAST_DELAY_CEILING_SECS);
}

#[tokio::test]
async fn an_unknown_or_non_fast_endpoint_is_refused_and_not_audited() {
    let state = state_with(vec![("normal", false)]);
    let (status, body) = post(
        Arc::clone(&state),
        Some(TOKEN),
        serde_json::json!({ "alias": "missing", "target_secs": 30 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
    let (status, body) = post(
        Arc::clone(&state),
        Some(TOKEN),
        serde_json::json!({ "alias": "normal", "target_secs": 30 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "not_fast");
    let (rows, _) = state.audit_ring.since(0);
    assert!(rows.iter().all(|r| r.action != Action::FastDelaySet));
}

#[tokio::test]
async fn the_route_needs_the_bearer_token() {
    let state = state_with(vec![("fast", true)]);
    let (status, _) = post(
        Arc::clone(&state),
        None,
        serde_json::json!({ "alias": "fast", "target_secs": 30 }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        state.endpoints.read().await["fast"].fast_delay_target_secs(),
        None,
        "an unauthenticated request changes nothing"
    );
}
