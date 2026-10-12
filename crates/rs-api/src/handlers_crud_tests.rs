//! The event and endpoint CRUD handlers (`handlers_events.rs`,
//! `handlers_endpoints.rs`), called directly against an in-memory DB.
//! Before #367 they were exercised only from rs-service's e2e test, so
//! cargo-mutants (which runs only the mutated crate's tests) saw every
//! side effect as untested: a handler replaced by `Ok(200)` still passed.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;

use crate::handlers::{
    CreateEndpointRequest, UpdateEndpointRequest, activate_event, attach_endpoint_to_event,
    create_endpoint, deactivate_event, delete_endpoint, detach_endpoint_from_event,
    get_endpoint_by_id, get_event_endpoints, list_endpoints, list_events, start_delivering,
    update_endpoint,
};
use crate::state::AppState;

async fn state() -> (AppState, broadcast::Receiver<WsEvent>) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let (ws_tx, ws_rx) = broadcast::channel(16);
    let state = AppState::new_for_tests(pool, rs_core::config::Config::for_testing(), ws_tx);
    (state, ws_rx)
}

fn ws_action(rx: &mut broadcast::Receiver<WsEvent>) -> (String, bool, bool) {
    match rx.try_recv().expect("the handler must broadcast a WsEvent") {
        WsEvent::StreamingEvent {
            action,
            receiving,
            delivering,
            ..
        } => (action, receiving, delivering),
        other => panic!("expected WsEvent::StreamingEvent, got {other:?}"),
    }
}

fn create_req(alias: &str, service_type: &str) -> Json<CreateEndpointRequest> {
    Json(CreateEndpointRequest {
        alias: alias.to_string(),
        service_type: service_type.to_string(),
        stream_key: "key-1".to_string(),
        is_fast: None,
    })
}

async fn create_ok(state: &AppState, alias: &str) -> i64 {
    let (status, Json(body)) = create_endpoint(State(state.clone()), create_req(alias, "YT_RTMP"))
        .await
        .expect("a valid endpoint is created");
    assert_eq!(status, StatusCode::CREATED);
    body["id"]
        .as_i64()
        .expect("the response carries the new id")
}

fn update_req() -> UpdateEndpointRequest {
    UpdateEndpointRequest {
        alias: None,
        service_type: None,
        stream_key: None,
        enabled: None,
        is_fast: None,
    }
}

#[tokio::test]
async fn list_events_returns_the_stored_events() {
    let (state, _rx) = state().await;
    db::create_streaming_event(&state.pool, "sunday")
        .await
        .unwrap();
    let Json(events) = list_events(State(state.clone())).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name, "sunday");
}

#[tokio::test]
async fn activate_deactivate_and_start_delivering_update_the_event_and_broadcast() {
    let (state, mut rx) = state().await;
    let id = db::create_streaming_event(&state.pool, "sunday")
        .await
        .unwrap();

    assert_eq!(
        activate_event(State(state.clone()), Path(id)).await,
        Ok(StatusCode::OK)
    );
    let evt = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(evt.receiving_activated, "activate sets receiving");
    assert_eq!(ws_action(&mut rx), ("activated".into(), true, false));

    assert_eq!(
        start_delivering(State(state.clone()), Path(id)).await,
        Ok(StatusCode::OK)
    );
    let evt = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(evt.delivering_activated, "start_delivering sets delivering");
    assert_eq!(
        ws_action(&mut rx),
        ("delivering_started".into(), true, true)
    );

    assert_eq!(
        deactivate_event(State(state.clone()), Path(id)).await,
        Ok(StatusCode::OK)
    );
    let evt = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !evt.receiving_activated && !evt.delivering_activated,
        "deactivate clears both"
    );
    assert_eq!(ws_action(&mut rx), ("deactivated".into(), false, false));
}

#[tokio::test]
async fn event_actions_on_an_unknown_event_are_404() {
    let (state, _rx) = state().await;
    let nf = Err(StatusCode::NOT_FOUND);
    assert_eq!(activate_event(State(state.clone()), Path(999)).await, nf);
    assert_eq!(deactivate_event(State(state.clone()), Path(999)).await, nf);
    assert_eq!(start_delivering(State(state.clone()), Path(999)).await, nf);
}

#[tokio::test]
async fn create_endpoint_validates_alias_and_service_type() {
    let (state, _rx) = state().await;
    let bad = |alias: &str, st: &str| create_endpoint(State(state.clone()), create_req(alias, st));
    assert_eq!(
        bad("", "YT_RTMP").await.unwrap_err(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        bad("   ", "YT_RTMP").await.unwrap_err(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        bad(&"a".repeat(256), "YT_RTMP").await.unwrap_err(),
        StatusCode::BAD_REQUEST,
        "256 chars is over the limit"
    );
    assert_eq!(
        bad("yt", "TWITCH").await.unwrap_err(),
        StatusCode::BAD_REQUEST
    );

    // 255 chars is exactly the limit, and every listed service type is valid.
    let long_ok = create_ok(&state, &"a".repeat(255)).await;
    for st in ["FB", "YT_RTMP", "VIMEO", "INSTAGRAM", "TEST_FILE"] {
        let (status, _) = create_endpoint(State(state.clone()), create_req(st, st))
            .await
            .unwrap_or_else(|e| panic!("{st} must be accepted, got {e}"));
        assert_eq!(status, StatusCode::CREATED);
    }
    let Json(all) = list_endpoints(State(state.clone())).await.unwrap();
    assert_eq!(all.len(), 6, "list returns every created endpoint");
    assert!(all.iter().any(|e| e.id == long_ok && e.alias.len() == 255));
}

#[tokio::test]
async fn update_endpoint_validates_then_persists() {
    let (state, _rx) = state().await;
    let id = create_ok(&state, "yt").await;
    let upd =
        |req: UpdateEndpointRequest| update_endpoint(State(state.clone()), Path(id), Json(req));

    let rejected = [
        UpdateEndpointRequest {
            service_type: Some("TWITCH".into()),
            ..update_req()
        },
        UpdateEndpointRequest {
            alias: Some("  ".into()),
            ..update_req()
        },
        UpdateEndpointRequest {
            alias: Some("a".repeat(256)),
            ..update_req()
        },
    ];
    for req in rejected {
        assert_eq!(upd(req).await, Err(StatusCode::BAD_REQUEST));
    }
    let unchanged = db::get_endpoint_config(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.alias, "yt", "a rejected update changes nothing");

    let req = UpdateEndpointRequest {
        alias: Some("b".repeat(255)),
        service_type: Some("FB".into()),
        stream_key: Some("key-2".into()),
        enabled: Some(false),
        is_fast: Some(true),
    };
    assert_eq!(upd(req).await, Ok(StatusCode::OK));
    let ep = db::get_endpoint_config(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ep.alias, "b".repeat(255));
    assert_eq!(ep.service_type, "FB");
    assert_eq!(ep.stream_key, "key-2");
    assert!(!ep.enabled && ep.is_fast);

    assert_eq!(
        update_endpoint(State(state.clone()), Path(999), Json(update_req())).await,
        Err(StatusCode::NOT_FOUND)
    );
}

#[tokio::test]
async fn attach_list_detach_and_delete_endpoint() {
    let (state, _rx) = state().await;
    let event = db::create_streaming_event(&state.pool, "sunday")
        .await
        .unwrap();
    let ep = create_ok(&state, "yt").await;

    assert_eq!(
        attach_endpoint_to_event(State(state.clone()), Path((event, ep))).await,
        Ok(StatusCode::CREATED)
    );
    let Json(linked) = get_event_endpoints(State(state.clone()), Path(event))
        .await
        .unwrap();
    assert_eq!(linked.iter().map(|e| e.id).collect::<Vec<_>>(), vec![ep]);

    assert_eq!(
        detach_endpoint_from_event(State(state.clone()), Path((event, ep))).await,
        Ok(StatusCode::NO_CONTENT)
    );
    let Json(linked) = get_event_endpoints(State(state.clone()), Path(event))
        .await
        .unwrap();
    assert!(linked.is_empty(), "detach removes the link");

    assert_eq!(
        delete_endpoint(State(state.clone()), Path(ep)).await,
        Ok(StatusCode::NO_CONTENT)
    );
    assert!(
        db::get_endpoint_config(&state.pool, ep)
            .await
            .unwrap()
            .is_none()
    );
}

/// #391: the endpoint read routes are internet-reachable (behind Access) and
/// their bodies end up in logs and transcripts. None of them may carry the
/// stored stream key; each carries the `***` + last-4 mask instead.
#[tokio::test]
async fn endpoint_read_routes_never_return_the_stored_stream_key() {
    let (state, _rx) = state().await;
    let stored_key = "FB-0000000000-0-fixturefixtMnOp";
    let (_, Json(body)) = create_endpoint(
        State(state.clone()),
        Json(CreateEndpointRequest {
            alias: "fb".into(),
            service_type: "FB".into(),
            stream_key: stored_key.into(),
            is_fast: None,
        }),
    )
    .await
    .unwrap();
    let id = body["id"].as_i64().unwrap();
    let event = db::create_streaming_event(&state.pool, "sunday")
        .await
        .unwrap();
    db::attach_endpoint_to_event(&state.pool, event, id)
        .await
        .unwrap();
    let template = db::create_template(&state.pool, "tpl", None, None)
        .await
        .unwrap();
    db::attach_endpoint_to_template(&state.pool, template, id)
        .await
        .unwrap();

    let Json(list) = list_endpoints(State(state.clone())).await.unwrap();
    let Json(one) = get_endpoint_by_id(State(state.clone()), Path(id))
        .await
        .unwrap();
    let Json(by_event) = get_event_endpoints(State(state.clone()), Path(event))
        .await
        .unwrap();
    let Json(by_template) =
        crate::template_handlers::get_template_endpoints(State(state.clone()), Path(template))
            .await
            .unwrap();

    for (route, body) in [
        ("GET /endpoints", serde_json::to_string(&list).unwrap()),
        ("GET /endpoints/{id}", serde_json::to_string(&one).unwrap()),
        (
            "GET /events/{id}/endpoints",
            serde_json::to_string(&by_event).unwrap(),
        ),
        (
            "GET /templates/{id}/endpoints",
            serde_json::to_string(&by_template).unwrap(),
        ),
    ] {
        assert!(
            !body.contains(stored_key),
            "{route} leaked the stream key: {body}"
        );
        assert!(
            body.contains("\"stream_key\":\"***MnOp\""),
            "{route} must carry the masked key: {body}"
        );
    }

    let stored = db::get_endpoint_config(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.stream_key, stored_key,
        "masking never touches the DB row"
    );
}

/// #391: a client that reads the masked list and PUTs the row back verbatim
/// (or sends an empty key) must keep the stored key, never overwrite it with
/// the mask. A genuinely new key still replaces it.
#[tokio::test]
async fn update_with_masked_or_empty_key_keeps_the_stored_key() {
    let (state, _rx) = state().await;
    let id = create_ok(&state, "yt").await; // stored key "key-1"
    let put = |key: &str| {
        let state = state.clone();
        let key = key.to_string();
        async move {
            update_endpoint(
                State(state),
                Path(id),
                Json(UpdateEndpointRequest {
                    stream_key: Some(key),
                    ..update_req()
                }),
            )
            .await
        }
    };
    let stored = || async {
        db::get_endpoint_config(&state.pool, id)
            .await
            .unwrap()
            .unwrap()
            .stream_key
    };

    let Json(listed) = get_endpoint_by_id(State(state.clone()), Path(id))
        .await
        .unwrap();
    assert_eq!(put(&listed.stream_key).await, Ok(StatusCode::OK));
    assert_eq!(stored().await, "key-1", "echoing the mask keeps the key");

    assert_eq!(put("").await, Ok(StatusCode::OK));
    assert_eq!(stored().await, "key-1", "an empty key keeps the key");

    assert_eq!(put("abcd-efgh-ijkl-mnop").await, Ok(StatusCode::OK));
    assert_eq!(
        stored().await,
        "abcd-efgh-ijkl-mnop",
        "a new key replaces it"
    );
}
