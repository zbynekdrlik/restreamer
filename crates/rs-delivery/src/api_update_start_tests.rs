//! Tests for the VPS-side POST /api/endpoints/update_start handler.
//!
//! The handler tears down the existing EndpointHandle for the given alias
//! and respawns it with a new start_chunk_id, mirroring the existing
//! add_endpoint pattern (api.rs:401). Called by the host at VPS-ready
//! time to push a freshly-computed live-edge to is_fast endpoints.

use crate::api::{UpdateStartRequest, update_start_handler};
use crate::{AppState, EndpointHandle};
use axum::{Json, extract::State, http::StatusCode};
use std::sync::Arc;

fn test_state_with_stub_endpoint(alias: &str, start: i64) -> Arc<AppState> {
    let state = Arc::new(AppState::new_for_test());
    let stub = EndpointHandle::stub_for_test(start);
    state
        .endpoints
        .blocking_write()
        .insert(alias.to_string(), stub);
    state
}

#[tokio::test]
async fn update_start_swaps_handle_for_known_alias() {
    let state = test_state_with_stub_endpoint("kiko", 100);

    let req = UpdateStartRequest {
        alias: "kiko".to_string(),
        new_start_chunk_id: 250,
    };
    let result = update_start_handler(State(state.clone()), Json(req)).await;
    assert!(result.is_ok(), "expected 200, got {result:?}");

    let endpoints = state.endpoints.read().await;
    let handle = endpoints.get("kiko").expect("kiko must still exist");
    assert_eq!(
        handle.start_chunk_id(),
        250,
        "EndpointHandle's start_chunk_id must reflect new value"
    );
}

#[tokio::test]
async fn update_start_returns_404_for_unknown_alias() {
    let state = test_state_with_stub_endpoint("kiko", 100);

    let req = UpdateStartRequest {
        alias: "ghost".to_string(),
        new_start_chunk_id: 250,
    };
    let err = update_start_handler(State(state.clone()), Json(req))
        .await
        .expect_err("ghost alias should fail");
    assert_eq!(err, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn update_start_emits_endpoint_start_chunk_updated_audit_on_success() {
    let state = test_state_with_stub_endpoint("kiko", 100);

    let (pre_rows, _pre_cursor) = state.audit_ring.since(0);
    let pre_count = pre_rows.len();

    let req = UpdateStartRequest {
        alias: "kiko".to_string(),
        new_start_chunk_id: 250,
    };
    update_start_handler(State(state.clone()), Json(req))
        .await
        .unwrap();

    let (rows, _) = state.audit_ring.since(0);
    assert_eq!(
        rows.len(),
        pre_count + 1,
        "exactly one audit row added by update_start"
    );
    let row = rows.last().unwrap();
    assert_eq!(
        row.action,
        rs_core::audit::Action::EndpointStartChunkUpdated
    );
    assert_eq!(row.detail["alias"], "kiko");
    assert_eq!(row.detail["old_start_chunk_id"], 100);
    assert_eq!(row.detail["new_start_chunk_id"], 250);
}
