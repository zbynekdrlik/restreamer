//! #368: the VPS `/api/status` endpoint entry carries the YouTube backup
//! ingest state (CI asserts the backup is connected and pushing).

use crate::api::router;
use crate::{AppState, EndpointHandle};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tower::util::ServiceExt;

const TOKEN: &str = "backup-status-token";

async fn status_json(state: Arc<AppState>) -> serde_json::Value {
    let req = Request::builder()
        .uri("/api/status")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn state_with(alias: &str, handle: EndpointHandle) -> Arc<AppState> {
    let mut state = AppState::new_for_test();
    state.auth_token = tokio::sync::RwLock::new(Some(TOKEN.to_string()));
    let state = Arc::new(state);
    state
        .endpoints
        .write()
        .await
        .insert(alias.to_string(), handle);
    state
}

#[tokio::test]
async fn a_mirrored_endpoint_reports_its_backup_ingest() {
    let handle = EndpointHandle::stub_for_test(1);
    // A real mirror whose backup task idles (nothing is ever offered), so
    // the counters are exactly what the state below records.
    let tee = crate::backup_mirror::build_endpoint_pusher(
        &crate::backup_mirror::PushTarget {
            alias: "e2e rtmp".to_string(),
            service_type: rs_ffmpeg::ServiceType::YtRtmp,
            stream_key: "k".to_string(),
            backup_ingest: true,
            audit_ring: None,
        },
        &handle.stats_for_test(),
    )
    .await;
    assert!(tee.has_backup());
    let state = state_with("e2e rtmp", handle).await;

    let json = status_json(state).await;
    let ep = &json["endpoints"][0];
    assert_eq!(ep["alias"], "e2e rtmp");
    let backup = &ep["backup_ingest"];
    assert_eq!(backup["connected"], false, "nothing pushed yet: {ep}");
    assert_eq!(backup["chunks_pushed"], 0);
    assert_eq!(backup["dropped_chunks"], 0);
    assert_eq!(backup["reconnect_count"], 0);
    assert!(backup["last_error"].is_null());
    drop(tee);
}

#[tokio::test]
async fn an_endpoint_without_a_backup_has_no_backup_ingest_entry() {
    let state = state_with("FB-Zbynek", EndpointHandle::stub_for_test(1)).await;
    let json = status_json(state).await;
    let ep = &json["endpoints"][0];
    assert_eq!(ep["alias"], "FB-Zbynek");
    assert!(
        ep.get("backup_ingest").is_none(),
        "no backup -> the field is omitted: {ep}"
    );
}
