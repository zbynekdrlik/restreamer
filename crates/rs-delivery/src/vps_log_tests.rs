//! #424: the VPS `/api/logs` serves the pinned session start AND the newest
//! tail, so the host's post-mortem capture (limit 5000) holds the go-live too
//! (it used to hold only the last 1000 lines of a long event).

use crate::api::{LogsResponse, router};
use crate::{AppState, VPS_LOG_HEAD_ENTRIES, VPS_LOG_TAIL_ENTRIES, vps_log_buffer};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::log_buffer::{LOG_GAP_TARGET, LogEntry};
use std::sync::Arc;
use tower::util::ServiceExt;

const TOKEN: &str = "vps-log-token";

#[tokio::test]
async fn the_log_serves_the_session_start_and_the_newest_tail() {
    assert_eq!((VPS_LOG_HEAD_ENTRIES, VPS_LOG_TAIL_ENTRIES), (2500, 2499));
    let mut state = AppState::new_for_test();
    state.log_buffer = vps_log_buffer();
    state.auth_token = tokio::sync::RwLock::new(Some(TOKEN.to_string()));
    let total = VPS_LOG_HEAD_ENTRIES + VPS_LOG_TAIL_ENTRIES + 50;
    for i in 0..total {
        state.log_buffer.push(LogEntry {
            level: "INFO".into(),
            target: "chunk_push".into(),
            message: format!("line-{i}"),
        });
    }
    let req = Request::builder()
        // The host's capture asks for exactly the whole buffer.
        .uri("/api/logs?limit=5000")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let resp = router(Arc::new(state)).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let logs: LogsResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        logs.entries.len(),
        VPS_LOG_HEAD_ENTRIES + VPS_LOG_TAIL_ENTRIES + 1
    );
    assert_eq!(logs.entries[0].message, format!("line-{}", total - 1));
    let gap = &logs.entries[VPS_LOG_TAIL_ENTRIES];
    assert_eq!(gap.target, LOG_GAP_TARGET);
    assert!(gap.message.starts_with("50 log entries"), "{}", gap.message);
    assert_eq!(
        logs.entries.last().map(|e| e.message.as_str()),
        Some("line-0"),
        "the session start is kept"
    );
}
