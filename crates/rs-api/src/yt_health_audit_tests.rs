//! Verifies that the audit emitter fires exactly once on top_issue transition.

use crate::delivery_yt_health::record_and_maybe_emit;
use rs_core::audit::{Action, AuditRow};
use tokio::sync::mpsc;

#[tokio::test]
async fn record_and_maybe_emit_fires_on_first_observation() {
    let (tx, mut rx) = mpsc::channel::<AuditRow>(8);
    let emitted = record_and_maybe_emit(None, Some("videoIngestionStarved"), "ytbb", &tx).await;
    assert!(emitted, "first observation must emit");
    let row = rx.recv().await.expect("row must be sent");
    assert_eq!(row.action, Action::YoutubeIssueChanged);
    assert_eq!(row.endpoint.as_deref(), Some("ytbb"));
}

#[tokio::test]
async fn record_and_maybe_emit_silent_on_same_value() {
    let (tx, mut rx) = mpsc::channel::<AuditRow>(8);
    let emitted = record_and_maybe_emit(
        Some("videoIngestionStarved"),
        Some("videoIngestionStarved"),
        "ytbb",
        &tx,
    )
    .await;
    assert!(!emitted);
    assert!(rx.try_recv().is_err());
}
