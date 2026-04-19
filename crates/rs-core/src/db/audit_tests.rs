//! Tests for audit_log DB access.

use super::*;
use crate::audit::{Action, AuditRow, Severity, Source};
use tokio::sync::broadcast;

#[tokio::test]
async fn insert_batch_persists_rows_and_broadcasts() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, mut ws_rx) = broadcast::channel(16);

    let rows = vec![
        AuditRow {
            severity: Severity::Info,
            source: Source::Operator,
            event_id: Some(1),
            instance_id: None,
            endpoint: None,
            action: Action::EventStarted,
            detail: serde_json::json!({"name":"test"}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Error,
            source: Source::Ffmpeg,
            event_id: Some(1),
            instance_id: Some(42),
            endpoint: Some("YT NLW 4k".to_string()),
            action: Action::EndpointFfmpegDied,
            detail: serde_json::json!({"chunk_id":1436,"reason_class":"youtube_rtmp_closed"}),
            ts_override: None,
        },
    ];
    audit::insert_batch(&pool, &rows, &ws_tx).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let _ev1 = ws_rx.recv().await.unwrap();
    let _ev2 = ws_rx.recv().await.unwrap();
}

#[tokio::test]
async fn query_filters_event_and_severity() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _rx) = broadcast::channel(16);

    let rows = vec![
        AuditRow {
            severity: Severity::Info,
            source: Source::Operator,
            event_id: Some(1),
            instance_id: None,
            endpoint: None,
            action: Action::EventStarted,
            detail: serde_json::json!({}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Error,
            source: Source::Ffmpeg,
            event_id: Some(1),
            instance_id: None,
            endpoint: None,
            action: Action::EndpointFfmpegDied,
            detail: serde_json::json!({}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Info,
            source: Source::Operator,
            event_id: Some(2),
            instance_id: None,
            endpoint: None,
            action: Action::EventStarted,
            detail: serde_json::json!({}),
            ts_override: None,
        },
    ];
    audit::insert_batch(&pool, &rows, &ws_tx).await.unwrap();

    let filtered = audit::query(
        &pool,
        audit::Filter {
            event_id: Some(1),
            severities: vec!["error".to_string()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].severity, "error");
    assert_eq!(filtered[0].event_id, Some(1));
}

#[tokio::test]
async fn get_by_id_roundtrip() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _rx) = broadcast::channel(16);

    audit::insert_batch(
        &pool,
        &[AuditRow {
            severity: Severity::Warn,
            source: Source::Vps,
            event_id: Some(5),
            instance_id: Some(7),
            endpoint: Some("ep1".to_string()),
            action: Action::EndpointAliveTransition,
            detail: serde_json::json!({"was":true,"is":false}),
            ts_override: None,
        }],
        &ws_tx,
    )
    .await
    .unwrap();

    let id: i64 = sqlx::query_scalar("SELECT id FROM audit_log LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let row = audit::get_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.severity, "warn");
    assert_eq!(row.event_id, Some(5));
    assert_eq!(row.endpoint.as_deref(), Some("ep1"));
    assert_eq!(row.detail["was"], true);

    let missing = audit::get_by_id(&pool, 999999).await.unwrap();
    assert!(missing.is_none());
}
