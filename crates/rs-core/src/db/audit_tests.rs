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

/// End-to-end audit flow: `record()` → `audit_writer_task` → `audit_log` row
/// persisted AND `WsEvent::AuditAppended` broadcast. This is the pipeline
/// that PR #129 wired up but left partially connected; we assert the full
/// loop works so a future regression can't silently drop audit rows again.
#[tokio::test]
async fn record_through_writer_task_persists_and_broadcasts() {
    use crate::audit;
    use tokio::sync::mpsc;

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, mut ws_rx) = broadcast::channel(16);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditRow>(1024);

    // Spawn the writer exactly as the runtime does.
    let writer_pool = pool.clone();
    let writer_ws_tx = ws_tx.clone();
    let writer = tokio::spawn(async move {
        audit::audit_writer_task(writer_pool, writer_ws_tx, audit_rx).await;
    });

    // Fire a VPS-sourced row through the fire-and-forget API, exactly as
    // mirror_vps_audit does after the C3 wire-up.
    audit::record(
        &audit_tx,
        AuditRow {
            severity: Severity::Warn,
            source: Source::Ffmpeg,
            event_id: Some(101),
            instance_id: Some(42),
            endpoint: Some("YT NLW 4k".into()),
            action: Action::EndpointFfmpegDied,
            detail: serde_json::json!({
                "lifetime_secs": 47,
                "reason": "youtube_rtmp_closed",
            }),
            ts_override: None,
        },
    );

    // Writer batches for up to 100ms, so give it room. Poll for up to 2s.
    let mut count: i64 = 0;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        count = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(&pool)
            .await
            .unwrap();
        if count > 0 {
            break;
        }
    }
    assert_eq!(count, 1, "row must be persisted end-to-end");

    // Matching WS broadcast must have fired.
    let ev = tokio::time::timeout(std::time::Duration::from_secs(1), ws_rx.recv())
        .await
        .expect("broadcast timed out")
        .unwrap();
    match ev {
        crate::models::WsEvent::AuditAppended {
            source,
            action,
            endpoint,
            event_id,
            ..
        } => {
            assert_eq!(source, "ffmpeg");
            assert_eq!(action, "endpoint_ffmpeg_died");
            assert_eq!(endpoint.as_deref(), Some("YT NLW 4k"));
            assert_eq!(event_id, Some(101));
        }
        other => panic!("expected AuditAppended, got {other:?}"),
    }

    // Drop the sender so the writer task exits cleanly.
    drop(audit_tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), writer).await;
}

/// `audit_writer_task` must flush on the time deadline even if the batch
/// is nowhere near 32 rows. Single-row submission should still land in
/// the DB within ~100 ms (the FLUSH_AFTER constant).
#[tokio::test]
async fn writer_task_flushes_on_time_deadline() {
    use crate::audit;
    use tokio::sync::mpsc;

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _ws_rx) = broadcast::channel(16);
    let (audit_tx, audit_rx) = mpsc::channel::<AuditRow>(16);

    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        audit::audit_writer_task(writer_pool, ws_tx, audit_rx).await;
    });

    let start = std::time::Instant::now();
    audit::record(
        &audit_tx,
        AuditRow {
            severity: Severity::Info,
            source: Source::Operator,
            event_id: Some(7),
            instance_id: None,
            endpoint: None,
            action: Action::EventStarted,
            detail: serde_json::json!({}),
            ts_override: None,
        },
    );

    // Time-based flush: should land within a few hundred ms, not wait
    // for batch to fill. Wait up to 500 ms and assert.
    let mut count: i64 = 0;
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        count = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(&pool)
            .await
            .unwrap();
        if count > 0 {
            break;
        }
    }
    assert_eq!(count, 1, "single row must be flushed on time deadline");
    assert!(
        start.elapsed() < std::time::Duration::from_millis(600),
        "time-based flush took {:?} — must be <600 ms",
        start.elapsed()
    );

    drop(audit_tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), writer).await;
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

// ---------------------------------------------------------------------
// group_audit_rows — issue #169 (activity-log burst dedup)
// ---------------------------------------------------------------------

fn fake_row(id: i64, ts: &str, source: &str, action: &str, endpoint: Option<&str>) -> AuditLogRow {
    AuditLogRow {
        id,
        ts: ts.to_string(),
        severity: "warn".to_string(),
        source: source.to_string(),
        event_id: None,
        instance_id: None,
        endpoint: endpoint.map(|s| s.to_string()),
        action: action.to_string(),
        detail: serde_json::json!({"id": id}),
    }
}

#[test]
fn group_audit_rows_empty_returns_empty() {
    let groups = audit::group_audit_rows(Vec::new(), 60);
    assert!(groups.is_empty());
}

#[test]
fn group_audit_rows_window_zero_keeps_singletons() {
    // window=0 disables grouping. Each row → its own group.
    let rows = vec![
        fake_row(
            3,
            "2026-05-04T18:00:30Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
        fake_row(
            2,
            "2026-05-04T18:00:15Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
        fake_row(
            1,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
    ];
    let groups = audit::group_audit_rows(rows, 0);
    assert_eq!(groups.len(), 3);
    assert!(groups.iter().all(|g| g.count == 1));
}

#[test]
fn group_audit_rows_collapses_repeats_within_window() {
    // 3 identical events 15 s apart, window=60 s → collapse to 1 group.
    let rows = vec![
        fake_row(
            3,
            "2026-05-04T18:00:30Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
        fake_row(
            2,
            "2026-05-04T18:00:15Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
        fake_row(
            1,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
    ];
    let groups = audit::group_audit_rows(rows, 60);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].count, 3);
    assert_eq!(groups[0].last_ts, "2026-05-04T18:00:30Z");
    assert_eq!(groups[0].first_ts, "2026-05-04T18:00:00Z");
    // Sample id = newest row in the group (the first one in the input,
    // since input is sorted newest-first).
    assert_eq!(groups[0].sample_id, 3);
}

#[test]
fn group_audit_rows_keeps_distinct_keys_separate() {
    // Same time, different endpoints → 2 groups.
    let rows = vec![
        fake_row(
            2,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ),
        fake_row(
            1,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-NewLevel"),
        ),
    ];
    let groups = audit::group_audit_rows(rows, 60);
    assert_eq!(groups.len(), 2);
    assert!(groups.iter().all(|g| g.count == 1));
}

#[test]
fn group_audit_rows_breaks_group_when_window_exceeded() {
    // Two rows far apart with one matching key but ts span > window.
    // → 2 groups, NOT 1.
    let rows = vec![
        fake_row(
            2,
            "2026-05-04T18:30:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("YT NLW 4k"),
        ),
        fake_row(
            1,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("YT NLW 4k"),
        ),
    ];
    let groups = audit::group_audit_rows(rows, 60);
    assert_eq!(groups.len(), 2);
}

#[test]
fn group_audit_rows_window_boundary_inclusive() {
    // Boundary: rows EXACTLY window_secs apart should be grouped
    // (kills mutant that flips < to <=).
    let rows = vec![
        fake_row(
            2,
            "2026-05-04T18:01:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("YT NLW 4k"),
        ),
        fake_row(
            1,
            "2026-05-04T18:00:00Z",
            "vps",
            "endpoint_rtmp_push_died",
            Some("YT NLW 4k"),
        ),
    ];
    let groups = audit::group_audit_rows(rows, 60);
    assert_eq!(
        groups.len(),
        1,
        "exactly 60 s gap is at boundary, must group"
    );
    assert_eq!(groups[0].count, 2);
}

#[test]
fn group_audit_rows_25_in_5min_collapses_to_one() {
    // The user-visible motivation case from #169: 25 endpoint_rtmp_push_died
    // rows within 5 min for one alias must collapse to a single grouped row
    // showing count=25.
    let mut rows = Vec::new();
    let base = chrono::DateTime::parse_from_rfc3339("2026-05-04T18:05:00Z").unwrap();
    for i in 0..25 {
        let t = base - chrono::Duration::seconds(i * 12); // 12 s apart
        rows.push(fake_row(
            (25 - i) as i64,
            &t.to_rfc3339(),
            "vps",
            "endpoint_rtmp_push_died",
            Some("FB-Zbynek"),
        ));
    }
    let groups = audit::group_audit_rows(rows, 300);
    assert_eq!(groups.len(), 1, "all 25 must collapse to one");
    assert_eq!(groups[0].count, 25);
}
