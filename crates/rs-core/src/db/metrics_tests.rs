//! Tests for delivery_endpoint_metrics DB access.

use super::*;

#[tokio::test]
async fn insert_metrics_row_round_trips() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    metrics::insert(
        &pool,
        1234567890_i64,
        1,
        1,
        "yt1",
        true,
        100,
        99,
        10.5,
        1_000_000,
        0,
        Some("normal"),
    )
    .await
    .unwrap();

    let rows = metrics::query(
        &pool,
        metrics::Filter {
            event_id: Some(1),
            alias: Some("yt1".into()),
            since_ms: None,
            until_ms: None,
            limit: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].alias, "yt1");
    assert!((rows[0].chunk_delay_secs - 10.5).abs() < 1e-9);
    assert!(rows[0].alive);
}

#[tokio::test]
async fn query_filters_by_event_and_alias() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    for (ev, alias, ts) in &[(1, "yt1", 1000), (1, "yt2", 1000), (2, "yt1", 1000)] {
        metrics::insert(
            &pool,
            *ts,
            1,
            *ev,
            alias,
            true,
            10,
            10,
            5.0,
            1000,
            0,
            Some("normal"),
        )
        .await
        .unwrap();
    }

    let rows = metrics::query(
        &pool,
        metrics::Filter {
            event_id: Some(1),
            alias: Some("yt1".into()),
            since_ms: None,
            until_ms: None,
            limit: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].alias, "yt1");
    assert_eq!(rows[0].event_id, 1);
}

#[tokio::test]
async fn query_filters_by_time_range() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    for ts in &[1000_i64, 2000, 3000, 4000] {
        metrics::insert(&pool, *ts, 1, 1, "yt1", true, 10, 10, 5.0, 1000, 0, None)
            .await
            .unwrap();
    }
    let rows = metrics::query(
        &pool,
        metrics::Filter {
            event_id: Some(1),
            alias: None,
            since_ms: Some(2000),
            until_ms: Some(3500),
            limit: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].ts_ms, 2000);
    assert_eq!(rows[1].ts_ms, 3000);
}
