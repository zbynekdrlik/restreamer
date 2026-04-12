use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn migration_v13_delivery_log_tables_exist() {
    let pool = setup_db().await;

    sqlx::query(
        "INSERT INTO delivery_logs (instance_id, event_id, log_text) VALUES (0, 1, 'test log')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let count: i64 = sqlx::query("SELECT COUNT(*) as c FROM delivery_logs")
        .fetch_one(&pool)
        .await
        .map(|r| r.get("c"))
        .unwrap();
    assert_eq!(count, 1);

    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs)
         VALUES (0, 1, 'YT HLS', 1000, 42, 65, 'stdin_closed', 'Connection reset', 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let count: i64 = sqlx::query("SELECT COUNT(*) as c FROM delivery_restart_log")
        .fetch_one(&pool)
        .await
        .map(|r| r.get("c"))
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn insert_and_query_delivery_restart_log() {
    let pool = setup_db().await;

    insert_delivery_restart_record(
        &pool,
        99,
        Some(1),
        "YT HLS",
        1000,
        42,
        65,
        "stdin_closed",
        Some("Connection reset"),
        2,
    )
    .await
    .unwrap();
    insert_delivery_restart_record(
        &pool,
        99,
        Some(1),
        "YT HLS",
        2000,
        43,
        3,
        "stdin_closed",
        None,
        4,
    )
    .await
    .unwrap();
    insert_delivery_restart_record(
        &pool,
        100,
        Some(2),
        "Facebook",
        3000,
        10,
        120,
        "stdin_closed",
        Some("Broken pipe"),
        1,
    )
    .await
    .unwrap();

    let records = get_delivery_restart_log(&pool, 99).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].alias, "YT HLS");
    assert_eq!(records[0].timestamp_ms, 1000);
    assert_eq!(records[0].lifetime_secs, 65);
    assert_eq!(records[0].stderr_tail.as_deref(), Some("Connection reset"));
    assert_eq!(records[1].timestamp_ms, 2000);

    let records2 = get_delivery_restart_log(&pool, 100).await.unwrap();
    assert_eq!(records2.len(), 1);
    assert_eq!(records2[0].alias, "Facebook");
}

#[tokio::test]
async fn insert_and_query_delivery_logs() {
    let pool = setup_db().await;

    insert_delivery_log(
        &pool,
        99,
        Some(1),
        "INFO rs_delivery: started\nINFO endpoint: ffmpeg spawned",
    )
    .await
    .unwrap();

    let log = get_delivery_log(&pool, 99).await.unwrap();
    assert!(log.is_some());
    let log = log.unwrap();
    assert!(log.contains("ffmpeg spawned"));

    let empty = get_delivery_log(&pool, 999).await.unwrap();
    assert!(empty.is_none());
}

#[tokio::test]
async fn restart_log_dedup_by_timestamp() {
    let pool = setup_db().await;

    insert_delivery_restart_record(
        &pool,
        99,
        Some(1),
        "YT HLS",
        1000,
        42,
        65,
        "stdin_closed",
        None,
        2,
    )
    .await
    .unwrap();
    insert_delivery_restart_record(
        &pool,
        99,
        Some(1),
        "YT HLS",
        1000,
        42,
        65,
        "stdin_closed",
        None,
        2,
    )
    .await
    .unwrap();

    let records = get_delivery_restart_log(&pool, 99).await.unwrap();
    assert_eq!(records.len(), 1, "duplicate timestamp_ms should be ignored");
}
