use super::*;

#[tokio::test]
async fn fresh_database_reaches_max_schema_version() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    let current: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(current, MAX_SCHEMA_VERSION);
}

#[tokio::test]
async fn migrations_idempotent_when_schema_version_rewound() {
    // Simulates the #112 failure mode: a prior interrupted run left the
    // schema fully advanced but schema_version was rolled back to an older
    // value. Re-running migrations must succeed — ALTER TABLE ADD COLUMN
    // statements must no-op when the column already exists.
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    // Simulate rolled-back schema_version while schema is intact.
    sqlx::query("DELETE FROM schema_version WHERE version > 4")
        .execute(&pool)
        .await
        .unwrap();
    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 4, "precondition: schema_version rewound to 4");

    // Without the fix, V5 fails with 'duplicate column name: auth_token'.
    run_migrations(&pool).await.unwrap();

    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, MAX_SCHEMA_VERSION);
}

#[tokio::test]
async fn migration_v18_creates_audit_log_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    let cols: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('audit_log')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for expected in [
        "id",
        "ts",
        "severity",
        "source",
        "event_id",
        "instance_id",
        "endpoint",
        "action",
        "detail",
    ] {
        assert!(
            cols.iter().any(|c| c == expected),
            "audit_log missing column {expected}; have {cols:?}"
        );
    }

    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='audit_log'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in ["idx_audit_ts", "idx_audit_event", "idx_audit_sev"] {
        assert!(
            indexes.iter().any(|i| i == expected),
            "audit_log missing index {expected}; have {indexes:?}"
        );
    }
}

#[tokio::test]
async fn migration_v19_creates_metrics_and_cursor_column() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('delivery_endpoint_metrics')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in [
        "id",
        "ts_ms",
        "instance_id",
        "event_id",
        "alias",
        "alive",
        "current_chunk_id",
        "chunks_processed",
        "chunk_delay_secs",
        "bytes_processed_total",
        "ffmpeg_restart_count",
        "delivery_mode",
    ] {
        assert!(
            cols.iter().any(|c| c == expected),
            "delivery_endpoint_metrics missing column {expected}; have {cols:?}"
        );
    }

    let inst_cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('delivery_instances')")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        inst_cols.iter().any(|c| c == "last_audit_cursor"),
        "delivery_instances missing last_audit_cursor; have {inst_cols:?}"
    );
}

#[tokio::test]
async fn run_migrations_reaches_v19() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let v: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version),0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 19);
}
