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
async fn run_migrations_reaches_max_schema_version() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let v: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version),0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, crate::db::MAX_SCHEMA_VERSION as i64);
}

#[tokio::test]
async fn migration_v20_adds_drift_telemetry_schema() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    // chunk_records.wall_clock_written_at_ms added
    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('chunk_records')")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        cols.iter().any(|c| c == "wall_clock_written_at_ms"),
        "chunk_records must have wall_clock_written_at_ms column after V20"
    );

    // clock_skew_samples table exists with expected columns
    let skew_cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('clock_skew_samples')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in &[
        "id",
        "event_id",
        "measured_at_ms",
        "local_before_ms",
        "vps_reported_ms",
        "local_after_ms",
        "skew_ms",
        "rtt_ms",
    ] {
        assert!(
            skew_cols.iter().any(|c| c == expected),
            "clock_skew_samples missing column {expected}; got {skew_cols:?}"
        );
    }

    // ffmpeg_progress_samples table exists with expected columns
    let prog_cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('ffmpeg_progress_samples')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in &[
        "id",
        "event_id",
        "endpoint_alias",
        "measured_at_ms",
        "ffmpeg_media_time_ms",
        "wall_clock_ms",
    ] {
        assert!(
            prog_cols.iter().any(|c| c == expected),
            "ffmpeg_progress_samples missing column {expected}; got {prog_cols:?}"
        );
    }

    // Schema version matches MAX
    let v: i32 = sqlx::query_scalar("SELECT MAX(version) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, crate::db::MAX_SCHEMA_VERSION);
}

#[tokio::test]
async fn migrate_v24_adds_host_emit_ts_and_s3_upload_complete_ts() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let host_emit: Option<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('chunk_records') WHERE name = ?1")
            .bind("host_emit_ts")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(host_emit.as_deref(), Some("host_emit_ts"));

    let s3_complete: Option<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('chunk_records') WHERE name = ?1")
            .bind("s3_upload_complete_ts")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(s3_complete.as_deref(), Some("s3_upload_complete_ts"));
}

#[tokio::test]
async fn migrate_v24_is_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    // Re-run: must be a no-op (no column-already-exists error).
    crate::db::run_migrations(&pool).await.unwrap();
    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 24);
}

#[tokio::test]
async fn max_schema_version_is_24() {
    assert_eq!(crate::db::MAX_SCHEMA_VERSION, 24);
}
