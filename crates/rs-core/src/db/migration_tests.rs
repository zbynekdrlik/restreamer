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
async fn migrate_latest_is_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    // Re-run: must be a no-op (no column-already-exists error).
    crate::db::run_migrations(&pool).await.unwrap();
    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, crate::db::MAX_SCHEMA_VERSION);
}

#[tokio::test]
async fn max_schema_version_constant() {
    // Update this when bumping MAX_SCHEMA_VERSION; protects against silent
    // changes that skip the migration-versioning convention.
    assert_eq!(crate::db::MAX_SCHEMA_VERSION, 28);
}

#[tokio::test]
async fn migrate_v27_adds_connected_at_column() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('youtube_oauth')")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        cols.iter().any(|c| c == "connected_at"),
        "connected_at column missing; got {:?}",
        cols
    );
}

#[tokio::test]
async fn migrate_v27_creates_oauth_device_grants_table() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='oauth_device_grants'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1, "oauth_device_grants table missing");
    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('oauth_device_grants')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in [
        "label",
        "device_code",
        "user_code",
        "verification_url",
        "interval_secs",
        "expires_at",
        "status",
        "error",
        "started_at",
    ] {
        assert!(
            cols.iter().any(|c| c == expected),
            "missing column {expected}; got {:?}",
            cols
        );
    }
}

#[tokio::test]
async fn migrate_v27_is_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    // Re-run; must not error and must not duplicate the table.
    crate::db::run_migrations(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='oauth_device_grants'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

// `max_schema_version_is_28` removed — duplicate of `max_schema_version_constant`
// above which asserts the same thing via the same import path. Update the
// constant in one place when adding a new migration.

#[tokio::test]
async fn migrate_v28_flips_ffmpeg_pushers_to_rust() {
    // Exercises the migration DISPATCHER (not the inlined SQL): runs all
    // migrations, rewinds `schema_version` to 27, inserts a legacy ffmpeg
    // row, then re-invokes `run_migrations` so the dispatcher fires
    // `migrate_v28` against the populated table. Any future typo /
    // wrong-WHERE / flipped-operands inside `migrate_v28` itself fails the
    // assertion.
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast, pusher) \
         VALUES ('legacy-ffmpeg', 'YT_RTMP', 'sk-test', 0, 'ffmpeg')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Rewind schema_version so the dispatcher's `(current+1)..=MAX` loop
    // re-applies v28 against the now-populated table.
    sqlx::query("DELETE FROM schema_version WHERE version > 27")
        .execute(&pool)
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let pusher: String =
        sqlx::query_scalar("SELECT pusher FROM endpoint_configs WHERE alias = 'legacy-ffmpeg'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        pusher, "rust",
        "migrate_v28 dispatcher must flip any 'ffmpeg' pusher to 'rust'"
    );
    // Idempotency: re-running once more (now with no ffmpeg rows) must
    // not error and must leave the rust row untouched.
    sqlx::query("DELETE FROM schema_version WHERE version > 27")
        .execute(&pool)
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let pusher: String =
        sqlx::query_scalar("SELECT pusher FROM endpoint_configs WHERE alias = 'legacy-ffmpeg'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        pusher, "rust",
        "v28 must be idempotent on already-flipped rows"
    );
}

#[tokio::test]
async fn create_endpoint_config_uses_rust_pusher() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let id = crate::db::create_endpoint_config(&pool, "test-ep", "YT_RTMP", "key", false)
        .await
        .unwrap();
    let pusher: String = sqlx::query_scalar("SELECT pusher FROM endpoint_configs WHERE id = ?1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        pusher, "rust",
        "create_endpoint_config must default new endpoints to 'rust' (regression for #196)"
    );
}

#[tokio::test]
async fn migration_v25_adds_label_unique_with_default_backfill() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    // 1. `label` and `channel_id` columns exist.
    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('youtube_oauth')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in ["label", "channel_id"] {
        assert!(
            cols.iter().any(|c| c == expected),
            "youtube_oauth missing column {expected}; have {cols:?}"
        );
    }

    // 2. UNIQUE INDEX on label exists.
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='youtube_oauth'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        indexes.iter().any(|i| i == "idx_youtube_oauth_label"),
        "missing idx_youtube_oauth_label; have {indexes:?}"
    );

    // 3. UNIQUE actually enforces — two rows with same label must fail.
    sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a','r','u','c','s','sc')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let dup = sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a2','r2','u','c','s','sc')",
    )
    .execute(&pool)
    .await;
    assert!(dup.is_err(), "duplicate label should be rejected");

    // 4. Default row: fresh DB must have a seeded `default` row at id=1
    //    so multi-label list_oauths always sees it.
    let pool2 = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool2).await.unwrap();
    let label: Option<String> = sqlx::query_scalar("SELECT label FROM youtube_oauth WHERE id = 1")
        .fetch_optional(&pool2)
        .await
        .unwrap();
    assert_eq!(
        label.as_deref(),
        Some("default"),
        "fresh DB must have a seeded 'default' row at id=1"
    );
}

#[tokio::test]
async fn migration_v26_adds_youtube_oauth_id_to_endpoint_configs() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    // 1. Column exists.
    let cols: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT name, type, \"notnull\" FROM pragma_table_info('endpoint_configs')")
            .fetch_all(&pool)
            .await
            .unwrap();
    let oauth_col = cols
        .iter()
        .find(|(n, _, _)| n == "youtube_oauth_id")
        .expect("endpoint_configs must have youtube_oauth_id column");
    assert!(
        oauth_col.1.to_uppercase().contains("INTEGER"),
        "youtube_oauth_id must be INTEGER; got type={}",
        oauth_col.1
    );
    assert_eq!(oauth_col.2, 0, "youtube_oauth_id must be nullable");

    // 2. New endpoints default to NULL.
    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('e1','YT_RTMP','k')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let oauth_id: Option<i64> =
        sqlx::query_scalar("SELECT youtube_oauth_id FROM endpoint_configs WHERE alias = 'e1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(oauth_id.is_none(), "new row must default to NULL");

    // 3. Linkage works: insert oauth row, link endpoint, read back.
    sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a','r','u','c','s','sc')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let bb_id: i64 = sqlx::query_scalar("SELECT id FROM youtube_oauth WHERE label = 'bb'")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE endpoint_configs SET youtube_oauth_id = ?1 WHERE alias = 'e1'")
        .bind(bb_id)
        .execute(&pool)
        .await
        .unwrap();
    let read_back: i64 =
        sqlx::query_scalar("SELECT youtube_oauth_id FROM endpoint_configs WHERE alias = 'e1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(read_back, bb_id);
}

#[tokio::test]
async fn v29_flips_fb_endpoints_from_ffmpeg_to_rust() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    // Insert one FB row on ffmpeg, one YT_RTMP row on ffmpeg, one VIMEO row on ffmpeg.
    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast, pusher) \
         VALUES ('fb-test', 'FB', 'fb-key', 0, 'ffmpeg'), \
                ('yt-test', 'YT_RTMP', 'yt-key', 0, 'ffmpeg'), \
                ('vimeo-test', 'VIMEO', 'v-key', 0, 'ffmpeg')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Rewind schema_version so v29 re-runs through the dispatcher.
    sqlx::query("DELETE FROM schema_version WHERE version > 28")
        .execute(&pool)
        .await
        .unwrap();

    // Re-run migrations (this is the contract under test: dispatcher reaches v29).
    crate::db::run_migrations(&pool).await.unwrap();

    let fb: String = sqlx::query_scalar(
        "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let yt: String = sqlx::query_scalar(
        "SELECT pusher FROM endpoint_configs WHERE alias = 'yt-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let vimeo: String = sqlx::query_scalar(
        "SELECT pusher FROM endpoint_configs WHERE alias = 'vimeo-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(fb, "rust", "v29 must flip FB ffmpeg->rust");
    assert_eq!(yt, "ffmpeg", "v29 must NOT touch non-FB rows");
    assert_eq!(vimeo, "ffmpeg", "v29 must NOT touch non-FB rows");
}

#[tokio::test]
async fn v29_is_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast, pusher) \
         VALUES ('fb-idem', 'FB', 'fb-key', 0, 'ffmpeg')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // First v29 run.
    sqlx::query("DELETE FROM schema_version WHERE version > 28")
        .execute(&pool)
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    // Second v29 run (idempotency check).
    sqlx::query("DELETE FROM schema_version WHERE version > 28")
        .execute(&pool)
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    let fb: String = sqlx::query_scalar(
        "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-idem'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fb, "rust", "v29 idempotent: still rust after re-run");
}

#[tokio::test]
async fn v29_does_not_touch_fb_rows_already_on_rust() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast, pusher) \
         VALUES ('fb-already-rust', 'FB', 'fb-key', 0, 'rust')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("DELETE FROM schema_version WHERE version > 28")
        .execute(&pool)
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    let fb: String = sqlx::query_scalar(
        "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-already-rust'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fb, "rust", "v29 only matches WHERE pusher='ffmpeg'");
}
