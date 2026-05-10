//! Database migration runner.
//!
//! All schema migrations are idempotent — ALTER TABLE ADD COLUMN and
//! RENAME COLUMN go through helpers that check `pragma_table_info` first.
//! This allows a rerun to recover from an interrupted previous migration
//! (e.g. the DB was fully advanced but schema_version was rolled back).
//! See issue #112.

use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use crate::error::Result;

/// Maximum schema version. Must equal the highest version in the migration list.
/// Tests assert that `run_migrations` reaches this exact value.
pub const MAX_SCHEMA_VERSION: i32 = 24;

/// Returns true if the column exists on the table, false otherwise.
///
/// Uses `pragma_table_info` as a table-valued function so the table name
/// can be interpolated safely (sqlx cannot bind PRAGMA arguments).
/// Table names must come from trusted code constants — never user input.
async fn column_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
) -> sqlx::Result<bool> {
    let query = format!("SELECT name FROM pragma_table_info('{table}') WHERE name = ?1");
    let row: Option<String> = sqlx::query_scalar(&query)
        .bind(column)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.is_some())
}

/// Idempotent `ALTER TABLE ... ADD COLUMN`. No-ops if the column already
/// exists. `col_def` is the full column definition including the column
/// name and type (e.g. `"auth_token TEXT NOT NULL DEFAULT ''"`).
async fn add_column_if_missing(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
    col_def: &str,
) -> sqlx::Result<()> {
    if column_exists(tx, table, column).await? {
        return Ok(());
    }
    sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {col_def}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Idempotent `ALTER TABLE ... RENAME COLUMN`. No-ops if `new_name`
/// already exists on the table.
async fn rename_column_if_old_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> sqlx::Result<()> {
    if column_exists(tx, table, new_name).await? {
        return Ok(());
    }
    sqlx::query(&format!(
        "ALTER TABLE {table} RENAME COLUMN {old_name} TO {new_name}"
    ))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Execute a multi-statement SQL script inside a transaction.
/// Splits on `;` and skips empty statements.
/// Used for pure-DDL migrations that only contain CREATE TABLE / CREATE INDEX
/// (already idempotent via IF NOT EXISTS).
async fn execute_sql_statements(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sql: &str,
) -> sqlx::Result<()> {
    for statement in sql.split(';') {
        let trimmed = statement.trim();
        if !trimmed.is_empty() {
            sqlx::query(trimmed).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

async fn migrate_v5(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "delivery_instances",
        "auth_token",
        "auth_token TEXT NOT NULL DEFAULT ''",
    )
    .await
}

async fn migrate_v6(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(tx, "chunk_records", "sent_at", "sent_at TEXT").await?;
    add_column_if_missing(
        tx,
        "delivery_endpoint_status",
        "bytes_processed_total",
        "bytes_processed_total INTEGER NOT NULL DEFAULT 0",
    )
    .await
}

async fn migrate_v7(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    rename_column_if_old_exists(
        tx,
        "delivery_endpoint_status",
        "buff_size_bytes",
        "chunks_processed",
    )
    .await
}

async fn migrate_v8(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "sequence_number",
        "sequence_number INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    // Backfill is safe to re-run: deterministic sequence from IDs.
    sqlx::query(
        "UPDATE chunk_records SET sequence_number = (
            SELECT COUNT(*) FROM chunk_records c2
            WHERE c2.streaming_event_id = chunk_records.streaming_event_id
            AND c2.id <= chunk_records.id
        )",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_chunks_event_sequence ON chunk_records(streaming_event_id, sequence_number)",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn migrate_v9(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "streaming_events",
        "cache_delay_secs",
        "cache_delay_secs INTEGER",
    )
    .await
}

async fn migrate_v10(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "chunk_format",
        "chunk_format TEXT NOT NULL DEFAULT 'ts'",
    )
    .await
}

async fn migrate_v11(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "duration_ms",
        "duration_ms INTEGER NOT NULL DEFAULT 0",
    )
    .await
}

async fn migrate_v12(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS event_templates (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL UNIQUE,
            cache_delay_secs INTEGER
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS template_endpoints (
            template_id INTEGER NOT NULL REFERENCES event_templates(id) ON DELETE CASCADE,
            endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
            PRIMARY KEY (template_id, endpoint_id)
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    add_column_if_missing(tx, "streaming_events", "created_from", "created_from TEXT").await
}

async fn migrate_v14(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "streaming_events",
        "rescue_video_url",
        "rescue_video_url TEXT",
    )
    .await
}

async fn migrate_v15(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "event_templates",
        "rescue_video_url",
        "rescue_video_url TEXT",
    )
    .await
}

async fn migrate_v17(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_attempts",
        "upload_attempts INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_first_attempt_at",
        "upload_first_attempt_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_completed_at",
        "upload_completed_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_duration_ms",
        "upload_duration_ms INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_last_error",
        "upload_last_error TEXT",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_next_retry_at",
        "upload_next_retry_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_failed_permanently",
        "upload_failed_permanently INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_chunks_upload_queue
          ON chunk_records(upload_failed_permanently, sent, in_process, upload_next_retry_at, id)
          WHERE sent = 0 AND in_process = 0 AND upload_failed_permanently = 0
        "#,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Return the highest schema version recorded in `schema_version`, or 0
/// if the table does not yet exist or is empty. Used by the runtime to
/// emit a `MigrationsApplied` audit row with `from_version` + `to_version`.
pub async fn current_schema_version(pool: &SqlitePool) -> Result<i32> {
    // Create table if missing so the query below succeeds on a fresh DB.
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        .execute(pool)
        .await?;
    let v: i32 = sqlx::query("SELECT COALESCE(MAX(version), 0) as v FROM schema_version")
        .fetch_one(pool)
        .await
        .map(|r| r.get("v"))?;
    Ok(v)
}

/// Run database migrations.
///
/// Each migration is wrapped in its own transaction so a failure rolls
/// back that one migration and halts startup with an error. ALTER TABLE
/// ADD COLUMN / RENAME COLUMN statements go through idempotent helpers
/// so partial prior state does not break resumption.
pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        .execute(pool)
        .await?;

    let current: i32 = sqlx::query("SELECT COALESCE(MAX(version), 0) as v FROM schema_version")
        .fetch_one(pool)
        .await
        .map(|r| r.get("v"))?;

    for version in (current + 1)..=MAX_SCHEMA_VERSION {
        let mut tx = pool.begin().await?;
        match version {
            1 => execute_sql_statements(&mut tx, MIGRATION_V1_SQL).await?,
            2 => execute_sql_statements(&mut tx, MIGRATION_V2_SQL).await?,
            3 => execute_sql_statements(&mut tx, MIGRATION_V3_SQL).await?,
            4 => execute_sql_statements(&mut tx, MIGRATION_V4_SQL).await?,
            5 => migrate_v5(&mut tx).await?,
            6 => migrate_v6(&mut tx).await?,
            7 => migrate_v7(&mut tx).await?,
            8 => migrate_v8(&mut tx).await?,
            9 => migrate_v9(&mut tx).await?,
            10 => migrate_v10(&mut tx).await?,
            11 => migrate_v11(&mut tx).await?,
            12 => migrate_v12(&mut tx).await?,
            13 => execute_sql_statements(&mut tx, MIGRATION_V13_SQL).await?,
            14 => migrate_v14(&mut tx).await?,
            15 => migrate_v15(&mut tx).await?,
            16 => execute_sql_statements(&mut tx, MIGRATION_V16_SQL).await?,
            17 => migrate_v17(&mut tx).await?,
            18 => execute_sql_statements(&mut tx, MIGRATION_V18_SQL).await?,
            19 => migrate_v19(&mut tx).await?,
            20 => migrate_v20(&mut tx).await?,
            21 => migrate_v21(&mut tx).await?,
            22 => migrate_v22(&mut tx).await?,
            23 => execute_sql_statements(&mut tx, MIGRATION_V23_SQL).await?,
            24 => migrate_v24(&mut tx).await?,
            _ => unreachable!("unhandled migration version {version}"),
        }
        sqlx::query("INSERT OR REPLACE INTO schema_version (version) VALUES (?1)")
            .bind(version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }

    // Startup cleanup: delete old sent chunk records to keep the DB fast.
    // Without this, CI runs accumulate 100K+ rows making startup take >30s.
    let deleted: i64 = sqlx::query(
        "DELETE FROM chunk_records WHERE sent = 1 AND created_at < datetime('now', '-1 hour')",
    )
    .execute(pool)
    .await
    .map(|r| r.rows_affected() as i64)
    .unwrap_or(0);
    if deleted > 0 {
        tracing::info!("Cleaned {deleted} old chunk records from database");
    }

    Ok(())
}

const MIGRATION_V1_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS client_profile (
    id        INTEGER PRIMARY KEY,
    user_uuid TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS streaming_events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    identifier           TEXT UNIQUE,
    short_description    TEXT,
    date_of_event        TEXT NOT NULL DEFAULT (datetime('now')),
    server_ip            TEXT DEFAULT '',
    received_bytes       INTEGER NOT NULL DEFAULT 0,
    receiving_activated  INTEGER NOT NULL DEFAULT 0,
    delivering_activated INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chunk_records (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    streaming_event_id INTEGER NOT NULL REFERENCES streaming_events(id) ON DELETE CASCADE,
    chunk_file_path    TEXT NOT NULL,
    data_size          INTEGER NOT NULL,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    md5                TEXT NOT NULL DEFAULT '',
    in_process         INTEGER NOT NULL DEFAULT 0,
    sent               INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_chunks_unsent ON chunk_records(streaming_event_id, sent, in_process)
    WHERE sent = 0 AND in_process = 0
"#;

const MIGRATION_V2_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS endpoint_configs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    alias          TEXT NOT NULL UNIQUE,
    service_type   TEXT NOT NULL CHECK(service_type IN ('YT_HLS','FB','YT_RTMP','VIMEO','INSTAGRAM','TEST_FILE')),
    stream_key     TEXT NOT NULL DEFAULT '',
    enabled        INTEGER NOT NULL DEFAULT 1,
    position_last  INTEGER NOT NULL DEFAULT 0,
    delivered_bytes INTEGER NOT NULL DEFAULT 0,
    is_fast        INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS event_endpoints (
    event_id    INTEGER NOT NULL REFERENCES streaming_events(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
    PRIMARY KEY (event_id, endpoint_id)
);

CREATE TABLE IF NOT EXISTS delivery_instances (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    hetzner_id     INTEGER NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    ipv4           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'creating' CHECK(status IN ('creating','running','stopping','deleted')),
    server_type    TEXT NOT NULL DEFAULT 'cx23',
    event_id       INTEGER REFERENCES streaming_events(id) ON DELETE SET NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_health_at TEXT,
    auth_token     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS delivery_endpoint_status (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     INTEGER NOT NULL REFERENCES delivery_instances(id) ON DELETE CASCADE,
    alias           TEXT NOT NULL,
    alive           INTEGER NOT NULL DEFAULT 0,
    buff_size_bytes INTEGER NOT NULL DEFAULT 0,
    current_chunk_id INTEGER NOT NULL DEFAULT 0,
    last_check_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS youtube_oauth (
    id             INTEGER PRIMARY KEY DEFAULT 1,
    access_token   TEXT NOT NULL DEFAULT '',
    refresh_token  TEXT NOT NULL DEFAULT '',
    token_uri      TEXT NOT NULL DEFAULT 'https://oauth2.googleapis.com/token',
    client_id      TEXT NOT NULL DEFAULT '',
    client_secret  TEXT NOT NULL DEFAULT '',
    scopes         TEXT NOT NULL DEFAULT '',
    expires_at     TEXT
);

"#;

const MIGRATION_V3_SQL: &str = r#"
DROP TABLE IF EXISTS scheduled_streams;

CREATE TABLE IF NOT EXISTS streaming_events_new (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL UNIQUE,
    received_bytes       INTEGER NOT NULL DEFAULT 0,
    receiving_activated  INTEGER NOT NULL DEFAULT 0,
    delivering_activated INTEGER NOT NULL DEFAULT 0
);

INSERT INTO streaming_events_new (id, name, received_bytes, receiving_activated, delivering_activated)
    SELECT id, COALESCE(identifier, 'Event-' || id), received_bytes, receiving_activated, delivering_activated
    FROM streaming_events;

DROP TABLE streaming_events;

ALTER TABLE streaming_events_new RENAME TO streaming_events
"#;

const MIGRATION_V4_SQL: &str = r#"
CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_endpoint_status_instance_alias
    ON delivery_endpoint_status(instance_id, alias);

PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS delivery_instances_new (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    hetzner_id     INTEGER NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    ipv4           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'creating' CHECK(status IN ('creating','running','stopping','deleted','failed')),
    server_type    TEXT NOT NULL DEFAULT 'cx23',
    event_id       INTEGER REFERENCES streaming_events(id) ON DELETE SET NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_health_at TEXT
);

INSERT INTO delivery_instances_new (id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at)
    SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
    FROM delivery_instances;

DROP TABLE delivery_instances;

ALTER TABLE delivery_instances_new RENAME TO delivery_instances;

PRAGMA foreign_keys = ON
"#;

const MIGRATION_V13_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL,
    event_id    INTEGER,
    captured_at TEXT NOT NULL DEFAULT (datetime('now')),
    log_text    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_restart_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   INTEGER NOT NULL,
    event_id      INTEGER,
    alias         TEXT NOT NULL,
    timestamp_ms  INTEGER NOT NULL,
    chunk_id      INTEGER NOT NULL,
    lifetime_secs INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    stderr_tail   TEXT,
    backoff_secs  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delivery_restart_log_instance
    ON delivery_restart_log(instance_id);

CREATE INDEX IF NOT EXISTS idx_delivery_logs_instance
    ON delivery_logs(instance_id)
"#;

// V16: widen the delivery_instances.status CHECK constraint to allow the
// new orchestrator phases (booting, initializing, delivering). Without
// this, writes from poll_and_init to "booting" fail with
//   CHECK constraint failed: status IN ('creating','running',...)
// which then sets the instance to "failed" and leaves the operator
// staring at a broken dashboard.
//
// SQLite doesn't support ALTER TABLE ... DROP CONSTRAINT so we have to
// recreate the table, copying rows over.
const MIGRATION_V16_SQL: &str = r#"
CREATE TABLE delivery_instances_v16 (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hetzner_id      INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    ipv4            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'creating' CHECK(status IN (
        'creating', 'running', 'stopping', 'deleted', 'failed',
        'booting', 'initializing', 'delivering'
    )),
    server_type     TEXT NOT NULL,
    event_id        INTEGER REFERENCES streaming_events(id) ON DELETE SET NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_health_at  TEXT,
    auth_token      TEXT NOT NULL DEFAULT ''
);

INSERT INTO delivery_instances_v16
    (id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at, auth_token)
SELECT
    id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at, auth_token
FROM delivery_instances;

DROP TABLE delivery_instances;
ALTER TABLE delivery_instances_v16 RENAME TO delivery_instances
"#;

const MIGRATION_V18_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    severity    TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    event_id    INTEGER,
    instance_id INTEGER,
    endpoint    TEXT,
    action      TEXT    NOT NULL,
    detail      TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_ts    ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_sev   ON audit_log(severity, ts DESC);
"#;

const MIGRATION_V19_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_endpoint_metrics (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms                 INTEGER NOT NULL,
    instance_id           INTEGER NOT NULL,
    event_id              INTEGER NOT NULL,
    alias                 TEXT    NOT NULL,
    alive                 INTEGER NOT NULL,
    current_chunk_id      INTEGER NOT NULL,
    chunks_processed      INTEGER NOT NULL,
    chunk_delay_secs      REAL    NOT NULL,
    bytes_processed_total INTEGER NOT NULL,
    ffmpeg_restart_count  INTEGER NOT NULL,
    delivery_mode         TEXT
);
CREATE INDEX IF NOT EXISTS idx_dem_event_alias
    ON delivery_endpoint_metrics(event_id, alias, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_dem_ts
    ON delivery_endpoint_metrics(ts_ms DESC);
"#;

async fn migrate_v19(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    execute_sql_statements(tx, MIGRATION_V19_SQL).await?;
    add_column_if_missing(
        tx,
        "delivery_instances",
        "last_audit_cursor",
        "last_audit_cursor INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    Ok(())
}

async fn migrate_v20(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // Producer wall-clock per chunk
    add_column_if_missing(
        tx,
        "chunk_records",
        "wall_clock_written_at_ms",
        "wall_clock_written_at_ms INTEGER",
    )
    .await?;

    // Clock-skew samples (stream.lan <-> VPS)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS clock_skew_samples (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        INTEGER NOT NULL,
            measured_at_ms  INTEGER NOT NULL,
            local_before_ms INTEGER NOT NULL,
            vps_reported_ms INTEGER NOT NULL,
            local_after_ms  INTEGER NOT NULL,
            skew_ms         INTEGER NOT NULL,
            rtt_ms          INTEGER NOT NULL
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_clock_skew_event_time
         ON clock_skew_samples(event_id, measured_at_ms)",
    )
    .execute(&mut **tx)
    .await?;

    // ffmpeg consumer-rate samples (one per stderr `time=` line, sampled)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ffmpeg_progress_samples (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id             INTEGER NOT NULL,
            endpoint_alias       TEXT    NOT NULL,
            measured_at_ms       INTEGER NOT NULL,
            ffmpeg_media_time_ms INTEGER NOT NULL,
            wall_clock_ms        INTEGER NOT NULL
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_ffmpeg_progress_event_time
         ON ffmpeg_progress_samples(event_id, measured_at_ms)",
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn migrate_v21(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // YT_HLS endpoint type removed (#135): the HLS muxer interacted badly
    // with the producer-side timestamp corrections needed to fix long-running
    // cache drift. The operator confirmed HLS was not used in production.
    //
    // V2's CHECK constraint intentionally still lists YT_HLS. SQLite does not
    // support altering CHECK constraints in place, and the workarounds
    // (table rebuild, writable_schema hack) trade complexity for no behavioural
    // gain — the application no longer constructs YT_HLS endpoints, so the
    // constraint widening is dead code in practice. Leaving V2 unchanged keeps
    // production schemas (where V2 already ran) byte-identical to fresh dev
    // schemas.
    sqlx::query("DELETE FROM endpoint_configs WHERE service_type = 'YT_HLS'")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn migrate_v22(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // Add pusher column for PusherKind (#103). Default 'ffmpeg' preserves
    // existing endpoint behaviour for all rows created before this migration.
    add_column_if_missing(
        tx,
        "endpoint_configs",
        "pusher",
        "pusher TEXT NOT NULL DEFAULT 'ffmpeg'",
    )
    .await
}

// V23: covering index on delivery_instances(event_id, id DESC) so
// `get_delivery_instance_by_event` (which now ORDER BYs id DESC LIMIT 1
// after the #165 stale-row fix) avoids a partition full scan as the
// instance row count grows over long-lived deployments. Partial index on
// `status != 'deleted'` matches the WHERE clause exactly.
const MIGRATION_V23_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_delivery_instances_event_id_active
    ON delivery_instances(event_id, id DESC)
    WHERE status != 'deleted';
"#;

async fn migrate_v24(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // Per-chunk lifecycle stages A and B (host clock, millis since epoch).
    // NULL on any chunk uploaded by a pre-v24 host or whose uploader did
    // not complete the second timestamp. Cross-host gap math handles
    // NULL by returning Duration::ZERO.
    add_column_if_missing(
        tx,
        "chunk_records",
        "host_emit_ts",
        "host_emit_ts INTEGER NULL",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "s3_upload_complete_ts",
        "s3_upload_complete_ts INTEGER NULL",
    )
    .await
}
