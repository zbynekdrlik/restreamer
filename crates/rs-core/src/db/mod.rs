use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

use crate::error::Result;
use crate::models::{ChunkRecord, ChunkStats, ClientProfile, StreamingEvent};

mod v2;
pub use v2::*;

#[cfg(test)]
mod tests;

/// Create a SQLite connection pool.
pub async fn create_pool(db_path: &Path) -> Result<SqlitePool> {
    let url = format!("sqlite:{}?mode=rwc", db_path.display());
    let options = SqliteConnectOptions::from_str(&url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .create_if_missing(true)
        .pragma("foreign_keys", "1");

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Create an in-memory SQLite pool for testing.
pub async fn create_memory_pool() -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")?.pragma("foreign_keys", "1");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok(pool)
}

/// Run database migrations.
///
/// Uses a version tracking table to support incremental schema changes.
/// Wrapped in a transaction so partial failures don't leave the DB inconsistent.
pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        .execute(pool)
        .await?;

    let current: i32 = sqlx::query("SELECT COALESCE(MAX(version), 0) as v FROM schema_version")
        .fetch_one(pool)
        .await
        .map(|r| r.get("v"))?;

    let migrations: &[(i32, &str)] = &[
        (1, MIGRATION_V1_SQL),
        (2, MIGRATION_V2_SQL),
        (3, MIGRATION_V3_SQL),
        (4, MIGRATION_V4_SQL),
        (5, MIGRATION_V5_SQL),
        (6, MIGRATION_V6_SQL),
        (7, MIGRATION_V7_SQL),
        (8, MIGRATION_V8_SQL),
        (9, MIGRATION_V9_SQL),
        (10, MIGRATION_V10_SQL),
        (11, MIGRATION_V11_SQL),
        (12, MIGRATION_V12_SQL),
    ];

    for &(version, sql) in migrations {
        if current < version {
            let mut tx = pool.begin().await?;
            for statement in sql.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() {
                    sqlx::query(trimmed).execute(&mut *tx).await?;
                }
            }
            sqlx::query("INSERT OR REPLACE INTO schema_version (version) VALUES (?1)")
                .bind(version)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
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

const MIGRATION_V5_SQL: &str = r#"
ALTER TABLE delivery_instances ADD COLUMN auth_token TEXT NOT NULL DEFAULT ''
"#;

const MIGRATION_V6_SQL: &str = r#"
ALTER TABLE chunk_records ADD COLUMN sent_at TEXT;
ALTER TABLE delivery_endpoint_status ADD COLUMN bytes_processed_total INTEGER NOT NULL DEFAULT 0
"#;

const MIGRATION_V7_SQL: &str = r#"
ALTER TABLE delivery_endpoint_status RENAME COLUMN buff_size_bytes TO chunks_processed
"#;

const MIGRATION_V8_SQL: &str = r#"
ALTER TABLE chunk_records ADD COLUMN sequence_number INTEGER NOT NULL DEFAULT 0;

UPDATE chunk_records SET sequence_number = (
    SELECT COUNT(*) FROM chunk_records c2
    WHERE c2.streaming_event_id = chunk_records.streaming_event_id
    AND c2.id <= chunk_records.id
);

CREATE INDEX idx_chunks_event_sequence ON chunk_records(streaming_event_id, sequence_number)
"#;

const MIGRATION_V9_SQL: &str = r#"
ALTER TABLE streaming_events ADD COLUMN cache_delay_secs INTEGER
"#;

const MIGRATION_V10_SQL: &str = r#"
ALTER TABLE chunk_records ADD COLUMN chunk_format TEXT NOT NULL DEFAULT 'ts'
"#;

const MIGRATION_V11_SQL: &str = r#"
ALTER TABLE chunk_records ADD COLUMN duration_ms INTEGER NOT NULL DEFAULT 0
"#;

const MIGRATION_V12_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS event_templates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    cache_delay_secs INTEGER
);

CREATE TABLE IF NOT EXISTS template_endpoints (
    template_id INTEGER NOT NULL REFERENCES event_templates(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
    PRIMARY KEY (template_id, endpoint_id)
);

ALTER TABLE streaming_events ADD COLUMN created_from TEXT
"#;

// --- Client Profile ---

pub async fn get_client_profile(pool: &SqlitePool) -> Result<Option<ClientProfile>> {
    let row = sqlx::query("SELECT id, user_uuid FROM client_profile LIMIT 1")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| ClientProfile {
        id: r.get("id"),
        user_uuid: r.get("user_uuid"),
    }))
}

pub async fn upsert_client_profile(pool: &SqlitePool, user_uuid: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO client_profile (id, user_uuid) VALUES (1, ?1)
         ON CONFLICT(id) DO UPDATE SET user_uuid = ?1",
    )
    .bind(user_uuid)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Streaming Events ---

pub async fn get_streaming_event(pool: &SqlitePool) -> Result<Option<StreamingEvent>> {
    // Prefer the event with receiving_activated=1, fall back to highest ID
    let row = sqlx::query(
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from
         FROM streaming_events ORDER BY receiving_activated DESC, id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| StreamingEvent {
        id: r.get("id"),
        name: r.get("name"),
        received_bytes: r.get("received_bytes"),
        receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
        delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
        cache_delay_secs: r.get("cache_delay_secs"),
        created_from: r.get("created_from"),
    }))
}

pub async fn get_streaming_event_by_id(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<StreamingEvent>> {
    let row = sqlx::query(
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from
         FROM streaming_events WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| StreamingEvent {
        id: r.get("id"),
        name: r.get("name"),
        received_bytes: r.get("received_bytes"),
        receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
        delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
        cache_delay_secs: r.get("cache_delay_secs"),
        created_from: r.get("created_from"),
    }))
}

pub async fn upsert_streaming_event(pool: &SqlitePool, name: &str) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO streaming_events (name, receiving_activated, delivering_activated)
         VALUES (?1, 1, 1)
         ON CONFLICT(name) DO UPDATE SET
             receiving_activated = 1,
             delivering_activated = 1
         RETURNING id",
    )
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn update_streaming_event_flags(
    pool: &SqlitePool,
    id: i64,
    receiving: bool,
    delivering: bool,
) -> Result<()> {
    let recv = receiving as i32;
    let deliv = delivering as i32;
    sqlx::query(
        "UPDATE streaming_events SET receiving_activated = ?1, delivering_activated = ?2 WHERE id = ?3",
    )
    .bind(recv)
    .bind(deliv)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_receiving_activated(pool: &SqlitePool, id: i64, receiving: bool) -> Result<()> {
    let val = receiving as i32;
    sqlx::query("UPDATE streaming_events SET receiving_activated = ?1 WHERE id = ?2")
        .bind(val)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_delivering_activated(pool: &SqlitePool, id: i64, delivering: bool) -> Result<()> {
    let val = delivering as i32;
    sqlx::query("UPDATE streaming_events SET delivering_activated = ?1 WHERE id = ?2")
        .bind(val)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Deactivate both receiving and delivering in a single atomic update.
pub async fn deactivate_event(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE streaming_events SET receiving_activated = 0, delivering_activated = 0 WHERE id = ?1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_received_bytes(
    pool: &SqlitePool,
    event_id: i64,
    additional_bytes: i64,
) -> Result<()> {
    sqlx::query("UPDATE streaming_events SET received_bytes = received_bytes + ?1 WHERE id = ?2")
        .bind(additional_bytes)
        .bind(event_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_streaming_event(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM streaming_events WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delete all streaming events except the one with the given ID.
///
/// Returns the number of deleted rows. This prevents stale events from
/// interfering with `get_streaming_event()`.
pub async fn delete_other_streaming_events(pool: &SqlitePool, keep_id: i64) -> Result<u64> {
    let result = sqlx::query("DELETE FROM streaming_events WHERE id != ?1")
        .bind(keep_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

// --- Chunk Records ---

pub async fn insert_chunk(
    pool: &SqlitePool,
    streaming_event_id: i64,
    chunk_file_path: &str,
    data_size: i64,
    md5: &str,
    duration_ms: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO chunk_records (streaming_event_id, chunk_file_path, data_size, md5, sequence_number, duration_ms)
         VALUES (?1, ?2, ?3, ?4,
           COALESCE((SELECT MAX(sequence_number) FROM chunk_records WHERE streaming_event_id = ?1), 0) + 1,
           ?5
         ) RETURNING id",
    )
    .bind(streaming_event_id)
    .bind(chunk_file_path)
    .bind(data_size)
    .bind(md5)
    .bind(duration_ms)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn get_unsent_chunks(pool: &SqlitePool, limit: i64) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
         in_process, sent, sequence_number, duration_ms
         FROM chunk_records
         WHERE sent = 0 AND in_process = 0
         ORDER BY id ASC
         LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ChunkRecord {
            id: r.get("id"),
            streaming_event_id: r.get("streaming_event_id"),
            chunk_file_path: r.get("chunk_file_path"),
            data_size: r.get("data_size"),
            created_at: r.get("created_at"),
            md5: r.get("md5"),
            in_process: r.get::<i32, _>("in_process") != 0,
            sent: r.get::<i32, _>("sent") != 0,
            sequence_number: r.get("sequence_number"),
            duration_ms: r.get("duration_ms"),
        })
        .collect())
}

pub async fn set_chunk_in_process(pool: &SqlitePool, id: i64, in_process: bool) -> Result<()> {
    let val = in_process as i32;
    sqlx::query("UPDATE chunk_records SET in_process = ?1 WHERE id = ?2")
        .bind(val)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_chunk_sent(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records SET sent = 1, in_process = 0, sent_at = datetime('now') WHERE id = ?1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_chunks_paginated(
    pool: &SqlitePool,
    offset: i64,
    limit: i64,
) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
         in_process, sent, sequence_number, duration_ms
         FROM chunk_records ORDER BY id DESC LIMIT ?1 OFFSET ?2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ChunkRecord {
            id: r.get("id"),
            streaming_event_id: r.get("streaming_event_id"),
            chunk_file_path: r.get("chunk_file_path"),
            data_size: r.get("data_size"),
            created_at: r.get("created_at"),
            md5: r.get("md5"),
            in_process: r.get::<i32, _>("in_process") != 0,
            sent: r.get::<i32, _>("sent") != 0,
            sequence_number: r.get("sequence_number"),
            duration_ms: r.get("duration_ms"),
        })
        .collect())
}

pub async fn get_chunk_stats(pool: &SqlitePool, chunk_duration_ms: u64) -> Result<ChunkStats> {
    let row = sqlx::query(
        r#"SELECT
            COUNT(*) as total_chunks,
            COALESCE(SUM(CASE WHEN sent = 0 AND in_process = 0 THEN 1 ELSE 0 END), 0) as pending_chunks,
            COALESCE(SUM(CASE WHEN sent = 1 THEN 1 ELSE 0 END), 0) as sent_chunks,
            COALESCE(SUM(CASE WHEN in_process = 1 THEN 1 ELSE 0 END), 0) as in_process_chunks,
            COALESCE(SUM(data_size), 0) as total_bytes
           FROM chunk_records"#,
    )
    .fetch_one(pool)
    .await?;

    let total_chunks: i32 = row.get("total_chunks");
    let pending_chunks: i32 = row.get("pending_chunks");
    let sent_chunks: i32 = row.get("sent_chunks");
    let in_process_chunks: i32 = row.get("in_process_chunks");
    let total_bytes: i64 = row.get("total_bytes");

    Ok(ChunkStats {
        total_chunks: total_chunks as i64,
        pending_chunks: pending_chunks as i64,
        sent_chunks: sent_chunks as i64,
        in_process_chunks: in_process_chunks as i64,
        total_bytes,
        buffer_duration_secs: pending_chunks as f64 * (chunk_duration_ms as f64 / 1000.0),
    })
}

/// Get the first (minimum) chunk ID for a specific streaming event.
/// Returns None if no chunks exist for the event.
pub async fn get_first_chunk_id_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Option<i64>> {
    let row =
        sqlx::query("SELECT MIN(id) as min_id FROM chunk_records WHERE streaming_event_id = ?1")
            .bind(streaming_event_id)
            .fetch_one(pool)
            .await?;
    Ok(row.get::<Option<i64>, _>("min_id"))
}

/// Get the latest (maximum) chunk ID for a specific streaming event.
/// Returns None if no chunks exist for the event.
pub async fn get_latest_chunk_id_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Option<i64>> {
    let row =
        sqlx::query("SELECT MAX(id) as max_id FROM chunk_records WHERE streaming_event_id = ?1")
            .bind(streaming_event_id)
            .fetch_one(pool)
            .await?;
    Ok(row.get::<Option<i64>, _>("max_id"))
}

/// Get the first (minimum) sequence number for a specific streaming event.
/// Returns None if no chunks exist for the event.
pub async fn get_first_sequence_number_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Option<i64>> {
    let row = sqlx::query(
        "SELECT MIN(sequence_number) as min_seq FROM chunk_records WHERE streaming_event_id = ?1",
    )
    .bind(streaming_event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<Option<i64>, _>("min_seq"))
}

/// Get the latest (maximum) sequence number for a specific streaming event.
/// Returns None if no chunks exist for the event.
pub async fn get_latest_sequence_number_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Option<i64>> {
    let row = sqlx::query(
        "SELECT MAX(sequence_number) as max_seq FROM chunk_records WHERE streaming_event_id = ?1",
    )
    .bind(streaming_event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<Option<i64>, _>("max_seq"))
}

/// Get all chunks for a specific streaming event, ordered by sequence number.
pub async fn get_chunks_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
         in_process, sent, sequence_number, duration_ms
         FROM chunk_records WHERE streaming_event_id = ?1
         ORDER BY sequence_number ASC",
    )
    .bind(streaming_event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ChunkRecord {
            id: r.get("id"),
            streaming_event_id: r.get("streaming_event_id"),
            chunk_file_path: r.get("chunk_file_path"),
            data_size: r.get("data_size"),
            created_at: r.get("created_at"),
            md5: r.get("md5"),
            in_process: r.get::<i32, _>("in_process") != 0,
            sent: r.get::<i32, _>("sent") != 0,
            sequence_number: r.get("sequence_number"),
            duration_ms: r.get("duration_ms"),
        })
        .collect())
}

/// Count chunks that have been sent to S3 for a specific streaming event.
pub async fn get_sent_chunk_count_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) as cnt FROM chunk_records WHERE streaming_event_id = ?1 AND sent = 1",
    )
    .bind(streaming_event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i32, _>("cnt") as i64)
}

/// Count chunks on local disk (not yet uploaded to S3) for a specific streaming event.
pub async fn get_pending_chunk_count_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) as cnt FROM chunk_records WHERE streaming_event_id = ?1 AND sent = 0",
    )
    .bind(streaming_event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i32, _>("cnt") as i64)
}

/// Compute the cache duration: total content on S3 that has NOT yet been delivered.
/// Only counts sent chunks with sequence_number above the delivery position.
/// During buffering (delivered_up_to = 0), this equals the total sent duration.
pub async fn get_cache_duration_secs(
    pool: &SqlitePool,
    event_id: i64,
    delivered_up_to: i64,
) -> Result<f64> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) as total_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1 AND sequence_number > ?2",
    )
    .bind(event_id)
    .bind(delivered_up_to)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("total_ms") as f64 / 1000.0)
}

/// Total content duration of chunks uploaded to S3 for an event.
/// Only counts chunks with sent = 1. Used for buffer-fill wait.
pub async fn get_sent_duration_ms(pool: &SqlitePool, event_id: i64) -> Result<i64> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) as total_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("total_ms"))
}

/// Delete all chunks for a specific streaming event.
/// Used to clear stale chunks when restarting a stream so buffer starts at 0%.
pub async fn delete_chunks_for_event(pool: &SqlitePool, streaming_event_id: i64) -> Result<u64> {
    let result = sqlx::query("DELETE FROM chunk_records WHERE streaming_event_id = ?1")
        .bind(streaming_event_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn delete_all_chunks(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM chunk_records")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
