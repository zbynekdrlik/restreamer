use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

use crate::error::Result;
use crate::models::{
    ChunkRecord, ChunkStats, ClientProfile, DeliveryEndpointStatus, DeliveryInstance,
    EndpointConfig, EventEndpoint, ScheduledStream, StreamingEvent, YouTubeOAuth,
};

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

/// Schema version for migration tracking.
const SCHEMA_VERSION: i32 = 2;

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
    server_type    TEXT NOT NULL DEFAULT 'cx22',
    event_id       INTEGER REFERENCES streaming_events(id) ON DELETE SET NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_health_at TEXT
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

CREATE TABLE IF NOT EXISTS scheduled_streams (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL REFERENCES streaming_events(id) ON DELETE CASCADE,
    start_time      TEXT NOT NULL,
    repeat_interval TEXT CHECK(repeat_interval IS NULL OR repeat_interval IN ('weekly','daily')),
    last_run_at     TEXT,
    next_run_at     TEXT,
    enabled         INTEGER NOT NULL DEFAULT 1
);

ALTER TABLE streaming_events ADD COLUMN buffer INTEGER NOT NULL DEFAULT 1
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
    let row = sqlx::query(
        "SELECT id, identifier, short_description, date_of_event,
         server_ip, received_bytes, receiving_activated, delivering_activated
         FROM streaming_events ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| StreamingEvent {
        id: r.get("id"),
        identifier: r.get("identifier"),
        short_description: r.get("short_description"),
        date_of_event: r.get("date_of_event"),
        server_ip: r.get("server_ip"),
        received_bytes: r.get("received_bytes"),
        receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
        delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
    }))
}

pub async fn get_streaming_event_by_id(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<StreamingEvent>> {
    let row = sqlx::query(
        "SELECT id, identifier, short_description, date_of_event,
         server_ip, received_bytes, receiving_activated, delivering_activated
         FROM streaming_events WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| StreamingEvent {
        id: r.get("id"),
        identifier: r.get("identifier"),
        short_description: r.get("short_description"),
        date_of_event: r.get("date_of_event"),
        server_ip: r.get("server_ip"),
        received_bytes: r.get("received_bytes"),
        receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
        delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
    }))
}

pub async fn upsert_streaming_event(
    pool: &SqlitePool,
    identifier: &str,
    short_description: Option<&str>,
    server_ip: &str,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO streaming_events (identifier, short_description, server_ip, receiving_activated, delivering_activated)
         VALUES (?1, ?2, ?3, 1, 1)
         ON CONFLICT(identifier) DO UPDATE SET
             short_description = COALESCE(?2, streaming_events.short_description),
             server_ip = ?3,
             receiving_activated = 1,
             delivering_activated = 1
         RETURNING id",
    )
    .bind(identifier)
    .bind(short_description)
    .bind(server_ip)
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
/// winning the `ORDER BY id DESC` query in `get_streaming_event()`.
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
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO chunk_records (streaming_event_id, chunk_file_path, data_size, md5)
         VALUES (?1, ?2, ?3, ?4) RETURNING id",
    )
    .bind(streaming_event_id)
    .bind(chunk_file_path)
    .bind(data_size)
    .bind(md5)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn get_unsent_chunks(pool: &SqlitePool, limit: i64) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
         in_process, sent
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
    sqlx::query("UPDATE chunk_records SET sent = 1, in_process = 0 WHERE id = ?1")
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
         in_process, sent
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

pub async fn delete_all_chunks(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM chunk_records")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

// --- Endpoint Configs ---

pub async fn list_endpoint_configs(pool: &SqlitePool) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, created_at, updated_at
         FROM endpoint_configs ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EndpointConfig {
            id: r.get("id"),
            alias: r.get("alias"),
            service_type: r.get("service_type"),
            stream_key: r.get("stream_key"),
            enabled: r.get::<i32, _>("enabled") != 0,
            position_last: r.get("position_last"),
            delivered_bytes: r.get("delivered_bytes"),
            is_fast: r.get::<i32, _>("is_fast") != 0,
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub async fn get_endpoint_config(pool: &SqlitePool, id: i64) -> Result<Option<EndpointConfig>> {
    let row = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, created_at, updated_at
         FROM endpoint_configs WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| EndpointConfig {
        id: r.get("id"),
        alias: r.get("alias"),
        service_type: r.get("service_type"),
        stream_key: r.get("stream_key"),
        enabled: r.get::<i32, _>("enabled") != 0,
        position_last: r.get("position_last"),
        delivered_bytes: r.get("delivered_bytes"),
        is_fast: r.get::<i32, _>("is_fast") != 0,
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }))
}

pub async fn create_endpoint_config(
    pool: &SqlitePool,
    alias: &str,
    service_type: &str,
    stream_key: &str,
    is_fast: bool,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast)
         VALUES (?1, ?2, ?3, ?4) RETURNING id",
    )
    .bind(alias)
    .bind(service_type)
    .bind(stream_key)
    .bind(is_fast as i32)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn update_endpoint_config(
    pool: &SqlitePool,
    id: i64,
    alias: &str,
    service_type: &str,
    stream_key: &str,
    enabled: bool,
    is_fast: bool,
) -> Result<()> {
    sqlx::query(
        "UPDATE endpoint_configs SET alias = ?1, service_type = ?2, stream_key = ?3,
         enabled = ?4, is_fast = ?5, updated_at = datetime('now') WHERE id = ?6",
    )
    .bind(alias)
    .bind(service_type)
    .bind(stream_key)
    .bind(enabled as i32)
    .bind(is_fast as i32)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_endpoint_config(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM endpoint_configs WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// --- Event Endpoints (M2M) ---

pub async fn attach_endpoint_to_event(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO event_endpoints (event_id, endpoint_id) VALUES (?1, ?2)",
    )
    .bind(event_id)
    .bind(endpoint_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn detach_endpoint_from_event(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM event_endpoints WHERE event_id = ?1 AND endpoint_id = ?2")
        .bind(event_id)
        .bind(endpoint_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_event_endpoints(
    pool: &SqlitePool,
    event_id: i64,
) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT e.id, e.alias, e.service_type, e.stream_key, e.enabled, e.position_last,
         e.delivered_bytes, e.is_fast, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN event_endpoints ee ON ee.endpoint_id = e.id
         WHERE ee.event_id = ?1
         ORDER BY e.id",
    )
    .bind(event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EndpointConfig {
            id: r.get("id"),
            alias: r.get("alias"),
            service_type: r.get("service_type"),
            stream_key: r.get("stream_key"),
            enabled: r.get::<i32, _>("enabled") != 0,
            position_last: r.get("position_last"),
            delivered_bytes: r.get("delivered_bytes"),
            is_fast: r.get::<i32, _>("is_fast") != 0,
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

// --- Delivery Instances ---

pub async fn create_delivery_instance(
    pool: &SqlitePool,
    hetzner_id: i64,
    name: &str,
    ipv4: &str,
    server_type: &str,
    event_id: Option<i64>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO delivery_instances (hetzner_id, name, ipv4, server_type, event_id)
         VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id",
    )
    .bind(hetzner_id)
    .bind(name)
    .bind(ipv4)
    .bind(server_type)
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn get_delivery_instance(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<DeliveryInstance>> {
    let row = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
         FROM delivery_instances WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| DeliveryInstance {
        id: r.get("id"),
        hetzner_id: r.get("hetzner_id"),
        name: r.get("name"),
        ipv4: r.get("ipv4"),
        status: r.get("status"),
        server_type: r.get("server_type"),
        event_id: r.get("event_id"),
        created_at: r.get("created_at"),
        last_health_at: r.get("last_health_at"),
    }))
}

pub async fn update_delivery_instance_status(
    pool: &SqlitePool,
    id: i64,
    status: &str,
) -> Result<()> {
    sqlx::query("UPDATE delivery_instances SET status = ?1 WHERE id = ?2")
        .bind(status)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_delivery_instance_health(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("UPDATE delivery_instances SET last_health_at = datetime('now') WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_delivery_instance(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM delivery_instances WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_delivery_instances(pool: &SqlitePool) -> Result<Vec<DeliveryInstance>> {
    let rows = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
         FROM delivery_instances ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DeliveryInstance {
            id: r.get("id"),
            hetzner_id: r.get("hetzner_id"),
            name: r.get("name"),
            ipv4: r.get("ipv4"),
            status: r.get("status"),
            server_type: r.get("server_type"),
            event_id: r.get("event_id"),
            created_at: r.get("created_at"),
            last_health_at: r.get("last_health_at"),
        })
        .collect())
}

// --- YouTube OAuth ---

pub async fn get_youtube_oauth(pool: &SqlitePool) -> Result<Option<YouTubeOAuth>> {
    let row = sqlx::query(
        "SELECT id, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at
         FROM youtube_oauth WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| YouTubeOAuth {
        id: r.get("id"),
        access_token: r.get("access_token"),
        refresh_token: r.get("refresh_token"),
        token_uri: r.get("token_uri"),
        client_id: r.get("client_id"),
        client_secret: r.get("client_secret"),
        scopes: r.get("scopes"),
        expires_at: r.get("expires_at"),
    }))
}

pub async fn upsert_youtube_oauth(
    pool: &SqlitePool,
    access_token: &str,
    refresh_token: &str,
    token_uri: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &str,
    expires_at: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO youtube_oauth (id, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at)
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(id) DO UPDATE SET
             access_token = ?1, refresh_token = ?2, token_uri = ?3,
             client_id = ?4, client_secret = ?5, scopes = ?6, expires_at = ?7",
    )
    .bind(access_token)
    .bind(refresh_token)
    .bind(token_uri)
    .bind(client_id)
    .bind(client_secret)
    .bind(scopes)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Scheduled Streams ---

pub async fn list_scheduled_streams(pool: &SqlitePool) -> Result<Vec<ScheduledStream>> {
    let rows = sqlx::query(
        "SELECT id, event_id, start_time, repeat_interval, last_run_at, next_run_at, enabled
         FROM scheduled_streams ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ScheduledStream {
            id: r.get("id"),
            event_id: r.get("event_id"),
            start_time: r.get("start_time"),
            repeat_interval: r.get("repeat_interval"),
            last_run_at: r.get("last_run_at"),
            next_run_at: r.get("next_run_at"),
            enabled: r.get::<i32, _>("enabled") != 0,
        })
        .collect())
}

pub async fn create_scheduled_stream(
    pool: &SqlitePool,
    event_id: i64,
    start_time: &str,
    repeat_interval: Option<&str>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO scheduled_streams (event_id, start_time, repeat_interval, next_run_at)
         VALUES (?1, ?2, ?3, ?2) RETURNING id",
    )
    .bind(event_id)
    .bind(start_time)
    .bind(repeat_interval)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn update_scheduled_stream(
    pool: &SqlitePool,
    id: i64,
    start_time: &str,
    repeat_interval: Option<&str>,
    enabled: bool,
) -> Result<()> {
    sqlx::query(
        "UPDATE scheduled_streams SET start_time = ?1, repeat_interval = ?2, enabled = ?3, next_run_at = ?1 WHERE id = ?4",
    )
    .bind(start_time)
    .bind(repeat_interval)
    .bind(enabled as i32)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_scheduled_stream(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM scheduled_streams WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_due_scheduled_streams(pool: &SqlitePool, now: &str) -> Result<Vec<ScheduledStream>> {
    let rows = sqlx::query(
        "SELECT id, event_id, start_time, repeat_interval, last_run_at, next_run_at, enabled
         FROM scheduled_streams
         WHERE enabled = 1 AND next_run_at <= ?1
         ORDER BY next_run_at",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| ScheduledStream {
            id: r.get("id"),
            event_id: r.get("event_id"),
            start_time: r.get("start_time"),
            repeat_interval: r.get("repeat_interval"),
            last_run_at: r.get("last_run_at"),
            next_run_at: r.get("next_run_at"),
            enabled: r.get::<i32, _>("enabled") != 0,
        })
        .collect())
}

pub async fn mark_scheduled_stream_run(
    pool: &SqlitePool,
    id: i64,
    last_run_at: &str,
    next_run_at: Option<&str>,
    enabled: bool,
) -> Result<()> {
    sqlx::query(
        "UPDATE scheduled_streams SET last_run_at = ?1, next_run_at = ?2, enabled = ?3 WHERE id = ?4",
    )
    .bind(last_run_at)
    .bind(next_run_at)
    .bind(enabled as i32)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Streaming Events (extended) ---

pub async fn list_streaming_events(pool: &SqlitePool) -> Result<Vec<StreamingEvent>> {
    let rows = sqlx::query(
        "SELECT id, identifier, short_description, date_of_event,
         server_ip, received_bytes, receiving_activated, delivering_activated
         FROM streaming_events ORDER BY id DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| StreamingEvent {
            id: r.get("id"),
            identifier: r.get("identifier"),
            short_description: r.get("short_description"),
            date_of_event: r.get("date_of_event"),
            server_ip: r.get("server_ip"),
            received_bytes: r.get("received_bytes"),
            receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
            delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
        })
        .collect())
}

pub async fn create_streaming_event(
    pool: &SqlitePool,
    identifier: &str,
    short_description: &str,
    date_of_event: &str,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO streaming_events (identifier, short_description, date_of_event)
         VALUES (?1, ?2, ?3) RETURNING id",
    )
    .bind(identifier)
    .bind(short_description)
    .bind(date_of_event)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_db() -> SqlitePool {
        let pool = create_memory_pool().await.unwrap();
        run_migrations(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn client_profile_crud() {
        let pool = setup_db().await;
        assert!(get_client_profile(&pool).await.unwrap().is_none());

        upsert_client_profile(&pool, "test-uuid").await.unwrap();
        let profile = get_client_profile(&pool).await.unwrap().unwrap();
        assert_eq!(profile.user_uuid, "test-uuid");

        upsert_client_profile(&pool, "updated-uuid").await.unwrap();
        let profile = get_client_profile(&pool).await.unwrap().unwrap();
        assert_eq!(profile.user_uuid, "updated-uuid");
    }

    #[tokio::test]
    async fn streaming_event_crud() {
        let pool = setup_db().await;
        assert!(get_streaming_event(&pool).await.unwrap().is_none());

        let id = upsert_streaming_event(&pool, "evt-1", Some("Test Event"), "192.168.1.1")
            .await
            .unwrap();
        assert!(id > 0);

        let event = get_streaming_event(&pool).await.unwrap().unwrap();
        assert_eq!(event.identifier.as_deref(), Some("evt-1"));
        assert!(event.receiving_activated);
        assert!(event.delivering_activated);

        update_streaming_event_flags(&pool, id, false, false)
            .await
            .unwrap();
        let event = get_streaming_event(&pool).await.unwrap().unwrap();
        assert!(!event.receiving_activated);
        assert!(!event.delivering_activated);

        update_received_bytes(&pool, id, 1024).await.unwrap();
        let event = get_streaming_event(&pool).await.unwrap().unwrap();
        assert_eq!(event.received_bytes, 1024);

        delete_streaming_event(&pool, id).await.unwrap();
        assert!(get_streaming_event(&pool).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn chunk_record_crud() {
        let pool = setup_db().await;
        let event_id = upsert_streaming_event(&pool, "evt-1", None, "127.0.0.1")
            .await
            .unwrap();

        let chunk_id = insert_chunk(&pool, event_id, "/tmp/chunk1.bin", 512, "abc123")
            .await
            .unwrap();
        assert!(chunk_id > 0);

        let unsent = get_unsent_chunks(&pool, 10).await.unwrap();
        assert_eq!(unsent.len(), 1);
        assert_eq!(unsent[0].md5, "abc123");
        assert!(!unsent[0].in_process);
        assert!(!unsent[0].sent);

        set_chunk_in_process(&pool, chunk_id, true).await.unwrap();
        let unsent = get_unsent_chunks(&pool, 10).await.unwrap();
        assert_eq!(unsent.len(), 0);

        set_chunk_sent(&pool, chunk_id).await.unwrap();
        let stats = get_chunk_stats(&pool, 1000).await.unwrap();
        assert_eq!(stats.total_chunks, 1);
        assert_eq!(stats.sent_chunks, 1);
        assert_eq!(stats.pending_chunks, 0);
    }

    #[tokio::test]
    async fn chunk_stats_and_pagination() {
        let pool = setup_db().await;
        let event_id = upsert_streaming_event(&pool, "evt-1", None, "127.0.0.1")
            .await
            .unwrap();

        for i in 0..5 {
            insert_chunk(
                &pool,
                event_id,
                &format!("/tmp/chunk{i}.bin"),
                100 * (i + 1),
                &format!("md5_{i}"),
            )
            .await
            .unwrap();
        }

        let stats = get_chunk_stats(&pool, 1000).await.unwrap();
        assert_eq!(stats.total_chunks, 5);
        assert_eq!(stats.pending_chunks, 5);
        assert_eq!(stats.total_bytes, 100 + 200 + 300 + 400 + 500);

        let page = get_chunks_paginated(&pool, 0, 3).await.unwrap();
        assert_eq!(page.len(), 3);

        let page2 = get_chunks_paginated(&pool, 3, 3).await.unwrap();
        assert_eq!(page2.len(), 2);
    }

    #[tokio::test]
    async fn delete_all_chunks_works() {
        let pool = setup_db().await;
        let event_id = upsert_streaming_event(&pool, "evt-1", None, "127.0.0.1")
            .await
            .unwrap();

        for i in 0..3 {
            insert_chunk(&pool, event_id, &format!("/tmp/c{i}.bin"), 100, "md5")
                .await
                .unwrap();
        }

        let deleted = delete_all_chunks(&pool).await.unwrap();
        assert_eq!(deleted, 3);

        let stats = get_chunk_stats(&pool, 1000).await.unwrap();
        assert_eq!(stats.total_chunks, 0);
    }

    #[tokio::test]
    async fn cascade_delete() {
        let pool = setup_db().await;
        let event_id = upsert_streaming_event(&pool, "evt-1", None, "127.0.0.1")
            .await
            .unwrap();
        insert_chunk(&pool, event_id, "/tmp/c.bin", 100, "md5")
            .await
            .unwrap();

        delete_streaming_event(&pool, event_id).await.unwrap();
        let stats = get_chunk_stats(&pool, 1000).await.unwrap();
        assert_eq!(stats.total_chunks, 0);
    }

    #[tokio::test]
    async fn delete_other_streaming_events_keeps_only_target() {
        let pool = setup_db().await;

        // Insert 3 events
        let id1 = upsert_streaming_event(&pool, "evt-1", Some("Event 1"), "10.0.0.1")
            .await
            .unwrap();
        let id2 = upsert_streaming_event(&pool, "evt-2", Some("Event 2"), "10.0.0.2")
            .await
            .unwrap();
        let id3 = upsert_streaming_event(&pool, "evt-3", Some("Event 3"), "10.0.0.3")
            .await
            .unwrap();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);

        // Delete all except id2
        let deleted = delete_other_streaming_events(&pool, id2).await.unwrap();
        assert_eq!(deleted, 2);

        // Only id2 should remain
        let remaining = get_streaming_event(&pool).await.unwrap().unwrap();
        assert_eq!(remaining.id, id2);
        assert_eq!(remaining.identifier.as_deref(), Some("evt-2"));

        // id1 and id3 should be gone
        assert!(
            get_streaming_event_by_id(&pool, id1)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            get_streaming_event_by_id(&pool, id3)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_other_streaming_events_noop_when_only_one() {
        let pool = setup_db().await;

        let id = upsert_streaming_event(&pool, "evt-1", Some("Only Event"), "10.0.0.1")
            .await
            .unwrap();

        let deleted = delete_other_streaming_events(&pool, id).await.unwrap();
        assert_eq!(deleted, 0);

        // Event should still exist
        let event = get_streaming_event(&pool).await.unwrap().unwrap();
        assert_eq!(event.id, id);
    }

    #[tokio::test]
    async fn delete_other_streaming_events_cascades_chunks() {
        let pool = setup_db().await;

        let id1 = upsert_streaming_event(&pool, "stale", Some("Stale"), "10.0.0.1")
            .await
            .unwrap();
        let id2 = upsert_streaming_event(&pool, "active", Some("Active"), "10.0.0.2")
            .await
            .unwrap();

        // Add chunks to both events
        insert_chunk(&pool, id1, "/tmp/stale.bin", 100, "md5_stale")
            .await
            .unwrap();
        insert_chunk(&pool, id2, "/tmp/active.bin", 200, "md5_active")
            .await
            .unwrap();

        // Delete stale event
        let deleted = delete_other_streaming_events(&pool, id2).await.unwrap();
        assert_eq!(deleted, 1);

        // Only active event's chunk should remain (cascade delete)
        let stats = get_chunk_stats(&pool, 1000).await.unwrap();
        assert_eq!(stats.total_chunks, 1);
        assert_eq!(stats.total_bytes, 200);
    }

    #[tokio::test]
    async fn migration_is_idempotent() {
        let pool = setup_db().await;
        // Running migrations again should not fail
        run_migrations(&pool).await.unwrap();
        // Tables should still work
        upsert_client_profile(&pool, "test").await.unwrap();
        let profile = get_client_profile(&pool).await.unwrap().unwrap();
        assert_eq!(profile.user_uuid, "test");
    }

    // --- V2 table tests ---

    #[tokio::test]
    async fn endpoint_config_crud() {
        let pool = setup_db().await;
        let list = list_endpoint_configs(&pool).await.unwrap();
        assert!(list.is_empty());

        let id = create_endpoint_config(&pool, "YouTube", "YT_HLS", "yt-key-123", false)
            .await
            .unwrap();
        assert!(id > 0);

        let ep = get_endpoint_config(&pool, id).await.unwrap().unwrap();
        assert_eq!(ep.alias, "YouTube");
        assert_eq!(ep.service_type, "YT_HLS");
        assert!(ep.enabled);
        assert!(!ep.is_fast);

        update_endpoint_config(&pool, id, "YouTube HLS", "YT_HLS", "new-key", true, true)
            .await
            .unwrap();
        let ep = get_endpoint_config(&pool, id).await.unwrap().unwrap();
        assert_eq!(ep.alias, "YouTube HLS");
        assert!(ep.is_fast);

        delete_endpoint_config(&pool, id).await.unwrap();
        assert!(get_endpoint_config(&pool, id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn event_endpoint_attachment() {
        let pool = setup_db().await;

        let event_id = upsert_streaming_event(&pool, "evt-1", Some("Test"), "127.0.0.1")
            .await
            .unwrap();
        let ep1 = create_endpoint_config(&pool, "YT", "YT_HLS", "key1", false)
            .await
            .unwrap();
        let ep2 = create_endpoint_config(&pool, "FB", "FB", "key2", false)
            .await
            .unwrap();

        attach_endpoint_to_event(&pool, event_id, ep1).await.unwrap();
        attach_endpoint_to_event(&pool, event_id, ep2).await.unwrap();

        let eps = get_event_endpoints(&pool, event_id).await.unwrap();
        assert_eq!(eps.len(), 2);

        detach_endpoint_from_event(&pool, event_id, ep1).await.unwrap();
        let eps = get_event_endpoints(&pool, event_id).await.unwrap();
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].alias, "FB");
    }

    #[tokio::test]
    async fn event_endpoint_cascade_on_event_delete() {
        let pool = setup_db().await;

        let event_id = upsert_streaming_event(&pool, "evt-1", Some("Test"), "127.0.0.1")
            .await
            .unwrap();
        let ep_id = create_endpoint_config(&pool, "YT", "YT_HLS", "key1", false)
            .await
            .unwrap();
        attach_endpoint_to_event(&pool, event_id, ep_id).await.unwrap();

        delete_streaming_event(&pool, event_id).await.unwrap();
        // Endpoint config should still exist (only the link is deleted)
        assert!(get_endpoint_config(&pool, ep_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delivery_instance_crud() {
        let pool = setup_db().await;

        let id = create_delivery_instance(&pool, 12345, "rs-delivery-1", "1.2.3.4", "cx22", None)
            .await
            .unwrap();
        assert!(id > 0);

        let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
        assert_eq!(inst.hetzner_id, 12345);
        assert_eq!(inst.name, "rs-delivery-1");
        assert_eq!(inst.status, "creating");

        update_delivery_instance_status(&pool, id, "running").await.unwrap();
        let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
        assert_eq!(inst.status, "running");

        update_delivery_instance_health(&pool, id).await.unwrap();
        let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
        assert!(inst.last_health_at.is_some());

        let list = list_delivery_instances(&pool).await.unwrap();
        assert_eq!(list.len(), 1);

        delete_delivery_instance(&pool, id).await.unwrap();
        assert!(get_delivery_instance(&pool, id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn youtube_oauth_crud() {
        let pool = setup_db().await;

        assert!(get_youtube_oauth(&pool).await.unwrap().is_none());

        upsert_youtube_oauth(
            &pool,
            "access-tok",
            "refresh-tok",
            "https://oauth2.googleapis.com/token",
            "client-id",
            "client-val",
            "youtube.readonly",
            Some("2099-01-01T00:00:00Z"),
        )
        .await
        .unwrap();

        let oauth = get_youtube_oauth(&pool).await.unwrap().unwrap();
        assert_eq!(oauth.access_token, "access-tok");
        assert_eq!(oauth.refresh_token, "refresh-tok");
        assert_eq!(oauth.scopes, "youtube.readonly");

        // Upsert should update
        upsert_youtube_oauth(
            &pool,
            "new-access",
            "refresh-tok",
            "https://oauth2.googleapis.com/token",
            "client-id",
            "client-val",
            "youtube.readonly",
            None,
        )
        .await
        .unwrap();

        let oauth = get_youtube_oauth(&pool).await.unwrap().unwrap();
        assert_eq!(oauth.access_token, "new-access");
        assert!(oauth.expires_at.is_none());
    }

    #[tokio::test]
    async fn scheduled_stream_crud() {
        let pool = setup_db().await;

        let event_id = upsert_streaming_event(&pool, "evt-1", Some("Sunday"), "127.0.0.1")
            .await
            .unwrap();

        let id = create_scheduled_stream(&pool, event_id, "2026-03-15T09:00:00", Some("weekly"))
            .await
            .unwrap();
        assert!(id > 0);

        let list = list_scheduled_streams(&pool).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].repeat_interval.as_deref(), Some("weekly"));
        assert!(list[0].enabled);

        update_scheduled_stream(&pool, id, "2026-03-15T10:00:00", Some("weekly"), true)
            .await
            .unwrap();

        // Due schedules
        let due = get_due_scheduled_streams(&pool, "2026-03-15T10:00:00")
            .await
            .unwrap();
        assert_eq!(due.len(), 1);

        let not_due = get_due_scheduled_streams(&pool, "2026-03-15T09:00:00")
            .await
            .unwrap();
        assert!(not_due.is_empty());

        mark_scheduled_stream_run(
            &pool,
            id,
            "2026-03-15T10:00:00",
            Some("2026-03-22T10:00:00"),
            true,
        )
        .await
        .unwrap();

        let list = list_scheduled_streams(&pool).await.unwrap();
        assert_eq!(list[0].last_run_at.as_deref(), Some("2026-03-15T10:00:00"));
        assert_eq!(
            list[0].next_run_at.as_deref(),
            Some("2026-03-22T10:00:00")
        );

        delete_scheduled_stream(&pool, id).await.unwrap();
        assert!(list_scheduled_streams(&pool).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn scheduled_stream_cascade_on_event_delete() {
        let pool = setup_db().await;
        let event_id = upsert_streaming_event(&pool, "evt-1", Some("Test"), "127.0.0.1")
            .await
            .unwrap();
        create_scheduled_stream(&pool, event_id, "2026-03-15T09:00:00", None)
            .await
            .unwrap();

        delete_streaming_event(&pool, event_id).await.unwrap();
        assert!(list_scheduled_streams(&pool).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_streaming_events_and_create() {
        let pool = setup_db().await;
        assert!(list_streaming_events(&pool).await.unwrap().is_empty());

        let id = create_streaming_event(&pool, "new-evt", "Test Event", "2026-03-15")
            .await
            .unwrap();
        assert!(id > 0);

        let events = list_streaming_events(&pool).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].short_description.as_deref(), Some("Test Event"));
        assert!(!events[0].receiving_activated);
    }

    #[tokio::test]
    async fn endpoint_unique_alias_constraint() {
        let pool = setup_db().await;
        create_endpoint_config(&pool, "YouTube", "YT_HLS", "key1", false)
            .await
            .unwrap();
        // Duplicate alias should fail
        let result = create_endpoint_config(&pool, "YouTube", "FB", "key2", false).await;
        assert!(result.is_err());
    }
}
