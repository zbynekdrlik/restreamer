use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

use crate::error::Result;
use crate::models::{ChunkRecord, ChunkStats, ClientProfile, StreamingEvent};

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
const SCHEMA_VERSION: i32 = 1;

/// Run database migrations.
///
/// Uses a version tracking table to support incremental schema changes.
pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        .execute(pool)
        .await?;

    let current: i32 = sqlx::query("SELECT COALESCE(MAX(version), 0) as v FROM schema_version")
        .fetch_one(pool)
        .await
        .map(|r| r.get("v"))
        .unwrap_or(0);

    if current < SCHEMA_VERSION {
        for statement in MIGRATION_V1_SQL.split(';') {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(pool).await?;
            }
        }
        sqlx::query("INSERT OR REPLACE INTO schema_version (version) VALUES (?1)")
            .bind(SCHEMA_VERSION)
            .execute(pool)
            .await?;
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

pub async fn get_chunk_stats(pool: &SqlitePool) -> Result<ChunkStats> {
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
        buffer_duration_secs: pending_chunks as f64,
    })
}

pub async fn delete_all_chunks(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM chunk_records")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
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
        let stats = get_chunk_stats(&pool).await.unwrap();
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

        let stats = get_chunk_stats(&pool).await.unwrap();
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

        let stats = get_chunk_stats(&pool).await.unwrap();
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
        let stats = get_chunk_stats(&pool).await.unwrap();
        assert_eq!(stats.total_chunks, 0);
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
}
