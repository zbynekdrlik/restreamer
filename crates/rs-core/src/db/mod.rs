use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

use crate::error::Result;
use crate::models::{ChunkRecord, ChunkStats, ClientProfile, StreamingEvent};

mod templates;
pub use templates::*;

mod v2;
pub use v2::*;

mod migrations;
pub use migrations::{MAX_SCHEMA_VERSION, current_schema_version, run_migrations};

pub mod upload;
pub use upload::{
    list_recent_uploads, mark_chunk_in_process, mark_upload_permanently_failed,
    pick_next_uploadable_chunk, pick_next_uploadable_chunks, record_upload_attempt,
    record_upload_failure, record_upload_success, reset_orphaned_in_process,
};

pub mod audit;

pub mod drift;

pub mod metrics;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod upload_tests;

#[cfg(test)]
mod template_tests;

#[cfg(test)]
mod delivery_log_tests;

#[cfg(test)]
mod delivery_status_tests;

#[cfg(test)]
mod migration_tests;

#[cfg(test)]
mod audit_tests;

#[cfg(test)]
mod drift_tests;

#[cfg(test)]
mod metrics_tests;

#[cfg(test)]
mod pool_tests;

#[cfg(test)]
mod streaming_event_flag_tests;

/// Create a SQLite connection pool.
pub async fn create_pool(db_path: &Path) -> Result<SqlitePool> {
    let url = format!("sqlite:{}?mode=rwc", db_path.display());
    let options = SqliteConnectOptions::from_str(&url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_millis(5000))
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
    let options = SqliteConnectOptions::from_str("sqlite::memory:")?
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_millis(5000))
        .pragma("foreign_keys", "1");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok(pool)
}

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
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from, rescue_video_url
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
        rescue_video_url: r.get("rescue_video_url"),
    }))
}

pub async fn get_streaming_event_by_id(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<StreamingEvent>> {
    let row = sqlx::query(
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from, rescue_video_url
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
        rescue_video_url: r.get("rescue_video_url"),
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
         in_process, sent, sequence_number, duration_ms,
         upload_attempts, upload_first_attempt_at, upload_completed_at,
         upload_duration_ms, upload_last_error, upload_next_retry_at, upload_failed_permanently
         FROM chunk_records
         WHERE sent = 0 AND in_process = 0
         ORDER BY id ASC
         LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(upload::row_to_chunk_record).collect())
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
         in_process, sent, sequence_number, duration_ms,
         upload_attempts, upload_first_attempt_at, upload_completed_at,
         upload_duration_ms, upload_last_error, upload_next_retry_at, upload_failed_permanently
         FROM chunk_records ORDER BY id DESC LIMIT ?1 OFFSET ?2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(upload::row_to_chunk_record).collect())
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

/// Compute the start chunk that gives approximately `target_ms` of buffer
/// from the latest sent chunk. Walks backwards from the newest sent chunk,
/// accumulating `duration_ms` until the target is reached.
///
/// Return values:
/// - **Content ≤ target (within the scanned window):** returns the oldest
///   seq in the scanned window. Normally that is the event's `first_seq`,
///   so VPS starts from the beginning and warmup waits for more content.
/// - **Content > target:** returns a later seq so the VPS starts with
///   exactly the target window of buffer (VPS boot took longer than the
///   cache target, or OBS was started early).
/// - **Scanned window exhausted without reaching target** (very long
///   event with >MAX_WALK_ROWS sent chunks but short per-chunk duration):
///   returns the oldest seq in the scanned window, not the event's true
///   first_seq. Those older chunks are well past the live edge and
///   irrelevant anyway.
pub async fn compute_target_start_chunk(
    pool: &SqlitePool,
    event_id: i64,
    target_ms: i64,
) -> Result<i64> {
    // Bounded walk: for any realistic target (up to 1000s = 17 min) and chunk
    // size (≥100ms), 10_000 rows is far more than the accumulator needs.
    // Capping prevents loading millions of rows on a multi-hour event.
    const MAX_WALK_ROWS: i64 = 10_000;

    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT sequence_number, duration_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1
         ORDER BY sequence_number DESC
         LIMIT ?2",
    )
    .bind(event_id)
    .bind(MAX_WALK_ROWS)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(1);
    }

    let latest_seq = rows[0].0;
    let mut accum: i64 = 0;
    let mut start = latest_seq;
    for (seq, dur) in &rows {
        accum += dur;
        start = *seq;
        if accum >= target_ms {
            break;
        }
    }

    // Defense-in-depth (#146): if every walked row has duration_ms = 0
    // (the PR #144 corruption pattern), the loop above walked all rows
    // and returned the OLDEST seq -- which the orchestrator then sends
    // as start_chunk_id to the VPS. That chunk has long been pruned
    // from S3, hanging warmup. Fall back to live-edge so delivery
    // starts (with empty buffer) instead of hanging.
    if accum == 0 {
        tracing::warn!(
            event_id,
            row_count = rows.len(),
            "compute_target_start_chunk: all sent chunks have duration_ms=0; using latest seq as start_chunk_id"
        );
        return Ok(latest_seq);
    }

    Ok(start)
}

/// Get all chunks for a specific streaming event, ordered by sequence number.
pub async fn get_chunks_for_event(
    pool: &SqlitePool,
    streaming_event_id: i64,
) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
         in_process, sent, sequence_number, duration_ms,
         upload_attempts, upload_first_attempt_at, upload_completed_at,
         upload_duration_ms, upload_last_error, upload_next_retry_at, upload_failed_permanently
         FROM chunk_records WHERE streaming_event_id = ?1
         ORDER BY sequence_number ASC",
    )
    .bind(streaming_event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(upload::row_to_chunk_record).collect())
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
///
/// During warmup (`delivered_up_to == 0`) the VPS hasn't started consuming yet,
/// so the raw sum equals total sent duration and keeps growing past the target.
/// To prevent dashboard overshoot, the result is capped at `target_secs` when
/// `delivered_up_to == 0`. Once the VPS starts playing (`delivered_up_to > 0`),
/// the raw value is returned uncapped.
pub async fn get_cache_duration_secs(
    pool: &SqlitePool,
    event_id: i64,
    delivered_up_to: i64,
    target_secs: f64,
) -> Result<f64> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) as total_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1 AND sequence_number > ?2",
    )
    .bind(event_id)
    .bind(delivered_up_to)
    .fetch_one(pool)
    .await?;
    let raw = row.get::<i64, _>("total_ms") as f64 / 1000.0;
    if delivered_up_to == 0 {
        Ok(raw.min(target_secs))
    } else {
        Ok(raw)
    }
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
