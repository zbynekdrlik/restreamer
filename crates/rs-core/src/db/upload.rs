use sqlx::Row;
use sqlx::SqlitePool;

use crate::models::{ChunkRecord, UploadChunkRow};

use super::Result;

pub(super) fn row_to_chunk_record(r: sqlx::sqlite::SqliteRow) -> ChunkRecord {
    ChunkRecord {
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
        upload_attempts: r.get("upload_attempts"),
        upload_first_attempt_at: r.get("upload_first_attempt_at"),
        upload_completed_at: r.get("upload_completed_at"),
        upload_duration_ms: r.get("upload_duration_ms"),
        upload_last_error: r.get("upload_last_error"),
        upload_next_retry_at: r.get("upload_next_retry_at"),
        upload_failed_permanently: r.get::<i32, _>("upload_failed_permanently") != 0,
    }
}

/// Record the start of an upload attempt. Bumps `upload_attempts`, sets
/// `upload_first_attempt_at` if NULL.
pub async fn record_upload_attempt(pool: &SqlitePool, chunk_id: i64, now_ms: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_attempts = upload_attempts + 1,
             upload_first_attempt_at = COALESCE(upload_first_attempt_at, ?2)
         WHERE id = ?1",
    )
    .bind(chunk_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a failed upload. Sets last_error + duration, schedules next retry,
/// releases in_process so another worker can pick it up after backoff.
pub async fn record_upload_failure(
    pool: &SqlitePool,
    chunk_id: i64,
    error: &str,
    next_retry_at_ms: i64,
    duration_ms: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_last_error = ?2,
             upload_next_retry_at = ?3,
             upload_duration_ms = ?4,
             in_process = 0
         WHERE id = ?1",
    )
    .bind(chunk_id)
    .bind(error)
    .bind(next_retry_at_ms)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a chunk as permanently failed after the retry budget is exhausted.
pub async fn mark_upload_permanently_failed(pool: &SqlitePool, chunk_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_failed_permanently = 1,
             in_process = 0
         WHERE id = ?1",
    )
    .bind(chunk_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Atomically pick the oldest eligible chunk and mark it `in_process=1`.
/// Returns None if nothing is eligible. Eligibility:
///   - sent = 0
///   - in_process = 0
///   - upload_failed_permanently = 0
///   - upload_next_retry_at IS NULL OR upload_next_retry_at <= now_ms
pub async fn pick_next_uploadable_chunk(
    pool: &SqlitePool,
    now_ms: i64,
) -> Result<Option<ChunkRecord>> {
    let mut tx = pool.begin().await?;

    let row: Option<sqlx::sqlite::SqliteRow> = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
                in_process, sent, sequence_number, duration_ms,
                upload_attempts, upload_first_attempt_at, upload_completed_at,
                upload_duration_ms, upload_last_error, upload_next_retry_at,
                upload_failed_permanently
         FROM chunk_records
         WHERE sent = 0
           AND in_process = 0
           AND upload_failed_permanently = 0
           AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= ?1)
         ORDER BY id ASC
         LIMIT 1",
    )
    .bind(now_ms)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let chunk = row_to_chunk_record(row);

    // Atomic claim — if another worker grabbed it, our UPDATE affects 0 rows.
    let result = sqlx::query(
        "UPDATE chunk_records SET in_process = 1
         WHERE id = ?1 AND in_process = 0 AND sent = 0",
    )
    .bind(chunk.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    if result.rows_affected() == 0 {
        return Ok(None);
    }
    Ok(Some(chunk))
}

/// Claim-batch version: returns up to `limit` chunks that are unsent,
/// not-in-process, not permanently-failed, past their retry-time. Does
/// NOT mark them `in_process` — the caller must do that per chunk via
/// [`mark_chunk_in_process`].
///
/// Used by the claim-coordinator (single SELECT every ~200ms) instead
/// of the old per-worker picker that caused SQLite BUSY thrash when N
/// workers raced the same SELECT/UPDATE transaction (issue #120).
pub async fn pick_next_uploadable_chunks(
    pool: &SqlitePool,
    now_ms: i64,
    limit: i64,
) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
                in_process, sent, sequence_number, duration_ms,
                upload_attempts, upload_first_attempt_at, upload_completed_at,
                upload_duration_ms, upload_last_error, upload_next_retry_at,
                upload_failed_permanently
         FROM chunk_records
         WHERE sent = 0 AND in_process = 0 AND upload_failed_permanently = 0
           AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= ?1)
         ORDER BY upload_next_retry_at ASC, id ASC
         LIMIT ?2",
    )
    .bind(now_ms)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_chunk_record).collect())
}

/// Conditional claim — flips `in_process` 0→1 for a chunk. Returns
/// `true` if we won the claim, `false` if some other actor already
/// claimed or the row no longer exists.
pub async fn mark_chunk_in_process(pool: &SqlitePool, id: i64) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE chunk_records SET in_process = 1
         WHERE id = ?1 AND in_process = 0 AND sent = 0",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Record a successful upload.
pub async fn record_upload_success(
    pool: &SqlitePool,
    chunk_id: i64,
    completed_at_ms: i64,
    duration_ms: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET sent = 1,
             in_process = 0,
             upload_completed_at = ?2,
             upload_duration_ms = ?3,
             upload_last_error = NULL
         WHERE id = ?1",
    )
    .bind(chunk_id)
    .bind(completed_at_ms)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Reset abandoned in_process=1 claims left by prior runs/crashes.
/// Only clears rows that are not yet sent and not permanently failed —
/// the picker will then re-eligibilise them immediately.
/// Returns the number of rows reset. Safe to call at startup.
pub async fn reset_orphaned_in_process(pool: &SqlitePool) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE chunk_records
         SET in_process = 0
         WHERE in_process = 1 AND sent = 0 AND upload_failed_permanently = 0",
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// List the most-recent N chunks (by id desc) with upload telemetry joined
/// to the streaming event name.
pub async fn list_recent_uploads(pool: &SqlitePool, limit: i64) -> Result<Vec<UploadChunkRow>> {
    let rows = sqlx::query(
        "SELECT c.id, e.name, c.sequence_number, c.data_size,
                c.upload_attempts, c.upload_duration_ms,
                c.sent, c.in_process, c.upload_failed_permanently,
                c.upload_last_error, c.upload_first_attempt_at, c.upload_completed_at
         FROM chunk_records c
         LEFT JOIN streaming_events e ON e.id = c.streaming_event_id
         ORDER BY c.id DESC
         LIMIT ?1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let sent: i64 = r.get("sent");
            let in_proc: i64 = r.get("in_process");
            let failed: i64 = r.get("upload_failed_permanently");
            let attempts: i64 = r.get("upload_attempts");
            let last_error: Option<String> = r.get("upload_last_error");
            let status = if sent == 1 {
                "sent"
            } else if failed == 1 {
                "failed"
            } else if in_proc == 1 || attempts > 0 || last_error.is_some() {
                "retrying"
            } else {
                "pending"
            }
            .to_string();

            UploadChunkRow {
                chunk_id: r.get("id"),
                event_identifier: r.try_get::<String, _>("name").unwrap_or_default(),
                sequence_number: r.get("sequence_number"),
                size_bytes: r.get("data_size"),
                attempts,
                duration_ms: r.get("upload_duration_ms"),
                status,
                last_error,
                first_attempt_at: r.get("upload_first_attempt_at"),
                completed_at: r.get("upload_completed_at"),
            }
        })
        .collect())
}
