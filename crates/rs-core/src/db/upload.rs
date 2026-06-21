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

/// Count chunks that were marked `upload_failed_permanently = 1` whose
/// first upload attempt is at or after `since_ms` (a unix-epoch millisecond
/// timestamp). The dashboard upload-strip uses this to escalate from
/// `TransientBurst` (yellow) to `PermanentFailures` (red) only when the
/// data loss is recent (typically last 5 min).
///
/// Anchor is `upload_first_attempt_at` rather than the row insertion time
/// because the chunk's failure window starts when retries began, not when
/// the chunk was created (which may be hours earlier on stale rows).
pub async fn count_permanently_failed_since(pool: &SqlitePool, since_ms: i64) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM chunk_records
         WHERE upload_failed_permanently = 1
           AND upload_first_attempt_at IS NOT NULL
           AND upload_first_attempt_at >= ?1",
    )
    .bind(since_ms)
    .fetch_one(pool)
    .await?;
    Ok(n)
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
///
/// Implemented as a SINGLE atomic statement on the pool — `UPDATE ... SET
/// in_process = 1 WHERE id = (SELECT ... LIMIT 1) RETURNING <cols>` — with NO
/// `BEGIN`/commit transaction. This is the #256 fix.
///
/// The OLD picker opened `pool.begin()` (sqlx default = BEGIN DEFERRED),
/// SELECTed the hot `ORDER BY id ASC LIMIT 1` row, then UPDATEd it in the same
/// tx. The deferred read took a read snapshot; the subsequent UPDATE had to
/// UPGRADE that snapshot to a write lock. When any concurrent committer (chunk
/// INSERT / audit batch / `update_received_bytes`) committed between the
/// SELECT-snapshot and the UPDATE, SQLite returned `SQLITE_BUSY_SNAPSHOT`
/// (code 517) IMMEDIATELY — `busy_timeout` never retries 517 — plus
/// `SQLITE_BUSY` (code 5) under writer-writer contention. With 2-8 workers on a
/// 5-connection pool this produced the 30+ minute "database is locked" storm
/// that starved the live-event upload pipeline (2026-06-19 outage).
///
/// The single statement takes the SQLite write lock on its FIRST (and only)
/// statement: there is no read snapshot to invalidate (kills 517) and no
/// read->write upgrade window (kills the storm). `busy_timeout` now fully
/// covers the only remaining failure mode (plain writer-writer code 5, which it
/// retries). It does NOT serialise the workers (the reason the #120
/// single-claimer coordinator was reverted) — each worker still issues its own
/// independent claim; the `WHERE in_process = 0` guard guarantees exactly one
/// winner per row.
///
/// The inner `SELECT` chooses the oldest eligible row; the outer `UPDATE`
/// flips only that row to `in_process = 1` (re-checking eligibility so two
/// near-simultaneous claims can't both win), and `RETURNING` hands back the
/// full row so callers keep the same `Option<ChunkRecord>` contract.
pub async fn pick_next_uploadable_chunk(
    pool: &SqlitePool,
    now_ms: i64,
) -> Result<Option<ChunkRecord>> {
    let row: Option<sqlx::sqlite::SqliteRow> = sqlx::query(
        "UPDATE chunk_records
            SET in_process = 1
          WHERE id = (
                SELECT id FROM chunk_records
                 WHERE sent = 0
                   AND in_process = 0
                   AND upload_failed_permanently = 0
                   AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= ?1)
                 ORDER BY id ASC
                 LIMIT 1
          )
            AND in_process = 0
            AND sent = 0
        RETURNING id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
                  in_process, sent, sequence_number, duration_ms,
                  upload_attempts, upload_first_attempt_at, upload_completed_at,
                  upload_duration_ms, upload_last_error, upload_next_retry_at,
                  upload_failed_permanently",
    )
    .bind(now_ms)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(row_to_chunk_record))
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
