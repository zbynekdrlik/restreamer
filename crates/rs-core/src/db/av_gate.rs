//! Persistence for YouTube A/V-gate sessions (#357, table `av_gate_sessions`).
//!
//! The row is the durable state of one session. The in-process driver task
//! owns the session while Restreamer runs and writes the WHOLE row on every
//! transition (`save`). After a crash, the boot reconcile reads the rows that
//! never reached a terminal state (`list_unfinished`) and tears them down.

use crate::error::Result;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

/// One `av_gate_sessions` row. `state` is the wire string
/// (`starting|ready|processing|done|failed`); the typed state machine lives in
/// rs-api, which owns the transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvGateSessionRow {
    pub id: String,
    pub requester: String,
    pub title: String,
    pub state: String,
    pub broadcast_id: Option<String>,
    pub stream_id: Option<String>,
    pub event_id: Option<i64>,
    /// A `live` transition was ATTEMPTED (set before the call, so a lost
    /// response still counts): the broadcast MUST be completed on every exit
    /// path (owner rule, 2026-10-05). False = it never went on air.
    pub went_live: bool,
    /// The teardown failed; the cleanup sweep retries it until clean.
    pub cleanup_pending: bool,
    /// Teardown progress: the broadcast is completed (or never went live).
    pub broadcast_done: bool,
    /// Teardown progress: the event is stopped and its servers are gone.
    pub event_done: bool,
    pub vod_id: Option<String>,
    pub reason: Option<String>,
    /// YouTube Data API units this session spent (quota guard input).
    pub quota_units: i64,
    pub created_at: String,
    pub ready_at: Option<String>,
    pub stop_requested_at: Option<String>,
    pub processing_at: Option<String>,
    pub finished_at: Option<String>,
}

impl AvGateSessionRow {
    /// A fresh `starting` row.
    pub fn new_starting(id: &str, requester: &str, title: &str, created_at: &str) -> Self {
        Self {
            id: id.to_string(),
            requester: requester.to_string(),
            title: title.to_string(),
            state: "starting".to_string(),
            broadcast_id: None,
            stream_id: None,
            event_id: None,
            went_live: false,
            cleanup_pending: false,
            broadcast_done: false,
            event_done: false,
            vod_id: None,
            reason: None,
            quota_units: 0,
            created_at: created_at.to_string(),
            ready_at: None,
            stop_requested_at: None,
            processing_at: None,
            finished_at: None,
        }
    }
}

const COLUMNS: &str = "id, requester, title, state, broadcast_id, stream_id, event_id, went_live, \
     cleanup_pending, broadcast_done, event_done, vod_id, reason, quota_units, created_at, ready_at, stop_requested_at, processing_at, \
     finished_at";

fn row_to_session(r: sqlx::sqlite::SqliteRow) -> AvGateSessionRow {
    AvGateSessionRow {
        id: r.get("id"),
        requester: r.get("requester"),
        title: r.get("title"),
        state: r.get("state"),
        broadcast_id: r.get("broadcast_id"),
        stream_id: r.get("stream_id"),
        event_id: r.get("event_id"),
        went_live: r.get::<i64, _>("went_live") != 0,
        cleanup_pending: r.get::<i64, _>("cleanup_pending") != 0,
        broadcast_done: r.get::<i64, _>("broadcast_done") != 0,
        event_done: r.get::<i64, _>("event_done") != 0,
        vod_id: r.get("vod_id"),
        reason: r.get("reason"),
        quota_units: r.get("quota_units"),
        created_at: r.get("created_at"),
        ready_at: r.get("ready_at"),
        stop_requested_at: r.get("stop_requested_at"),
        processing_at: r.get("processing_at"),
        finished_at: r.get("finished_at"),
    }
}

/// Insert or fully overwrite a session row (the driver is the only writer of
/// a given id, so a whole-row upsert is race-free and keeps every caller to
/// one code path).
pub async fn save(pool: &SqlitePool, s: &AvGateSessionRow) -> Result<()> {
    sqlx::query(
        "INSERT INTO av_gate_sessions (id, requester, title, state, broadcast_id, stream_id, \
             event_id, went_live, cleanup_pending, broadcast_done, event_done, vod_id, reason, \
             quota_units, created_at, ready_at, stop_requested_at, processing_at, finished_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, \
             ?18, ?19)
         ON CONFLICT(id) DO UPDATE SET
             requester = excluded.requester,
             title = excluded.title,
             state = excluded.state,
             broadcast_id = excluded.broadcast_id,
             stream_id = excluded.stream_id,
             event_id = excluded.event_id,
             went_live = excluded.went_live,
             cleanup_pending = excluded.cleanup_pending,
             broadcast_done = excluded.broadcast_done,
             event_done = excluded.event_done,
             vod_id = excluded.vod_id,
             reason = excluded.reason,
             quota_units = excluded.quota_units,
             created_at = excluded.created_at,
             ready_at = excluded.ready_at,
             stop_requested_at = excluded.stop_requested_at,
             processing_at = excluded.processing_at,
             finished_at = excluded.finished_at",
    )
    .bind(&s.id)
    .bind(&s.requester)
    .bind(&s.title)
    .bind(&s.state)
    .bind(&s.broadcast_id)
    .bind(&s.stream_id)
    .bind(s.event_id)
    .bind(i64::from(s.went_live))
    .bind(i64::from(s.cleanup_pending))
    .bind(i64::from(s.broadcast_done))
    .bind(i64::from(s.event_done))
    .bind(&s.vod_id)
    .bind(&s.reason)
    .bind(s.quota_units)
    .bind(&s.created_at)
    .bind(&s.ready_at)
    .bind(&s.stop_requested_at)
    .bind(&s.processing_at)
    .bind(&s.finished_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch one session by id.
pub async fn get(pool: &SqlitePool, id: &str) -> Result<Option<AvGateSessionRow>> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM av_gate_sessions WHERE id = ?1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_to_session))
}

/// Every session that has not reached `done`/`failed`, oldest first. Read by
/// the boot reconcile.
pub async fn list_unfinished(pool: &SqlitePool) -> Result<Vec<AvGateSessionRow>> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM av_gate_sessions \
         WHERE state NOT IN ('done', 'failed') ORDER BY created_at ASC"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_session).collect())
}

/// Sessions whose teardown failed and must be retried, oldest first.
pub async fn list_cleanup_pending(pool: &SqlitePool) -> Result<Vec<AvGateSessionRow>> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM av_gate_sessions \
         WHERE cleanup_pending != 0 ORDER BY created_at ASC"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_to_session).collect())
}

/// Sum of `quota_units` over sessions created at or after `since` (an
/// RFC 3339 UTC timestamp, compared as text: every `created_at` is written in
/// the same fixed-width `…Z` format).
pub async fn quota_units_since(pool: &SqlitePool, since: &str) -> Result<i64> {
    let total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(quota_units), 0) FROM av_gate_sessions WHERE created_at >= ?1",
    )
    .bind(since)
    .fetch_one(pool)
    .await?;
    Ok(total)
}

#[cfg(test)]
#[path = "av_gate_tests.rs"]
mod tests;
