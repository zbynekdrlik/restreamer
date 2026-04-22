//! delivery_endpoint_metrics DB access.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRow {
    pub id: i64,
    pub ts_ms: i64,
    pub instance_id: i64,
    pub event_id: i64,
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub bytes_processed_total: i64,
    pub ffmpeg_restart_count: i64,
    pub delivery_mode: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub event_id: Option<i64>,
    pub alias: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub limit: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &SqlitePool,
    ts_ms: i64,
    instance_id: i64,
    event_id: i64,
    alias: &str,
    alive: bool,
    current_chunk_id: i64,
    chunks_processed: i64,
    chunk_delay_secs: f64,
    bytes_processed_total: i64,
    ffmpeg_restart_count: i64,
    delivery_mode: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_endpoint_metrics
         (ts_ms, instance_id, event_id, alias, alive, current_chunk_id,
          chunks_processed, chunk_delay_secs, bytes_processed_total,
          ffmpeg_restart_count, delivery_mode)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(ts_ms)
    .bind(instance_id)
    .bind(event_id)
    .bind(alias)
    .bind(alive as i64)
    .bind(current_chunk_id)
    .bind(chunks_processed)
    .bind(chunk_delay_secs)
    .bind(bytes_processed_total)
    .bind(ffmpeg_restart_count)
    .bind(delivery_mode)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn query(pool: &SqlitePool, f: Filter) -> Result<Vec<MetricsRow>> {
    let mut sql = String::from(
        "SELECT id, ts_ms, instance_id, event_id, alias, alive, current_chunk_id,
         chunks_processed, chunk_delay_secs, bytes_processed_total,
         ffmpeg_restart_count, delivery_mode
         FROM delivery_endpoint_metrics WHERE 1=1",
    );
    let mut binds: Vec<String> = Vec::new();
    if let Some(ev) = f.event_id {
        sql.push_str(&format!(" AND event_id = ?{}", binds.len() + 1));
        binds.push(ev.to_string());
    }
    if let Some(a) = &f.alias {
        sql.push_str(&format!(" AND alias = ?{}", binds.len() + 1));
        binds.push(a.clone());
    }
    if let Some(s) = f.since_ms {
        sql.push_str(&format!(" AND ts_ms >= ?{}", binds.len() + 1));
        binds.push(s.to_string());
    }
    if let Some(u) = f.until_ms {
        sql.push_str(&format!(" AND ts_ms <= ?{}", binds.len() + 1));
        binds.push(u.to_string());
    }
    sql.push_str(" ORDER BY ts_ms ASC");
    sql.push_str(&format!(
        " LIMIT {}",
        f.limit.unwrap_or(2000).clamp(1, 20000)
    ));

    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    let rows = q.fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| MetricsRow {
            id: r.get("id"),
            ts_ms: r.get("ts_ms"),
            instance_id: r.get("instance_id"),
            event_id: r.get("event_id"),
            alias: r.get("alias"),
            alive: r.get::<i64, _>("alive") != 0,
            current_chunk_id: r.get("current_chunk_id"),
            chunks_processed: r.get("chunks_processed"),
            chunk_delay_secs: r.get("chunk_delay_secs"),
            bytes_processed_total: r.get("bytes_processed_total"),
            ffmpeg_restart_count: r.get("ffmpeg_restart_count"),
            delivery_mode: r.get("delivery_mode"),
        })
        .collect())
}

pub async fn rotate(pool: &SqlitePool, keep_days: i64) -> Result<i64> {
    let cutoff_ms = chrono::Utc::now().timestamp_millis() - keep_days * 86_400_000;
    let res = sqlx::query("DELETE FROM delivery_endpoint_metrics WHERE ts_ms < ?1")
        .bind(cutoff_ms)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() as i64)
}
