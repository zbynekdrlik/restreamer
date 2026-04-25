use sqlx::{Row, SqlitePool};

use crate::error::Result;

/// Insert a chunk record and stamp the producer wall-clock time.
///
/// Wraps the existing `insert_chunk` and updates the `wall_clock_written_at_ms` column
/// added in migration V20.
#[allow(clippy::too_many_arguments)]
pub async fn insert_chunk_with_walltime(
    pool: &SqlitePool,
    streaming_event_id: i64,
    chunk_file_path: &str,
    data_size: i64,
    md5: &str,
    duration_ms: i64,
    wall_clock_written_at_ms: i64,
) -> Result<i64> {
    let id = super::insert_chunk(
        pool,
        streaming_event_id,
        chunk_file_path,
        data_size,
        md5,
        duration_ms,
    )
    .await?;
    sqlx::query("UPDATE chunk_records SET wall_clock_written_at_ms = ?1 WHERE id = ?2")
        .bind(wall_clock_written_at_ms)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(id)
}

/// Insert one clock-skew sample (producer vs VPS wall-clock).
#[allow(clippy::too_many_arguments)]
pub async fn insert_clock_skew_sample(
    pool: &SqlitePool,
    event_id: i64,
    measured_at_ms: i64,
    local_before_ms: i64,
    vps_reported_ms: i64,
    local_after_ms: i64,
    skew_ms: i64,
    rtt_ms: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO clock_skew_samples
         (event_id, measured_at_ms, local_before_ms, vps_reported_ms,
          local_after_ms, skew_ms, rtt_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(event_id)
    .bind(measured_at_ms)
    .bind(local_before_ms)
    .bind(vps_reported_ms)
    .bind(local_after_ms)
    .bind(skew_ms)
    .bind(rtt_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// A time-series data point used by the diagnostics API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DriftSample {
    /// Wall-clock epoch milliseconds for this sample.
    pub t_ms: i64,
    /// Dimensionless ratio or millisecond offset depending on the series.
    pub value: f64,
}

/// Return the producer rate series for a streaming event.
///
/// Each point is `duration_ms / d_wall_clock` for consecutive chunk pairs,
/// where 1.0 means the producer is exactly keeping pace with real time.
pub async fn list_chunk_producer_rate(
    pool: &SqlitePool,
    event_id: i64,
    since_ms: i64,
) -> Result<Vec<DriftSample>> {
    let rows = sqlx::query(
        "SELECT duration_ms AS d_ts,
                wall_clock_written_at_ms AS wc
         FROM chunk_records
         WHERE streaming_event_id = ?1
           AND wall_clock_written_at_ms IS NOT NULL
           AND wall_clock_written_at_ms >= ?2
         ORDER BY id ASC",
    )
    .bind(event_id)
    .bind(since_ms)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len().saturating_sub(1));
    let mut prev_wc: Option<i64> = None;
    for r in &rows {
        let wc: i64 = r.get("wc");
        let d_ts: i64 = r.get("d_ts");
        if let Some(p) = prev_wc {
            let d_wc = wc - p;
            if d_wc > 0 && d_ts >= 0 {
                out.push(DriftSample {
                    t_ms: wc,
                    value: (d_ts as f64) / (d_wc as f64),
                });
            }
        }
        prev_wc = Some(wc);
    }
    Ok(out)
}

/// Return the clock-skew series for a streaming event.
///
/// Each point's `value` is `skew_ms` (positive = VPS clock ahead of producer).
pub async fn list_clock_skew(
    pool: &SqlitePool,
    event_id: i64,
    since_ms: i64,
) -> Result<Vec<DriftSample>> {
    let rows = sqlx::query(
        "SELECT measured_at_ms, skew_ms FROM clock_skew_samples
         WHERE event_id = ?1 AND measured_at_ms >= ?2
         ORDER BY measured_at_ms ASC",
    )
    .bind(event_id)
    .bind(since_ms)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| DriftSample {
            t_ms: r.get::<i64, _>("measured_at_ms"),
            value: r.get::<i64, _>("skew_ms") as f64,
        })
        .collect())
}
