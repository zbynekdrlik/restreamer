//! Per-delivery ffmpeg progress poller: periodically fetches `recent_progress`
//! rows from the VPS `/api/status?progress_since=<cursor>` and persists them
//! to `ffmpeg_progress_samples` on stream.lan.
//!
//! Follows the same pattern as `clock_skew_probe.rs` — a single
//! `spawn_progress_poll` call from `poll_and_init` starts the background task.

use std::time::Duration;

use sqlx::SqlitePool;

/// Poll interval for drift progress samples. Shorter than the skew probe (30s)
/// because ffmpeg emits progress at ~2 samples/sec/endpoint; with 4 endpoints
/// and a 500-cap ring, any poll gap > ~1 minute risks silent sample loss.
const POLL_INTERVAL_SECS: u64 = 10;

/// Spawn a background task that polls the VPS for ffmpeg progress samples and
/// persists them to `ffmpeg_progress_samples`.
///
/// Exits when `delivering_activated` is false/gone in the DB (mirrors the
/// pattern used by `clock_skew_probe::spawn_skew_probe`).
pub fn spawn_progress_poll(
    pool: SqlitePool,
    event_id: i64,
    vps_base_url: String,
    auth_token: String,
) {
    tokio::spawn(async move {
        let mut cursor: i64 = 0;
        let mut tick = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // skip immediate first tick

        loop {
            tick.tick().await;

            // Stop if the event is no longer delivering.
            match rs_core::db::get_streaming_event_by_id(&pool, event_id).await {
                Ok(Some(evt)) if !evt.delivering_activated => {
                    tracing::info!(
                        event_id,
                        "Progress poll stopping: event no longer delivering"
                    );
                    return;
                }
                Ok(None) => {
                    tracing::info!(event_id, "Progress poll stopping: event deleted");
                    return;
                }
                Err(e) => {
                    tracing::warn!(event_id, "Progress poll DB error: {e}");
                }
                _ => {}
            }

            match poll_once(&pool, event_id, &vps_base_url, &auth_token, cursor).await {
                Ok(new_cursor) => {
                    cursor = new_cursor;
                }
                Err(e) => {
                    tracing::warn!(event_id, "Progress poll failed: {e}");
                }
            }
        }
    });
}

/// Perform one poll: fetch `recent_progress` since `cursor`, persist each row,
/// return the new cursor.
async fn poll_once(
    pool: &SqlitePool,
    event_id: i64,
    vps_base_url: &str,
    auth_token: &str,
    cursor: i64,
) -> anyhow::Result<i64> {
    #[derive(serde::Deserialize)]
    struct ProgressRow {
        endpoint_alias: String,
        media_time_ms: i64,
        wall_clock_ms: i64,
    }

    #[derive(serde::Deserialize)]
    struct StatusBody {
        #[serde(default)]
        recent_progress: Vec<ProgressRow>,
        #[serde(default)]
        next_progress_cursor: i64,
    }

    let url = format!("{vps_base_url}/api/status?progress_since={cursor}");
    let body: StatusBody = reqwest::Client::new()
        .get(&url)
        .bearer_auth(auth_token)
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .json()
        .await?;

    if body.recent_progress.is_empty() {
        return Ok(cursor);
    }

    // measured_at_ms = stream.lan receive time (SQL ORDER BY key).
    // wall_clock_ms = VPS emit time (used by list_ffmpeg_consumer_rate for Δ).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for row in &body.recent_progress {
        if let Err(e) = rs_core::db::drift::insert_ffmpeg_progress_sample(
            pool,
            event_id,
            &row.endpoint_alias,
            now_ms,
            row.media_time_ms,
            row.wall_clock_ms,
        )
        .await
        {
            tracing::warn!(
                event_id,
                alias = %row.endpoint_alias,
                "Failed to persist ffmpeg progress sample: {e}"
            );
        } else {
            tracing::debug!(
                event_id,
                alias = %row.endpoint_alias,
                media_time_ms = row.media_time_ms,
                "Ffmpeg progress sample stored"
            );
        }
    }

    Ok(body.next_progress_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Integration test: mock VPS returns two progress rows, assert both are
    /// persisted to `ffmpeg_progress_samples`.
    #[tokio::test]
    async fn poll_once_persists_progress_samples() {
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        let event_id = rs_core::db::create_streaming_event(&pool, "poll-test-evt")
            .await
            .unwrap();

        // Build a minimal mock VPS that returns two progress rows.
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_clone = Arc::clone(&call_count);
        let app = axum::Router::new().route(
            "/api/status",
            get(move || {
                let cc = Arc::clone(&call_count_clone);
                async move {
                    *cc.lock().await += 1;
                    axum::Json(serde_json::json!({
                        "status": "ok",
                        "endpoint_count": 1,
                        "endpoints": [],
                        "recent_progress": [
                            {
                                "id": 1,
                                "endpoint_alias": "yt1",
                                "media_time_ms": 5000_i64,
                                "wall_clock_ms": 1_700_000_000_000_i64
                            },
                            {
                                "id": 2,
                                "endpoint_alias": "yt1",
                                "media_time_ms": 7000_i64,
                                "wall_clock_ms": 1_700_000_002_000_i64
                            }
                        ],
                        "next_progress_cursor": 2_i64
                    }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let new_cursor = poll_once(&pool, event_id, &format!("http://{addr}"), "test-token", 0)
            .await
            .unwrap();

        assert_eq!(new_cursor, 2);

        // Verify both rows were persisted.
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT endpoint_alias, ffmpeg_media_time_ms, wall_clock_ms
             FROM ffmpeg_progress_samples WHERE event_id = ?1
             ORDER BY ffmpeg_media_time_ms ASC",
        )
        .bind(event_id)
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "yt1");
        assert_eq!(rows[0].1, 5000);
        assert_eq!(rows[0].2, 1_700_000_000_000_i64);
        assert_eq!(rows[1].1, 7000);
    }

    /// Cursor advances: second call with cursor=2 returns empty, cursor stays 2.
    #[tokio::test]
    async fn poll_once_cursor_does_not_regress_on_empty_response() {
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        let event_id = rs_core::db::create_streaming_event(&pool, "poll-cursor-evt")
            .await
            .unwrap();

        let app = axum::Router::new().route(
            "/api/status",
            get(|| async {
                axum::Json(serde_json::json!({
                    "status": "ok",
                    "endpoint_count": 0,
                    "endpoints": [],
                    "recent_progress": [],
                    "next_progress_cursor": 0_i64
                }))
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let new_cursor = poll_once(
            &pool,
            event_id,
            &format!("http://{addr}"),
            "test-token",
            5, // start with cursor=5
        )
        .await
        .unwrap();

        // Empty response — cursor must not regress.
        assert_eq!(new_cursor, 5);
    }
}
