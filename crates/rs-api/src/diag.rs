//! Diagnostic dump endpoint for stream.snv.
//! See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
//! Issue #176.

use axum::Json;
use axum::extract::State;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use sqlx::SqlitePool;

use crate::state::AppState;

/// Source-of-truth abstraction for `build_dump`. Production uses
/// `ProductionSources`; unit tests use `MockSources`.
pub trait DumpSources: Send + Sync {
    fn pool(&self) -> &SqlitePool;
    fn current_event_id(&self) -> Option<i64>;
    fn vps_state(&self) -> BoxFuture<'_, (Value, Value)>;
}

pub struct ProductionSources {
    pub pool: SqlitePool,
    pub event_id: Option<i64>,
    pub vps_url: Option<String>,
}

impl DumpSources for ProductionSources {
    fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    fn current_event_id(&self) -> Option<i64> {
        self.event_id
    }
    fn vps_state(&self) -> BoxFuture<'_, (Value, Value)> {
        Box::pin(async move {
            let Some(url) = &self.vps_url else {
                return (
                    json!({ "error": "no VPS configured" }),
                    json!({ "error": "no VPS configured" }),
                );
            };
            let client = reqwest::Client::new();
            match client
                .get(format!("{url}/api/v1/delivery/status"))
                .send()
                .await
            {
                Ok(r) => match r.json::<Value>().await {
                    Ok(v) => (
                        v.get("disk_cache_stats")
                            .cloned()
                            .unwrap_or_else(|| json!({ "error": "missing in vps response" })),
                        v.get("s3_fetch_profile")
                            .cloned()
                            .unwrap_or_else(|| json!({ "error": "missing in vps response" })),
                    ),
                    Err(e) => (
                        json!({ "error": format!("decode: {e}") }),
                        json!({ "error": format!("decode: {e}") }),
                    ),
                },
                Err(e) => (
                    json!({ "error": format!("vps unreachable: {e}") }),
                    json!({ "error": format!("vps unreachable: {e}") }),
                ),
            }
        })
    }
}

async fn fetch_audit_60min(pool: &SqlitePool) -> Vec<Value> {
    let rows: Result<Vec<(i64, String, String, String, String)>, _> = sqlx::query_as(
        "SELECT id, ts, severity, action, detail FROM audit_log \
         WHERE ts > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-60 minutes') \
         ORDER BY id DESC LIMIT 5000",
    )
    .fetch_all(pool)
    .await;
    rows.map(|rs| {
        rs.into_iter()
            .map(|(id, ts, sev, action, detail)| {
                json!({
                    "id": id,
                    "ts": ts,
                    "severity": sev,
                    "action": action,
                    "detail": serde_json::from_str::<Value>(&detail).unwrap_or_else(|_| Value::String(detail))
                })
            })
            .collect()
    })
    .unwrap_or_default()
}

async fn fetch_endpoint_timeline(pool: &SqlitePool, event_id: Option<i64>) -> Value {
    let Some(eid) = event_id else {
        return json!({});
    };
    let cutoff_ms = chrono::Utc::now()
        .timestamp_millis()
        .saturating_sub(60 * 60 * 1000);
    let rows: Result<Vec<(String, i64, i64, i64, f64, i64)>, _> = sqlx::query_as(
        "SELECT alias, ts_ms, current_chunk_id, chunks_processed, chunk_delay_secs, bytes_processed_total \
         FROM delivery_endpoint_metrics \
         WHERE event_id = ? AND ts_ms >= ? \
         ORDER BY ts_ms ASC",
    )
    .bind(eid)
    .bind(cutoff_ms)
    .fetch_all(pool)
    .await;
    let rows = rows.unwrap_or_default();
    let mut by_alias: std::collections::BTreeMap<String, Vec<Value>> =
        std::collections::BTreeMap::new();
    for (alias, ts_ms, chunk_id, processed, delay, bytes) in rows {
        by_alias.entry(alias).or_default().push(json!({
            "ts_ms": ts_ms,
            "current_chunk_id": chunk_id,
            "chunks_processed": processed,
            "chunk_delay_secs": delay,
            "bytes_processed_total": bytes,
        }));
    }
    serde_json::to_value(by_alias).unwrap_or_else(|_| json!({}))
}

pub async fn build_dump<S: DumpSources>(sources: &S) -> Value {
    let event_id = sources.current_event_id();
    let pool = sources.pool();
    let audit_60min = fetch_audit_60min(pool).await;
    let timeline = fetch_endpoint_timeline(pool, event_id).await;
    let (disk_cache_stats, s3_fetch_profile) = sources.vps_state().await;
    json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
        "event_id": event_id,
        "audit_60min": audit_60min,
        "endpoint_timeline": timeline,
        "disk_cache_stats": disk_cache_stats,
        "s3_fetch_profile": s3_fetch_profile,
    })
}

pub async fn diag_dump_handler(State(state): State<AppState>) -> Json<Value> {
    // Resolve current event + VPS URL from AppState. The exact accessors
    // depend on AppState's API; if `current_event_id()` and
    // `current_vps_url()` aren't present, fall back to None and the dump
    // still works (audit + timeline still populate).
    let event_id = current_event_id_from_state(&state).await;
    let vps_url = current_vps_url_from_state(&state).await;
    let sources = ProductionSources {
        pool: state.pool.clone(),
        event_id,
        vps_url,
    };
    Json(build_dump(&sources).await)
}

/// Best-effort accessor: returns the most-recent active streaming event
/// id, or `None`. Implementation reads from the DB directly to avoid
/// depending on AppState methods that may differ between versions.
async fn current_event_id_from_state(state: &AppState) -> Option<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT id FROM streaming_events WHERE delivering_activated = 1 \
         ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten()
}

/// Best-effort accessor: returns the URL of the most-recent active
/// delivery instance (`http://<ipv4>:8920`), or `None`.
async fn current_vps_url_from_state(state: &AppState) -> Option<String> {
    let row: Result<Option<(String,)>, _> = sqlx::query_as(
        "SELECT ipv4 FROM delivery_instances \
         WHERE status IN ('delivering','running','ready') \
         ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&state.pool)
    .await;
    row.ok().flatten().map(|(ip,)| format!("http://{ip}:8920"))
}

#[cfg(test)]
pub(crate) struct MockSources {
    pool: SqlitePool,
    event_id: Option<i64>,
    vps_unreachable: bool,
}

#[cfg(test)]
impl MockSources {
    pub async fn full() -> Self {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                severity TEXT NOT NULL,
                source TEXT NOT NULL,
                event_id INTEGER,
                instance_id INTEGER,
                endpoint TEXT,
                action TEXT NOT NULL,
                detail TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE delivery_endpoint_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER NOT NULL,
                instance_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                alias TEXT NOT NULL,
                alive INTEGER NOT NULL,
                current_chunk_id INTEGER NOT NULL,
                chunks_processed INTEGER NOT NULL,
                chunk_delay_secs REAL NOT NULL,
                bytes_processed_total INTEGER NOT NULL,
                ffmpeg_restart_count INTEGER NOT NULL,
                delivery_mode TEXT
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO audit_log (ts, severity, source, action, detail) \
             VALUES (strftime('%Y-%m-%dT%H:%M:%fZ','now','-5 minutes'), 'info', 'operator', 'EndpointStarted', '{\"alias\":\"YT\"}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        Self {
            pool,
            event_id: Some(9289),
            vps_unreachable: false,
        }
    }
    pub async fn vps_unreachable() -> Self {
        let mut s = Self::full().await;
        s.vps_unreachable = true;
        s
    }
}

#[cfg(test)]
impl DumpSources for MockSources {
    fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    fn current_event_id(&self) -> Option<i64> {
        self.event_id
    }
    fn vps_state(&self) -> BoxFuture<'_, (Value, Value)> {
        let unreachable = self.vps_unreachable;
        Box::pin(async move {
            if unreachable {
                (
                    json!({ "error": "vps unreachable: simulated" }),
                    json!({ "error": "vps unreachable: simulated" }),
                )
            } else {
                (
                    json!({ "in_flight": 0, "cached_chunks": 120 }),
                    json!({
                        "count": 1234,
                        "bytes_total": 1_000_000_000u64,
                        "p50_latency_ms": 45,
                        "p99_latency_ms": 320,
                        "fail_count_by_class": {}
                    }),
                )
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_dump_with_full_sources_returns_complete_json() {
        let sources = MockSources::full().await;
        let dump = build_dump(&sources).await;
        assert!(dump["generated_at"].is_string());
        assert!(dump["audit_60min"].is_array());
        assert!(dump["endpoint_timeline"].is_object());
        assert!(dump["disk_cache_stats"].is_object());
        assert!(dump["s3_fetch_profile"].is_object());
        assert_eq!(dump["event_id"], 9289);
    }

    #[tokio::test]
    async fn build_dump_with_vps_unreachable_returns_partial() {
        let sources = MockSources::vps_unreachable().await;
        let dump = build_dump(&sources).await;
        // Failed sub-section replaced with { "error": "..." } per spec §7.
        assert!(dump["disk_cache_stats"]["error"].is_string());
        assert!(dump["s3_fetch_profile"]["error"].is_string());
        // Other sections still populated.
        assert!(dump["audit_60min"].is_array());
    }
}
