//! Delivery status types + status-assembly helpers.
//!
//! Split from `delivery.rs` to keep that file under the 1000-line file-size
//! gate. Contains:
//! - `EndpointRestartRecord`, `EndpointDeliveryStatus`, `DeliveryStatus`,
//!   `YouTubeStatus` types.
//! - `pick_last_error_line_inline` — stderr last-error extractor.
//! - `load_restart_history_from_db` — post-mortem history loader.
//! - `DeliveryOrchestrator::get_delivery_status` and `poll_delivery_metrics`
//!   (second impl block).

use std::time::Duration;

use tracing::{info, warn};

use rs_core::db;
use rs_core::models::DeliveryEndpointMetrics;

use crate::delivery::DeliveryOrchestrator;
use crate::delivery_helpers::is_delivery_active;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndpointRestartRecord {
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: u64,
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: u64,
    /// Extracted last error-looking line from `stderr_tail`. Populated only
    /// when the record is sourced from the local DB (post-mortem path) to
    /// spare the dashboard from re-parsing stderr every render.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_last_error_line: Option<String>,
}

/// Mirror of `rs_delivery::ffmpeg_reason::pick_last_error_line` to avoid a
/// cross-crate dependency cycle. Keep logic identical.
pub(crate) fn pick_last_error_line_inline(stderr_tail: &str) -> Option<String> {
    stderr_tail
        .lines()
        .rev()
        .filter(|l| {
            let l = l.trim();
            !l.is_empty()
                && !l.starts_with("size=")
                && !l.starts_with("frame=")
                && !l.starts_with("ffmpeg version ")
                && !l.starts_with("  built with ")
                && !l.starts_with("  configuration: ")
                && !l.starts_with("  lib")
        })
        .find(|l| {
            let l = l.to_ascii_lowercase();
            l.contains("error")
                || l.contains("broken pipe")
                || l.contains("fatal")
                || l.contains("invalid")
                || l.contains("failed")
                || l.contains("timeout")
        })
        .map(|s| s.trim().to_string())
}

/// Load the most recent `limit` restart records for an endpoint from the
/// local `delivery_restart_log` table. Newest row first. The DB is the
/// durable source of truth for post-mortem analysis — the VPS may be
/// unreachable or rebuilt, but the host keeps the history.
pub(crate) async fn load_restart_history_from_db(
    pool: &sqlx::SqlitePool,
    instance_id: i64,
    alias: &str,
    limit: i64,
) -> Vec<EndpointRestartRecord> {
    use sqlx::Row;
    let rows = match sqlx::query(
        "SELECT timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs, stderr_tail
         FROM delivery_restart_log
         WHERE instance_id = ?1 AND alias = ?2
         ORDER BY timestamp_ms DESC
         LIMIT ?3",
    )
    .bind(instance_id)
    .bind(alias)
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            warn!("Failed to load restart history from DB: {e}");
            return Vec::new();
        }
    };
    rows.into_iter()
        .map(|row| {
            let stderr_tail: Option<String> = row.try_get("stderr_tail").ok().flatten();
            let stderr_last_error_line =
                stderr_tail.as_deref().and_then(pick_last_error_line_inline);
            EndpointRestartRecord {
                timestamp_ms: row.get("timestamp_ms"),
                chunk_id: row.get("chunk_id"),
                lifetime_secs: row.get::<i64, _>("lifetime_secs") as u64,
                reason: row.get("reason"),
                stderr_tail,
                backoff_secs: row.get::<i64, _>("backoff_secs") as u64,
                stderr_last_error_line,
            }
        })
        .collect()
}

#[derive(Debug, serde::Serialize)]
pub struct EndpointDeliveryStatus {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub stall_reason: Option<String>,
    pub ffmpeg_restart_count: u32,
    /// Rust-pusher reconnect counter. Companion to `ffmpeg_restart_count`
    /// for the rust pusher path. Read from VPS `/api/status` JSON
    /// (defaults to 0 if VPS rs-delivery is older than this field).
    /// Issue #172.
    #[serde(default)]
    pub reconnect_count: u32,
    pub last_error: Option<String>,
    pub ffmpeg_last_stderr: Option<String>,
    pub is_fast: bool,
    /// Per-endpoint audit log of recent ffmpeg restarts (capped at 100).
    /// Empty when the rs-delivery binary on the VPS is older than this
    /// field's introduction.
    #[serde(default)]
    pub restart_history: Vec<EndpointRestartRecord>,
    /// Current delivery mode: "normal", "warmup", "rescue", or "recovering".
    /// None when the rs-delivery binary on the VPS is older than the
    /// rescue-mode feature.
    #[serde(default)]
    pub delivery_mode: Option<String>,
    /// ETA in seconds until rescue mode exits. None when not in rescue mode.
    #[serde(default)]
    pub rescue_eta_secs: Option<u64>,
}

/// Result of querying delivery status.
#[derive(Debug, serde::Serialize)]
pub struct DeliveryStatus {
    pub instance: Option<rs_core::models::DeliveryInstance>,
    pub server_ready: bool,
    pub endpoints: Vec<EndpointDeliveryStatus>,
}

/// Result of querying YouTube status.
#[derive(Debug, serde::Serialize)]
pub struct YouTubeStatus {
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub error: Option<String>,
}

impl DeliveryOrchestrator {
    /// Get delivery status for an event.
    pub async fn get_delivery_status(&self, event_id: i64) -> anyhow::Result<DeliveryStatus> {
        let instance = db::get_delivery_instance_by_event(self.pool(), event_id).await?;

        // Read cached is_fast map (populated in init_endpoints, empty before init)
        let fast_map = {
            let cache = self.endpoint_fast_cache_lock().await;
            cache.get(&event_id).cloned().unwrap_or_default()
        };

        let (server_ready, endpoints) = match &instance {
            Some(inst) if is_delivery_active(&inst.status) => {
                // Fetch live status from rs-delivery
                let delivery_url = format!("http://{}:8000", inst.ipv4);
                let client = reqwest::Client::new();

                match client
                    .get(format!("{delivery_url}/api/status"))
                    .bearer_auth(&inst.auth_token)
                    .timeout(Duration::from_secs(5))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        let body: serde_json::Value = resp.json().await.unwrap_or_default();
                        let ep_entries = body["endpoints"].as_array().cloned().unwrap_or_default();

                        let mut statuses = Vec::new();
                        for entry in ep_entries {
                            let alias = entry["alias"].as_str().unwrap_or("").to_string();
                            let alive = entry["alive"].as_bool().unwrap_or(false);
                            let chunk_id = entry["current_chunk_id"].as_i64().unwrap_or(0);
                            let bytes_total = entry["bytes_processed_total"].as_i64().unwrap_or(0);
                            let chunks_processed = entry["chunks_processed"].as_i64().unwrap_or(0);
                            let stall_reason =
                                entry["stall_reason"].as_str().map(|s| s.to_string());
                            let ffmpeg_restart_count =
                                entry["ffmpeg_restart_count"].as_u64().unwrap_or(0) as u32;
                            let reconnect_count =
                                entry["reconnect_count"].as_u64().unwrap_or(0) as u32;
                            let last_error = entry["last_error"].as_str().map(|s| s.to_string());
                            let ffmpeg_last_stderr =
                                entry["ffmpeg_last_stderr"].as_str().map(|s| s.to_string());
                            let delivery_mode =
                                entry["delivery_mode"].as_str().map(|s| s.to_string());
                            let rescue_eta_secs = entry["rescue_eta_secs"].as_u64();
                            let restart_history_from_vps: Vec<EndpointRestartRecord> =
                                entry["restart_history"]
                                    .as_array()
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|v| {
                                                serde_json::from_value::<EndpointRestartRecord>(
                                                    v.clone(),
                                                )
                                                .ok()
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();

                            // Persist restart records to DB for post-mortem analysis.
                            // The dedup INSERT ignores records already saved from previous polls.
                            for record in &restart_history_from_vps {
                                if let Err(e) = db::insert_delivery_restart_record(
                                    self.pool(),
                                    inst.id,
                                    inst.event_id,
                                    &alias,
                                    record.timestamp_ms,
                                    record.chunk_id,
                                    record.lifetime_secs as i64,
                                    &record.reason,
                                    record.stderr_tail.as_deref(),
                                    record.backoff_secs as i64,
                                )
                                .await
                                {
                                    warn!("Failed to persist restart record: {e}");
                                }
                            }

                            // Source restart_history from the DB (durable local store)
                            // rather than the VPS response. The VPS can be rebuilt or
                            // unreachable, but the host keeps every record we've ever
                            // ingested. This is the post-mortem-grade source of truth.
                            let restart_history =
                                load_restart_history_from_db(self.pool(), inst.id, &alias, 10)
                                    .await;

                            // Per-endpoint cache delay = "buffer above the consumer position",
                            // measured by summing duration_ms of chunks above chunk_id. Gives
                            // each endpoint its own value so the dashboard surfaces drift on
                            // individual endpoints (regression test at
                            // e2e/frontend.spec.ts:994).
                            //
                            // Note: the per-endpoint "lag FROM live edge" alternative (helper
                            // `get_endpoint_lag_secs`, kept for future use) was tried in #189
                            // but live testing on streamsnv 2026-05-11 showed the VPS reports
                            // current_chunk_id tracking the FETCHER (near live edge) so the
                            // lag reads ~0s right after first push — visually worse than the
                            // 1-tick "buffer above" artifact. The 1726s ghost from #187 is
                            // already fixed by the 1.5x cap at delivery.rs (commit b928376).
                            let chunk_delay_secs =
                                db::get_cache_duration_secs(self.pool(), event_id, chunk_id)
                                    .await
                                    .unwrap_or(0.0);

                            // Update DB with latest status
                            if let Err(e) = db::upsert_delivery_endpoint_status(
                                self.pool(),
                                inst.id,
                                &alias,
                                alive,
                                chunks_processed,
                                chunk_id,
                                bytes_total,
                            )
                            .await
                            {
                                warn!("Failed to update endpoint status: {e}");
                            }

                            statuses.push(EndpointDeliveryStatus {
                                alias: alias.clone(),
                                alive,
                                current_chunk_id: chunk_id,
                                bytes_processed_total: bytes_total,
                                chunks_processed,
                                chunk_delay_secs,
                                stall_reason,
                                ffmpeg_restart_count,
                                reconnect_count,
                                last_error,
                                ffmpeg_last_stderr,
                                is_fast: fast_map.get(&alias).copied().unwrap_or(false),
                                restart_history,
                                delivery_mode,
                                rescue_eta_secs,
                            });
                        }

                        db::update_delivery_instance_health(self.pool(), inst.id)
                            .await
                            .ok();

                        (true, statuses)
                    }
                    Ok(resp) => {
                        warn!(
                            status = %resp.status(),
                            "Delivery status check returned non-success"
                        );
                        (false, Vec::new())
                    }
                    Err(e) => {
                        warn!("Delivery status check failed: {e}");
                        (false, Vec::new())
                    }
                }
            }
            Some(inst) => {
                info!(
                    status = %inst.status,
                    "Delivery instance not in running state"
                );
                (false, Vec::new())
            }
            _ => (false, Vec::new()),
        };

        Ok(DeliveryStatus {
            instance,
            server_ready,
            endpoints,
        })
    }

    /// Poll delivery metrics and return data suitable for WsEvent broadcast.
    /// Returns (instance_name, status, server_ip, endpoint_count, Vec<DeliveryEndpointMetrics>).
    pub async fn poll_delivery_metrics(
        &self,
        event_id: i64,
    ) -> anyhow::Result<(
        String,
        String,
        Option<String>,
        u32,
        Vec<DeliveryEndpointMetrics>,
    )> {
        let status = self.get_delivery_status(event_id).await?;

        let (name, inst_status, server_ip) = match &status.instance {
            Some(inst) => (
                inst.name.clone(),
                inst.status.clone(),
                Some(inst.ipv4.clone()),
            ),
            None => ("none".to_string(), "none".to_string(), None),
        };

        let metrics: Vec<DeliveryEndpointMetrics> = status
            .endpoints
            .into_iter()
            .map(|ep| DeliveryEndpointMetrics {
                alias: ep.alias,
                alive: ep.alive,
                current_chunk_id: ep.current_chunk_id,
                bytes_processed_total: ep.bytes_processed_total,
                chunks_processed: ep.chunks_processed,
                chunk_delay_secs: ep.chunk_delay_secs,
                stall_reason: ep.stall_reason,
                ffmpeg_restart_count: ep.ffmpeg_restart_count,
                reconnect_count: ep.reconnect_count,
                last_error: ep.last_error,
                is_fast: ep.is_fast,
                delivery_mode: ep.delivery_mode,
                rescue_eta_secs: ep.rescue_eta_secs,
            })
            .collect();

        let endpoint_count = metrics.len() as u32;
        Ok((name, inst_status, server_ip, endpoint_count, metrics))
    }
}
