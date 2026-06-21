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

use std::sync::OnceLock;
use std::time::{Duration, Instant};

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
    /// Current signed content-PTS A/V skew in ms (positive = audio behind
    /// video) for rust-pusher endpoints. Read from VPS `/api/status` JSON
    /// (defaults to 0 if the VPS rs-delivery is older than this field). The
    /// operator dashboard alarms on a sustained non-zero value; the #258 E2E
    /// gate asserts it stays ~0 (issue #257).
    #[serde(default)]
    pub av_skew_ms: i64,
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

/// Display cap (seconds) for a FAST endpoint's per-endpoint delay number.
/// Fast endpoints (`delivery_delay == 0`) are meant to be near-live and, after
/// an outage, jump back to the live edge (see `producer_lag`). Their "buffer
/// above the consumer" can momentarily spike (a burst of fresh chunks lands
/// before the next poll reads `current_chunk_id`), so the raw value is
/// meaningless as a steady-state number. Cap it at a small constant so the
/// dashboard shows a bounded, sane figure instead of a 7800s ghost.
const FAST_ENDPOINT_DELAY_CAP_SECS: f64 = 30.0;

/// Multiplier for the DELAYED-endpoint per-endpoint delay cap. Mirrors the
/// global pipeline cap at `lib.rs` (`target_delay * 1.5`): 1.5× leaves room to
/// surface a genuinely oversized cache without surfacing the entire historical
/// S3 backlog when `current_chunk_id` briefly reads 0 (Stop+Start, VPS spin-up,
/// pre-first-push prefill). See #187.
const DELAYED_ENDPOINT_DELAY_CAP_MULT: f64 = 1.5;

/// Bound the per-endpoint `chunk_delay_secs` shown on the dashboard.
///
/// The raw value from `get_cache_duration_secs` is uncapped — it sums every
/// sent chunk above the endpoint's read position, which can be the entire S3
/// backlog (e.g. 7800s) when the endpoint is behind or `current_chunk_id`
/// reads 0. The GLOBAL pipeline value is already capped at `target * 1.5`
/// (lib.rs), but the per-endpoint value was not — so individual endpoints
/// showed nonsensical huge numbers.
///
/// - **Fast endpoints** (`delivery_delay_secs == 0`): a `target * 1.5` cap
///   would be `0`, which is wrong. Cap at `FAST_ENDPOINT_DELAY_CAP_SECS`.
/// - **Delayed endpoints**: cap at `target_delay_secs * 1.5`, mirroring the
///   global cap. A zero/garbage target falls back to the fast cap so the
///   number can never blow up.
pub(crate) fn cap_endpoint_delay_secs(raw: f64, is_fast: bool, target_delay_secs: u64) -> f64 {
    let raw = raw.max(0.0);
    if is_fast || target_delay_secs == 0 {
        raw.min(FAST_ENDPOINT_DELAY_CAP_SECS)
    } else {
        raw.min(target_delay_secs as f64 * DELAYED_ENDPOINT_DELAY_CAP_MULT)
    }
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

        // Per-event target delay (seconds) for the delayed-endpoint display cap.
        // Honor the per-event cache_delay_secs override; fall back to the
        // configured default. Same resolution as lib.rs / delivery.rs.
        let target_delay_secs: u64 = db::get_streaming_event_by_id(self.pool(), event_id)
            .await
            .ok()
            .flatten()
            .and_then(|ev| ev.cache_delay_secs.map(|s| s as u64))
            .unwrap_or(self.config().delivery.delivery_delay_secs);

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
                            // #257: content-PTS A/V skew (signed; positive =
                            // audio behind video). 0 when the VPS rs-delivery
                            // predates this field.
                            let av_skew_ms = entry["av_skew_ms"].as_i64().unwrap_or(0);
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
                            //
                            // The raw value is UNCAPPED — a fast/behind endpoint (or one
                            // reading current_chunk_id=0) sums the whole S3 backlog and shows
                            // a 7800s ghost. The global pipeline value is capped at
                            // target*1.5 in lib.rs; mirror that per-endpoint. Fast endpoints
                            // (target=0) get a small constant cap instead of 1.5*0=0.
                            let is_fast = fast_map.get(&alias).copied().unwrap_or(false);
                            let chunk_delay_secs = cap_endpoint_delay_secs(
                                db::get_cache_duration_secs(self.pool(), event_id, chunk_id)
                                    .await
                                    .unwrap_or(0.0),
                                is_fast,
                                target_delay_secs,
                            );

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
                                av_skew_ms,
                                last_error,
                                ffmpeg_last_stderr,
                                is_fast,
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

        // Load endpoint_configs once so we can look up youtube_oauth_id by alias.
        let configs = rs_core::db::list_endpoint_configs(self.pool())
            .await
            .unwrap_or_default();
        let mut metrics: Vec<DeliveryEndpointMetrics> = Vec::with_capacity(status.endpoints.len());
        for ep in status.endpoints.into_iter() {
            let mut m = DeliveryEndpointMetrics {
                alias: ep.alias,
                alive: ep.alive,
                current_chunk_id: ep.current_chunk_id,
                bytes_processed_total: ep.bytes_processed_total,
                chunks_processed: ep.chunks_processed,
                chunk_delay_secs: ep.chunk_delay_secs,
                stall_reason: ep.stall_reason,
                ffmpeg_restart_count: ep.ffmpeg_restart_count,
                reconnect_count: ep.reconnect_count,
                av_skew_ms: ep.av_skew_ms,
                last_error: ep.last_error,
                is_fast: ep.is_fast,
                delivery_mode: ep.delivery_mode,
                rescue_eta_secs: ep.rescue_eta_secs,
                youtube_health: None,
                lifecycle: rs_core::models::EndpointLifecycle::Live,
            };
            if let Some(cfg) = configs.iter().find(|c| c.alias == m.alias) {
                if cfg.youtube_oauth_id.is_some() && cfg.service_type == "YT_RTMP" {
                    attach_yt_health_cached(self.pool(), cfg, &mut m, self.audit_tx()).await;
                }
            }
            // Host-compute the operator-facing lifecycle from the metrics we
            // just assembled (outage = blue/buffering, auth/disk = red).
            m.lifecycle =
                rs_core::models::EndpointLifecycle::compute(&rs_core::models::LifecycleInput {
                    alive: m.alive,
                    chunks_processed: m.chunks_processed,
                    delivery_mode: m.delivery_mode.clone(),
                    stall_reason: m.stall_reason.clone(),
                    last_error: m.last_error.clone(),
                    disk_critical: self.disk_critical(),
                });
            metrics.push(m);
        }

        let endpoint_count = metrics.len() as u32;
        Ok((name, inst_status, server_ip, endpoint_count, metrics))
    }
}

/// Fetch YT `liveStreams.list` for the endpoint's linked OAuth label,
/// find the stream whose `cdn.ingestionInfo.streamName` matches the
/// endpoint's `stream_key`, and attach `YoutubeHealth` to `metrics`.
///
/// Errors are mapped to `YoutubeHealth.error` (never propagated) so the
/// probe never breaks the surrounding monitor loop.
pub async fn attach_yt_health(
    pool: &sqlx::SqlitePool,
    endpoint: &rs_core::models::EndpointConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
) {
    use rs_core::models::YoutubeHealth;

    let Some(oauth_id) = endpoint.youtube_oauth_id else {
        return;
    };
    let label = match rs_core::db::youtube_oauth::get_oauth_by_id(pool, oauth_id).await {
        Ok(Some(o)) => o.label,
        Ok(None) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_missing".into()),
            });
            return;
        }
        Err(e) => {
            tracing::warn!(error = %e, endpoint_id = endpoint.id, oauth_id = oauth_id, "yt_health: db lookup failed");
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("db_error".into()),
            });
            return;
        }
    };

    // Spec §11: per-project quota tracker. `liveStreams.list` costs 1 unit;
    // when budget is exhausted, skip the probe and surface `quota_throttled`
    // so the dashboard shows the operator the project hit Google's daily cap.
    if youtube_quota_tracker().acquire(1).is_err() {
        metrics.youtube_health = Some(YoutubeHealth {
            stream_status: "unknown".into(),
            health_status: "unknown".into(),
            top_issue: None,
            resolution: None,
            frame_rate: None,
            age_secs: 0,
            error: Some("quota_throttled".into()),
        });
        return;
    }

    match rs_youtube::streams::list_streams_for_label(pool, &label).await {
        Ok(streams) => {
            let bound = streams.iter().find(|s| {
                s.cdn
                    .as_ref()
                    .and_then(|c| c.ingestion_info.as_ref())
                    .and_then(|i| i.stream_name.as_deref())
                    == Some(endpoint.stream_key.as_str())
            });
            metrics.youtube_health = Some(match bound {
                Some(s) => crate::delivery_yt_health::extract_top_issue(s),
                None => YoutubeHealth {
                    stream_status: "unbound".into(),
                    health_status: "n/a".into(),
                    top_issue: None,
                    resolution: None,
                    frame_rate: None,
                    age_secs: 0,
                    error: Some("stream_not_in_mine_list".into()),
                },
            });
        }
        Err(rs_youtube::YouTubeError::TokenExpired(_)) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_invalid".into()),
            });
        }
        Err(rs_youtube::YouTubeError::Api { status: 403, .. }) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_app_not_production".into()),
            });
        }
        Err(rs_youtube::YouTubeError::Api { status: 429, .. }) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("quota_exceeded".into()),
            });
        }
        Err(e) => {
            tracing::warn!(label = %label, error = %e, "yt_health probe failed");
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("probe_error".into()),
            });
        }
    }
}

/// Test-only: clear the per-endpoint YT health cache. Different tests use
/// pools that allocate endpoint id=1 each, so the cache must be reset to
/// avoid one test seeing another's cached snapshot.
#[cfg(test)]
pub fn clear_yt_health_cache_for_test() {
    yt_health_cache().clear();
}

/// Per-project YouTube Data API quota tracker. Single global instance keyed
/// by `daily_quota` from `youtube.device_flow` config (default 10_000).
/// `acquire(1)` is called before every `liveStreams.list` probe in
/// `attach_yt_health`. Exhausted budget → `error: "quota_throttled"`.
fn youtube_quota_tracker() -> &'static rs_youtube::quota::QuotaTracker {
    static T: OnceLock<rs_youtube::quota::QuotaTracker> = OnceLock::new();
    T.get_or_init(|| rs_youtube::quota::QuotaTracker::new(10_000))
}

fn yt_health_cache() -> &'static dashmap::DashMap<i64, (Instant, rs_core::models::YoutubeHealth)> {
    static C: OnceLock<dashmap::DashMap<i64, (Instant, rs_core::models::YoutubeHealth)>> =
        OnceLock::new();
    C.get_or_init(dashmap::DashMap::new)
}

/// Adaptive cache TTL for the YT health probe. 60s when both `health_status`
/// is `good` AND no `top_issue` is set AND no `error` is present; 15s otherwise.
/// Spec section 5.
pub fn ttl_for_health(h: &rs_core::models::YoutubeHealth) -> Duration {
    if h.health_status == "good" && h.top_issue.is_none() && h.error.is_none() {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(15)
    }
}

/// Adaptive-TTL minimum interval per endpoint id (see `ttl_for_health`).
/// Returns the cached value (with refreshed `age_secs`) if still fresh;
/// otherwise calls `attach_yt_health` and stores the result.
pub async fn attach_yt_health_cached(
    pool: &sqlx::SqlitePool,
    endpoint: &rs_core::models::EndpointConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
    audit_tx: Option<&tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>>,
) {
    // Capture prior top_issue BEFORE the freshness short-circuit so we can
    // emit the audit transition only on the slow path.
    let prior_issue: Option<String> = yt_health_cache()
        .get(&endpoint.id)
        .and_then(|e| e.value().1.top_issue.clone());

    if let Some(entry) = yt_health_cache().get(&endpoint.id) {
        let (when, h) = entry.value().clone();
        let age = when.elapsed();
        if age < ttl_for_health(&h) {
            let mut h_aged = h;
            h_aged.age_secs = age.as_secs() as i64;
            metrics.youtube_health = Some(h_aged);
            return;
        }
    }
    attach_yt_health(pool, endpoint, metrics).await;
    if let Some(h) = metrics.youtube_health.as_ref() {
        yt_health_cache().insert(endpoint.id, (Instant::now(), h.clone()));
        if let Some(tx) = audit_tx {
            let _ = crate::delivery_yt_health::record_and_maybe_emit(
                prior_issue.as_deref(),
                h.top_issue.as_deref(),
                &endpoint.alias,
                tx,
            )
            .await;
        }
    }
}
