//! Typed audit log with fire-and-forget write API.
//!
//! Callers invoke `record()` which pushes into a bounded `mpsc` channel.
//! A dedicated `audit_writer_task` drains the channel, batches INSERTs
//! into `audit_log`, and broadcasts `WsEvent::AuditAppended` to clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

use crate::models::WsEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Operator,
    Inpoint,
    Uploader,
    Delivery,
    Vps,
    Ffmpeg,
    S3,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    EventStarted,
    EventStopped,
    DeliveryStarted,
    DeliveryStopped,
    EndpointAdded,
    EndpointRemoved,
    S3Cleared,
    ConfigChanged,
    RtmpConnected,
    RtmpDisconnected,
    RtmpHandshakeFailed,
    VpsCreating,
    VpsReady,
    VpsDeleted,
    VpsUnreachable,
    DeliveryInitSent,
    DeliveryInitResponse,
    EndpointStarted,
    EndpointAliveTransition,
    EndpointFfmpegDied,
    EndpointFfmpegRestartFailed,
    /// Rust RTMP pusher (`PusherKind::Rust`) lost its session and is
    /// reconnecting. Distinct from `EndpointFfmpegDied` so the dashboard
    /// can render the correct icon/label for the rust path — operators
    /// looking at the activity feed should immediately see "rust pusher
    /// reconnected" rather than the misleading "ffmpeg died" (#103).
    EndpointRtmpPushDied,
    S3UploadFailed,
    S3FetchFailed,
    /// Disk cache started pre-filling for an event. Emitted on first
    /// EndpointReader registration. Issue #174.
    DiskCachePrefillStarted,
    /// Disk cache window is fully populated for at least one endpoint;
    /// the first push is imminent.
    DiskCachePrefillReady,
    /// Rate-limited summary (1/min): number of chunks evicted by
    /// EvictionTask. Useful for spotting churn.
    DiskCacheChunkEvicted,
    /// DownloadService bandwidth cap reached; sustained S3 latency
    /// expected. Operator may want to investigate Hetzner status.
    DiskCacheDownloadThrottled,
    /// EndpointReader.wait_for_chunk timed out (default 60 s).
    /// Indicates a real S3 outage longer than the cache window.
    DiskCacheStallTimeout,
    /// Disk write failed (ENOSPC / EIO). Severity::Error.
    DiskCacheWriteFailed,
    /// Reader pushed successfully after a stall; the cache absorbed
    /// the transient. Pair with DiskCacheStallTimeout to bound outage
    /// duration in the audit log.
    DiskCacheReaderRecovered,
    /// Per-endpoint push sample emitted by EndpointReader on chunk push.
    /// Rate-limited 1/min/endpoint via RateLimiter keyed by
    /// (DiskCachePushSample, endpoint_alias). Carries chunk_supply_lag_ms,
    /// inter_chunk_gap_ms, burst_factor, current_chunk_delay_secs, and
    /// delivery_delay_secs target. Issue #176.
    DiskCachePushSample,
    RestreamerStarted,
    MigrationsApplied,
    /// DB write that drives UI state failed. Emitted when e.g.
    /// `delivering_activated` couldn't be flipped — the backend keeps
    /// running but the dashboard will show stale state until the next
    /// successful poll. Operators watching the audit feed can tell
    /// "dashboard is wrong, backend is right" without paging anyone.
    DbUiFlagStale,
    /// Host (stream.snv) lost internet egress. Emitted by `internet_probe`
    /// when N consecutive HEAD probes to a known stable URL fail.
    /// Operators should distinguish "host LAN/ISP flake" from
    /// "VPS code regression" -- this row is the former. Issue #176.
    HostInternetUnreachable,
    /// Host (stream.snv) recovered internet egress after a previous
    /// `HostInternetUnreachable`. Emitted on first successful probe
    /// after a stretch of failures. Issue #176.
    HostInternetRecovered,
    /// Per-chunk lifecycle steady-state sample emitted every Nth chunk
    /// per endpoint (default N=30). Carries the 5 inter-stage gaps
    /// (A->B through E->F) + worst-stage label. Severity::Info.
    /// Counter-based sampling owned by LifecycleSampler — NOT routed
    /// through the shared RateLimiter.
    DiskCacheLifecycleSample,
    /// Single chunk where any one stage gap exceeded the breach threshold
    /// (default 4_000ms = 2x chunk_duration). Severity::Warn; per-endpoint
    /// 5s window owned by LifecycleSampler (a separate Instant per
    /// endpoint — NOT the shared 60s RateLimiter).
    DiskCacheLifecycleBreach,
    /// On endpoint death, dump the last 5 chunks' full lifecycle timings
    /// in one row so the operator can pinpoint which stage stalled.
    /// Severity::Warn; never rate-limited.
    EndpointLifecyclePredeath,

    /// Host-side: at VPS "delivering" transition, recomputed fresh live-edge
    /// chunk_id for an is_fast=true endpoint and POSTed it to the VPS.
    /// Detail JSON: {from_chunk_id, to_chunk_id, gap_chunks, alias, instance_id}.
    FastEndpointJumpedToLiveEdge,

    /// VPS-side: replaced an endpoint's start_chunk_id at host request
    /// (handler: POST /api/endpoints/update_start). Detail JSON:
    /// {alias, old_start_chunk_id, new_start_chunk_id}.
    EndpointStartChunkUpdated,

    /// Host-side: YT health probe observed `configurationIssues[0].type`
    /// change for an endpoint. Detail JSON:
    /// `{from: Option<String>, to: Option<String>}`. Bounded at most once
    /// per 30 s per endpoint by the surrounding caller.
    YoutubeIssueChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub severity: Severity,
    pub source: Source,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: Action,
    pub detail: Value,
    /// Optional pre-set timestamp (used when mirroring VPS rows to preserve their ts).
    /// `None` means "use current wall clock at INSERT".
    pub ts_override: Option<String>,
}

/// Rate limiter for noisy audit categories. Keyed by (Action, class-string).
/// Emits at most 1 row per minute per key.
pub struct RateLimiter {
    last: dashmap::DashMap<(Action, String), Instant>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            last: dashmap::DashMap::new(),
        }
    }

    pub fn allow(&self, action: Action, class: &str) -> bool {
        let key = (action, class.to_string());
        let now = Instant::now();
        let mut allow = true;
        self.last
            .entry(key)
            .and_modify(|t| {
                if now.duration_since(*t) < Duration::from_secs(60) {
                    allow = false;
                } else {
                    *t = now;
                }
            })
            .or_insert(now);
        allow
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Push an audit row into the writer channel. Non-blocking.
/// On channel-full: Info rows are dropped; Warn/Error/Critical rows are
/// preserved via an async retry on a spawned task so operator-actionable
/// evidence is never lost under load (the 2026-04-19 post-mortem required
/// `Warn` rows — e.g. `EndpointFfmpegDied` — to be present after the fact).
pub fn record(tx: &mpsc::Sender<AuditRow>, row: AuditRow) {
    match tx.try_send(row.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(r))
            if matches!(
                r.severity,
                Severity::Warn | Severity::Error | Severity::Critical
            ) =>
        {
            let tx2 = tx.clone();
            tokio::spawn(async move {
                let _ = tx2.send(r).await;
            });
        }
        Err(_) => { /* drop Info under pressure */ }
    }
}

/// Drains the audit channel, INSERTs rows (batched), broadcasts WS events.
pub async fn audit_writer_task(
    pool: SqlitePool,
    ws_tx: broadcast::Sender<WsEvent>,
    mut rx: mpsc::Receiver<AuditRow>,
) {
    const BATCH_MAX: usize = 32;
    const FLUSH_AFTER: Duration = Duration::from_millis(100);

    let mut buf: Vec<AuditRow> = Vec::with_capacity(BATCH_MAX);
    loop {
        let Some(first) = rx.recv().await else {
            return;
        };
        buf.push(first);

        let deadline = Instant::now() + FLUSH_AFTER;
        while buf.len() < BATCH_MAX {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(r)) => buf.push(r),
                _ => break,
            }
        }

        if let Err(e) = crate::db::audit::insert_batch(&pool, &buf, &ws_tx).await {
            tracing::error!("audit batch insert failed: {e}");
        }
        buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_serde_snake_case() {
        assert_eq!(serde_json::to_string(&Severity::Info).unwrap(), r#""info""#);
        assert_eq!(
            serde_json::to_string(&Severity::Critical).unwrap(),
            r#""critical""#
        );
        let back: Severity = serde_json::from_str(r#""warn""#).unwrap();
        assert_eq!(back, Severity::Warn);
    }

    #[test]
    fn source_serde_snake_case() {
        assert_eq!(serde_json::to_string(&Source::Vps).unwrap(), r#""vps""#);
        assert_eq!(
            serde_json::to_string(&Source::Operator).unwrap(),
            r#""operator""#
        );
    }

    #[test]
    fn action_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Action::EndpointFfmpegDied).unwrap(),
            r#""endpoint_ffmpeg_died""#
        );
        assert_eq!(
            serde_json::to_string(&Action::RtmpConnected).unwrap(),
            r#""rtmp_connected""#
        );
    }

    #[test]
    fn rate_limiter_allows_first_and_blocks_within_minute() {
        let rl = RateLimiter::new();
        assert!(rl.allow(Action::S3UploadFailed, "timeout"));
        assert!(!rl.allow(Action::S3UploadFailed, "timeout"));
        assert!(rl.allow(Action::S3UploadFailed, "404"));
    }

    #[tokio::test]
    async fn record_try_send_drops_info_on_full_channel() {
        let (tx, mut rx) = mpsc::channel::<AuditRow>(1);
        let row = AuditRow {
            severity: Severity::Info,
            source: Source::System,
            event_id: None,
            instance_id: None,
            endpoint: None,
            action: Action::RestreamerStarted,
            detail: serde_json::json!({}),
            ts_override: None,
        };
        record(&tx, row.clone());
        record(&tx, row.clone());
        drop(tx);
        let mut count = 0;
        while rx.recv().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 1, "second Info row should have been dropped");
    }

    #[test]
    fn rate_limiter_keys_disk_cache_push_sample_per_endpoint() {
        let rl = RateLimiter::new();
        assert!(rl.allow(Action::DiskCachePushSample, "FB-NewLevel"));
        assert!(!rl.allow(Action::DiskCachePushSample, "FB-NewLevel"));
        // Different endpoint key -> separate slot, must allow.
        assert!(rl.allow(Action::DiskCachePushSample, "YT NLCH 4K"));
        assert!(!rl.allow(Action::DiskCachePushSample, "YT NLCH 4K"));
    }

    #[test]
    fn action_disk_cache_lifecycle_sample_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::DiskCacheLifecycleSample).unwrap(),
            r#""disk_cache_lifecycle_sample""#
        );
        let back: Action = serde_json::from_str(r#""disk_cache_lifecycle_sample""#).unwrap();
        assert_eq!(back, Action::DiskCacheLifecycleSample);
    }

    #[test]
    fn action_disk_cache_lifecycle_breach_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::DiskCacheLifecycleBreach).unwrap(),
            r#""disk_cache_lifecycle_breach""#
        );
        let back: Action = serde_json::from_str(r#""disk_cache_lifecycle_breach""#).unwrap();
        assert_eq!(back, Action::DiskCacheLifecycleBreach);
    }

    #[test]
    fn action_endpoint_lifecycle_predeath_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::EndpointLifecyclePredeath).unwrap(),
            r#""endpoint_lifecycle_predeath""#
        );
        let back: Action = serde_json::from_str(r#""endpoint_lifecycle_predeath""#).unwrap();
        assert_eq!(back, Action::EndpointLifecyclePredeath);
    }
}
