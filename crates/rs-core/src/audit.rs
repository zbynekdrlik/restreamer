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
    S3UploadFailed,
    S3FetchFailed,
    RestreamerStarted,
    MigrationsApplied,
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
/// On channel-full, Info/Warn rows are dropped; Error/Critical blocking-send
/// via a spawned task so we never lose them.
pub fn record(tx: &mpsc::Sender<AuditRow>, row: AuditRow) {
    match tx.try_send(row.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(r))
            if matches!(r.severity, Severity::Error | Severity::Critical) =>
        {
            let tx2 = tx.clone();
            tokio::spawn(async move {
                let _ = tx2.send(r).await;
            });
        }
        Err(_) => { /* drop Info/Warn under pressure */ }
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
}
