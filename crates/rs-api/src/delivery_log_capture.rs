//! Post-mortem delivery-log capture (#307).
//!
//! At delivery stop we want the VPS's `rs-delivery.log` even when the VPS is
//! unreachable/dead — otherwise "why did the VPS die" is unanswerable (2× live
//! incident 2026-07-22, run 29897271675: the VPS went fully unreachable
//! mid-soak, the at-stop HTTP fetch could not reach it, and cleanup deleted the
//! server, leaving zero evidence).
//!
//! Two sources are tried, in order of freshness:
//!   1. **HTTP** `GET {vps}:8000/api/logs` — the complete, structured, freshest
//!      log, but needs a live VPS.
//!   2. **S3 fallback** — the cloud-init `log-uploader.sh` uploads
//!      `rs-delivery.log` to `s3://{bucket}/delivery-logs/{hostname}.log` every
//!      15s, so the last snapshot survives the VPS's death. The guest hostname
//!      equals the Hetzner server name, which is `instance.name`
//!      (`rs-delivery-evt{event_id}`).
//!
//! Only when BOTH fail do we emit a loud `delivery_log_lost` audit row so the
//! evidence gap is VISIBLE (the previous behavior failed silently).
//!
//! The per-VPS S3 log object is reaped on instance deletion
//! ([`cleanup_delivery_log_s3`]) so the `delivery-logs/` prefix — which lives
//! outside the per-event prefixes the chunk-wipe targets — does not accumulate.

use std::time::Duration;

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::DeliveryInstance;
use sqlx::SqlitePool;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::delivery_helpers::persist_delivery_log_to_disk;

/// S3 object key under which a VPS uploads its own `rs-delivery.log`
/// (cloud-init `log-uploader.sh`: `delivery-logs/$(hostname).log`). The guest
/// hostname equals the Hetzner server name, which is `instance.name`.
pub(crate) fn delivery_log_s3_key(instance_name: &str) -> String {
    format!("delivery-logs/{instance_name}.log")
}

/// Where the captured post-mortem log came from — the pure decision core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogCaptureOutcome {
    /// The live VPS HTTP endpoint returned a non-empty log (freshest source).
    FromHttp(String),
    /// The VPS was unreachable/empty over HTTP; the S3 fallback copy had it.
    FromS3(String),
    /// Neither source yielded a log — loud `delivery_log_lost` territory.
    Lost { reason: String },
}

/// Pure source-selection: prefer the freshest HTTP log; fall back to the S3
/// copy; otherwise LOST with a machine-readable reason. HTTP-/S3-free so it is
/// directly unit-testable.
///
/// * `http_log` — `Some(text)` when the HTTP fetch succeeded (text may be
///   empty), `None` when it failed/was unreachable.
/// * `s3_log` — `Ok(Some(text))` when the S3 object was fetched (may be empty),
///   `Ok(None)` when the object was absent (404), `Err(msg)` on an S3 error.
pub(crate) fn decide_log_capture(
    http_log: Option<String>,
    s3_log: Result<Option<String>, String>,
) -> LogCaptureOutcome {
    if let Some(text) = http_log {
        if !text.is_empty() {
            return LogCaptureOutcome::FromHttp(text);
        }
    }
    match s3_log {
        Ok(Some(text)) if !text.is_empty() => LogCaptureOutcome::FromS3(text),
        Ok(Some(_)) => LogCaptureOutcome::Lost {
            reason: "s3_object_empty".to_string(),
        },
        Ok(None) => LogCaptureOutcome::Lost {
            reason: "s3_object_missing".to_string(),
        },
        Err(e) => LogCaptureOutcome::Lost {
            reason: format!("s3_error: {e}"),
        },
    }
}

/// Fetch the VPS's log over HTTP (`GET /api/logs?limit=5000`) and render it as
/// chronological text lines. Returns `None` when the VPS is unreachable / the
/// response is non-success / the body cannot be parsed (all the cases where the
/// S3 fallback must take over). `Some("")` is possible for a reachable VPS with
/// an empty log and is treated as "try S3" by the decider.
async fn fetch_vps_log_http(instance: &DeliveryInstance) -> Option<String> {
    let client = reqwest::Client::new();
    let url = format!("http://{}:8000/api/logs?limit=5000", instance.ipv4);
    let resp = match client
        .get(&url)
        .bearer_auth(&instance.auth_token)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            warn!(status = %r.status(), "VPS log capture returned non-success");
            return None;
        }
        Err(e) => {
            warn!("VPS log capture failed (VPS may be unresponsive): {e}");
            return None;
        }
    };
    let body = match resp.json::<serde_json::Value>().await {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to parse VPS log response: {e}");
            return None;
        }
    };
    // The /api/logs endpoint returns newest-first; store chronologically.
    let text = body["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .rev()
                .map(|e| {
                    format!(
                        "[{}] {} {}",
                        e["level"].as_str().unwrap_or("?"),
                        e["target"].as_str().unwrap_or("?"),
                        e["message"].as_str().unwrap_or("")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    Some(text)
}

/// Fetch the S3 fallback copy of the VPS log (`delivery-logs/<name>.log`).
/// `Ok(None)` = object absent, `Err` = S3 error.
async fn fetch_delivery_log_from_s3(
    config: &Config,
    instance_name: &str,
) -> Result<Option<String>, String> {
    let key = delivery_log_s3_key(instance_name);
    let s3 = rs_endpoint::s3::S3Client::new(&config.s3).map_err(|e| format!("s3 init: {e}"))?;
    match tokio::time::timeout(Duration::from_secs(15), s3.get_object_string(&key)).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("s3 get timed out after 15s".to_string()),
    }
}

/// Capture the VPS log before the instance is deleted (#307). Tries the live
/// HTTP endpoint first, then the S3 fallback copy; persists whichever it gets
/// to the DB and to disk (`C:\ProgramData\Restreamer\delivery-logs\`). When
/// BOTH sources fail, emits a loud `delivery_log_lost` audit row so the
/// evidence gap is visible instead of silent.
pub(crate) async fn capture_delivery_log_before_delete(
    config: &Config,
    audit_tx: Option<&mpsc::Sender<AuditRow>>,
    pool: &SqlitePool,
    instance: &DeliveryInstance,
) {
    let http_log = fetch_vps_log_http(instance).await;
    // Only reach for S3 when HTTP did not already yield a usable log.
    let need_s3 = !matches!(&http_log, Some(t) if !t.is_empty());
    let s3_log = if need_s3 {
        fetch_delivery_log_from_s3(config, &instance.name).await
    } else {
        Ok(None)
    };

    match decide_log_capture(http_log, s3_log) {
        LogCaptureOutcome::FromHttp(log_text) => {
            persist_captured_log(pool, instance, &log_text, "http").await;
        }
        LogCaptureOutcome::FromS3(log_text) => {
            info!(
                instance_id = instance.id,
                key = %delivery_log_s3_key(&instance.name),
                "Recovered VPS log from S3 after at-stop HTTP fetch failed (#307)"
            );
            persist_captured_log(pool, instance, &log_text, "s3").await;
        }
        LogCaptureOutcome::Lost { reason } => {
            let key = delivery_log_s3_key(&instance.name);
            warn!(
                instance_id = instance.id,
                key = %key,
                reason,
                "delivery_log_lost: no post-mortem log from HTTP or S3 (#307)"
            );
            if let Some(tx) = audit_tx {
                rs_core::audit::record(
                    tx,
                    AuditRow {
                        severity: Severity::Error,
                        source: Source::Delivery,
                        event_id: instance.event_id,
                        instance_id: Some(instance.id),
                        endpoint: None,
                        action: Action::DeliveryLogLost,
                        detail: serde_json::json!({
                            "instance_name": instance.name,
                            "s3_key": key,
                            "reason": reason,
                        }),
                        ts_override: None,
                    },
                );
            }
        }
    }
}

/// Persist a captured log to the DB row and to disk. Best-effort: a DB failure
/// is logged, never propagated (the disk copy is the durable artifact).
async fn persist_captured_log(
    pool: &SqlitePool,
    instance: &DeliveryInstance,
    log_text: &str,
    source: &str,
) {
    match db::insert_delivery_log(pool, instance.id, instance.event_id, log_text).await {
        Ok(()) => info!(
            instance_id = instance.id,
            lines = log_text.lines().count(),
            source,
            "Captured VPS logs before deletion"
        ),
        Err(e) => warn!(source, "Failed to persist VPS logs: {e}"),
    }
    persist_delivery_log_to_disk(instance.id, instance.event_id, log_text);
}

/// Reap the per-VPS S3 log object when its instance is deleted so the
/// `delivery-logs/` prefix does not accumulate (#307). Best-effort — a failure
/// only logs (the object is small and re-used per event_id anyway).
pub(crate) async fn cleanup_delivery_log_s3(config: &Config, instance_name: &str) {
    let key = delivery_log_s3_key(instance_name);
    let s3 = match rs_endpoint::s3::S3Client::new(&config.s3) {
        Ok(s3) => s3,
        Err(e) => {
            warn!(key = %key, "delivery-log S3 cleanup skipped (s3 init failed): {e}");
            return;
        }
    };
    match tokio::time::timeout(Duration::from_secs(15), s3.delete_object(&key)).await {
        Ok(Ok(())) => info!(key = %key, "Reaped per-VPS S3 log object on instance deletion (#307)"),
        Ok(Err(e)) => warn!(key = %key, "delivery-log S3 cleanup failed: {e}"),
        Err(_) => warn!(key = %key, "delivery-log S3 cleanup timed out after 15s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_key_uses_hostname_stem() {
        assert_eq!(
            delivery_log_s3_key("rs-delivery-evt42"),
            "delivery-logs/rs-delivery-evt42.log"
        );
    }

    #[test]
    fn http_log_wins_when_present() {
        let out = decide_log_capture(Some("boot ok\npanic".into()), Ok(Some("stale".into())));
        assert_eq!(out, LogCaptureOutcome::FromHttp("boot ok\npanic".into()));
    }

    #[test]
    fn empty_http_falls_back_to_s3() {
        // Reachable-but-empty HTTP must not shadow a real S3 copy.
        let out = decide_log_capture(Some(String::new()), Ok(Some("s3 body".into())));
        assert_eq!(out, LogCaptureOutcome::FromS3("s3 body".into()));
    }

    #[test]
    fn unreachable_http_uses_s3() {
        let out = decide_log_capture(None, Ok(Some("s3 body".into())));
        assert_eq!(out, LogCaptureOutcome::FromS3("s3 body".into()));
    }

    #[test]
    fn both_missing_is_lost_with_reason() {
        // HTTP unreachable + S3 object absent → loud delivery_log_lost.
        assert_eq!(
            decide_log_capture(None, Ok(None)),
            LogCaptureOutcome::Lost {
                reason: "s3_object_missing".into()
            }
        );
        // HTTP unreachable + S3 error.
        assert_eq!(
            decide_log_capture(None, Err("connection reset".into())),
            LogCaptureOutcome::Lost {
                reason: "s3_error: connection reset".into()
            }
        );
        // HTTP empty + S3 empty.
        assert_eq!(
            decide_log_capture(Some(String::new()), Ok(Some(String::new()))),
            LogCaptureOutcome::Lost {
                reason: "s3_object_empty".into()
            }
        );
    }
}
