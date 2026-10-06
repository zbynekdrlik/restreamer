//! Delivery VPS health-monitor loop. Extracted from `delivery.rs` so the
//! main file stays under the 1000-line CI cap (#174 review finding 5).
//!
//! `monitor_delivery_health` runs as a background task per delivery
//! instance: every 30s, it polls the VPS `/api/health` endpoint. After
//! 3 consecutive failures it logs + audit-emits and surfaces a warning
//! to the operator. The VPS is NOT auto-restarted -- "unreachable"
//! usually means the orchestrator host lost internet, not a VPS crash.

use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;

use crate::delivery::DeliveryOrchestrator;
use crate::delivery_helpers::is_delivery_active;

impl DeliveryOrchestrator {
    /// Monitor delivery VPS health continuously. Auto-restart on persistent failure.
    ///
    /// Runs every 30s. After 3 consecutive failures (90s), surfaces an
    /// operator warning. Does NOT restart the VPS -- "unreachable"
    /// usually means stream.lan lost internet, and the VPS recovers on
    /// its own when the network returns. Retries indefinitely; the
    /// 90 s detection window provides natural throttling.
    pub async fn monitor_delivery_health(
        self: &Arc<Self>,
        event_id: i64,
        instance_id: i64,
        _cached_delivery: std::sync::Arc<std::sync::RwLock<crate::state::CachedDeliveryStatus>>,
        ws_tx: tokio::sync::broadcast::Sender<rs_core::models::WsEvent>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // skip immediate tick

        let mut health = HealthEdges::default();
        let client = reqwest::Client::new();

        // #84: fire a one-shot "stream running too long" warning once this
        // delivery passes the operator threshold. A fresh warner per loop (the
        // loop lives exactly one delivery) gives "once per event, re-arm on
        // stop" for free. `0` disables it. Read from the orchestrator's config
        // snapshot, matching how `delivery_delay_secs` is read.
        let long_stream_warn_secs = self.config().delivery.long_stream_warn_secs;
        let mut long_stream_warner = rs_core::long_stream::LongStreamWarner::new();

        loop {
            interval.tick().await;

            // Check if event is still delivering (operator may have stopped)
            match db::get_streaming_event_by_id(self.pool(), event_id).await {
                Ok(Some(evt)) if !evt.delivering_activated => {
                    info!(
                        event_id,
                        "Health monitor stopping: event no longer delivering"
                    );
                    return;
                }
                Ok(None) => {
                    info!(event_id, "Health monitor stopping: event deleted");
                    return;
                }
                Err(e) => {
                    warn!(event_id, "Health monitor DB error (event): {e}");
                }
                _ => {}
            }

            // Check if instance still exists and is running
            let instance = match db::get_delivery_instance(self.pool(), instance_id).await {
                Ok(Some(inst)) if is_delivery_active(&inst.status) => inst,
                Ok(Some(inst)) => {
                    info!(
                        event_id,
                        status = %inst.status,
                        "Health monitor stopping: instance no longer running"
                    );
                    return;
                }
                Ok(None) => {
                    info!(event_id, "Health monitor stopping: instance deleted");
                    return;
                }
                Err(e) => {
                    warn!(event_id, "Health monitor DB error: {e}");
                    continue;
                }
            };

            // #84: warn ONCE per delivery when it has been running longer than
            // the operator threshold — a stream possibly left on after the
            // event finished. Independent of VPS health. The audit row is
            // routed to Discord as a standalone heads-up (see notify::classify).
            if let Some(elapsed) =
                rs_core::long_stream::elapsed_secs(&instance.created_at, chrono::Utc::now())
            {
                if long_stream_warner.observe(elapsed, long_stream_warn_secs) {
                    warn!(
                        event_id,
                        elapsed_secs = elapsed,
                        threshold_secs = long_stream_warn_secs,
                        "Delivery running longer than the long-stream warning threshold (#84)"
                    );
                    if let Some(tx) = self.audit_tx() {
                        rs_core::audit::record(
                            tx,
                            AuditRow {
                                severity: Severity::Warn,
                                source: Source::Delivery,
                                event_id: Some(event_id),
                                instance_id: Some(instance_id),
                                endpoint: None,
                                action: Action::LongStreamWarning,
                                detail: serde_json::json!({
                                    "elapsed_secs": elapsed,
                                    "threshold_secs": long_stream_warn_secs,
                                }),
                                ts_override: None,
                            },
                        );
                    }
                }
            }

            // Check health. `last_error` is captured so audit rows carry a
            // useful message instead of just `false`.
            let mut last_error: Option<String> = None;
            let healthy = match client
                .get(format!("http://{}:8000/api/health", instance.ipv4))
                .bearer_auth(&instance.auth_token)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => true,
                Ok(resp) => {
                    let status = resp.status();
                    warn!(
                        event_id,
                        status = %status,
                        "Delivery VPS health returned non-success"
                    );
                    last_error = Some(format!("http_{status}"));
                    false
                }
                Err(e) => {
                    warn!(event_id, "Delivery VPS health check failed: {e}");
                    last_error = Some(e.to_string());
                    false
                }
            };

            let consecutive_failures = match health.observe(healthy) {
                HealthEdge::Steady => None,
                HealthEdge::Recovered { after } => {
                    info!(
                        event_id,
                        previous_failures = after,
                        "Delivery VPS health recovered"
                    );
                    // #367 review: the paired recovery of `VpsUnreachable`.
                    if let Some(tx) = self.audit_tx() {
                        rs_core::audit::record(tx, vps_reachable_row(event_id, instance_id, after));
                    }
                    None
                }
                HealthEdge::Failed { consecutive } => Some(consecutive),
            };
            if let Some(consecutive_failures) = consecutive_failures {
                error!(
                    event_id,
                    consecutive_failures,
                    "Delivery VPS health check failed ({consecutive_failures}/3)"
                );

                // Audit (rate-limited): one row per minute per failure class
                // so a persistent outage doesn't flood `audit_log`.
                if let Some(tx) = self.audit_tx() {
                    let class = last_error.as_deref().unwrap_or("unknown");
                    if crate::delivery::DELIVERY_RL.allow(Action::VpsUnreachable, class) {
                        rs_core::audit::record(
                            tx,
                            AuditRow {
                                severity: Severity::Warn,
                                source: Source::Delivery,
                                event_id: Some(event_id),
                                instance_id: Some(instance_id),
                                endpoint: None,
                                action: Action::VpsUnreachable,
                                detail: serde_json::json!({
                                    "consecutive_failures": consecutive_failures,
                                    "last_error": last_error,
                                }),
                                ts_override: None,
                            },
                        );
                    }
                }

                if consecutive_failures >= 3 {
                    error!(
                        event_id,
                        consecutive_failures,
                        "Delivery VPS unreachable for 90s -- monitoring continues, VPS NOT restarted"
                    );
                    let _ = ws_tx.send(rs_core::models::WsEvent::ActivityFeed {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        severity: "warning".to_string(),
                        message: "Delivery VPS unreachable -- waiting for network recovery"
                            .to_string(),
                        source: "delivery".to_string(),
                    });
                }
            } else {
                db::update_delivery_instance_health(self.pool(), instance_id)
                    .await
                    .ok();
            }
        }
    }
}

/// What one health poll means for VPS reachability (#367 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthEdge {
    /// Healthy, and so was the previous poll.
    Steady,
    /// The first healthy poll after `after` consecutive failures: the
    /// moment to write `VpsReachable`.
    Recovered { after: u32 },
    /// Failed, `consecutive` failures in a row so far.
    Failed { consecutive: u32 },
}

/// Counts consecutive failed health polls and reports each poll's edge, so
/// the recovery row is written exactly once per outage (never on every
/// healthy 30 s poll).
#[derive(Debug, Default)]
pub(crate) struct HealthEdges {
    consecutive_failures: u32,
}

impl HealthEdges {
    pub(crate) fn observe(&mut self, healthy: bool) -> HealthEdge {
        if healthy {
            match std::mem::take(&mut self.consecutive_failures) {
                0 => HealthEdge::Steady,
                after => HealthEdge::Recovered { after },
            }
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            HealthEdge::Failed {
                consecutive: self.consecutive_failures,
            }
        }
    }
}

/// The `VpsReachable` row (#367 review): the paired recovery of
/// `VpsUnreachable`. Without it the outage notifier's VPS-reachability
/// episode stayed open for the rest of the delivery and a later real VPS
/// death was deduped away.
pub(crate) fn vps_reachable_row(event_id: i64, instance_id: i64, after: u32) -> AuditRow {
    AuditRow {
        severity: Severity::Info,
        source: Source::Delivery,
        event_id: Some(event_id),
        instance_id: Some(instance_id),
        endpoint: None,
        action: Action::VpsReachable,
        detail: serde_json::json!({ "recovered_after_failures": after }),
        ts_override: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_then_healthy_is_exactly_one_recovery_edge() {
        let mut edges = HealthEdges::default();
        assert_eq!(edges.observe(true), HealthEdge::Steady);
        assert_eq!(edges.observe(false), HealthEdge::Failed { consecutive: 1 });
        assert_eq!(edges.observe(false), HealthEdge::Failed { consecutive: 2 });
        assert_eq!(edges.observe(false), HealthEdge::Failed { consecutive: 3 });
        assert_eq!(edges.observe(true), HealthEdge::Recovered { after: 3 });
        assert_eq!(
            edges.observe(true),
            HealthEdge::Steady,
            "no recovery row on every later healthy poll"
        );
        assert_eq!(
            edges.observe(false),
            HealthEdge::Failed { consecutive: 1 },
            "the count restarts after a recovery"
        );
        assert_eq!(edges.observe(true), HealthEdge::Recovered { after: 1 });
    }

    #[test]
    fn vps_reachable_row_shape() {
        let row = vps_reachable_row(7, 42, 3);
        assert_eq!(row.action, Action::VpsReachable);
        assert_eq!(row.severity, Severity::Info);
        assert_eq!(row.source, Source::Delivery);
        assert_eq!(row.event_id, Some(7));
        assert_eq!(row.instance_id, Some(42));
        assert_eq!(row.endpoint, None);
        assert_eq!(row.detail["recovered_after_failures"], 3);
    }
}
