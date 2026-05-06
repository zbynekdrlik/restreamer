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

        let mut consecutive_failures = 0u32;
        let client = reqwest::Client::new();

        loop {
            interval.tick().await;

            // Check if event is still delivering (operator may have stopped)
            match db::get_streaming_event_by_id(&self.pool(), event_id).await {
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
            let instance = match db::get_delivery_instance(&self.pool(), instance_id).await {
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

            if healthy {
                if consecutive_failures > 0 {
                    info!(
                        event_id,
                        previous_failures = consecutive_failures,
                        "Delivery VPS health recovered"
                    );
                }
                consecutive_failures = 0;
                db::update_delivery_instance_health(&self.pool(), instance_id)
                    .await
                    .ok();
            } else {
                consecutive_failures += 1;
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
            }
        }
    }
}
