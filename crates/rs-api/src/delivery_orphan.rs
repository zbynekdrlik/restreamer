//! #244: orphaned-VPS cleanup — delete the Hetzner VPS behind a delivery
//! instance that will never serve (failed `poll_and_init` start, or a stale
//! leftover row being replaced by a fresh spawn).
//!
//! Split from `delivery.rs` to keep that file under the 1000-line file-size gate.

use tracing::{error, info, warn};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;

use crate::delivery::DeliveryOrchestrator;

impl DeliveryOrchestrator {
    /// #244: Delete the Hetzner VPS backing a delivery instance that will never
    /// serve — a `poll_and_init` start that failed, or a stale leftover row
    /// being replaced by a fresh spawn — then mark the row "deleted" and emit a
    /// `vps_deleted` audit row (the #75 last-destroy surface reads it, so the
    /// operator sees WHY the VPS went away). Best-effort: a `delete_server`
    /// failure is logged + audited (reason `"delete_error"`) but never
    /// propagates, so the caller's own error handling is unaffected.
    ///
    /// Before this, both call sites orphaned a running, billed VPS: the failure
    /// handler dropped the task after marking "failed", and the stale-row
    /// cleanup relabelled the row "deleted" — neither ever hit Hetzner.
    pub(crate) async fn cleanup_orphan_delivery_vps(
        &self,
        instance_id: i64,
        event_id: i64,
        trigger: &str,
    ) {
        let instance = match db::get_delivery_instance(self.pool(), instance_id).await {
            Ok(Some(i)) => i,
            Ok(None) => {
                warn!(
                    instance_id,
                    trigger, "cleanup_orphan_delivery_vps: instance row not found"
                );
                return;
            }
            Err(e) => {
                error!(
                    instance_id,
                    trigger, "cleanup_orphan_delivery_vps: load failed: {e}"
                );
                return;
            }
        };

        let reason = match self.hetzner().delete_server(instance.hetzner_id).await {
            Ok(()) => {
                info!(
                    hetzner_id = instance.hetzner_id,
                    instance_id, trigger, "Deleted orphaned delivery VPS (#244)"
                );
                trigger.to_string()
            }
            Err(e) => {
                error!(
                    hetzner_id = instance.hetzner_id,
                    instance_id, "Failed to delete orphaned delivery VPS: {e}"
                );
                "delete_error".to_string()
            }
        };

        if let Err(e) =
            db::update_delivery_instance_status(self.pool(), instance_id, "deleted").await
        {
            error!(instance_id, "Failed to mark orphaned instance deleted: {e}");
        }

        if let Some(tx) = self.audit_tx() {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event_id),
                    instance_id: Some(instance_id),
                    endpoint: None,
                    action: Action::VpsDeleted,
                    detail: serde_json::json!({
                        "hetzner_id": instance.hetzner_id,
                        "ipv4": instance.ipv4,
                        "reason": reason,
                        "trigger": trigger,
                    }),
                    ts_override: None,
                },
            );
        }
    }
}
