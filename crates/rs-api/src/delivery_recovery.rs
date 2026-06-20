//! Boot-time delivery reconciliation (#252). Extracted into its own module so
//! `delivery.rs` stays under the 1000-line CI cap.
//!
//! After a stream.lan host/app crash, restarting Restreamer.exe must resume an
//! actively-delivering event WITHOUT any operator action. All live-delivery
//! management state (`poll_handles`, `endpoint_fast_cache`, `resume_positions`)
//! lives in-memory on `DeliveryOrchestrator` and is reset empty every launch;
//! the only boot-time delivery task was the read-only `delivery_broadcast_loop`.
//! This module re-establishes that management against the DURABLE DB state that
//! was already persisted (just never read at boot):
//!   - `streaming_events.delivering_activated` — was an event delivering?
//!   - `delivery_instances` — the live VPS row (hetzner_id / ipv4 / auth_token)
//!   - `endpoint_configs` (via `get_event_endpoints`) — per-endpoint is_fast
//!   - `delivery_endpoint_status.current_chunk_id` — per-endpoint resume position

use std::collections::HashMap;
use std::sync::Arc;

use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::delivery::DeliveryOrchestrator;
use crate::delivery_helpers::is_delivery_active;

impl DeliveryOrchestrator {
    /// Reconcile in-memory delivery management against persisted DB state on
    /// process boot. Called from `ServiceCore::run_with_signal` right after
    /// `resume_pending_grants`, so a crash-restarted host resumes an
    /// actively-delivering event with NO operator POST.
    ///
    /// Decision branches (all logged with event_id/instance_id for unattended
    /// post-crash debugging):
    ///   1. No event with `delivering_activated = 1` → nothing was delivering,
    ///      no-op.
    ///   2. Event delivering but no live `delivery_instances` row (status not in
    ///      the live set) → the VPS is gone/dead; no-op (a fresh operator Start
    ///      Delivering will recreate it). We do NOT spawn a task against a dead
    ///      VPS.
    ///   3. Event delivering AND a live instance → repopulate
    ///      `endpoint_fast_cache`, seed `resume_positions` from persisted
    ///      per-endpoint progress, then spawn the same `poll_and_init` →
    ///      `monitor_delivery_health` task the operator path spawns, tracking
    ///      its `JoinHandle` in `poll_handles`.
    pub async fn reconcile_delivery_on_boot(
        self: &Arc<Self>,
        ws_tx: broadcast::Sender<WsEvent>,
    ) -> anyhow::Result<()> {
        // Branch 1: was anything delivering at crash time?
        let event = match db::get_streaming_event(self.pool()).await {
            Ok(Some(e)) if e.delivering_activated => e,
            Ok(Some(e)) => {
                info!(
                    event_id = e.id,
                    "Boot delivery reconcile: current event not delivering — nothing to resume"
                );
                return Ok(());
            }
            Ok(None) => {
                info!("Boot delivery reconcile: no current streaming event — nothing to resume");
                return Ok(());
            }
            Err(e) => {
                warn!("Boot delivery reconcile: DB error reading current event: {e}");
                return Err(e.into());
            }
        };
        let event_id = event.id;

        // Branch 2: is there a live VPS instance to manage?
        let instance = match db::get_delivery_instance_by_event(self.pool(), event_id).await {
            Ok(Some(inst)) if is_delivery_active(&inst.status) => inst,
            Ok(Some(inst)) => {
                info!(
                    event_id,
                    instance_id = inst.id,
                    status = %inst.status,
                    "Boot delivery reconcile: event was delivering but instance is not live — \
                     not re-arming a dead VPS (operator Start Delivering will recreate)"
                );
                return Ok(());
            }
            Ok(None) => {
                info!(
                    event_id,
                    "Boot delivery reconcile: event was delivering but no instance row — \
                     nothing to re-arm"
                );
                return Ok(());
            }
            Err(e) => {
                warn!(
                    event_id,
                    "Boot delivery reconcile: DB error reading instance: {e}"
                );
                return Err(e.into());
            }
        };
        let instance_id = instance.id;

        info!(
            event_id,
            instance_id,
            ipv4 = %instance.ipv4,
            status = %instance.status,
            "Boot delivery reconcile: resuming delivery management for crash-recovered event"
        );

        // Branch 3a: repopulate endpoint_fast_cache from persisted config so
        // is_fast resolves correctly (it would read false until init otherwise —
        // delivery_status.rs:208-212). This mirrors what poll_and_init does at
        // delivery.rs:618, done up-front so the dashboard/status path is correct
        // immediately rather than only after the (re-spawned) poll_and_init runs.
        let endpoints = db::get_event_endpoints(self.pool(), event_id).await?;
        for ep in &endpoints {
            self.update_endpoint_fast_cache(event_id, &ep.alias, ep.is_fast)
                .await;
        }
        info!(
            event_id,
            instance_id,
            endpoints = endpoints.len(),
            "Boot delivery reconcile: repopulated endpoint_fast_cache"
        );

        // Branch 3b: seed resume_positions from persisted per-endpoint progress
        // (delivery_endpoint_status.current_chunk_id). poll_and_init drains this
        // map and resumes each endpoint at its last-delivered chunk instead of
        // recomputing a fresh live edge (delivery.rs:634-687). Only positions
        // for endpoints still attached to the event are seeded.
        let statuses = db::get_delivery_endpoint_statuses(self.pool(), instance_id).await?;
        let attached: std::collections::HashSet<&str> =
            endpoints.iter().map(|e| e.alias.as_str()).collect();
        let mut resume: HashMap<String, i64> = HashMap::new();
        for st in &statuses {
            if attached.contains(st.alias.as_str()) && st.current_chunk_id > 0 {
                resume.insert(st.alias.clone(), st.current_chunk_id);
            }
        }
        if resume.is_empty() {
            info!(
                event_id,
                instance_id,
                "Boot delivery reconcile: no persisted resume positions (>0) — \
                 poll_and_init will compute a fresh live edge"
            );
        } else {
            info!(
                event_id,
                instance_id,
                positions = resume.len(),
                "Boot delivery reconcile: seeded resume_positions from persisted progress"
            );
            self.seed_resume_positions(event_id, resume).await;
        }

        // Branch 3c: re-arm poll_and_init -> monitor_delivery_health, exactly as
        // the operator delivery_start handler does (delivery_handlers.rs:160-190).
        // The JoinHandle is tracked in poll_handles so stop_delivery can abort it.
        let event_name = event.name.clone();
        let auth_token = instance.auth_token.clone();
        let orch = Arc::clone(self);
        let poll_handles = self.poll_handles();
        let cached_delivery = Arc::new(std::sync::RwLock::new(
            crate::state::CachedDeliveryStatus::default(),
        ));
        let ws_tx_clone = ws_tx.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = orch
                .poll_and_init(instance_id, event_id, &event_name, &auth_token)
                .await
            {
                error!(
                    event_id,
                    instance_id, "Boot reconcile poll_and_init failed: {e}"
                );
                if let Err(e) =
                    db::update_delivery_instance_status(orch.pool(), instance_id, "failed").await
                {
                    error!(
                        instance_id,
                        "Failed to mark instance failed after boot reconcile: {e}"
                    );
                }
                orch.poll_handles().lock().await.remove(&instance_id);
                return;
            }
            info!(
                event_id,
                instance_id, "Boot reconcile: delivery health monitor started"
            );
            orch.monitor_delivery_health(event_id, instance_id, cached_delivery, ws_tx_clone)
                .await;
            orch.poll_handles().lock().await.remove(&instance_id);
        });
        poll_handles.lock().await.insert(instance_id, handle);

        info!(
            event_id,
            instance_id, "Boot delivery reconcile: delivery management re-established"
        );
        Ok(())
    }

    /// Read-only snapshot of the seeded resume positions for an event (test +
    /// diagnostic accessor; the live consumer is `poll_and_init`, which drains
    /// the map). Returns `None` when no positions are seeded for the event.
    pub async fn resume_positions_snapshot(&self, event_id: i64) -> Option<HashMap<String, i64>> {
        self.resume_positions_for_event(event_id).await
    }
}
