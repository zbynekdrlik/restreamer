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
//!
//! Recovery resumes at the LIVE EDGE (current OBS position), NOT by replaying the
//! persisted per-endpoint backlog: re-pushing hours-old chunks would violate the
//! hard strict-1x rule (feedback_rtmp_push_always_1x) and get the stream killed
//! by YouTube/FB. It therefore leaves `resume_positions` empty and lets
//! `poll_and_init` take its tested live-edge branch for every endpoint.

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
    ///      `endpoint_fast_cache`, then (unless an operator Start Delivering
    ///      already won the race and tracked a handle for this instance) spawn
    ///      the same `poll_and_init` → `monitor_delivery_health` task the
    ///      operator path spawns, tracking its `JoinHandle` in `poll_handles`.
    ///      Resumes at the live edge — does NOT seed `resume_positions` (no
    ///      backlog replay; strict 1x).
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

        // Branch 3b: resume at the LIVE EDGE — deliberately do NOT seed
        // resume_positions. The crash-recovered VPS must resume near the current
        // OBS position, NOT replay the persisted backlog:
        //   * Re-pushing hours-old chunks violates the hard strict-1x rule
        //     (feedback_rtmp_push_always_1x) — YouTube/FB kill a stream that
        //     floods historical chunks.
        //   * poll_and_init's resume branch (delivery.rs ~636) starts at
        //     MIN(sequence_number) for any endpoint missing a seeded position
        //     (its current_chunk_id was still 0 at crash — a fast endpoint that
        //     had not yet fetched, or one momentarily reading 0,
        //     delivery_status.rs:319), so a PARTIAL seed makes those endpoints
        //     replay the whole event from chunk 1.
        //   * That resume branch also lacks the find_first_chunk_id_at_or_after
        //     S3-existence guard (#174) the live-edge branch has, so it hangs the
        //     producer on chunks cleared during the outage.
        // Leaving resume_positions empty makes poll_and_init take the tested
        // live-edge branch (compute_target_start_chunk + S3-existence advance)
        // for EVERY endpoint — the same path the operator Start Delivering uses.
        info!(
            event_id,
            instance_id,
            "Boot delivery reconcile: resuming at live edge (no backlog replay — strict 1x)"
        );

        // Branch 3c: re-arm poll_and_init -> monitor_delivery_health, exactly as
        // the operator delivery_start handler does (delivery_handlers.rs:160-190).
        // The JoinHandle is tracked in poll_handles so stop_delivery can abort it.
        //
        // Guard against a double-spawn race: if an operator pressed Start
        // Delivering after boot began (start_delivery reuses this same live
        // instance_id and inserts its own handle), a handle already exists for
        // instance_id. Overwriting it would DETACH (not abort) the operator's
        // task, leaving two poll_and_init -> monitor loops + two /api/init POSTs
        // for one VPS. If a handle is already present, the operator path owns it
        // — skip boot re-arm.
        if self.poll_handles().lock().await.contains_key(&instance_id) {
            info!(
                event_id,
                instance_id,
                "Boot delivery reconcile: a delivery task is already tracked for this \
                 instance (operator Start Delivering won the race) — not re-arming"
            );
            return Ok(());
        }

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
