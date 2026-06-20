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

use crate::delivery::DeliveryOrchestrator;

impl DeliveryOrchestrator {
    /// Reconcile in-memory delivery management against persisted DB state on
    /// process boot. Stub: filled in by the #252 fix commit.
    pub async fn reconcile_delivery_on_boot(self: &std::sync::Arc<Self>) -> anyhow::Result<()> {
        Ok(())
    }

    /// Read-only snapshot of the seeded resume positions for an event (test +
    /// diagnostic accessor; the live consumer is `poll_and_init`, which drains
    /// the map). Returns `None` when no positions are seeded for the event.
    pub async fn resume_positions_snapshot(&self, event_id: i64) -> Option<HashMap<String, i64>> {
        self.resume_positions_for_event(event_id).await
    }
}
