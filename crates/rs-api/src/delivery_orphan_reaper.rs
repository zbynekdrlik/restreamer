//! #352: runtime Hetzner-side orphan-VPS reconciliation ("orphan reaper").
//!
//! Every other teardown path is keyed on a `delivery_instances` DB row
//! (`stop_delivery`, `cleanup_orphan_delivery_vps`, `reconcile_delivery_on_boot`).
//! When that row is lost — a DB reset/reinstall, a crash in the create window
//! (`delivery.rs` `create_server` BEFORE `create_delivery_instance`), or a forced
//! kill mid-stop — the Hetzner VPS becomes invisible to the app and bills forever.
//! The only prior sweep was CI-side and scoped to CI's own `client_uuid`.
//!
//! This module treats **Hetzner as the source of truth for what is billing**: list
//! the servers labelled `app=restreamer,client_uuid=<this install>`, and any server
//! with no live DB row is an orphan — logged, audited (`vps_orphan_detected`),
//! surfaced on the dashboard, and auto-deleted after a configurable grace period.
//!
//! Safety (the #137 cross-install regression): a server is only ever touched when
//! BOTH its `app` and `client_uuid` labels match this install — verified locally by
//! [`classify_orphan_vps`] in addition to the server-side `label_selector`. A server
//! carrying a different `client_uuid` is never listed, never counted, never deleted.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, Ordering};

use chrono::{DateTime, Utc};
use rs_cloud::hetzner::Server;
use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;
use tracing::{error, info, warn};

use crate::delivery::DeliveryOrchestrator;

/// An orphaned Hetzner VPS: a labelled server for this install with no live
/// `delivery_instances` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanVps {
    pub hetzner_id: i64,
    pub name: String,
    pub ipv4: String,
    /// Age in seconds at classification time (now - server.created).
    pub age_secs: i64,
    /// True when the orphan has aged past the delete grace and should be
    /// auto-deleted this cycle. False when it is only old enough to detect +
    /// surface (still within the delete grace, or auto-delete disabled).
    pub delete: bool,
}

/// Pure decision function (the unit-test target): given the Hetzner server list
/// (already server-side filtered, but re-verified here), the set of `hetzner_id`s
/// that have a live DB row, the current time, this install's `client_uuid`, and the
/// two grace windows, return the orphans.
///
/// A server is an orphan ONLY when ALL hold:
///   * `labels[app] == "restreamer"` AND `labels[client_uuid] == expected_client_uuid`
///     — defense in depth over the server-side `label_selector` (#137). A missing or
///     mismatched label is skipped, never touched.
///   * its id is NOT in `live_hetzner_ids` (no live DB row tracks it).
///   * its `created` timestamp parses AND its age >= `detect_grace_secs` — a younger
///     rowless server is an in-flight create (`create_server` before the row write),
///     left alone. An unparseable `created` is skipped (fail-safe: never delete a
///     server whose age is unknown).
///
/// `delete` is set when `age_secs >= delete_grace_secs` AND `delete_grace_secs > 0`
/// (a `0` delete grace means detect-and-surface only, never auto-delete).
pub fn classify_orphan_vps(
    servers: &[Server],
    live_hetzner_ids: &HashSet<i64>,
    now: DateTime<Utc>,
    expected_client_uuid: &str,
    detect_grace_secs: i64,
    delete_grace_secs: i64,
) -> Vec<OrphanVps> {
    let mut orphans = Vec::new();
    for s in servers {
        // #137 guard (defense in depth over the server-side label_selector):
        // NEVER touch a server unless it is unambiguously THIS install's —
        // `app=restreamer` AND `client_uuid=<expected>`. A missing or
        // mismatched label is skipped outright.
        if s.labels.get("app").map(String::as_str) != Some("restreamer") {
            continue;
        }
        if s.labels.get("client_uuid").map(String::as_str) != Some(expected_client_uuid) {
            continue;
        }
        // A server with a live DB row is tracked, not an orphan.
        if live_hetzner_ids.contains(&s.id) {
            continue;
        }
        // Age gate. An unparseable `created` is skipped (fail-safe: never
        // delete a server whose age we cannot determine).
        let age_secs = match DateTime::parse_from_rfc3339(&s.created) {
            Ok(created) => (now - created.with_timezone(&Utc)).num_seconds(),
            Err(_) => continue,
        };
        // Younger than the detect grace → an in-flight create (create_server
        // ran, the DB row write has not landed yet); leave it alone.
        if age_secs < detect_grace_secs {
            continue;
        }
        // Past the detect grace with no row → an orphan. Mark it for deletion
        // only once it is past the (positive) delete grace.
        let delete = delete_grace_secs > 0 && age_secs >= delete_grace_secs;
        orphans.push(OrphanVps {
            hetzner_id: s.id,
            name: s.name.clone(),
            ipv4: s.public_net.ipv4.ip.clone(),
            age_secs,
            delete,
        });
    }
    orphans
}

impl DeliveryOrchestrator {
    /// #352: one reconciliation sweep — treat Hetzner as the source of truth for
    /// what is billing. List the servers labelled for THIS install, classify any
    /// with no live DB row as orphans (via [`classify_orphan_vps`]), log + audit
    /// (`vps_orphan_detected`) each, auto-delete those past the delete grace, and
    /// publish the count still billing into `orphan_count` (the dashboard banner
    /// signal). Called once on boot and on a periodic timer from the runtime.
    ///
    /// Fail-closed at every step: an empty `client_uuid`, a Hetzner list error,
    /// or a DB error all abort the sweep WITHOUT deleting anything — a delete is
    /// only ever made from a complete, unambiguous picture (#137).
    pub(crate) async fn reconcile_orphan_vps(&self, orphan_count: &AtomicU8) {
        let config = self.config();
        let uuid = config.client_uuid.clone();
        if uuid.is_empty() {
            warn!("orphan reaper: client_uuid is empty — refusing to sweep (fail-closed, #137)");
            return;
        }
        let selector = format!("app=restreamer,client_uuid={uuid}");
        let servers = match self.hetzner().list_servers(Some(&selector)).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "orphan reaper: Hetzner list_servers failed, skipping this cycle (never \
                     delete on incomplete info): {e}"
                );
                return;
            }
        };
        let live_ids: HashSet<i64> = match db::list_delivery_instances(self.pool()).await {
            Ok(rows) => rows.into_iter().map(|r| r.hetzner_id).collect(),
            Err(e) => {
                warn!("orphan reaper: DB list_delivery_instances failed, skipping this cycle: {e}");
                return;
            }
        };

        let orphans = classify_orphan_vps(
            &servers,
            &live_ids,
            Utc::now(),
            &uuid,
            config.delivery.orphan_detect_grace_secs as i64,
            config.delivery.orphan_delete_grace_secs as i64,
        );

        if orphans.is_empty() {
            orphan_count.store(0, Ordering::Relaxed);
            return;
        }

        let mut still_billing: u32 = 0;
        for o in &orphans {
            warn!(
                hetzner_id = o.hetzner_id,
                name = %o.name,
                ipv4 = %o.ipv4,
                age_secs = o.age_secs,
                will_delete = o.delete,
                "orphan reaper: Hetzner VPS labelled for this install has NO live \
                 delivery_instances row — billing but invisible to the app (money leak, #352)"
            );
            let mut auto_deleted = false;
            if o.delete {
                match self.hetzner().delete_server(o.hetzner_id).await {
                    Ok(()) => {
                        auto_deleted = true;
                        info!(
                            hetzner_id = o.hetzner_id,
                            age_secs = o.age_secs,
                            "orphan reaper: auto-deleted orphaned VPS past the delete grace (#352)"
                        );
                    }
                    Err(e) => {
                        error!(
                            hetzner_id = o.hetzner_id,
                            "orphan reaper: failed to delete orphaned VPS (still billing): {e}"
                        );
                    }
                }
            }
            if !auto_deleted {
                still_billing += 1;
            }
            if let Some(tx) = self.audit_tx() {
                rs_core::audit::record(
                    tx,
                    AuditRow {
                        severity: Severity::Warn,
                        source: Source::Delivery,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: Action::VpsOrphanDetected,
                        detail: serde_json::json!({
                            "hetzner_id": o.hetzner_id,
                            "name": o.name,
                            "ipv4": o.ipv4,
                            "age_secs": o.age_secs,
                            "auto_deleted": auto_deleted,
                        }),
                        ts_override: None,
                    },
                );
            }
        }
        // The banner reflects money STILL leaking — orphans not (yet) deleted.
        orphan_count.store(still_billing.min(u8::MAX as u32) as u8, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "delivery_orphan_reaper_tests.rs"]
mod tests;
