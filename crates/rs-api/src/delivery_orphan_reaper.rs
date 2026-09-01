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

use chrono::{DateTime, Utc};
use rs_cloud::hetzner::Server;

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
    // RED stub — real classification lands in the GREEN commit.
    let _ = (
        servers,
        live_hetzner_ids,
        now,
        expected_client_uuid,
        detect_grace_secs,
        delete_grace_secs,
    );
    Vec::new()
}

#[cfg(test)]
#[path = "delivery_orphan_reaper_tests.rs"]
mod tests;
