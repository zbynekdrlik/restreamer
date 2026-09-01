//! #352: unit tests for the pure orphan-classification decision function.
//!
//! These use real Hetzner + DB data shapes (no mocks of internal code, per the
//! ticket): a `Server` built exactly as the Hetzner API deserializes it, and a
//! plain `HashSet<i64>` standing in for the live-DB-row id set.

use super::*;
use rs_cloud::hetzner::{Ipv4, PublicNet, Server, ServerType};
use std::collections::HashSet;

const THIS_UUID: &str = "test-uuid-00000000";

/// Build a Hetzner `Server` with the given id, `created` timestamp (RFC3339),
/// and labels — the exact shape `list_servers` returns.
fn server(id: i64, created: &str, labels: &[(&str, &str)]) -> Server {
    Server {
        id,
        name: format!("rs-delivery-evt{id}"),
        status: "running".to_string(),
        public_net: PublicNet {
            ipv4: Ipv4 {
                ip: format!("1.2.3.{id}"),
            },
        },
        server_type: ServerType {
            name: "cpx32".to_string(),
        },
        created: created.to_string(),
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

/// This install's labels (app + matching client_uuid).
fn ours() -> Vec<(&'static str, &'static str)> {
    vec![
        ("app", "restreamer"),
        ("event_id", "9343"),
        ("client_uuid", THIS_UUID),
    ]
}

fn now() -> chrono::DateTime<chrono::Utc> {
    "2026-08-11T12:00:00+00:00".parse().unwrap()
}

// 30 min detect grace, 3 h delete grace — the production defaults.
const DETECT: i64 = 1800;
const DELETE: i64 = 10800;

/// The evidenced money-leak case: a server labelled for THIS install, created
/// well before the detect grace, with NO live DB row → detected as an orphan
/// within one cycle. (rs-delivery-evt9343 in the ticket.)
#[test]
fn rowless_old_server_is_detected() {
    let servers = vec![server(160756914, "2026-08-11T06:49:35+00:00", &ours())];
    let live: HashSet<i64> = HashSet::new(); // DB row deleted out from under the app
    let orphans = classify_orphan_vps(&servers, &live, now(), THIS_UUID, DETECT, DELETE);
    assert_eq!(
        orphans.len(),
        1,
        "rowless labelled server must be an orphan"
    );
    assert_eq!(orphans[0].hetzner_id, 160756914);
    // ~5h10m old > 3h delete grace → auto-delete this cycle.
    assert!(
        orphans[0].delete,
        "an orphan older than the delete grace deletes"
    );
}

/// #137 CRITICAL: a server carrying a DIFFERENT client_uuid (another install,
/// e.g. streampp) is NEVER listed as an orphan and NEVER deleted, even though it
/// too has no row on THIS install. (rs-delivery-evt32 in the ticket.)
#[test]
fn foreign_client_uuid_is_never_an_orphan() {
    let foreign = vec![
        ("app", "restreamer"),
        ("event_id", "32"),
        ("client_uuid", "f59e3ecd-different-install"),
    ];
    let servers = vec![server(160624800, "2026-08-09T06:49:35+00:00", &foreign)];
    let live: HashSet<i64> = HashSet::new();
    let orphans = classify_orphan_vps(&servers, &live, now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "a different install's server must never be classified as an orphan (#137)"
    );
}

/// A server missing the `app=restreamer` label (some unrelated VPS on the same
/// project token) is never touched.
#[test]
fn non_restreamer_server_is_ignored() {
    let other = vec![("app", "something-else"), ("client_uuid", THIS_UUID)];
    let servers = vec![server(500, "2026-01-01T00:00:00+00:00", &other)];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "non-restreamer servers are out of scope"
    );
}

/// A server WITH a live DB row (a genuinely-delivering VPS) is tracked, not an
/// orphan — regardless of age.
#[test]
fn tracked_server_is_not_an_orphan() {
    let servers = vec![server(777, "2026-08-01T00:00:00+00:00", &ours())];
    let live: HashSet<i64> = [777].into_iter().collect();
    let orphans = classify_orphan_vps(&servers, &live, now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "a server with a live DB row must never be reaped, even when old"
    );
}

/// The create-window reverse gap (#4): a rowless server YOUNGER than the detect
/// grace is an in-flight `create_server` whose DB row has not been written yet —
/// it must be left alone, never nuked mid-create.
#[test]
fn young_rowless_server_is_not_yet_an_orphan() {
    // created 10 min before `now` — inside the 30-min detect grace.
    let servers = vec![server(888, "2026-08-11T11:50:00+00:00", &ours())];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "a freshly-created rowless server is an in-flight create, not an orphan"
    );
}

/// An orphan aged past the detect grace but still within the delete grace is
/// DETECTED (surfaced) but NOT auto-deleted this cycle.
#[test]
fn orphan_within_delete_grace_is_detected_but_not_deleted() {
    // created 1h before now: > 30m detect, < 3h delete.
    let servers = vec![server(999, "2026-08-11T11:00:00+00:00", &ours())];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, DELETE);
    assert_eq!(orphans.len(), 1);
    assert!(
        !orphans[0].delete,
        "an orphan younger than the delete grace is surfaced, not deleted"
    );
    assert_eq!(orphans[0].age_secs, 3600);
}

/// A `0` delete grace means detect-and-surface only — an orphan of any age is
/// never marked for auto-deletion.
#[test]
fn zero_delete_grace_never_auto_deletes() {
    let servers = vec![server(1001, "2026-08-01T00:00:00+00:00", &ours())];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, 0);
    assert_eq!(orphans.len(), 1, "still detected");
    assert!(!orphans[0].delete, "delete grace 0 disables auto-delete");
}

/// A server whose `created` timestamp does not parse is skipped — fail-safe: we
/// never delete a server whose age we cannot determine.
#[test]
fn unparseable_created_is_skipped() {
    let servers = vec![server(1002, "not-a-timestamp", &ours())];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "an unparseable created timestamp must be skipped, never deleted"
    );
}

/// A server labelled `app=restreamer` but carrying NO `client_uuid` label at all
/// (a mis-labelled / partially-labelled server) is never an orphan — the #137
/// guard requires an EXACT client_uuid match, not merely "not someone else's".
#[test]
fn missing_client_uuid_label_is_never_an_orphan() {
    let no_uuid = vec![("app", "restreamer"), ("event_id", "5")];
    let servers = vec![server(600, "2020-01-01T00:00:00+00:00", &no_uuid)];
    let orphans = classify_orphan_vps(&servers, &HashSet::new(), now(), THIS_UUID, DETECT, DELETE);
    assert!(
        orphans.is_empty(),
        "a server missing the client_uuid label must never be touched (#137)"
    );
}

/// Boundary: age EXACTLY at the detect grace is detected (the skip is `age <
/// detect_grace`, so `==` is NOT skipped); age exactly at the delete grace marks
/// deletion (`age >= delete_grace`).
#[test]
fn grace_boundaries_are_inclusive_for_action() {
    // created exactly DETECT (1800s) before now → age == 1800 → detected.
    let at_detect = vec![server(700, "2026-08-11T11:30:00+00:00", &ours())];
    let d = classify_orphan_vps(
        &at_detect,
        &HashSet::new(),
        now(),
        THIS_UUID,
        DETECT,
        DELETE,
    );
    assert_eq!(d.len(), 1, "age == detect_grace is detected");
    assert_eq!(d[0].age_secs, 1800);
    assert!(!d[0].delete, "still within the delete grace");

    // created exactly DELETE (10800s = 3h) before now → age == 10800 → delete.
    let at_delete = vec![server(701, "2026-08-11T09:00:00+00:00", &ours())];
    let x = classify_orphan_vps(
        &at_delete,
        &HashSet::new(),
        now(),
        THIS_UUID,
        DETECT,
        DELETE,
    );
    assert_eq!(x.len(), 1);
    assert_eq!(x[0].age_secs, 10800);
    assert!(x[0].delete, "age == delete_grace marks deletion");
}

/// Mixed real-world list: one of ours to reap, one foreign, one tracked — only
/// ours-and-rowless is returned.
#[test]
fn mixed_list_returns_only_our_rowless_orphans() {
    let foreign = vec![("app", "restreamer"), ("client_uuid", "other-install")];
    let servers = vec![
        server(160756914, "2026-08-11T06:49:35+00:00", &ours()), // orphan
        server(160624800, "2026-08-09T06:49:35+00:00", &foreign), // foreign — skip
        server(777, "2026-08-01T00:00:00+00:00", &ours()),       // tracked — skip
    ];
    let live: HashSet<i64> = [777].into_iter().collect();
    let orphans = classify_orphan_vps(&servers, &live, now(), THIS_UUID, DETECT, DELETE);
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].hetzner_id, 160756914);
}
