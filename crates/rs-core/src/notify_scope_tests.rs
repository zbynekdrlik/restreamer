//! #367 review (B1, B2): an outage episode cannot outlive its subject, and an
//! operator override row is never an alert onset.
//!
//! Keyed episodes (family, stage, endpoint) end only on their own paired
//! recovery. Several onsets have no guaranteed paired recovery: rescue ends
//! on a stop without `RescueRecovered`, a live pusher is dropped for the
//! rescue clip while its invariant guard is latched, a delivery or an
//! endpoint goes away, `VpsUnreachable` / `S3UploadFailed` have no recovery
//! row of their own. Without a scope end such an episode stays open for the
//! process lifetime and every later onset on that key is deduplicated away.
//! A lifecycle row closes the episodes of the subject it ends SILENTLY:
//! nothing recovered, so no "recovered" alert, but the next onset alerts.

use super::*;

/// A delivery (VPS) ends. Only the END edges: a start request on an already
/// live delivery reuses its instance yet still records `DeliveryStarted`
/// and re-emits `VpsReady` (round-2 review finding 3), so start edges must
/// not close the live delivery's episodes.
const DELIVERY_BOUNDARIES: [Action; 2] = [Action::DeliveryStopped, Action::VpsDeleted];

/// Start edges: recorded again on a reused, still-live delivery.
const DELIVERY_START_EDGES: [Action; 2] = [Action::DeliveryStarted, Action::VpsReady];

/// One endpoint's delivery begins anew or ends (rows carry the alias).
const ENDPOINT_BOUNDARIES: [Action; 3] = [
    Action::EndpointAdded,
    Action::EndpointRemoved,
    Action::EndpointStartChunkUpdated,
];

/// A streaming event (and so its chunk uploads) begins or ends.
const EVENT_BOUNDARIES: [Action; 2] = [Action::EventStarted, Action::EventStopped];

fn s3_permanent() -> AuditRow {
    row_detail(
        Action::S3UploadFailed,
        serde_json::json!({ "permanent": true, "attempt": 5, "error_class": "forbidden" }),
    )
}

#[test]
fn lifecycle_rows_are_outage_relevant() {
    let n = notifier();
    for action in DELIVERY_BOUNDARIES
        .into_iter()
        .chain(ENDPOINT_BOUNDARIES)
        .chain(EVENT_BOUNDARIES)
    {
        assert!(
            n.is_outage_relevant(&row(action)),
            "{action:?} must reach observe() so it can end its scope"
        );
    }
}

/// A delivery boundary ends every VPS-side episode (rescue, push-stage A/V
/// invariant, host -> VPS reachability) silently, and leaves the host-level
/// and ingest-side episodes alone.
#[test]
fn a_delivery_boundary_closes_the_vps_side_episodes_silently() {
    for boundary in DELIVERY_BOUNDARIES {
        let mut n = notifier();
        let rescue_on = row_ep(Action::RescueActivated, "YT A");
        let rescue_off = row_ep(Action::RescueRecovered, "YT A");
        let push = row_av(Action::AvInvariantViolated, "push", Some("FB B"));
        let ingest = row_av(Action::AvInvariantViolated, "ingest", None);
        let vps = row(Action::VpsUnreachable);
        let internet = row(Action::HostInternetUnreachable);
        let skew = row(Action::IngestSkewDetected);
        for r in [&rescue_on, &push, &ingest, &vps, &internet, &skew] {
            assert!(n.observe(r, None).is_some(), "{:?} opens", r.action);
        }

        assert!(
            n.observe(&row(boundary), None).is_none(),
            "{boundary:?} closes silently, it is not a recovery"
        );
        assert!(
            n.observe(&rescue_off, None).is_none(),
            "{boundary:?}: the rescue episode is closed, so no late 'recovered'"
        );
        assert!(
            n.observe(&rescue_on, None).is_some(),
            "{boundary:?}: a rescue on the next delivery alerts again"
        );
        assert!(
            n.observe(&push, None).is_some(),
            "{boundary:?}: a push-side violation on the next delivery alerts again"
        );
        assert!(
            n.observe(&vps, None).is_some(),
            "{boundary:?}: the next VPS's unreachability alerts again"
        );
        assert!(
            n.observe(&ingest, None).is_none(),
            "{boundary:?} must not end the ingest-side invariant episode"
        );
        assert!(
            n.observe(&internet, None).is_none(),
            "{boundary:?} must not end the host internet episode"
        );
        assert!(
            n.observe(&skew, None).is_none(),
            "{boundary:?} must not end the ingest skew episode"
        );
    }
}

/// An endpoint boundary ends only THAT endpoint's VPS-side episodes.
#[test]
fn an_endpoint_boundary_closes_only_that_endpoints_episodes() {
    for boundary in ENDPOINT_BOUNDARIES {
        let mut n = notifier();
        let a_rescue = row_ep(Action::RescueActivated, "YT A");
        let b_rescue = row_ep(Action::RescueActivated, "FB B");
        let a_push = row_av(Action::AvInvariantViolated, "push", Some("YT A"));
        let b_push = row_av(Action::AvInvariantViolated, "push", Some("FB B"));
        let vps = row(Action::VpsUnreachable);
        // Rescue first: entering rescue would end A's push-side episode by
        // itself (see `entering_rescue_ends_the_live_pushers_invariant_episode`).
        for r in [&a_rescue, &b_rescue, &a_push, &b_push, &vps] {
            assert!(n.observe(r, None).is_some());
        }

        assert!(n.observe(&row_ep(boundary, "YT A"), None).is_none());
        // Push-side rows BEFORE the rescue rows: a RescueActivated row is
        // itself a live-pusher scope end for its endpoint.
        assert!(
            n.observe(&a_push, None).is_some(),
            "{boundary:?} for A re-arms A's push-side invariant"
        );
        assert!(
            n.observe(&b_push, None).is_none(),
            "{boundary:?} for A must not touch B's invariant episode"
        );
        assert!(
            n.observe(&a_rescue, None).is_some(),
            "{boundary:?} for A re-arms A's rescue"
        );
        assert!(
            n.observe(&b_rescue, None).is_none(),
            "{boundary:?} for A must not touch B's rescue episode"
        );
        assert!(
            n.observe(&vps, None).is_none(),
            "{boundary:?} must not end the VPS reachability episode"
        );
    }
}

/// Entering rescue drops the endpoint's live pusher (the rescue clip runs on
/// a fresh one), so the dropped pusher's latched invariant episode ends with
/// it: a violation of the replacement pusher alerts again.
#[test]
fn entering_rescue_ends_the_live_pushers_invariant_episode() {
    let mut n = notifier();
    let a_push = row_av(Action::AvInvariantViolated, "push", Some("YT A"));
    let b_push = row_av(Action::AvInvariantViolated, "push", Some("FB B"));
    assert!(n.observe(&a_push, None).is_some());
    assert!(n.observe(&b_push, None).is_some());

    assert!(
        n.observe(&row_ep(Action::RescueActivated, "YT A"), None)
            .is_some(),
        "entering rescue still alerts as a rescue onset"
    );
    assert!(
        n.observe(&a_push, None).is_some(),
        "the replacement pusher's violation on A alerts again"
    );
    assert!(
        n.observe(&b_push, None).is_none(),
        "B's live pusher was not dropped: its episode stays open"
    );
}

/// An event boundary re-arms S3 upload failures, and only those.
#[test]
fn an_event_boundary_rearms_s3_upload_failures() {
    for boundary in EVENT_BOUNDARIES {
        let mut n = notifier();
        assert!(n.observe(&s3_permanent(), None).is_some());
        assert!(n.observe(&s3_permanent(), None).is_none(), "deduped");
        assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());

        assert!(n.observe(&row(boundary), None).is_none());
        assert!(
            n.observe(&s3_permanent(), None).is_some(),
            "{boundary:?}: the next event's S3 failure alerts again"
        );
        assert!(
            n.observe(&row(Action::VpsUnreachable), None).is_none(),
            "{boundary:?} must not end the VPS reachability episode"
        );
    }
}

/// `HostInternetRecovered` ends the host-level families it plausibly cured
/// (internet, VPS reachability, S3 upload) with ONE recovered alert.
#[test]
fn an_internet_recovery_ends_the_vps_and_s3_episodes_with_one_alert() {
    let mut n = notifier();
    assert!(n.observe(&s3_permanent(), None).is_some());
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
    assert!(
        n.observe(&row(Action::HostInternetRecovered), None)
            .is_some()
    );
    assert!(
        n.observe(&row(Action::HostInternetRecovered), None)
            .is_none(),
        "nothing left open: no second recovered alert"
    );
    assert!(n.observe(&s3_permanent(), None).is_some());
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
}

/// #311 holds for lifecycle rows too: a CI delivery never closes a real
/// event's episodes.
#[test]
fn a_ci_event_lifecycle_row_never_closes_a_real_episode() {
    let mut n = notifier();
    let rescue_on = row_ep(Action::RescueActivated, "YT A");
    assert!(n.observe(&rescue_on, None).is_some());
    assert!(
        n.observe(&row(Action::DeliveryStopped), Some("E2E-Test"))
            .is_none()
    );
    assert!(
        n.observe(&rescue_on, None).is_none(),
        "the real rescue episode is still open"
    );
}

/// B2: the operator's force-start override re-records `IngestSkewDetected`
/// with `state: "override"`. Since #367 it can be written while only the
/// ingest INVARIANT guard is latched, so it must never open an IngestSkew
/// episode (nothing would close it) nor send "restart OBS" for a
/// Restreamer-side fault. It is an audit record of a bypass, not an onset.
#[test]
fn an_ingest_skew_override_row_never_alerts_or_opens_an_episode() {
    let mut n = notifier();
    let override_row = row_detail(
        Action::IngestSkewDetected,
        serde_json::json!({ "skew_ms": 700, "threshold_ms": 2_000, "state": "override" }),
    );
    assert!(n.observe(&override_row, None).is_none());
    assert!(
        n.episodes.is_empty(),
        "an override row must not open an episode"
    );

    let detected = row_detail(
        Action::IngestSkewDetected,
        serde_json::json!({ "skew_ms": 2_500, "threshold_ms": 2_000, "state": "detected" }),
    );
    assert!(n.observe(&detected, None).is_some());
    assert!(n.observe(&override_row, None).is_none());
    assert!(n.observe(&row(Action::IngestSkewRecovered), None).is_some());
}

// ---- round-2 review --------------------------------------------------

/// Finding 3: a start request on a LIVE delivery reuses its instance but
/// still records `DeliveryStarted` / re-emits `VpsReady`. Those start edges
/// must not close the live delivery's episodes (its later `RescueRecovered`
/// would then send no "recovered").
#[test]
fn a_start_edge_on_a_live_delivery_keeps_its_episodes() {
    for edge in DELIVERY_START_EDGES {
        let mut n = notifier();
        let rescue_on = row_ep(Action::RescueActivated, "YT A");
        let push = row_av(Action::AvInvariantViolated, "push", Some("FB B"));
        let vps = row(Action::VpsUnreachable);
        for r in [&rescue_on, &push, &vps] {
            assert!(n.observe(r, None).is_some());
        }
        assert!(n.observe(&row(edge), None).is_none());
        assert!(
            n.observe(&push, None).is_none(),
            "{edge:?} must not end the live push-side episode"
        );
        assert!(
            n.observe(&vps, None).is_none(),
            "{edge:?} must not end the live VPS reachability episode"
        );
        assert!(
            n.observe(&row_ep(Action::RescueRecovered, "YT A"), None)
                .is_some(),
            "{edge:?}: the live rescue still ends with its own 'recovered'"
        );
    }
}

/// Finding 1: the delivery monitor's `VpsReachable` is the paired recovery
/// of `VpsUnreachable`. Without it a self-healed VPS blip kept the episode
/// open for the rest of the delivery and a later real VPS death was deduped.
#[test]
fn a_vps_recovery_ends_only_the_vps_reachability_episode() {
    let mut n = notifier();
    let vps = row_detail(
        Action::VpsUnreachable,
        serde_json::json!({ "consecutive_failures": 3 }),
    );
    let internet = row(Action::HostInternetUnreachable);
    assert!(n.observe(&vps, None).is_some());
    assert!(n.observe(&internet, None).is_some());
    assert!(
        n.observe(&row(Action::VpsReachable), None).is_some(),
        "the VPS is back: one 'recovered' alert"
    );
    assert!(
        n.observe(&row(Action::VpsReachable), None).is_none(),
        "no second 'recovered' for a closed episode"
    );
    assert!(
        n.observe(&vps, None).is_some(),
        "a later VPS outage in the same delivery alerts again"
    );
    assert!(
        n.observe(&internet, None).is_none(),
        "a VPS recovery must not end the host internet episode"
    );
}

/// Finding 2: a session reset CLEARS the ingest skew latch, but nothing has
/// measured the source back in sync (the re-baselined detector absorbs a
/// persisting constant offset). Its `IngestSkewRecovered` (`state: "reset"`)
/// therefore ends the episode SILENTLY: no false "znova zosynchronizované",
/// and the next desync alerts again.
#[test]
fn a_reset_recovery_closes_the_skew_episode_silently() {
    let mut n = notifier();
    let detected = row(Action::IngestSkewDetected);
    let reset = row_detail(
        Action::IngestSkewRecovered,
        serde_json::json!({ "skew_ms": 2_500, "threshold_ms": 2_000, "state": "reset" }),
    );
    assert!(n.observe(&detected, None).is_some());
    assert!(
        n.observe(&reset, None).is_none(),
        "a reset is not a measured recovery: no 'synced again' alert"
    );
    assert!(n.episodes.is_empty(), "but the episode is closed");
    assert!(
        n.observe(&detected, None).is_some(),
        "the next desync alerts again"
    );
    let measured = row_detail(
        Action::IngestSkewRecovered,
        serde_json::json!({ "skew_ms": 100, "threshold_ms": 2_000, "state": "recovered" }),
    );
    assert!(
        n.observe(&measured, None).is_some(),
        "a MEASURED recovery still alerts"
    );
}
