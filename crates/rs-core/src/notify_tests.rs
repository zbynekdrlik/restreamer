//! Tests for `notify.rs`. Loaded via `#[cfg(test)] #[path = "notify_tests.rs"] mod tests;`
//! to keep the production file under the 1000-line CI gate.

use super::*;
use crate::audit::{Severity, Source};
use serde_json::Value;

fn row(action: Action) -> AuditRow {
    AuditRow {
        severity: Severity::Warn,
        source: Source::System,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action,
        detail: serde_json::json!({}),
        ts_override: None,
    }
}

fn row_ep(action: Action, ep: &str) -> AuditRow {
    AuditRow {
        endpoint: Some(ep.to_string()),
        ..row(action)
    }
}

/// Build a row carrying a specific `detail` payload — the discriminator
/// #315 keys on to tell a real outage from telemetry / transient noise for
/// the same action.
fn row_detail(action: Action, detail: Value) -> AuditRow {
    AuditRow {
        detail,
        ..row(action)
    }
}

/// An A/V invariant edge built by the SAME row builders both stages use
/// (`av_invariant_violated_row` / `av_invariant_restored_row`, #367), plus
/// the endpoint alias a push-side row carries (`rs-delivery`
/// `emit_av_invariant_event`; ingest rows have none). Building the detail
/// here by hand would let a change to the builders' `stage` key slip past
/// the episode tests.
fn row_av(action: Action, stage: &str, ep: Option<&str>) -> AuditRow {
    let (severity, built, detail) = match action {
        Action::AvInvariantViolated => {
            crate::audit::av_invariant_violated_row(stage, 3_300, 4_000, -700, 50)
        }
        Action::AvInvariantRestored => crate::audit::av_invariant_restored_row(stage, 3),
        other => panic!("row_av builds A/V invariant rows only, got {other:?}"),
    };
    AuditRow {
        severity,
        endpoint: ep.map(str::to_string),
        action: built,
        detail,
        ..row(action)
    }
}

/// Notifier state constructed directly so the pure `observe` core can be
/// exercised without a live webhook / bot endpoint.
fn notifier() -> OutageNotifier {
    OutageNotifier {
        sink: AlertSink::Webhook {
            url: "http://127.0.0.1:1/unused".to_string(),
        },
        client: reqwest::Client::new(),
        episodes: Episodes::default(),
    }
}

#[test]
fn classify_maps_outage_and_recovery_actions() {
    assert!(matches!(
        classify(Action::VpsUnreachable),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::S3UploadFailed),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::HostInternetUnreachable),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::RescueActivated),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::RescueRecovered),
        Some(Signal::Recovery(..))
    ));
    assert!(matches!(
        classify(Action::HostInternetRecovered),
        Some(Signal::Recovery(..))
    ));
    // #354: the ingest A/V-skew banner must alert too — the incident it
    // fixes is precisely a source desync that alerted NO ONE.
    assert!(matches!(
        classify(Action::IngestSkewDetected),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::IngestSkewRecovered),
        Some(Signal::Recovery(..))
    ));
    // Unrelated actions are ignored.
    assert!(classify(Action::EventStarted).is_none());
    assert!(classify(Action::DiskCachePushSample).is_none());
}

#[test]
fn ingest_skew_onset_alerts_then_recovery_rearms() {
    let mut n = notifier();
    // Detected fires once, the per-chunk-boundary repeat is deduped.
    assert!(
        n.observe(&row(Action::IngestSkewDetected), None).is_some(),
        "first IngestSkewDetected must alert (#354 -- the incident this fixes alerted no one)"
    );
    assert!(n.observe(&row(Action::IngestSkewDetected), None).is_none());
    // The operator's `force:true` override re-fires the SAME onset action
    // (delivery_handlers.rs) -- also a dedup, not a second alert.
    assert!(n.observe(&row(Action::IngestSkewDetected), None).is_none());
    // Recovery clears the episode and re-arms.
    assert!(n.observe(&row(Action::IngestSkewRecovered), None).is_some());
    assert!(
        n.observe(&row(Action::IngestSkewDetected), None).is_some(),
        "a NEW desync after recovery must alert again"
    );
}

/// Every action `classify()` routes to an alert has its OWN operator text
/// (a deleted `slovak_text` arm would fall into `_ => ""` and post an
/// empty Discord message), and the texts are distinct.
#[test]
fn every_classified_action_has_its_own_text() {
    let routed = [
        Action::VpsUnreachable,
        Action::S3UploadFailed,
        Action::HostInternetUnreachable,
        Action::RescueActivated,
        Action::IngestSkewDetected,
        Action::AvInvariantViolated,
        Action::RescueRecovered,
        Action::HostInternetRecovered,
        Action::IngestSkewRecovered,
        Action::AvInvariantRestored,
        Action::LongStreamWarning,
        Action::VpsReachable,
    ];
    let mut seen = std::collections::HashSet::new();
    for action in routed {
        assert!(classify(action).is_some(), "{action:?} must be routed");
        let text = slovak_text(action);
        assert!(!text.is_empty(), "{action:?} has no operator text");
        assert!(seen.insert(text), "{action:?} reuses another action's text");
    }
}

/// #367: an absolute A/V invariant violation (any stage: ingest chunker
/// or VPS pusher) must reach the operator's Discord like the #354 ingest
/// skew, and its Restored edge closes the episode and re-arms.
#[test]
fn av_invariant_violation_alerts_then_restored_rearms() {
    assert!(matches!(
        classify(Action::AvInvariantViolated),
        Some(Signal::Onset(..))
    ));
    assert!(matches!(
        classify(Action::AvInvariantRestored),
        Some(Signal::Recovery(..))
    ));
    assert!(!slovak_text(Action::AvInvariantViolated).is_empty());
    assert!(!slovak_text(Action::AvInvariantRestored).is_empty());

    let mut n = notifier();
    let violated = row_av(Action::AvInvariantViolated, "push", Some("YT NLW 4k"));
    let restored = row_av(Action::AvInvariantRestored, "push", Some("YT NLW 4k"));
    let alert = n
        .observe(&violated, None)
        .expect("first AvInvariantViolated must alert");
    assert!(
        alert.content.contains("YT NLW 4k"),
        "a push-side violation names its endpoint: {}",
        alert.content
    );
    assert!(
        n.observe(&violated, None).is_none(),
        "deduped within the episode"
    );
    assert!(n.observe(&restored, None).is_some());
    assert!(
        n.observe(&violated, None).is_some(),
        "a new violation after Restored alerts again"
    );
}

// ---- #367 ROZHODNUTE item 1: episodes keyed by (family, stage, endpoint) ----
// One global episode let endpoint A's recovery clear the alert while
// endpoint B was still violated, and B's edge-triggered guard never
// re-alerted: a false all-clear. Each (family, stage, endpoint) now alerts
// and recovers on its own.

/// The exact ROZHODNUTE scenario: A violated, B violated, A restored. B is
/// still in its own alert episode, and B's later restore emits its own
/// "restored".
#[test]
fn av_invariant_episodes_are_keyed_per_endpoint() {
    let mut n = notifier();
    let a_violated = row_av(Action::AvInvariantViolated, "push", Some("YT A"));
    let a_restored = row_av(Action::AvInvariantRestored, "push", Some("YT A"));
    let b_violated = row_av(Action::AvInvariantViolated, "push", Some("FB B"));
    let b_restored = row_av(Action::AvInvariantRestored, "push", Some("FB B"));

    assert!(n.observe(&a_violated, None).is_some(), "A violated alerts");
    let b_alert = n
        .observe(&b_violated, None)
        .expect("B violated alerts on its own, not deduped by A's episode");
    assert!(b_alert.content.contains("FB B"), "{}", b_alert.content);
    assert!(n.observe(&a_restored, None).is_some(), "A restored alerts");
    assert!(
        n.observe(&b_violated, None).is_none(),
        "A's restore must not end B's episode: B's repeat stays deduped"
    );
    let b_restored_alert = n
        .observe(&b_restored, None)
        .expect("B's own restore emits its own restored");
    assert!(
        b_restored_alert.content.contains("FB B"),
        "{}",
        b_restored_alert.content
    );
    assert!(
        n.observe(&b_restored, None).is_none(),
        "no second restored once B's episode is closed"
    );
    assert!(
        n.observe(&a_violated, None).is_some(),
        "A's restore re-armed A"
    );
}

/// The ingest stage (no endpoint) and a push endpoint are separate
/// episodes of the same family.
#[test]
fn av_invariant_ingest_and_push_episodes_are_independent() {
    let mut n = notifier();
    let ingest_violated = row_av(Action::AvInvariantViolated, "ingest", None);
    let ingest_restored = row_av(Action::AvInvariantRestored, "ingest", None);
    let push_violated = row_av(Action::AvInvariantViolated, "push", Some("YT A"));
    let push_restored = row_av(Action::AvInvariantRestored, "push", Some("YT A"));

    assert!(n.observe(&ingest_violated, None).is_some());
    assert!(
        n.observe(&push_violated, None).is_some(),
        "a push violation is its own episode, not a repeat of the ingest one"
    );
    assert!(n.observe(&ingest_restored, None).is_some());
    assert!(
        n.observe(&push_violated, None).is_none(),
        "the ingest restore must not end the push episode"
    );
    assert!(n.observe(&push_restored, None).is_some());
}

/// The same contract for the rescue family: each endpoint enters and leaves
/// rescue on its own.
#[test]
fn rescue_episodes_are_keyed_per_endpoint() {
    let mut n = notifier();
    let a_on = row_ep(Action::RescueActivated, "YT A");
    let b_on = row_ep(Action::RescueActivated, "FB B");
    let a_off = row_ep(Action::RescueRecovered, "YT A");
    let b_off = row_ep(Action::RescueRecovered, "FB B");

    assert!(n.observe(&a_on, None).is_some());
    assert!(
        n.observe(&b_on, None).is_some(),
        "B entering rescue alerts on its own"
    );
    assert!(n.observe(&a_off, None).is_some());
    assert!(
        n.observe(&b_on, None).is_none(),
        "A's recovery must not end B's rescue episode"
    );
    assert!(
        n.observe(&b_off, None).is_some(),
        "B's own recovery emits its own recovered"
    );
    assert!(
        n.observe(&a_off, None).is_none(),
        "A's episode is already closed"
    );
}

/// A recovery ends only its OWN family's episode, on the SAME endpoint.
#[test]
fn a_recovery_ends_only_its_own_family() {
    let mut n = notifier();
    let rescue_on = row_ep(Action::RescueActivated, "YT A");
    let rescue_off = row_ep(Action::RescueRecovered, "YT A");
    let av_violated = row_av(Action::AvInvariantViolated, "push", Some("YT A"));
    let av_restored = row_av(Action::AvInvariantRestored, "push", Some("YT A"));
    let rescue_key = EpisodeKey::of(Family::Rescue, &rescue_on);

    assert!(n.observe(&rescue_on, None).is_some());
    assert!(n.observe(&av_violated, None).is_some());
    assert!(n.observe(&row(Action::IngestSkewDetected), None).is_some());
    assert!(n.observe(&row(Action::IngestSkewRecovered), None).is_some());
    // Looked up directly: re-observing `rescue_on` would ALSO end A's live
    // pusher scope (notify_scope_tests.rs) and close the invariant episode.
    assert!(
        n.episodes.open.contains_key(&rescue_key),
        "an ingest-skew recovery must not end the rescue episode"
    );
    assert!(
        n.observe(&av_violated, None).is_none(),
        "an ingest-skew recovery must not end the A/V invariant episode"
    );
    assert!(n.observe(&rescue_off, None).is_some());
    assert!(
        n.observe(&av_violated, None).is_none(),
        "a rescue recovery must not end the A/V invariant episode"
    );
    assert!(n.observe(&av_restored, None).is_some());
}

/// The host-level families (internet egress, VPS reachability, S3 upload)
/// are NOT ended by a per-endpoint rescue recovery; `HostInternetRecovered`
/// ends all three. (Their scope ends are in notify_scope_tests.rs.)
#[test]
fn host_level_episodes_are_not_ended_by_a_rescue_recovery() {
    let mut n = notifier();
    assert!(
        n.observe(&row(Action::HostInternetUnreachable), None)
            .is_some()
    );
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
    assert!(
        n.observe(&row_ep(Action::RescueActivated, "YT A"), None)
            .is_some()
    );
    assert!(
        n.observe(&row_ep(Action::RescueRecovered, "YT A"), None)
            .is_some()
    );
    assert!(
        n.observe(&row(Action::HostInternetUnreachable), None)
            .is_none(),
        "a rescue recovery must not end the host internet episode"
    );
    assert!(
        n.observe(&row(Action::VpsUnreachable), None).is_none(),
        "a rescue recovery must not end the VPS reachability episode"
    );
    assert!(
        n.observe(&row(Action::HostInternetRecovered), None)
            .is_some()
    );
    assert!(
        n.observe(&row(Action::VpsUnreachable), None).is_some(),
        "the internet recovery also ended the VPS reachability episode"
    );
}

#[test]
fn first_onset_alerts_then_dedups_within_episode() {
    let mut n = notifier();
    // First VpsUnreachable fires.
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
    // The per-retry storm is suppressed (edge-triggered).
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_none());
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_none());
}

#[test]
fn distinct_onsets_each_alert_once_in_one_episode() {
    let mut n = notifier();
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
    // A different transition (S3 upload failing) is its own alert.
    assert!(n.observe(&row(Action::S3UploadFailed), None).is_some());
    // But repeats of either are still deduped.
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_none());
    assert!(n.observe(&row(Action::S3UploadFailed), None).is_none());
}

#[test]
fn recovery_fires_only_when_in_outage() {
    let mut n = notifier();
    // Recovery with no active outage must NOT fire a spurious "all-clear".
    assert!(n.observe(&row(Action::RescueRecovered), None).is_none());
    // Now enter an outage, then recover.
    assert!(n.observe(&row(Action::RescueActivated), None).is_some());
    assert!(n.observe(&row(Action::RescueRecovered), None).is_some());
    // Recovery again with no active outage is suppressed.
    assert!(n.observe(&row(Action::RescueRecovered), None).is_none());
}

#[test]
fn recovery_resets_and_rearms_onset_alerts() {
    let mut n = notifier();
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_none()); // deduped
    assert!(
        n.observe(&row(Action::HostInternetRecovered), None)
            .is_some()
    ); // recover
    // A genuinely new outage after recovery must alert again.
    assert!(n.observe(&row(Action::VpsUnreachable), None).is_some());
}

#[test]
fn unrelated_action_does_not_touch_state() {
    let mut n = notifier();
    assert!(n.observe(&row(Action::EventStarted), None).is_none());
    assert!(n.episodes.is_empty());
    // A real onset right after still fires (state was untouched).
    assert!(n.observe(&row(Action::S3UploadFailed), None).is_some());
}

#[test]
fn is_e2e_event_name_matches_ci_events_only() {
    // The two CI events ci.yml creates (contract with #311).
    assert!(is_e2e_event_name("E2E-Test"));
    assert!(is_e2e_event_name("E2E-FB-Test"));
    // Any future E2E-prefixed CI event is covered.
    assert!(is_e2e_event_name("E2E-Whatever"));
    // Real operator events are not.
    assert!(!is_e2e_event_name("Nedeľná bohoslužba"));
    assert!(!is_e2e_event_name("Sunday E2E recap")); // "E2E" mid-name, not a CI event
    assert!(!is_e2e_event_name(""));
}

#[test]
fn is_outage_relevant_gates_the_name_lookup() {
    let n = notifier();
    // Outage onset + recovery actions are relevant (the writer will resolve
    // the event name for these to drive #311 suppression).
    assert!(n.is_outage_relevant(&row(Action::VpsUnreachable)));
    assert!(n.is_outage_relevant(&row(Action::RescueActivated)));
    assert!(n.is_outage_relevant(&row(Action::RescueRecovered)));
    // Everything else is not — the writer skips the DB lookup entirely.
    // (Lifecycle rows such as EventStarted ARE relevant since #367: they end
    // episode scopes, see notify_scope_tests.rs.)
    assert!(!n.is_outage_relevant(&row(Action::ConfigChanged)));
    assert!(!n.is_outage_relevant(&row(Action::DiskCachePushSample)));
}

#[test]
fn e2e_named_event_is_suppressed_real_still_alerts() {
    // #311: CI test events (E2E-*) deliberately trigger outage edges several
    // times per run; they must NOT reach the operator's alert channel.
    let mut n = notifier();
    assert!(
        n.observe(&row(Action::RescueActivated), Some("E2E-Test"))
            .is_none(),
        "E2E-Test event must be suppressed"
    );
    assert!(
        n.observe(&row(Action::VpsUnreachable), Some("E2E-FB-Test"))
            .is_none(),
        "E2E-FB-Test event must be suppressed"
    );
    // A real (non-E2E) event still alerts.
    let mut real = notifier();
    assert!(
        real.observe(&row(Action::RescueActivated), Some("Nedeľná bohoslužba"))
            .is_some(),
        "a real event must still alert"
    );
    // A host-level row with no event name is never suppressed on that basis.
    let mut host = notifier();
    assert!(
        host.observe(&row(Action::HostInternetUnreachable), None)
            .is_some(),
        "host-level (no event name) signal must still alert"
    );
}

#[test]
fn e2e_suppression_does_not_touch_episode_state() {
    // A suppressed E2E onset must not flip the notifier into an outage
    // episode, so it can never disturb a real outage's dedup/episode state.
    let mut n = notifier();
    assert!(
        n.observe(&row(Action::VpsUnreachable), Some("E2E-Test"))
            .is_none()
    );
    assert!(
        n.episodes.is_empty(),
        "E2E event must not start an outage episode"
    );
    // A subsequent REAL onset still alerts (state was untouched).
    assert!(
        n.observe(&row(Action::VpsUnreachable), Some("Real Event"))
            .is_some()
    );
}

// ---- #315: alert on the DETAIL, not just the action ----------------
// The 2026-07-23 live event fired FALSE outage alerts because `classify`
// keyed only on the `Action` enum. The same action can mean "stream is
// down" or "harmless telemetry/transient blip" depending on its `detail`
// payload; these tests pin the discriminators.

#[test]
fn vps_unreachable_mirror_phase_is_suppressed() {
    // `delivery_audit_mirror` emits VpsUnreachable with detail.phase=="mirror"
    // when the host fails to PULL the VPS audit log. Delivery is unaffected —
    // it is telemetry-only and must NOT alert (2026-07-23 15:33 + 16:33 FALSE).
    let mut n = notifier();
    assert!(
        n.observe(
            &row_detail(
                Action::VpsUnreachable,
                serde_json::json!({ "phase": "mirror", "error": "timeout" }),
            ),
            None,
        )
        .is_none(),
        "mirror-phase VpsUnreachable is telemetry-only and must not alert (#315)"
    );
    assert!(
        n.episodes.is_empty(),
        "a suppressed mirror poll must not start an outage episode"
    );
}

#[test]
fn vps_unreachable_health_monitor_alerts_only_at_threshold() {
    // The delivery health monitor emits VpsUnreachable with
    // detail.consecutive_failures and only treats it as a real problem at
    // >= 3 (delivery_monitor.rs). The alert must mirror that threshold — the
    // first 1-2 transient failures must NOT alert.
    let mut n1 = notifier();
    assert!(
        n1.observe(
            &row_detail(
                Action::VpsUnreachable,
                serde_json::json!({ "consecutive_failures": 1 }),
            ),
            None,
        )
        .is_none(),
        "consecutive_failures=1 is transient, must not alert (#315)"
    );
    let mut n2 = notifier();
    assert!(
        n2.observe(
            &row_detail(
                Action::VpsUnreachable,
                serde_json::json!({ "consecutive_failures": 2 }),
            ),
            None,
        )
        .is_none(),
        "consecutive_failures=2 is transient, must not alert (#315)"
    );
    let mut n3 = notifier();
    assert!(
        n3.observe(
            &row_detail(
                Action::VpsUnreachable,
                serde_json::json!({ "consecutive_failures": 3 }),
            ),
            None,
        )
        .is_some(),
        "consecutive_failures=3 is a real delivery outage, must alert (#315)"
    );
}

#[test]
fn s3_upload_failed_transient_suppressed_permanent_alerts() {
    // A single transient retry (permanent:false, attempt:1 — e.g. a 408
    // timeout the uploader retries) must NOT alert (2026-07-23 17:16 FALSE).
    let mut n1 = notifier();
    assert!(
        n1.observe(
            &row_detail(
                Action::S3UploadFailed,
                serde_json::json!({ "permanent": false, "attempt": 1, "error_class": "timeout" }),
            ),
            None,
        )
        .is_none(),
        "a single transient S3 retry must not alert (#315)"
    );
    assert!(
        n1.episodes.is_empty(),
        "a suppressed transient S3 failure must not start an outage episode"
    );
    // A permanent (terminal) failure IS a real problem and alerts.
    let mut n2 = notifier();
    assert!(
        n2.observe(
            &row_detail(
                Action::S3UploadFailed,
                serde_json::json!({ "permanent": true, "attempt": 5, "error_class": "forbidden" }),
            ),
            None,
        )
        .is_some(),
        "a permanent S3 failure is a real outage, must alert (#315)"
    );
}

#[test]
fn genuinely_real_onsets_still_alert_under_315() {
    // #315 must NOT change the genuinely-real signals: rescue activation and
    // host-internet loss were correct on 2026-07-23 and stay alerting.
    let mut r = notifier();
    assert!(
        r.observe(&row(Action::RescueActivated), None).is_some(),
        "RescueActivated must still alert (#315)"
    );
    let mut h = notifier();
    assert!(
        h.observe(&row(Action::HostInternetUnreachable), None)
            .is_some(),
        "HostInternetUnreachable must still alert (#315)"
    );
}

#[test]
fn build_alert_appends_endpoint_alias() {
    let a = build_alert(
        Action::RescueActivated,
        &row_ep(Action::RescueActivated, "YT-4K"),
    );
    assert!(a.content.contains("núdzové video"));
    assert!(a.content.contains("(endpoint: YT-4K)"));
    // Rows without an endpoint carry no alias suffix.
    let b = build_alert(Action::VpsUnreachable, &row(Action::VpsUnreachable));
    assert!(!b.content.contains("endpoint:"));
}

#[test]
fn from_config_disabled_when_empty_enabled_when_set() {
    // Nothing set (blank-but-present webhook) -> disabled.
    let empty = NotificationsConfig {
        discord_webhook_url: "   ".to_string(),
        ..Default::default()
    };
    assert!(OutageNotifier::from_config(&empty).is_none());

    // Webhook only -> webhook sink.
    let set = NotificationsConfig {
        discord_webhook_url: "https://discord.example/webhook/abc".to_string(),
        ..Default::default()
    };
    let n = OutageNotifier::from_config(&set).expect("webhook enables");
    assert!(matches!(n.sink, AlertSink::Webhook { .. }));
}

#[test]
fn from_config_bot_mode_when_both_bot_fields_set() {
    let cfg = NotificationsConfig {
        discord_bot_token: "  tok-123  ".to_string(),
        discord_channel_id: "  1373592666733940816 ".to_string(),
        ..Default::default()
    };
    let n = OutageNotifier::from_config(&cfg).expect("bot fields enable");
    // Whitespace is trimmed off both fields.
    assert_eq!(
        n.sink,
        AlertSink::Bot {
            token: "tok-123".to_string(),
            channel_id: "1373592666733940816".to_string(),
        }
    );
}

#[test]
fn from_config_bot_needs_both_fields() {
    // Token without channel -> not bot mode (and no webhook) -> disabled.
    let token_only = NotificationsConfig {
        discord_bot_token: "tok-123".to_string(),
        ..Default::default()
    };
    assert!(OutageNotifier::from_config(&token_only).is_none());
    // Channel without token -> disabled.
    let channel_only = NotificationsConfig {
        discord_channel_id: "123".to_string(),
        ..Default::default()
    };
    assert!(OutageNotifier::from_config(&channel_only).is_none());
}

#[test]
fn from_config_bot_wins_when_both_bot_and_webhook_set() {
    let cfg = NotificationsConfig {
        discord_webhook_url: "https://discord.example/webhook/abc".to_string(),
        discord_bot_token: "tok-123".to_string(),
        discord_channel_id: "1373592666733940816".to_string(),
    };
    let n = OutageNotifier::from_config(&cfg).expect("enabled");
    assert!(
        matches!(n.sink, AlertSink::Bot { .. }),
        "bot mode must win over webhook when both are set"
    );
}

/// Drive `post_alert` against a one-shot mock HTTP server (mocking the
/// external Discord webhook is allowed) and assert it POSTs the expected
/// JSON body.
#[tokio::test]
async fn post_alert_posts_json_content_to_webhook() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let mut data: Vec<u8> = Vec::new();
        loop {
            let n = sock.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buf[..n]);
            let s = String::from_utf8_lossy(&data);
            if let Some(hdr_end) = s.find("\r\n\r\n") {
                let content_len = s
                    .lines()
                    .find_map(|l| {
                        let ll = l.to_ascii_lowercase();
                        ll.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if data.len() >= hdr_end + 4 + content_len {
                    break;
                }
            }
        }
        // Minimal success response (Discord returns 204 No Content).
        sock.write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap();
        let s = String::from_utf8_lossy(&data).to_string();
        let body = s.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        let _ = tx.send(body);
    });

    let client = reqwest::Client::new();
    let alert = DiscordAlert {
        content: "TEST výpadok".to_string(),
    };
    post_alert(&client, &format!("http://{addr}/webhook"), &alert)
        .await
        .unwrap();

    let body = rx.await.unwrap();
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["content"], "TEST výpadok");
}

/// Drive `post_alert_bot` against a one-shot mock HTTP server and assert it
/// POSTs to `channels/{id}/messages` with the `Authorization: Bot <token>`
/// header and the `{"content": ...}` JSON body (#306). Captures the FULL raw
/// request (request line + headers + body).
#[tokio::test]
async fn post_alert_bot_posts_to_channel_with_bot_auth() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let mut data: Vec<u8> = Vec::new();
        loop {
            let n = sock.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buf[..n]);
            let s = String::from_utf8_lossy(&data);
            if let Some(hdr_end) = s.find("\r\n\r\n") {
                let content_len = s
                    .lines()
                    .find_map(|l| {
                        let ll = l.to_ascii_lowercase();
                        ll.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if data.len() >= hdr_end + 4 + content_len {
                    break;
                }
            }
        }
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let _ = tx.send(String::from_utf8_lossy(&data).to_string());
    });

    let client = reqwest::Client::new();
    let alert = DiscordAlert {
        content: "TEST výpadok".to_string(),
    };
    post_alert_bot(
        &client,
        &format!("http://{addr}"),
        "1373592666733940816",
        "tok-secret",
        &alert,
    )
    .await
    .unwrap();

    let raw = rx.await.unwrap();
    // Request targets the thread-as-channel messages endpoint.
    assert!(
        raw.starts_with("POST /channels/1373592666733940816/messages "),
        "unexpected request line in:\n{raw}"
    );
    // Bot-token authorization header is present (case-insensitive header name).
    let has_bot_auth = raw.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("authorization:") && l.contains("Bot tok-secret")
    });
    assert!(
        has_bot_auth,
        "missing 'Authorization: Bot <token>' in:\n{raw}"
    );
    // Body carries the alert content as JSON.
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("");
    let v: Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["content"], "TEST výpadok");
}

// #84: the long-stream warning is a STANDALONE heads-up — it must fire an
// alert but never enter outage-episode state, so it can neither be cleared
// by a later recovery nor blocked by an outage's dedup.
#[test]
fn long_stream_warning_is_standalone_not_an_outage() {
    assert!(matches!(
        classify(Action::LongStreamWarning),
        Some(Signal::Standalone(_))
    ));

    let mut n = notifier();
    // Fires the heads-up...
    assert!(n.observe(&row(Action::LongStreamWarning), None).is_some());
    // ...but does NOT flip the notifier into an outage episode.
    assert!(
        n.episodes.is_empty(),
        "standalone warning must not open an outage episode or touch its dedup set"
    );

    // A subsequent recovery therefore emits NO spurious 'recovered'.
    assert!(
        n.observe(&row(Action::RescueRecovered), None).is_none(),
        "no episode was active, so recovery must be silent"
    );

    // The emitter owns dedup (once per delivery), so the notifier itself
    // does NOT suppress a second standalone warning.
    assert!(n.observe(&row(Action::LongStreamWarning), None).is_some());
}

// #311: a CI test event's long-stream warning must never reach the
// operator alert channel (same E2E-name suppression as outage onsets).
#[test]
fn long_stream_warning_suppressed_for_e2e_event() {
    let mut n = notifier();
    assert!(
        n.observe(&row(Action::LongStreamWarning), Some("E2E-Test"))
            .is_none()
    );
}

// #367 review B1/B2: episode scopes and the override row, in their own file
// for the 1000-line cap.
#[path = "notify_scope_tests.rs"]
mod scope;
