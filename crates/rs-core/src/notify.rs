//! Discord webhook outage notifier (#261).
//!
//! Fires an immediate, deduped Discord alert on each delivery-outage state
//! transition. It is hooked at the audit writer (`audit_writer_task`), so ONE
//! dispatcher covers BOTH host-side signals (`VpsUnreachable`,
//! `S3UploadFailed`, `HostInternetUnreachable`) and VPS-side signals mirrored
//! into the SAME audit channel by `delivery_audit_mirror` (`RescueActivated`,
//! `RescueRecovered`) — no duplication.
//!
//! Edge-triggered / deduped: an outage episode alerts once per distinct signal,
//! NOT once per retry. The audit `RateLimiter` already throttles the storm
//! actions to 1/min BEFORE the writer; this layer collapses a whole episode to
//! one alert per state transition. A recovery signal ends the episode and
//! re-arms the onset alerts so a genuinely new outage alerts again.
//!
//! Two delivery mechanisms (#306): a **bot token** posting to the Discord REST
//! API (`channels/{id}/messages` with an `Authorization: Bot <token>` header —
//! a thread IS a channel, so this targets the operator's alerts-snv thread, the
//! same pattern camera-box uses) OR the original **webhook** POST (#261). Bot
//! mode wins when both are configured. Disabled when neither is set (the
//! default), so the feature ships dark until the operator fills one in.
//! The token / webhook URL are runtime secrets — never committed to the repo.

use std::collections::HashSet;
use std::time::Duration;

use crate::audit::{Action, AuditRow};
use crate::config::NotificationsConfig;

/// A single Discord alert ready to POST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordAlert {
    pub content: String,
}

/// Classification of an audit action for outage alerting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    /// Outage onset — one alert per distinct onset action per episode.
    Onset(Action),
    /// Recovery / all-clear — ends the episode and re-arms onset alerts.
    Recovery(Action),
    /// #84: a standalone operator heads-up that is NOT part of outage-episode
    /// semantics — it fires an alert but never touches `in_outage`/`alerted`,
    /// so it cannot flip the notifier into a fake outage (which would make a
    /// later `RescueRecovered` emit a spurious "recovered"). The emitter
    /// guarantees its own once-per-occurrence dedup (the long-stream monitor
    /// arms once per delivery).
    Standalone(Action),
}

/// Route an audit action to an outage signal, or `None` if it is not
/// outage-relevant. Keep in sync with the emission sites verified for #261.
fn classify(action: Action) -> Option<Signal> {
    match action {
        Action::VpsUnreachable
        | Action::S3UploadFailed
        | Action::HostInternetUnreachable
        | Action::RescueActivated
        // #354: the ingest-side A/V-skew banner exists BECAUSE the 2026-08-30
        // incidents alerted no one ("žiadny alert nikam nešiel") — the source
        // (OBS) desync was only ever visible on the dashboard. Route it
        // through the SAME onset/recovery pairing as HostInternetUnreachable.
        | Action::IngestSkewDetected => Some(Signal::Onset(action)),
        Action::RescueRecovered
        | Action::HostInternetRecovered
        | Action::IngestSkewRecovered => Some(Signal::Recovery(action)),
        // #84: standalone heads-up, deliberately OUTSIDE the outage episode.
        Action::LongStreamWarning => Some(Signal::Standalone(action)),
        _ => None,
    }
}

/// #315: some onset ACTIONS are emitted for BOTH real outages and harmless
/// telemetry / transient noise — the discriminator is the row's `detail`
/// payload, not the action. Returns `true` when this onset row must be
/// SUPPRESSED (no alert) despite its action being outage-relevant.
///
/// Keying only on the action fired FALSE alerts during the 2026-07-23 live
/// event (the alert channel's first live use), so the classifier must consult
/// the detail. The rule is symmetric across the two ambiguous actions: suppress
/// ONLY when the detail POSITIVELY signals noise; anything else (including an
/// unknown/absent discriminator from a future emitter) keeps the safety net and
/// alerts, so a genuine outage is never silently dropped.
fn onset_suppressed_by_detail(action: Action, detail: &serde_json::Value) -> bool {
    match action {
        Action::VpsUnreachable => {
            // (1) `delivery_audit_mirror` — telemetry-only audit-log poll,
            //     detail.phase == "mirror". Delivery is unaffected → suppress.
            if detail.get("phase").and_then(|v| v.as_str()) == Some("mirror") {
                return true;
            }
            // (2) `delivery_monitor` — real delivery health poll,
            //     detail.consecutive_failures. The monitor itself only treats it
            //     as a real problem at >= 3 (delivery_monitor.rs); the first 1-2
            //     transient failures must not alert. Suppress while < 3.
            if let Some(cf) = detail.get("consecutive_failures").and_then(|v| v.as_i64()) {
                return cf < 3;
            }
            // Neither field (no known emitter) → keep the safety net, alert.
            false
        }
        Action::S3UploadFailed => {
            // A single transient retry (`permanent:false`, e.g. a 408 timeout the
            // uploader retries) is not an outage → suppress. A permanent
            // (terminal) failure alerts. We suppress on the POSITIVE transient
            // signal (permanent == false) rather than "alert only when
            // permanent == true": it is symmetric with VpsUnreachable above, and
            // every real emitter (uploader.rs) always sets `permanent`, so for
            // production traffic this is identical to permanent-only while a
            // genuinely sustained internet outage is still covered by the
            // HostInternetUnreachable / RescueActivated signals.
            detail.get("permanent").and_then(|v| v.as_bool()) == Some(false)
        }
        // Other onsets (HostInternetUnreachable, RescueActivated) carry no
        // detail-based discriminator and always alert.
        _ => false,
    }
}

/// True when an event name belongs to the CI E2E test events, whose deliberate
/// outage edges must never reach the operator's alert channel (#311). The two
/// CI events are `E2E-Test` and `E2E-FB-Test` (ci.yml owns the names); the
/// robust rule is the `E2E-` prefix so a future CI event named the same way is
/// covered automatically.
fn is_e2e_event_name(name: &str) -> bool {
    name.starts_with("E2E-")
}

/// Human, Slovak, operator-facing alert text for an outage action.
fn slovak_text(action: Action) -> &'static str {
    match action {
        Action::VpsUnreachable => {
            "⚠️ Výpadok spojenia so streamovacím serverom (VPS) — vysielanie je ohrozené, riešime."
        }
        Action::S3UploadFailed => {
            "⚠️ Nahrávanie streamu do cloudu zlyháva — pravdepodobne výpadok internetu na streamovacom PC."
        }
        Action::HostInternetUnreachable => {
            "⚠️ Streamovacie PC stratilo internet — vysielanie je ohrozené."
        }
        Action::RescueActivated => {
            "⚠️ Výpadok potvrdený — beží núdzové video (rescue). Diváci vidia náhradu, riešime."
        }
        Action::RescueRecovered => "✅ Spojenie obnovené — vysielanie pokračuje normálne.",
        Action::HostInternetRecovered => "✅ Internet na streamovacom PC obnovený.",
        Action::IngestSkewDetected => {
            "🔴 Zvuk a obraz z OBS sú rozídené — reštartuj stream v OBS. Delivery sa nedá spustiť, \
             kým to platí."
        }
        Action::IngestSkewRecovered => "✅ Zvuk a obraz z OBS sú znova zosynchronizované.",
        Action::LongStreamWarning => {
            "⏱️ Stream beží už veľmi dlho — over, či ho netreba ukončiť (možno zostal omylom zapnutý)."
        }
        // classify() only routes the nine actions above into this function.
        _ => "",
    }
}

/// Discord REST API base for bot-token posting (#306). A thread is addressed
/// as a channel: `POST {base}/channels/{id}/messages`. Split out as a const so
/// tests can drive [`post_alert_bot`] against a local mock base.
const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

/// Where an alert is delivered — bot token (#306) or webhook (#261).
#[derive(Debug, Clone, PartialEq, Eq)]
enum AlertSink {
    /// Bot token → REST API `channels/{id}/messages` with `Authorization: Bot`.
    Bot { token: String, channel_id: String },
    /// Legacy webhook URL → `{"content": ...}` POST.
    Webhook { url: String },
}

/// Edge-triggered Discord outage notifier. Built from config; owned mutably by
/// the audit writer task, which calls [`observe`](Self::observe) on each row.
pub struct OutageNotifier {
    sink: AlertSink,
    client: reqwest::Client,
    /// Whether an outage episode is currently active.
    in_outage: bool,
    /// Onset actions already alerted during the current episode (dedup key).
    alerted: HashSet<Action>,
}

impl OutageNotifier {
    /// Build from config; returns `None` (disabled) when no delivery mechanism
    /// is configured. Bot mode (#306) wins when both `discord_bot_token` and
    /// `discord_channel_id` are set; otherwise the webhook URL (#261) is used
    /// when set; otherwise disabled. All comparisons ignore surrounding
    /// whitespace so a blank-but-present field still counts as unset.
    pub fn from_config(cfg: &NotificationsConfig) -> Option<Self> {
        let token = cfg.discord_bot_token.trim();
        let channel_id = cfg.discord_channel_id.trim();
        let webhook = cfg.discord_webhook_url.trim();

        let sink = if !token.is_empty() && !channel_id.is_empty() {
            AlertSink::Bot {
                token: token.to_string(),
                channel_id: channel_id.to_string(),
            }
        } else if !webhook.is_empty() {
            AlertSink::Webhook {
                url: webhook.to_string(),
            }
        } else {
            return None;
        };

        Some(Self {
            sink,
            client: reqwest::Client::new(),
            in_outage: false,
            alerted: HashSet::new(),
        })
    }

    /// Cheap pre-check: does this row carry an action the notifier would alert
    /// on? The audit writer uses it to gate the (rare) event-name DB lookup that
    /// drives #311 CI-event suppression, so non-outage rows cost nothing.
    pub fn is_outage_relevant(&self, row: &AuditRow) -> bool {
        classify(row.action).is_some()
    }

    /// Pure edge-trigger / dedup core. Returns `Some(alert)` when this row is a
    /// state transition that should fire, `None` otherwise. Mutates the episode
    /// state. HTTP-free, so it is directly unit-testable.
    ///
    /// `event_name` is the resolved name of `row.event_id` (the caller looks it
    /// up), used to suppress CI test events (#311). `None` means the row is not
    /// tied to a named event (e.g. host-level internet signals) and is never
    /// suppressed on that basis.
    pub fn observe(&mut self, row: &AuditRow, event_name: Option<&str>) -> Option<DiscordAlert> {
        let signal = classify(row.action)?;
        // #311: never alert for CI test events. The two CI events are E2E-Test
        // and E2E-FB-Test, whose OBS-disconnect / rescue-gate / network-drop
        // steps deliberately trigger outage edges several times per run; without
        // this they would spam the operator's alerts-snv thread and drown real
        // pings. Return BEFORE mutating episode state so a CI event can never
        // disturb the dedup/episode tracking of a genuine outage.
        if let Some(name) = event_name {
            if is_e2e_event_name(name) {
                tracing::debug!(
                    event = %name,
                    action = ?row.action,
                    "outage notifier: suppressing alert for CI test event (#311)"
                );
                return None;
            }
        }
        match signal {
            Signal::Onset(action) => {
                // #315: the SAME onset action can be a real outage or telemetry /
                // transient noise depending on its detail payload. Suppress the
                // noise BEFORE touching episode state (like the E2E gate above),
                // so a mirror-poll / transient blip can never flip the notifier
                // into an outage episode or disturb a real outage's dedup.
                if onset_suppressed_by_detail(action, &row.detail) {
                    tracing::debug!(
                        action = ?action,
                        detail = %row.detail,
                        "outage notifier: suppressing alert — detail signals telemetry/transient noise (#315)"
                    );
                    return None;
                }
                self.in_outage = true;
                // First occurrence of this onset in the episode alerts; repeats
                // (the per-retry storm) are suppressed.
                if self.alerted.insert(action) {
                    Some(build_alert(action, row))
                } else {
                    None
                }
            }
            Signal::Recovery(action) => {
                if self.in_outage {
                    self.in_outage = false;
                    self.alerted.clear();
                    Some(build_alert(action, row))
                } else {
                    // No spurious "recovered" when nothing was flagged as down.
                    None
                }
            }
            // #84: fire the heads-up without touching episode state. The
            // emitter (the long-stream monitor) already dedups to once per
            // delivery, so no per-episode `alerted` tracking is needed here.
            Signal::Standalone(action) => Some(build_alert(action, row)),
        }
    }

    /// Fire-and-forget POST of the alert to Discord; never blocks the writer.
    /// Routes to the bot REST endpoint (#306) or the webhook (#261) per the
    /// configured [`AlertSink`].
    pub fn spawn_dispatch(&self, alert: DiscordAlert) {
        let client = self.client.clone();
        let sink = self.sink.clone();
        tokio::spawn(async move {
            let res = match &sink {
                AlertSink::Bot { token, channel_id } => {
                    post_alert_bot(&client, DISCORD_API_BASE, channel_id, token, &alert).await
                }
                AlertSink::Webhook { url } => post_alert(&client, url, &alert).await,
            };
            if let Err(e) = res {
                tracing::warn!("discord outage alert POST failed: {e}");
            }
        });
    }
}

/// Compose the alert content: the Slovak transition text, plus the endpoint
/// alias when the row carries one.
fn build_alert(action: Action, row: &AuditRow) -> DiscordAlert {
    let mut content = slovak_text(action).to_string();
    if let Some(ep) = &row.endpoint {
        content.push_str(&format!(" (endpoint: {ep})"));
    }
    DiscordAlert { content }
}

/// POST a single alert to a Discord webhook URL (`{"content": ...}`), 10 s
/// timeout. Public so tests can drive it against a mock server.
pub async fn post_alert(
    client: &reqwest::Client,
    url: &str,
    alert: &DiscordAlert,
) -> reqwest::Result<()> {
    client
        .post(url)
        .json(&serde_json::json!({ "content": alert.content }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// POST a single alert via a Discord BOT TOKEN to `{api_base}/channels/{id}/
/// messages` (#306), with an `Authorization: Bot <token>` header and the same
/// `{"content": ...}` body, 10 s timeout. A thread is a channel, so `channel_id`
/// may be a thread id (the operator's alerts-snv thread). `api_base` is
/// [`DISCORD_API_BASE`] in production; tests pass a local mock base. Public so
/// tests can drive it against a mock server.
pub async fn post_alert_bot(
    client: &reqwest::Client,
    api_base: &str,
    channel_id: &str,
    token: &str,
    alert: &DiscordAlert,
) -> reqwest::Result<()> {
    client
        .post(format!("{api_base}/channels/{channel_id}/messages"))
        .header("Authorization", format!("Bot {token}"))
        .json(&serde_json::json!({ "content": alert.content }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[cfg(test)]
mod tests {
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

    /// Notifier state constructed directly so the pure `observe` core can be
    /// exercised without a live webhook / bot endpoint.
    fn notifier() -> OutageNotifier {
        OutageNotifier {
            sink: AlertSink::Webhook {
                url: "http://127.0.0.1:1/unused".to_string(),
            },
            client: reqwest::Client::new(),
            in_outage: false,
            alerted: HashSet::new(),
        }
    }

    #[test]
    fn classify_maps_outage_and_recovery_actions() {
        assert!(matches!(
            classify(Action::VpsUnreachable),
            Some(Signal::Onset(_))
        ));
        assert!(matches!(
            classify(Action::S3UploadFailed),
            Some(Signal::Onset(_))
        ));
        assert!(matches!(
            classify(Action::HostInternetUnreachable),
            Some(Signal::Onset(_))
        ));
        assert!(matches!(
            classify(Action::RescueActivated),
            Some(Signal::Onset(_))
        ));
        assert!(matches!(
            classify(Action::RescueRecovered),
            Some(Signal::Recovery(_))
        ));
        assert!(matches!(
            classify(Action::HostInternetRecovered),
            Some(Signal::Recovery(_))
        ));
        // #354: the ingest A/V-skew banner must alert too — the incident it
        // fixes is precisely a source desync that alerted NO ONE.
        assert!(matches!(
            classify(Action::IngestSkewDetected),
            Some(Signal::Onset(_))
        ));
        assert!(matches!(
            classify(Action::IngestSkewRecovered),
            Some(Signal::Recovery(_))
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
        assert!(!n.in_outage);
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
        assert!(!n.is_outage_relevant(&row(Action::EventStarted)));
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
        assert!(!n.in_outage, "E2E event must not start an outage episode");
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
            !n.in_outage,
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
            !n1.in_outage,
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
}
