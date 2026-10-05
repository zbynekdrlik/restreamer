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
//! Episodes are keyed by (family, stage, endpoint) (#367): a recovery ends
//! ONLY the episode of its own [`Family`] on its own stage and endpoint. One
//! global episode let endpoint A's `AvInvariantRestored` clear the alert while
//! endpoint B was still violated, and B's edge-triggered guard never
//! re-alerted: a false all-clear.
//!
//! Two delivery mechanisms (#306): a **bot token** posting to the Discord REST
//! API (`channels/{id}/messages` with an `Authorization: Bot <token>` header —
//! a thread IS a channel, so this targets the operator's alerts-snv thread, the
//! same pattern camera-box uses) OR the original **webhook** POST (#261). Bot
//! mode wins when both are configured. Disabled when neither is set (the
//! default), so the feature ships dark until the operator fills one in.
//! The token / webhook URL are runtime secrets — never committed to the repo.

use std::collections::{HashMap, HashSet};
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
    Onset(Action, Family),
    /// Recovery / all-clear — ends its own family's episode (on the row's
    /// stage + endpoint) and re-arms that episode's onset alerts.
    Recovery(Action, Family),
    /// #84: a standalone operator heads-up that is NOT part of outage-episode
    /// semantics — it fires an alert but never opens or ends an episode, so
    /// it cannot flip the notifier into a fake outage (which would make a
    /// later `RescueRecovered` emit a spurious "recovered"). The emitter
    /// guarantees its own once-per-occurrence dedup (the long-stream monitor
    /// arms once per delivery).
    Standalone(Action),
}

/// The outage condition an onset / recovery belongs to (#367). A recovery
/// ends only the episodes of its own family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Family {
    /// Host-level connectivity: internet egress, S3 upload, host -> VPS
    /// reachability. `VpsUnreachable` and `S3UploadFailed` have no recovery
    /// action of their own (the delivery monitor waits for the network), so
    /// `HostInternetRecovered` ends the whole family's episode.
    HostConnectivity,
    /// A VPS endpoint playing the rescue clip (per endpoint).
    Rescue,
    /// The source (OBS) A/V skew seen at ingest (#354).
    IngestSkew,
    /// The absolute A/V invariant (#367), per stage (`ingest` / `push`) and,
    /// on the push stage, per endpoint.
    AvInvariant,
}

/// Route an audit action to an outage signal, or `None` if it is not
/// outage-relevant. Keep in sync with the emission sites verified for #261.
fn classify(action: Action) -> Option<Signal> {
    let signal = match action {
        Action::VpsUnreachable | Action::S3UploadFailed | Action::HostInternetUnreachable => {
            Signal::Onset(action, Family::HostConnectivity)
        }
        Action::HostInternetRecovered => Signal::Recovery(action, Family::HostConnectivity),
        Action::RescueActivated => Signal::Onset(action, Family::Rescue),
        Action::RescueRecovered => Signal::Recovery(action, Family::Rescue),
        // #354: the ingest-side A/V-skew banner exists BECAUSE the 2026-08-30
        // incidents alerted no one ("žiadny alert nikam nešiel") — the source
        // (OBS) desync was only ever visible on the dashboard. Route it
        // through the SAME onset/recovery pairing as HostInternetUnreachable.
        Action::IngestSkewDetected => Signal::Onset(action, Family::IngestSkew),
        Action::IngestSkewRecovered => Signal::Recovery(action, Family::IngestSkew),
        // #367: an absolute A/V invariant violation at ANY stage (ingest
        // chunker or VPS pusher) is a desync the audience hears/sees -- the
        // 2026-10-01 incident alerted no one because every guard was
        // baseline-relative.
        Action::AvInvariantViolated => Signal::Onset(action, Family::AvInvariant),
        Action::AvInvariantRestored => Signal::Recovery(action, Family::AvInvariant),
        // #84: standalone heads-up, deliberately OUTSIDE the outage episode.
        Action::LongStreamWarning => Signal::Standalone(action),
        _ => return None,
    };
    Some(signal)
}

/// One outage episode (#367): the family plus the row's `detail.stage` and
/// endpoint alias. Every emitter writes the same stage / endpoint on an
/// onset and on its paired recovery (host-level rows carry neither), so the
/// recovery finds exactly the episode its onset opened.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EpisodeKey {
    family: Family,
    stage: Option<String>,
    endpoint: Option<String>,
}

impl EpisodeKey {
    fn of(family: Family, row: &AuditRow) -> Self {
        Self {
            family,
            stage: row
                .detail
                .get("stage")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            endpoint: row.endpoint.clone(),
        }
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
        Action::AvInvariantViolated => {
            "🔴 Restreamer posunul zvuk voči obrazu (chyba synchronizácie vo vysielaní) — \
             diváci môžu mať rozídený zvuk a obraz, treba to riešiť."
        }
        Action::AvInvariantRestored => {
            "✅ Synchronizácia zvuku a obrazu v Restreameri je znova v poriadku."
        }
        // classify() only routes the eleven actions above into this function.
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
    /// Open outage episode(s) and their alerted onsets.
    episodes: Episodes,
}

/// Outage-episode bookkeeping (#367): the open episodes, each with the onset
/// actions already alerted in it (the dedup set).
#[derive(Debug, Default)]
struct Episodes {
    open: HashMap<EpisodeKey, HashSet<Action>>,
}

impl Episodes {
    /// Record an onset in its episode, opening the episode if needed. True
    /// when this onset has not alerted yet in that episode (the first
    /// occurrence alerts, repeats are deduped).
    fn onset(&mut self, key: EpisodeKey, action: Action) -> bool {
        self.open.entry(key).or_default().insert(action)
    }

    /// End the episode `key`; every other episode stays open. True when it
    /// was open: only then is a recovery alert due (no spurious "recovered"
    /// when nothing was flagged as down).
    fn recover(&mut self, key: &EpisodeKey) -> bool {
        self.open.remove(key).is_some()
    }

    /// No outage episode is open.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.open.is_empty()
    }
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
            episodes: Episodes::default(),
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
            Signal::Onset(action, family) => {
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
                // First occurrence of this onset in its episode alerts;
                // repeats (the per-retry storm) are suppressed.
                let key = EpisodeKey::of(family, row);
                if self.episodes.onset(key.clone(), action) {
                    tracing::info!(episode = ?key, action = ?action, "outage notifier: onset alert");
                    Some(build_alert(action, row))
                } else {
                    None
                }
            }
            Signal::Recovery(action, family) => {
                let key = EpisodeKey::of(family, row);
                if self.episodes.recover(&key) {
                    tracing::info!(
                        episode = ?key,
                        action = ?action,
                        "outage notifier: episode recovered"
                    );
                    Some(build_alert(action, row))
                } else {
                    // No spurious "recovered" when this episode was not open.
                    tracing::debug!(
                        episode = ?key,
                        action = ?action,
                        "outage notifier: recovery with no open episode -- no alert"
                    );
                    None
                }
            }
            // #84: fire the heads-up without touching episode state. The
            // emitter (the long-stream monitor) already dedups to once per
            // delivery, so no per-episode dedup is needed here.
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
    // #84: enrich the long-stream heads-up with the concrete runtime + limit
    // the operator would otherwise only see in the audit detail.
    if action == Action::LongStreamWarning {
        if let Some(elapsed) = row.detail.get("elapsed_secs").and_then(|v| v.as_u64()) {
            content.push_str(&format!(" (beží ~{:.1} h", elapsed as f64 / 3600.0));
            if let Some(thr) = row.detail.get("threshold_secs").and_then(|v| v.as_u64()) {
                content.push_str(&format!(", limit {:.1} h", thr as f64 / 3600.0));
            }
            content.push(')');
        }
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
#[path = "notify_tests.rs"]
mod tests;
