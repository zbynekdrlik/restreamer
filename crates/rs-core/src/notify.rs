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
//! Disabled when `notifications.discord_webhook_url` is empty (the default), so
//! the feature ships dark until the operator sets the webhook in `config.json`.
//! The webhook URL is a runtime secret — it is never committed to the repo.

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
}

/// Route an audit action to an outage signal, or `None` if it is not
/// outage-relevant. Keep in sync with the emission sites verified for #261.
fn classify(action: Action) -> Option<Signal> {
    match action {
        Action::VpsUnreachable
        | Action::S3UploadFailed
        | Action::HostInternetUnreachable
        | Action::RescueActivated => Some(Signal::Onset(action)),
        Action::RescueRecovered | Action::HostInternetRecovered => Some(Signal::Recovery(action)),
        _ => None,
    }
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
        // classify() only routes the six actions above into this function.
        _ => "",
    }
}

/// Edge-triggered Discord outage notifier. Built from config; owned mutably by
/// the audit writer task, which calls [`observe`](Self::observe) on each row.
pub struct OutageNotifier {
    webhook_url: String,
    client: reqwest::Client,
    /// Whether an outage episode is currently active.
    in_outage: bool,
    /// Onset actions already alerted during the current episode (dedup key).
    alerted: HashSet<Action>,
}

impl OutageNotifier {
    /// Build from config; returns `None` (disabled) when the webhook URL is
    /// empty or whitespace-only.
    pub fn from_config(cfg: &NotificationsConfig) -> Option<Self> {
        let url = cfg.discord_webhook_url.trim();
        if url.is_empty() {
            return None;
        }
        Some(Self {
            webhook_url: url.to_string(),
            client: reqwest::Client::new(),
            in_outage: false,
            alerted: HashSet::new(),
        })
    }

    /// Pure edge-trigger / dedup core. Returns `Some(alert)` when this row is a
    /// state transition that should fire, `None` otherwise. Mutates the episode
    /// state. HTTP-free, so it is directly unit-testable.
    pub fn observe(&mut self, row: &AuditRow) -> Option<DiscordAlert> {
        match classify(row.action)? {
            Signal::Onset(action) => {
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
        }
    }

    /// Fire-and-forget POST of the alert to Discord; never blocks the writer.
    pub fn spawn_dispatch(&self, alert: DiscordAlert) {
        let client = self.client.clone();
        let url = self.webhook_url.clone();
        tokio::spawn(async move {
            if let Err(e) = post_alert(&client, &url, &alert).await {
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

    /// Disabled notifier state — constructed directly so the pure `observe`
    /// core can be exercised without a live webhook.
    fn notifier() -> OutageNotifier {
        OutageNotifier {
            webhook_url: "http://127.0.0.1:1/unused".to_string(),
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
        // Unrelated actions are ignored.
        assert!(classify(Action::EventStarted).is_none());
        assert!(classify(Action::DiskCachePushSample).is_none());
    }

    #[test]
    fn first_onset_alerts_then_dedups_within_episode() {
        let mut n = notifier();
        // First VpsUnreachable fires.
        assert!(n.observe(&row(Action::VpsUnreachable)).is_some());
        // The per-retry storm is suppressed (edge-triggered).
        assert!(n.observe(&row(Action::VpsUnreachable)).is_none());
        assert!(n.observe(&row(Action::VpsUnreachable)).is_none());
    }

    #[test]
    fn distinct_onsets_each_alert_once_in_one_episode() {
        let mut n = notifier();
        assert!(n.observe(&row(Action::VpsUnreachable)).is_some());
        // A different transition (S3 upload failing) is its own alert.
        assert!(n.observe(&row(Action::S3UploadFailed)).is_some());
        // But repeats of either are still deduped.
        assert!(n.observe(&row(Action::VpsUnreachable)).is_none());
        assert!(n.observe(&row(Action::S3UploadFailed)).is_none());
    }

    #[test]
    fn recovery_fires_only_when_in_outage() {
        let mut n = notifier();
        // Recovery with no active outage must NOT fire a spurious "all-clear".
        assert!(n.observe(&row(Action::RescueRecovered)).is_none());
        // Now enter an outage, then recover.
        assert!(n.observe(&row(Action::RescueActivated)).is_some());
        assert!(n.observe(&row(Action::RescueRecovered)).is_some());
        // Recovery again with no active outage is suppressed.
        assert!(n.observe(&row(Action::RescueRecovered)).is_none());
    }

    #[test]
    fn recovery_resets_and_rearms_onset_alerts() {
        let mut n = notifier();
        assert!(n.observe(&row(Action::VpsUnreachable)).is_some());
        assert!(n.observe(&row(Action::VpsUnreachable)).is_none()); // deduped
        assert!(n.observe(&row(Action::HostInternetRecovered)).is_some()); // recover
        // A genuinely new outage after recovery must alert again.
        assert!(n.observe(&row(Action::VpsUnreachable)).is_some());
    }

    #[test]
    fn unrelated_action_does_not_touch_state() {
        let mut n = notifier();
        assert!(n.observe(&row(Action::EventStarted)).is_none());
        assert!(!n.in_outage);
        // A real onset right after still fires (state was untouched).
        assert!(n.observe(&row(Action::S3UploadFailed)).is_some());
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
        let empty = NotificationsConfig {
            discord_webhook_url: "   ".to_string(),
        };
        assert!(OutageNotifier::from_config(&empty).is_none());

        let set = NotificationsConfig {
            discord_webhook_url: "https://discord.example/webhook/abc".to_string(),
        };
        assert!(OutageNotifier::from_config(&set).is_some());
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
}
