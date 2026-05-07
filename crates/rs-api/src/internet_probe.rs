//! Periodic host->internet reachability probe.
//!
//! Runs every 30s. Issues a HEAD request to a stable Hetzner-edge URL
//! (`https://nbg1.your-objectstorage.com/`). After N=3 consecutive
//! failures, emits a `HostInternetUnreachable` audit row. On first
//! success after a previous unreachable, emits `HostInternetRecovered`.
//!
//! The task lives on the host (rs-api). The probe URL must be reachable
//! from healthy stream.snv but ALSO be the same path that S3 uploads +
//! VPS HTTP polls take (so a real internet blip trips this probe before
//! it cascades to those code paths). Hetzner's Object Storage edge fits.
//!
//! Issue #176 follow-up.

use rs_core::audit::{Action, AuditRow, Severity, Source, record};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;

const PROBE_URL: &str = "https://nbg1.your-objectstorage.com/";
const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const FAIL_STREAK_TO_DECLARE_DOWN: u32 = 3;

pub fn spawn_internet_probe(audit_tx: mpsc::Sender<AuditRow>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run_probe_loop(audit_tx).await })
}

async fn run_probe_loop(audit_tx: mpsc::Sender<AuditRow>) {
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("internet_probe: failed to build reqwest client: {e}");
            return;
        }
    };
    let mut tick = interval(PROBE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // skip the immediate first tick
    let mut consecutive_failures: u32 = 0;
    let mut declared_down = false;
    loop {
        tick.tick().await;
        let ok = probe_once(&client).await;
        if ok {
            if declared_down {
                emit_recovered(&audit_tx, consecutive_failures).await;
                declared_down = false;
            }
            consecutive_failures = 0;
        } else {
            consecutive_failures = consecutive_failures.saturating_add(1);
            tracing::debug!(
                consecutive_failures,
                "internet_probe: HEAD {} failed",
                PROBE_URL
            );
            if consecutive_failures >= FAIL_STREAK_TO_DECLARE_DOWN && !declared_down {
                emit_unreachable(&audit_tx, consecutive_failures).await;
                declared_down = true;
            }
        }
    }
}

async fn probe_once(client: &reqwest::Client) -> bool {
    match client.head(PROBE_URL).send().await {
        // Any HTTP response (even 4xx/5xx) proves DNS + TCP + TLS + routing.
        // Only network-level failures count as unreachable.
        Ok(_resp) => true,
        Err(e) => {
            tracing::debug!("internet_probe: probe failed: {e}");
            false
        }
    }
}

async fn emit_unreachable(audit_tx: &mpsc::Sender<AuditRow>, consecutive: u32) {
    tracing::warn!(
        consecutive_failures = consecutive,
        probe_url = PROBE_URL,
        "internet_probe: host internet egress declared UNREACHABLE after {} consecutive failures",
        consecutive
    );
    let row = AuditRow {
        severity: Severity::Warn,
        source: Source::Operator,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: Action::HostInternetUnreachable,
        detail: serde_json::json!({
            "consecutive_failures": consecutive,
            "probe_url": PROBE_URL,
        }),
        ts_override: None,
    };
    record(audit_tx, row);
}

async fn emit_recovered(audit_tx: &mpsc::Sender<AuditRow>, prior_consecutive: u32) {
    tracing::info!(
        recovered_after_failures = prior_consecutive,
        probe_url = PROBE_URL,
        "internet_probe: host internet egress RECOVERED after {} prior failures",
        prior_consecutive
    );
    let row = AuditRow {
        severity: Severity::Info,
        source: Source::Operator,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: Action::HostInternetRecovered,
        detail: serde_json::json!({
            "recovered_after_failures": prior_consecutive,
            "probe_url": PROBE_URL,
        }),
        ts_override: None,
    };
    record(audit_tx, row);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_serde_host_internet_unreachable() {
        let s = serde_json::to_string(&Action::HostInternetUnreachable).unwrap();
        assert_eq!(s, r#""host_internet_unreachable""#);
    }

    #[test]
    fn action_serde_host_internet_recovered() {
        let s = serde_json::to_string(&Action::HostInternetRecovered).unwrap();
        assert_eq!(s, r#""host_internet_recovered""#);
    }

    #[test]
    fn source_operator_serde() {
        let s = serde_json::to_string(&Source::Operator).unwrap();
        assert_eq!(s, r#""operator""#);
    }
}
