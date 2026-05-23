//! Operator-facing endpoint lifecycle semaphore.
//!
//! Extracted from `models.rs` to keep that file under the 1000-line CI cap.
//! Computes a coarse state from per-endpoint delivery metrics so the
//! dashboard can render green (live) / blue (survivable, auto-recovering) /
//! red (operator action required).

use serde::{Deserialize, Serialize};

/// Operator-facing endpoint lifecycle. Drives the dashboard semaphore:
/// Pending=gray, Live=green, Buffering/Rescue/Recovering=blue (survivable,
/// auto-recovering, NO action needed), Attention=red (operator MUST act).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointLifecycle {
    Pending,
    Live,
    Buffering,
    Rescue,
    Recovering,
    Attention,
}

/// Inputs the host has when computing lifecycle for one endpoint.
pub struct LifecycleInput {
    pub alive: bool,
    pub chunks_processed: i64,
    pub delivery_mode: Option<String>,
    pub stall_reason: Option<String>,
    pub last_error: Option<String>,
    pub disk_critical: bool,
}

impl EndpointLifecycle {
    pub fn compute(i: &LifecycleInput) -> Self {
        // RED only for states the operator must act on. Disk-critical always
        // forces Attention. An actionable last_error (auth/key reject) only
        // forces Attention while the endpoint is DOWN — a recovered (alive)
        // endpoint can carry a STALE actionable error that is cleared only on
        // the next successful push, so it must not paint a healthy endpoint
        // red.
        if i.disk_critical {
            return EndpointLifecycle::Attention;
        }
        if !i.alive && last_error_is_actionable(i.last_error.as_deref()) {
            return EndpointLifecycle::Attention;
        }
        match i.delivery_mode.as_deref() {
            Some("rescue") => return EndpointLifecycle::Rescue,
            Some("recovering") | Some("warmup") => return EndpointLifecycle::Recovering,
            _ => {}
        }
        if i.alive && i.stall_reason.is_some() {
            return EndpointLifecycle::Buffering; // survivable upstream stall = blue
        }
        if !i.alive && i.chunks_processed == 0 {
            return EndpointLifecycle::Pending;
        }
        if !i.alive {
            // Dead with no actionable error => treat as recovering (the
            // pusher reconnects forever); never a bare red.
            return EndpointLifecycle::Recovering;
        }
        EndpointLifecycle::Live
    }
}

/// An error string the OPERATOR must act on (auth/key rejected). Network /
/// transient errors are NOT actionable — they auto-recover.
fn last_error_is_actionable(err: Option<&str>) -> bool {
    let Some(e) = err else { return false };
    let e = e.to_ascii_lowercase();
    e.contains("rejected")
        || e.contains("bad stream key")
        || e.contains("unauthorized")
        || e.contains("forbidden")
        || e.contains("invalid stream key")
        || e.contains("badname")
}

/// Default lifecycle for older payloads lacking the field — degrade to Live
/// so the dashboard does not flag a false alarm.
pub fn default_lifecycle() -> EndpointLifecycle {
    EndpointLifecycle::Live
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    fn input(
        alive: bool,
        mode: Option<&str>,
        stall: Option<&str>,
        err: Option<&str>,
    ) -> LifecycleInput {
        LifecycleInput {
            alive,
            chunks_processed: if alive { 100 } else { 0 },
            delivery_mode: mode.map(|s| s.to_string()),
            stall_reason: stall.map(|s| s.to_string()),
            last_error: err.map(|s| s.to_string()),
            disk_critical: false,
        }
    }

    #[test]
    fn rescue_mode_is_blue_rescue() {
        let i = input(true, Some("rescue"), None, None);
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Rescue);
    }

    #[test]
    fn recovering_and_warmup_are_blue_recovering() {
        let r = input(true, Some("recovering"), None, None);
        assert_eq!(
            EndpointLifecycle::compute(&r),
            EndpointLifecycle::Recovering
        );
        let w = input(true, Some("warmup"), None, None);
        assert_eq!(
            EndpointLifecycle::compute(&w),
            EndpointLifecycle::Recovering
        );
    }

    #[test]
    fn upstream_stall_is_blue_buffering_not_red() {
        // A transient network stall must NOT be red — it is survivable.
        let i = input(
            true,
            Some("normal"),
            Some("waiting for chunk 42 (S3)"),
            None,
        );
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Buffering);
    }

    #[test]
    fn auth_reject_is_red_attention() {
        let i = input(false, None, None, Some("PublishRejected: bad stream key"));
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Attention);
    }

    #[test]
    fn alive_endpoint_with_stale_actionable_error_is_live_not_red() {
        // A recovered endpoint (alive=true) can still carry a STALE
        // actionable last_error — it is cleared only on the next successful
        // push. The stale error must NOT paint the healthy endpoint red.
        let i = input(
            true,
            Some("normal"),
            None,
            Some("PublishRejected: bad stream key"),
        );
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Live);
    }

    #[test]
    fn disk_critical_is_red_attention() {
        let mut i = input(true, Some("normal"), None, None);
        i.disk_critical = true;
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Attention);
    }

    #[test]
    fn healthy_is_green_live() {
        let i = input(true, Some("normal"), None, None);
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Live);
    }

    #[test]
    fn not_started_is_pending() {
        let i = input(false, None, None, None);
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Pending);
    }
}
