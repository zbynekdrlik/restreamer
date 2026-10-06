//! YouTube A/V-gate session API (#357): the shared YouTube leg of the
//! restreamer and camera-box release gates.
//!
//! A session takes the CI channel's reusable stream ("e2e rtmp"), binds a
//! fresh unlisted broadcast to it, activates the CI test event and starts
//! delivery, transitions the broadcast live once YouTube receives data, and on
//! `stop` drains the event cache, completes the broadcast, tears the delivery
//! down and waits for YouTube to finish the VOD. The caller does the rest
//! (OBS, recording, measurement).
//!
//! Layout:
//! - this file: the wire types, the one-session-at-a-time registry, the rig
//!   seam and the timings;
//! - `av_gate_driver.rs`: the state machine (start, readiness, stop, teardown,
//!   processing, reaper, boot reconcile);
//! - `av_gate_rig.rs`: the production rig over [`AppState`] (event + delivery
//!   + Hetzner);
//! - `av_gate_handlers.rs`: the HTTP handlers and the token check.
//!
//! Cleanup guarantee: every exit path (failure at any step, idle timeout, a
//! dead driver task, a restart mid-session) runs the same teardown, which
//! completes the broadcast if it went live, stops the event and its delivery
//! and verifies that no Hetzner server is left for the event.
//!
//! [`AppState`]: crate::state::AppState

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rs_core::config::AvGateConfig;
use rs_core::db::av_gate::AvGateSessionRow;
use serde::Serialize;
use tokio::sync::watch;

/// One session's YouTube quota cost estimate (insert 50 + bind 50 + two
/// transitions 100 + status polls), used by the daily budget guard.
pub const SESSION_QUOTA_ESTIMATE: u32 = 400;

/// The session lifecycle, as the API reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Broadcast created and bound, event activated, delivery booting.
    Starting,
    /// Delivery delivering, YouTube stream active, broadcast live.
    Ready,
    /// Broadcast completed and delivery torn down; YouTube is making the VOD.
    Processing,
    /// The VOD exists (`vod_id`).
    Done,
    /// The session ended without a VOD (`reason`).
    Failed,
}

impl SessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Processing => "processing",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

/// `GET /api/v1/av-gate/session/{id}` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionView {
    pub session_id: String,
    pub state: String,
    pub requester: String,
    pub title: String,
    pub broadcast_id: Option<String>,
    pub vod_id: Option<String>,
    pub reason: Option<String>,
    pub quota_units: i64,
    /// The teardown failed and is being retried; no new session starts
    /// until it is clean.
    pub cleanup_pending: bool,
    pub timestamps: SessionTimestamps,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionTimestamps {
    pub created: String,
    pub ready: Option<String>,
    pub stop_requested: Option<String>,
    pub processing: Option<String>,
    pub finished: Option<String>,
}

impl From<AvGateSessionRow> for SessionView {
    fn from(r: AvGateSessionRow) -> Self {
        Self {
            session_id: r.id,
            state: r.state,
            requester: r.requester,
            title: r.title,
            broadcast_id: r.broadcast_id,
            vod_id: r.vod_id,
            reason: r.reason,
            quota_units: r.quota_units,
            cleanup_pending: r.cleanup_pending,
            timestamps: SessionTimestamps {
                created: r.created_at,
                ready: r.ready_at,
                stop_requested: r.stop_requested_at,
                processing: r.processing_at,
                finished: r.finished_at,
            },
        }
    }
}

/// The session that currently holds the rig (stream key, test event, OBS).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Holder {
    pub session_id: String,
    pub requester: String,
}

struct Active {
    holder: Holder,
    stop: watch::Sender<bool>,
}

/// One session at a time. A session holds the slot from admission until its
/// teardown finished (`processing` or `failed`): the reusable stream, the CI
/// event and stream OBS are single resources. Closed until the boot reconcile
/// has dealt with whatever a restart left behind.
#[derive(Default)]
pub struct AvGateRegistry {
    active: Mutex<Option<Active>>,
    reconciled: std::sync::atomic::AtomicBool,
    /// Held while a cleanup retry round runs: the operator's force-clear must
    /// not race it (the retry would save a stale row over the clear).
    pub(crate) cleanup_round: tokio::sync::Mutex<()>,
    /// Sessions whose VOD wait was resumed after a restart, so a restarted
    /// maintenance loop does not resume one twice.
    resumed: Mutex<std::collections::HashSet<String>>,
}

impl AvGateRegistry {
    /// Record that `session_id`'s VOD wait was resumed. False if it already was.
    pub fn mark_resumed(&self, session_id: &str) -> bool {
        self.resumed
            .lock()
            .expect("av-gate registry poisoned")
            .insert(session_id.to_string())
    }

    /// Open the API: the boot reconcile finished.
    pub fn mark_reconciled(&self) {
        self.reconciled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_reconciled(&self) -> bool {
        self.reconciled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Take the slot for `holder`, or return whoever has it. On success the
    /// returned receiver flips to `true` when a stop is requested.
    pub fn claim(&self, holder: Holder) -> Result<watch::Receiver<bool>, Holder> {
        let mut slot = self.active.lock().expect("av-gate registry poisoned");
        if let Some(a) = slot.as_ref() {
            return Err(a.holder.clone());
        }
        let (stop, rx) = watch::channel(false);
        *slot = Some(Active { holder, stop });
        Ok(rx)
    }

    /// Free the slot, but only if `session_id` still holds it.
    pub fn release(&self, session_id: &str) {
        let mut slot = self.active.lock().expect("av-gate registry poisoned");
        if slot
            .as_ref()
            .is_some_and(|a| a.holder.session_id == session_id)
        {
            *slot = None;
        }
    }

    /// Ask the holding session to stop. False when `session_id` does not hold
    /// the slot.
    pub fn request_stop(&self, session_id: &str) -> bool {
        let slot = self.active.lock().expect("av-gate registry poisoned");
        match slot.as_ref() {
            Some(a) if a.holder.session_id == session_id => {
                a.stop.send_replace(true);
                true
            }
            _ => false,
        }
    }

    /// The current holder, if any.
    pub fn holder(&self) -> Option<Holder> {
        let slot = self.active.lock().expect("av-gate registry poisoned");
        slot.as_ref().map(|a| a.holder.clone())
    }
}

/// The av-gate part of `AppState`: the session registry, plus (tests only) a
/// seam that swaps the production rig and the Google endpoints.
#[derive(Default)]
pub struct AvGateHub {
    pub registry: std::sync::Arc<AvGateRegistry>,
    #[cfg(test)]
    pub(crate) seam: Mutex<Option<TestSeam>>,
}

/// Test-only replacement for the production rig and endpoints.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestSeam {
    pub rig: std::sync::Arc<dyn AvGateRig>,
    pub api_base: String,
    pub token_uri: String,
    pub timings: AvGateTimings,
    /// Replaces the process-wide project bucket, so a test cannot drain the
    /// one every other test shares.
    pub quota_bucket: Option<&'static rs_youtube::quota::QuotaTracker>,
}

/// The CI event a session activates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RigEvent {
    pub id: i64,
    /// The event's cache delay: how long the delivery keeps sending after the
    /// source stopped. The stop path waits this plus a margin before it
    /// completes the broadcast, or the VOD would lose its last minutes.
    pub drain: Duration,
}

/// Where the event's delivery VPS is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RigDelivery {
    /// No live delivery row (never started, failed or deleted).
    NotRunning,
    /// Creating, booting or initialising.
    Booting,
    /// Pushing to the endpoints.
    Delivering,
}

/// Why the rig did not start the event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartEventError {
    /// Refused before touching anything (another event is live, delivery not
    /// configured, the event is gone). Nothing of the event needs stopping.
    Refused(String),
    /// Something may have been started (flags set, a VPS created).
    Failed(String),
}

/// The Restreamer side of a session: the CI event, its delivery and the
/// Hetzner servers. Production: `av_gate_rig::AppRig`. The seam exists because
/// the real delivery boots a Hetzner VPS (external, minutes, billed); the
/// session state machine is tested against a scripted rig and the production
/// rig is tested on its own.
#[async_trait]
pub trait AvGateRig: Send + Sync {
    async fn resolve_event(&self, name: &str) -> Result<RigEvent, String>;
    async fn start_event(&self, event_id: i64) -> Result<(), StartEventError>;
    async fn delivery(&self, event_id: i64) -> Result<RigDelivery, String>;
    async fn stop_event(&self, event_id: i64) -> Result<(), String>;
    /// The event is receiving or delivering right now.
    async fn event_active(&self, event_id: i64) -> Result<bool, String>;
    /// Hetzner servers still existing for this box's `event_id`.
    async fn server_count(&self, event_id: i64) -> Result<usize, String>;
}

/// Every wait in the session, injectable so tests run in milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvGateTimings {
    /// Readiness polling while `starting`, and the retry spacing elsewhere.
    pub poll: Duration,
    /// Added to the event's cache delay before the broadcast is completed.
    pub drain_extra: Duration,
    /// No stop this long after creation: the session is reaped.
    pub idle_timeout: Duration,
    /// How long the teardown waits for the Hetzner servers to disappear.
    pub servers_gone_timeout: Duration,
    /// VOD processing polling.
    pub processing_poll: Duration,
    pub processing_timeout: Duration,
    /// First wait before a failed teardown is retried; doubles per failed
    /// round, capped at 24x.
    pub cleanup_retry: Duration,
}

impl AvGateTimings {
    pub fn from_config(cfg: &AvGateConfig) -> Self {
        Self {
            poll: Duration::from_secs(15),
            drain_extra: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(cfg.idle_timeout_secs),
            servers_gone_timeout: Duration::from_secs(180),
            processing_poll: Duration::from_secs(30),
            processing_timeout: Duration::from_secs(cfg.processing_timeout_secs),
            cleanup_retry: Duration::from_secs(5 * 60),
        }
    }
}

/// The daily budget guard: a session is admitted only if the units spent in
/// the last 24 h plus one session's estimate stay within the budget.
pub fn quota_allows(spent: i64, estimate: u32, budget: u32) -> bool {
    spent + i64::from(estimate) <= i64::from(budget)
}

/// Request-body limits. YouTube caps a broadcast title at 100 characters.
pub const MAX_REQUESTER_CHARS: usize = 64;
pub const MAX_TITLE_CHARS: usize = 100;

/// Validate `{requester, title?}`; returns the trimmed requester and the
/// broadcast title to use.
pub fn validate_request(
    requester: &str,
    title: Option<&str>,
    session_id: &str,
) -> Result<(String, String), String> {
    let requester = requester.trim();
    if requester.is_empty() || requester.chars().count() > MAX_REQUESTER_CHARS {
        return Err(format!(
            "requester must be 1..={MAX_REQUESTER_CHARS} characters"
        ));
    }
    let title = match title.map(str::trim) {
        Some(t) if t.is_empty() || t.chars().count() > MAX_TITLE_CHARS => {
            return Err(format!("title must be 1..={MAX_TITLE_CHARS} characters"));
        }
        Some(t) => t.to_string(),
        None => format!(
            "A/V gate {requester} {}",
            session_id.chars().take(8).collect::<String>()
        ),
    };
    Ok((requester.to_string(), title))
}

#[cfg(test)]
#[path = "av_gate_tests.rs"]
mod tests;
