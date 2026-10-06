//! The A/V-gate session state machine (#357). See `av_gate.rs` for the
//! overview and the cleanup guarantee.
//!
//! One driver task per session owns it from creation to `done`/`failed`. Every
//! exit path funnels through [`Session::teardown`], which records per resource
//! what it already released (`broadcast_done`, `event_done`); anything left
//! sets `cleanup_pending`, and the maintenance loop (`av_gate_lifecycle.rs`)
//! retries only the missing part until clean, refusing new sessions meanwhile.

use std::sync::Arc;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rs_core::audit::{self, Action, AuditRow, Severity, Source};
use rs_core::db::av_gate::{self as store, AvGateSessionRow};
use rs_youtube::manage::{BroadcastTransition, ManageClient, VodStatus};
use rs_youtube::quota::QuotaTracker;
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::av_gate::{
    AvGateRegistry, AvGateRig, AvGateTimings, RigDelivery, SessionState, StartEventError,
};

/// Consecutive failed polls (YouTube or rig) before a wait gives up.
const MAX_POLL_ERRORS: u32 = 5;
/// Failed `transition live` calls before the session gives up.
const MAX_LIVE_ATTEMPTS: u32 = 3;
/// Attempts to complete the broadcast during a teardown.
const COMPLETE_ATTEMPTS: u32 = 3;

/// Everything a session needs, shared by its driver and its supervisor.
pub struct SessionCtx {
    pub pool: SqlitePool,
    pub audit_tx: mpsc::Sender<AuditRow>,
    pub registry: Arc<AvGateRegistry>,
    pub rig: Arc<dyn AvGateRig>,
    pub timings: AvGateTimings,
    pub event_name: String,
    pub stream_title: String,
    pub daily_quota_budget: u32,
    /// The project-wide YouTube quota bucket the health polling also draws
    /// from. Admission needs a session's estimate left in it.
    pub quota_bucket: Option<&'static QuotaTracker>,
}

/// RFC 3339 UTC with milliseconds and `Z`: fixed width, so the quota guard
/// can compare `created_at` values as text.
pub fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// True once `count` consecutive failures reached `max`.
fn exhausted(count: u32, max: u32) -> bool {
    count >= max
}

/// `reason` with `note` appended (`"; "`-separated, no leading separator).
fn append_reason(reason: Option<String>, note: &str) -> String {
    match reason.filter(|r| !r.is_empty()) {
        Some(r) => format!("{r}; {note}"),
        None => note.to_string(),
    }
}

/// What the teardown must do with a broadcast in `life_cycle`.
#[derive(Debug, PartialEq, Eq)]
enum LifeAction {
    /// It is (or was) on air: transition it to `complete`.
    Complete,
    /// It is mid-transition: ask again shortly.
    Wait,
    /// Never on air, already complete, or revoked: nothing to do.
    Nothing,
}

fn life_action(life_cycle: &str) -> LifeAction {
    match life_cycle {
        "live" | "testing" => LifeAction::Complete,
        "liveStarting" | "testStarting" => LifeAction::Wait,
        _ => LifeAction::Nothing,
    }
}

/// Map what YouTube says about the VOD to the session's next step.
#[derive(Debug, PartialEq, Eq)]
enum VodStep {
    Done,
    Failed(String),
    Pending,
}

fn vod_step(status: &VodStatus) -> VodStep {
    let processing = status.processing.as_deref();
    let upload = status.upload.as_deref();
    if processing == Some("succeeded") || upload == Some("processed") {
        return VodStep::Done;
    }
    match (processing, upload) {
        (Some(s @ ("failed" | "terminated")), _)
        | (_, Some(s @ ("failed" | "rejected" | "deleted"))) => {
            VodStep::Failed(format!("YouTube could not process the VOD ({s})"))
        }
        _ => VodStep::Pending,
    }
}

/// One session in flight: its durable row plus the client it spends quota on.
pub(crate) struct Session {
    ctx: Arc<SessionCtx>,
    yt: Option<Arc<ManageClient>>,
    /// The row's quota spend before this client spent anything.
    base_units: i64,
    row: AvGateSessionRow,
}

/// Why a session stopped waiting.
enum Wake {
    Stop,
    Idle,
    Failed(String),
}

impl Session {
    /// A session over `row` with a client that has spent nothing for it yet.
    fn new(ctx: Arc<SessionCtx>, yt: Option<Arc<ManageClient>>, row: AvGateSessionRow) -> Self {
        let base_units = row.quota_units;
        Self::with_base(ctx, yt, row, base_units)
    }

    /// A session whose client already spent units on top of `base_units`
    /// (the reaper reuses the dead driver's client).
    fn with_base(
        ctx: Arc<SessionCtx>,
        yt: Option<Arc<ManageClient>>,
        row: AvGateSessionRow,
        base_units: i64,
    ) -> Self {
        Self {
            ctx,
            yt,
            base_units,
            row,
        }
    }

    fn yt(&self) -> Result<&ManageClient, String> {
        self.yt
            .as_deref()
            .ok_or_else(|| "no YouTube manage client (oauth file unusable)".to_string())
    }

    /// Write the row (with the current quota spend). A DB error is logged:
    /// the session itself keeps going and its cleanup does not depend on it.
    async fn persist(&mut self) {
        if let Some(yt) = &self.yt {
            self.row.quota_units = self.base_units + i64::from(yt.units_used());
        }
        if let Err(e) = store::save(&self.ctx.pool, &self.row).await {
            error!(session = %self.row.id, "av-gate: persisting the session failed: {e}");
        }
    }

    fn audit(&self, severity: Severity, action: Action, mut detail: Value) {
        detail["session_id"] = json!(self.row.id);
        audit::record(
            &self.ctx.audit_tx,
            AuditRow {
                severity,
                source: Source::System,
                event_id: self.row.event_id,
                instance_id: None,
                endpoint: None,
                action,
                detail,
                ts_override: None,
            },
        );
    }

    async fn set_state(&mut self, state: SessionState) {
        info!(session = %self.row.id, "av-gate: session -> {}", state.as_str());
        self.row.state = state.as_str().to_string();
        self.persist().await;
    }

    /// Stream lookup, broadcast insert + bind, event start. Returns the
    /// event's drain time. Every resource is recorded on the row as soon as it
    /// exists, so a teardown after a failure releases exactly what was taken.
    async fn start_leg(&mut self) -> Result<Duration, String> {
        let yt = Arc::clone(self.yt.as_ref().ok_or("no YouTube manage client")?);
        let title = self.ctx.stream_title.clone();
        let stream = yt
            .find_stream_by_title(&title)
            .await
            .map_err(|e| format!("YouTube stream lookup failed: {e}"))?
            .ok_or_else(|| format!("no YouTube stream titled {title:?} on the CI channel"))?;
        if !stream.is_reusable {
            return Err(format!(
                "YouTube stream {title:?} is not reusable; the gate binds only a reusable \
                 stream (owner rule 2026-10-05)"
            ));
        }
        self.row.stream_id = Some(stream.id.clone());
        let event = self.ctx.rig.resolve_event(&self.ctx.event_name).await?;
        let broadcast_id = yt
            .insert_broadcast(&self.row.title, &now_ts())
            .await
            .map_err(|e| format!("liveBroadcasts.insert failed: {e}"))?;
        self.row.broadcast_id = Some(broadcast_id.clone());
        self.persist().await;
        yt.bind_broadcast(&broadcast_id, &stream.id)
            .await
            .map_err(|e| format!("liveBroadcasts.bind failed: {e}"))?;
        // Recorded BEFORE the start, so a crash inside it still leaves the
        // boot reconcile an event to stop.
        self.row.event_id = Some(event.id);
        self.persist().await;
        match self.ctx.rig.start_event(event.id).await {
            Ok(()) => Ok(event.drain),
            Err(StartEventError::Refused(r)) => {
                self.row.event_id = None;
                self.persist().await;
                Err(r)
            }
            Err(StartEventError::Failed(r)) => Err(r),
        }
    }

    /// One readiness poll while `starting`. `Ok(true)` = ready. The life cycle
    /// is read first, so a `live` transition is only (re)sent while the
    /// broadcast is not on air yet; `went_live` is persisted BEFORE it is
    /// sent, so even a lost response leaves the teardown a broadcast to
    /// complete.
    async fn readiness_step(
        &mut self,
        errors: &mut u32,
        live_failures: &mut u32,
    ) -> Result<bool, String> {
        let event_id = self.row.event_id.ok_or("session has no event")?;
        let yt = Arc::clone(self.yt.as_ref().ok_or("no YouTube manage client")?);
        let bid = self.row.broadcast_id.clone().unwrap_or_default();
        let sid = self.row.stream_id.clone().unwrap_or_default();
        let probe = async {
            match self.ctx.rig.delivery(event_id).await? {
                RigDelivery::NotRunning => {
                    return Ok(Err(
                        "the delivery VPS is not running (start failed or it died)".to_string(),
                    ));
                }
                RigDelivery::Booting => return Ok(Ok(false)),
                RigDelivery::Delivering => {}
            }
            let life = yt
                .broadcast_life_cycle(&bid)
                .await
                .map_err(|e| e.to_string())?;
            match life.as_deref() {
                Some("live") => return Ok(Ok(true)),
                Some("liveStarting") => return Ok(Ok(false)),
                _ => {}
            }
            let status = yt.stream_status(&sid).await.map_err(|e| e.to_string())?;
            if status.as_deref() != Some("active") {
                return Ok(Ok(false));
            }
            self.row.went_live = true;
            self.persist().await;
            if let Err(e) = yt
                .transition_broadcast(&bid, BroadcastTransition::Live)
                .await
            {
                *live_failures += 1;
                if exhausted(*live_failures, MAX_LIVE_ATTEMPTS) {
                    return Ok(Err(format!("transition to live failed: {e}")));
                }
                warn!(session = %self.row.id, "av-gate: transition live failed, retrying: {e}");
            }
            Ok::<_, String>(Ok(false))
        };
        match probe.await {
            Ok(outcome) => {
                *errors = 0;
                outcome
            }
            Err(e) => {
                *errors += 1;
                warn!(session = %self.row.id, "av-gate: readiness poll failed: {e}");
                if exhausted(*errors, MAX_POLL_ERRORS) {
                    Err(format!("readiness polling failed {MAX_POLL_ERRORS}x: {e}"))
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Wait in `starting` (polling readiness) and then in `ready`, until a
    /// stop, the idle deadline or a failure.
    async fn wait_for_stop(&mut self, stop_rx: &mut watch::Receiver<bool>) -> Wake {
        let deadline = tokio::time::Instant::now() + self.ctx.timings.idle_timeout;
        let (mut errors, mut live_failures) = (0, 0);
        loop {
            let ready = self.row.state == SessionState::Ready.as_str();
            tokio::select! {
                _ = stop_rx.wait_for(|s| *s) => {
                    return if ready {
                        Wake::Stop
                    } else {
                        Wake::Failed("stopped before the session was ready".to_string())
                    };
                }
                _ = tokio::time::sleep_until(deadline) => return Wake::Idle,
                _ = tokio::time::sleep(self.ctx.timings.poll), if !ready => {}
            }
            match self.readiness_step(&mut errors, &mut live_failures).await {
                Ok(true) => {
                    self.row.ready_at = Some(now_ts());
                    self.set_state(SessionState::Ready).await;
                    self.audit(
                        Severity::Info,
                        Action::AvGateSessionReady,
                        json!({ "broadcast_id": self.row.broadcast_id }),
                    );
                }
                Ok(false) => self.persist().await,
                Err(reason) => return Wake::Failed(reason),
            }
        }
    }

    /// Complete the broadcast if it is on air. `Ok` also when there is
    /// nothing to complete.
    async fn complete_broadcast(&self, broadcast_id: &str) -> Result<(), String> {
        let yt = self.yt()?;
        let mut last = String::new();
        for _ in 0..COMPLETE_ATTEMPTS {
            match yt.broadcast_life_cycle(broadcast_id).await {
                Ok(None) => return Ok(()),
                Ok(Some(life)) => match life_action(&life) {
                    LifeAction::Nothing => return Ok(()),
                    LifeAction::Wait => last = format!("still {life}"),
                    LifeAction::Complete => {
                        match yt
                            .transition_broadcast(broadcast_id, BroadcastTransition::Complete)
                            .await
                        {
                            Ok(()) => return Ok(()),
                            Err(e) => last = e.to_string(),
                        }
                    }
                },
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(self.ctx.timings.poll).await;
        }
        Err(format!("broadcast {broadcast_id} not completed: {last}"))
    }

    /// Poll until no Hetzner server is left for the event.
    async fn wait_servers_gone(&self, event_id: i64) -> Result<(), String> {
        let mut last = String::new();
        let poll = async {
            loop {
                match self.ctx.rig.server_count(event_id).await {
                    Ok(0) => return,
                    Ok(n) => {
                        last = format!("{n} Hetzner server(s) still exist for event {event_id}")
                    }
                    Err(e) => last = format!("Hetzner server check failed: {e}"),
                }
                tokio::time::sleep(self.ctx.timings.poll).await;
            }
        };
        match tokio::time::timeout(self.ctx.timings.servers_gone_timeout, poll).await {
            Ok(()) => Ok(()),
            Err(_) => Err(last),
        }
    }

    /// Stop the event and wait for its servers. On a RETRY the session is
    /// long over: an active event then belongs to another run (restreamer's
    /// own CI E2E uses it too) and must not be stopped.
    async fn release_event(&self, event_id: i64, retry: bool) -> Result<(), String> {
        if retry && self.ctx.rig.event_active(event_id).await? {
            return Err(format!(
                "event {event_id} is active again (another run?); not stopping it"
            ));
        }
        self.ctx
            .rig
            .stop_event(event_id)
            .await
            .map_err(|e| format!("stopping the event failed: {e}"))?;
        self.wait_servers_gone(event_id).await
    }

    /// Release whatever the session still holds: complete the broadcast if a
    /// live transition was ever attempted, stop the event and wait for its
    /// servers. Each half that succeeds is recorded and never repeated.
    /// Returns the problems; anything left sets `cleanup_pending`. Runs on
    /// EVERY exit path (`retry` = from the cleanup loop, after the session).
    async fn teardown(&mut self, retry: bool) -> Vec<String> {
        let mut problems = Vec::new();
        if !self.row.broadcast_done {
            let outcome = match self.row.broadcast_id.clone() {
                Some(bid) if self.row.went_live => self.complete_broadcast(&bid).await,
                _ => Ok(()),
            };
            match outcome {
                Ok(()) => self.row.broadcast_done = true,
                Err(e) => problems.push(e),
            }
        }
        if !self.row.event_done {
            let outcome = match self.row.event_id {
                Some(eid) => self.release_event(eid, retry).await,
                None => Ok(()),
            };
            match outcome {
                Ok(()) => self.row.event_done = true,
                Err(e) => problems.push(e),
            }
        }
        self.row.cleanup_pending = !problems.is_empty();
        problems
    }

    /// End the session as `failed`, after the teardown already ran.
    async fn finish_failed(&mut self, reason: String) {
        warn!(session = %self.row.id, "av-gate: session failed: {reason}");
        self.row.reason = Some(reason.clone());
        self.row.finished_at = Some(now_ts());
        self.set_state(SessionState::Failed).await;
        self.audit(
            Severity::Warn,
            Action::AvGateSessionFailed,
            json!({ "reason": reason }),
        );
        self.ctx.registry.release(&self.row.id);
    }

    /// Tear down, then fail with `reason` plus any teardown problem.
    async fn teardown_and_fail(&mut self, reason: String) {
        let problems = self.teardown(false).await;
        let reason = if problems.is_empty() {
            reason
        } else {
            format!("{reason}; teardown: {}", problems.join("; "))
        };
        self.finish_failed(reason).await;
    }

    fn reaped(&self, cause: &str) {
        self.audit(
            Severity::Warn,
            Action::AvGateSessionReaped,
            json!({ "cause": cause }),
        );
    }

    /// Wait for YouTube to finish the VOD. Every poll persists the quota
    /// spend, so the next admission sees it.
    async fn await_vod(&mut self) {
        let Some(bid) = self.row.broadcast_id.clone() else {
            return self
                .finish_failed("no broadcast to wait for".to_string())
                .await;
        };
        let Some(yt) = self.yt.clone() else {
            return self
                .finish_failed("cannot wait for the VOD: no YouTube manage client".to_string())
                .await;
        };
        let deadline = tokio::time::Instant::now() + self.ctx.timings.processing_timeout;
        let mut errors = 0;
        let outcome = loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let secs = self.ctx.timings.processing_timeout.as_secs();
                    break Err(format!("the VOD was not processed within {secs} s"));
                }
                _ = tokio::time::sleep(self.ctx.timings.processing_poll) => {}
            }
            let step = match yt.vod_status(&bid).await {
                Ok(status) => {
                    errors = 0;
                    vod_step(&status)
                }
                Err(e) => {
                    errors += 1;
                    if exhausted(errors, MAX_POLL_ERRORS) {
                        VodStep::Failed(format!("VOD status polling failed: {e}"))
                    } else {
                        VodStep::Pending
                    }
                }
            };
            self.persist().await;
            match step {
                VodStep::Pending => {}
                VodStep::Done => break Ok(()),
                VodStep::Failed(reason) => break Err(reason),
            }
        };
        match outcome {
            Ok(()) => {
                self.row.vod_id = Some(bid.clone());
                self.row.finished_at = Some(now_ts());
                self.set_state(SessionState::Done).await;
                self.audit(
                    Severity::Info,
                    Action::AvGateSessionDone,
                    json!({ "vod_id": bid }),
                );
            }
            Err(reason) => self.finish_failed(reason).await,
        }
    }

    /// The caller's stop: drain, teardown, `processing`, then the VOD.
    async fn stop_flow(&mut self, drain: Duration) {
        let drain = drain + self.ctx.timings.drain_extra;
        self.row.stop_requested_at = Some(now_ts());
        self.persist().await;
        self.audit(
            Severity::Info,
            Action::AvGateSessionStopRequested,
            json!({ "drain_secs": drain.as_secs() }),
        );
        tokio::time::sleep(drain).await;
        let problems = self.teardown(false).await;
        if !problems.is_empty() {
            return self
                .finish_failed(format!("teardown: {}", problems.join("; ")))
                .await;
        }
        self.row.processing_at = Some(now_ts());
        self.set_state(SessionState::Processing).await;
        self.audit(
            Severity::Info,
            Action::AvGateSessionProcessing,
            json!({ "broadcast_id": self.row.broadcast_id }),
        );
        self.ctx.registry.release(&self.row.id);
        self.await_vod().await;
    }

    /// The driver: from `starting` to `done`/`failed`.
    async fn drive(mut self, mut stop_rx: watch::Receiver<bool>, drain: Duration) {
        match self.wait_for_stop(&mut stop_rx).await {
            Wake::Stop => self.stop_flow(drain).await,
            Wake::Idle => {
                self.reaped("idle_timeout");
                let secs = self.ctx.timings.idle_timeout.as_secs();
                self.teardown_and_fail(format!("idle timeout: no stop within {secs} s"))
                    .await;
            }
            Wake::Failed(reason) => self.teardown_and_fail(reason).await,
        }
    }
}

// Creation, the reaper, the boot reconcile and the cleanup loop: a child
// module (so it reaches `Session`'s private items), split out to keep this
// file under the 1000-line cap.
#[path = "av_gate_lifecycle.rs"]
mod lifecycle;
pub use lifecycle::*;

#[cfg(test)]
#[path = "av_gate_driver_tests.rs"]
pub(crate) mod tests;
