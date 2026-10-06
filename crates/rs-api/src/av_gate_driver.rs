//! The A/V-gate session state machine (#357). See `av_gate.rs` for the
//! overview and the cleanup guarantee.
//!
//! One driver task per session owns it from creation to `done`/`failed`. Every
//! exit path funnels through [`Session::teardown`], and a supervisor task
//! tears the session down if the driver itself dies.

use std::sync::Arc;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rs_core::audit::{self, Action, AuditRow, Severity, Source};
use rs_core::db::av_gate::{self as store, AvGateSessionRow};
use rs_youtube::manage::{BroadcastTransition, ManageClient};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::av_gate::{
    AvGateRegistry, AvGateRig, AvGateTimings, Holder, RigDelivery, SESSION_QUOTA_ESTIMATE,
    SessionState, StartEventError, quota_allows,
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
}

/// What `create_session` did.
#[derive(Debug, PartialEq, Eq)]
pub enum CreateOutcome {
    Created {
        session_id: String,
        broadcast_id: String,
    },
    Busy(Holder),
    QuotaExceeded {
        spent: i64,
        budget: u32,
    },
    /// The start failed; the teardown already ran and the session is `failed`.
    StartFailed {
        session_id: String,
        reason: String,
    },
    Internal(String),
}

/// RFC 3339 UTC with milliseconds and `Z`: fixed width, so the quota guard
/// can compare `created_at` values as text.
pub fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The start of the quota guard's rolling 24 h window.
fn quota_window_start() -> String {
    (Utc::now() - chrono::Duration::hours(24)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// True once `count` consecutive failures reached `max`.
fn exhausted(count: u32, max: u32) -> bool {
    count >= max
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

/// Map a VOD `processingStatus` to the session's next step.
#[derive(Debug, PartialEq, Eq)]
enum VodStep {
    Done,
    Failed(String),
    Pending,
}

fn vod_step(status: Option<&str>) -> VodStep {
    match status {
        Some("succeeded") => VodStep::Done,
        Some(s @ ("failed" | "terminated")) => {
            VodStep::Failed(format!("YouTube could not process the VOD ({s})"))
        }
        _ => VodStep::Pending,
    }
}

/// One session in flight: its durable row plus the client it spends quota on.
pub(crate) struct Session {
    ctx: Arc<SessionCtx>,
    yt: Option<Arc<ManageClient>>,
    /// `quota_units` already on the row before this client was built (a
    /// session resumed after a restart keeps its earlier spend).
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
    fn new(ctx: Arc<SessionCtx>, yt: Option<Arc<ManageClient>>, row: AvGateSessionRow) -> Self {
        Self {
            base_units: row.quota_units,
            ctx,
            yt,
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
        match self.ctx.rig.start_event(event.id).await {
            Ok(()) => self.row.event_id = Some(event.id),
            Err(StartEventError::Refused(r)) => return Err(r),
            Err(StartEventError::Failed(r)) => {
                self.row.event_id = Some(event.id);
                return Err(r);
            }
        }
        self.persist().await;
        Ok(event.drain)
    }

    /// One readiness poll while `starting`. `Ok(true)` = ready.
    async fn readiness_step(
        &mut self,
        errors: &mut u32,
        live_failures: &mut u32,
    ) -> Result<bool, String> {
        let event_id = self.row.event_id.ok_or("session has no event")?;
        let yt = Arc::clone(self.yt.as_ref().ok_or("no YouTube manage client")?);
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
            let bid = self.row.broadcast_id.clone().unwrap_or_default();
            if !self.row.went_live {
                let sid = self.row.stream_id.clone().unwrap_or_default();
                let status = yt.stream_status(&sid).await.map_err(|e| e.to_string())?;
                if status.as_deref() != Some("active") {
                    return Ok(Ok(false));
                }
                if let Err(e) = yt
                    .transition_broadcast(&bid, BroadcastTransition::Live)
                    .await
                {
                    *live_failures += 1;
                    if exhausted(*live_failures, MAX_LIVE_ATTEMPTS) {
                        return Ok(Err(format!("transition to live failed: {e}")));
                    }
                    warn!(session = %self.row.id, "av-gate: transition live failed, retrying: {e}");
                    return Ok(Ok(false));
                }
                self.row.went_live = true;
            }
            let life = yt
                .broadcast_life_cycle(&bid)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(Ok(life.as_deref() == Some("live")))
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

    /// Release everything the session took. Returns the problems; empty means
    /// clean. Runs on EVERY exit path.
    async fn teardown(&mut self) -> Vec<String> {
        let mut problems = Vec::new();
        if let Some(bid) = self.row.broadcast_id.clone() {
            if let Err(e) = self.complete_broadcast(&bid).await {
                problems.push(e);
            }
        }
        if let Some(eid) = self.row.event_id {
            if let Err(e) = self.ctx.rig.stop_event(eid).await {
                problems.push(format!("stopping the event failed: {e}"));
            }
            if let Err(e) = self.wait_servers_gone(eid).await {
                problems.push(e);
            }
        }
        if !problems.is_empty() {
            error!(session = %self.row.id, "av-gate: teardown problems: {problems:?}");
        }
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
        let problems = self.teardown().await;
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

    /// Wait for YouTube to finish the VOD.
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
        let mut errors = 0;
        let poll = async {
            loop {
                tokio::time::sleep(self.ctx.timings.processing_poll).await;
                match yt.video_processing_status(&bid).await {
                    Ok(status) => {
                        errors = 0;
                        match vod_step(status.as_deref()) {
                            VodStep::Done => return Ok(()),
                            VodStep::Failed(r) => return Err(r),
                            VodStep::Pending => {}
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        if exhausted(errors, MAX_POLL_ERRORS) {
                            return Err(format!("VOD status polling failed: {e}"));
                        }
                    }
                }
            }
        };
        let outcome = tokio::time::timeout(self.ctx.timings.processing_timeout, poll).await;
        match outcome {
            Ok(Ok(())) => {
                self.row.vod_id = Some(bid.clone());
                self.row.finished_at = Some(now_ts());
                self.set_state(SessionState::Done).await;
                self.audit(
                    Severity::Info,
                    Action::AvGateSessionDone,
                    json!({ "vod_id": bid }),
                );
            }
            Ok(Err(reason)) => self.finish_failed(reason).await,
            Err(_) => {
                let secs = self.ctx.timings.processing_timeout.as_secs();
                self.finish_failed(format!("the VOD was not processed within {secs} s"))
                    .await
            }
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
        let problems = self.teardown().await;
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

/// Run the driver, and tear the session down if the driver task dies.
fn spawn_driver(session: Session, stop_rx: watch::Receiver<bool>, drain: Duration) {
    let ctx = Arc::clone(&session.ctx);
    let yt = session.yt.clone();
    let id = session.row.id.clone();
    let driver = tokio::spawn(session.drive(stop_rx, drain));
    tokio::spawn(async move {
        if driver.await.is_err() {
            reap_dead_driver(ctx, yt, &id).await;
        }
    });
}

/// The driver task died (a panic): clean up from the durable row.
pub(crate) async fn reap_dead_driver(
    ctx: Arc<SessionCtx>,
    yt: Option<Arc<ManageClient>>,
    session_id: &str,
) {
    error!(session = %session_id, "av-gate: the session driver died");
    let row = match store::get(&ctx.pool, session_id).await {
        Ok(Some(r)) => r,
        other => {
            error!(session = %session_id, "av-gate: dead driver's row unreadable: {other:?}");
            ctx.registry.release(session_id);
            return;
        }
    };
    let mut s = Session::new(ctx, yt, row);
    s.reaped("driver_died");
    if s.row.state == SessionState::Processing.as_str() {
        return s
            .finish_failed("the session driver died while waiting for the VOD".to_string())
            .await;
    }
    s.teardown_and_fail("the session driver died".to_string())
        .await;
}

/// `POST /api/v1/av-gate/session`. `requester`/`title` are already validated.
pub async fn create_session(
    ctx: Arc<SessionCtx>,
    yt: Arc<ManageClient>,
    session_id: String,
    requester: String,
    title: String,
) -> CreateOutcome {
    let spent = match store::quota_units_since(&ctx.pool, &quota_window_start()).await {
        Ok(v) => v,
        Err(e) => return CreateOutcome::Internal(format!("quota lookup failed: {e}")),
    };
    if !quota_allows(spent, SESSION_QUOTA_ESTIMATE, ctx.daily_quota_budget) {
        return CreateOutcome::QuotaExceeded {
            spent,
            budget: ctx.daily_quota_budget,
        };
    }
    let stop_rx = match ctx.registry.claim(Holder {
        session_id: session_id.clone(),
        requester: requester.clone(),
    }) {
        Ok(rx) => rx,
        Err(holder) => return CreateOutcome::Busy(holder),
    };
    let row = AvGateSessionRow::new_starting(&session_id, &requester, &title, &now_ts());
    if let Err(e) = store::save(&ctx.pool, &row).await {
        ctx.registry.release(&session_id);
        return CreateOutcome::Internal(format!("saving the session failed: {e}"));
    }
    let mut s = Session::new(ctx, Some(yt), row);
    match s.start_leg().await {
        Ok(drain) => {
            s.persist().await;
            s.audit(
                Severity::Info,
                Action::AvGateSessionStarted,
                json!({
                    "requester": s.row.requester,
                    "broadcast_id": s.row.broadcast_id,
                    "stream_id": s.row.stream_id,
                    "event_id": s.row.event_id,
                }),
            );
            let broadcast_id = s.row.broadcast_id.clone().unwrap_or_default();
            spawn_driver(s, stop_rx, drain);
            CreateOutcome::Created {
                session_id,
                broadcast_id,
            }
        }
        Err(reason) => {
            s.teardown_and_fail(reason).await;
            CreateOutcome::StartFailed {
                session_id,
                reason: s.row.reason.clone().unwrap_or_default(),
            }
        }
    }
}

/// After a restart: a session left `starting`/`ready` still owns a broadcast
/// and a delivery, so it is torn down and failed; a `processing` one resumes
/// its VOD wait. `yt` is `Err` when the oauth file is unusable: the rig is
/// still torn down, and the reason says the broadcast could not be completed.
pub async fn reconcile_on_boot(ctx: Arc<SessionCtx>, yt: Result<Arc<ManageClient>, String>) {
    let rows = match store::list_unfinished(&ctx.pool).await {
        Ok(rows) => rows,
        Err(e) => {
            error!("av-gate boot reconcile: listing sessions failed: {e}");
            return;
        }
    };
    if let Err(e) = &yt {
        if !rows.is_empty() {
            warn!("av-gate boot reconcile: no YouTube client: {e}");
        }
    }
    for row in rows {
        let mut s = Session::new(Arc::clone(&ctx), yt.as_ref().ok().cloned(), row);
        if s.row.state == SessionState::Processing.as_str() {
            info!(session = %s.row.id, "av-gate boot reconcile: resuming the VOD wait");
            tokio::spawn(async move { s.await_vod().await });
            continue;
        }
        s.reaped("boot_reconcile");
        s.teardown_and_fail("Restreamer restarted during the session".to_string())
            .await;
    }
}

#[cfg(test)]
#[path = "av_gate_driver_tests.rs"]
pub(crate) mod tests;
