//! A/V-gate session lifecycle around the state machine (#357): admission and
//! start (`spawn_create`), the reaper for a dead start or driver, the boot
//! reconcile, the failed-teardown retry loop with its supervisor, and the
//! operator's status and force-clear. Child module of `av_gate_driver.rs`.

use std::sync::Arc;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rs_core::audit::{Action, Severity};
use rs_core::db::av_gate::{self as store, AvGateSessionRow};
use rs_youtube::manage::ManageClient;
use serde::Serialize;
use serde_json::json;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

use super::{Session, SessionCtx, append_reason, now_ts};
use crate::av_gate::{Holder, SESSION_QUOTA_ESTIMATE, SessionState, quota_allows};

/// The cleanup retry backoff doubles up to this many times (5 min -> 160 min,
/// then capped at 24 x the base: 2 h).
const MAX_BACKOFF_DOUBLINGS: u32 = 5;

/// Builds a fresh manage client (one per resumed session, so each row's quota
/// spend is its own). `Err` when the oauth file is unusable.
pub type ClientFactory = Arc<dyn Fn() -> Result<ManageClient, String> + Send + Sync>;

/// What `create_session` did.
#[derive(Debug, PartialEq, Eq)]
pub enum CreateOutcome {
    Created {
        session_id: String,
        broadcast_id: String,
    },
    Busy(Holder),
    /// The boot reconcile has not finished: an unfinished session from before
    /// the restart may still own the event.
    NotReady,
    /// An earlier session's teardown failed and is being retried.
    CleanupPending(Vec<String>),
    /// Over the av-gate's own rolling-24 h budget.
    QuotaExceeded {
        spent: i64,
        budget: u32,
    },
    /// The project-wide bucket (shared with the health polling) cannot pay a
    /// whole session.
    ProjectQuotaLow {
        remaining: u32,
    },
    /// The start failed; the teardown already ran and the session is `failed`.
    StartFailed {
        session_id: String,
        reason: String,
    },
    Internal(String),
}

/// `GET /api/v1/av-gate/status` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateStatus {
    /// False until the boot reconcile ran: sessions are refused (503).
    pub reconciled: bool,
    pub holder: Option<Holder>,
    /// Sessions whose teardown is being retried: sessions are refused (409).
    pub cleanup_pending: Vec<String>,
}

/// The start of the quota guard's rolling 24 h window.
pub(crate) fn quota_window_start() -> String {
    (Utc::now() - chrono::Duration::hours(24)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The project bucket has a whole session's estimate left.
pub(crate) fn bucket_allows(remaining: u32, estimate: u32) -> bool {
    remaining >= estimate
}

/// The wait before the next cleanup retry after `failed_rounds` failed ones.
pub(crate) fn cleanup_retry_delay(base: Duration, failed_rounds: u32) -> Duration {
    let doubled = base * 2u32.pow(failed_rounds.min(MAX_BACKOFF_DOUBLINGS));
    doubled.min(base * 24)
}

/// The failed-round count after a retry round that left `pending` sessions.
pub(crate) fn next_failed_rounds(pending: usize, failed_rounds: u32) -> u32 {
    match pending {
        0 => 0,
        _ => failed_rounds.saturating_add(1),
    }
}

/// Run the driver, and tear the session down if the driver task dies.
fn spawn_driver(session: Session, stop_rx: watch::Receiver<bool>, drain: Duration) {
    let ctx = Arc::clone(&session.ctx);
    let yt = session.yt.clone();
    let base = session.base_units;
    let id = session.row.id.clone();
    let driver = tokio::spawn(session.drive(stop_rx, drain));
    tokio::spawn(async move {
        if driver.await.is_err() {
            reap_dead_driver(ctx, yt, base, &id).await;
        }
    });
}

/// The driver (or the start) died: clean up from the durable row. `base` is
/// the row's spend before `yt` was used for it.
pub(crate) async fn reap_dead_driver(
    ctx: Arc<SessionCtx>,
    yt: Option<Arc<ManageClient>>,
    base: i64,
    session_id: &str,
) {
    error!(session = %session_id, "av-gate: the session task died");
    let row = match store::get(&ctx.pool, session_id).await {
        Ok(Some(r)) => r,
        other => {
            error!(session = %session_id, "av-gate: dead session's row unreadable: {other:?}");
            ctx.registry.release(session_id);
            return;
        }
    };
    let finished =
        row.state == SessionState::Done.as_str() || row.state == SessionState::Failed.as_str();
    if finished {
        // It ended before it died: nothing of the rig is its own any more.
        ctx.registry.release(session_id);
        return;
    }
    let mut s = Session::with_base(ctx, yt, row, base);
    s.reaped("driver_died");
    if s.row.state == SessionState::Processing.as_str() {
        return s
            .finish_failed("the session driver died while waiting for the VOD".to_string())
            .await;
    }
    s.teardown_and_fail("the session driver died".to_string())
        .await;
}

/// The admission checks and the start. Callers use [`spawn_create`]. The
/// slot is claimed FIRST, so a concurrent teardown that is about to leave a
/// pending cleanup has already written it when the check below reads.
pub(crate) async fn create_session(
    ctx: Arc<SessionCtx>,
    yt: Arc<ManageClient>,
    session_id: String,
    requester: String,
    title: String,
) -> CreateOutcome {
    if !ctx.registry.is_reconciled() {
        return CreateOutcome::NotReady;
    }
    let stop_rx = match ctx.registry.claim(Holder {
        session_id: session_id.clone(),
        requester: requester.clone(),
    }) {
        Ok(rx) => rx,
        Err(holder) => return CreateOutcome::Busy(holder),
    };
    let refused = admission_refusal(&ctx).await;
    if let Some(outcome) = refused {
        ctx.registry.release(&session_id);
        return outcome;
    }
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

/// Why a claimed session may not start: a pending cleanup, the av-gate's own
/// daily budget, or the shared project bucket.
async fn admission_refusal(ctx: &SessionCtx) -> Option<CreateOutcome> {
    match store::list_cleanup_pending(&ctx.pool).await {
        Ok(rows) if rows.is_empty() => {}
        Ok(rows) => {
            return Some(CreateOutcome::CleanupPending(
                rows.into_iter().map(|r| r.id).collect(),
            ));
        }
        Err(e) => {
            return Some(CreateOutcome::Internal(format!(
                "cleanup lookup failed: {e}"
            )));
        }
    }
    let spent = match store::quota_units_since(&ctx.pool, &quota_window_start()).await {
        Ok(v) => v,
        Err(e) => return Some(CreateOutcome::Internal(format!("quota lookup failed: {e}"))),
    };
    if !quota_allows(spent, SESSION_QUOTA_ESTIMATE, ctx.daily_quota_budget) {
        return Some(CreateOutcome::QuotaExceeded {
            spent,
            budget: ctx.daily_quota_budget,
        });
    }
    match ctx.quota_bucket.map(|b| b.remaining()) {
        Some(remaining) if !bucket_allows(remaining, SESSION_QUOTA_ESTIMATE) => {
            Some(CreateOutcome::ProjectQuotaLow { remaining })
        }
        _ => None,
    }
}

/// `POST /api/v1/av-gate/session`, off the request future: the start keeps
/// going (and ends in a driver or a teardown) even if the HTTP client
/// disconnects and the handler is dropped, and a panic in it is reaped. A
/// caller that lost the response learns the session id from the 409 holder.
pub fn spawn_create(
    ctx: Arc<SessionCtx>,
    yt: Arc<ManageClient>,
    session_id: String,
    requester: String,
    title: String,
) -> JoinHandle<CreateOutcome> {
    tokio::spawn(async move {
        let start = tokio::spawn(create_session(
            Arc::clone(&ctx),
            Arc::clone(&yt),
            session_id.clone(),
            requester,
            title,
        ));
        match start.await {
            Ok(outcome) => outcome,
            Err(e) => {
                reap_dead_driver(ctx, Some(yt), 0, &session_id).await;
                CreateOutcome::Internal(format!("the session start died: {e}"))
            }
        }
    })
}

/// After a restart: a session left `starting`/`ready` still owns a broadcast
/// and a delivery, so it is torn down and failed; a `processing` one resumes
/// its VOD wait. Each row gets its own client; when the oauth file is unusable
/// the rig is still torn down and the reason says the broadcast was not
/// completed. Opens the API (`mark_reconciled`) only if the sessions could be
/// listed.
pub async fn reconcile_on_boot(ctx: &Arc<SessionCtx>, clients: &ClientFactory) {
    let rows = match store::list_unfinished(&ctx.pool).await {
        Ok(rows) => rows,
        Err(e) => {
            error!("av-gate boot reconcile: listing sessions failed: {e}");
            return;
        }
    };
    for row in rows {
        let yt = clients().ok().map(Arc::new);
        let mut s = Session::new(Arc::clone(ctx), yt, row);
        if s.row.state == SessionState::Processing.as_str() {
            info!(session = %s.row.id, "av-gate boot reconcile: resuming the VOD wait");
            tokio::spawn(async move { s.await_vod().await });
            continue;
        }
        s.reaped("boot_reconcile");
        s.teardown_and_fail("Restreamer restarted during the session".to_string())
            .await;
    }
    ctx.registry.mark_reconciled();
}

/// Retry every failed teardown once (only the part still missing). Returns
/// how many are still pending.
pub async fn retry_cleanups(ctx: &Arc<SessionCtx>, clients: &ClientFactory) -> usize {
    let rows = match store::list_cleanup_pending(&ctx.pool).await {
        Ok(rows) => rows,
        Err(e) => {
            error!("av-gate cleanup retry: listing sessions failed: {e}");
            return 1;
        }
    };
    let mut pending = 0;
    for row in rows {
        let yt = clients().ok().map(Arc::new);
        let mut s = Session::new(Arc::clone(ctx), yt, row);
        let problems = s.teardown(true).await;
        s.audit(
            Severity::Warn,
            Action::AvGateSessionReaped,
            json!({ "cause": "cleanup_retry", "problems": problems }),
        );
        if problems.is_empty() {
            s.row.reason = Some(append_reason(
                s.row.reason.take(),
                "cleanup completed on a later retry",
            ));
        } else {
            pending += 1;
        }
        s.persist().await;
    }
    pending
}

/// The maintenance loop: the boot reconcile (retried until it could list the
/// sessions), then failed teardowns retried with backoff (`cleanup_retry`,
/// doubling, capped at 24x).
pub async fn run_maintenance(ctx: Arc<SessionCtx>, clients: ClientFactory) {
    let mut failed_rounds = 0;
    loop {
        if !ctx.registry.is_reconciled() {
            reconcile_on_boot(&ctx, &clients).await;
        }
        let pending = retry_cleanups(&ctx, &clients).await;
        failed_rounds = next_failed_rounds(pending, failed_rounds);
        tokio::time::sleep(cleanup_retry_delay(
            ctx.timings.cleanup_retry,
            failed_rounds,
        ))
        .await;
    }
}

/// Keep the maintenance loop alive: a panic in it (a rig or DB bug) restarts
/// it after `cleanup_retry` instead of leaving the API closed for good.
pub async fn supervise_maintenance(ctx: Arc<SessionCtx>, clients: ClientFactory) {
    loop {
        let task = tokio::spawn(run_maintenance(Arc::clone(&ctx), Arc::clone(&clients)));
        if let Err(e) = task.await {
            error!("av-gate maintenance task died, restarting it: {e}");
        }
        tokio::time::sleep(ctx.timings.cleanup_retry).await;
    }
}

/// `GET /api/v1/av-gate/status`.
pub async fn gate_status(ctx: &SessionCtx) -> Result<GateStatus, String> {
    let pending = store::list_cleanup_pending(&ctx.pool)
        .await
        .map_err(|e| e.to_string())?;
    Ok(GateStatus {
        reconciled: ctx.registry.is_reconciled(),
        holder: ctx.registry.holder(),
        cleanup_pending: pending.into_iter().map(|r| r.id).collect(),
    })
}

/// What an operator's force-clear did.
#[derive(Debug, PartialEq, Eq)]
pub enum ClearOutcome {
    Cleared,
    NotPending,
    NotFound,
}

/// The operator's way out of a cleanup that can never succeed (a broken
/// oauth file, a resource removed by hand): drop `cleanup_pending` WITHOUT
/// retrying it, with an audit row. Whoever calls it owns what may still be
/// live or billing.
pub async fn clear_cleanup(
    ctx: &Arc<SessionCtx>,
    session_id: &str,
) -> Result<ClearOutcome, String> {
    let row = match store::get(&ctx.pool, session_id)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(r) => r,
        None => return Ok(ClearOutcome::NotFound),
    };
    if !row.cleanup_pending {
        return Ok(ClearOutcome::NotPending);
    }
    let mut s = Session::new(Arc::clone(ctx), None, row);
    s.row.cleanup_pending = false;
    s.row.reason = Some(append_reason(
        s.row.reason.take(),
        "cleanup cleared by an operator",
    ));
    s.persist().await;
    s.audit(
        Severity::Warn,
        Action::AvGateSessionReaped,
        json!({ "cause": "operator_clear" }),
    );
    Ok(ClearOutcome::Cleared)
}
