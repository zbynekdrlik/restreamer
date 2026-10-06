//! Admission and start (#357): a failure at every start step, the mutex,
//! the quota guards, the boot gate and the start task. Child of
//! `av_gate_driver_tests.rs`, reusing its harness.

use super::super::*;
use super::{EVENT, FakeRig, Harness, reason, timings};
use std::sync::atomic::Ordering;

use crate::av_gate::Holder;

// ---- a failure at every start step --------------------------------------------

async fn start_fails(h: &Harness, id: &str) -> AvGateSessionRow {
    match h.create(id).await {
        CreateOutcome::StartFailed { session_id, reason } => {
            assert_eq!(session_id, id);
            let row = h.row(id).await;
            assert_eq!(row.state, "failed");
            assert_eq!(row.reason.as_deref(), Some(reason.as_str()));
            assert!(row.finished_at.is_some());
            assert_eq!(h.ctx.registry.holder(), None, "the slot must be free");
            row
        }
        other => panic!("expected StartFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn a_failed_stream_lookup_touches_nothing() {
    let h = Harness::new().await;
    h.yt_state.failing("lookup");
    let row = start_fails(&h, "s1").await;
    assert!(reason(&row).contains("stream lookup failed"), "{row:?}");
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
    assert!(h.rig.calls().is_empty());
}

#[tokio::test]
async fn a_missing_stream_touches_nothing() {
    let h = Harness::new().await;
    *h.yt_state.stream_title.lock().unwrap() = "something else".to_string();
    let row = start_fails(&h, "s1").await;
    assert!(
        reason(&row).contains("no YouTube stream titled \"e2e rtmp\""),
        "{row:?}"
    );
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_non_reusable_stream_is_never_bound() {
    let h = Harness::new().await;
    h.yt_state.reusable.store(false, Ordering::SeqCst);
    let row = start_fails(&h, "s1").await;
    assert!(reason(&row).contains("not reusable"), "{row:?}");
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
    assert!(h.rig.calls().is_empty());
}

#[tokio::test]
async fn a_refused_event_resolution_creates_no_broadcast() {
    let rig = FakeRig::default();
    *rig.resolve.lock().unwrap() = Err("another event is active (\"Sunday\")".to_string());
    let h = Harness::with(rig, timings()).await;
    let row = start_fails(&h, "s1").await;
    assert!(reason(&row).contains("another event is active"), "{row:?}");
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
    assert_eq!(h.rig.calls(), vec!["resolve:E2E-Test"]);
}

#[tokio::test]
async fn a_failed_insert_starts_no_event() {
    let h = Harness::new().await;
    h.yt_state.failing("insert");
    let row = start_fails(&h, "s1").await;
    assert!(
        reason(&row).contains("liveBroadcasts.insert failed"),
        "{row:?}"
    );
    assert_eq!(row.broadcast_id, None);
    assert_eq!(h.rig.calls(), vec!["resolve:E2E-Test"]);
}

#[tokio::test]
async fn a_failed_bind_leaves_the_unstarted_broadcast_alone() {
    let h = Harness::new().await;
    h.yt_state.failing("bind");
    let row = start_fails(&h, "s1").await;
    assert!(
        reason(&row).contains("liveBroadcasts.bind failed"),
        "{row:?}"
    );
    assert_eq!(row.broadcast_id.as_deref(), Some("bc-1"));
    assert!(
        h.yt_state.transitions().is_empty(),
        "a never-live broadcast is not completed"
    );
    assert_eq!(h.rig.calls(), vec!["resolve:E2E-Test"]);
}

#[tokio::test]
async fn a_refused_event_start_is_not_torn_down() {
    let rig = FakeRig::default();
    *rig.start.lock().unwrap() = Err(StartEventError::Refused("busy".to_string()));
    let h = Harness::with(rig, timings()).await;
    let row = start_fails(&h, "s1").await;
    assert_eq!(reason(&row), "busy");
    assert_eq!(row.event_id, None);
    assert_eq!(
        h.rig.calls(),
        vec!["resolve:E2E-Test".to_string(), format!("start:{EVENT}")],
        "nothing was started, so nothing may be stopped"
    );
}

#[tokio::test]
async fn a_failed_event_start_is_torn_down() {
    let rig = FakeRig::default();
    *rig.start.lock().unwrap() = Err(StartEventError::Failed("vps".to_string()));
    let mut h = Harness::with(rig, timings()).await;
    let row = start_fails(&h, "s1").await;
    assert_eq!(reason(&row), "vps");
    assert_eq!(row.event_id, Some(EVENT));
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(h.actions(), vec![Action::AvGateSessionFailed]);
}

// ---- the mutex and the quota guard -------------------------------------------

#[tokio::test]
async fn a_second_session_gets_the_holder_until_the_first_is_torn_down() {
    let h = Harness::new().await;
    h.ready_session("s-first").await;
    assert_eq!(
        h.create("s-second").await,
        CreateOutcome::Busy(Holder {
            session_id: "s-first".to_string(),
            requester: "camera-box".to_string(),
        })
    );
    assert!(
        store::get(&h.ctx.pool, "s-second").await.unwrap().is_none(),
        "a refused session leaves no row"
    );
    assert!(h.ctx.registry.request_stop("s-first"));
    h.wait_state("s-first", SessionState::Done).await;
    *h.yt_state.life.lock().unwrap() = "ready".to_string();
    h.ready_session("s-second").await;
}

async fn spend(h: &Harness, id: &str, created_at: &str, units: i64) {
    let mut row = AvGateSessionRow::new_starting(id, "r", "t", created_at);
    row.state = "done".to_string();
    row.quota_units = units;
    store::save(&h.ctx.pool, &row).await.unwrap();
}

#[tokio::test]
async fn the_quota_guard_refuses_a_session_over_the_daily_budget() {
    let h = Harness::new().await;
    spend(&h, "old", "2000-01-01T00:00:00.000Z", 100_000).await;
    spend(&h, "recent", &now_ts(), 3_601).await;
    assert_eq!(
        h.create("s1").await,
        CreateOutcome::QuotaExceeded {
            spent: 3_601,
            budget: 4_000
        }
    );
    assert_eq!(h.ctx.registry.holder(), None);
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_quota_guard_admits_a_session_that_exactly_fits() {
    let h = Harness::new().await;
    spend(&h, "recent", &now_ts(), 3_600).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
}

#[tokio::test]
async fn nothing_starts_before_the_boot_reconcile_ran() {
    let h = Harness::new().await;
    let outcome = create_session(
        h.fresh_ctx(timings()),
        h.yt(),
        "s1".to_string(),
        "r".to_string(),
        "t".to_string(),
    )
    .await;
    assert_eq!(outcome, CreateOutcome::NotReady);
    assert!(h.rig.calls().is_empty());
}

#[tokio::test]
async fn a_dropped_create_request_still_runs_the_session_to_its_end() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let rig = FakeRig::default();
    *rig.start_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let h = Harness::with(rig, timings()).await;
    let handle = spawn_create(
        Arc::clone(&h.ctx),
        h.yt(),
        "s1".to_string(),
        "camera-box".to_string(),
        "t".to_string(),
    );
    let wait_start = async {
        while !h.rig.called(&format!("start:{EVENT}")) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait_start)
        .await
        .unwrap();
    // The client disconnects: axum drops the handler, and with it the handle.
    drop(handle);
    gate.notify_one();
    h.wait_state("s1", SessionState::Ready).await;
    assert_eq!(
        h.ctx.registry.holder().map(|h| h.session_id).as_deref(),
        Some("s1"),
        "the session is driven (and will be reaped) even without its caller"
    );
}

#[tokio::test]
async fn a_start_that_panics_is_reaped() {
    let rig = FakeRig::default();
    rig.panic_on_start.store(true, Ordering::SeqCst);
    let h = Harness::with(rig, timings()).await;
    let outcome = spawn_create(
        Arc::clone(&h.ctx),
        h.yt(),
        "s1".to_string(),
        "camera-box".to_string(),
        "t".to_string(),
    )
    .await
    .unwrap();
    assert!(
        matches!(&outcome, CreateOutcome::Internal(e) if e.contains("start died")),
        "{outcome:?}"
    );
    let row = h.row("s1").await;
    assert_eq!(row.state, "failed");
    assert_eq!(reason(&row), "the session driver died");
    assert_eq!(row.event_id, Some(EVENT), "recorded before the start");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(h.ctx.registry.holder(), None);
}

#[tokio::test]
async fn spawn_create_returns_the_outcome() {
    let h = Harness::new().await;
    let outcome = spawn_create(
        Arc::clone(&h.ctx),
        h.yt(),
        "s1".to_string(),
        "camera-box".to_string(),
        "t".to_string(),
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        CreateOutcome::Created {
            session_id: "s1".to_string(),
            broadcast_id: "bc-1".to_string()
        }
    );
}

#[test]
fn the_project_bucket_must_hold_a_whole_session() {
    assert!(bucket_allows(400, 400));
    assert!(!bucket_allows(399, 400));
    assert!(bucket_allows(10_000, 400));
}

#[tokio::test]
async fn a_low_project_bucket_refuses_the_session_and_frees_the_slot() {
    static LOW: std::sync::OnceLock<rs_youtube::quota::QuotaTracker> = std::sync::OnceLock::new();
    let h = Harness::new().await;
    let ctx = h.fresh_ctx(timings());
    ctx.registry.mark_reconciled();
    let ctx = Arc::new(SessionCtx {
        quota_bucket: Some(LOW.get_or_init(|| rs_youtube::quota::QuotaTracker::new(399))),
        pool: ctx.pool.clone(),
        audit_tx: ctx.audit_tx.clone(),
        registry: Arc::clone(&ctx.registry),
        rig: Arc::clone(&ctx.rig),
        timings: timings(),
        event_name: "E2E-Test".to_string(),
        stream_title: "e2e rtmp".to_string(),
        daily_quota_budget: 4_000,
    });
    let outcome = create_session(
        Arc::clone(&ctx),
        h.yt(),
        "s1".into(),
        "r".into(),
        "t".into(),
    )
    .await;
    assert_eq!(outcome, CreateOutcome::ProjectQuotaLow { remaining: 399 });
    assert_eq!(ctx.registry.holder(), None);
    assert_eq!(h.yt_state.inserts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_project_bucket_with_room_admits_the_session() {
    static ROOMY: std::sync::OnceLock<rs_youtube::quota::QuotaTracker> = std::sync::OnceLock::new();
    let h = Harness::new().await;
    let base = h.fresh_ctx(timings());
    base.registry.mark_reconciled();
    let ctx = Arc::new(SessionCtx {
        quota_bucket: Some(ROOMY.get_or_init(|| rs_youtube::quota::QuotaTracker::new(400))),
        pool: base.pool.clone(),
        audit_tx: base.audit_tx.clone(),
        registry: Arc::clone(&base.registry),
        rig: Arc::clone(&base.rig),
        timings: timings(),
        event_name: "E2E-Test".to_string(),
        stream_title: "e2e rtmp".to_string(),
        daily_quota_budget: 4_000,
    });
    let outcome = create_session(ctx, h.yt(), "s1".into(), "r".into(), "t".into()).await;
    assert!(
        matches!(outcome, CreateOutcome::Created { .. }),
        "{outcome:?}"
    );
}
