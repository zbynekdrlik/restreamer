//! Failed teardowns, their retries, the maintenance loop and its supervisor,
//! and the operator's status + force-clear (#357). Child of
//! `av_gate_driver_tests.rs`, reusing its harness.

use super::super::*;
use super::{EVENT, FakeRig, Harness, reason, timings};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use rs_core::audit::Action;

async fn stop_and_wait(h: &Harness, id: &str, state: SessionState) -> AvGateSessionRow {
    assert!(h.ctx.registry.request_stop(id));
    h.wait_state(id, state).await
}

async fn seed(h: &Harness, id: &str, state: &str, live: bool) {
    let mut row = AvGateSessionRow::new_starting(id, "r", "t", &now_ts());
    row.state = state.to_string();
    row.broadcast_id = Some("bc-1".to_string());
    row.stream_id = Some("st-e2e".to_string());
    row.event_id = Some(EVENT);
    row.went_live = live;
    row.quota_units = 100;
    store::save(&h.ctx.pool, &row).await.unwrap();
}

#[test]
fn cleanup_retries_back_off_to_a_two_hour_cap() {
    let base = Duration::from_secs(300);
    assert_eq!(cleanup_retry_delay(base, 0), Duration::from_secs(300));
    assert_eq!(cleanup_retry_delay(base, 1), Duration::from_secs(600));
    assert_eq!(cleanup_retry_delay(base, 4), Duration::from_secs(4_800));
    assert_eq!(cleanup_retry_delay(base, 5), Duration::from_secs(7_200));
    assert_eq!(cleanup_retry_delay(base, 40), Duration::from_secs(7_200));
}

#[test]
fn failed_rounds_grow_until_a_round_is_clean() {
    assert_eq!(next_failed_rounds(0, 7), 0);
    assert_eq!(next_failed_rounds(1, 0), 1);
    assert_eq!(next_failed_rounds(3, 4), 5);
    assert_eq!(next_failed_rounds(1, u32::MAX), u32::MAX);
}

#[test]
fn append_reason_joins_without_a_leading_separator() {
    assert_eq!(append_reason(None, "b"), "b");
    assert_eq!(append_reason(Some(String::new()), "b"), "b");
    assert_eq!(append_reason(Some("a".to_string()), "b"), "a; b");
}

// ---- failed teardowns stay pending until a retry is clean ----------------------

#[tokio::test]
async fn a_failed_teardown_blocks_new_sessions_until_a_retry_cleans_it() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1)]);
    let mut h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(row.cleanup_pending, "{row:?}");
    assert_eq!(
        h.create("s2").await,
        CreateOutcome::CleanupPending(vec!["s1".to_string()])
    );

    // Still failing: stays pending.
    assert_eq!(retry_cleanups(&h.ctx, &h.clients()).await, 1);
    assert!(h.row("s1").await.cleanup_pending);

    *h.rig.servers.lock().unwrap() = VecDeque::from([Ok(0)]);
    assert_eq!(retry_cleanups(&h.ctx, &h.clients()).await, 0);
    let row = h.row("s1").await;
    assert!(!row.cleanup_pending);
    assert_eq!(row.state, "failed");
    assert!(
        reason(&row).ends_with("; cleanup completed on a later retry"),
        "{row:?}"
    );
    let reaped = h
        .actions()
        .iter()
        .filter(|a| **a == Action::AvGateSessionReaped)
        .count();
    assert_eq!(reaped, 2, "one audit row per retry");
    *h.yt_state.life.lock().unwrap() = "ready".to_string();
    h.ready_session("s2").await;
}

#[tokio::test]
async fn a_clean_teardown_leaves_nothing_pending() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    let row = stop_and_wait(&h, "s1", SessionState::Done).await;
    assert!(!row.cleanup_pending);
    assert_eq!(retry_cleanups(&h.ctx, &h.clients()).await, 0);
}

#[tokio::test]
async fn the_maintenance_loop_reconciles_then_retries_until_clean() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1), Ok(1), Ok(1), Ok(1), Ok(1), Ok(0)]);
    let h = Harness::with(rig, timings()).await;
    seed(&h, "s1", "ready", true).await;
    let ctx = h.fresh_ctx(AvGateTimings {
        servers_gone_timeout: Duration::from_millis(1),
        ..timings()
    });
    let task = tokio::spawn(run_maintenance(Arc::clone(&ctx), h.clients()));
    let clean = async {
        loop {
            let row = h.row("s1").await;
            if row.state == "failed" && !row.cleanup_pending {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let row = tokio::time::timeout(Duration::from_secs(8), clean)
        .await
        .expect("the loop must eventually clean the session");
    task.abort();
    assert!(ctx.registry.is_reconciled());
    assert!(reason(&row).starts_with("Restreamer restarted during the session"));
    assert!(reason(&row).ends_with("cleanup completed on a later retry"));
}

#[tokio::test]
async fn a_boot_reconcile_that_cannot_list_keeps_the_api_closed() {
    let h = Harness::new().await;
    let ctx = h.fresh_ctx(timings());
    sqlx::query("ALTER TABLE av_gate_sessions RENAME TO av_gate_sessions_gone")
        .execute(&h.ctx.pool)
        .await
        .unwrap();
    reconcile_on_boot(&ctx, &h.clients()).await;
    assert!(!ctx.registry.is_reconciled());
    assert_eq!(retry_cleanups(&ctx, &h.clients()).await, 1);
}

#[tokio::test]
async fn a_retry_never_stops_an_event_another_run_has_taken() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1)]);
    let h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    stop_and_wait(&h, "s1", SessionState::Failed).await;
    let stops = |h: &Harness| {
        h.rig
            .calls()
            .iter()
            .filter(|c| **c == format!("stop:{EVENT}"))
            .count()
    };
    assert_eq!(stops(&h), 1);
    h.rig.active.store(true, Ordering::SeqCst);
    assert_eq!(retry_cleanups(&h.ctx, &h.clients()).await, 1);
    assert_eq!(stops(&h), 1, "an active event belongs to someone else now");
    let row = h.row("s1").await;
    assert!(row.cleanup_pending);
    assert!(h.rig.called(&format!("active:{EVENT}")));
}

#[tokio::test]
async fn a_retry_repeats_only_the_half_that_failed() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    h.yt_state.failing("complete");
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(!row.broadcast_done && row.event_done, "{row:?}");
    let calls_before = h.rig.calls().len();
    h.yt_state.healed("complete");
    assert_eq!(retry_cleanups(&h.ctx, &h.clients()).await, 0);
    assert_eq!(
        h.rig.calls().len(),
        calls_before,
        "the event half already succeeded: the rig is not touched again"
    );
    assert_eq!(
        h.yt_state.transitions(),
        vec!["live", "complete", "complete", "complete", "complete"]
    );
    let row = h.row("s1").await;
    assert!(row.broadcast_done && !row.cleanup_pending);
}

#[tokio::test]
async fn a_never_live_broadcast_needs_no_youtube_to_clean_up() {
    let h = Harness::new().await;
    seed(&h, "s1", "starting", false).await;
    let no_client: ClientFactory = Arc::new(|| Err("oauth file missing".to_string()));
    reconcile_on_boot(&h.ctx, &no_client).await;
    let row = h.row("s1").await;
    assert_eq!(reason(&row), "Restreamer restarted during the session");
    assert!(!row.cleanup_pending && row.broadcast_done && row.event_done);
    assert!(h.yt_state.transitions().is_empty());
}

#[tokio::test]
async fn the_supervisor_restarts_a_maintenance_loop_that_panicked() {
    let rig = FakeRig::default();
    rig.panic_on_servers.store(true, Ordering::SeqCst);
    let h = Harness::with(rig, timings()).await;
    seed(&h, "s1", "ready", false).await;
    let ctx = h.fresh_ctx(timings());
    let task = tokio::spawn(supervise_maintenance(Arc::clone(&ctx), h.clients()));
    let clean = async {
        loop {
            let row = h.row("s1").await;
            if row.state == "failed" && !row.cleanup_pending {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(8), clean)
        .await
        .expect("the restarted loop must finish the job");
    task.abort();
    assert!(ctx.registry.is_reconciled());
    let servers = h
        .rig
        .calls()
        .iter()
        .filter(|c| c.starts_with("servers:"))
        .count();
    assert!(servers >= 2, "the first check panicked, a later one passed");
}

#[tokio::test]
async fn status_reports_the_gate_and_clear_drops_a_stuck_cleanup() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1)]);
    let mut h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    let status = gate_status(&h.ctx).await.unwrap();
    assert!(status.reconciled);
    assert_eq!(status.holder.map(|h| h.session_id).as_deref(), Some("s1"));
    assert!(status.cleanup_pending.is_empty());
    stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert_eq!(
        gate_status(&h.ctx).await.unwrap(),
        GateStatus {
            reconciled: true,
            holder: None,
            cleanup_pending: vec!["s1".to_string()],
        }
    );
    h.actions();
    assert_eq!(clear_cleanup(&h.ctx, "s1").await, Ok(ClearOutcome::Cleared));
    let row = h.row("s1").await;
    assert!(!row.cleanup_pending);
    assert!(
        reason(&row).ends_with("; cleanup cleared by an operator"),
        "{row:?}"
    );
    assert_eq!(h.actions(), vec![Action::AvGateSessionReaped]);
    assert_eq!(
        clear_cleanup(&h.ctx, "s1").await,
        Ok(ClearOutcome::NotPending)
    );
    assert_eq!(
        clear_cleanup(&h.ctx, "nope").await,
        Ok(ClearOutcome::NotFound)
    );
    assert!(
        gate_status(&h.ctx)
            .await
            .unwrap()
            .cleanup_pending
            .is_empty()
    );
}

#[tokio::test]
async fn a_dead_driver_of_a_finished_session_only_frees_the_slot() {
    let h = Harness::new().await;
    let _rx = h
        .ctx
        .registry
        .claim(crate::av_gate::Holder {
            session_id: "s1".to_string(),
            requester: "r".to_string(),
        })
        .unwrap();
    seed(&h, "s1", "done", true).await;
    reap_dead_driver(Arc::clone(&h.ctx), None, 0, "s1").await;
    assert_eq!(h.row("s1").await.state, "done");
    assert!(h.rig.calls().is_empty());
    assert_eq!(h.ctx.registry.holder(), None);
}
