//! The stop path, the teardown, VOD processing, the reaper and the boot
//! reconcile (#357). Child of `av_gate_driver_tests.rs`, reusing its harness.

use super::super::*;
use super::{EVENT, FakeRig, Harness, reason, timings};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use crate::av_gate::{Holder, RigEvent};
use rs_core::audit::Action;

async fn stop_and_wait(h: &Harness, id: &str, state: SessionState) -> AvGateSessionRow {
    assert!(h.ctx.registry.request_stop(id));
    h.wait_state(id, state).await
}

// ---- drain + teardown problems on the stop path ------------------------------

#[tokio::test]
async fn the_stop_waits_for_the_cache_drain_before_completing() {
    let rig = FakeRig::default();
    *rig.resolve.lock().unwrap() = Ok(RigEvent {
        id: EVENT,
        drain: Duration::from_millis(60),
    });
    let h = Harness::with(
        rig,
        AvGateTimings {
            drain_extra: Duration::from_millis(60),
            ..timings()
        },
    )
    .await;
    h.ready_session("s1").await;
    assert!(h.ctx.registry.request_stop("s1"));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        h.yt_state.transitions(),
        vec!["live"],
        "nothing is completed while the cache (60 ms + 60 ms) drains"
    );
    assert!(h.row("s1").await.stop_requested_at.is_some());
    h.wait_state("s1", SessionState::Done).await;
    assert_eq!(h.yt_state.transitions(), vec!["live", "complete"]);
}

#[tokio::test]
async fn servers_that_never_go_fail_the_stop_with_the_count() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1)]);
    let mut h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert_eq!(
        reason(&row),
        format!("teardown: 1 Hetzner server(s) still exist for event {EVENT}")
    );
    assert_eq!(row.processing_at, None, "never reached processing");
    assert_eq!(h.yt_state.transitions(), vec!["live", "complete"]);
    assert_eq!(h.ctx.registry.holder(), None);
    assert_eq!(h.actions().last(), Some(&Action::AvGateSessionFailed));
}

#[tokio::test]
async fn servers_that_disappear_on_a_later_poll_are_fine() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Ok(1), Err("blip".to_string()), Ok(0)]);
    let h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    stop_and_wait(&h, "s1", SessionState::Done).await;
    let polls = h
        .rig
        .calls()
        .iter()
        .filter(|c| c.starts_with("servers:"))
        .count();
    assert_eq!(polls, 3);
}

#[tokio::test]
async fn a_failing_server_check_is_reported() {
    let rig = FakeRig::default();
    *rig.servers.lock().unwrap() = VecDeque::from([Err("401 Unauthorized".to_string())]);
    let h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(
        reason(&row).contains("Hetzner server check failed: 401 Unauthorized"),
        "{row:?}"
    );
}

#[tokio::test]
async fn a_failing_event_stop_is_reported_and_left_pending() {
    let rig = FakeRig::default();
    *rig.stop.lock().unwrap() = Err("HTTP 500".to_string());
    let h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert_eq!(
        reason(&row),
        "teardown: stopping the event failed: HTTP 500"
    );
    assert!(row.cleanup_pending && row.broadcast_done && !row.event_done);
    assert!(
        !h.rig.called(&format!("servers:{EVENT}")),
        "servers are checked once the stop succeeded"
    );
}

#[tokio::test]
async fn a_broadcast_that_will_not_complete_is_retried_then_reported() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    h.yt_state.failing("complete");
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(
        reason(&row)
            .contains("broadcast bc-1 not completed: API error: 500 - invalidTransition: scripted"),
        "{row:?}"
    );
    assert_eq!(
        h.yt_state.transitions(),
        vec!["live", "complete", "complete", "complete"]
    );
    assert!(
        h.rig.called(&format!("stop:{EVENT}")),
        "the delivery is stopped even when YouTube refuses"
    );
}

#[tokio::test]
async fn a_broadcast_stuck_starting_is_reported() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    *h.yt_state.life.lock().unwrap() = "liveStarting".to_string();
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(reason(&row).contains("still liveStarting"), "{row:?}");
    assert_eq!(h.yt_state.transitions(), vec!["live"]);
}

#[tokio::test]
async fn an_unreadable_life_cycle_is_reported() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    h.yt_state.failing("life");
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(
        reason(&row).contains("not completed: API error: 500"),
        "{row:?}"
    );
}

// ---- processing ---------------------------------------------------------------------

#[tokio::test]
async fn a_vod_youtube_could_not_process_fails_the_session() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "failed".to_string();
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert_eq!(reason(&row), "YouTube could not process the VOD (failed)");
    assert!(row.processing_at.is_some());
    assert_eq!(row.vod_id, None);
}

#[tokio::test]
async fn a_vod_still_processing_at_the_timeout_fails_the_session() {
    let h = Harness::with(
        FakeRig::default(),
        AvGateTimings {
            processing_timeout: Duration::from_millis(60),
            ..timings()
        },
    )
    .await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "processing".to_string();
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert_eq!(reason(&row), "the VOD was not processed within 0 s");
}

#[tokio::test]
async fn repeated_vod_poll_errors_fail_the_session() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    h.yt_state.failing("video");
    let row = stop_and_wait(&h, "s1", SessionState::Failed).await;
    assert!(
        reason(&row).starts_with("VOD status polling failed"),
        "{row:?}"
    );
}

#[tokio::test]
async fn vod_poll_errors_between_successes_are_forgiven() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "processing".to_string();
    assert!(h.ctx.registry.request_stop("s1"));
    h.wait_state("s1", SessionState::Processing).await;
    for _ in 0..3 {
        h.yt_state.failing("video");
        tokio::time::sleep(Duration::from_millis(12)).await;
        h.yt_state.healed("video");
        tokio::time::sleep(Duration::from_millis(12)).await;
    }
    *h.yt_state.video.lock().unwrap() = "succeeded".to_string();
    let row = h.wait_state("s1", SessionState::Done).await;
    assert_eq!(row.vod_id.as_deref(), Some("bc-1"));
}

#[tokio::test]
async fn processing_frees_the_slot_for_the_next_session() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "processing".to_string();
    stop_and_wait(&h, "s1", SessionState::Processing).await;
    assert_eq!(h.ctx.registry.holder(), None);
}

// ---- the reaper --------------------------------------------------------------------

#[tokio::test]
async fn a_ready_session_with_no_stop_is_reaped_at_the_idle_timeout() {
    let mut h = Harness::with(
        FakeRig::default(),
        AvGateTimings {
            idle_timeout: Duration::from_millis(250),
            ..timings()
        },
    )
    .await;
    h.ready_session("s1").await;
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert_eq!(reason(&row), "idle timeout: no stop within 0 s");
    assert_eq!(h.yt_state.transitions(), vec!["live", "complete"]);
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(h.ctx.registry.holder(), None);
    let actions = h.actions();
    assert!(
        actions.contains(&Action::AvGateSessionReaped),
        "{actions:?}"
    );
    assert_eq!(actions.last(), Some(&Action::AvGateSessionFailed));
}

#[tokio::test]
async fn a_session_that_never_gets_ready_is_reaped_too() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::Booting)]);
    let h = Harness::with(
        rig,
        AvGateTimings {
            idle_timeout: Duration::from_millis(120),
            ..timings()
        },
    )
    .await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert!(reason(&row).starts_with("idle timeout"), "{row:?}");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
}

#[tokio::test]
async fn a_driver_that_dies_is_torn_down_by_its_supervisor() {
    let rig = FakeRig::default();
    rig.panic_on_delivery.store(true, Ordering::SeqCst);
    let mut h = Harness::with(rig, timings()).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert_eq!(reason(&row), "the session driver died");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(h.ctx.registry.holder(), None);
    assert!(h.actions().contains(&Action::AvGateSessionReaped));
}

#[tokio::test]
async fn a_dead_driver_in_processing_is_only_marked_failed() {
    let h = Harness::new().await;
    let mut row = AvGateSessionRow::new_starting("s1", "r", "t", &now_ts());
    row.state = "processing".to_string();
    row.broadcast_id = Some("bc-1".to_string());
    row.event_id = Some(EVENT);
    store::save(&h.ctx.pool, &row).await.unwrap();
    reap_dead_driver(Arc::clone(&h.ctx), Some(h.yt()), 0, "s1").await;
    let row = h.row("s1").await;
    assert_eq!(row.state, "failed");
    assert_eq!(
        reason(&row),
        "the session driver died while waiting for the VOD"
    );
    assert!(h.rig.calls().is_empty(), "processing is already torn down");
}

#[tokio::test]
async fn a_dead_driver_without_a_row_still_frees_the_slot() {
    let h = Harness::new().await;
    let _rx = h
        .ctx
        .registry
        .claim(Holder {
            session_id: "ghost".to_string(),
            requester: "r".to_string(),
        })
        .unwrap();
    reap_dead_driver(Arc::clone(&h.ctx), None, 0, "ghost").await;
    assert_eq!(h.ctx.registry.holder(), None);
}

// ---- boot reconcile ---------------------------------------------------------------------

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

#[tokio::test]
async fn boot_reconcile_tears_down_a_session_left_ready() {
    let mut h = Harness::new().await;
    seed(&h, "s1", "ready", true).await;
    *h.yt_state.life.lock().unwrap() = "live".to_string();
    reconcile_on_boot(&h.ctx, &h.clients()).await;
    let row = h.row("s1").await;
    assert_eq!(row.state, "failed");
    assert_eq!(reason(&row), "Restreamer restarted during the session");
    assert_eq!(h.yt_state.transitions(), vec!["complete"]);
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(row.quota_units, 151, "earlier spend + life 1 + complete 50");
    assert_eq!(
        h.actions(),
        vec![Action::AvGateSessionReaped, Action::AvGateSessionFailed]
    );
}

#[tokio::test]
async fn boot_reconcile_resumes_the_vod_wait_of_a_processing_session() {
    let h = Harness::new().await;
    seed(&h, "s1", "processing", true).await;
    reconcile_on_boot(&h.ctx, &h.clients()).await;
    let row = h.wait_state("s1", SessionState::Done).await;
    assert_eq!(row.vod_id.as_deref(), Some("bc-1"));
    assert!(h.rig.calls().is_empty());
}

#[tokio::test]
async fn boot_reconcile_without_youtube_still_stops_the_delivery() {
    let h = Harness::new().await;
    seed(&h, "s1", "starting", true).await;
    seed(&h, "s2", "processing", true).await;
    let no_client: ClientFactory = Arc::new(|| Err("oauth file missing".to_string()));
    reconcile_on_boot(&h.ctx, &no_client).await;
    let row = h.row("s1").await;
    assert_eq!(row.state, "failed");
    assert!(reason(&row).contains("no YouTube manage client"), "{row:?}");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    let row = h.wait_state("s2", SessionState::Failed).await;
    assert!(reason(&row).contains("no YouTube manage client"), "{row:?}");
}

#[tokio::test]
async fn boot_reconcile_leaves_finished_sessions_alone() {
    let h = Harness::new().await;
    seed(&h, "s-done", "done", true).await;
    seed(&h, "s-failed", "failed", true).await;
    reconcile_on_boot(&h.ctx, &h.clients()).await;
    assert_eq!(h.row("s-done").await.state, "done");
    assert_eq!(h.row("s-failed").await.state, "failed");
    assert!(h.rig.calls().is_empty());
    assert!(h.yt_state.transitions().is_empty());
}

#[tokio::test]
async fn a_reaped_session_counts_its_own_client_spend_once() {
    let h = Harness::new().await;
    let yt = h.yt();
    yt.stream_status("st-e2e").await.unwrap();
    assert_eq!(yt.units_used(), 1);
    let mut row = AvGateSessionRow::new_starting("s1", "r", "t", &now_ts());
    row.state = "processing".to_string();
    row.broadcast_id = Some("bc-1".to_string());
    row.quota_units = 4;
    store::save(&h.ctx.pool, &row).await.unwrap();
    // The row is stale (4); the session's base before this client was 9.
    reap_dead_driver(Arc::clone(&h.ctx), Some(yt), 9, "s1").await;
    assert_eq!(h.row("s1").await.quota_units, 10, "base 9 + the client's 1");
}

#[tokio::test]
async fn a_processing_session_persists_its_quota_spend_as_it_polls() {
    let h = Harness::with(
        FakeRig::default(),
        AvGateTimings {
            processing_timeout: Duration::from_secs(30),
            ..timings()
        },
    )
    .await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "processing".to_string();
    let processing = stop_and_wait(&h, "s1", SessionState::Processing).await;
    let grew = async {
        while h.row("s1").await.quota_units <= processing.quota_units + 2 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), grew)
        .await
        .expect("each VOD poll must persist its unit");
    assert_eq!(h.row("s1").await.state, "processing");
}

#[tokio::test]
async fn an_upload_status_of_processed_also_completes_the_session() {
    let h = Harness::new().await;
    h.ready_session("s1").await;
    *h.yt_state.video.lock().unwrap() = "processing".to_string();
    *h.yt_state.upload.lock().unwrap() = "processed".to_string();
    let row = stop_and_wait(&h, "s1", SessionState::Done).await;
    assert_eq!(row.vod_id.as_deref(), Some("bc-1"));
}
