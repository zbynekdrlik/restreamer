//! #368 ingest gap metric, driven through `MediaReceiver::run()`: frames
//! that stop arriving and video timestamps that jump are counted in the
//! shared `InpointState` and written as `IngestFrameGap` rows. Child of
//! `media_receiver_tests.rs` (`#[path]`): the harness lives there.

use super::*;
use std::time::Duration;

use rs_core::audit::{Action, AuditRow};

/// Video ts of frame `k` at 30 fps.
fn ts30(k: u32) -> u32 {
    (f64::from(k) * 1_000.0 / 30.0).round() as u32
}

/// Next audit row of `action`, skipping others (RtmpConnected, ...).
async fn next_row(
    rx: &mut tokio::sync::mpsc::Receiver<AuditRow>,
    action: Action,
) -> Option<AuditRow> {
    loop {
        let row = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .ok()??;
        if row.action == action {
            return Some(row);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn an_ingest_stall_and_obs_dropped_frames_are_counted_and_audited() {
    let _wd = watchdog("an_ingest_stall_and_obs_dropped_frames_are_counted_and_audited");
    let (audit_tx, mut audit_rx) = tokio::sync::mpsc::channel::<AuditRow>(64);
    let state = InpointState::new().with_audit_tx(audit_tx);
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");

    // 3 s of a healthy 30 fps stream: nothing counted.
    for k in 0..90 {
        tx.send(a_frame(ts30(k))).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    assert_eq!(state.ingest_gaps().snapshot().arrival_gaps, 0);
    assert_eq!(state.ingest_gaps().snapshot().source_ts_jumps, 0);
    let interval = state.ingest_gaps().snapshot().frame_interval_us;
    assert!((33_300..=33_370).contains(&interval), "{interval}");

    // OBS's send stalls for ~500 ms; it then drops 12 frames from its queue
    // and resumes with frame 101.
    tokio::time::sleep(Duration::from_millis(500)).await;
    tx.send(a_frame(ts30(101))).unwrap();
    tokio::time::sleep(Duration::from_millis(33)).await;

    let gaps = state.ingest_gaps().snapshot();
    assert_eq!(gaps.arrival_gaps, 1);
    assert!(gaps.last_arrival_gap_ms >= 500, "{gaps:?}");
    assert_eq!(gaps.source_ts_jumps, 1);
    assert_eq!(gaps.dropped_frames, 11, "frames 90..=100 never arrived");
    assert!(gaps.last_jump_at_ms > 0);

    let arrival = next_row(&mut audit_rx, Action::IngestFrameGap)
        .await
        .expect("an IngestFrameGap row for the arrival gap");
    assert_eq!(arrival.detail["kind"], "arrival_gap");
    assert_eq!(
        arrival.detail["stream_identifier"],
        format!("{}", test_identifier())
    );
    let jump = next_row(&mut audit_rx, Action::IngestFrameGap)
        .await
        .expect("an IngestFrameGap row for the source-ts jump");
    assert_eq!(jump.detail["kind"], "source_ts_jump");
    assert_eq!(jump.detail["dropped_frames"], 11);
    assert_eq!(jump.detail["from_ts"], ts30(89));
    assert_eq!(jump.detail["to_ts"], ts30(101));
    drop(tx);
}

/// The wait for a (re)subscription is not a frame gap: the gap clock and the
/// frame-interval estimate start over with every subscription.
#[tokio::test(start_paused = true)]
async fn a_resubscription_starts_the_gap_measurement_over() {
    let _wd = watchdog("a_resubscription_starts_the_gap_measurement_over");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let id = test_identifier();
    let tx = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    for k in 0..30 {
        tx.send(a_frame(ts30(k))).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    drop(tx);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The publisher comes back 2 s later with a new timeline.
    let tx = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("republish must be subscribed");
    for k in 0..30 {
        tx.send(a_frame(ts30(k) + 90_000)).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    let gaps = state.ingest_gaps().snapshot();
    assert_eq!(gaps.arrival_gaps, 0, "{gaps:?}");
    assert_eq!(gaps.source_ts_jumps, 0, "{gaps:?}");
    drop(tx);
}
