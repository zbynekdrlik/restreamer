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
        // 5 s ahead: without the reset this would be a counted jump.
        tx.send(a_frame(ts30(k) + 5_000)).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    let gaps = state.ingest_gaps().snapshot();
    assert_eq!(gaps.arrival_gaps, 0, "{gaps:?}");
    assert_eq!(gaps.source_ts_jumps, 0, "{gaps:?}");
    drop(tx);
}

/// The 30 s frame stall and the re-subscription that follows are ONE
/// arrival gap of the same session, the biggest dropout there is (#368
/// review): the re-subscription forgets the video timeline, never the
/// arrival clock.
#[tokio::test(start_paused = true)]
async fn a_frame_stall_and_its_resubscription_are_one_measured_gap() {
    let _wd = watchdog("a_frame_stall_and_its_resubscription_are_one_measured_gap");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    for k in 0..30 {
        tx.send(a_frame(ts30(k))).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    // The publisher stays registered but sends nothing: FRAME_TIMEOUT, then
    // the receiver re-subscribes and gets a fresh frame channel.
    let (tx_again, rx_again) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_again);
    next_accepted(&mut log_rx, FRAME_TIMEOUT + Duration::from_secs(5))
        .await
        .expect("the stalled subscription must be re-subscribed");
    // The publisher went on sending while nobody was subscribed: its next
    // frames are 2 s ahead. Those frames were lost on OUR side, not dropped
    // by OBS, so the re-subscription starts the video timeline over: no
    // jump. The arrival gap is counted.
    for k in 0..10 {
        tx_again.send(a_frame(ts30(k + 90))).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    let gaps = state.ingest_gaps().snapshot();
    assert_eq!(gaps.arrival_gaps, 1, "{gaps:?}");
    assert!(
        gaps.last_arrival_gap_ms >= FRAME_TIMEOUT.as_millis() as u64,
        "{gaps:?}"
    );
    assert_eq!(gaps.source_ts_jumps, 0, "{gaps:?}");
    drop(tx);
}

/// A replayed AVC sequence header (ts 0) is an arrival, not a video step.
#[tokio::test(start_paused = true)]
async fn a_sequence_header_is_no_video_step() {
    let _wd = watchdog("a_sequence_header_is_no_video_step");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    for k in 0..60 {
        tx.send(a_frame(ts30(k) + 20_000)).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    let header = FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01][..]),
    };
    let before = state.ingest_gaps().snapshot().frame_interval_us;
    tx.send(header).unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // As a video step, ts 0 would be a backward discontinuity, which drops
    // the frame-interval estimate.
    assert_eq!(state.ingest_gaps().snapshot().frame_interval_us, before);
    assert!(before > 0);
    // The next frame 20 s "after" the header would be a counted jump if the
    // header were a step; it is the next normal frame instead.
    for k in 60..90 {
        tx.send(a_frame(ts30(k) + 20_000)).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    let gaps = state.ingest_gaps().snapshot();
    assert_eq!(gaps.source_ts_jumps, 0, "{gaps:?}");
    assert!(
        gaps.frame_interval_us > 0,
        "the estimate survived: {gaps:?}"
    );
    drop(tx);
}

/// The status no longer shows the previous stream's frame interval once it
/// ended.
#[tokio::test(start_paused = true)]
async fn the_frame_interval_is_cleared_when_the_stream_ends() {
    let _wd = watchdog("the_frame_interval_is_cleared_when_the_stream_ends");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    for k in 0..30 {
        tx.send(a_frame(ts30(k))).unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
    }
    assert!(state.ingest_gaps().snapshot().frame_interval_us > 0);
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(state.ingest_gaps().snapshot().frame_interval_us, 0);
}
