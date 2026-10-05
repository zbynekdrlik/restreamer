//! Tests for `media_receiver.rs`. Loaded via `#[cfg(test)] #[path = "media_receiver_tests.rs"] mod tests;`
//! to keep the production file under the 1000-line CI gate.

use super::*;
use std::time::Duration;
use streamhub::define::DataReceiver;

/// Helper: create a MediaReceiver with controlled mock channels.
/// Returns (MediaReceiver, hub_event_rx) so tests can intercept Subscribe events
/// and respond with a controlled frame channel.
fn create_test_receiver() -> (
    MediaReceiver,
    tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
    tokio::sync::broadcast::Sender<BroadcastEvent>,
) {
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let flv_sink = Arc::new(FlvChunkSink::new_null());
    let state = InpointState::new();

    let receiver = MediaReceiver::new(event_rx, hub_tx, flv_sink, state);

    (receiver, hub_rx, event_tx)
}

/// Helper: spawn a mock hub that responds to Subscribe events with a given
/// frame sender. Returns the frame_tx for the test to control.
fn spawn_mock_hub(
    mut hub_rx: tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
) -> tokio::sync::mpsc::UnboundedSender<FrameData> {
    let (frame_tx, frame_rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(event) = hub_rx.recv().await {
            if let StreamHubEvent::Subscribe { result_sender, .. } = event {
                let data_receiver = DataReceiver {
                    frame_receiver: Some(frame_rx),
                    packet_receiver: None,
                };
                let _ = result_sender.send(Ok((data_receiver, None)));
                // Only handle one subscription per mock hub
                return;
            }
        }
    });

    frame_tx
}

#[tokio::test]
async fn frame_timeout_returns_after_stall() {
    // Simulate: frames flow, then stop (stall). process_stream should return
    // StreamEnd::Timeout within FRAME_TIMEOUT, not hang forever.
    tokio::time::pause();

    let (receiver, hub_rx, _event_tx) = create_test_receiver();
    let frame_tx = spawn_mock_hub(hub_rx);

    let identifier = StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    };

    // Send a few frames
    let video_frame = FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    };
    frame_tx.send(video_frame).unwrap();

    // Now don't send more frames. The timeout should fire.
    // We keep frame_tx alive (don't drop it) -- this simulates xiu holding
    // the channel open but not sending.

    // Call process_stream -- it should return Timeout after FRAME_TIMEOUT
    let result = receiver.process_stream(&identifier).await;

    // It consumed the one frame, then waited for FRAME_TIMEOUT with no more frames
    assert_eq!(result, StreamEnd::Timeout);
}

#[tokio::test]
async fn frame_channel_close_returns_channel_closed() {
    // When the frame channel closes (publisher disconnect), process_stream
    // should return ChannelClosed.
    tokio::time::pause();

    let (receiver, hub_rx, _event_tx) = create_test_receiver();
    let frame_tx = spawn_mock_hub(hub_rx);

    let identifier = StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    };

    // Drop the frame_tx immediately -- channel closes
    drop(frame_tx);

    let result = receiver.process_stream(&identifier).await;

    assert_eq!(result, StreamEnd::ChannelClosed);
}

#[tokio::test]
async fn subscription_timeout_returns_failed() {
    // When the hub never responds to Subscribe, process_stream should return
    // SubscriptionFailed after SUBSCRIPTION_TIMEOUT.
    tokio::time::pause();

    let (receiver, _hub_rx, _event_tx) = create_test_receiver();
    // Don't spawn mock hub -- nobody responds to Subscribe

    let identifier = StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    };

    let result = receiver.process_stream(&identifier).await;

    assert_eq!(result, StreamEnd::SubscriptionFailed);
}

#[tokio::test]
async fn frame_timeout_flushes_flv_chunk() {
    // When timeout fires, any buffered FLV data should be flushed.
    tokio::time::pause();

    let (_event_tx_b, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();

    let dir = tempfile::tempdir().unwrap();
    let flv_sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60), // long duration -- won't auto-flush
    ));
    let state = InpointState::new();

    let receiver = MediaReceiver::new(event_rx, hub_tx, flv_sink.clone(), state);

    let frame_tx = spawn_mock_hub(hub_rx);

    let identifier = StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    };

    // Send sequence header then keyframe to start a chunk
    let seq_header = FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]),
    };
    frame_tx.send(seq_header).unwrap();

    let keyframe = FrameData::Video {
        timestamp: 100,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD][..]),
    };
    frame_tx.send(keyframe).unwrap();

    // Don't send more -- stall
    // process_stream should timeout and flush the partial chunk

    // Yield so the mock hub can process
    tokio::task::yield_now().await;

    let result = receiver.process_stream(&identifier).await;

    assert_eq!(result, StreamEnd::Timeout);

    // The partial FLV chunk should have been flushed
    assert!(
        flv_sink.chunk_count().await > 0,
        "FLV chunk sink should have flushed partial data on timeout"
    );
}

// ---------------------------------------------------------------------------
// #367 event-driven receiver: a programmable hub that answers EVERY Subscribe
// from a "current publisher" slot (accept), or rejects when no publisher is
// up. That is the real xiu hub's behaviour across a publisher restart.
// ---------------------------------------------------------------------------

/// One Subscribe request the programmable hub answered.
#[derive(Debug, Clone, Copy)]
struct SubRecord {
    at: tokio::time::Instant,
    accepted: bool,
}

type PublisherSlot = Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<FrameData>>>>;

/// Spawn a hub that serves every Subscribe from `slot` (taking the current
/// publisher's frame receiver), or rejects with `NoAppOrStreamName` when the
/// slot is empty, and logs each answer to the returned channel.
fn spawn_programmable_hub(
    mut hub_rx: tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
) -> (
    PublisherSlot,
    tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
) {
    let slot: PublisherSlot = Arc::new(std::sync::Mutex::new(None));
    let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
    let hub_slot = Arc::clone(&slot);
    tokio::spawn(async move {
        while let Some(event) = hub_rx.recv().await {
            if let StreamHubEvent::Subscribe { result_sender, .. } = event {
                let frames = hub_slot.lock().unwrap().take();
                let accepted = frames.is_some();
                let reply = match frames {
                    Some(rx) => Ok((
                        DataReceiver {
                            frame_receiver: Some(rx),
                            packet_receiver: None,
                        },
                        None,
                    )),
                    None => Err(streamhub::errors::StreamHubError {
                        value: streamhub::errors::StreamHubErrorValue::NoAppOrStreamName,
                    }),
                };
                let _ = result_sender.send(reply);
                let _ = log_tx.send(SubRecord {
                    at: tokio::time::Instant::now(),
                    accepted,
                });
            }
        }
    });
    (slot, log_rx)
}

/// A publisher (re)connects. The hub registers its stream, THEN broadcasts
/// Publish (the streamhub `publish()` order). Returns the publisher's frame
/// sender; dropping it is the publisher disconnecting.
fn publish(
    slot: &PublisherSlot,
    event_tx: &tokio::sync::broadcast::Sender<BroadcastEvent>,
    identifier: &StreamIdentifier,
) -> tokio::sync::mpsc::UnboundedSender<FrameData> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx);
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: identifier.clone(),
        })
        .expect("receiver subscribed to broadcast events");
    tx
}

async fn next_accepted(
    log_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
    within: Duration,
) -> Option<SubRecord> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, log_rx.recv()).await {
            Ok(Some(rec)) if rec.accepted => return Some(rec),
            Ok(Some(_rejected)) => continue,
            _ => return None,
        }
    }
}

fn test_identifier() -> StreamIdentifier {
    StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    }
}

/// Design test 2 (#367): the Thursday lost-event chain. A Publish (OBS
/// reconnecting) arrives while the receiver sits on a STALLED subscription.
/// The pre-fix receiver never read `event_rx` there, so that Publish stayed
/// queued and was consumed later as a STALE event. The receiver then stayed
/// one event behind for good, and every later real republish was joined
/// 0-8 s late via the retry ladder (GOP-cache replay, baked-in offset).
/// The next republish must be subscribed within 100 ms and must re-anchor
/// the chunker session.
#[tokio::test(start_paused = true)]
async fn publish_during_stall_is_not_lost_and_next_republish_joins_immediately() {
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let state = InpointState::new();
    // start_new_session() clears the ingest-skew latch, which makes the
    // re-anchor observable from outside the receiver.
    let sink = Arc::new(FlvChunkSink::new_null().with_ingest_state(state.clone(), 2_000));
    let receiver = MediaReceiver::new(event_rx, hub_tx, sink, state.clone());
    let (slot, mut log_rx) = spawn_programmable_hub(hub_rx);
    let id = test_identifier();
    let _run = tokio::spawn(receiver.run());

    // P1: first publisher, one frame, then it stalls (process freeze).
    let tx1 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("first publish must be subscribed");
    tx1.send(FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    })
    .unwrap();

    // P2 arrives 10 s into the stall: OBS reconnected on a new connection.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let tx2 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(60))
        .await
        .expect("the reconnected publisher must be subscribed");

    // P2's publisher goes away (the operator closes OBS).
    drop(tx2);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // P3: the real fresh publish. Raise the skew latch first so that the
    // re-anchor (start_new_session clears it) is observable.
    state.set_ingest_skew_active(true);
    let _tx3 = publish(&slot, &event_tx, &id);
    let published_at = tokio::time::Instant::now();
    let sub = next_accepted(&mut log_rx, Duration::from_secs(30))
        .await
        .expect("the fresh publish must eventually be subscribed");
    let lag = sub.at.duration_since(published_at);
    assert!(
        lag <= Duration::from_millis(100),
        "the fresh publish must be subscribed within 100 ms, got {lag:?}; a late join \
         replays the GOP cache and bakes an A/V offset in (#367)"
    );
    tokio::task::yield_now().await;
    assert!(
        !state.ingest_skew_active(),
        "the fresh publish must re-anchor the chunker session (start_new_session)"
    );
    drop(tx1);
}

/// Design test 3 (#367): a `RecvError::Lagged` on the hub's broadcast
/// channel used to `break` `run()`. The RTMP server reads that as a clean
/// stop, so ingest silently died until a process restart. A lag must be
/// logged and survived.
#[tokio::test(start_paused = true)]
async fn lagged_broadcast_does_not_end_run() {
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        InpointState::new(),
    );
    let (slot, mut log_rx) = spawn_programmable_hub(hub_rx);
    let id = test_identifier();
    let run = tokio::spawn(receiver.run());

    // Overflow the 16-slot broadcast ring before the receiver reads once.
    for i in 0..40 {
        event_tx
            .send(BroadcastEvent::UnSubscribe {
                id: format!("noise-{i}"),
                result_sender: None,
            })
            .unwrap();
    }
    let _tx = publish(&slot, &event_tx, &id);

    let sub = next_accepted(&mut log_rx, Duration::from_secs(5)).await;
    assert!(
        sub.is_some(),
        "after a Lagged broadcast the receiver must keep running and subscribe to the Publish"
    );
    assert!(!run.is_finished(), "a Lagged broadcast must not end run()");
}
