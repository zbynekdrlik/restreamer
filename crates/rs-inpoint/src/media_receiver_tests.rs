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
