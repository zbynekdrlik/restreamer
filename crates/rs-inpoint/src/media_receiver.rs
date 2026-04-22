use std::sync::Arc;
use std::time::{Duration, Instant};

use streamhub::define::{
    BroadcastEvent, BroadcastEventReceiver, FrameData, StreamHubEvent, StreamHubEventSender,
    SubDataType, SubscribeType, SubscriberInfo,
};
use streamhub::stream::StreamIdentifier;
use streamhub::utils::{RandomDigitCount, Uuid};
use tracing::{debug, error, info, warn};

use rs_core::models::InpointState;

use crate::flv_chunker::FlvChunkSink;

/// If no frames arrive for this long, assume the stream stalled and re-subscribe.
/// 30s is well above the ~33ms frame interval at 30fps, so this won't trigger
/// during normal streaming.
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for hub subscription response.
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Interval for frame processing heartbeat log.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum re-subscribe retries before giving up on a published stream.
const MAX_RESUBSCRIBE_RETRIES: u32 = 30;

/// How a stream processing session ended.
#[derive(Debug, PartialEq, Eq)]
enum StreamEnd {
    /// Normal: publisher disconnected, frame channel closed.
    ChannelClosed,
    /// Stall: no frames received for FRAME_TIMEOUT -- will re-subscribe.
    Timeout,
    /// Hub rejected subscription or timed out -- will retry.
    SubscriptionFailed,
}

/// Receives media data from the xiu StreamsHub and processes it into FLV chunks.
pub struct MediaReceiver {
    event_rx: BroadcastEventReceiver,
    hub_event_tx: StreamHubEventSender,
    flv_chunk_sink: Arc<FlvChunkSink>,
    inpoint_state: InpointState,
}

impl MediaReceiver {
    pub fn new(
        event_rx: BroadcastEventReceiver,
        hub_event_tx: StreamHubEventSender,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
    ) -> Self {
        Self {
            event_rx,
            hub_event_tx,
            flv_chunk_sink,
            inpoint_state,
        }
    }

    /// Run the media receiver loop, processing published streams until shutdown.
    pub async fn run(mut self) {
        info!("Media receiver started, waiting for RTMP publishers");

        loop {
            match self.event_rx.recv().await {
                Ok(event) => match event {
                    BroadcastEvent::Publish { identifier } => {
                        info!("Stream published: {identifier}");
                        self.inpoint_state.mark_connected().await;
                        // Audit: RtmpConnected.
                        if let Some(tx) = self.inpoint_state.audit_tx() {
                            rs_core::audit::record(
                                tx,
                                rs_core::audit::AuditRow {
                                    severity: rs_core::audit::Severity::Info,
                                    source: rs_core::audit::Source::Inpoint,
                                    event_id: None,
                                    instance_id: None,
                                    endpoint: None,
                                    action: rs_core::audit::Action::RtmpConnected,
                                    detail: serde_json::json!({
                                        "stream_identifier": format!("{identifier}"),
                                    }),
                                    ts_override: None,
                                },
                            );
                        }

                        // Subscribe and process frames with automatic re-subscribe on stall
                        let mut retry_count = 0u32;
                        loop {
                            let result = self.process_stream(&identifier).await;
                            match result {
                                StreamEnd::ChannelClosed => break,
                                StreamEnd::Timeout | StreamEnd::SubscriptionFailed => {
                                    retry_count += 1;
                                    if retry_count >= MAX_RESUBSCRIBE_RETRIES {
                                        error!(
                                            retry = retry_count,
                                            result = ?result,
                                            "Max re-subscribe retries reached, giving up: {identifier}"
                                        );
                                        break;
                                    }
                                    let delay_secs = (retry_count as u64 * 2).min(10);
                                    warn!(
                                        retry = retry_count,
                                        result = ?result,
                                        "Re-subscribing to stream in {delay_secs}s: {identifier}"
                                    );
                                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                                }
                            }
                        }

                        info!("Stream ended: {identifier}");
                        let duration_secs = self.inpoint_state.mark_disconnected().await;
                        // Audit: RtmpDisconnected with session duration.
                        if let Some(tx) = self.inpoint_state.audit_tx() {
                            rs_core::audit::record(
                                tx,
                                rs_core::audit::AuditRow {
                                    severity: rs_core::audit::Severity::Info,
                                    source: rs_core::audit::Source::Inpoint,
                                    event_id: None,
                                    instance_id: None,
                                    endpoint: None,
                                    action: rs_core::audit::Action::RtmpDisconnected,
                                    detail: serde_json::json!({
                                        "stream_identifier": format!("{identifier}"),
                                        "duration_secs": duration_secs,
                                    }),
                                    ts_override: None,
                                },
                            );
                        }
                    }
                    BroadcastEvent::UnPublish { identifier } => {
                        info!("Stream unpublished: {identifier}");
                        let duration_secs = self.inpoint_state.mark_disconnected().await;
                        self.flv_chunk_sink.flush().await;
                        // Audit: also record UnPublish as RtmpDisconnected so
                        // operators see why the ingest dropped.
                        if let Some(tx) = self.inpoint_state.audit_tx() {
                            rs_core::audit::record(
                                tx,
                                rs_core::audit::AuditRow {
                                    severity: rs_core::audit::Severity::Info,
                                    source: rs_core::audit::Source::Inpoint,
                                    event_id: None,
                                    instance_id: None,
                                    endpoint: None,
                                    action: rs_core::audit::Action::RtmpDisconnected,
                                    detail: serde_json::json!({
                                        "stream_identifier": format!("{identifier}"),
                                        "duration_secs": duration_secs,
                                        "reason": "unpublish",
                                    }),
                                    ts_override: None,
                                },
                            );
                        }
                    }
                    BroadcastEvent::Subscribe { identifier, .. } => {
                        debug!("New subscriber for stream: {identifier}");
                    }
                    BroadcastEvent::UnSubscribe { .. } => {
                        debug!("Subscriber disconnected");
                    }
                },
                Err(e) => {
                    error!("Broadcast event channel closed: {e}");
                    break;
                }
            }
        }

        info!("Media receiver stopped");
    }

    /// Subscribe to a published stream and process its frames until it ends.
    ///
    /// Returns how the stream ended:
    /// - `ChannelClosed`: normal end (publisher disconnected)
    /// - `Timeout`: no frames for 30s (stall detected, should re-subscribe)
    /// - `SubscriptionFailed`: hub rejected or timed out
    async fn process_stream(&self, identifier: &StreamIdentifier) -> StreamEnd {
        // Create subscriber info for the hub
        let sub_id = Uuid::new(RandomDigitCount::Six);
        let sub_info = SubscriberInfo {
            id: sub_id,
            sub_type: SubscribeType::RtmpPull,
            notify_info: streamhub::define::NotifyInfo {
                request_url: String::new(),
                remote_addr: String::from("local-chunker"),
            },
            sub_data_type: SubDataType::Frame,
        };

        // Send subscribe request to the hub via oneshot channel
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        if self
            .hub_event_tx
            .send(StreamHubEvent::Subscribe {
                identifier: identifier.clone(),
                info: sub_info,
                result_sender: result_tx,
            })
            .is_err()
        {
            warn!("Failed to send subscribe request to hub");
            return StreamEnd::SubscriptionFailed;
        }

        // Wait for subscription result with timeout
        let sub_result = match tokio::time::timeout(SUBSCRIPTION_TIMEOUT, result_rx).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(e))) => {
                warn!("Hub rejected subscription: {e}");
                return StreamEnd::SubscriptionFailed;
            }
            Ok(Err(_)) => {
                warn!("Hub subscription channel dropped");
                return StreamEnd::SubscriptionFailed;
            }
            Err(_) => {
                warn!(
                    "Subscription timeout after {}s",
                    SUBSCRIPTION_TIMEOUT.as_secs()
                );
                return StreamEnd::SubscriptionFailed;
            }
        };

        // Get the frame receiver
        let mut frame_rx = match sub_result.0.frame_receiver {
            Some(rx) => rx,
            None => {
                warn!("No frame receiver in subscription result");
                return StreamEnd::SubscriptionFailed;
            }
        };

        info!("Subscribed to stream, processing frames");

        // Heartbeat tracking
        let mut frames_since_heartbeat = 0u64;
        let mut total_frames = 0u64;
        let mut last_heartbeat = Instant::now();

        // Process frames with timeout -- detect stalls and recover
        loop {
            match tokio::time::timeout(FRAME_TIMEOUT, frame_rx.recv()).await {
                Ok(Some(frame)) => {
                    total_frames += 1;
                    frames_since_heartbeat += 1;

                    // Periodic heartbeat
                    if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                        info!(
                            frames_last_60s = frames_since_heartbeat,
                            total_frames, "Frame processing heartbeat"
                        );
                        frames_since_heartbeat = 0;
                        last_heartbeat = Instant::now();
                    }

                    match frame {
                        FrameData::Video { timestamp, data } => {
                            self.flv_chunk_sink.write_video(timestamp, &data).await;
                        }
                        FrameData::Audio { timestamp, data } => {
                            self.flv_chunk_sink.write_audio(timestamp, &data).await;
                        }
                        FrameData::MediaInfo { .. } => {
                            debug!("Received media info");
                        }
                        FrameData::MetaData { .. } => {
                            debug!("Received metadata");
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed -- publisher disconnected normally
                    info!(
                        total_frames,
                        "Frame channel closed, flushing remaining data"
                    );
                    self.flv_chunk_sink.flush().await;
                    return StreamEnd::ChannelClosed;
                }
                Err(_) => {
                    // Timeout -- no frames for FRAME_TIMEOUT
                    error!(
                        total_frames,
                        timeout_secs = FRAME_TIMEOUT.as_secs(),
                        "No frames received -- stream stalled, will re-subscribe"
                    );
                    self.flv_chunk_sink.flush().await;
                    return StreamEnd::Timeout;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
