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
                        // Re-anchor audio+video onto a fresh shared 0-based
                        // session epoch on every (re)publish. The xiu server
                        // stays up across an OBS mid-stream restart, so Publish
                        // is the reliable per-session boundary; without this the
                        // video wall-clock keeps counting the dead-air gap while
                        // audio xiu restarts ~0, baking A/V skew into the chunks
                        // (#255 — event 9315 had ~25.5s audio-behind-video).
                        self.flv_chunk_sink.start_new_session().await;
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
#[path = "media_receiver_tests.rs"]
mod tests;
