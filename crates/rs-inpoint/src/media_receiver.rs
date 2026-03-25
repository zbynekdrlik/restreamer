use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use streamhub::define::{
    BroadcastEvent, BroadcastEventReceiver, FrameData, StreamHubEvent, StreamHubEventSender,
    SubDataType, SubscribeType, SubscriberInfo,
};
use streamhub::stream::StreamIdentifier;
use streamhub::utils::{RandomDigitCount, Uuid};
use tracing::{debug, error, info, warn};
use xflv::demuxer::{FlvAudioTagDemuxer, FlvVideoTagDemuxer};

use rs_core::models::InpointState;

use crate::chunker::ChunkSink;
use crate::flv_chunker::FlvChunkSink;
use crate::muxer::TsMuxer;

/// If no frames arrive for this long, assume the stream stalled and re-subscribe.
/// 30s is well above the ~33ms frame interval at 30fps, so this won't trigger
/// during normal streaming.
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for hub subscription response.
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Interval for frame processing heartbeat log.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// How a stream processing session ended.
#[derive(Debug, PartialEq, Eq)]
enum StreamEnd {
    /// Normal: publisher disconnected, frame channel closed.
    ChannelClosed,
    /// Stall: no frames received for FRAME_TIMEOUT — will re-subscribe.
    Timeout,
    /// Hub rejected subscription or timed out — will retry.
    SubscriptionFailed,
}

/// Chunk format determines how media data is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkFormat {
    /// Legacy: demux FLV → mux MPEG-TS → chunk files.
    Ts,
    /// Direct: wrap raw FLV tag bodies in FLV envelope → chunk files.
    /// Zero format conversion overhead — YouTube receives exactly what OBS sends.
    Flv,
}

impl ChunkFormat {
    pub fn from_config(s: &str) -> Self {
        match s {
            "ts" => Self::Ts,
            _ => Self::Flv,
        }
    }
}

/// Receives media data from the xiu StreamsHub and processes it into chunks.
///
/// Supports two modes:
/// - **TS mode**: Demuxes FLV → muxes to MPEG-TS → ChunkSink (legacy)
/// - **FLV mode**: Wraps raw FLV tag data directly → FlvChunkSink (zero overhead)
pub struct MediaReceiver {
    event_rx: BroadcastEventReceiver,
    hub_event_tx: StreamHubEventSender,
    chunk_sink: Arc<ChunkSink>,
    flv_chunk_sink: Arc<FlvChunkSink>,
    inpoint_state: InpointState,
    chunk_format: ChunkFormat,
}

impl MediaReceiver {
    pub fn new(
        event_rx: BroadcastEventReceiver,
        hub_event_tx: StreamHubEventSender,
        chunk_sink: Arc<ChunkSink>,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
        chunk_format: ChunkFormat,
    ) -> Self {
        Self {
            event_rx,
            hub_event_tx,
            chunk_sink,
            flv_chunk_sink,
            inpoint_state,
            chunk_format,
        }
    }

    /// Run the media receiver loop, processing published streams until shutdown.
    pub async fn run(mut self) {
        info!(
            chunk_format = ?self.chunk_format,
            "Media receiver started, waiting for RTMP publishers"
        );

        let mut frame_processor = match FrameProcessor::new(self.chunk_sink.clone()) {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to create frame processor: {e}");
                return;
            }
        };

        loop {
            match self.event_rx.recv().await {
                Ok(event) => match event {
                    BroadcastEvent::Publish { identifier } => {
                        info!("Stream published: {identifier}");
                        self.inpoint_state.set_connected(true);

                        // Subscribe and process frames with automatic re-subscribe on stall
                        let mut retry_count = 0u32;
                        loop {
                            let result =
                                self.process_stream(&identifier, &mut frame_processor).await;
                            match result {
                                StreamEnd::ChannelClosed => break,
                                StreamEnd::Timeout | StreamEnd::SubscriptionFailed => {
                                    retry_count += 1;
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
                        self.inpoint_state.set_connected(false);
                    }
                    BroadcastEvent::UnPublish { identifier } => {
                        info!("Stream unpublished: {identifier}");
                        self.inpoint_state.set_connected(false);
                        match self.chunk_format {
                            ChunkFormat::Flv => self.flv_chunk_sink.flush().await,
                            ChunkFormat::Ts => {
                                frame_processor.reset().await.ok();
                            }
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
    async fn process_stream(
        &self,
        identifier: &StreamIdentifier,
        processor: &mut FrameProcessor,
    ) -> StreamEnd {
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

        // Process frames with timeout — detect stalls and recover
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
                        FrameData::Video { timestamp, data } => match self.chunk_format {
                            ChunkFormat::Flv => {
                                self.flv_chunk_sink.write_video(timestamp, &data).await;
                            }
                            ChunkFormat::Ts => {
                                processor.process_video(timestamp, data).await;
                            }
                        },
                        FrameData::Audio { timestamp, data } => match self.chunk_format {
                            ChunkFormat::Flv => {
                                self.flv_chunk_sink.write_audio(timestamp, &data).await;
                            }
                            ChunkFormat::Ts => {
                                processor.process_audio(timestamp, data).await;
                            }
                        },
                        FrameData::MediaInfo { .. } => {
                            debug!("Received media info");
                        }
                        FrameData::MetaData { .. } => {
                            debug!("Received metadata");
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed — publisher disconnected normally
                    info!(
                        total_frames,
                        "Frame channel closed, flushing remaining data"
                    );
                    self.flush_current(processor).await;
                    return StreamEnd::ChannelClosed;
                }
                Err(_) => {
                    // Timeout — no frames for FRAME_TIMEOUT
                    error!(
                        total_frames,
                        timeout_secs = FRAME_TIMEOUT.as_secs(),
                        "No frames received — stream stalled, will re-subscribe"
                    );
                    self.flush_current(processor).await;
                    return StreamEnd::Timeout;
                }
            }
        }
    }

    /// Flush the active chunk sink based on current format.
    async fn flush_current(&self, processor: &mut FrameProcessor) {
        match self.chunk_format {
            ChunkFormat::Flv => self.flv_chunk_sink.flush().await,
            ChunkFormat::Ts => {
                processor.reset().await.ok();
            }
        }
    }
}

/// Process a stream of FLV-formatted audio/video frames into MPEG-TS chunks.
///
/// This is the core processing pipeline that runs for each active RTMP publish session.
/// It demuxes FLV data (from RTMP) into H.264/AAC elementary streams, then muxes them
/// into proper MPEG-TS for the chunker.
pub struct FrameProcessor {
    video_demuxer: FlvVideoTagDemuxer,
    audio_demuxer: FlvAudioTagDemuxer,
    ts_muxer: TsMuxer,
    chunk_sink: Arc<ChunkSink>,
    received_bytes: u64,
}

impl FrameProcessor {
    pub fn new(chunk_sink: Arc<ChunkSink>) -> Result<Self, crate::InpointError> {
        let mut ts_muxer = TsMuxer::new();
        ts_muxer.init_streams()?;
        Ok(Self {
            video_demuxer: FlvVideoTagDemuxer::new(),
            audio_demuxer: FlvAudioTagDemuxer::new(),
            ts_muxer,
            chunk_sink,
            received_bytes: 0,
        })
    }

    /// Process a video frame from FLV-formatted data.
    pub async fn process_video(&mut self, timestamp: u32, data: BytesMut) {
        self.received_bytes += data.len() as u64;

        match self.video_demuxer.demux(timestamp, data) {
            Ok(Some(video_data)) => {
                let is_keyframe = video_data.frame_type == 1;
                if let Err(e) = self.ts_muxer.write_video(
                    video_data.pts,
                    video_data.dts,
                    is_keyframe,
                    video_data.data,
                ) {
                    warn!("Failed to mux video frame: {e}");
                    return;
                }
                let ts_output = self.ts_muxer.get_data();
                if !ts_output.is_empty() {
                    self.chunk_sink.write_data(&ts_output).await;
                }
            }
            Ok(None) => {
                // Sequence header (codec config) processed internally by demuxer
                debug!("Video sequence header processed");
            }
            Err(e) => {
                warn!("Video demux error: {e}");
            }
        }
    }

    /// Process an audio frame from FLV-formatted data.
    pub async fn process_audio(&mut self, timestamp: u32, data: BytesMut) {
        self.received_bytes += data.len() as u64;

        match self.audio_demuxer.demux(timestamp, data) {
            Ok(audio_data) => {
                if !audio_data.has_data {
                    debug!("Audio sequence header processed");
                    return;
                }
                if let Err(e) =
                    self.ts_muxer
                        .write_audio(audio_data.pts, audio_data.dts, audio_data.data)
                {
                    warn!("Failed to mux audio frame: {e}");
                    return;
                }
                let ts_output = self.ts_muxer.get_data();
                if !ts_output.is_empty() {
                    self.chunk_sink.write_data(&ts_output).await;
                }
            }
            Err(e) => {
                warn!("Audio demux error: {e}");
            }
        }
    }

    /// Get total received bytes count.
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes
    }

    /// Flush and reset the processor for a new stream.
    pub async fn reset(&mut self) -> Result<(), crate::InpointError> {
        self.chunk_sink.flush().await;
        self.ts_muxer.reset();
        self.ts_muxer.init_streams()?;
        self.video_demuxer = FlvVideoTagDemuxer::new();
        self.audio_demuxer = FlvAudioTagDemuxer::new();
        self.received_bytes = 0;
        Ok(())
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
        let chunk_sink = Arc::new(ChunkSink::new_null());
        let flv_sink = Arc::new(FlvChunkSink::new_null());
        let state = InpointState::new();

        let receiver = MediaReceiver::new(
            event_rx,
            hub_tx,
            chunk_sink,
            flv_sink,
            state,
            ChunkFormat::Flv,
        );

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

        let (receiver, hub_rx, event_tx) = create_test_receiver();
        let frame_tx = spawn_mock_hub(hub_rx);

        let chunk_sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(chunk_sink).unwrap();

        let identifier = StreamIdentifier::Rtmp {
            app_name: "live".to_string(),
            stream_name: "test".to_string(),
        };

        // Send a Publish event to trigger process_stream via run()
        // But we'll call process_stream directly for unit testing.

        // Send a few frames
        let video_frame = FrameData::Video {
            timestamp: 0,
            data: BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
        };
        frame_tx.send(video_frame).unwrap();

        // Now drop the frame_tx sender — but DON'T close it (simulate stall by
        // keeping it alive but never sending). To simulate stall, we just don't
        // send more frames. The timeout should fire.
        // We keep frame_tx alive (don't drop it) — this simulates xiu holding
        // the channel open but not sending.

        // Call process_stream — it should return Timeout after FRAME_TIMEOUT
        let result = receiver.process_stream(&identifier, &mut processor).await;

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

        let chunk_sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(chunk_sink).unwrap();

        let identifier = StreamIdentifier::Rtmp {
            app_name: "live".to_string(),
            stream_name: "test".to_string(),
        };

        // Drop the frame_tx immediately — channel closes
        drop(frame_tx);

        let result = receiver.process_stream(&identifier, &mut processor).await;

        assert_eq!(result, StreamEnd::ChannelClosed);
    }

    #[tokio::test]
    async fn subscription_timeout_returns_failed() {
        // When the hub never responds to Subscribe, process_stream should return
        // SubscriptionFailed after SUBSCRIPTION_TIMEOUT.
        tokio::time::pause();

        let (receiver, _hub_rx, _event_tx) = create_test_receiver();
        // Don't spawn mock hub — nobody responds to Subscribe

        let chunk_sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(chunk_sink).unwrap();

        let identifier = StreamIdentifier::Rtmp {
            app_name: "live".to_string(),
            stream_name: "test".to_string(),
        };

        let result = receiver.process_stream(&identifier, &mut processor).await;

        assert_eq!(result, StreamEnd::SubscriptionFailed);
    }

    #[tokio::test]
    async fn frame_timeout_flushes_flv_chunk() {
        // When timeout fires, any buffered FLV data should be flushed.
        tokio::time::pause();

        let (event_tx_b, event_rx) = tokio::sync::broadcast::channel(16);
        let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();

        let dir = tempfile::tempdir().unwrap();
        let chunk_sink = Arc::new(ChunkSink::new_null());
        let flv_sink = Arc::new(FlvChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_secs(60), // long duration — won't auto-flush
        ));
        let state = InpointState::new();

        let receiver = MediaReceiver::new(
            event_rx,
            hub_tx,
            chunk_sink.clone(),
            flv_sink.clone(),
            state,
            ChunkFormat::Flv,
        );

        let frame_tx = spawn_mock_hub(hub_rx);

        let mut processor = FrameProcessor::new(chunk_sink).unwrap();

        let identifier = StreamIdentifier::Rtmp {
            app_name: "live".to_string(),
            stream_name: "test".to_string(),
        };

        // Send sequence header then keyframe to start a chunk
        let seq_header = FrameData::Video {
            timestamp: 0,
            data: BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]),
        };
        frame_tx.send(seq_header).unwrap();

        let keyframe = FrameData::Video {
            timestamp: 100,
            data: BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD][..]),
        };
        frame_tx.send(keyframe).unwrap();

        // Don't send more — stall
        // process_stream should timeout and flush the partial chunk

        // Yield so the mock hub can process
        tokio::task::yield_now().await;

        let result = receiver.process_stream(&identifier, &mut processor).await;

        assert_eq!(result, StreamEnd::Timeout);

        // The partial FLV chunk should have been flushed
        assert!(
            flv_sink.chunk_count().await > 0,
            "FLV chunk sink should have flushed partial data on timeout"
        );
    }

    #[tokio::test]
    async fn frame_processor_creates_successfully() {
        let sink = Arc::new(ChunkSink::new_null());
        let processor = FrameProcessor::new(sink);
        assert!(processor.is_ok());
    }

    #[tokio::test]
    async fn frame_processor_handles_invalid_video() {
        let sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(sink).unwrap();

        // Invalid FLV video data should not panic
        let data = BytesMut::from(&[0xFF, 0xFF, 0xFF][..]);
        processor.process_video(0, data).await;
        // Should still track bytes even on demux error
        assert_eq!(processor.received_bytes(), 3);
    }

    #[tokio::test]
    async fn frame_processor_handles_invalid_audio() {
        let sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(sink).unwrap();

        // Invalid FLV audio data should not panic
        let data = BytesMut::from(&[0xFF, 0xFF][..]);
        processor.process_audio(0, data).await;
        assert_eq!(processor.received_bytes(), 2);
    }

    #[tokio::test]
    async fn frame_processor_reset_clears_state() {
        let sink = Arc::new(ChunkSink::new_null());
        let mut processor = FrameProcessor::new(sink).unwrap();

        let data = BytesMut::from(&[0xAA; 100][..]);
        processor.process_video(0, data).await;
        assert!(processor.received_bytes() > 0);

        processor.reset().await.unwrap();
        assert_eq!(processor.received_bytes(), 0);
    }

    #[tokio::test]
    async fn frame_processor_video_sequence_header() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(ChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_secs(60),
        ));
        let mut processor = FrameProcessor::new(sink.clone()).unwrap();

        // Construct a minimal FLV video sequence header:
        // byte 0: frame_type(1=keyframe)<<4 | codec_id(7=AVC) = 0x17
        // byte 1: avc_packet_type(0=sequence header)
        // bytes 2-4: composition time offset (0)
        // followed by AVCDecoderConfigurationRecord
        let mut seq_header = BytesMut::new();
        seq_header.extend_from_slice(&[
            0x17, // keyframe + AVC
            0x00, // AVC sequence header
            0x00, 0x00, 0x00, // composition time offset
            // Minimal AVCDecoderConfigurationRecord
            0x01, // configurationVersion
            0x64, // AVCProfileIndication (High)
            0x00, // profile_compatibility
            0x1F, // AVCLevelIndication
            0xFF, // lengthSizeMinusOne = 3 (4 bytes NALU length)
            0xE1, // numOfSequenceParameterSets = 1
            0x00, 0x04, // SPS length
            0x67, 0x64, 0x00, 0x1F, // SPS data
            0x01, // numOfPictureParameterSets = 1
            0x00, 0x02, // PPS length
            0x68, 0xEB, // PPS data
        ]);
        processor.process_video(0, seq_header).await;

        // The sequence header should be consumed by demuxer (returns None)
        // No chunks should be produced
        assert_eq!(sink.chunk_count().await, 0);
    }
}
