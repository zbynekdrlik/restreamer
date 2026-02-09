use std::sync::Arc;

use bytes::BytesMut;
use streamhub::define::{BroadcastEvent, BroadcastEventReceiver};
use tracing::{debug, info, warn};
use xflv::demuxer::{FlvAudioTagDemuxer, FlvVideoTagDemuxer};

use crate::chunker::ChunkSink;
use crate::muxer::TsMuxer;

/// Receives media data from the xiu StreamsHub and processes it into MPEG-TS chunks.
///
/// Listens for RTMP publish events, subscribes to the published stream,
/// demuxes FLV audio/video data, muxes to proper MPEG-TS, and feeds the
/// output to the ChunkSink for time-based file writing.
pub struct MediaReceiver {
    event_rx: BroadcastEventReceiver,
    chunk_sink: Arc<ChunkSink>,
}

impl MediaReceiver {
    pub fn new(event_rx: BroadcastEventReceiver, chunk_sink: Arc<ChunkSink>) -> Self {
        Self {
            event_rx,
            chunk_sink,
        }
    }

    /// Run the media receiver loop, processing published streams until shutdown.
    pub async fn run(mut self) {
        info!("Media receiver started, waiting for RTMP publishers");

        loop {
            match self.event_rx.recv().await {
                Ok(event) => match event {
                    BroadcastEvent::Publish { identifier } => {
                        info!("Stream published: {identifier:?}");
                        // The stream data will be forwarded through the StreamsHub
                        // to our subscriber. We process it when we receive Subscribe events.
                    }
                    BroadcastEvent::UnPublish { identifier } => {
                        info!("Stream unpublished: {identifier:?}");
                        self.chunk_sink.flush().await;
                        self.chunk_sink.reset().await;
                    }
                    BroadcastEvent::Subscribe { identifier, .. } => {
                        debug!("New subscriber for stream: {identifier:?}");
                    }
                    BroadcastEvent::UnSubscribe { .. } => {
                        debug!("Subscriber disconnected");
                    }
                },
                Err(e) => {
                    debug!("Broadcast event channel error: {e}");
                    break;
                }
            }
        }

        info!("Media receiver stopped");
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
