//! Real FLV pipeline E2E tests.
//!
//! These tests feed properly-formatted FLV data (as received from an RTMP publisher
//! like OBS) through the FrameProcessor and verify that valid MPEG-TS chunk files
//! are produced. This exercises the actual media processing pipeline:
//! FLV demux → H.264/AAC extraction → MPEG-TS mux → ChunkSink → file I/O.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use rs_inpoint::chunker::ChunkSink;
use rs_inpoint::media_receiver::FrameProcessor;

/// Build a proper FLV AVC Sequence Header (AVCDecoderConfigurationRecord).
/// This is what OBS sends as the first video tag after publish.
fn build_avc_sequence_header() -> BytesMut {
    let mut buf = BytesMut::new();
    // FLV video tag header:
    // byte 0: frame_type(1=keyframe) << 4 | codec_id(7=AVC) = 0x17
    buf.extend_from_slice(&[0x17]);
    // byte 1: AVC packet type 0 = sequence header
    buf.extend_from_slice(&[0x00]);
    // bytes 2-4: composition time offset = 0
    buf.extend_from_slice(&[0x00, 0x00, 0x00]);
    // AVCDecoderConfigurationRecord:
    buf.extend_from_slice(&[
        0x01, // configurationVersion
        0x42, // AVCProfileIndication (Baseline)
        0xC0, // profile_compatibility
        0x1E, // AVCLevelIndication (3.0)
        0xFF, // lengthSizeMinusOne = 3 (4-byte NALU lengths)
        0xE1, // numOfSequenceParameterSets = 1
    ]);
    // SPS (Sequence Parameter Set) - minimal valid Baseline SPS
    let sps = [0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x00, 0xA0, 0x47, 0xFE, 0xC8];
    buf.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    buf.extend_from_slice(&sps);
    // numOfPictureParameterSets = 1
    buf.extend_from_slice(&[0x01]);
    // PPS (Picture Parameter Set) - minimal valid PPS
    let pps = [0x68, 0xCE, 0x38, 0x80];
    buf.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    buf.extend_from_slice(&pps);
    buf
}

/// Build a proper FLV AVC NALU keyframe.
/// This is what OBS sends for each I-frame.
fn build_avc_keyframe(pts_ms: u32, composition_offset: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    // FLV video tag header:
    // byte 0: frame_type(1=keyframe) << 4 | codec_id(7=AVC) = 0x17
    buf.extend_from_slice(&[0x17]);
    // byte 1: AVC packet type 1 = NALU
    buf.extend_from_slice(&[0x01]);
    // bytes 2-4: composition time offset (signed 24-bit, big-endian)
    let cto_bytes = composition_offset.to_be_bytes();
    buf.extend_from_slice(&cto_bytes[1..4]);
    // NALU data with 4-byte length prefix (Annex B style in AVCC format)
    // IDR slice NALU (type 5)
    let nalu_data = [
        0x65, // NALU type = 5 (IDR slice)
        0x88, 0x84, 0x00, 0x1F, 0xFF, 0xFE, 0xD8, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x03, 0x00,
    ];
    // 4-byte NALU length prefix
    buf.extend_from_slice(&(nalu_data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&nalu_data);
    let _ = pts_ms; // pts is passed as timestamp parameter to process_video
    buf
}

/// Build a proper FLV AVC inter-frame (P-frame).
fn build_avc_interframe(pts_ms: u32, composition_offset: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    // byte 0: frame_type(2=inter) << 4 | codec_id(7=AVC) = 0x27
    buf.extend_from_slice(&[0x27]);
    // byte 1: AVC packet type 1 = NALU
    buf.extend_from_slice(&[0x01]);
    // composition time offset
    let cto_bytes = composition_offset.to_be_bytes();
    buf.extend_from_slice(&cto_bytes[1..4]);
    // P-frame NALU (type 1 = non-IDR slice)
    let nalu_data = [
        0x41, // NALU type = 1 (non-IDR slice)
        0x9A, 0x24, 0x6C, 0x41, 0x4F, 0xFE, 0xD8, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x03, 0x00,
    ];
    buf.extend_from_slice(&(nalu_data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&nalu_data);
    let _ = pts_ms;
    buf
}

/// Build a proper FLV AAC Sequence Header (AudioSpecificConfig).
/// This is what OBS sends as the first audio tag.
fn build_aac_sequence_header() -> BytesMut {
    let mut buf = BytesMut::new();
    // FLV audio tag header:
    // byte 0: sound_format(10=AAC)<<4 | sample_rate(3=44100)<<2 | sample_size(1=16bit)<<1 | channels(1=stereo)
    // = 0xAF
    buf.extend_from_slice(&[0xAF]);
    // byte 1: AAC packet type 0 = sequence header
    buf.extend_from_slice(&[0x00]);
    // AudioSpecificConfig (2 bytes for AAC-LC):
    // 5 bits: audioObjectType = 2 (AAC-LC) -> 00010
    // 4 bits: samplingFrequencyIndex = 4 (44100Hz) -> 0100
    // 4 bits: channelConfiguration = 2 (stereo) -> 0010
    // 3 bits: padding -> 000
    // = 00010 0100 0010 000 = 0x12 0x10
    buf.extend_from_slice(&[0x12, 0x10]);
    buf
}

/// Build a proper FLV AAC Raw audio frame.
fn build_aac_raw_frame(pts_ms: u32) -> BytesMut {
    let mut buf = BytesMut::new();
    // byte 0: same as sequence header
    buf.extend_from_slice(&[0xAF]);
    // byte 1: AAC packet type 1 = raw
    buf.extend_from_slice(&[0x01]);
    // Raw AAC frame data (silent frame for AAC-LC at 44100Hz stereo)
    // This is a minimal valid AAC frame
    buf.extend_from_slice(&[
        0xDE, 0x04, 0x00, 0x4C, 0x61, 0x76, 0x63, 0x35, 0x38, 0x2E, 0x31, 0x33, 0x34, 0x2E, 0x31,
        0x30, 0x30, 0x00, 0x42, 0x20, 0x08, 0xC1, 0x18, 0x38,
    ]);
    let _ = pts_ms;
    buf
}

#[tokio::test]
async fn flv_video_sequence_header_then_keyframe_produces_ts_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    // Step 1: Send AVC sequence header (configures the demuxer)
    let seq_header = build_avc_sequence_header();
    processor.process_video(0, seq_header).await;

    // Step 2: Send a real AVC keyframe
    let keyframe = build_avc_keyframe(0, 0);
    processor.process_video(0, keyframe).await;

    // Step 3: Flush and verify chunk
    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for chunk")
        .expect("Channel error");

    assert!(chunk.path.exists(), "Chunk file must exist on disk");
    assert!(chunk.size > 0, "Chunk must have non-zero size");
    assert!(!chunk.md5.is_empty(), "Chunk must have MD5 hash");

    // Verify the chunk file contains valid MPEG-TS data
    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(
        file_data.len() % 188,
        0,
        "MPEG-TS output must be 188-byte aligned"
    );
    assert_eq!(file_data[0], 0x47, "First byte must be TS sync byte 0x47");

    // Verify ALL packets start with sync byte
    for (i, packet) in file_data.chunks(188).enumerate() {
        assert_eq!(
            packet[0], 0x47,
            "TS packet {i} must start with sync byte 0x47"
        );
    }
}

#[tokio::test]
async fn flv_audio_sequence_header_then_frame_produces_ts_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    // Step 1: Send AAC sequence header
    let seq_header = build_aac_sequence_header();
    processor.process_audio(0, seq_header).await;

    // Step 2: Send a real AAC frame
    let frame = build_aac_raw_frame(0);
    processor.process_audio(0, frame).await;

    // Step 3: Flush and verify
    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for chunk")
        .expect("Channel error");

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);

    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(file_data.len() % 188, 0);
    assert_eq!(file_data[0], 0x47);
}

#[tokio::test]
async fn flv_mixed_av_stream_produces_valid_ts() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    // OBS sends sequence headers first, then interleaved A/V data
    processor
        .process_video(0, build_avc_sequence_header())
        .await;
    processor
        .process_audio(0, build_aac_sequence_header())
        .await;

    // Send several video and audio frames with increasing timestamps
    processor.process_video(0, build_avc_keyframe(0, 0)).await;
    processor.process_audio(0, build_aac_raw_frame(0)).await;
    processor.process_audio(23, build_aac_raw_frame(23)).await;
    processor
        .process_video(33, build_avc_interframe(33, 0))
        .await;
    processor.process_audio(46, build_aac_raw_frame(46)).await;
    processor
        .process_video(66, build_avc_interframe(66, 0))
        .await;
    processor.process_audio(69, build_aac_raw_frame(69)).await;
    processor
        .process_video(100, build_avc_keyframe(100, 0))
        .await;

    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for chunk")
        .expect("Channel error");

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);

    // Verify MPEG-TS structure
    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(file_data.len() % 188, 0);

    // Count TS packets - should have multiple for interleaved A/V
    let packet_count = file_data.len() / 188;
    assert!(
        packet_count >= 3,
        "Mixed A/V stream should produce multiple TS packets, got {packet_count}"
    );

    // All packets must have sync byte
    for (i, packet) in file_data.chunks(188).enumerate() {
        assert_eq!(packet[0], 0x47, "TS packet {i} missing sync byte");
    }

    // Verify received bytes counter
    assert!(
        processor.received_bytes() > 0,
        "FrameProcessor should track received bytes"
    );
}

#[tokio::test]
async fn flv_gop_pattern_produces_multiple_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(50), // Very short chunk duration
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    // Send sequence headers
    processor
        .process_video(0, build_avc_sequence_header())
        .await;
    processor
        .process_audio(0, build_aac_sequence_header())
        .await;

    // GOP 1: keyframe + audio frames
    processor.process_video(0, build_avc_keyframe(0, 0)).await;
    processor.process_audio(0, build_aac_raw_frame(0)).await;
    processor
        .process_video(33, build_avc_interframe(33, 0))
        .await;

    // Wait for chunk duration to pass
    tokio::time::sleep(Duration::from_millis(60)).await;

    // GOP 2: new keyframe triggers chunk boundary
    processor
        .process_video(100, build_avc_keyframe(100, 0))
        .await;
    processor.process_audio(100, build_aac_raw_frame(100)).await;

    // First chunk should be produced
    let chunk1 = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for chunk 1")
        .expect("Channel error");
    assert!(chunk1.path.exists());
    assert_eq!(chunk1.index, 0);

    // Flush to get the second chunk
    sink.flush().await;

    let chunk2 = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for chunk 2")
        .expect("Channel error");
    assert!(chunk2.path.exists());
    assert_eq!(chunk2.index, 1);
    assert_ne!(chunk1.path, chunk2.path);
}

#[tokio::test]
async fn flv_pipeline_md5_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    processor
        .process_video(0, build_avc_sequence_header())
        .await;
    processor.process_video(0, build_avc_keyframe(0, 0)).await;
    processor
        .process_audio(0, build_aac_sequence_header())
        .await;
    processor.process_audio(0, build_aac_raw_frame(0)).await;

    sink.flush().await;

    let chunk = chunk_rx.recv().await.unwrap();

    // Verify MD5 matches actual file content
    use md5::Digest;
    let file_data = std::fs::read(&chunk.path).unwrap();
    let mut hasher = md5::Md5::new();
    Digest::update(&mut hasher, &file_data);
    let computed_md5 = format!("{:x}", hasher.finalize());
    assert_eq!(
        chunk.md5, computed_md5,
        "Chunk MD5 must match file content hash"
    );
}

#[tokio::test]
async fn flv_processor_reset_between_streams() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut chunk_rx = sink.subscribe();
    let mut processor = FrameProcessor::new(sink.clone()).unwrap();

    // Stream 1
    processor
        .process_video(0, build_avc_sequence_header())
        .await;
    processor.process_video(0, build_avc_keyframe(0, 0)).await;
    assert!(processor.received_bytes() > 0);

    // Reset (simulates stream end + new stream start)
    processor.reset().await.unwrap();
    assert_eq!(processor.received_bytes(), 0);

    // Chunk from stream 1 should be flushed
    let chunk1 = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for stream 1 chunk")
        .expect("Channel error");
    assert!(chunk1.path.exists());

    // Stream 2 - should work after reset
    processor
        .process_video(0, build_avc_sequence_header())
        .await;
    processor.process_video(0, build_avc_keyframe(0, 0)).await;
    sink.flush().await;

    let chunk2 = tokio::time::timeout(Duration::from_millis(500), chunk_rx.recv())
        .await
        .expect("Timed out waiting for stream 2 chunk")
        .expect("Channel error");
    assert!(chunk2.path.exists());
    assert_ne!(chunk1.path, chunk2.path);
}
