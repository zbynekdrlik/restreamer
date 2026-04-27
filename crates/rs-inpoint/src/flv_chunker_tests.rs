//! Tests for `flv_chunker.rs`. Loaded via `#[cfg(test)] #[path = "flv_chunker_tests.rs"] mod tests;`
//! to keep the production file under the 1000-line CI gate.

use super::*;
use std::sync::Arc;

/// Find the first FLV audio tag whose body begins with `marker` and return
/// its 32-bit timestamp (24-bit low + 8-bit high per the FLV spec). Returns
/// None if no such tag is found in the stream.
///
/// Used by tests that need to verify timestamps written into the actual FLV
/// byte stream (e.g. `audio_flv_tag_carries_xiu_timestamp`) rather than
/// trusting in-memory accounting fields.
fn first_flv_audio_timestamp_with_marker(bytes: &[u8], marker: &[u8]) -> Option<u32> {
    // FLV header is 9 bytes + 4 bytes "previous tag size 0" trailer.
    let mut offset = 9 + 4;
    while offset + 11 <= bytes.len() {
        let tag_type = bytes[offset];
        let data_size = ((bytes[offset + 1] as u32) << 16)
            | ((bytes[offset + 2] as u32) << 8)
            | (bytes[offset + 3] as u32);
        let ts_low = ((bytes[offset + 4] as u32) << 16)
            | ((bytes[offset + 5] as u32) << 8)
            | (bytes[offset + 6] as u32);
        let ts_high = bytes[offset + 7] as u32;
        let ts = (ts_high << 24) | ts_low;
        let body_start = offset + 11;
        let body_end = body_start + data_size as usize;
        if tag_type == FLV_TAG_AUDIO
            && body_end <= bytes.len()
            && bytes[body_start..].starts_with(marker)
        {
            return Some(ts);
        }
        offset = body_end + 4; // skip body + 4-byte previous-tag-size trailer
    }
    None
}

#[tokio::test]
async fn null_sink_discards_data() {
    let sink = FlvChunkSink::new_null();
    let data = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &data).await;
    assert_eq!(sink.chunk_count().await, 0);
}

#[tokio::test]
async fn saves_video_sequence_header() {
    let sink = FlvChunkSink::new_null();
    // Sequence header: keyframe AVC + packet type 0
    let seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &seq).await;
    let inner = sink.inner.lock().await;
    assert!(inner.video_sequence_header.is_some());
}

#[tokio::test]
async fn saves_audio_sequence_header() {
    let sink = FlvChunkSink::new_null();
    // AAC sequence header: 0xAF = AAC + stereo, 0x00 = seq header
    let seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &seq).await;
    let inner = sink.inner.lock().await;
    assert!(inner.audio_sequence_header.is_some());
}

#[tokio::test]
async fn produces_chunk_after_duration() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(50),
    ));
    let mut rx = sink.subscribe();

    // First: send sequence headers
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    // Send a keyframe to start first chunk
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;

    // Wait for chunk duration
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send another keyframe to trigger flush
    let keyframe2 = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD][..]);
    sink.write_video(1000, &keyframe2).await;

    let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);

    // Verify FLV header
    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(&file_data[0..3], b"FLV");
    assert_eq!(file_data[3], 0x01); // version
    assert_eq!(file_data[4], 0x05); // audio + video flags
}

#[tokio::test]
async fn flush_produces_final_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;

    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);
}

#[tokio::test]
async fn reset_clears_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
    sink.write_video(0, &keyframe).await;

    sink.reset().await;
    sink.flush().await;

    assert_eq!(sink.chunk_count().await, 0);
}

#[tokio::test]
async fn ignores_non_keyframe_before_first_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

    // Inter frame (0x27 = non-keyframe AVC)
    let inter = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
    sink.write_video(0, &inter).await;

    let inner = sink.inner.lock().await;
    assert!(inner.buffer.is_empty());
    assert!(inner.chunk_start.is_none());
}

#[tokio::test]
async fn flv_tag_structure_is_correct() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));
    let mut rx = sink.subscribe();

    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    let payload = [0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
    let keyframe = BytesMut::from(&payload[..]);
    sink.write_video(100, &keyframe).await;

    sink.flush().await;

    let chunk = rx.recv().await.unwrap();
    let file_data = std::fs::read(&chunk.path).unwrap();

    // FLV header (9) + prev tag size 0 (4) = 13 bytes
    // Then video sequence header tag
    let offset = 13;
    assert_eq!(file_data[offset], FLV_TAG_VIDEO); // tag type = video

    // Find the actual keyframe tag after sequence header
    let seq_data_size = 7u32; // our sequence header is 7 bytes
    let seq_tag_total = 11 + seq_data_size as usize + 4; // header + data + prev_tag_size
    let kf_offset = offset + seq_tag_total;

    assert_eq!(file_data[kf_offset], FLV_TAG_VIDEO); // tag type = video
    // Data size (3 bytes)
    let data_size = ((file_data[kf_offset + 1] as u32) << 16)
        | ((file_data[kf_offset + 2] as u32) << 8)
        | (file_data[kf_offset + 3] as u32);
    assert_eq!(data_size, payload.len() as u32);
}

/// First video frame after session reset must get timestamp = 0.
#[tokio::test]
async fn write_video_first_frame_stamps_ts_zero() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

    // Seed sequence header so the chunk starts immediately.
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(999_999, &video_seq).await;

    // First keyframe — xiu timestamp ignored, wall-clock anchor is set.
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(999_999, &keyframe).await;

    let inner = sink.inner.lock().await;
    // chunk_first_ts is set by write_chunk_header from current_session_ts.
    // The very first call sets anchor and returns 0.
    assert_eq!(
        inner.chunk_first_ts, 0,
        "first frame must anchor session and produce ts=0"
    );
}

/// After reset(), next frame must restart timestamp from 0.
#[tokio::test]
async fn session_start_resets_on_reset() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

    // Seed sequence header.
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    // Write first frame to establish session anchor.
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
    sink.write_video(0, &keyframe).await;

    // Small delay so next frame would have a non-zero wall-clock ts.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Reset session — anchor must be cleared.
    sink.reset().await;

    {
        let inner = sink.inner.lock().await;
        assert_eq!(
            inner.session_start_wall_clock_ms, 0,
            "reset() must clear session_start_wall_clock_ms"
        );
    }

    // Write the next keyframe — its ts should be 0 again.
    sink.write_video(0, &keyframe).await;
    let inner = sink.inner.lock().await;
    assert_eq!(
        inner.chunk_first_ts, 0,
        "first frame after reset must produce ts=0"
    );
}

/// Timestamps must be strictly non-decreasing across successive frames.
#[tokio::test]
async fn wall_clock_ts_monotonic_across_frames() {
    let dir = tempfile::tempdir().unwrap();
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;

    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
    let interframe = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xBB][..]);

    // First keyframe — starts chunk.
    sink.write_video(0, &keyframe).await;
    let ts0 = sink.inner.lock().await.chunk_last_ts;

    tokio::time::sleep(Duration::from_millis(5)).await;

    // Inter-frame — same chunk.
    sink.write_video(0, &interframe).await;
    let ts1 = sink.inner.lock().await.chunk_last_ts;

    tokio::time::sleep(Duration::from_millis(5)).await;

    sink.write_video(0, &interframe).await;
    let ts2 = sink.inner.lock().await.chunk_last_ts;

    assert!(ts1 >= ts0, "ts1={ts1} should be >= ts0={ts0}");
    assert!(ts2 >= ts1, "ts2={ts2} should be >= ts1={ts1}");
}

/// REGRESSION (PR #144): write_audio used to overwrite chunk_last_ts with
/// the xiu RTMP timestamp (small values) while chunk_first_ts held the
/// larger wall-clock value from write_chunk_header. The subtraction
/// underflowed -> duration_ms fell to the "wrapped around" else-branch and
/// returned 0 for EVERY chunk. That cascaded into a 15-minute orchestrator
/// wait, start_chunk_id=1 in the VPS init, and silent VPS warmup spin.
///
/// This test runs a realistic frame sequence and asserts the resulting
/// chunk's duration_ms reflects video wall-clock span, not audio xiu_ts.
#[tokio::test]
async fn chunk_duration_tracks_video_wall_span_not_audio_xiu_ts() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(50),
    ));
    let mut rx = sink.subscribe();

    // Seed sequence headers so chunks form correctly.
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;
    let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &audio_seq).await;

    // First keyframe -- anchors the session, starts chunk #0.
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;

    // Realistic frame mix: audio frames AND video P-frames (inter-frames)
    // arrive between keyframes. P-frames advance chunk_last_ts via write_video
    // (line 224); audio frames must NOT touch chunk_last_ts (the regression
    // we are guarding against). The xiu timestamps on audio (20, 43, 66) are
    // small values that, pre-fix, would underflow chunk_first_ts and produce
    // duration_ms = 0.
    let aac = BytesMut::from(&[0xAF, 0x01, 0x12, 0x34, 0x56][..]);
    let interframe = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xBB][..]);

    sink.write_audio(20, &aac).await;
    sink.write_video(0, &interframe).await; // advances chunk_last_ts
    sink.write_audio(43, &aac).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    sink.write_video(0, &interframe).await; // advances chunk_last_ts to ~100ms
    sink.write_audio(66, &aac).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    sink.write_video(0, &interframe).await; // advances chunk_last_ts to ~200ms

    // Second keyframe flushes the chunk (50ms min duration was hit).
    let keyframe2 = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD][..]);
    sink.write_video(0, &keyframe2).await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("chunk should be emitted within timeout")
        .expect("recv should succeed");

    // ~200 ms of wall-clock between first and second keyframes; allow
    // generous slop for CI scheduling jitter. Pre-fix this is 0.
    assert!(
        chunk.duration_ms >= 150 && chunk.duration_ms <= 350,
        "duration_ms must reflect video wall-clock span (~200ms), got {}",
        chunk.duration_ms
    );
}

/// Audio FLV tags must still carry the xiu RTMP timestamp in the FLV
/// byte stream (PR #142 chipmunk fix preserved). This is the user-facing
/// guarantee -- chunk_last_ts is an internal accounting variable that
/// MUST NOT be confused with what gets written into the audio tag.
#[tokio::test]
async fn audio_flv_tag_carries_xiu_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;
    let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &audio_seq).await;

    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;

    // AAC payload tag with xiu timestamp 42.
    let aac = BytesMut::from(&[0xAF, 0x01, 0xDE, 0xAD][..]);
    sink.write_audio(42, &aac).await;

    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("chunk should flush")
        .expect("recv should succeed");

    let bytes = std::fs::read(&chunk.path).unwrap();

    // The audio sequence header has body 0xAF 0x00; the AAC payload tag we
    // wrote has body 0xAF 0x01. Match on the latter to skip the seq header.
    let ts = first_flv_audio_timestamp_with_marker(&bytes, &[0xAF, 0x01]);
    assert_eq!(
        ts,
        Some(42),
        "audio FLV tag must carry xiu timestamp 42 in the byte stream"
    );
}
