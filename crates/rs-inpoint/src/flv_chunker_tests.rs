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

/// Find the first FLV tag of `tag_type` whose body begins with `marker` and
/// return its 32-bit timestamp. Generalises
/// `first_flv_audio_timestamp_with_marker` to any tag type so tests can read
/// the video-tag timestamp written into the actual FLV byte stream.
fn first_flv_tag_timestamp(bytes: &[u8], tag_type: u8, marker: &[u8]) -> Option<u32> {
    // FLV header is 9 bytes + 4 bytes "previous tag size 0" trailer.
    let mut offset = 9 + 4;
    while offset + 11 <= bytes.len() {
        let this_type = bytes[offset];
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
        if this_type == tag_type
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

/// Audio FLV tags carry the xiu RTMP timestamp DELTAS (chipmunk fix #142
/// preserved) re-based onto the 0-based per-session epoch (#255). The
/// user-facing guarantee is that audio PTS keeps the drift-free AAC cadence
/// (inter-tag deltas), aligned with the also-0-based video epoch — NOT that
/// the absolute xiu offset survives.
///
/// BEHAVIOUR CHANGE (#255): pre-fix audio carried the RAW xiu ts (first tag at
/// xiu 42 -> tag ts 42); now audio is session-relative (first tag captures the
/// session origin -> tag ts 0, the next tag carries its delta). This is the
/// deliberate fix: after an OBS republish the new session's audio xiu restarts
/// near 0 while video also restarts at 0, so audio MUST be offset-removed to
/// stay aligned. The chipmunk fix is fully preserved because only the constant
/// per-session offset is removed — the inter-tag deltas are byte-identical.
#[tokio::test]
async fn audio_flv_tag_is_session_relative_and_preserves_xiu_deltas() {
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

    // Two AAC payload tags at xiu 42 and 63 (delta 21ms = one 48kHz AAC frame).
    // Distinct body markers so each can be located independently.
    let aac_first = BytesMut::from(&[0xAF, 0x01, 0xDE, 0xAD][..]);
    let aac_second = BytesMut::from(&[0xAF, 0x01, 0xBE, 0xEF][..]);
    sink.write_audio(42, &aac_first).await;
    sink.write_audio(63, &aac_second).await;

    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("chunk should flush")
        .expect("recv should succeed");

    let bytes = std::fs::read(&chunk.path).unwrap();

    // First real audio tag (xiu 42) captures the session origin -> ts 0.
    let ts_first = first_flv_audio_timestamp_with_marker(&bytes, &[0xAF, 0x01, 0xDE, 0xAD]);
    assert_eq!(
        ts_first,
        Some(0),
        "first audio tag of the session must re-zero to the session epoch"
    );

    // Second tag (xiu 63) keeps the xiu delta (63 - 42 = 21) -> chipmunk fix.
    let ts_second = first_flv_audio_timestamp_with_marker(&bytes, &[0xAF, 0x01, 0xBE, 0xEF]);
    assert_eq!(
        ts_second,
        Some(21),
        "audio FLV tag must preserve the xiu inter-tag delta (chipmunk fix #142)"
    );
}

/// REGRESSION (#255): On an OBS mid-stream unpublish->republish the FLV
/// chunker must re-anchor BOTH audio and video onto a single 0-based session
/// epoch. Pre-fix, video kept counting on its never-reset wall-clock anchor
/// (~600ms into session 1) while audio restarted from raw xiu ts ~0 on the
/// fresh session -> ~600ms (in production ~25.5s) audio-behind-video skew
/// baked into the session-2 chunk bytes.
///
/// Session 1: write seq headers + keyframe, then interleave audio while
/// ~600ms of wall-clock elapses (video `current_session_ts` advances). flush.
/// `start_new_session()` (the republish boundary).
/// Session 2: keyframe + audio at xiu ts 0/21/43. Read the session-2 chunk
/// and assert the first real video tag and first real audio tag are within
/// 100ms of each other. RED pre-fix (skew ~600ms); GREEN after re-anchor.
#[tokio::test]
async fn republish_keeps_audio_and_video_aligned() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    // --- Session 1 ---
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;
    let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &audio_seq).await;

    // First keyframe anchors the session wall-clock and starts chunk #0.
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;

    // Interleave audio (xiu cadence) while ~600ms of wall-clock passes so the
    // video session timestamp advances well past 100ms.
    let aac_s1 = BytesMut::from(&[0xAF, 0x01, 0x11, 0x11][..]);
    for i in 0..6u32 {
        sink.write_audio(i * 21, &aac_s1).await;
        let interframe = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xBB][..]);
        sink.write_video(0, &interframe).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sink.flush().await;

    // Drain the session-1 chunk so the next recv() is the session-2 chunk.
    let _ = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("session-1 chunk should flush")
        .expect("recv should succeed");

    // --- Republish boundary ---
    sink.start_new_session().await;

    // --- Session 2 --- fresh xiu epoch (audio restarts near 0).
    // First keyframe of the new session, with a recognizable marker so the
    // tag finder can locate the first REAL video tag (not the seq header).
    let keyframe2 = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD][..]);
    sink.write_video(0, &keyframe2).await;

    // Audio frames on the fresh session at xiu ts 0, 21, 43.
    let aac_s2 = BytesMut::from(&[0xAF, 0x01, 0x22, 0x22][..]);
    sink.write_audio(0, &aac_s2).await;
    sink.write_audio(21, &aac_s2).await;
    sink.write_audio(43, &aac_s2).await;

    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("session-2 chunk should flush")
        .expect("recv should succeed");

    let bytes = std::fs::read(&chunk.path).unwrap();

    // First REAL video tag (the 0xCC,0xDD keyframe) and first REAL audio tag
    // (0x22,0x22 payload) of session 2.
    let video_ts = first_flv_tag_timestamp(
        &bytes,
        FLV_TAG_VIDEO,
        &[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD],
    )
    .expect("session-2 video tag must be present");
    let audio_ts = first_flv_audio_timestamp_with_marker(&bytes, &[0xAF, 0x01, 0x22, 0x22])
        .expect("session-2 audio tag must be present");

    let skew = (video_ts as i64 - audio_ts as i64).abs();
    assert!(
        skew < 100,
        "after republish, A/V skew must be <100ms; got video_ts={video_ts} audio_ts={audio_ts} skew={skew}ms"
    );
}

/// `start_new_session()` re-anchors the per-session epoch but MUST preserve
/// the running chunk numbering and the saved codec sequence headers, so the
/// next chunk stays independently playable and chunk indices never reset.
#[tokio::test]
async fn start_new_session_preserves_chunk_index_and_sequence_headers() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));

    // Seed sequence headers + a keyframe + flush to advance chunk_index to 1.
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;
    let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &audio_seq).await;
    let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
    sink.write_video(0, &keyframe).await;
    sink.flush().await;

    let index_before = sink.chunk_count().await;
    assert_eq!(index_before, 1, "one chunk should have been produced");

    sink.start_new_session().await;

    // chunk_index preserved (NOT reset to 0).
    assert_eq!(
        sink.chunk_count().await,
        index_before,
        "start_new_session() must not reset chunk_index"
    );

    // Saved sequence headers preserved (so next chunk is independently playable).
    {
        let inner = sink.inner.lock().await;
        assert!(
            inner.video_sequence_header.is_some(),
            "start_new_session() must keep the saved video sequence header"
        );
        assert!(
            inner.audio_sequence_header.is_some(),
            "start_new_session() must keep the saved audio sequence header"
        );
        // But the per-session epoch IS re-zeroed.
        assert_eq!(
            inner.session_start_wall_clock_ms, 0,
            "start_new_session() must re-zero the wall-clock anchor"
        );
    }
}
