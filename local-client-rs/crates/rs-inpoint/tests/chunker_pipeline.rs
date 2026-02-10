//! Integration tests for the MPEG-TS muxer → chunker pipeline.
//!
//! These tests verify that media data flows correctly from the TsMuxer
//! through to the ChunkSink, producing valid chunk files on disk.

use std::sync::Arc;
use std::time::Duration;

use md5::Digest;
use rs_inpoint::chunker::ChunkSink;
use rs_inpoint::muxer::TsMuxer;

#[tokio::test]
async fn muxer_to_chunker_video_produces_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    // Write a synthetic H.264 keyframe (minimal NALU with start code)
    // Start code + IDR slice
    let nalu_data = vec![
        0x00, 0x00, 0x00, 0x01, // start code
        0x65, // IDR slice NALU type (keyframe)
        0x88, 0x84, 0x00, 0x1F, // minimal slice header
        0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
    ];
    let data = bytes::BytesMut::from(&nalu_data[..]);

    muxer.write_video(0, 0, true, data).unwrap();

    let ts_output = muxer.get_data();
    assert!(!ts_output.is_empty(), "Muxer should produce TS output");

    // Verify output is 188-byte aligned TS packets
    assert_eq!(
        ts_output.len() % 188,
        0,
        "TS output must be 188-byte aligned"
    );
    assert_eq!(ts_output[0], 0x47, "First byte must be TS sync byte");

    // Feed to chunker
    sink.write_data(&ts_output).await;
    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("Timed out waiting for chunk")
        .expect("Channel error");

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);
    assert!(!chunk.md5.is_empty());

    // Read the chunk file and verify it contains valid TS data
    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(file_data.len() % 188, 0);
    assert_eq!(file_data[0], 0x47);
}

#[tokio::test]
async fn muxer_to_chunker_audio_produces_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    // Write synthetic AAC audio data (ADTS header + frame)
    let mut adts_frame = vec![
        0xFF, 0xF1, // ADTS sync word + ID + layer + protection
        0x50, // Profile (LC) + sampling freq (44100)
        0x80, // Channel config (2) + frame length high bits
        0x02, 0x00, // frame length low bits + buffer fullness
    ];
    // Pad to make it a valid-ish ADTS frame
    adts_frame.extend_from_slice(&vec![0xAA; 100]);
    // Fix frame length in ADTS header (6 + 100 = 106 bytes)
    let frame_len: u16 = adts_frame.len() as u16;
    adts_frame[3] = (adts_frame[3] & 0xFC) | ((frame_len >> 11) as u8 & 0x03);
    adts_frame[4] = ((frame_len >> 3) as u8) & 0xFF;
    adts_frame[5] = ((frame_len & 0x07) as u8) << 5;

    let data = bytes::BytesMut::from(&adts_frame[..]);
    muxer.write_audio(0, 0, data).unwrap();

    let ts_output = muxer.get_data();
    assert!(
        !ts_output.is_empty(),
        "Muxer should produce TS output for audio"
    );

    sink.write_data(&ts_output).await;
    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("Timed out waiting for chunk")
        .expect("Channel error");

    assert!(chunk.path.exists());
    assert!(chunk.size > 0);
}

#[tokio::test]
async fn muxer_to_chunker_mixed_av_produces_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    // Write video
    let video_data = bytes::BytesMut::from(
        &[
            0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x1F, 0x00, 0x00, 0x03, 0x00,
        ][..],
    );
    muxer.write_video(0, 0, true, video_data).unwrap();
    let ts_video = muxer.get_data();
    sink.write_data(&ts_video).await;

    // Write audio
    let mut adts = vec![0xFF, 0xF1, 0x50, 0x80, 0x02, 0x00];
    adts.extend_from_slice(&vec![0xAA; 50]);
    let data = bytes::BytesMut::from(&adts[..]);
    muxer.write_audio(0, 0, data).unwrap();
    let ts_audio = muxer.get_data();
    sink.write_data(&ts_audio).await;

    // Flush and verify we get a chunk with both audio and video data
    sink.flush().await;

    let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel error");

    assert!(chunk.path.exists());
    // Chunk should contain more data than just video or just audio
    assert!(chunk.size > ts_video.len());
}

#[tokio::test]
async fn chunker_produces_multiple_chunks_over_time() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(50), // Very short duration
    ));
    let mut rx = sink.subscribe();

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    // Write first batch of data
    let video1 = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x1F][..]);
    muxer.write_video(0, 0, true, video1).unwrap();
    sink.write_data(&muxer.get_data()).await;

    // Wait for chunk duration to pass
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Write second batch (this triggers the first chunk)
    let video2 = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x2F][..]);
    muxer.write_video(1000, 1000, true, video2).unwrap();
    sink.write_data(&muxer.get_data()).await;

    // First chunk
    let chunk1 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("Timed out waiting for chunk 1")
        .expect("Channel error");
    assert_eq!(chunk1.index, 0);
    assert!(chunk1.path.exists());

    // Write third batch into the new chunk window
    let video3 = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x3F][..]);
    muxer.write_video(2000, 2000, true, video3).unwrap();
    sink.write_data(&muxer.get_data()).await;

    // Flush remaining data as second chunk
    sink.flush().await;

    let chunk2 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("Timed out waiting for chunk 2")
        .expect("Channel error");
    assert_eq!(chunk2.index, 1);
    assert!(chunk2.path.exists());

    // Verify they are different files
    assert_ne!(chunk1.path, chunk2.path);
}

#[tokio::test]
async fn chunk_md5_matches_file_content() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));
    let mut rx = sink.subscribe();

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    let video = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x1F][..]);
    muxer.write_video(0, 0, true, video).unwrap();
    sink.write_data(&muxer.get_data()).await;
    sink.flush().await;

    let chunk = rx.recv().await.unwrap();

    // Verify MD5 of file matches reported MD5
    let file_data = std::fs::read(&chunk.path).unwrap();
    let mut hasher = md5::Md5::new();
    Digest::update(&mut hasher, &file_data);
    let computed_md5 = format!("{:x}", hasher.finalize());
    assert_eq!(chunk.md5, computed_md5);
}

#[tokio::test]
async fn muxer_reset_allows_new_stream() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60),
    ));

    let mut muxer = TsMuxer::new();
    muxer.init_streams().unwrap();

    // Write some data
    let video = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x1F][..]);
    muxer.write_video(0, 0, true, video).unwrap();
    sink.write_data(&muxer.get_data()).await;

    // Reset everything (simulating stream end)
    sink.flush().await;
    sink.reset().await;
    muxer.reset();
    muxer.init_streams().unwrap();

    // Write data again (new stream)
    let video2 = bytes::BytesMut::from(&[0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x3F][..]);
    muxer.write_video(0, 0, true, video2).unwrap();
    let ts_output = muxer.get_data();
    assert!(
        !ts_output.is_empty(),
        "Muxer should produce output after reset"
    );
}
