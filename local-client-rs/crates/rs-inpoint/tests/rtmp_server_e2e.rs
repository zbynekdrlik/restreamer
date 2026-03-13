//! Real RTMP server E2E tests using ffmpeg as the RTMP publisher.
//!
//! These tests start the actual RtmpServer (which uses xiu's RTMP implementation),
//! then use ffmpeg to publish a real H.264/AAC stream over RTMP. The pipeline
//! exercises: TCP → RTMP protocol → StreamsHub → MediaReceiver → FLV demux →
//! MPEG-TS mux → ChunkSink → chunk files on disk.
//!
//! Requires: ffmpeg with libx264 and aac encoder support.

use std::sync::Arc;
use std::time::Duration;

use rs_inpoint::chunker::ChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;

/// Find an available TCP port by binding to port 0.
fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Wait for a TCP port to become connectable (server is ready).
async fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Check if ffmpeg is available.
fn ffmpeg_available() -> bool {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

/// Spawn ffmpeg to publish a short test stream to the RTMP server.
/// Uses `-re` to force real-time encoding speed (otherwise ffmpeg encodes
/// synthetic sources much faster than real-time, defeating time-based chunking).
fn spawn_ffmpeg_publisher(port: u16, duration_secs: u32) -> std::process::Child {
    std::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            // Force real-time encoding speed
            "-re",
            // Video: synthetic test pattern
            "-f",
            "lavfi",
            "-i",
            &format!("testsrc=size=320x240:rate=30:duration={duration_secs}"),
            // Audio: sine wave
            "-f",
            "lavfi",
            "-i",
            &format!("sine=frequency=1000:sample_rate=44100:duration={duration_secs}"),
            // Video codec: H.264
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "30", // keyframe every 30 frames (1 second at 30fps)
            "-bf",
            "0", // no B-frames
            // Audio codec: AAC
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-ar",
            "44100",
            // Output: FLV over RTMP
            "-f",
            "flv",
            &format!("rtmp://127.0.0.1:{port}/live/test"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn ffmpeg — is ffmpeg installed?")
}

#[tokio::test]
async fn rtmp_server_receives_ffmpeg_stream_and_produces_chunks() {
    if !ffmpeg_available() {
        panic!("ffmpeg is required for RTMP E2E tests but was not found in PATH");
    }

    let port = find_available_port();
    let dir = tempfile::tempdir().unwrap();
    let chunk_sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(500), // 500ms chunks for fast testing
    ));
    let mut chunk_rx = chunk_sink.subscribe();

    // Start the real RTMP server
    let server = RtmpServer::new("127.0.0.1", port);
    let shutdown = server.shutdown_handle();
    let sink = Arc::clone(&chunk_sink);
    let inpoint_state = rs_core::models::InpointState::new();
    let server_task = tokio::spawn(async move { server.run(sink, inpoint_state).await });

    // Wait for RTMP server to be ready (TCP port accepting connections)
    assert!(
        wait_for_port(port, Duration::from_secs(5)).await,
        "RTMP server failed to bind to port {port} within 5 seconds"
    );

    // Start ffmpeg to publish a 3-second stream
    let mut ffmpeg = spawn_ffmpeg_publisher(port, 3);

    // Wait for first chunk (ffmpeg needs ~1s to start encoding + 0.5s chunk duration)
    let chunk = tokio::time::timeout(Duration::from_secs(15), chunk_rx.recv())
        .await
        .expect("Timed out waiting for first chunk from ffmpeg stream")
        .expect("Channel error");

    assert!(chunk.path.exists(), "Chunk file must exist");
    assert!(chunk.size > 0, "Chunk must have non-zero size");
    assert!(!chunk.md5.is_empty(), "Chunk must have MD5");

    // Verify the chunk contains valid MPEG-TS data
    let file_data = std::fs::read(&chunk.path).unwrap();
    assert_eq!(
        file_data.len() % 188,
        0,
        "Output must be 188-byte aligned MPEG-TS"
    );
    assert_eq!(file_data[0], 0x47, "First byte must be TS sync byte 0x47");

    // Verify all packets have sync byte
    for (i, pkt) in file_data.chunks(188).enumerate() {
        assert_eq!(pkt[0], 0x47, "TS packet {i} missing sync byte");
    }

    // Wait for ffmpeg to finish (or kill it)
    let _ = ffmpeg.kill();
    let _ = ffmpeg.wait();

    // Shutdown the RTMP server
    let _ = shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
}

#[tokio::test]
async fn rtmp_server_produces_multiple_chunks_from_stream() {
    if !ffmpeg_available() {
        panic!("ffmpeg is required for RTMP E2E tests but was not found in PATH");
    }

    // Use env_logger to capture xiu's log crate output
    let _ = env_logger::builder().is_test(true).try_init();

    let port = find_available_port();
    let dir = tempfile::tempdir().unwrap();
    let chunk_sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(2), // 2-second chunks (resilient to slow CI)
    ));
    let mut chunk_rx = chunk_sink.subscribe();

    let server = RtmpServer::new("127.0.0.1", port);
    let shutdown = server.shutdown_handle();
    let sink = Arc::clone(&chunk_sink);
    let inpoint_state = rs_core::models::InpointState::new();
    let server_task = tokio::spawn(async move { server.run(sink, inpoint_state).await });

    assert!(
        wait_for_port(port, Duration::from_secs(5)).await,
        "RTMP server failed to bind"
    );

    // 15-second stream with 2-second chunks should produce at least 2 chunks
    // (generous timing for CI runners with limited CPU)
    let mut ffmpeg = spawn_ffmpeg_publisher(port, 15);

    // Collect chunks as they arrive while ffmpeg runs
    let mut chunks = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);

    while chunks.len() < 4 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(20), chunk_rx.recv()).await {
            Ok(Ok(chunk)) => {
                eprintln!(
                    "  [chunk {}] {} bytes, md5={}",
                    chunk.index, chunk.size, chunk.md5
                );
                chunks.push(chunk);
            }
            Ok(Err(e)) => {
                eprintln!("  [chunk_rx] channel error: {e}");
                break;
            }
            Err(_) => {
                eprintln!("  [chunk_rx] timeout waiting for chunk");
                break;
            }
        }
    }

    assert!(
        chunks.len() >= 2,
        "Expected at least 2 chunks from 15-second stream, got {}",
        chunks.len()
    );

    // Verify each chunk is a valid, distinct MPEG-TS file
    let mut seen_paths = std::collections::HashSet::new();
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(chunk.path.exists(), "Chunk {i} file must exist");
        assert!(chunk.size > 0, "Chunk {i} must have data");
        assert!(
            seen_paths.insert(&chunk.path),
            "Each chunk must have a unique path"
        );

        let data = std::fs::read(&chunk.path).unwrap();
        assert_eq!(data.len() % 188, 0, "Chunk {i} must be TS-aligned");
        assert_eq!(data[0], 0x47, "Chunk {i} must start with TS sync byte");
    }

    // Chunks should have sequential indices
    assert_eq!(chunks[0].index, 0);
    assert_eq!(chunks[1].index, 1);

    let _ = ffmpeg.kill();
    let _ = ffmpeg.wait();
    let _ = shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
}

#[tokio::test]
async fn rtmp_server_chunk_md5_matches_file() {
    if !ffmpeg_available() {
        panic!("ffmpeg is required for RTMP E2E tests but was not found in PATH");
    }

    let port = find_available_port();
    let dir = tempfile::tempdir().unwrap();
    let chunk_sink = Arc::new(ChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_millis(500),
    ));
    let mut chunk_rx = chunk_sink.subscribe();

    let server = RtmpServer::new("127.0.0.1", port);
    let shutdown = server.shutdown_handle();
    let sink = Arc::clone(&chunk_sink);
    let inpoint_state = rs_core::models::InpointState::new();
    let server_task = tokio::spawn(async move { server.run(sink, inpoint_state).await });

    assert!(
        wait_for_port(port, Duration::from_secs(5)).await,
        "RTMP server failed to bind"
    );

    let mut ffmpeg = spawn_ffmpeg_publisher(port, 3);

    let chunk = tokio::time::timeout(Duration::from_secs(15), chunk_rx.recv())
        .await
        .expect("Timed out")
        .expect("Channel error");

    // Verify MD5 integrity
    use md5::Digest;
    let file_data = std::fs::read(&chunk.path).unwrap();
    let mut hasher = md5::Md5::new();
    Digest::update(&mut hasher, &file_data);
    let computed_md5 = format!("{:x}", hasher.finalize());
    assert_eq!(
        chunk.md5, computed_md5,
        "Chunk MD5 must match actual file content"
    );

    let _ = ffmpeg.kill();
    let _ = ffmpeg.wait();
    let _ = shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
}
