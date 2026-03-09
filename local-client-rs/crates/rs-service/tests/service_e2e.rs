//! Full service E2E tests: RTMP → chunks → DB → API verification.
//!
//! These tests start the core service components (RTMP server, API server, database),
//! publish a real RTMP stream via ffmpeg, and verify the entire pipeline:
//! 1. RTMP stream is accepted
//! 2. MPEG-TS chunks are produced and stored on disk
//! 3. Chunks are recorded in the SQLite database
//! 4. API endpoints reflect the correct state

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rs_api::state::AppState;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use rs_inpoint::chunker::ChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;
use tokio::sync::broadcast;

fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn ffmpeg_available() -> bool {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

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

fn spawn_ffmpeg(port: u16, duration_secs: u32) -> std::process::Child {
    std::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            // Force real-time encoding speed so time-based chunking works
            "-re",
            "-f",
            "lavfi",
            "-i",
            &format!("testsrc=size=320x240:rate=30:duration={duration_secs}"),
            "-f",
            "lavfi",
            "-i",
            &format!("sine=frequency=1000:sample_rate=44100:duration={duration_secs}"),
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "30",
            "-bf",
            "0",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-ar",
            "44100",
            "-f",
            "flv",
            &format!("rtmp://127.0.0.1:{port}/live/test"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn ffmpeg")
}

/// Start the core service components for testing:
/// - In-memory SQLite DB with migrations
/// - RTMP server on given port
/// - API server on random port
/// - Chunk-to-DB forwarding task
///
/// Returns (API base URL, DB pool, RTMP shutdown handle, task handles).
async fn start_test_service(
    rtmp_port: u16,
    chunk_dir: &std::path::Path,
) -> (
    String,
    sqlx::SqlitePool,
    broadcast::Sender<()>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    // Database
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    // Config
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(256);

    // API server on random port
    let api_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let api_state = AppState::new(pool.clone(), config, ws_tx.clone());
    let (actual_addr, _api_handle) = rs_api::serve(api_state, api_addr).await.unwrap();
    let api_base = format!("http://{actual_addr}/api/v1");

    // Chunk sink
    let chunk_sink = Arc::new(ChunkSink::new(
        chunk_dir.to_path_buf(),
        Duration::from_millis(500),
    ));

    // Chunk → DB forwarding (mirrors service.rs logic)
    let mut chunk_rx = chunk_sink.subscribe();
    let fwd_pool = pool.clone();
    let fwd_ws_tx = ws_tx.clone();
    let chunk_fwd_task = tokio::spawn(async move {
        loop {
            match chunk_rx.recv().await {
                Ok(chunk_info) => {
                    if let Ok(Some(event)) = db::get_streaming_event(&fwd_pool).await {
                        let path_str = chunk_info.path.to_string_lossy().to_string();
                        if let Ok(id) = db::insert_chunk(
                            &fwd_pool,
                            event.id,
                            &path_str,
                            chunk_info.size as i64,
                            &chunk_info.md5,
                        )
                        .await
                        {
                            let _ = db::update_received_bytes(
                                &fwd_pool,
                                event.id,
                                chunk_info.size as i64,
                            )
                            .await;
                            let _ = fwd_ws_tx.send(WsEvent::ChunkReceived {
                                id,
                                data_size: chunk_info.size as i64,
                                md5: chunk_info.md5,
                            });
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });

    // RTMP server
    let server = RtmpServer::new("127.0.0.1", rtmp_port);
    let shutdown = server.shutdown_handle();
    let sink = Arc::clone(&chunk_sink);
    let inpoint_state = rs_core::models::InpointState::new();
    let rtmp_task = tokio::spawn(async move {
        let _ = server.run(sink, inpoint_state).await;
    });

    (api_base, pool, shutdown, vec![rtmp_task, chunk_fwd_task])
}

#[tokio::test]
async fn full_pipeline_rtmp_to_db_chunks() {
    if !ffmpeg_available() {
        panic!("ffmpeg is required for service E2E tests but was not found in PATH");
    }

    let rtmp_port = find_available_port();
    let dir = tempfile::tempdir().unwrap();

    let (api_base, pool, shutdown, tasks) = start_test_service(rtmp_port, dir.path()).await;

    // Wait for RTMP server to accept connections
    assert!(
        wait_for_port(rtmp_port, Duration::from_secs(5)).await,
        "RTMP server failed to bind within 5 seconds"
    );

    // Create a streaming event directly in DB
    let event_id =
        db::upsert_streaming_event(&pool, "e2e-test-stream", Some("E2E test"), "127.0.0.1")
            .await
            .unwrap();
    assert!(event_id > 0);

    // Publish a 4-second stream via ffmpeg
    let mut ffmpeg = spawn_ffmpeg(rtmp_port, 4);

    // Wait for ffmpeg to finish
    let ffmpeg_status = tokio::task::spawn_blocking(move || ffmpeg.wait())
        .await
        .unwrap()
        .unwrap();

    // Give chunk processing time to finish (flush + DB insert)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify chunks exist in the database via API
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{api_base}/chunks?offset=0&limit=50"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let chunks: Vec<serde_json::Value> = resp.json().await.unwrap();

    assert!(
        !chunks.is_empty(),
        "Database should contain chunks after RTMP stream"
    );

    // Verify chunk records have valid data
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(
            chunk["data_size"].as_i64().unwrap() > 0,
            "Chunk {i} must have positive data_size"
        );
        assert!(
            !chunk["md5"].as_str().unwrap().is_empty(),
            "Chunk {i} must have MD5 hash"
        );
        assert!(
            !chunk["chunk_file_path"].as_str().unwrap().is_empty(),
            "Chunk {i} must have file path"
        );
    }

    // Verify chunk stats
    let resp = client
        .get(format!("{api_base}/chunks/stats"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert!(
        stats["total_chunks"].as_i64().unwrap() > 0,
        "Chunk stats should show total > 0"
    );
    assert!(
        stats["total_bytes"].as_i64().unwrap() > 0,
        "Chunk stats should show bytes > 0"
    );

    // Verify streaming event received_bytes was updated
    let resp = client
        .get(format!("{api_base}/streaming-event"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let event: serde_json::Value = resp.json().await.unwrap();
    assert!(
        event["received_bytes"].as_i64().unwrap() > 0,
        "Streaming event should have received_bytes > 0"
    );

    // Verify chunk files exist on disk
    let chunk_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "bin"))
        .collect();
    assert!(
        !chunk_files.is_empty(),
        "Chunk .bin files should exist on disk"
    );

    // Verify each chunk file is valid MPEG-TS
    for entry in &chunk_files {
        let data = std::fs::read(entry.path()).unwrap();
        assert_eq!(
            data.len() % 188,
            0,
            "File {} must be 188-byte aligned",
            entry.path().display()
        );
        assert_eq!(
            data[0],
            0x47,
            "File {} must start with TS sync byte",
            entry.path().display()
        );
    }

    // Cleanup
    let _ = shutdown.send(());
    for task in tasks {
        task.abort();
    }

    assert!(
        ffmpeg_status.success() || ffmpeg_status.code().is_some(),
        "ffmpeg should have completed"
    );
}

#[tokio::test]
async fn api_shows_correct_status_during_stream() {
    if !ffmpeg_available() {
        panic!("ffmpeg is required for service E2E tests but was not found in PATH");
    }

    let rtmp_port = find_available_port();
    let dir = tempfile::tempdir().unwrap();

    let (api_base, pool, shutdown, tasks) = start_test_service(rtmp_port, dir.path()).await;

    // Wait for RTMP server to accept connections
    assert!(
        wait_for_port(rtmp_port, Duration::from_secs(5)).await,
        "RTMP server failed to bind"
    );

    // Create streaming event directly in DB
    db::upsert_streaming_event(
        &pool,
        "status-test-stream",
        Some("Status test"),
        "127.0.0.1",
    )
    .await
    .unwrap();

    // Health endpoint should work
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{api_base}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Health check must succeed");

    // Start streaming
    let mut ffmpeg = spawn_ffmpeg(rtmp_port, 5);

    // Wait for some data to arrive
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Check chunks are appearing
    let resp = client
        .get(format!("{api_base}/chunks/stats"))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert!(
        stats["total_chunks"].as_i64().unwrap() > 0,
        "Chunks should be accumulating while streaming"
    );

    // Kill ffmpeg and cleanup
    let _ = ffmpeg.kill();
    let _ = ffmpeg.wait();
    let _ = shutdown.send(());
    for task in tasks {
        task.abort();
    }
}
