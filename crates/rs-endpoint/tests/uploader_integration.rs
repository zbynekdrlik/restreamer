//! Integration tests for ChunkUploader that verify the full upload flow
//! with a mock S3 server.
//!
//! These tests exercise:
//! - S3 upload with correct key format (`{event_id}/{sequence_number}_{duration_ms}_{event_id}.bin`)
//! - Database state transitions (in_process, sent)
//! - Local file cleanup after successful upload

use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::put;
use rs_core::config::S3Config;
use rs_core::db;
use rs_core::models::WsEvent;
use rs_endpoint::s3::S3Client;
use rs_endpoint::uploader::ChunkUploader;
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

// --- Mock S3 Server ---

/// Shared state for mock S3 server to track uploads
#[derive(Debug, Default)]
struct MockS3State {
    upload_count: AtomicUsize,
    last_key: parking_lot::Mutex<String>,
    should_fail: AtomicBool,
}

async fn mock_s3_put_object(
    State(state): State<Arc<MockS3State>>,
    Path((bucket, key)): Path<(String, String)>,
    _body: Bytes,
) -> StatusCode {
    if state.should_fail.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    state.upload_count.fetch_add(1, Ordering::SeqCst);
    *state.last_key.lock() = format!("{bucket}/{key}");
    StatusCode::OK
}

async fn start_mock_s3_server(state: Arc<MockS3State>) -> String {
    let app = Router::new()
        .route("/{bucket}/{*key}", put(mock_s3_put_object))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

// --- Test Helpers ---

async fn setup_test_db() -> SqlitePool {
    let pool = db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    pool
}

fn create_test_chunk_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(content).unwrap();
    path
}

fn create_s3_config(endpoint: &str) -> S3Config {
    S3Config {
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: endpoint.to_string(),
        access_key_id: "test-key".to_string(),
        secret_access_key: "test-secret".to_string(),
    }
}

// --- Integration Tests ---

#[tokio::test]
async fn uploader_full_flow_success() {
    let s3_state = Arc::new(MockS3State::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-integration-test")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_1.bin", b"test chunk data");

    db::insert_chunk(
        &pool,
        event.id,
        chunk_path.to_str().unwrap(),
        17,
        "abc123",
        0,
    )
    .await
    .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx);

    uploader.upload_batch().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify S3 upload was called with correct key format
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 1);
    let last_key = s3_state.last_key.lock().clone();
    assert!(
        last_key.contains("evt-integration-test/1_0_evt-integration-test.bin"),
        "S3 key should match format: {last_key}"
    );

    // Verify chunk is marked as sent in DB
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert!(chunks.is_empty(), "Chunk should be marked as sent");

    // Verify local file was deleted
    assert!(
        !chunk_path.exists(),
        "Local chunk file should be deleted after upload"
    );
}

#[tokio::test]
async fn uploader_s3_failure_keeps_chunk_unsent() {
    let s3_state = Arc::new(MockS3State::default());
    s3_state.should_fail.store(true, Ordering::SeqCst);
    let s3_url = start_mock_s3_server(s3_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-s3-fail")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_s3fail.bin", b"data");

    db::insert_chunk(
        &pool,
        event.id,
        chunk_path.to_str().unwrap(),
        4,
        "md5hash",
        0,
    )
    .await
    .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx);

    uploader.upload_batch().await;

    // Chunk should still be unsent
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "Chunk should remain unsent after S3 failure"
    );

    // Local file should still exist
    assert!(
        chunk_path.exists(),
        "Local file should not be deleted on S3 failure"
    );
}

#[tokio::test]
async fn uploader_multiple_chunks_uploads_concurrently() {
    let s3_state = Arc::new(MockS3State::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-multi")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();

    for i in 1..=5 {
        let path = create_test_chunk_file(
            &temp_dir,
            &format!("chunk_{i}.bin"),
            format!("chunk data {i}").as_bytes(),
        );
        db::insert_chunk(
            &pool,
            event.id,
            path.to_str().unwrap(),
            12 + i as i64,
            &format!("md5_{i}"),
            0,
        )
        .await
        .unwrap();
    }

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx);

    uploader.upload_batch().await;

    // All 5 chunks should be uploaded
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 5);

    // All chunks should be sent
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert!(chunks.is_empty(), "All chunks should be marked as sent");
}

#[tokio::test]
async fn uploader_chunk_without_event_skipped() {
    let s3_state = Arc::new(MockS3State::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;

    let pool = setup_test_db().await;

    // Insert a chunk with a non-existent event_id (orphan chunk)
    sqlx::query(
        "INSERT INTO streaming_events (id, name, receiving_activated, delivering_activated)
         VALUES (99, 'temp-event', 1, 0)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_orphan.bin", b"data");

    db::insert_chunk(&pool, 99, chunk_path.to_str().unwrap(), 4, "md5hash", 0)
        .await
        .unwrap();

    // Delete the event so the chunk becomes orphaned
    db::delete_streaming_event(&pool, 99).await.unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx);

    uploader.upload_batch().await;

    // Orphaned chunks (deleted event) should not cause panics
    // The chunks were cascade-deleted with the event, so no uploads happen
    assert_eq!(
        s3_state.upload_count.load(Ordering::SeqCst),
        0,
        "No S3 uploads should happen for chunks from deleted events"
    );
}

#[tokio::test]
async fn uploader_ws_event_sent_on_upload() {
    let s3_state = Arc::new(MockS3State::default());
    let s3_url = start_mock_s3_server(s3_state).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-ws-test")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_ws.bin", b"data");

    db::insert_chunk(
        &pool,
        event.id,
        chunk_path.to_str().unwrap(),
        4,
        "md5hash",
        0,
    )
    .await
    .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx);

    uploader.upload_batch().await;

    let event = tokio::time::timeout(Duration::from_secs(1), ws_rx.recv())
        .await
        .expect("Should receive WS event")
        .expect("Should not be lagged");

    match event {
        WsEvent::ChunkUploaded { chunk_id } => {
            assert_eq!(chunk_id, 1);
        }
        other => panic!("Expected ChunkUploaded event, got: {other:?}"),
    }
}
