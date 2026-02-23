//! Integration tests for ChunkUploader that verify the full upload flow
//! with mock S3 and Manager HTTP servers.
//!
//! These tests exercise:
//! - S3 upload with correct key format (`{event_id}/{chunk_id}_{event_id}.bin`)
//! - Manager notification with correct JSON payload (chunk_id, chunk_identifier, chunk_size)
//! - Manager verification with correct request (se_identifier, chunk_id)
//! - Database state transitions (in_process, sent)
//! - Local file cleanup after successful upload

use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{post, put};
use axum::{Json, Router};
use rs_core::config::S3Config;
use rs_core::db;
use rs_core::models::WsEvent;
use rs_endpoint::manager_api::ManagerClient;
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

// --- Mock Manager Server ---

/// Shared state for mock manager server to track notifications
#[derive(Debug, Default)]
struct MockManagerState {
    notification_count: AtomicUsize,
    last_notification: parking_lot::Mutex<Option<serde_json::Value>>,
    check_chunk_count: AtomicUsize,
    last_check_request: parking_lot::Mutex<Option<serde_json::Value>>,
    notification_should_fail: AtomicBool,
    check_should_fail: AtomicBool,
    check_returns_false: AtomicBool,
}

async fn mock_chunk_upload(
    State(state): State<Arc<MockManagerState>>,
    Json(payload): Json<serde_json::Value>,
) -> StatusCode {
    if state.notification_should_fail.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    state.notification_count.fetch_add(1, Ordering::SeqCst);
    *state.last_notification.lock() = Some(payload);
    StatusCode::OK
}

async fn mock_check_chunk(
    State(state): State<Arc<MockManagerState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if state.check_should_fail.load(Ordering::SeqCst) {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    state.check_chunk_count.fetch_add(1, Ordering::SeqCst);
    *state.last_check_request.lock() = Some(payload);

    let chunk_exists = !state.check_returns_false.load(Ordering::SeqCst);
    Ok(Json(serde_json::json!({ "chunk_exists": chunk_exists })))
}

async fn start_mock_manager_server(state: Arc<MockManagerState>) -> String {
    let app = Router::new()
        .route("/chunk-upload/", post(mock_chunk_upload))
        .route("/api/check-chunk/", post(mock_check_chunk))
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
    // Set up mock servers
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    // Set up database with streaming event and chunk
    let pool = setup_test_db().await;
    db::upsert_streaming_event(
        &pool,
        "evt-integration-test",
        Some("Test Event"),
        "127.0.0.1",
    )
    .await
    .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    // Create a temp file for the chunk
    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_1.bin", b"test chunk data");

    // Insert chunk into DB
    db::insert_chunk(
        &pool,
        event.id,
        chunk_path.to_str().unwrap(),
        17, // "test chunk data".len()
        "abc123",
    )
    .await
    .unwrap();

    // Create uploader with mock services
    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    // Run one batch
    uploader.upload_batch().await;

    // Allow async operations to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify S3 upload was called with correct key format
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 1);
    let last_key = s3_state.last_key.lock().clone();
    // Key should be: test-bucket/evt-integration-test/1_evt-integration-test.bin
    assert!(
        last_key.contains("evt-integration-test/1_evt-integration-test.bin"),
        "S3 key should match Python format: {last_key}"
    );

    // Verify manager notification was called with correct payload
    assert_eq!(manager_state.notification_count.load(Ordering::SeqCst), 1);
    let notification = manager_state.last_notification.lock().clone().unwrap();
    assert_eq!(notification["chunk_id"], 1);
    assert_eq!(notification["chunk_identifier"], "evt-integration-test");
    assert_eq!(notification["chunk_size"], 17);

    // Verify check_chunk was called with correct payload
    assert_eq!(manager_state.check_chunk_count.load(Ordering::SeqCst), 1);
    let check_req = manager_state.last_check_request.lock().clone().unwrap();
    assert_eq!(check_req["se_identifier"], "evt-integration-test");
    assert_eq!(check_req["chunk_id"], 1);

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
    // Set up mock servers with S3 failure
    let s3_state = Arc::new(MockS3State::default());
    s3_state.should_fail.store(true, Ordering::SeqCst);
    let manager_state = Arc::new(MockManagerState::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-s3-fail", Some("S3 Fail Test"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_s3fail.bin", b"data");

    db::insert_chunk(&pool, event.id, chunk_path.to_str().unwrap(), 4, "md5hash")
        .await
        .unwrap();

    // Create uploader - use custom S3 config that will fail
    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    // Run one batch (will fail on S3 upload)
    uploader.upload_batch().await;

    // Manager should NOT have been notified (S3 failed first)
    assert_eq!(manager_state.notification_count.load(Ordering::SeqCst), 0);

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
async fn uploader_manager_notification_failure_keeps_chunk_unsent() {
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    manager_state
        .notification_should_fail
        .store(true, Ordering::SeqCst);
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-notify-fail", Some("Notify Fail"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_notifyfail.bin", b"data");

    db::insert_chunk(&pool, event.id, chunk_path.to_str().unwrap(), 4, "md5hash")
        .await
        .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    uploader.upload_batch().await;

    // S3 should have been called
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 1);

    // Chunk should still be unsent (manager notification failed)
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "Chunk should remain unsent after notification failure"
    );
}

#[tokio::test]
async fn uploader_check_chunk_false_keeps_chunk_unsent() {
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    manager_state
        .check_returns_false
        .store(true, Ordering::SeqCst);
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-check-false", Some("Check False"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_checkfalse.bin", b"data");

    db::insert_chunk(&pool, event.id, chunk_path.to_str().unwrap(), 4, "md5hash")
        .await
        .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    uploader.upload_batch().await;

    // All calls should have been made
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 1);
    assert_eq!(manager_state.notification_count.load(Ordering::SeqCst), 1);
    assert_eq!(manager_state.check_chunk_count.load(Ordering::SeqCst), 1);

    // But chunk should remain unsent because check_chunk returned false
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "Chunk should remain unsent when check_chunk returns false"
    );
}

#[tokio::test]
async fn uploader_multiple_chunks_uploads_concurrently() {
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-multi", Some("Multi Chunk"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();

    // Create 5 chunks
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
        )
        .await
        .unwrap();
    }

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    uploader.upload_batch().await;

    // All 5 chunks should be uploaded
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 5);
    assert_eq!(manager_state.notification_count.load(Ordering::SeqCst), 5);
    assert_eq!(manager_state.check_chunk_count.load(Ordering::SeqCst), 5);

    // All chunks should be sent
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    assert!(chunks.is_empty(), "All chunks should be marked as sent");
}

#[tokio::test]
async fn uploader_chunk_without_event_identifier_skipped() {
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    let s3_url = start_mock_s3_server(s3_state.clone()).await;
    let manager_url = start_mock_manager_server(manager_state.clone()).await;

    let pool = setup_test_db().await;
    // Create event WITHOUT identifier (identifier = None scenario)
    // This simulates a corrupted or incomplete event record
    sqlx::query(
        "INSERT INTO streaming_events (id, server_ip, receiving_activated, delivering_activated)
         VALUES (1, '127.0.0.1', true, false)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_no_ident.bin", b"data");

    db::insert_chunk(&pool, 1, chunk_path.to_str().unwrap(), 4, "md5hash")
        .await
        .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    uploader.upload_batch().await;

    // No uploads should occur (chunk skipped due to missing identifier)
    assert_eq!(
        s3_state.upload_count.load(Ordering::SeqCst),
        0,
        "No S3 uploads should happen for chunks without event identifier"
    );
    assert_eq!(manager_state.notification_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn uploader_ws_event_sent_on_upload() {
    let s3_state = Arc::new(MockS3State::default());
    let manager_state = Arc::new(MockManagerState::default());
    let s3_url = start_mock_s3_server(s3_state).await;
    let manager_url = start_mock_manager_server(manager_state).await;

    let pool = setup_test_db().await;
    db::upsert_streaming_event(&pool, "evt-ws-test", Some("WS Test"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path = create_test_chunk_file(&temp_dir, "chunk_ws.bin", b"data");

    db::insert_chunk(&pool, event.id, chunk_path.to_str().unwrap(), 4, "md5hash")
        .await
        .unwrap();

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let manager = ManagerClient::new(&manager_url).unwrap();
    let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, manager, ws_tx);

    uploader.upload_batch().await;

    // Should receive ChunkUploaded event
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
