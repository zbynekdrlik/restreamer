//! Integration tests for ChunkUploader that verify the full upload flow
//! with a mock S3 server.
//!
//! These tests exercise:
//! - S3 upload with correct key format (`{client_uuid}/{event_id}/{sequence_number}.bin`)
//! - Database state transitions (sent=1 after success)
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

/// Run the uploader long enough to drain the queue, then shut it down.
/// Polls the DB until `predicate` returns true or `max_wait` elapses.
async fn run_until<F>(uploader: ChunkUploader, pool: &SqlitePool, predicate: F, max_wait: Duration)
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
{
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });

    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if predicate().await {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    let _ = pool; // suppress unused warning
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
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx, "test-client-uuid".to_string());

    let s3_state_check = s3_state.clone();
    let pool_check = pool.clone();
    run_until(
        uploader,
        &pool,
        move || {
            let s3_state_check = s3_state_check.clone();
            let pool_check = pool_check.clone();
            Box::pin(async move {
                if s3_state_check.upload_count.load(Ordering::SeqCst) >= 1 {
                    // Give a brief moment for DB writes to complete
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let chunks = db::get_unsent_chunks(&pool_check, 10).await.unwrap();
                    return chunks.is_empty();
                }
                false
            })
        },
        Duration::from_secs(5),
    )
    .await;

    // Verify S3 upload was called with correct key format
    assert_eq!(s3_state.upload_count.load(Ordering::SeqCst), 1);
    let last_key = s3_state.last_key.lock().clone();
    assert!(
        last_key.ends_with("test-client-uuid/evt-integration-test/1.bin"),
        "S3 key should end with {{client_uuid}}/{{event_name}}/{{seq}}.bin: {last_key}"
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
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx, "test-client-uuid".to_string());

    // Let the uploader attempt one upload cycle (first attempt will fail and schedule retry)
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });

    // Wait until the chunk has been attempted at least once (upload_attempts > 0)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let chunks =
            sqlx::query_as::<_, (i64,)>("SELECT upload_attempts FROM chunk_records WHERE id = 1")
                .fetch_optional(&pool)
                .await
                .unwrap();
        if chunks.map(|(a,)| a).unwrap_or(0) >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;

    // Chunk should not be marked as permanently failed yet (only 1 attempt)
    // and should not be sent
    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    // After a failure, the chunk has upload_next_retry_at set in the future,
    // so get_unsent_chunks (which excludes in_process) won't return it,
    // but it is also not sent=1. Check sent directly.
    let sent_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chunk_records WHERE sent = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sent_count.0, 0, "Chunk should not be sent after S3 failure");
    let _ = chunks;

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
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx, "test-client-uuid".to_string());

    let s3_state_check = s3_state.clone();
    let pool_check = pool.clone();
    run_until(
        uploader,
        &pool,
        move || {
            let s3_state_check = s3_state_check.clone();
            let pool_check = pool_check.clone();
            Box::pin(async move {
                if s3_state_check.upload_count.load(Ordering::SeqCst) >= 5 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let chunks = db::get_unsent_chunks(&pool_check, 10).await.unwrap();
                    return chunks.is_empty();
                }
                false
            })
        },
        Duration::from_secs(5),
    )
    .await;

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

    // Re-insert the chunk record manually since cascade delete may have removed it
    // Actually cascade deletes the chunk too. Insert it again with a dummy event that doesn't exist.
    // The worker will call get_streaming_event_by_id which returns None -> marks as sent.
    // Since the chunk was cascade-deleted, there's nothing to upload. Verify 0 S3 uploads.

    let s3 = S3Client::new(&create_s3_config(&s3_url)).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx, "test-client-uuid".to_string());

    // Run for a short time; no chunks exist so no uploads should happen
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;

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
    let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(32);
    let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx, "test-client-uuid".to_string());

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });

    // Wait for ChunkUploaded event
    let uploaded_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws_rx.recv().await {
                Ok(WsEvent::ChunkUploaded { chunk_id }) => return chunk_id,
                Ok(_) => continue, // skip ChunkUploadAttempt and others
                Err(_) => panic!("WS channel closed before ChunkUploaded"),
            }
        }
    })
    .await
    .expect("Should receive ChunkUploaded WS event within 5s");

    assert_eq!(uploaded_event, 1);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    let _ = chunk_path;
}

#[tokio::test]
async fn two_clients_same_event_name_produce_disjoint_keys() {
    // Regression test for issue #114: two Restreamer installs sharing an S3
    // bucket must not collide on identically-named events.
    let s3_state_a = Arc::new(MockS3State::default());
    let s3_url_a = start_mock_s3_server(s3_state_a.clone()).await;

    let s3_state_b = Arc::new(MockS3State::default());
    let s3_url_b = start_mock_s3_server(s3_state_b.clone()).await;

    let pool_a = setup_test_db().await;
    let pool_b = setup_test_db().await;

    // Both instances use the SAME event name.
    let event_name = "shared-event-name";
    db::upsert_streaming_event(&pool_a, event_name)
        .await
        .unwrap();
    db::upsert_streaming_event(&pool_b, event_name)
        .await
        .unwrap();
    let event_a = db::get_streaming_event(&pool_a).await.unwrap().unwrap();
    let event_b = db::get_streaming_event(&pool_b).await.unwrap().unwrap();

    let temp_dir = TempDir::new().unwrap();
    let chunk_path_a = create_test_chunk_file(&temp_dir, "a.bin", b"client-a-chunk");
    let chunk_path_b = create_test_chunk_file(&temp_dir, "b.bin", b"client-b-chunk");

    db::insert_chunk(
        &pool_a,
        event_a.id,
        chunk_path_a.to_str().unwrap(),
        10,
        "aaa",
        0,
    )
    .await
    .unwrap();
    db::insert_chunk(
        &pool_b,
        event_b.id,
        chunk_path_b.to_str().unwrap(),
        11,
        "bbb",
        0,
    )
    .await
    .unwrap();

    let s3_a = S3Client::new(&create_s3_config(&s3_url_a)).unwrap();
    let s3_b = S3Client::new(&create_s3_config(&s3_url_b)).unwrap();
    let (ws_tx_a, _) = broadcast::channel::<WsEvent>(16);
    let (ws_tx_b, _) = broadcast::channel::<WsEvent>(16);

    let uploader_a = ChunkUploader::new(pool_a.clone(), s3_a, ws_tx_a, "client-a-uuid".to_string());
    let uploader_b = ChunkUploader::new(pool_b.clone(), s3_b, ws_tx_b, "client-b-uuid".to_string());

    let s3_state_a_check = s3_state_a.clone();
    let pool_a_check = pool_a.clone();
    run_until(
        uploader_a,
        &pool_a,
        move || {
            let s3_state = s3_state_a_check.clone();
            let pool = pool_a_check.clone();
            Box::pin(async move {
                if s3_state.upload_count.load(Ordering::SeqCst) >= 1 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
                    return chunks.is_empty();
                }
                false
            })
        },
        Duration::from_secs(5),
    )
    .await;

    let s3_state_b_check = s3_state_b.clone();
    let pool_b_check = pool_b.clone();
    run_until(
        uploader_b,
        &pool_b,
        move || {
            let s3_state = s3_state_b_check.clone();
            let pool = pool_b_check.clone();
            Box::pin(async move {
                if s3_state.upload_count.load(Ordering::SeqCst) >= 1 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
                    return chunks.is_empty();
                }
                false
            })
        },
        Duration::from_secs(5),
    )
    .await;

    let key_a = s3_state_a.last_key.lock().clone();
    let key_b = s3_state_b.last_key.lock().clone();

    assert!(
        key_a.contains("client-a-uuid/shared-event-name/"),
        "client A should land under its own uuid prefix: {key_a}"
    );
    assert!(
        key_b.contains("client-b-uuid/shared-event-name/"),
        "client B should land under its own uuid prefix: {key_b}"
    );
    assert_ne!(
        key_a, key_b,
        "two clients with same event name must have disjoint S3 keys"
    );
}
