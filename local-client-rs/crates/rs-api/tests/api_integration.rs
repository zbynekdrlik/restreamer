//! Integration tests that start a real HTTP server and test the API endpoints
//! with actual HTTP requests. These tests verify the full HTTP request/response
//! cycle including serialization, routing, CORS, and error handling.

use std::net::SocketAddr;

use rs_api::state::AppState;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;

/// Create a test AppState with in-memory SQLite.
async fn test_state() -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    AppState::new(pool, config, ws_tx)
}

/// Start a real HTTP server on a random port and return the base URL.
async fn start_server(state: AppState) -> (String, SocketAddr) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (actual_addr, _handle) = rs_api::serve(state, addr).await.unwrap();
    let base = format!("http://{actual_addr}/api/v1");
    (base, actual_addr)
}

#[tokio::test]
async fn health_endpoint_returns_200() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;

    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn status_endpoint_returns_valid_json() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;

    let resp = reqwest::get(format!("{base}/status")).await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.get("inpoint").is_some());
    assert!(body.get("endpoint").is_some());
    assert!(body.get("delivery").is_some());
    assert!(body.get("streaming_event").is_some());
}

#[tokio::test]
async fn streaming_event_lifecycle() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (base, _) = start_server(state).await;

    // Initially no event
    let resp = reqwest::get(format!("{base}/streaming-event"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.is_null());

    // Create a streaming event directly in DB
    db::upsert_streaming_event(&pool, "evt-integration-1", Some("Test Event"), "127.0.0.1")
        .await
        .unwrap();

    // Now it should be present
    let resp = reqwest::get(format!("{base}/streaming-event"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["identifier"], "evt-integration-1");
    assert_eq!(body["short_description"], "Test Event");

    // Delete it via API
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base}/streaming-event"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify it's gone
    let resp = reqwest::get(format!("{base}/streaming-event"))
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.is_null());
}

#[tokio::test]
async fn chunks_crud_via_http() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (base, _) = start_server(state).await;

    // Create a streaming event (chunks require one)
    db::upsert_streaming_event(&pool, "evt-chunk-test", Some("Chunk Test"), "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    // Insert chunks directly
    db::insert_chunk(&pool, event.id, "/tmp/chunk1.ts", 1024, "abc123")
        .await
        .unwrap();
    db::insert_chunk(&pool, event.id, "/tmp/chunk2.ts", 2048, "def456")
        .await
        .unwrap();

    // GET /chunks returns both
    let resp = reqwest::get(format!("{base}/chunks")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let chunks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(chunks.len(), 2);
    // Verify both chunks are present (order may vary by DB implementation)
    let sizes: Vec<i64> = chunks
        .iter()
        .map(|c| c["data_size"].as_i64().unwrap())
        .collect();
    assert!(sizes.contains(&1024));
    assert!(sizes.contains(&2048));

    // GET /chunks/stats returns correct counts
    let resp = reqwest::get(format!("{base}/chunks/stats")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(stats["total_chunks"], 2);
    assert_eq!(stats["pending_chunks"], 2);
    assert_eq!(stats["sent_chunks"], 0);

    // DELETE /chunks removes all
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base}/chunks"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let deleted: u64 = resp.json().await.unwrap();
    assert_eq!(deleted, 2);

    // Verify empty
    let resp = reqwest::get(format!("{base}/chunks")).await.unwrap();
    let chunks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(chunks.is_empty());
}

#[tokio::test]
async fn chunks_pagination() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (base, _) = start_server(state).await;

    db::upsert_streaming_event(&pool, "evt-pag", None, "127.0.0.1")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    // Insert 5 chunks
    for i in 0..5 {
        db::insert_chunk(
            &pool,
            event.id,
            &format!("/tmp/c{i}.ts"),
            i * 100,
            &format!("md5_{i}"),
        )
        .await
        .unwrap();
    }

    // Request with limit=2
    let resp = reqwest::get(format!("{base}/chunks?limit=2"))
        .await
        .unwrap();
    let chunks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(chunks.len(), 2);

    // Request with offset=3
    let resp = reqwest::get(format!("{base}/chunks?offset=3&limit=10"))
        .await
        .unwrap();
    let chunks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(chunks.len(), 2); // Only 2 remaining after offset 3
}

#[tokio::test]
async fn config_get_redacts_credentials() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;

    let resp = reqwest::get(format!("{base}/config")).await.unwrap();
    assert_eq!(resp.status(), 200);

    let config: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(config["s3"]["access_key_id"], "***");
    assert_eq!(config["s3"]["secret_access_key"], "***");
    // Non-sensitive fields preserved
    assert_eq!(config["client_uuid"], "test-uuid-00000000");
}

#[tokio::test]
async fn config_patch_updates_and_validates() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;
    let client = reqwest::Client::new();

    // Valid patch
    let resp = client
        .patch(format!("{base}/config"))
        .json(&serde_json::json!({
            "client_uuid": "updated-uuid-12345678"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let config: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(config["client_uuid"], "updated-uuid-12345678");

    // Invalid patch (empty client_uuid)
    let resp = client
        .patch(format!("{base}/config"))
        .json(&serde_json::json!({ "client_uuid": "" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn toggle_receiving_and_delivering() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (base, _) = start_server(state).await;
    let client = reqwest::Client::new();

    // Without streaming event → 404
    let resp = client
        .post(format!("{base}/actions/toggle-receiving"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Create event
    db::upsert_streaming_event(&pool, "evt-toggle", None, "127.0.0.1")
        .await
        .unwrap();

    // Toggle receiving (was true by default → should flip)
    let resp = client
        .post(format!("{base}/actions/toggle-receiving"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify the flag flipped in DB
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(!event.receiving_activated);

    // Toggle delivering
    let resp = client
        .post(format!("{base}/actions/toggle-delivering"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(!event.delivering_activated);
}

#[tokio::test]
async fn restart_actions_without_channels_return_503() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/actions/restart-inpoint"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    let resp = client
        .post(format!("{base}/actions/restart-endpoint"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn restart_actions_with_channels_send_signal() {
    let mut state = test_state().await;
    let (inpoint_tx, mut inpoint_rx) = tokio::sync::mpsc::channel(1);
    let (endpoint_tx, mut endpoint_rx) = tokio::sync::mpsc::channel(1);
    state.inpoint_restart_tx = Some(inpoint_tx);
    state.endpoint_restart_tx = Some(endpoint_tx);
    let (base, _) = start_server(state).await;
    let client = reqwest::Client::new();

    // Restart inpoint
    let resp = client
        .post(format!("{base}/actions/restart-inpoint"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(inpoint_rx.try_recv().is_ok());

    // Restart endpoint
    let resp = client
        .post(format!("{base}/actions/restart-endpoint"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(endpoint_rx.try_recv().is_ok());
}

#[tokio::test]
async fn logs_endpoints_filter_correctly() {
    let mut state = test_state().await;
    let buffer = rs_core::log_buffer::LogBuffer::new(100);
    buffer.push(rs_core::log_buffer::LogEntry {
        level: "INFO".into(),
        target: "rs_inpoint::rtmp".into(),
        message: "RTMP connection accepted".into(),
    });
    buffer.push(rs_core::log_buffer::LogEntry {
        level: "WARN".into(),
        target: "rs_endpoint::uploader".into(),
        message: "S3 upload retry".into(),
    });
    buffer.push(rs_core::log_buffer::LogEntry {
        level: "ERROR".into(),
        target: "rs_inpoint::muxer".into(),
        message: "Muxer failed".into(),
    });
    state.log_buffer = buffer;

    let (base, _) = start_server(state).await;

    // Inpoint logs should return 2 entries (filtered by "rs_inpoint" prefix)
    let resp = reqwest::get(format!("{base}/logs/inpoint")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(
        entries[0]["target"]
            .as_str()
            .unwrap()
            .starts_with("rs_inpoint")
    );

    // Endpoint logs should return 1 entry
    let resp = reqwest::get(format!("{base}/logs/endpoint")).await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0]["message"]
            .as_str()
            .unwrap()
            .contains("S3 upload")
    );
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;

    let resp = reqwest::get(format!("{base}/does-not-exist"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn clear_chunks_resets_stats_to_zero() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (base, _) = start_server(state).await;

    // Simulate old session: create event + chunks, mark some as sent
    let event_id = db::upsert_streaming_event(&pool, "old-session", None, "127.0.0.1")
        .await
        .unwrap();
    db::insert_chunk(&pool, event_id, "/tmp/old1.bin", 1024, "md5a")
        .await
        .unwrap();
    let chunk2 = db::insert_chunk(&pool, event_id, "/tmp/old2.bin", 2048, "md5b")
        .await
        .unwrap();
    db::set_chunk_sent(&pool, chunk2).await.unwrap();

    // Verify pre-state: 2 total (1 sent, 1 pending)
    let resp = reqwest::get(format!("{base}/chunks/stats")).await.unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(stats["total_chunks"], 2);
    assert_eq!(stats["sent_chunks"], 1);
    assert_eq!(stats["pending_chunks"], 1);

    // Clear all chunks
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{base}/chunks"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify clean state: 0/0/0
    let resp = reqwest::get(format!("{base}/chunks/stats")).await.unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(stats["total_chunks"], 0);
    assert_eq!(stats["sent_chunks"], 0);
    assert_eq!(stats["pending_chunks"], 0);
}

#[tokio::test]
async fn cors_allows_any_origin() {
    let state = test_state().await;
    let (base, _) = start_server(state).await;
    let client = reqwest::Client::new();

    // Any origin should be accepted (LAN access)
    let resp = client
        .get(format!("{base}/health"))
        .header("Origin", "http://192.168.1.100:8910")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let cors_header = resp.headers().get("access-control-allow-origin");
    assert!(cors_header.is_some());
    assert_eq!(cors_header.unwrap(), "*");
}
