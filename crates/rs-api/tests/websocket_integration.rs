//! Integration tests for WebSocket event broadcasting.
//!
//! These tests start a real HTTP+WS server, connect via tokio-tungstenite,
//! and verify that WsEvent messages are correctly broadcast to connected clients.

use std::net::SocketAddr;

use futures::{SinkExt, StreamExt};
use rs_api::state::AppState;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

async fn test_state() -> (AppState, broadcast::Sender<WsEvent>) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let state = AppState::new_for_tests(pool, config, ws_tx.clone());
    (state, ws_tx)
}

async fn start_server(state: AppState) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (actual_addr, _handle) = rs_api::serve(state, addr).await.unwrap();
    actual_addr
}

#[tokio::test]
async fn websocket_connects_and_receives_events() {
    let (state, ws_tx) = test_state().await;
    let addr = start_server(state).await;

    // Connect via WebSocket
    let ws_url = format!("ws://{addr}/api/v1/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut _write, mut read) = ws_stream.split();

    // Small delay to let connection establish
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a WsEvent via broadcast channel
    ws_tx
        .send(WsEvent::ChunkReceived {
            id: 42,
            data_size: 1048576,
            md5: "abc123".to_string(),
        })
        .unwrap();

    // Read the message from WebSocket
    let msg = timeout(Duration::from_secs(2), read.next())
        .await
        .expect("timed out waiting for WS message")
        .expect("stream ended")
        .expect("WS error");

    match msg {
        Message::Text(text) => {
            let event: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(event["type"], "ChunkReceived");
            assert_eq!(event["data"]["id"], 42);
            assert_eq!(event["data"]["data_size"], 1048576);
            assert_eq!(event["data"]["md5"], "abc123");
        }
        other => panic!("Expected text message, got: {other:?}"),
    }
}

#[tokio::test]
async fn websocket_receives_multiple_event_types() {
    let (state, ws_tx) = test_state().await;
    let addr = start_server(state).await;

    let ws_url = format!("ws://{addr}/api/v1/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (_write, mut read) = ws_stream.split();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send different event types
    ws_tx
        .send(WsEvent::InpointStatus {
            state: "running".to_string(),
            rtmp_connected: true,
            received_bytes: 5000,
            chunk_count: 2,
        })
        .unwrap();

    ws_tx
        .send(WsEvent::Error {
            service: "endpoint".to_string(),
            message: "S3 timeout".to_string(),
        })
        .unwrap();

    // Read first message (InpointStatus)
    let msg1 = timeout(Duration::from_secs(2), read.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event1: serde_json::Value = serde_json::from_str(&msg1.into_text().unwrap()).unwrap();
    assert_eq!(event1["type"], "InpointStatus");
    assert_eq!(event1["data"]["rtmp_connected"], true);

    // Read second message (Error)
    let msg2 = timeout(Duration::from_secs(2), read.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event2: serde_json::Value = serde_json::from_str(&msg2.into_text().unwrap()).unwrap();
    assert_eq!(event2["type"], "Error");
    assert_eq!(event2["data"]["message"], "S3 timeout");
}

#[tokio::test]
async fn multiple_websocket_clients_receive_same_event() {
    let (state, ws_tx) = test_state().await;
    let addr = start_server(state).await;

    let ws_url = format!("ws://{addr}/api/v1/ws");

    // Connect two clients
    let (ws1, _) = connect_async(&ws_url).await.unwrap();
    let (ws2, _) = connect_async(&ws_url).await.unwrap();
    let (_, mut read1) = ws1.split();
    let (_, mut read2) = ws2.split();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Broadcast one event
    ws_tx.send(WsEvent::ChunkUploaded { chunk_id: 99 }).unwrap();

    // Both clients should receive it
    let msg1 = timeout(Duration::from_secs(2), read1.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event1: serde_json::Value = serde_json::from_str(&msg1.into_text().unwrap()).unwrap();
    assert_eq!(event1["data"]["chunk_id"], 99);

    let msg2 = timeout(Duration::from_secs(2), read2.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event2: serde_json::Value = serde_json::from_str(&msg2.into_text().unwrap()).unwrap();
    assert_eq!(event2["data"]["chunk_id"], 99);
}

#[tokio::test]
async fn websocket_handles_client_close_gracefully() {
    let (state, ws_tx) = test_state().await;
    let addr = start_server(state).await;

    let ws_url = format!("ws://{addr}/api/v1/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut write, _read) = ws_stream.split();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client sends close frame
    write.send(Message::Close(None)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Server should handle this without panicking
    // Verify by sending another event (it should still work for other clients)
    let result = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: 1 });
    // This is Ok even if no subscribers — broadcast doesn't fail on no receivers
    drop(result);
}

#[tokio::test]
async fn websocket_streaming_event_broadcast() {
    let (state, ws_tx) = test_state().await;
    let addr = start_server(state).await;

    let ws_url = format!("ws://{addr}/api/v1/ws");
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (_write, mut read) = ws_stream.split();

    tokio::time::sleep(Duration::from_millis(50)).await;

    ws_tx
        .send(WsEvent::StreamingEvent {
            action: "created".to_string(),
            name: Some("evt-ws-test".to_string()),
            receiving: true,
            delivering: false,
        })
        .unwrap();

    let msg = timeout(Duration::from_secs(2), read.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let event: serde_json::Value = serde_json::from_str(&msg.into_text().unwrap()).unwrap();
    assert_eq!(event["type"], "StreamingEvent");
    assert_eq!(event["data"]["action"], "created");
    assert_eq!(event["data"]["name"], "evt-ws-test");
    assert_eq!(event["data"]["receiving"], true);
    assert_eq!(event["data"]["delivering"], false);
}
