use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, warn};

use rs_core::config::ObsConfig;
use rs_core::models::WsEvent;

/// OBS WebSocket v5 opcodes.
mod op {
    pub const HELLO: u8 = 0;
    pub const IDENTIFY: u8 = 1;
    pub const IDENTIFIED: u8 = 2;
    pub const EVENT: u8 = 5;
    pub const REQUEST: u8 = 6;
    pub const REQUEST_RESPONSE: u8 = 7;
}

/// Current OBS state, updated by the WebSocket listener.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ObsState {
    pub connected: bool,
    pub streaming: bool,
    pub recording: bool,
    pub stream_timecode: Option<String>,
}

impl ObsState {
    pub fn summary(&self) -> String {
        if !self.connected {
            "disconnected".to_string()
        } else if self.streaming {
            "streaming".to_string()
        } else {
            "connected".to_string()
        }
    }

    pub fn to_ws_event(&self) -> WsEvent {
        WsEvent::ObsStatus {
            connected: self.connected,
            streaming: self.streaming,
            recording: self.recording,
            stream_timecode: self.stream_timecode.clone(),
            summary: self.summary(),
        }
    }
}

/// Command sent from API handlers to the OBS client loop.
#[derive(Debug)]
pub enum ObsCommand {
    StartStream,
    StopStream,
}

/// OBS WebSocket v5 client with auto-reconnect and state broadcasting.
pub struct ObsClient {
    state: Arc<RwLock<ObsState>>,
    cmd_tx: mpsc::Sender<ObsCommand>,
}

impl ObsClient {
    /// Spawn the OBS WebSocket client background task.
    /// Returns the client handle for API interactions.
    pub fn spawn(config: ObsConfig, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        let state = Arc::new(RwLock::new(ObsState::default()));
        let (cmd_tx, cmd_rx) = mpsc::channel(16);

        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            obs_connection_loop(config, state_clone, ws_tx, cmd_rx).await;
        });

        Self { state, cmd_tx }
    }

    /// Get current OBS state snapshot.
    pub async fn get_status(&self) -> ObsState {
        self.state.read().await.clone()
    }

    /// Send StartStream command to OBS.
    pub async fn start_stream(&self) -> Result<(), String> {
        self.cmd_tx
            .send(ObsCommand::StartStream)
            .await
            .map_err(|e| format!("OBS client not running: {e}"))
    }

    /// Send StopStream command to OBS.
    pub async fn stop_stream(&self) -> Result<(), String> {
        self.cmd_tx
            .send(ObsCommand::StopStream)
            .await
            .map_err(|e| format!("OBS client not running: {e}"))
    }
}

/// Main reconnection loop: connect → handshake → listen → on disconnect, retry.
async fn obs_connection_loop(
    config: ObsConfig,
    state: Arc<RwLock<ObsState>>,
    ws_tx: broadcast::Sender<WsEvent>,
    mut cmd_rx: mpsc::Receiver<ObsCommand>,
) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!("OBS WebSocket: connecting to {}", config.ws_url);

        match connect_and_run(&config, &state, &ws_tx, &mut cmd_rx).await {
            Ok(()) => {
                // cmd channel closed = client dropped = stop reconnecting
                info!("OBS WebSocket: client stopped, exiting connection loop");
                break;
            }
            Err(e) => {
                warn!("OBS WebSocket: connection error: {e}");
            }
        }

        // Mark disconnected
        {
            let mut s = state.write().await;
            s.connected = false;
            s.streaming = false;
            s.recording = false;
            s.stream_timecode = None;
            let _ = ws_tx.send(s.to_ws_event());
        }

        info!("OBS WebSocket: reconnecting in {:?}", backoff);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Single connection lifecycle: connect, handshake, listen for events.
async fn connect_and_run(
    config: &ObsConfig,
    state: &Arc<RwLock<ObsState>>,
    ws_tx: &broadcast::Sender<WsEvent>,
    cmd_rx: &mut mpsc::Receiver<ObsCommand>,
) -> Result<(), String> {
    let (ws_stream, _) = connect_async(&config.ws_url)
        .await
        .map_err(|e| format!("WebSocket connect failed: {e}"))?;

    let (mut sink, mut stream) = ws_stream.split();

    // Step 1: Receive Hello (op:0)
    let hello = read_json_message(&mut stream)
        .await
        .ok_or("No Hello from OBS")?;
    let hello_op = hello["op"].as_u64().unwrap_or(255) as u8;
    if hello_op != op::HELLO {
        return Err(format!("Expected Hello (op:0), got op:{hello_op}"));
    }
    debug!("OBS WebSocket: received Hello");

    // Step 2: Build Identify message (op:1)
    let identify = build_identify_message(&hello, &config.ws_password)?;
    send_json(&mut sink, &identify).await?;
    debug!("OBS WebSocket: sent Identify");

    // Step 3: Receive Identified (op:2)
    let identified = read_json_message(&mut stream)
        .await
        .ok_or("No Identified from OBS")?;
    let identified_op = identified["op"].as_u64().unwrap_or(255) as u8;
    if identified_op != op::IDENTIFIED {
        return Err(format!(
            "Expected Identified (op:2), got op:{identified_op}"
        ));
    }
    info!("OBS WebSocket: authenticated successfully");

    // Reset backoff on successful connection (caller handles this via Ok return,
    // but we also mark connected state here)
    {
        let mut s = state.write().await;
        s.connected = true;
    }

    // Step 4: Query initial state
    let request_id = "init-stream-status";
    let req = serde_json::json!({
        "op": op::REQUEST,
        "d": {
            "requestType": "GetStreamStatus",
            "requestId": request_id
        }
    });
    send_json(&mut sink, &req).await?;

    // Also query recording status
    let rec_req = serde_json::json!({
        "op": op::REQUEST,
        "d": {
            "requestType": "GetRecordStatus",
            "requestId": "init-record-status"
        }
    });
    send_json(&mut sink, &rec_req).await?;

    // Broadcast initial connected state
    let _ = ws_tx.send(state.read().await.to_ws_event());

    // Step 5: Event loop — listen for OBS events and commands from API
    loop {
        tokio::select! {
            msg = read_json_message(&mut stream) => {
                match msg {
                    Some(json) => {
                        handle_obs_message(&json, state, ws_tx).await;
                    }
                    None => {
                        return Err("OBS WebSocket stream ended".to_string());
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ObsCommand::StartStream) => {
                        let req = serde_json::json!({
                            "op": op::REQUEST,
                            "d": {
                                "requestType": "StartStream",
                                "requestId": "api-start-stream"
                            }
                        });
                        if let Err(e) = send_json(&mut sink, &req).await {
                            error!("Failed to send StartStream: {e}");
                        }
                    }
                    Some(ObsCommand::StopStream) => {
                        let req = serde_json::json!({
                            "op": op::REQUEST,
                            "d": {
                                "requestType": "StopStream",
                                "requestId": "api-stop-stream"
                            }
                        });
                        if let Err(e) = send_json(&mut sink, &req).await {
                            error!("Failed to send StopStream: {e}");
                        }
                    }
                    None => {
                        info!("OBS command channel closed, shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Build the Identify message (op:1), with optional password authentication.
fn build_identify_message(
    hello: &serde_json::Value,
    password: &str,
) -> Result<serde_json::Value, String> {
    let mut d = serde_json::json!({
        "rpcVersion": 1
    });

    // If the server requires authentication, compute the response
    if let Some(auth) = hello["d"]["authentication"].as_object() {
        if password.is_empty() {
            return Err("OBS requires authentication but no password configured".to_string());
        }
        let challenge = auth
            .get("challenge")
            .and_then(|v| v.as_str())
            .ok_or("Missing authentication.challenge")?;
        let salt = auth
            .get("salt")
            .and_then(|v| v.as_str())
            .ok_or("Missing authentication.salt")?;

        let auth_string = compute_obs_auth(password, salt, challenge);
        d["authentication"] = serde_json::Value::String(auth_string);
    }

    Ok(serde_json::json!({
        "op": op::IDENTIFY,
        "d": d
    }))
}

/// OBS WebSocket v5 authentication:
/// base64(sha256(base64(sha256(password + salt)) + challenge))
pub fn compute_obs_auth(password: &str, salt: &str, challenge: &str) -> String {
    // Step 1: sha256(password + salt) → base64
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(salt.as_bytes());
    let secret = BASE64.encode(hasher.finalize());

    // Step 2: sha256(secret + challenge) → base64
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(challenge.as_bytes());
    BASE64.encode(hasher.finalize())
}

/// Handle an incoming OBS message (event or request response).
async fn handle_obs_message(
    msg: &serde_json::Value,
    state: &Arc<RwLock<ObsState>>,
    ws_tx: &broadcast::Sender<WsEvent>,
) {
    let op_code = msg["op"].as_u64().unwrap_or(255) as u8;

    match op_code {
        op::EVENT => {
            let event_type = msg["d"]["eventType"].as_str().unwrap_or("");
            let event_data = &msg["d"]["eventData"];
            debug!("OBS event: {event_type}");

            match event_type {
                "StreamStateChanged" => {
                    let active = event_data["outputActive"].as_bool().unwrap_or(false);
                    let timecode = event_data["outputTimecode"].as_str().map(|s| s.to_string());
                    let mut s = state.write().await;
                    s.streaming = active;
                    if active {
                        s.stream_timecode = timecode;
                    } else {
                        s.stream_timecode = None;
                    }
                    let _ = ws_tx.send(s.to_ws_event());
                }
                "RecordStateChanged" => {
                    let active = event_data["outputActive"].as_bool().unwrap_or(false);
                    let mut s = state.write().await;
                    s.recording = active;
                    let _ = ws_tx.send(s.to_ws_event());
                }
                "ExitStarted" => {
                    info!("OBS is shutting down");
                    let mut s = state.write().await;
                    s.connected = false;
                    s.streaming = false;
                    s.recording = false;
                    s.stream_timecode = None;
                    let _ = ws_tx.send(s.to_ws_event());
                }
                _ => {
                    debug!("Unhandled OBS event: {event_type}");
                }
            }
        }
        op::REQUEST_RESPONSE => {
            let request_id = msg["d"]["requestId"].as_str().unwrap_or("");
            let response_data = &msg["d"]["responseData"];
            debug!("OBS response for: {request_id}");

            match request_id {
                "init-stream-status" => {
                    let active = response_data["outputActive"].as_bool().unwrap_or(false);
                    let timecode = response_data["outputTimecode"]
                        .as_str()
                        .map(|s| s.to_string());
                    let mut s = state.write().await;
                    s.streaming = active;
                    s.stream_timecode = if active { timecode } else { None };
                    let _ = ws_tx.send(s.to_ws_event());
                }
                "init-record-status" => {
                    let active = response_data["outputActive"].as_bool().unwrap_or(false);
                    let mut s = state.write().await;
                    s.recording = active;
                    let _ = ws_tx.send(s.to_ws_event());
                }
                _ => {}
            }
        }
        _ => {
            debug!("Unhandled OBS opcode: {op_code}");
        }
    }
}

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Read a JSON message from the WebSocket stream.
async fn read_json_message(stream: &mut WsStream) -> Option<serde_json::Value> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => match serde_json::from_str(&text) {
                Ok(json) => return Some(json),
                Err(e) => {
                    warn!("OBS: invalid JSON: {e}");
                    continue;
                }
            },
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) => return None,
            Some(Err(e)) => {
                warn!("OBS WebSocket read error: {e}");
                return None;
            }
            None => return None,
            _ => continue,
        }
    }
}

/// Send a JSON value as a WebSocket text message.
async fn send_json(sink: &mut WsSink, value: &serde_json::Value) -> Result<(), String> {
    let text = serde_json::to_string(value).map_err(|e| format!("JSON serialize: {e}"))?;
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|e| format!("WebSocket send: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_obs_auth_produces_valid_base64() {
        let result = compute_obs_auth("password123", "somesalt", "somechallenge");
        // Result should be valid base64
        assert!(BASE64.decode(&result).is_ok());
        // SHA256 output is 32 bytes → base64 is 44 chars
        assert_eq!(result.len(), 44);
    }

    #[test]
    fn compute_obs_auth_deterministic() {
        let a = compute_obs_auth("pass", "salt", "challenge");
        let b = compute_obs_auth("pass", "salt", "challenge");
        assert_eq!(a, b);
    }

    #[test]
    fn compute_obs_auth_different_inputs_different_output() {
        let a = compute_obs_auth("pass1", "salt", "challenge");
        let b = compute_obs_auth("pass2", "salt", "challenge");
        assert_ne!(a, b);
    }

    #[test]
    fn obs_state_summary_disconnected() {
        let state = ObsState::default();
        assert_eq!(state.summary(), "disconnected");
    }

    #[test]
    fn obs_state_summary_connected() {
        let state = ObsState {
            connected: true,
            ..Default::default()
        };
        assert_eq!(state.summary(), "connected");
    }

    #[test]
    fn obs_state_summary_streaming() {
        let state = ObsState {
            connected: true,
            streaming: true,
            ..Default::default()
        };
        assert_eq!(state.summary(), "streaming");
    }

    #[test]
    fn obs_state_to_ws_event() {
        let state = ObsState {
            connected: true,
            streaming: true,
            recording: false,
            stream_timecode: Some("00:01:23".to_string()),
        };
        let event = state.to_ws_event();
        match event {
            WsEvent::ObsStatus {
                connected,
                streaming,
                recording,
                stream_timecode,
                summary,
            } => {
                assert!(connected);
                assert!(streaming);
                assert!(!recording);
                assert_eq!(stream_timecode, Some("00:01:23".to_string()));
                assert_eq!(summary, "streaming");
            }
            _ => panic!("Expected ObsStatus"),
        }
    }

    #[test]
    fn build_identify_no_auth() {
        let hello = serde_json::json!({
            "op": 0,
            "d": {
                "obsWebSocketVersion": "5.0.0",
                "rpcVersion": 1
            }
        });
        let msg = build_identify_message(&hello, "").unwrap();
        assert_eq!(msg["op"], 1);
        assert_eq!(msg["d"]["rpcVersion"], 1);
        assert!(msg["d"]["authentication"].is_null());
    }

    #[test]
    fn build_identify_with_auth() {
        let hello = serde_json::json!({
            "op": 0,
            "d": {
                "obsWebSocketVersion": "5.0.0",
                "rpcVersion": 1,
                "authentication": {
                    "challenge": "test-challenge",
                    "salt": "test-salt"
                }
            }
        });
        let msg = build_identify_message(&hello, "mypassword").unwrap();
        assert_eq!(msg["op"], 1);
        assert!(msg["d"]["authentication"].is_string());
    }

    #[test]
    fn build_identify_auth_required_no_password() {
        let hello = serde_json::json!({
            "op": 0,
            "d": {
                "authentication": {
                    "challenge": "c",
                    "salt": "s"
                }
            }
        });
        let result = build_identify_message(&hello, "");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no password"));
    }

    #[tokio::test]
    async fn handle_stream_state_changed() {
        let state = Arc::new(RwLock::new(ObsState {
            connected: true,
            ..Default::default()
        }));
        let (ws_tx, mut ws_rx) = broadcast::channel(16);

        let msg = serde_json::json!({
            "op": 5,
            "d": {
                "eventType": "StreamStateChanged",
                "eventData": {
                    "outputActive": true,
                    "outputTimecode": "00:00:05"
                }
            }
        });

        handle_obs_message(&msg, &state, &ws_tx).await;

        let s = state.read().await;
        assert!(s.streaming);
        assert_eq!(s.stream_timecode, Some("00:00:05".to_string()));

        let event = ws_rx.try_recv().unwrap();
        match event {
            WsEvent::ObsStatus { streaming, .. } => assert!(streaming),
            _ => panic!("Expected ObsStatus"),
        }
    }

    #[tokio::test]
    async fn handle_record_state_changed() {
        let state = Arc::new(RwLock::new(ObsState {
            connected: true,
            ..Default::default()
        }));
        let (ws_tx, _) = broadcast::channel(16);

        let msg = serde_json::json!({
            "op": 5,
            "d": {
                "eventType": "RecordStateChanged",
                "eventData": {
                    "outputActive": true
                }
            }
        });

        handle_obs_message(&msg, &state, &ws_tx).await;
        assert!(state.read().await.recording);
    }

    #[tokio::test]
    async fn handle_exit_started() {
        let state = Arc::new(RwLock::new(ObsState {
            connected: true,
            streaming: true,
            recording: true,
            stream_timecode: Some("00:01:00".to_string()),
        }));
        let (ws_tx, _) = broadcast::channel(16);

        let msg = serde_json::json!({
            "op": 5,
            "d": {
                "eventType": "ExitStarted",
                "eventData": {}
            }
        });

        handle_obs_message(&msg, &state, &ws_tx).await;

        let s = state.read().await;
        assert!(!s.connected);
        assert!(!s.streaming);
        assert!(!s.recording);
    }

    #[tokio::test]
    async fn handle_get_stream_status_response() {
        let state = Arc::new(RwLock::new(ObsState {
            connected: true,
            ..Default::default()
        }));
        let (ws_tx, _) = broadcast::channel(16);

        let msg = serde_json::json!({
            "op": 7,
            "d": {
                "requestId": "init-stream-status",
                "requestStatus": { "result": true, "code": 100 },
                "responseData": {
                    "outputActive": true,
                    "outputTimecode": "01:23:45"
                }
            }
        });

        handle_obs_message(&msg, &state, &ws_tx).await;

        let s = state.read().await;
        assert!(s.streaming);
        assert_eq!(s.stream_timecode, Some("01:23:45".to_string()));
    }

    #[tokio::test]
    async fn handle_get_record_status_response() {
        let state = Arc::new(RwLock::new(ObsState {
            connected: true,
            ..Default::default()
        }));
        let (ws_tx, _) = broadcast::channel(16);

        let msg = serde_json::json!({
            "op": 7,
            "d": {
                "requestId": "init-record-status",
                "requestStatus": { "result": true, "code": 100 },
                "responseData": {
                    "outputActive": true
                }
            }
        });

        handle_obs_message(&msg, &state, &ws_tx).await;
        assert!(state.read().await.recording);
    }
}
