use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientProfile {
    pub id: i64,
    pub user_uuid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEvent {
    pub id: i64,
    pub identifier: Option<String>,
    pub short_description: Option<String>,
    pub date_of_event: String,
    pub server_ip: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    pub id: i64,
    pub streaming_event_id: i64,
    pub chunk_file_path: String,
    pub data_size: i64,
    pub created_at: String,
    pub md5: String,
    pub in_process: bool,
    pub sent: bool,
}

/// Endpoint configuration (e.g., YouTube HLS, Facebook RTMP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub id: i64,
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    pub enabled: bool,
    pub position_last: i64,
    pub delivered_bytes: i64,
    pub is_fast: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Event-endpoint many-to-many link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEndpoint {
    pub event_id: i64,
    pub endpoint_id: i64,
}

/// Hetzner delivery VPS instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInstance {
    pub id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub ipv4: String,
    pub status: String,
    pub server_type: String,
    pub event_id: Option<i64>,
    pub created_at: String,
    pub last_health_at: Option<String>,
}

/// Per-endpoint status on delivery VPS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEndpointStatus {
    pub id: i64,
    pub instance_id: i64,
    pub alias: String,
    pub alive: bool,
    pub buff_size_bytes: i64,
    pub current_chunk_id: i64,
    pub last_check_at: String,
}

/// YouTube OAuth tokens (single row, id=1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeOAuth {
    pub id: i64,
    pub access_token: String,
    pub refresh_token: String,
    pub token_uri: String,
    pub client_id: String,
    pub client_secret: String,
    pub scopes: String,
    pub expires_at: Option<String>,
}

/// Scheduled/recurring stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledStream {
    pub id: i64,
    pub event_id: i64,
    pub start_time: String,
    pub repeat_interval: Option<String>,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
    pub enabled: bool,
}

/// Real-time event broadcast over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsEvent {
    InpointStatus {
        state: String,
        rtmp_connected: bool,
        received_bytes: u64,
        chunk_count: u64,
    },
    EndpointStatus {
        state: String,
        pending_chunks: u64,
        active_uploads: u32,
        buffer_duration: String,
    },
    ChunkReceived {
        id: i64,
        data_size: i64,
        md5: String,
    },
    ChunkUploaded {
        chunk_id: i64,
    },
    StreamingEvent {
        action: String,
        identifier: Option<String>,
        receiving: bool,
        delivering: bool,
    },
    DeliveryStatus {
        instance_name: String,
        status: String,
        endpoint_count: u32,
    },
    ScheduleTriggered {
        schedule_id: i64,
        event_id: i64,
    },
    Error {
        service: String,
        message: String,
    },
}

/// Service status summary returned by the /status endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceStatus {
    pub inpoint: ComponentStatus,
    pub endpoint: ComponentStatus,
    pub delivery: ComponentStatus,
    pub streaming_event: Option<StreamingEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    pub state: String,
    pub details: serde_json::Value,
}

impl Default for ComponentStatus {
    fn default() -> Self {
        Self {
            state: String::new(),
            details: serde_json::Value::Object(Default::default()),
        }
    }
}

/// Shared state tracking whether an RTMP publisher (e.g. OBS) is connected.
///
/// Uses `Arc<AtomicBool>` so clones share the same underlying state.
/// Written by `MediaReceiver` on Publish/UnPublish, read by the API `/status` handler.
#[derive(Debug, Clone)]
pub struct InpointState {
    rtmp_connected: Arc<AtomicBool>,
}

impl InpointState {
    pub fn new() -> Self {
        Self {
            rtmp_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_connected(&self, connected: bool) {
        self.rtmp_connected.store(connected, Ordering::Relaxed);
    }

    pub fn is_connected(&self) -> bool {
        self.rtmp_connected.load(Ordering::Relaxed)
    }
}

impl Default for InpointState {
    fn default() -> Self {
        Self::new()
    }
}

/// Chunk statistics returned by the /chunks/stats endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkStats {
    pub total_chunks: i64,
    pub pending_chunks: i64,
    pub sent_chunks: i64,
    pub in_process_chunks: i64,
    pub total_bytes: i64,
    pub buffer_duration_secs: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_event_serde_roundtrip() {
        let events = vec![
            WsEvent::InpointStatus {
                state: "receiving".to_string(),
                rtmp_connected: true,
                received_bytes: 1024,
                chunk_count: 5,
            },
            WsEvent::EndpointStatus {
                state: "uploading".to_string(),
                pending_chunks: 10,
                active_uploads: 2,
                buffer_duration: "00:00:10".to_string(),
            },
            WsEvent::ChunkReceived {
                id: 1,
                data_size: 512,
                md5: "abc123".to_string(),
            },
            WsEvent::ChunkUploaded { chunk_id: 1 },
            WsEvent::StreamingEvent {
                action: "created".to_string(),
                identifier: Some("evt-1".to_string()),
                receiving: true,
                delivering: false,
            },
            WsEvent::DeliveryStatus {
                instance_name: "rs-delivery-1".to_string(),
                status: "running".to_string(),
                endpoint_count: 2,
            },
            WsEvent::ScheduleTriggered {
                schedule_id: 1,
                event_id: 1,
            },
            WsEvent::Error {
                service: "inpoint".to_string(),
                message: "connection lost".to_string(),
            },
        ];

        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: WsEvent = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&parsed).unwrap();
            assert_eq!(json, json2);
        }
    }

    #[test]
    fn streaming_event_serde() {
        let event = StreamingEvent {
            id: 1,
            identifier: Some("test-event".to_string()),
            short_description: Some("Test".to_string()),
            date_of_event: "2026-01-01 00:00:00".to_string(),
            server_ip: "127.0.0.1".to_string(),
            received_bytes: 0,
            receiving_activated: true,
            delivering_activated: false,
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: StreamingEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.identifier, event.identifier);
        assert_eq!(parsed.receiving_activated, true);
    }

    #[test]
    fn chunk_stats_default() {
        let stats = ChunkStats::default();
        assert_eq!(stats.total_chunks, 0);
        assert_eq!(stats.pending_chunks, 0);
        assert_eq!(stats.buffer_duration_secs, 0.0);
    }

    #[test]
    fn inpoint_state_defaults_to_disconnected() {
        let state = InpointState::new();
        assert!(!state.is_connected());
    }

    #[test]
    fn inpoint_state_set_connected() {
        let state = InpointState::new();
        state.set_connected(true);
        assert!(state.is_connected());
    }

    #[test]
    fn inpoint_state_clone_shares_state() {
        let state = InpointState::new();
        let clone = state.clone();
        state.set_connected(true);
        assert!(clone.is_connected());
    }

    #[test]
    fn inpoint_state_toggle() {
        let state = InpointState::new();
        state.set_connected(true);
        assert!(state.is_connected());
        state.set_connected(false);
        assert!(!state.is_connected());
    }
}
