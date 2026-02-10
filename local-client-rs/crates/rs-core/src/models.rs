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
    ManagerPoll {
        status_code: u16,
        message: String,
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
    pub poller: ComponentStatus,
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
            WsEvent::ManagerPoll {
                status_code: 200,
                message: "ok".to_string(),
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
}
