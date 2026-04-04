//! Global reactive store for the dashboard.
//!
//! Provides fine-grained signals so that each WebSocket event type
//! only re-renders the DOM nodes that depend on it.

use leptos::prelude::*;

use crate::api::{ChunkStats, EndpointConfig, LogEntry, StreamingEvent, YouTubeStatusResponse};

/// A timestamped error for the error list.
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    pub service: String,
    pub message: String,
}

/// Pipeline state from WebSocket.
#[derive(Debug, Clone, Default)]
pub struct PipelineState {
    pub state: String,
    pub event_id: Option<i64>,
    pub event_name: Option<String>,
    pub target_delay_secs: u64,
    pub session_start: Option<String>,
    pub local_buffer_chunks: i64,
    pub s3_queue_chunks: i64,
    pub cache_duration_secs: f64,
}

/// Activity feed entry from WebSocket.
#[derive(Debug, Clone)]
pub struct ActivityEntry {
    pub timestamp: String,
    pub severity: String,
    pub message: String,
    pub source: String,
}

/// Delivery VPS state tracked via WebSocket updates.
#[derive(Debug, Clone, Default)]
pub struct DeliveryState {
    pub status: String,
    pub instance_name: String,
    pub server_ip: Option<String>,
    pub endpoint_count: u32,
    pub endpoints: Vec<DeliveryEndpointState>,
}

/// Per-endpoint delivery metrics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeliveryEndpointState {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub bandwidth_bytes_sec: f64,
    pub prev_bytes_total: i64,
    pub prev_chunk_id: i64,
    pub stall_count: u32,
    pub stall_reason: Option<String>,
    pub ffmpeg_restart_count: u32,
    pub last_error: Option<String>,
    pub is_fast: bool,
}

/// OBS status from WebSocket.
#[derive(Debug, Clone, Default)]
pub struct ObsStatus {
    pub connected: bool,
    pub streaming: bool,
    pub recording: bool,
    pub stream_timecode: Option<String>,
    pub summary: String,
}

/// Central reactive state shared via Leptos context.
#[derive(Debug, Clone, Copy)]
pub struct DashboardStore {
    // Fine-grained status signals
    pub inpoint_connected: RwSignal<bool>,
    pub chunk_stats: RwSignal<ChunkStats>,
    pub streaming_event: RwSignal<Option<StreamingEvent>>,

    // WebSocket-specific live data
    pub ws_connected: RwSignal<bool>,
    pub errors: RwSignal<Vec<ErrorEntry>>,

    // Lists (fetched via HTTP, refreshed on WS events)
    pub events_list: RwSignal<Vec<StreamingEvent>>,
    pub endpoints_list: RwSignal<Vec<EndpointConfig>>,
    pub logs: RwSignal<Vec<LogEntry>>,
    pub log_component: RwSignal<String>,

    // Delivery monitoring
    pub delivery: RwSignal<DeliveryState>,

    // Pipeline state
    pub pipeline_state: RwSignal<PipelineState>,
    pub selected_event_id: RwSignal<Option<i64>>,

    // YouTube health (polled every 30s)
    pub youtube_health: RwSignal<Option<YouTubeStatusResponse>>,

    // OBS status (from WebSocket)
    pub obs_status: RwSignal<ObsStatus>,
}

impl DashboardStore {
    pub fn new() -> Self {
        Self {
            inpoint_connected: RwSignal::new(false),
            chunk_stats: RwSignal::new(ChunkStats::default()),
            streaming_event: RwSignal::new(None),
            ws_connected: RwSignal::new(false),
            errors: RwSignal::new(Vec::new()),
            events_list: RwSignal::new(Vec::new()),
            endpoints_list: RwSignal::new(Vec::new()),
            logs: RwSignal::new(Vec::new()),
            log_component: RwSignal::new("rs_inpoint".to_string()),
            delivery: RwSignal::new(DeliveryState::default()),
            pipeline_state: RwSignal::new(PipelineState::default()),
            selected_event_id: RwSignal::new(None),
            youtube_health: RwSignal::new(None),
            obs_status: RwSignal::new(ObsStatus::default()),
        }
    }

    /// Append an error entry, keeping at most 50 errors.
    pub fn push_error(&self, service: String, message: String) {
        self.errors.update(|errors| {
            errors.push(ErrorEntry { service, message });
            if errors.len() > 50 {
                errors.remove(0);
            }
        });
    }
}
