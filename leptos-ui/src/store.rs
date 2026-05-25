//! Global reactive store for the dashboard.
//!
//! Provides fine-grained signals so that each WebSocket event type
//! only re-renders the DOM nodes that depend on it.

use leptos::prelude::*;

use crate::api::{
    ChunkStats, EndpointConfig, EventTemplate, LogEntry, StreamingEvent, YouTubeStatusResponse,
};

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
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityEntry {
    pub timestamp: String,
    pub severity: String,
    pub message: String,
    pub source: String,
}

/// Audit log entry from WebSocket (`WsEvent::AuditAppended`).
#[derive(Debug, Clone, PartialEq)]
pub struct AuditEntry {
    pub id: i64,
    pub ts: String,
    pub severity: String,
    pub source: String,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: String,
    pub detail: serde_json::Value,
}

/// Per-endpoint live metrics sample (`WsEvent::MetricsSample`).
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSample {
    pub ts_ms: i64,
    pub event_id: i64,
    pub instance_id: i64,
    pub alias: String,
    pub chunk_delay_secs: f64,
    pub current_chunk_id: i64,
    pub chunks_processed: i64,
    pub alive: bool,
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

/// Per-endpoint YT health snapshot (mirrors rs-core::models::YoutubeHealth).
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
pub struct YoutubeHealth {
    pub stream_status: String,
    pub health_status: String,
    #[serde(default)]
    pub top_issue: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub frame_rate: Option<String>,
    #[serde(default)]
    pub age_secs: i64,
    #[serde(default)]
    pub error: Option<String>,
}

/// Endpoint lifecycle (frontend mirror of `rs-core::models::EndpointLifecycle`).
///
/// Drives the dashboard semaphore: survivable auto-recovery states
/// (`Buffering`/`Rescue`/`Recovering`) render calm/blue, only `Attention`
/// renders red. snake_case on the wire matches the backend serialization.
#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointLifecycle {
    Pending,
    Live,
    Buffering,
    Rescue,
    Recovering,
    Attention,
}

impl Default for EndpointLifecycle {
    fn default() -> Self {
        EndpointLifecycle::Live
    }
}

/// serde `default` helper for `WsDeliveryEndpoint::lifecycle`.
pub fn default_lifecycle() -> EndpointLifecycle {
    EndpointLifecycle::Live
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
    /// Rust-pusher reconnect counter (issue #172). Mirrors
    /// `ffmpeg_restart_count` for endpoints on the rust pusher path so
    /// the dashboard can show "reconn xN" badges on YT/FB upstream
    /// rotation events.
    pub reconnect_count: u32,
    pub last_error: Option<String>,
    pub is_fast: bool,
    pub delivery_mode: Option<String>,
    pub rescue_eta_secs: Option<u64>,
    pub youtube_health: Option<YoutubeHealth>,
    pub lifecycle: EndpointLifecycle,
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
    pub templates_list: RwSignal<Vec<EventTemplate>>,
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

    // Activity / audit feeds and per-endpoint metrics history.
    // Fed by WebSocket events ActivityFeed, AuditAppended and MetricsSample.
    pub activity_feed: RwSignal<Vec<ActivityEntry>>,
    pub audit_feed: RwSignal<Vec<AuditEntry>>,
    pub endpoint_metrics_history: RwSignal<std::collections::HashMap<String, Vec<MetricsSample>>>,

    // RTMP stable-since (seconds the publisher has been connected, monotonic).
    // Populated by polling `/status`; used to gate the Start-Delivery button.
    pub rtmp_stable_secs: RwSignal<u64>,

    // Local chunk-store disk-pressure level: "ok" | "warn" | "critical".
    // Polled from `/status`; drives the DiskPressureBanner (#231).
    pub disk_pressure: RwSignal<String>,
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
            templates_list: RwSignal::new(Vec::new()),
            logs: RwSignal::new(Vec::new()),
            log_component: RwSignal::new("rs_inpoint".to_string()),
            delivery: RwSignal::new(DeliveryState::default()),
            pipeline_state: RwSignal::new(PipelineState::default()),
            selected_event_id: RwSignal::new(None),
            youtube_health: RwSignal::new(None),
            obs_status: RwSignal::new(ObsStatus::default()),
            activity_feed: RwSignal::new(Vec::new()),
            audit_feed: RwSignal::new(Vec::new()),
            endpoint_metrics_history: RwSignal::new(std::collections::HashMap::new()),
            rtmp_stable_secs: RwSignal::new(0),
            disk_pressure: RwSignal::new("ok".to_string()),
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
