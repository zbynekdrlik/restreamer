//! Global reactive store for the dashboard.
//!
//! Provides fine-grained signals so that each WebSocket event type
//! only re-renders the DOM nodes that depend on it.

use leptos::prelude::*;

use crate::api::{ChunkStats, EndpointConfig, LogEntry, StreamingEvent};

/// A timestamped error for the error list.
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    pub service: String,
    pub message: String,
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
#[derive(Debug, Clone, Default)]
pub struct DeliveryEndpointState {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub buff_size_bytes: i64,
    pub bytes_processed_total: i64,
    pub chunk_delay_secs: f64,
    pub bandwidth_bps: f64,
    pub prev_bytes_total: i64,
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
