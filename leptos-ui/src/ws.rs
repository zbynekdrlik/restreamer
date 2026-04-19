//! WebSocket client with auto-reconnect and event dispatch.
//!
//! Connects to `/api/v1/ws`, deserializes `WsEvent` messages, and
//! dispatches them to the corresponding signals in `DashboardStore`.

use gloo_net::websocket::Message;
use gloo_net::websocket::futures::WebSocket;
use gloo_timers::callback::Timeout;
use leptos::prelude::*;
use serde::Deserialize;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::{DashboardStore, DeliveryEndpointState, DeliveryState};

/// Tagged WsEvent matching the backend's `#[serde(tag = "type", content = "data")]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "data")]
enum WsEvent {
    InpointStatus {
        #[allow(dead_code)]
        state: String,
        rtmp_connected: bool,
        received_bytes: u64,
        chunk_count: u64,
    },
    EndpointStatus {
        #[allow(dead_code)]
        state: String,
        pending_chunks: u64,
        active_uploads: u32,
        buffer_duration: String,
    },
    ChunkReceived {
        #[allow(dead_code)]
        id: i64,
        data_size: i64,
        #[allow(dead_code)]
        md5: String,
    },
    ChunkUploaded {
        #[allow(dead_code)]
        chunk_id: i64,
    },
    StreamingEvent {
        #[allow(dead_code)]
        action: String,
        #[allow(dead_code)]
        name: Option<String>,
        receiving: bool,
        delivering: bool,
    },
    DeliveryStatus {
        instance_name: String,
        status: String,
        server_ip: Option<String>,
        endpoint_count: u32,
        endpoints: Vec<WsDeliveryEndpoint>,
    },
    Error {
        service: String,
        message: String,
    },
    ActivityFeed {
        timestamp: String,
        severity: String,
        message: String,
        source: String,
    },
    PipelineState {
        state: String,
        event_id: Option<i64>,
        event_name: Option<String>,
        target_delay_secs: u64,
        session_start: Option<String>,
        #[serde(default)]
        local_buffer_chunks: i64,
        #[serde(default)]
        s3_queue_chunks: i64,
        #[serde(default)]
        cache_duration_secs: f64,
    },
    ObsStatus {
        connected: bool,
        streaming: bool,
        recording: bool,
        #[serde(default)]
        stream_timecode: Option<String>,
        summary: String,
    },
    AuditAppended {
        id: i64,
        ts: String,
        severity: String,
        source: String,
        event_id: Option<i64>,
        instance_id: Option<i64>,
        endpoint: Option<String>,
        action: String,
        detail: serde_json::Value,
    },
    MetricsSample {
        ts_ms: i64,
        event_id: i64,
        instance_id: i64,
        alias: String,
        chunk_delay_secs: f64,
        current_chunk_id: i64,
        chunks_processed: i64,
        alive: bool,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct WsDeliveryEndpoint {
    alias: String,
    alive: bool,
    current_chunk_id: i64,
    bytes_processed_total: i64,
    chunks_processed: i64,
    chunk_delay_secs: f64,
    #[serde(default)]
    stall_reason: Option<String>,
    #[serde(default)]
    ffmpeg_restart_count: u32,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    is_fast: bool,
    #[serde(default)]
    delivery_mode: Option<String>,
    #[serde(default)]
    rescue_eta_secs: Option<u64>,
}

/// Compute the WebSocket URL from the current page location.
fn ws_url() -> String {
    let location = gloo_utils::window().location();
    let protocol = location.protocol().unwrap_or_else(|_| "http:".into());
    let ws_proto = if protocol == "https:" { "wss:" } else { "ws:" };
    let host = location.host().unwrap_or_else(|_| "127.0.0.1:8910".into());

    // In Tauri, location is tauri://localhost — use direct address
    if protocol == "tauri:" {
        return "ws://127.0.0.1:8910/api/v1/ws".into();
    }

    format!("{ws_proto}//{host}/api/v1/ws")
}

/// Fetch the initial status from HTTP and populate the store.
async fn load_initial_state(store: DashboardStore) {
    if let Ok(status) = api::get_status().await {
        store.inpoint_connected.set(status.inpoint_connected);
        store.chunk_stats.set(status.chunk_stats);
        store.streaming_event.set(status.streaming_event);
    }
    // Fetch cached delivery status (instant, no VPS round-trip)
    if let Ok(ds) = api::get_delivery_status_cached().await {
        if !ds.status.is_empty() && ds.status != "none" {
            let endpoints: Vec<DeliveryEndpointState> = ds
                .endpoints
                .into_iter()
                .map(|ep| DeliveryEndpointState {
                    alias: ep.alias,
                    alive: ep.alive,
                    current_chunk_id: ep.current_chunk_id,
                    bytes_processed_total: ep.bytes_processed_total,
                    chunks_processed: ep.chunks_processed,
                    chunk_delay_secs: ep.chunk_delay_secs,
                    bandwidth_bytes_sec: 0.0,
                    prev_bytes_total: 0,
                    prev_chunk_id: 0,
                    stall_count: 0,
                    stall_reason: ep.stall_reason,
                    ffmpeg_restart_count: ep.ffmpeg_restart_count,
                    last_error: ep.last_error,
                    is_fast: ep.is_fast,
                    delivery_mode: ep.delivery_mode.clone(),
                    rescue_eta_secs: ep.rescue_eta_secs,
                })
                .collect();
            store.delivery.set(DeliveryState {
                status: ds.status,
                instance_name: ds.instance_name,
                server_ip: ds.server_ip,
                endpoint_count: ds.endpoint_count,
                endpoints,
            });
        }
    }
    if let Ok(events) = api::list_events().await {
        store.events_list.set(events);
    }
    if let Ok(endpoints) = api::list_endpoints().await {
        store.endpoints_list.set(endpoints);
    }
    // Fetch initial OBS status (WebSocket only sends changes, not current state)
    if let Some(obs) = api::get_obs_status().await {
        store.obs_status.set(crate::store::ObsStatus {
            connected: obs.connected,
            streaming: obs.streaming,
            recording: obs.recording,
            stream_timecode: obs.stream_timecode,
            summary: String::new(),
        });
    }
}

/// Dispatch a single WebSocket event to the store.
fn dispatch_event(store: DashboardStore, event: WsEvent) {
    match event {
        WsEvent::InpointStatus {
            rtmp_connected,
            received_bytes,
            chunk_count,
            ..
        } => {
            store.inpoint_connected.set(rtmp_connected);
            store.chunk_stats.update(|stats| {
                stats.total_bytes = received_bytes as i64;
                stats.total_chunks = chunk_count as i64;
            });
        }
        WsEvent::EndpointStatus {
            pending_chunks,
            active_uploads,
            buffer_duration,
            ..
        } => {
            store.chunk_stats.update(|stats| {
                stats.pending_chunks = pending_chunks as i64;
                stats.in_process_chunks = active_uploads as i64;
                // Parse buffer_duration from HH:MM:SS to seconds
                let secs = parse_duration(&buffer_duration);
                stats.buffer_duration_secs = secs;
            });
        }
        WsEvent::ChunkReceived { data_size, .. } => {
            store.chunk_stats.update(|stats| {
                stats.total_chunks += 1;
                stats.pending_chunks += 1;
                stats.total_bytes += data_size;
            });
        }
        WsEvent::ChunkUploaded { .. } => {
            store.chunk_stats.update(|stats| {
                if stats.pending_chunks > 0 {
                    stats.pending_chunks -= 1;
                }
                stats.sent_chunks += 1;
            });
        }
        WsEvent::StreamingEvent {
            receiving,
            delivering,
            ..
        } => {
            // Update the streaming event's flags in-place
            store.streaming_event.update(|evt| {
                if let Some(e) = evt.as_mut() {
                    e.receiving_activated = receiving;
                    e.delivering_activated = delivering;
                }
            });
            // Refresh the full events list from HTTP
            spawn_local(async move {
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
                // Also refresh current streaming event
                if let Ok(evt) = api::get_streaming_event().await {
                    store.streaming_event.set(evt);
                }
            });
        }
        WsEvent::DeliveryStatus {
            instance_name,
            status,
            server_ip,
            endpoint_count,
            endpoints,
        } => {
            store.delivery.update(|d| {
                // Compute bandwidth from bytes_processed_total delta (poll interval ~2s)
                let new_endpoints: Vec<DeliveryEndpointState> = endpoints
                    .into_iter()
                    .map(|ep| {
                        // Find previous state for this alias
                        let prev_state =
                            d.endpoints.iter().find(|prev_ep| prev_ep.alias == ep.alias);
                        let prev_bytes = prev_state.map(|p| p.bytes_processed_total).unwrap_or(0);
                        let prev_chunk = prev_state.map(|p| p.current_chunk_id).unwrap_or(0);
                        let prev_stall = prev_state.map(|p| p.stall_count).unwrap_or(0);

                        let delta = (ep.bytes_processed_total - prev_bytes).max(0) as f64;
                        let bandwidth_bytes_sec = delta / 2.0; // 2-second poll interval

                        // Stall detection: if chunk ID hasn't changed, increment counter
                        let stall_count = if prev_chunk > 0 && ep.current_chunk_id == prev_chunk {
                            prev_stall + 1
                        } else {
                            0
                        };

                        DeliveryEndpointState {
                            alias: ep.alias.clone(),
                            alive: ep.alive,
                            current_chunk_id: ep.current_chunk_id,
                            bytes_processed_total: ep.bytes_processed_total,
                            chunks_processed: ep.chunks_processed,
                            chunk_delay_secs: ep.chunk_delay_secs,
                            bandwidth_bytes_sec,
                            prev_bytes_total: prev_bytes,
                            prev_chunk_id: prev_chunk,
                            stall_count,
                            stall_reason: ep.stall_reason.clone(),
                            ffmpeg_restart_count: ep.ffmpeg_restart_count,
                            last_error: ep.last_error.clone(),
                            is_fast: ep.is_fast,
                            delivery_mode: ep.delivery_mode.clone(),
                            rescue_eta_secs: ep.rescue_eta_secs,
                        }
                    })
                    .collect();

                *d = DeliveryState {
                    status,
                    instance_name,
                    server_ip,
                    endpoint_count,
                    endpoints: new_endpoints,
                };
            });
        }
        WsEvent::Error { service, message } => {
            store.push_error(service, message);
        }
        WsEvent::ActivityFeed {
            timestamp,
            severity,
            message,
            source,
        } => {
            store.activity_feed.update(|feed| {
                feed.push(crate::store::ActivityEntry {
                    timestamp,
                    severity,
                    message,
                    source,
                });
                if feed.len() > 200 {
                    feed.remove(0);
                }
            });
        }
        WsEvent::AuditAppended {
            id,
            ts,
            severity,
            source,
            event_id,
            instance_id,
            endpoint,
            action,
            detail,
        } => {
            store.audit_feed.update(|feed| {
                feed.push(crate::store::AuditEntry {
                    id,
                    ts,
                    severity,
                    source,
                    event_id,
                    instance_id,
                    endpoint,
                    action,
                    detail,
                });
                if feed.len() > 500 {
                    feed.remove(0);
                }
            });
        }
        WsEvent::MetricsSample {
            ts_ms,
            event_id,
            instance_id,
            alias,
            chunk_delay_secs,
            current_chunk_id,
            chunks_processed,
            alive,
        } => {
            store.endpoint_metrics_history.update(|hist| {
                let entry = crate::store::MetricsSample {
                    ts_ms,
                    event_id,
                    instance_id,
                    alias: alias.clone(),
                    chunk_delay_secs,
                    current_chunk_id,
                    chunks_processed,
                    alive,
                };
                hist.entry(alias).or_default().push(entry);
            });
        }
        WsEvent::PipelineState {
            state,
            event_id,
            event_name,
            target_delay_secs,
            session_start,
            local_buffer_chunks,
            s3_queue_chunks,
            cache_duration_secs,
        } => {
            store.pipeline_state.set(crate::store::PipelineState {
                state,
                event_id,
                event_name,
                target_delay_secs,
                session_start,
                local_buffer_chunks,
                s3_queue_chunks,
                cache_duration_secs,
            });
        }
        WsEvent::ObsStatus {
            connected,
            streaming,
            recording,
            stream_timecode,
            summary,
        } => {
            store.obs_status.set(crate::store::ObsStatus {
                connected,
                streaming,
                recording,
                stream_timecode,
                summary,
            });
        }
    }
}

/// Parse "HH:MM:SS" into f64 seconds.
fn parse_duration(s: &str) -> f64 {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 3 {
        let h: f64 = parts[0].parse().unwrap_or(0.0);
        let m: f64 = parts[1].parse().unwrap_or(0.0);
        let s: f64 = parts[2].parse().unwrap_or(0.0);
        h * 3600.0 + m * 60.0 + s
    } else {
        0.0
    }
}

/// Start the WebSocket connection with auto-reconnect.
///
/// Should be called once on app mount. Will reconnect on disconnection
/// with exponential backoff (1s → 2s → 4s → 8s → max 30s).
pub fn connect_websocket(store: DashboardStore) {
    connect_with_backoff(store, 1000);
}

fn connect_with_backoff(store: DashboardStore, delay_ms: u32) {
    use futures::StreamExt;

    let url = ws_url();

    let ws = match WebSocket::open(&url) {
        Ok(ws) => {
            store.ws_connected.set(true);
            ws
        }
        Err(_) => {
            store.ws_connected.set(false);
            schedule_reconnect(store, delay_ms);
            return;
        }
    };

    let (_write, mut read) = ws.split();

    // Load initial state on connect
    spawn_local(load_initial_state(store));

    spawn_local(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(event) = serde_json::from_str::<WsEvent>(&text) {
                        dispatch_event(store, event);
                    }
                }
                Ok(Message::Bytes(_)) => {
                    // Binary messages not expected
                }
                Err(_) => {
                    break;
                }
            }
        }

        // Disconnected — reconnect
        store.ws_connected.set(false);
        schedule_reconnect(store, 1000);
    });
}

fn schedule_reconnect(store: DashboardStore, delay_ms: u32) {
    let next_delay = (delay_ms * 2).min(30_000);
    let timeout = Timeout::new(delay_ms, move || {
        connect_with_backoff(store, next_delay);
    });
    timeout.forget();
}
