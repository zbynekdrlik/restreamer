//! Dashboard component reading from the global store.

use leptos::prelude::*;

use crate::api::{format_bytes, format_duration};
use crate::store::{DashboardStore, DeliveryEndpointState};

/// Format delay as human-readable string (e.g., "11m 9s", "3.2s").
fn format_delay(secs: f64) -> String {
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let mins = (secs / 60.0).floor() as u64;
        let remaining = (secs % 60.0).floor() as u64;
        format!("{}m {}s", mins, remaining)
    }
}

/// Dashboard view reading live data from the store signals.
#[component]
pub fn DashboardView() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");

    view! {
        <div class="status-grid">
            // Streaming Event Card (full width)
            <div class="card event-card">
                <div class="card-header">
                    <span class="card-title">"Streaming Event"</span>
                    <span class=move || {
                        if store.streaming_event.get().is_some() {
                            "status-indicator active"
                        } else {
                            "status-indicator disconnected"
                        }
                    }></span>
                </div>
                {move || {
                    match store.streaming_event.get() {
                        Some(e) => view! {
                            <div class="event-info">
                                <div class="event-field">
                                    <div class="event-field-label">"Event Name"</div>
                                    <div class="event-field-value">{e.name}</div>
                                </div>
                                <div class="event-field">
                                    <div class="event-field-label">"Received"</div>
                                    <div class="event-field-value">{format_bytes(e.received_bytes)}</div>
                                </div>
                            </div>
                        }.into_any(),
                        None => view! {
                            <div class="card-value" style="color: var(--text-secondary)">
                                "No active streaming event"
                            </div>
                        }.into_any(),
                    }
                }}
            </div>

            // Inpoint Status Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Inpoint"</span>
                    <span class=move || {
                        if store.inpoint_connected.get() {
                            "status-indicator active"
                        } else {
                            match store.streaming_event.get() {
                                Some(e) if e.receiving_activated => "status-indicator idle",
                                Some(_) => "status-indicator idle",
                                None => "status-indicator disconnected",
                            }
                        }
                    }></span>
                </div>
                <div class="card-value">{move || {
                    if store.inpoint_connected.get() {
                        "Receiving"
                    } else {
                        match store.streaming_event.get() {
                            Some(e) if e.receiving_activated => "Waiting for OBS",
                            Some(_) => "Paused",
                            None => "No Event",
                        }
                    }
                }}</div>
                <div class="card-label">"RTMP Server Status"</div>
            </div>

            // Buffer Duration Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Buffer"</span>
                    <span class=move || {
                        if store.chunk_stats.get().pending_chunks > 0 {
                            "status-indicator active"
                        } else {
                            "status-indicator idle"
                        }
                    }></span>
                </div>
                <div class="card-value">{move || format_duration(store.chunk_stats.get().buffer_duration_secs)}</div>
                <div class="card-label">"Pending upload time"</div>
            </div>

            // Endpoint Status Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Uploader"</span>
                    <span class=move || {
                        let stats = store.chunk_stats.get();
                        if stats.pending_chunks > 0 { "status-indicator active" }
                        else if stats.total_chunks > 0 { "status-indicator idle" }
                        else { "status-indicator disconnected" }
                    }></span>
                </div>
                <div class="card-value">{move || {
                    let stats = store.chunk_stats.get();
                    if stats.pending_chunks > 0 { "Uploading" }
                    else if stats.total_chunks > 0 { "Idle" }
                    else { "Waiting" }
                }}</div>
                <div class="card-label">"S3 Upload Status"</div>
            </div>

            // Chunk Statistics Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Chunks"</span>
                </div>
                <div class="card-value">{move || store.chunk_stats.get().total_chunks}</div>
                <div class="card-label">{move || {
                    let stats = store.chunk_stats.get();
                    format!("{} pending, {} sent", stats.pending_chunks, stats.sent_chunks)
                }}</div>
            </div>

            // Total Data Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Total Data"</span>
                </div>
                <div class="card-value">{move || format_bytes(store.chunk_stats.get().total_bytes)}</div>
                <div class="card-label">"Total chunk data received"</div>
            </div>
        </div>

        // Chunk Summary Table
        <ChunkSummary />

        // Delivery Monitoring Section
        <DeliverySection />
    }
}

/// Chunk summary table reading from the store.
#[component]
fn ChunkSummary() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");

    view! {
        <div class="card" style="margin-top: 20px;">
            <h3 class="section-title">"Chunk Summary"</h3>
            <div class="chunk-list">
                <div class="chunk-row header">
                    <span>"Category"</span>
                    <span>"Count"</span>
                    <span>"Size"</span>
                    <span>"Status"</span>
                </div>
                <div class="chunk-row">
                    <span>"Pending Chunks"</span>
                    <span>{move || store.chunk_stats.get().pending_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status pending">"Pending"</span>
                </div>
                <div class="chunk-row">
                    <span>"Processing"</span>
                    <span>{move || store.chunk_stats.get().in_process_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status processing">"Uploading"</span>
                </div>
                <div class="chunk-row">
                    <span>"Sent Chunks"</span>
                    <span>{move || store.chunk_stats.get().sent_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status sent">"Sent"</span>
                </div>
                <div class="chunk-row" style="font-weight: 600;">
                    <span>"Total"</span>
                    <span>{move || store.chunk_stats.get().total_chunks}</span>
                    <span>{move || format_bytes(store.chunk_stats.get().total_bytes)}</span>
                    <span>"-"</span>
                </div>
            </div>
        </div>
    }
}

/// Format bandwidth as human-readable string (B/s, KB/s, MB/s).
fn format_bandwidth(bps: f64) -> String {
    if bps >= 1_048_576.0 {
        format!("{:.1} MB/s", bps / 1_048_576.0)
    } else if bps >= 1024.0 {
        format!("{:.1} KB/s", bps / 1024.0)
    } else {
        format!("{:.0} B/s", bps)
    }
}

/// Delivery monitoring section — always visible, shows idle state when not delivering.
#[component]
fn DeliverySection() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");

    view! {
        <div class="delivery-section">
            <div class="card-header">
                <span class="card-title">"Delivery Pipeline"</span>
                <span class=move || {
                    let status = store.delivery.get().status;
                    match status.as_str() {
                        "running" | "delivering" => "status-indicator active",
                        "creating" | "booting" | "deploying" | "ready" => "status-indicator idle",
                        "failed" => "status-indicator error",
                        _ => "status-indicator disconnected",
                    }
                }></span>
            </div>
            {move || {
                let delivery = store.delivery.get();
                let status = delivery.status.clone();
                let endpoints = delivery.endpoints.clone();
                let is_idle = status == "none" || status.is_empty();

                if is_idle {
                    view! {
                        <div class="delivery-idle">
                            <DeliveryLifecycle status="idle".to_string() />
                            <div class="card-value" style="color: var(--text-secondary); font-size: 1rem;">
                                "No active delivery"
                            </div>
                            <div class="card-label">"Start delivering on an event to monitor VPS endpoints"</div>
                        </div>
                    }.into_any()
                } else {
                    view! {
                        <div>
                            <DeliveryLifecycle status=status.clone() />
                            <div class="delivery-endpoints">
                                {endpoints.into_iter().map(|ep| {
                                    view! { <DeliveryEndpointCard endpoint=ep /> }
                                }).collect::<Vec<_>>()}
                            </div>
                        </div>
                    }.into_any()
                }
            }}
        </div>
    }
}

/// Horizontal lifecycle bar showing VPS deployment stages.
#[component]
fn DeliveryLifecycle(status: String) -> impl IntoView {
    let stages = ["creating", "booting", "deploying", "ready", "delivering"];
    let current_idx = stages.iter().position(|&s| s == status);

    view! {
        <div class="lifecycle-bar">
            {stages.iter().enumerate().map(|(i, stage)| {
                let class = match current_idx {
                    Some(idx) if i < idx => "lifecycle-stage completed",
                    Some(idx) if i == idx => "lifecycle-stage active",
                    _ => "lifecycle-stage pending",
                };
                let label = match *stage {
                    "creating" => "Creating",
                    "booting" => "Booting",
                    "deploying" => "Deploying",
                    "ready" => "Ready",
                    "delivering" => "Delivering",
                    _ => stage,
                };
                // Special case: "running" maps to delivering stage
                let class = if status == "running" && *stage == "delivering" {
                    "lifecycle-stage active"
                } else if status == "running" && i < 4 {
                    "lifecycle-stage completed"
                } else if status == "failed" {
                    "lifecycle-stage pending"
                } else {
                    class
                };
                view! {
                    <div class=class>
                        <span class="lifecycle-dot"></span>
                        <span class="lifecycle-label">{label}</span>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

/// Format stall reason as human-readable text.
fn format_stall_reason(reason: &str, restart_count: u32) -> String {
    match reason {
        "ffmpeg_crash_loop" => format!("ffmpeg crash loop ({} restarts)", restart_count),
        "chunk_gap" => "chunk gap (missing S3 data)".to_string(),
        "write_timeout" => "ffmpeg write timeout".to_string(),
        other => other.to_string(),
    }
}

/// Per-endpoint delivery metrics card.
#[component]
fn DeliveryEndpointCard(endpoint: DeliveryEndpointState) -> impl IntoView {
    let is_stalled = endpoint.stall_count >= 3 || endpoint.stall_reason.is_some();

    let alive_class = if is_stalled {
        "status-indicator idle"
    } else if endpoint.alive {
        "status-indicator active"
    } else {
        "status-indicator error"
    };
    let alive_text = if is_stalled {
        "Stalled"
    } else if endpoint.alive {
        "Alive"
    } else {
        "Dead"
    };

    // Delay color: green <30s, yellow <2min, red >=2min
    let delay_class = if endpoint.chunk_delay_secs < 30.0 {
        "delay-low"
    } else if endpoint.chunk_delay_secs < 120.0 {
        "delay-medium"
    } else {
        "delay-high"
    };

    let speed_text = if is_stalled {
        "Stalled".to_string()
    } else {
        format_bandwidth(endpoint.bandwidth_bytes_sec)
    };
    let speed_class = if is_stalled {
        "metric-value delay-high"
    } else {
        "metric-value"
    };

    let stall_reason_text = endpoint
        .stall_reason
        .as_ref()
        .map(|r| format_stall_reason(r, endpoint.ffmpeg_restart_count));
    let last_error_text = endpoint.last_error.clone();

    let alias = endpoint.alias.clone();
    view! {
        <div class="delivery-endpoint-card">
            <div class="endpoint-card-header">
                <span class="endpoint-alias">{alias}</span>
                <span class=alive_class></span>
                <span class="endpoint-alive-text">{alive_text}</span>
            </div>
            {stall_reason_text.map(|reason| view! {
                <div class="stall-reason">{reason}</div>
            })}
            {last_error_text.filter(|e| !e.is_empty()).map(|err| view! {
                <div class="stall-error">{format!("Last error: {}", err)}</div>
            })}
            <div class="endpoint-metrics">
                <div class="metric" title="How far behind live recording. Gap between local chunk and delivery chunk × chunk duration.">
                    <span class="metric-label">"Delay"</span>
                    <span class={format!("metric-value {delay_class}")}>{format_delay(endpoint.chunk_delay_secs)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">"Delivered"</span>
                    <span class="metric-value">{format!("{} chunks", endpoint.chunks_processed)}</span>
                    <span class="metric-sub">{format_bytes(endpoint.bytes_processed_total)}</span>
                </div>
                <div class="metric" title="Delivery throughput. Should match OBS bitrate when keeping up in real-time.">
                    <span class="metric-label">"Speed"</span>
                    <span class=speed_class>{speed_text}</span>
                </div>
            </div>
        </div>
    }
}
