//! Dashboard component reading from the global store.

use leptos::prelude::*;

use crate::api::{format_bytes, format_duration};
use crate::store::{DashboardStore, DeliveryEndpointState};

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

/// Delivery monitoring section — visible when delivery status is not "none".
#[component]
fn DeliverySection() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");

    view! {
        {move || {
            let delivery = store.delivery.get();
            if delivery.status == "none" || delivery.status.is_empty() {
                return view! { <div></div> }.into_any();
            }
            let status = delivery.status.clone();
            let endpoints = delivery.endpoints.clone();
            view! {
                <div class="delivery-section">
                    <h3 class="section-title">"Delivery Pipeline"</h3>
                    <DeliveryLifecycle status=status.clone() />
                    <div class="delivery-endpoints">
                        {endpoints.into_iter().map(|ep| {
                            view! { <DeliveryEndpointCard endpoint=ep /> }
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            }.into_any()
        }}
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

/// Per-endpoint delivery metrics card.
#[component]
fn DeliveryEndpointCard(endpoint: DeliveryEndpointState) -> impl IntoView {
    let alive_class = if endpoint.alive {
        "status-indicator active"
    } else {
        "status-indicator error"
    };
    let alive_text = if endpoint.alive { "Alive" } else { "Dead" };

    let delay_class = if endpoint.chunk_delay_secs < 5.0 {
        "delay-low"
    } else if endpoint.chunk_delay_secs < 10.0 {
        "delay-medium"
    } else {
        "delay-high"
    };

    let alias = endpoint.alias.clone();
    view! {
        <div class="delivery-endpoint-card">
            <div class="endpoint-card-header">
                <span class="endpoint-alias">{alias}</span>
                <span class=alive_class></span>
                <span class="endpoint-alive-text">{alive_text}</span>
            </div>
            <div class="endpoint-metrics">
                <div class="metric">
                    <span class="metric-label">"Chunk"</span>
                    <span class="metric-value">{"#"}{endpoint.current_chunk_id}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">"Delay"</span>
                    <span class={format!("metric-value {delay_class}")}>{format!("{:.1}s", endpoint.chunk_delay_secs)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">"Buffer"</span>
                    <span class="metric-value">{format_bytes(endpoint.buff_size_bytes)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">"Bandwidth"</span>
                    <span class="metric-value">{format_bandwidth(endpoint.bandwidth_bps)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">"Total"</span>
                    <span class="metric-value">{format_bytes(endpoint.bytes_processed_total)}</span>
                </div>
            </div>
        </div>
    }
}
