//! Dashboard component reading from the global store.

use leptos::prelude::*;

use crate::api::{format_bytes, format_duration};
use crate::store::DashboardStore;

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
