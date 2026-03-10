//! Dashboard component displaying service status cards.

use leptos::prelude::*;

use crate::api::{format_bytes, format_duration, StatusResponse};

/// Dashboard component with status cards.
#[component]
pub fn Dashboard(status: StatusResponse) -> impl IntoView {
    let event = status.streaming_event.clone();
    let stats = status.chunk_stats;

    // Determine inpoint status based on actual RTMP connection state
    let (inpoint_status, inpoint_class) = if status.inpoint_connected {
        ("Receiving", "active")
    } else {
        match &event {
            Some(e) if e.receiving_activated => ("Waiting for OBS", "idle"),
            Some(_) => ("Paused", "idle"),
            None => ("No Event", "disconnected"),
        }
    };

    // Determine endpoint status
    let endpoint_status = if stats.pending_chunks > 0 {
        ("Uploading", "active")
    } else if stats.total_chunks > 0 {
        ("Idle", "idle")
    } else {
        ("Waiting", "disconnected")
    };

    view! {
        <div class="status-grid">
            // Streaming Event Card (full width)
            <div class="card event-card">
                <div class="card-header">
                    <span class="card-title">"Streaming Event"</span>
                    <span class=format!("status-indicator {}", if event.is_some() { "active" } else { "disconnected" })></span>
                </div>
                {match event {
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
                }}
            </div>

            // Inpoint Status Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Inpoint"</span>
                    <span class=format!("status-indicator {}", inpoint_class)></span>
                </div>
                <div class="card-value">{inpoint_status}</div>
                <div class="card-label">"RTMP Server Status"</div>
            </div>

            // Buffer Duration Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Buffer"</span>
                    <span class=format!("status-indicator {}", if stats.pending_chunks > 0 { "active" } else { "idle" })></span>
                </div>
                <div class="card-value">{format_duration(stats.buffer_duration_secs)}</div>
                <div class="card-label">"Pending upload time"</div>
            </div>

            // Endpoint Status Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Uploader"</span>
                    <span class=format!("status-indicator {}", endpoint_status.1)></span>
                </div>
                <div class="card-value">{endpoint_status.0}</div>
                <div class="card-label">"S3 Upload Status"</div>
            </div>

            // Chunk Statistics Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Chunks"</span>
                </div>
                <div class="card-value">{stats.total_chunks}</div>
                <div class="card-label">
                    {format!("{} pending, {} sent", stats.pending_chunks, stats.sent_chunks)}
                </div>
            </div>

            // Total Data Card
            <div class="card">
                <div class="card-header">
                    <span class="card-title">"Total Data"</span>
                </div>
                <div class="card-value">{format_bytes(stats.total_bytes)}</div>
                <div class="card-label">"Total chunk data received"</div>
            </div>
        </div>
    }
}
