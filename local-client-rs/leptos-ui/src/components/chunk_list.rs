//! Chunk list component showing chunk statistics summary.

use leptos::prelude::*;

use crate::api::{format_bytes, ChunkStats};

/// Chunk list component displaying chunk statistics.
#[component]
pub fn ChunkList(stats: ChunkStats) -> impl IntoView {
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
                    <span>{stats.pending_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status pending">"Pending"</span>
                </div>

                <div class="chunk-row">
                    <span>"Processing"</span>
                    <span>{stats.in_process_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status processing">"Uploading"</span>
                </div>

                <div class="chunk-row">
                    <span>"Sent Chunks"</span>
                    <span>{stats.sent_chunks}</span>
                    <span>"-"</span>
                    <span class="chunk-status sent">"Sent"</span>
                </div>

                <div class="chunk-row" style="font-weight: 600;">
                    <span>"Total"</span>
                    <span>{stats.total_chunks}</span>
                    <span>{format_bytes(stats.total_bytes)}</span>
                    <span>"-"</span>
                </div>
            </div>
        </div>
    }
}
