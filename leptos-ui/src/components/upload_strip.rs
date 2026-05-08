//! Live S3 upload telemetry strip, rendered under the S3→VPS pipeline node.
//!
//! Polls `/api/v1/uploads/stats` every 2s and surfaces the four numbers an
//! operator needs at a glance: chunks/sec, median latency, in-flight vs
//! target concurrency, and rolling error rate. Click anywhere on the strip
//! to drill into the full uploads page.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn UploadStrip() -> impl IntoView {
    let stats: RwSignal<crate::api::UploadStats> = RwSignal::new(Default::default());

    // Poll every 2s — even when idle, the strip should update adaptive_target
    // and in_flight (both default to 0/0 so rendering stays stable).
    let _interval = Interval::new(2_000, move || {
        spawn_local(async move {
            if let Ok(s) = crate::api::fetch_upload_stats().await {
                stats.set(s);
            }
        });
    });
    std::mem::forget(_interval);

    // Fire one immediate fetch so the strip isn't blank for 2s on load.
    spawn_local(async move {
        if let Ok(s) = crate::api::fetch_upload_stats().await {
            stats.set(s);
        }
    });

    let on_click = move |_| {
        if let Some(w) = web_sys::window() {
            let _ = w.location().set_href("/uploads");
        }
    };

    view! {
        <div class="upload-strip" on:click=on_click title="S3 upload telemetry — click for detail">
            <span class="upload-strip__rate">
                {move || format!("Upload: {:.1} c/s", stats.get().chunks_per_sec)}
            </span>
            <span class="upload-strip__median">
                {move || format!("median {}ms", stats.get().median_ms)}
            </span>
            <span class="upload-strip__inflight">
                {move || format!("in-flight {}/{}", stats.get().in_flight, stats.get().adaptive_target)}
            </span>
            <span
                class=move || {
                    format!("upload-strip__state upload-strip__state--{}", stats.get().render.class)
                }
                title=move || stats.get().render.tooltip.clone()
            >
                {move || stats.get().render.label.clone()}
            </span>
        </div>
    }
}
