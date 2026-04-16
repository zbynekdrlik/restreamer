//! `/uploads` drill-down page — per-chunk S3 upload telemetry view.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::{self, UploadRow};

#[component]
pub fn UploadsView() -> impl IntoView {
    let rows: RwSignal<Vec<UploadRow>> = RwSignal::new(Vec::new());
    let filter_errors = RwSignal::new(false);

    // Initial fetch + 2s poll loop
    let refresh = move || {
        spawn_local(async move {
            if let Ok(r) = api::fetch_recent_uploads(200).await {
                rows.set(r);
            }
        });
    };
    refresh();
    let _interval = Interval::new(2_000, refresh);
    std::mem::forget(_interval);

    view! {
        <div class="uploads-page">
            <h1>"Uploads"</h1>
            <label class="uploads-filter">
                <input type="checkbox"
                       prop:checked=move || filter_errors.get()
                       on:change=move |ev| {
                           filter_errors.set(event_target_checked(&ev));
                       } />
                " Errors only"
            </label>
            <table class="uploads-table">
                <thead>
                    <tr>
                        <th>"id"</th>
                        <th>"event"</th>
                        <th>"seq"</th>
                        <th>"size"</th>
                        <th>"attempts"</th>
                        <th>"duration"</th>
                        <th>"status"</th>
                        <th>"error"</th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        let errs_only = filter_errors.get();
                        rows.get()
                            .into_iter()
                            .filter(|r| !errs_only
                                    || r.last_error.is_some()
                                    || r.status == "failed")
                            .map(|r| view! {
                                <tr class=format!("uploads-row uploads-row--{}", r.status)>
                                    <td>{r.chunk_id}</td>
                                    <td>{r.event_identifier.clone()}</td>
                                    <td>{r.sequence_number}</td>
                                    <td>{r.size_bytes}</td>
                                    <td>{r.attempts}</td>
                                    <td>{r.duration_ms.map(|d| format!("{d}ms"))
                                                       .unwrap_or_default()}</td>
                                    <td>{r.status.clone()}</td>
                                    <td>{r.last_error.unwrap_or_default()}</td>
                                </tr>
                            })
                            .collect_view()
                    }}
                </tbody>
            </table>
        </div>
    }
}
