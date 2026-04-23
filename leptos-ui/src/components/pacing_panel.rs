//! Pacing diagnostics panel.
//!
//! Fetches the three drift telemetry time-series from
//! `GET /api/v1/diagnostics/pacing` on mount and renders sample counts for
//! each series. Chart rendering is out of scope for Phase 1 — this panel
//! provides operator-visible confirmation that the drift instrumentation is
//! collecting data.

use crate::api::{self, PacingResponse};
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// Panel that displays pacing diagnostics sample counts for a streaming event.
///
/// `event_id` should be the ID of the currently active streaming event.
/// When `event_id` is 0 (no event selected) the panel shows a placeholder.
#[component]
pub fn PacingPanel(event_id: i64) -> impl IntoView {
    let data: RwSignal<Option<PacingResponse>> = RwSignal::new(None);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    // Fetch on mount. event_id is i64 (Copy), no clone needed.
    Effect::new(move |_| {
        if event_id == 0 {
            return;
        }
        spawn_local(async move {
            match api::fetch_pacing(event_id, Some(0), None).await {
                Ok(resp) => {
                    data.set(Some(resp));
                    error.set(None);
                }
                Err(e) => {
                    leptos::logging::warn!("pacing_panel fetch failed: {e}");
                    error.set(Some(e));
                }
            }
        });
    });

    view! {
        <div class="pacing-panel" data-testid="pacing-panel">
            <h3>"Pacing diagnostics"</h3>
            {move || error.get().map(|e| view! {
                <p class="pacing-panel__error">{format!("Error: {e}")}</p>
            })}
            {move || {
                if event_id == 0 {
                    return view! {
                        <p class="pacing-panel__placeholder">"No event selected"</p>
                    }.into_any();
                }
                match data.get() {
                    None => view! {
                        <p class="pacing-panel__loading">"Loading..."</p>
                    }.into_any(),
                    Some(r) => view! {
                        <div class="pacing-series" data-testid="producer-rate">
                            <h4>"Producer rate (ts/wall)"</h4>
                            <p>{format!("{} samples", r.producer_rate.len())}</p>
                        </div>
                        <div class="pacing-series" data-testid="consumer-rate">
                            <h4>"Consumer rate (ffmpeg_time/wall)"</h4>
                            <p>{format!("{} samples", r.consumer_rate.len())}</p>
                        </div>
                        <div class="pacing-series" data-testid="clock-skew">
                            <h4>"Clock skew (ms)"</h4>
                            <p>{format!("{} samples", r.clock_skew.len())}</p>
                        </div>
                    }.into_any(),
                }
            }}
        </div>
    }
}
