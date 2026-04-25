//! Pacing diagnostics panel.
//!
//! Fetches the three drift telemetry time-series from
//! `GET /api/v1/diagnostics/pacing` on mount and re-fetches whenever the
//! operator switches events via the dropdown. Chart rendering is out of scope
//! for Phase 1 — this panel provides operator-visible confirmation that the
//! drift instrumentation is collecting data.

use crate::api::{self, PacingResponse};
use crate::store::DashboardStore;
use leptos::prelude::*;

/// Panel that displays pacing diagnostics sample counts for a streaming event.
///
/// Reads the currently selected event from `DashboardStore` and re-fetches
/// reactively whenever the operator switches events.
#[component]
pub fn PacingPanel() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore must be provided");
    let event_id = move || store.selected_event_id.get().unwrap_or(0);

    let data: RwSignal<Option<PacingResponse>> = RwSignal::new(None);
    let error: RwSignal<Option<String>> = RwSignal::new(None);

    // Re-fetch whenever the selected event changes.
    Effect::new(move |_| {
        let id = event_id();
        if id == 0 {
            data.set(None);
            error.set(None);
            return;
        }
        leptos::task::spawn_local(async move {
            match api::fetch_pacing(id, Some(0), None).await {
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
                if event_id() == 0 {
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
                            <p class="pacing-panel__placeholder">"Select an endpoint to see per-endpoint drain rate"</p>
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
