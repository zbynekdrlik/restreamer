//! Loud banner shown when delivery is active but 0 endpoints are running.
//!
//! This is the highest-priority alarm on the dashboard: it means the
//! audience sees nothing while the operator might not realise because the
//! pipeline still appears healthy (RTMP green, buffer filling, VPS up).

use crate::store::{DashboardStore, zero_endpoint_alarm};
use leptos::prelude::*;

#[component]
pub fn ZeroEndpointBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let pipeline = store.pipeline_state;
    let delivery = store.delivery;

    // Show when the pipeline is active (a KNOWN, non-idle/stopping state) AND
    // the delivery layer has zero live endpoints. During pure "idle" — and on
    // a fresh load, before the first WS tick populates the pipeline state — we
    // stay silent (see `zero_endpoint_alarm`, the shared predicate the
    // app-level glow uses too).
    let show = Memo::new(move |_| {
        let ps = pipeline.get();
        let d = delivery.get();
        zero_endpoint_alarm(&ps, &d)
    });

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--critical" role="alert">
                {"\u{26A0} Delivery is active but 0 endpoints are running. Audience sees nothing."}
            </div>
        </Show>
    }
}
