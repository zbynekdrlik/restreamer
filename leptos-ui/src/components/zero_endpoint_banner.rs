//! Loud banner shown when delivery is active but 0 endpoints are running.
//!
//! This is the highest-priority alarm on the dashboard: it means the
//! audience sees nothing while the operator might not realise because the
//! pipeline still appears healthy (RTMP green, buffer filling, VPS up).

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn ZeroEndpointBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let pipeline = store.pipeline_state;
    let delivery = store.delivery;

    // Show when the pipeline is active (any non-idle state) AND the
    // delivery layer has zero live endpoints. During pure "idle" we stay
    // silent — that's the expected state before a stream starts.
    let show = Memo::new(move |_| {
        let ps = pipeline.get();
        let d = delivery.get();
        ps.state != "idle" && ps.state != "stopping" && d.endpoints.is_empty()
    });

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--critical" role="alert">
                {"\u{26A0} Delivery is active but 0 endpoints are running. Audience sees nothing."}
            </div>
        </Show>
    }
}
