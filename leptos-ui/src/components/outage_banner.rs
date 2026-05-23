//! Calm, single banner shown when any endpoint is in a survivable
//! auto-recovery state (Buffering/Rescue/Recovering). Replaces the wall of
//! red cards: the operator sees "protected, recovering, no action needed".

use crate::store::{DashboardStore, EndpointLifecycle};
use leptos::prelude::*;

#[component]
pub fn OutageBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let delivery = store.delivery;

    let recovering = Memo::new(move |_| {
        delivery.get().endpoints.iter().any(|e| {
            matches!(
                e.lifecycle,
                EndpointLifecycle::Buffering
                    | EndpointLifecycle::Rescue
                    | EndpointLifecycle::Recovering
            )
        })
    });
    // Only show when NO endpoint needs attention (attention has its own red).
    let any_attention = Memo::new(move |_| {
        delivery
            .get()
            .endpoints
            .iter()
            .any(|e| e.lifecycle == EndpointLifecycle::Attention)
    });

    view! {
        <Show when=move || recovering.get() && !any_attention.get()>
            <div class="banner banner--recovering" role="status">
                {"\u{1F6E1} Upstream outage detected \u{2014} all endpoints protected, rescue video live, recovering automatically. No action needed."}
            </div>
        </Show>
    }
}
