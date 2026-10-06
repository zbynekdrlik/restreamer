//! Dedicated banner for an unusually long-running delivery (#84).
//!
//! When a single delivery has been running longer than
//! `delivery.long_stream_warn_secs` (default 2.5 h), the stream may have been
//! left on after the event finished ("potentially not finished stream"). The
//! `LongStreamWarning` audit row + Discord ping are the one-shot signals; this
//! banner is the persistent dashboard one, so the operator sees at a glance
//! that a stream is still running long after it should have ended.
//!
//! Driven by `store.long_stream_warning`, refreshed every 2s from the
//! `/api/v1/status` poll. Amber (calm) `banner--warn` — a heads-up, not an
//! outage; delivery is still healthy, it just may need stopping.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn LongStreamBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let warning = store.long_stream_warning;

    view! {
        <Show when=move || warning.get()>
            <div class="banner banner--warn" role="alert" data-testid="long-stream-banner">
                {"\u{23F1}\u{FE0F} Stream beží už veľmi dlho — over, či ho netreba ukončiť (možno zostal omylom zapnutý)."}
            </div>
        </Show>
    }
}
