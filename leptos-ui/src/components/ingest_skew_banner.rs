//! Dedicated banner for ingest-side A/V desync at the SOURCE (OBS) (#354).
//!
//! When OBS feeds video and audio desynced past `inpoint.skew_threshold_ms`,
//! every delivery endpoint skew-kills in a loop and the audience sees a frozen
//! / stuttering stream. The old dashboard only showed a generic "upstream
//! outage" banner and per-endpoint reconnect counters — nothing named the
//! cause or the remedy. This banner does both, in plain Slovak, so the
//! operator restarts OBS immediately instead of diagnosing by luck.
//!
//! Driven by `store.ingest_skew_active` (latched over-threshold flag) +
//! `store.ingest_skew_ms` (the live skew, for the "~N s" number), both
//! refreshed every 2s from the `/api/v1/status` poll.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn IngestSkewBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let active = store.ingest_skew_active;
    let skew_ms = store.ingest_skew_ms;

    // Round the live skew to whole seconds for the operator-facing message.
    let secs = Memo::new(move |_| ((skew_ms.get().abs() as f64) / 1000.0).round() as i64);

    view! {
        <Show when=move || active.get()>
            <div class="banner banner--critical" role="alert" data-testid="ingest-skew-banner">
                {move || {
                    format!(
                        "\u{1F534} Zvuk a obraz z OBS sú rozídené o ~{} s — reštartuj stream v OBS. \
                         Kým to platí, delivery sa nedá spustiť (každý cieľ by spadol).",
                        secs.get()
                    )
                }}
            </div>
        </Show>
    }
}
