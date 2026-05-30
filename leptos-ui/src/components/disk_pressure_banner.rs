//! Dedicated banner for local chunk-store disk pressure (#231).
//!
//! The never-drop continuity guarantee buffers chunks on the laptop disk
//! until it fills, so disk pressure is the new safety valve. The audit row
//! and the per-endpoint red wall already signal CRITICAL, but a dedicated
//! banner is clearer than inferring "disk full" from a wall of red — and it
//! surfaces the early WARN (80%) state BEFORE the red wall, while there is
//! still time to act calmly.
//!
//! Driven by `store.disk_pressure` ("ok" | "warn" | "critical"), refreshed
//! every 2s from the `/api/v1/status` poll.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn DiskPressureBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let level = store.disk_pressure;

    let is_critical = Memo::new(move |_| level.get() == "critical");
    // WARN is suppressed once CRITICAL is reached so only one banner shows.
    let is_warn = Memo::new(move |_| level.get() == "warn");

    view! {
        <Show when=move || is_critical.get()>
            <div class="banner banner--critical" role="alert" data-testid="disk-pressure-banner">
                {"\u{1F534} Local disk CRITICALLY full (>=90%). Chunks are still buffered (never dropped), but free space soon or the oldest chunks will be dropped to keep delivering. End the event or clear disk space."}
            </div>
        </Show>
        <Show when=move || is_warn.get()>
            <div class="banner banner--warn" role="status" data-testid="disk-pressure-banner">
                {"\u{1F7E1} Local disk filling up (>=80%). Delivery continues normally and nothing is dropped \u{2014} free some space when convenient."}
            </div>
        </Show>
    }
}
