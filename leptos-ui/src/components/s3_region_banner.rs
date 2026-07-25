//! Dedicated banner for a non-standard S3 (Hetzner Object Storage) region
//! (#278).
//!
//! A stale per-install `config.json` can silently carry a degraded/wrong
//! `s3.region` across an upgrade — that's exactly what happened on
//! 2026-06-24 (streampp ran a live event on `nbg1`, a known-degraded
//! region, and the fast endpoint starved). The `s3_region_standard` audit
//! Critical row (emitted once at startup) is the log-level signal; this
//! banner is the loud dashboard-level one so an operator can never again
//! miss it while the box is running.
//!
//! Driven by `store.s3_region_standard`, refreshed every 2s from the
//! `/api/v1/status` poll.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn S3RegionBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let standard = store.s3_region_standard;

    let show = Memo::new(move |_| !standard.get());

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--critical" role="alert" data-testid="s3-region-banner">
                {"\u{1F534} S3 storage region is NOT the project standard (fsn1). A stale/degraded region can cause upload failures and starved endpoints. Fix s3.region in Settings."}
            </div>
        </Show>
    }
}
