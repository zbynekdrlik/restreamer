//! App-level unified "any issue" red edge-glow overlay (#73).
//!
//! The dashboard already flags every issue condition, but only PIECEMEAL and
//! only inside its own scroll area — per-endpoint red cards
//! (`EndpointLifecycle::Attention`) plus four independent banners
//! (`ZeroEndpointBanner`, `DiskPressureBanner` critical, `IngestSkewBanner`,
//! `S3RegionBanner`). There is no single, always-in-view "something is wrong"
//! aggregate cue. On 2026-06-19 the operator missed a total endpoint failure
//! because they were watching the stream, not scanning the dashboard for one
//! of several separate red elements.
//!
//! This is the missing aggregate signal: a fixed, full-viewport,
//! click-through (`pointer-events:none`) overlay carrying an inset red halo
//! that pulses around the viewport border whenever ANY of the conditions
//! below is active. It invents NO new backend signal — it ORs existing
//! `DashboardStore` signals through the shared predicates in
//! [`crate::store`], so it stays a true mirror of what the dashboard already
//! shows and is fully drivable through the scenario mock harness.
//!
//! It fires on exactly the five conditions the dashboard already renders red:
//!   1. any endpoint in `Attention` (`any_attention`),
//!   2. delivery active with zero endpoints (`zero_endpoint_alarm`),
//!   3. local disk `critical`,
//!   4. ingest A/V skew latched over threshold,
//!   5. a non-standard S3 region.
//!
//! Deliberately EXCLUDED, to stay consistent with the existing semaphore:
//! survivable auto-recovery states (`Buffering`/`Rescue`/`Recovering`, incl.
//! the `buffer_exhausted` badge that coincides with Rescue) are calm/blue —
//! "protected, recovering, no action needed" (the `OutageBanner` philosophy)
//! — so the red glow never fires on them. The early disk WARN (80%) and a
//! degraded YouTube health badge are likewise not red-critical here.

use crate::store::{DashboardStore, any_attention, zero_endpoint_alarm};
use leptos::prelude::*;

#[component]
pub fn AlertGlow() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let delivery = store.delivery;
    let pipeline = store.pipeline_state;
    let disk = store.disk_pressure;
    let skew_active = store.ingest_skew_active;
    let s3_standard = store.s3_region_standard;

    // Aggregate of exactly the conditions the dashboard ALREADY renders red,
    // via the shared predicates so it can never drift from the banners/cards.
    // Every signal is read (via `.get()`) INSIDE this closure, so the memo
    // re-evaluates when any of them changes.
    let any_issue = Memo::new(move |_| {
        let d = delivery.get();
        let ps = pipeline.get();

        let attention = any_attention(&d);
        let zero_endpoint = zero_endpoint_alarm(&ps, &d);
        let disk_critical = disk.get() == "critical";
        let skew = skew_active.get();
        let s3_bad = !s3_standard.get();

        attention || zero_endpoint || disk_critical || skew || s3_bad
    });

    view! {
        <Show when=move || any_issue.get()>
            <div class="alert-glow" data-testid="alert-glow" aria-hidden="true"></div>
        </Show>
    }
}
