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
//! that pulses around the viewport border whenever ANY of the conditions the
//! dashboard already renders red is active. It renders only when the
//! aggregate `any_issue` memo is true, and invents NO new backend signal —
//! it ORs the existing `DashboardStore` signals, so it is fully drivable
//! through the scenario mock harness and Playwright-testable.
//!
//! Semaphore consistency: survivable auto-recovery states
//! (Rescue/Buffering/Recovering) stay deliberately CALM/blue (the
//! `OutageBanner` philosophy). The red glow fires ONLY on genuine
//! attention/red conditions, so it never red-alarms a state the dashboard
//! intentionally treats as calm.

use crate::store::{DashboardStore, EndpointLifecycle};
use leptos::prelude::*;

#[component]
pub fn AlertGlow() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let delivery = store.delivery;
    let pipeline = store.pipeline_state;
    let disk = store.disk_pressure;
    let skew_active = store.ingest_skew_active;
    let s3_standard = store.s3_region_standard;

    // Aggregate of exactly the conditions the dashboard ALREADY renders red.
    let any_issue = Memo::new(move |_| {
        let d = delivery.get();

        // 1) Any endpoint needs the operator (red per-endpoint node).
        let attention = d
            .endpoints
            .iter()
            .any(|e| e.lifecycle == EndpointLifecycle::Attention);

        // 2) Delivery active but zero endpoints running — mirrors the
        //    ZeroEndpointBanner predicate (highest-priority alarm).
        let ps = pipeline.get();
        let zero_endpoint = ps.state != "idle" && ps.state != "stopping" && d.endpoints.is_empty();

        // 3) Local chunk-store disk critically full (never-drop safety valve).
        let disk_critical = disk.get() == "critical";

        // 4) Ingest-side A/V skew latched over threshold (OBS desync).
        let skew = skew_active.get();

        // 5) Non-standard / degraded S3 region.
        let s3_bad = !s3_standard.get();

        attention || zero_endpoint || disk_critical || skew || s3_bad
    });

    view! {
        <Show when=move || any_issue.get()>
            <div class="alert-glow" data-testid="alert-glow" aria-hidden="true"></div>
        </Show>
    }
}
