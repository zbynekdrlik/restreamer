//! Historical time-graph of OUTGOING (S3-upload) throughput in Mbps (#77).
//!
//! Polls `/api/v1/uploads/throughput` every 15 s and draws a dependency-free
//! inline SVG area+line chart of the box's outgoing-to-internet bitrate over
//! (at least) the last 3 hours, so the operator can see how upload behaved in
//! time. Mirrors the dependency-free SVG-polyline pattern already used by
//! `endpoint_history.rs`.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::{self, ThroughputSeries};

/// SVG viewBox geometry. The element itself is CSS-sized to the container
/// width; the viewBox keeps the coordinate math simple.
const VB_W: f64 = 600.0;
const VB_H: f64 = 80.0;

/// The x-axis window: 3 h, matching the server's retention. Samples are
/// placed by their real timestamp within `[t_last - WINDOW_MS, t_last]`.
const WINDOW_MS: f64 = 3.0 * 3600.0 * 1000.0;

#[component]
pub fn MbpsGraph() -> impl IntoView {
    let series: RwSignal<ThroughputSeries> = RwSignal::new(ThroughputSeries::default());

    // Poll on a 15 s cadence (matches the server bucket width) — far slower
    // than the 2 s upload-strip poll, since this payload is a ~720-point
    // series.
    let _interval = Interval::new(15_000, move || {
        spawn_local(async move {
            if let Ok(s) = api::fetch_throughput().await {
                series.set(s);
            }
        });
    });
    std::mem::forget(_interval);

    // One immediate fetch so the graph isn't blank until the first tick.
    spawn_local(async move {
        if let Ok(s) = api::fetch_throughput().await {
            series.set(s);
        }
    });

    // Peak + latest for the header readout.
    let peak = move || {
        series
            .get()
            .samples
            .iter()
            .map(|s| s.mbps)
            .fold(0.0_f64, f64::max)
    };
    let latest = move || series.get().samples.last().map(|s| s.mbps).unwrap_or(0.0);

    view! {
        <div class="mbps-graph" title="Outgoing upload bitrate to the internet over time (last 3h)">
            <div class="mbps-graph__header">
                <span class="mbps-graph__title">"Outgoing to internet (Mbps, 3h)"</span>
                <span class="mbps-graph__peak">
                    {move || format!("now {:.1} · peak {:.1}", latest(), peak())}
                </span>
            </div>
            {move || {
                let s = series.get();
                let pts = s.samples;
                if pts.len() < 2 {
                    return view! {
                        <p class="mbps-graph__empty">"no upload data yet"</p>
                    }
                    .into_any();
                }
                // X is scaled by REAL TIME against a fixed 3 h window ending
                // at the newest sample, so a partial series fills from the
                // right at its true temporal position instead of stretching
                // a few points across the whole width. `t_ms` is therefore
                // load-bearing, not decorative.
                let t_last = pts.last().map(|p| p.t_ms).unwrap_or(0) as f64;
                let t_start = t_last - WINDOW_MS;
                let x_of = |t_ms: i64| {
                    (((t_ms as f64 - t_start) / WINDOW_MS) * VB_W).clamp(0.0, VB_W)
                };
                // Scale Y to the peak, with a 1 Mbps floor so a quiet stream
                // doesn't blow tiny values up to full height.
                let max = pts.iter().map(|p| p.mbps).fold(1.0_f64, f64::max);
                let y_of = |mbps: f64| VB_H - (mbps / max * VB_H);
                let line: String = pts
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        format!(
                            "{} {:.1},{:.1}",
                            if i == 0 { "M" } else { "L" },
                            x_of(p.t_ms),
                            y_of(p.mbps)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // Close the area path down to the baseline under the series.
                let area = format!(
                    "{line} L {:.1},{:.1} L {:.1},{:.1} Z",
                    x_of(pts.last().unwrap().t_ms),
                    VB_H,
                    x_of(pts[0].t_ms),
                    VB_H
                );
                view! {
                    <svg
                        class="mbps-graph__svg"
                        viewBox=format!("0 0 {VB_W} {VB_H}")
                        preserveAspectRatio="none"
                    >
                        <path class="mbps-graph__area" d=area/>
                        <path class="mbps-graph__line" d=line/>
                    </svg>
                }
                .into_any()
            }}
        </div>
    }
}
