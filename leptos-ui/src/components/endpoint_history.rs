//! Sparkline of recent per-endpoint `chunk_delay_secs` samples.
//!
//! Reads from `DashboardStore.endpoint_metrics_history`, which is appended
//! by `WsEvent::MetricsSample` for each endpoint. The chart is a simple
//! SVG polyline — no external charting dependency.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn EndpointHistory(#[prop(into)] alias: Signal<String>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let history = store.endpoint_metrics_history;

    let series = Memo::new(move |_| {
        let alias = alias.get();
        let h = history.get();
        h.get(&alias).cloned().unwrap_or_default()
    });

    view! {
        <div class="endpoint-history">
            <h4>"chunk_delay over recent samples"</h4>
            {move || {
                let pts = series.get();
                if pts.is_empty() {
                    return view! { <p>"no data yet"</p> }.into_any();
                }
                let w = 300.0_f64;
                let h = 60.0_f64;
                let n = pts.len().max(2) as f64;
                let max = pts
                    .iter()
                    .map(|p| p.chunk_delay_secs)
                    .fold(1.0_f64, f64::max);
                let path: String = pts
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let x = (i as f64) * (w / (n - 1.0));
                        let y = h - (p.chunk_delay_secs / max * h);
                        format!("{} {x:.1},{y:.1}", if i == 0 { "M" } else { "L" })
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                view! {
                    <svg
                        width=w.to_string()
                        height=h.to_string()
                        style="background:#111;border-radius:3px"
                    >
                        <path d=path stroke="#4caf50" stroke-width="1.2" fill="none"/>
                    </svg>
                }
                .into_any()
            }}
        </div>
    }
}
