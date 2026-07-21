//! Live audit-log panel.
//!
//! Listens to `DashboardStore.audit_feed` (fed by `WsEvent::AuditAppended`)
//! and renders the 50 most recent entries, newest first, with a source
//! filter dropdown and a "Group bursts" toggle (#169).
//!
//! On mount, backfills the feed with the 50 most recent rows from
//! `GET /api/v1/audit` so operators see historical context immediately
//! — not just rows that arrive AFTER the WebSocket connects (that was
//! the 2026-04-20 "empty panel on page load" bug).
//!
//! #169: repeat-burst noise (e.g. 25 `endpoint_rtmp_push_died` rows in
//! 5 min) collapses into one row showing the count and the first→last span.
//! Grouping is done CLIENT-SIDE over the live in-memory feed so that rows
//! arriving via the WebSocket collapse too — a server-only `?group=true`
//! (see `audit_handlers`) would only group the mount-time backfill and leave
//! subsequent live rows ungrouped. The canonical grouping logic lives in
//! `rs_core::db::audit::group_audit_rows`; this is its wasm-side mirror (the UI
//! crate targets wasm32 and cannot depend on the native rs-core).

use crate::api::fetch_recent_audit;
use crate::store::{AuditEntry, DashboardStore};
use leptos::prelude::*;

/// A run of consecutive same-`(source, action, endpoint)` rows collapsed into
/// one. `count == 1` renders identically to an ungrouped row.
#[derive(Clone, PartialEq)]
struct GroupedRow {
    /// The newest row in the run — carries identity, severity, and the detail
    /// shown on drill-down.
    rep: AuditEntry,
    count: u32,
    /// Oldest ts in the run.
    first_ts: String,
    /// Newest ts in the run (== `rep.ts`).
    last_ts: String,
}

/// Epoch milliseconds for an ISO-8601 ts, or `None` if unparseable.
fn ts_millis(ts: &str) -> Option<f64> {
    let t = js_sys::Date::new(&wasm_bindgen::JsValue::from_str(ts)).get_time();
    if t.is_nan() { None } else { Some(t) }
}

/// Browser-local `hh:mm:ss` for an ISO-8601 UTC ts (matches the #50/#153 fix:
/// render through `js_sys::Date` in the operator's timezone, never raw UTC).
fn local_hms(ts: &str) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_str(ts));
    if date.get_time().is_nan() {
        ts.split('T')
            .nth(1)
            .unwrap_or(ts)
            .split('.')
            .next()
            .unwrap_or("")
            .to_string()
    } else {
        format!(
            "{:02}:{:02}:{:02}",
            date.get_hours(),
            date.get_minutes(),
            date.get_seconds()
        )
    }
}

/// Collapse a newest-first list of rows into grouped runs. Mirror of
/// `rs_core::db::audit::group_audit_rows`: a row joins the most-recent open
/// group of the same `(source, action, endpoint)` key when it is within
/// `window_secs` of that group's oldest row. `window_secs == 0` disables
/// grouping (every row becomes a singleton), which is how the toggle-off state
/// reproduces the exact ungrouped feed.
fn group_entries(rows: Vec<AuditEntry>, window_secs: i64) -> Vec<GroupedRow> {
    let mut out: Vec<GroupedRow> = Vec::new();
    for r in rows {
        if window_secs > 0 {
            if let Some(g) = out.iter_mut().rev().find(|g| {
                g.rep.source == r.source && g.rep.action == r.action && g.rep.endpoint == r.endpoint
            }) {
                if let (Some(oldest), Some(rt)) = (ts_millis(&g.first_ts), ts_millis(&r.ts)) {
                    if ((oldest - rt).abs() / 1000.0) <= window_secs as f64 {
                        g.count = g.count.saturating_add(1);
                        // Input is newest-first, so `r` is the oldest so far.
                        g.first_ts = r.ts.clone();
                        continue;
                    }
                }
            }
        }
        out.push(GroupedRow {
            first_ts: r.ts.clone(),
            last_ts: r.ts.clone(),
            count: 1,
            rep: r,
        });
    }
    out
}

/// Grouping window in seconds when "Group bursts" is on.
const GROUP_WINDOW_SECS: i64 = 60;

#[component]
pub fn AuditPanel() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let feed = store.audit_feed;
    let (filter_source, set_filter_source) = signal::<Option<String>>(None);
    // #169: default ON — the burst noise is the common case operators want hidden.
    let (group_bursts, set_group_bursts) = signal(true);

    // One-shot backfill on mount. The WebSocket handler de-duplicates by
    // `id`, so rows that arrive both via backfill and via a subsequent
    // `AuditAppended` broadcast won't appear twice.
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match fetch_recent_audit(50).await {
                Ok(rows) => {
                    let backfill: Vec<AuditEntry> = rows
                        .into_iter()
                        .map(|r| AuditEntry {
                            id: r.id,
                            ts: r.ts,
                            severity: r.severity,
                            source: r.source,
                            event_id: r.event_id,
                            instance_id: r.instance_id,
                            endpoint: r.endpoint,
                            action: r.action,
                            detail: r.detail,
                        })
                        // Dashboard renders newest-first (reversed), so
                        // feed storage order is oldest-first.
                        .rev()
                        .collect();
                    feed.update(|f| {
                        let existing_ids: std::collections::HashSet<i64> =
                            f.iter().map(|e| e.id).collect();
                        for e in backfill {
                            if !existing_ids.contains(&e.id) {
                                f.push(e);
                            }
                        }
                        // Cap at 200 so repeated backfills (e.g. on
                        // reconnect) don't grow unbounded.
                        while f.len() > 200 {
                            f.remove(0);
                        }
                    });
                }
                Err(e) => {
                    leptos::logging::warn!("audit_panel backfill failed: {e}");
                }
            }
        });
    });

    // Newest-first, source-filtered, then grouped (window 0 = ungrouped feed).
    let visible = Memo::new(move |_| {
        let src = filter_source.get();
        let window = if group_bursts.get() {
            GROUP_WINDOW_SECS
        } else {
            0
        };
        let filtered: Vec<AuditEntry> = feed
            .get()
            .into_iter()
            .rev()
            .filter(|e| src.as_deref().is_none_or(|s| e.source == s))
            .take(50)
            .collect();
        group_entries(filtered, window)
    });

    view! {
        <div class="audit-panel">
            <header class="audit-panel__header">
                <h3>"Activity"</h3>
                <label class="audit-panel__group-toggle">
                    <input
                        type="checkbox"
                        prop:checked=move || group_bursts.get()
                        on:change=move |ev| set_group_bursts.set(event_target_checked(&ev))
                    />
                    "Group bursts"
                </label>
                <select
                    class="audit-panel__filter"
                    on:change=move |ev| {
                        let v = event_target_value(&ev);
                        set_filter_source.set(if v == "all" { None } else { Some(v) });
                    }
                >
                    <option value="all">"all sources"</option>
                    <option value="operator">"operator"</option>
                    <option value="inpoint">"inpoint"</option>
                    <option value="uploader">"uploader"</option>
                    <option value="delivery">"delivery"</option>
                    <option value="vps">"vps"</option>
                    <option value="ffmpeg">"ffmpeg"</option>
                    <option value="s3">"s3"</option>
                    <option value="system">"system"</option>
                </select>
            </header>
            <ul class="audit-panel__list">
                <For
                    each=move || visible.get()
                    key=|g| (g.rep.id, g.count)
                    children=move |g: GroupedRow| {
                        let e = g.rep;
                        let sev_class = format!("audit-row audit-row--{}", e.severity);
                        let grouped = g.count > 1;
                        // Grouped: show the first→last local-time span; else the
                        // single row's local time.
                        let time = if grouped {
                            format!(
                                "{} \u{2014} {}",
                                local_hms(&g.first_ts),
                                local_hms(&g.last_ts)
                            )
                        } else {
                            local_hms(&g.last_ts)
                        };
                        let endpoint = e.endpoint.clone().unwrap_or_default();
                        let has_endpoint = !endpoint.is_empty();
                        let count = g.count;
                        let detail_json =
                            serde_json::to_string_pretty(&e.detail).unwrap_or_default();
                        view! {
                            <li class=sev_class>
                                <span class="audit-row__time">{time}</span>
                                <span class="audit-row__source">{e.source.clone()}</span>
                                <Show when=move || grouped>
                                    <span class="audit-row__count">{count}"\u{00d7}"</span>
                                </Show>
                                <span class="audit-row__action">{e.action.clone()}</span>
                                <Show when=move || has_endpoint>
                                    <span class="audit-row__endpoint">{endpoint.clone()}</span>
                                </Show>
                                <details class="audit-row__detail">
                                    <summary>"detail"</summary>
                                    <pre>{detail_json.clone()}</pre>
                                </details>
                            </li>
                        }
                    }
                />
            </ul>
        </div>
    }
}
