//! Live audit-log panel.
//!
//! Listens to `DashboardStore.audit_feed` (fed by `WsEvent::AuditAppended`)
//! and renders the 50 most recent entries, newest first, with a source
//! filter dropdown.
//!
//! On mount, backfills the feed with the 50 most recent rows from
//! `GET /api/v1/audit` so operators see historical context immediately
//! — not just rows that arrive AFTER the WebSocket connects (that was
//! the 2026-04-20 "empty panel on page load" bug).

use crate::api::fetch_recent_audit;
use crate::store::{AuditEntry, DashboardStore};
use leptos::prelude::*;

#[component]
pub fn AuditPanel() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let feed = store.audit_feed;
    let (filter_source, set_filter_source) = signal::<Option<String>>(None);

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

    let visible = Memo::new(move |_| {
        let src = filter_source.get();
        feed.get()
            .into_iter()
            .rev()
            .filter(|e| src.as_deref().is_none_or(|s| e.source == s))
            .take(50)
            .collect::<Vec<_>>()
    });

    view! {
        <div class="audit-panel">
            <header class="audit-panel__header">
                <h3>"Activity"</h3>
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
                    key=|e| e.id
                    children=move |e: AuditEntry| {
                        let sev_class = format!("audit-row audit-row--{}", e.severity);
                        let time = e
                            .ts
                            .split('T')
                            .nth(1)
                            .unwrap_or(&e.ts)
                            .split('.')
                            .next()
                            .unwrap_or("")
                            .to_string();
                        let endpoint = e.endpoint.clone().unwrap_or_default();
                        let has_endpoint = !endpoint.is_empty();
                        view! {
                            <li class=sev_class>
                                <span class="audit-row__time">{time}</span>
                                <span class="audit-row__source">{e.source.clone()}</span>
                                <span class="audit-row__action">{e.action.clone()}</span>
                                <Show when=move || has_endpoint>
                                    <span class="audit-row__endpoint">{endpoint.clone()}</span>
                                </Show>
                                <details class="audit-row__detail">
                                    <summary>"detail"</summary>
                                    <pre>
                                        {serde_json::to_string_pretty(&e.detail).unwrap_or_default()}
                                    </pre>
                                </details>
                            </li>
                        }
                    }
                />
            </ul>
        </div>
    }
}
