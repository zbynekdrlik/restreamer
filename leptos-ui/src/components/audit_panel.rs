//! Live audit-log panel.
//!
//! Listens to `DashboardStore.audit_feed` (fed by `WsEvent::AuditAppended`)
//! and renders the 50 most recent entries, newest first, with a source
//! filter dropdown.

use crate::store::{AuditEntry, DashboardStore};
use leptos::prelude::*;

#[component]
pub fn AuditPanel() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let feed = store.audit_feed;
    let (filter_source, set_filter_source) = signal::<Option<String>>(None);

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
