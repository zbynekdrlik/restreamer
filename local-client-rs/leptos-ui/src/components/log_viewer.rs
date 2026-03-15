//! Log viewer component reading from the global store.

use leptos::prelude::*;

use crate::api::{self, LogEntry};
use crate::store::DashboardStore;

/// Log viewer component with component filter dropdown.
#[component]
pub fn LogsView() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");
    let (loading, set_loading) = signal(true);
    let (error, set_error) = signal::<Option<String>>(None);

    // Fetch logs when component changes
    Effect::new(move |_| {
        let comp = store.log_component.get();
        set_loading.set(true);
        leptos::task::spawn_local(async move {
            match api::get_logs(&comp, 100).await {
                Ok(entries) => {
                    store.logs.set(entries);
                    set_error.set(None);
                }
                Err(e) => set_error.set(Some(e)),
            }
            set_loading.set(false);
        });
    });

    view! {
        <div class="card">
            <div class="card-header">
                <h3 class="section-title">"Service Logs"</h3>
                <select
                    on:change=move |ev| {
                        let target: web_sys::HtmlSelectElement = event_target(&ev);
                        store.log_component.set(target.value());
                    }
                    style="padding: 8px; border-radius: 4px; background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--bg-card);"
                >
                    <option value="rs_inpoint" selected=move || store.log_component.get() == "rs_inpoint">"Inpoint"</option>
                    <option value="rs_endpoint" selected=move || store.log_component.get() == "rs_endpoint">"Endpoint"</option>
                    <option value="rs_runtime" selected=move || store.log_component.get() == "rs_runtime">"Runtime"</option>
                </select>
            </div>

            {move || {
                if loading.get() {
                    return view! { <div class="loading">"Loading logs..."</div> }.into_any();
                }
                if let Some(e) = error.get() {
                    return view! {
                        <div class="error-message">{format!("Error loading logs: {}", e)}</div>
                    }.into_any();
                }
                let entries = store.logs.get();
                if entries.is_empty() {
                    view! {
                        <div class="log-viewer">
                            <div style="color: var(--text-secondary); text-align: center; padding: 20px;">
                                "No logs available for this component"
                            </div>
                        </div>
                    }.into_any()
                } else {
                    view! {
                        <div class="log-viewer">
                            <For
                                each=move || store.logs.get()
                                key=|entry| format!("{}-{}-{}", entry.level, entry.target, entry.message)
                                children=move |entry: LogEntry| {
                                    let level = entry.level.clone();
                                    let level_class = format!("log-level {}", level);
                                    view! {
                                        <div class="log-entry">
                                            <span class=level_class>
                                                {level}
                                            </span>
                                            <span class="log-target">{entry.target}</span>
                                            <span>{entry.message}</span>
                                        </div>
                                    }
                                }
                            />
                        </div>
                    }.into_any()
                }
            }}
        </div>
    }
}
