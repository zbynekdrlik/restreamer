//! Log viewer component for viewing service logs.

use leptos::prelude::*;

use crate::api::{self, LogEntry};

/// Log viewer component.
#[component]
pub fn LogViewer() -> impl IntoView {
    // Selected component filter
    let (component, set_component) = signal("rs_inpoint".to_string());

    // Logs signal
    let (logs, set_logs) = signal::<Option<Result<Vec<LogEntry>, String>>>(None);

    // Fetch logs when component changes
    Effect::new(move |_| {
        let comp = component.get();
        leptos::task::spawn_local(async move {
            let result = api::get_logs(&comp, 100).await;
            set_logs.set(Some(result));
        });
    });

    view! {
        <div class="card">
            <div class="card-header">
                <h3 class="section-title">"Service Logs"</h3>
                <select
                    on:change=move |ev| {
                        let target: web_sys::HtmlSelectElement = event_target(&ev);
                        set_component.set(target.value());
                    }
                    style="padding: 8px; border-radius: 4px; background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--bg-card);"
                >
                    <option value="rs_inpoint" selected=move || component.get() == "rs_inpoint">"Inpoint"</option>
                    <option value="rs_endpoint" selected=move || component.get() == "rs_endpoint">"Endpoint"</option>
                    <option value="rs_runtime" selected=move || component.get() == "rs_runtime">"Runtime"</option>
                </select>
            </div>

            {move || {
                match logs.get() {
                    None => {
                        view! { <div class="loading">"Loading logs..."</div> }.into_any()
                    }
                    Some(Ok(entries)) => {
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
                                        each=move || entries.clone()
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
                    }
                    Some(Err(e)) => {
                        view! {
                            <div class="error-message">{format!("Error loading logs: {}", e)}</div>
                        }.into_any()
                    }
                }
            }}
        </div>
    }
}
