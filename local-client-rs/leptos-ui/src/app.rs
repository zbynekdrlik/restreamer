//! Main application component.

use leptos::prelude::*;

use crate::api::{self, StatusResponse};
use crate::components::{ChunkList, Dashboard, Endpoints, Events, LogViewer, Schedules};

/// Main application component.
#[component]
pub fn App() -> impl IntoView {
    // Status signal that gets updated by polling
    let (status, set_status) = signal::<Option<Result<StatusResponse, String>>>(None);

    // Initial fetch
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            let result = api::get_status().await;
            set_status.set(Some(result));
        });
    });

    // Start polling timer
    Effect::new(move |_| {
        use gloo_timers::callback::Interval;
        let interval = Interval::new(3000, move || {
            leptos::task::spawn_local(async move {
                let result = api::get_status().await;
                set_status.set(Some(result));
            });
        });
        // Keep the interval alive
        std::mem::forget(interval);
    });

    // Active tab state
    let (active_tab, set_active_tab) = signal("dashboard");

    let tabs = vec![
        ("dashboard", "Dashboard"),
        ("events", "Events"),
        ("endpoints", "Endpoints"),
        ("schedules", "Schedules"),
        ("logs", "Logs"),
    ];

    view! {
        <div class="app">
            <header>
                <h1>"Restreamer"</h1>
                <span class="version">"v" {env!("CARGO_PKG_VERSION")}</span>
            </header>

            <div class="tabs">
                {tabs.into_iter().map(|(id, label)| {
                    view! {
                        <button
                            class=move || if active_tab.get() == id { "tab active" } else { "tab" }
                            on:click=move |_| set_active_tab.set(id)
                        >
                            {label}
                        </button>
                    }
                }).collect_view()}
            </div>

            {move || {
                match status.get() {
                    None => {
                        view! { <div class="loading">"Loading..."</div> }.into_any()
                    }
                    Some(Ok(data)) => {
                        view! {
                            <div class="tab-content">
                                <Show when=move || active_tab.get() == "dashboard">
                                    <DashboardView status=data.clone() />
                                </Show>
                                <Show when=move || active_tab.get() == "events">
                                    <Events />
                                </Show>
                                <Show when=move || active_tab.get() == "endpoints">
                                    <Endpoints />
                                </Show>
                                <Show when=move || active_tab.get() == "schedules">
                                    <Schedules />
                                </Show>
                                <Show when=move || active_tab.get() == "logs">
                                    <LogViewer />
                                </Show>
                            </div>
                        }.into_any()
                    }
                    Some(Err(e)) => {
                        view! {
                            <div class="error-message">
                                {format!("Error: {}", e)}
                            </div>
                        }.into_any()
                    }
                }
            }}
        </div>
    }
}

/// Dashboard view with status cards and chunk list.
#[component]
fn DashboardView(status: StatusResponse) -> impl IntoView {
    view! {
        <Dashboard status=status.clone() />
        <ChunkList stats=status.chunk_stats />
    }
}
