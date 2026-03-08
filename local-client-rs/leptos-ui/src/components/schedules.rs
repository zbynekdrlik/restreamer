//! Scheduled streams management component.

use leptos::prelude::*;

use crate::api;

/// Schedules tab: list, create, edit recurring stream schedules.
#[component]
pub fn Schedules() -> impl IntoView {
    let (schedules, set_schedules) = signal::<Vec<api::ScheduledStream>>(Vec::new());
    let (error, set_error) = signal::<Option<String>>(None);

    // Fetch on mount
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match api::list_schedules().await {
                Ok(sched) => set_schedules.set(sched),
                Err(e) => set_error.set(Some(e)),
            }
        });
    });

    view! {
        <div class="schedules-tab">
            <h2>"Scheduled Streams"</h2>

            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}

            <div class="schedule-list">
                {move || {
                    let scheds = schedules.get();
                    if scheds.is_empty() {
                        view! { <p class="empty">"No scheduled streams configured."</p> }.into_any()
                    } else {
                        scheds.into_iter().map(|s| {
                            let id = s.id;
                            let enabled = s.enabled;
                            view! {
                                <div class="schedule-card">
                                    <div class="schedule-info">
                                        <span>"Event #" {s.event_id}</span>
                                        <span>" | Start: " {s.start_time.clone()}</span>
                                        {s.repeat_interval.clone().map(|ri| view! {
                                            <span class="badge">{ri}</span>
                                        })}
                                        <span class=move || if enabled { "badge active" } else { "badge" }>
                                            {if enabled { "Enabled" } else { "Disabled" }}
                                        </span>
                                    </div>
                                    {s.next_run_at.clone().map(|next| view! {
                                        <div class="next-run">"Next: " {next}</div>
                                    })}
                                    <div class="schedule-actions">
                                        <button class="danger" on:click=move |_| {
                                            leptos::task::spawn_local(async move {
                                                let _ = api::delete_schedule(id).await;
                                                if let Ok(sched) = api::list_schedules().await {
                                                    set_schedules.set(sched);
                                                }
                                            });
                                        }>"Delete"</button>
                                    </div>
                                </div>
                            }
                        }).collect_view().into_any()
                    }
                }}
            </div>
        </div>
    }
}
