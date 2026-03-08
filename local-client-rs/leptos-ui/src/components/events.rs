//! Events management component.

use leptos::prelude::*;

use crate::api;

/// Events tab: list, create, activate/deactivate streaming events.
#[component]
pub fn Events() -> impl IntoView {
    let (events, set_events) = signal::<Vec<api::StreamingEvent>>(Vec::new());
    let (error, set_error) = signal::<Option<String>>(None);
    let (new_name, set_new_name) = signal(String::new());

    // Fetch events on mount
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match api::list_events().await {
                Ok(evts) => set_events.set(evts),
                Err(e) => set_error.set(Some(e)),
            }
        });
    });

    let on_create = move |_| {
        let name = new_name.get();
        if name.is_empty() {
            return;
        }
        leptos::task::spawn_local(async move {
            match api::create_event(&name).await {
                Ok(_) => {
                    set_new_name.set(String::new());
                    if let Ok(evts) = api::list_events().await {
                        set_events.set(evts);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    view! {
        <div class="events-tab">
            <h2>"Streaming Events"</h2>

            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}

            <div class="create-form">
                <input
                    type="text"
                    placeholder="Event identifier..."
                    prop:value=move || new_name.get()
                    on:input=move |ev| set_new_name.set(event_target_value(&ev))
                />
                <button on:click=on_create>"Create Event"</button>
            </div>

            <div class="event-list">
                {move || events.get().into_iter().map(|evt| {
                    let id = evt.id;
                    let identifier = evt.identifier.clone().unwrap_or_default();
                    let desc = evt.short_description.clone().unwrap_or_default();
                    let receiving = evt.receiving_activated;
                    let delivering = evt.delivering_activated;

                    view! {
                        <div class="event-card">
                            <div class="event-header">
                                <strong>{identifier}</strong>
                                <span class="event-desc">{desc}</span>
                            </div>
                            <div class="event-status">
                                <span class=move || if receiving { "badge active" } else { "badge" }>
                                    {if receiving { "Receiving" } else { "Idle" }}
                                </span>
                                <span class=move || if delivering { "badge active" } else { "badge" }>
                                    {if delivering { "Delivering" } else { "Stopped" }}
                                </span>
                            </div>
                            <div class="event-actions">
                                <button on:click=move |_| {
                                    leptos::task::spawn_local(async move {
                                        let _ = api::activate_event(id).await;
                                        if let Ok(evts) = api::list_events().await {
                                            set_events.set(evts);
                                        }
                                    });
                                }>"Activate"</button>
                                <button on:click=move |_| {
                                    leptos::task::spawn_local(async move {
                                        let _ = api::start_delivering(id).await;
                                        if let Ok(evts) = api::list_events().await {
                                            set_events.set(evts);
                                        }
                                    });
                                }>"Start Delivering"</button>
                                <button class="danger" on:click=move |_| {
                                    leptos::task::spawn_local(async move {
                                        let _ = api::deactivate_event(id).await;
                                        if let Ok(evts) = api::list_events().await {
                                            set_events.set(evts);
                                        }
                                    });
                                }>"Deactivate"</button>
                            </div>
                        </div>
                    }
                }).collect_view()}
            </div>
        </div>
    }
}
