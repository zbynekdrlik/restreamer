//! Events management component.

use leptos::prelude::*;

use crate::api;

/// Events tab: list, create, activate/deactivate streaming events with endpoint assignment.
#[component]
pub fn Events() -> impl IntoView {
    let (events, set_events) = signal::<Vec<api::StreamingEvent>>(Vec::new());
    let (all_endpoints, set_all_endpoints) = signal::<Vec<api::EndpointConfig>>(Vec::new());
    let (error, set_error) = signal::<Option<String>>(None);
    let (new_name, set_new_name) = signal(String::new());

    // Fetch events and endpoints on mount
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match api::list_events().await {
                Ok(evts) => set_events.set(evts),
                Err(e) => set_error.set(Some(e)),
            }
            match api::list_endpoints().await {
                Ok(eps) => set_all_endpoints.set(eps),
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
                    placeholder="Event name..."
                    prop:value=move || new_name.get()
                    on:input=move |ev| set_new_name.set(event_target_value(&ev))
                />
                <button on:click=on_create>"Create Event"</button>
            </div>

            <div class="event-list">
                {move || events.get().into_iter().map(|evt| {
                    let id = evt.id;
                    let name = evt.name.clone();
                    let receiving = evt.receiving_activated;
                    let delivering = evt.delivering_activated;

                    view! {
                        <div class="event-card">
                            <div class="event-header">
                                <strong>{name}</strong>
                            </div>
                            <div class="event-status">
                                <span class=move || if receiving { "badge active" } else { "badge" }>
                                    {if receiving { "Receiving" } else { "Idle" }}
                                </span>
                                <span class=move || if delivering { "badge active" } else { "badge" }>
                                    {if delivering { "Delivering" } else { "Stopped" }}
                                </span>
                            </div>
                            <EventEndpoints event_id=id all_endpoints=all_endpoints set_error=set_error />
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

/// Endpoint assignment UI within an event card.
#[component]
fn EventEndpoints(
    event_id: i64,
    all_endpoints: ReadSignal<Vec<api::EndpointConfig>>,
    set_error: WriteSignal<Option<String>>,
) -> impl IntoView {
    let (assigned, set_assigned) = signal::<Vec<api::EndpointConfig>>(Vec::new());
    let (selected_ep, set_selected_ep) = signal(String::new());

    // Fetch assigned endpoints on mount
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match api::get_event_endpoints(event_id).await {
                Ok(eps) => set_assigned.set(eps),
                Err(e) => set_error.set(Some(e)),
            }
        });
    });

    let on_assign = move |_| {
        let ep_id_str = selected_ep.get();
        if ep_id_str.is_empty() {
            return;
        }
        let ep_id: i64 = match ep_id_str.parse() {
            Ok(id) => id,
            Err(_) => return,
        };
        leptos::task::spawn_local(async move {
            match api::attach_endpoint(event_id, ep_id).await {
                Ok(_) => {
                    set_selected_ep.set(String::new());
                    if let Ok(eps) = api::get_event_endpoints(event_id).await {
                        set_assigned.set(eps);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    view! {
        <div class="assigned-endpoints">
            <div class="assigned-label">"Assigned Endpoints:"</div>
            <div class="assigned-list">
                {move || {
                    let eps = assigned.get();
                    if eps.is_empty() {
                        view! { <span class="empty-inline">"None"</span> }.into_any()
                    } else {
                        eps.into_iter().map(|ep| {
                            let ep_id = ep.id;
                            let alias = ep.alias.clone();
                            let stype = ep.service_type.clone();
                            view! {
                                <span class="assigned-ep">
                                    <span class="service-badge">{stype}</span>
                                    {alias}
                                    <button class="remove-ep" on:click=move |_| {
                                        leptos::task::spawn_local(async move {
                                            let _ = api::detach_endpoint(event_id, ep_id).await;
                                            if let Ok(eps) = api::get_event_endpoints(event_id).await {
                                                set_assigned.set(eps);
                                            }
                                        });
                                    }>"x"</button>
                                </span>
                            }
                        }).collect_view().into_any()
                    }
                }}
            </div>
            <div class="assign-form">
                <select
                    prop:value=move || selected_ep.get()
                    on:change=move |ev| set_selected_ep.set(event_target_value(&ev))
                >
                    <option value="">"-- Assign endpoint --"</option>
                    {move || {
                        let assigned_ids: Vec<i64> = assigned.get().iter().map(|e| e.id).collect();
                        all_endpoints.get().into_iter()
                            .filter(move |ep| !assigned_ids.contains(&ep.id))
                            .map(|ep| {
                                let val = ep.id.to_string();
                                let label = format!("{} ({})", ep.alias, ep.service_type);
                                view! { <option value={val}>{label}</option> }
                            }).collect_view()
                    }}
                </select>
                <button on:click=on_assign>"Assign"</button>
            </div>
        </div>
    }
}
