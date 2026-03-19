//! Settings view for configuring events, endpoints, and stream settings.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

/// Settings page with events, endpoints, and stream config.
#[component]
pub fn SettingsView() -> impl IntoView {
    view! {
        <div class="settings-page">
            <h2>"Settings"</h2>
            <EventsSection />
            <crate::components::EndpointsView />
        </div>
    }
}

/// Events management section.
#[component]
fn EventsSection() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let new_name = RwSignal::new(String::new());
    let loading = RwSignal::new(false);

    let on_create = move |_| {
        let name = new_name.get();
        if name.trim().is_empty() {
            return;
        }
        loading.set(true);
        spawn_local(async move {
            if api::create_event(&name).await.is_ok() {
                new_name.set(String::new());
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
            }
            loading.set(false);
        });
    };

    view! {
        <div class="settings-section">
            <h3>"Events"</h3>
            <div class="create-form">
                <input
                    type="text"
                    placeholder="Event name"
                    prop:value=move || new_name.get()
                    on:input=move |ev| new_name.set(event_target_value(&ev))
                />
                <button on:click=on_create disabled=move || loading.get()>"Create Event"</button>
            </div>
            <div class="items-list">
                {move || {
                    store.events_list.get().iter().map(|evt| {
                        let id = evt.id;
                        let name = evt.name.clone();
                        let recv = evt.receiving_activated;
                        let deliv = evt.delivering_activated;
                        let cache = evt.cache_delay_secs;

                        view! {
                            <div class="settings-card">
                                <div class="card-header">
                                    <strong>{name}</strong>
                                    <div class="badges">
                                        {if recv { Some(view! { <span class="badge active">"Receiving"</span> }) } else { None }}
                                        {if deliv { Some(view! { <span class="badge active">"Delivering"</span> }) } else { None }}
                                    </div>
                                </div>
                                <div class="card-body">
                                    <CacheDelayEditor event_id=id initial_delay=cache />
                                    <EventEndpoints event_id=id />
                                </div>
                                <div class="card-actions">
                                    <button class="btn-danger" on:click=move |_| {
                                        spawn_local(async move {
                                            let _ = api::delete_event(id).await;
                                            if let Ok(events) = api::list_events().await {
                                                store.events_list.set(events);
                                            }
                                        });
                                    }>"Delete"</button>
                                </div>
                            </div>
                        }
                    }).collect::<Vec<_>>()
                }}
            </div>
        </div>
    }
}

/// Editable cache delay input for an event.
#[component]
fn CacheDelayEditor(event_id: i64, initial_delay: Option<i64>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let delay_value = RwSignal::new(
        initial_delay
            .map(|d| d.to_string())
            .unwrap_or_default(),
    );

    let on_save = move |_| {
        let val = delay_value.get();
        let delay: Option<i64> = if val.trim().is_empty() {
            None
        } else {
            val.parse().ok()
        };
        let eid = event_id;
        spawn_local(async move {
            let req = api::UpdateEventRequest {
                cache_delay_secs: delay,
                ..Default::default()
            };
            let _ = api::update_event(eid, &req).await;
            if let Ok(events) = api::list_events().await {
                store.events_list.set(events);
            }
        });
    };

    view! {
        <div class="cache-edit">
            <label>"Cache delay (seconds):"</label>
            <input
                type="number"
                class="cache-delay-input"
                placeholder="Default (120)"
                prop:value=move || delay_value.get()
                on:input=move |ev| delay_value.set(event_target_value(&ev))
            />
            <button class="btn-small" on:click=on_save>"Save"</button>
            <span class="cache-hint">
                {move || {
                    if delay_value.get().trim().is_empty() {
                        "Using global default (120s)"
                    } else {
                        ""
                    }
                }}
            </span>
        </div>
    }
}

/// Event-endpoint assignment sub-component.
#[component]
fn EventEndpoints(event_id: i64) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let assigned = RwSignal::new(Vec::<api::EndpointConfig>::new());

    // Load assigned endpoints on mount
    let eid = event_id;
    spawn_local(async move {
        if let Ok(eps) = api::get_event_endpoints(eid).await {
            assigned.set(eps);
        }
    });

    view! {
        <div class="event-endpoints">
            <div class="assigned-endpoints">
                {move || {
                    assigned.get().iter().map(|ep| {
                        let ep_id = ep.id;
                        let alias = ep.alias.clone();
                        let eid = event_id;
                        view! {
                            <span class="endpoint-tag">
                                {alias}
                                <button class="tag-remove" on:click=move |_| {
                                    spawn_local(async move {
                                        let _ = api::detach_endpoint(eid, ep_id).await;
                                        if let Ok(eps) = api::get_event_endpoints(eid).await {
                                            assigned.set(eps);
                                        }
                                    });
                                }>{"\u{00D7}"}</button>
                            </span>
                        }
                    }).collect::<Vec<_>>()
                }}
            </div>
            <select on:change=move |ev| {
                let val = event_target_value(&ev);
                if let Ok(ep_id) = val.parse::<i64>() {
                    let eid = event_id;
                    spawn_local(async move {
                        let _ = api::attach_endpoint(eid, ep_id).await;
                        if let Ok(eps) = api::get_event_endpoints(eid).await {
                            assigned.set(eps);
                        }
                    });
                }
            }>
                <option value="">"+ Assign endpoint"</option>
                {move || {
                    let all = store.endpoints_list.get();
                    let assigned_ids: Vec<i64> = assigned.get().iter().map(|e| e.id).collect();
                    all.iter().filter(|ep| !assigned_ids.contains(&ep.id)).map(|ep| {
                        let id_str = ep.id.to_string();
                        let alias = ep.alias.clone();
                        view! { <option value={id_str}>{alias}</option> }
                    }).collect::<Vec<_>>()
                }}
            </select>
        </div>
    }
}

