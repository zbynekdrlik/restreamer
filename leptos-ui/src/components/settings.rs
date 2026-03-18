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
            <EndpointsSection />
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

/// Endpoints management section.
#[component]
fn EndpointsSection() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let new_alias = RwSignal::new(String::new());
    let new_type = RwSignal::new("YT_HLS".to_string());
    let new_key = RwSignal::new(String::new());
    let loading = RwSignal::new(false);

    let on_create = move |_| {
        let alias = new_alias.get();
        let stype = new_type.get();
        let key = new_key.get();
        if alias.trim().is_empty() {
            return;
        }
        loading.set(true);
        spawn_local(async move {
            if api::create_endpoint(&alias, &stype, &key).await.is_ok() {
                new_alias.set(String::new());
                new_key.set(String::new());
                if let Ok(endpoints) = api::list_endpoints().await {
                    store.endpoints_list.set(endpoints);
                }
            }
            loading.set(false);
        });
    };

    view! {
        <div class="settings-section">
            <h3>"Endpoints"</h3>
            <div class="create-form">
                <input
                    type="text"
                    placeholder="Alias"
                    prop:value=move || new_alias.get()
                    on:input=move |ev| new_alias.set(event_target_value(&ev))
                />
                <select
                    prop:value=move || new_type.get()
                    on:change=move |ev| new_type.set(event_target_value(&ev))
                >
                    <option value="YT_HLS">"YouTube HLS"</option>
                    <option value="YT_RTMP">"YouTube RTMP"</option>
                    <option value="FB">"Facebook"</option>
                    <option value="VIMEO">"Vimeo"</option>
                    <option value="INSTAGRAM">"Instagram"</option>
                    <option value="TEST_FILE">"Test File"</option>
                </select>
                <input
                    type="password"
                    placeholder="Stream key"
                    prop:value=move || new_key.get()
                    on:input=move |ev| new_key.set(event_target_value(&ev))
                />
                <button on:click=on_create disabled=move || loading.get()>"Create Endpoint"</button>
            </div>
            <div class="items-list">
                {move || {
                    store.endpoints_list.get().iter().map(|ep| {
                        let id = ep.id;
                        let alias = ep.alias.clone();
                        let stype = ep.service_type.clone();
                        let enabled = ep.enabled;
                        let is_fast = ep.is_fast;

                        view! {
                            <div class="settings-card">
                                <div class="card-header">
                                    <strong>{alias}</strong>
                                    <div class="badges">
                                        <span class="badge">{stype}</span>
                                        {if enabled {
                                            view! { <span class="badge active">"Enabled"</span> }.into_any()
                                        } else {
                                            view! { <span class="badge">"Disabled"</span> }.into_any()
                                        }}
                                        {if is_fast { Some(view! { <span class="badge fast">{"\u{26A1} Fast"}</span> }) } else { None }}
                                    </div>
                                </div>
                                <div class="card-actions">
                                    <button on:click=move |_| {
                                        let new_enabled = !enabled;
                                        spawn_local(async move {
                                            let req = api::UpdateEndpointRequest {
                                                enabled: Some(new_enabled),
                                                ..Default::default()
                                            };
                                            let _ = api::update_endpoint(id, &req).await;
                                            if let Ok(endpoints) = api::list_endpoints().await {
                                                store.endpoints_list.set(endpoints);
                                            }
                                        });
                                    }>{if enabled { "Disable" } else { "Enable" }}</button>
                                    <button class="btn-danger" on:click=move |_| {
                                        spawn_local(async move {
                                            let _ = api::delete_endpoint(id).await;
                                            if let Ok(endpoints) = api::list_endpoints().await {
                                                store.endpoints_list.set(endpoints);
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
