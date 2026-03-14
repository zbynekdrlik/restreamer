//! Endpoint configs management component.

use leptos::prelude::*;

use crate::api::{self, UpdateEndpointRequest};

/// Reusable service type options for select dropdowns.
#[component]
fn ServiceTypeOptions() -> impl IntoView {
    view! {
        <>
            <option value="YT_HLS">"YouTube HLS"</option>
            <option value="YT_RTMP">"YouTube RTMP"</option>
            <option value="FB">"Facebook"</option>
            <option value="VIMEO">"Vimeo"</option>
            <option value="INSTAGRAM">"Instagram"</option>
            <option value="TEST_FILE">"Test File"</option>
        </>
    }
}

/// Endpoints tab: list, create, edit endpoint configurations.
#[component]
pub fn Endpoints() -> impl IntoView {
    let (endpoints, set_endpoints) = signal::<Vec<api::EndpointConfig>>(Vec::new());
    let (error, set_error) = signal::<Option<String>>(None);
    let (new_alias, set_new_alias) = signal(String::new());
    let (new_type, set_new_type) = signal("YT_HLS".to_string());
    let (new_key, set_new_key) = signal(String::new());

    // Edit mode state
    let (editing_id, set_editing_id) = signal::<Option<i64>>(None);
    let (edit_alias, set_edit_alias) = signal(String::new());
    let (edit_type, set_edit_type) = signal(String::new());
    let (edit_key, set_edit_key) = signal(String::new());
    let (edit_enabled, set_edit_enabled) = signal(false);
    let (edit_fast, set_edit_fast) = signal(false);
    let (saving, set_saving) = signal(false);
    let (show_edit_key, set_show_edit_key) = signal(false);

    // Fetch on mount
    Effect::new(move |_| {
        leptos::task::spawn_local(async move {
            match api::list_endpoints().await {
                Ok(eps) => set_endpoints.set(eps),
                Err(e) => set_error.set(Some(e)),
            }
        });
    });

    let on_create = move |_| {
        let alias = new_alias.get();
        let stype = new_type.get();
        let key = new_key.get();
        if alias.is_empty() || key.is_empty() {
            return;
        }
        leptos::task::spawn_local(async move {
            match api::create_endpoint(&alias, &stype, &key).await {
                Ok(_) => {
                    set_new_alias.set(String::new());
                    set_new_key.set(String::new());
                    if let Ok(eps) = api::list_endpoints().await {
                        set_endpoints.set(eps);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    let start_edit = move |ep: &api::EndpointConfig| {
        set_edit_alias.set(ep.alias.clone());
        set_edit_type.set(ep.service_type.clone());
        set_edit_key.set(ep.stream_key.clone());
        set_edit_enabled.set(ep.enabled);
        set_edit_fast.set(ep.is_fast);
        set_editing_id.set(Some(ep.id));
    };

    let cancel_edit = move |_| {
        set_editing_id.set(None);
        set_error.set(None);
        set_show_edit_key.set(false);
    };

    let save_edit = move |_| {
        let id = match editing_id.get() {
            Some(id) => id,
            None => return,
        };
        let alias = edit_alias.get();
        let stype = edit_type.get();
        let key = edit_key.get();
        let enabled = edit_enabled.get();
        let is_fast = edit_fast.get();

        if alias.is_empty() {
            set_error.set(Some("Alias cannot be empty".to_string()));
            return;
        }

        set_saving.set(true);
        leptos::task::spawn_local(async move {
            let req = UpdateEndpointRequest {
                alias: Some(alias),
                service_type: Some(stype),
                stream_key: Some(key),
                enabled: Some(enabled),
                is_fast: Some(is_fast),
            };
            match api::update_endpoint(id, &req).await {
                Ok(_) => {
                    set_editing_id.set(None);
                    set_error.set(None);
                    set_show_edit_key.set(false);
                    if let Ok(eps) = api::list_endpoints().await {
                        set_endpoints.set(eps);
                    }
                }
                Err(e) => set_error.set(Some(format!("Update failed: {e}"))),
            }
            set_saving.set(false);
        });
    };

    view! {
        <div class="endpoints-tab">
            <h2>"Endpoint Configurations"</h2>

            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}

            <div class="create-form">
                <input
                    type="text"
                    placeholder="Alias (e.g. YouTube)"
                    prop:value=move || new_alias.get()
                    on:input=move |ev| set_new_alias.set(event_target_value(&ev))
                />
                <select
                    prop:value=move || new_type.get()
                    on:change=move |ev| set_new_type.set(event_target_value(&ev))
                >
                    <ServiceTypeOptions />
                </select>
                <input
                    type="text"
                    placeholder="Stream key..."
                    prop:value=move || new_key.get()
                    on:input=move |ev| set_new_key.set(event_target_value(&ev))
                />
                <button on:click=on_create>"Add Endpoint"</button>
            </div>

            <div class="endpoint-list">
                {move || endpoints.get().into_iter().map(|ep| {
                    let id = ep.id;
                    let enabled = ep.enabled;
                    let is_fast = ep.is_fast;
                    let alias = ep.alias.clone();
                    let service_type = ep.service_type.clone();
                    let ep_for_edit = ep.clone();

                    view! {
                        <div class="endpoint-card">
                            {move || {
                                if editing_id.get() == Some(id) {
                                    // Edit mode
                                    view! {
                                        <div class="endpoint-edit-form">
                                            <div class="edit-row">
                                                <label>"Alias"</label>
                                                <input
                                                    type="text"
                                                    prop:value=move || edit_alias.get()
                                                    on:input=move |ev| set_edit_alias.set(event_target_value(&ev))
                                                />
                                            </div>
                                            <div class="edit-row">
                                                <label>"Type"</label>
                                                <select
                                                    prop:value=move || edit_type.get()
                                                    on:change=move |ev| set_edit_type.set(event_target_value(&ev))
                                                >
                                                    <ServiceTypeOptions />
                                                </select>
                                            </div>
                                            <div class="edit-row">
                                                <label>"Stream Key"</label>
                                                <div class="key-input-wrapper">
                                                    <input
                                                        type=move || if show_edit_key.get() { "text" } else { "password" }
                                                        prop:value=move || edit_key.get()
                                                        on:input=move |ev| set_edit_key.set(event_target_value(&ev))
                                                    />
                                                    <button
                                                        type="button"
                                                        class="toggle-key-btn"
                                                        on:click=move |_| set_show_edit_key.set(!show_edit_key.get())
                                                    >
                                                        {move || if show_edit_key.get() { "Hide" } else { "Show" }}
                                                    </button>
                                                </div>
                                            </div>
                                            <div class="edit-row checkboxes">
                                                <label class="checkbox-label">
                                                    <input
                                                        type="checkbox"
                                                        prop:checked=move || edit_enabled.get()
                                                        on:change=move |ev| set_edit_enabled.set(event_target_checked(&ev))
                                                    />
                                                    "Enabled"
                                                </label>
                                                <label class="checkbox-label">
                                                    <input
                                                        type="checkbox"
                                                        prop:checked=move || edit_fast.get()
                                                        on:change=move |ev| set_edit_fast.set(event_target_checked(&ev))
                                                    />
                                                    "Fast Mode"
                                                </label>
                                            </div>
                                            <div class="edit-actions">
                                                <button
                                                    class="save"
                                                    on:click=save_edit
                                                    disabled=move || saving.get()
                                                >
                                                    {move || if saving.get() { "Saving..." } else { "Save" }}
                                                </button>
                                                <button on:click=cancel_edit disabled=move || saving.get()>"Cancel"</button>
                                            </div>
                                        </div>
                                    }.into_any()
                                } else {
                                    // View mode
                                    let ep_clone = ep_for_edit.clone();
                                    view! {
                                        <>
                                            <div class="endpoint-header">
                                                <strong>{alias.clone()}</strong>
                                                <span class="service-type">{service_type.clone()}</span>
                                                <span class=move || if enabled { "badge active" } else { "badge" }>
                                                    {if enabled { "Enabled" } else { "Disabled" }}
                                                </span>
                                                {if is_fast { Some(view! { <span class="badge fast">"Fast"</span> }) } else { None }}
                                            </div>
                                            <div class="endpoint-actions">
                                                <button on:click=move |_| start_edit(&ep_clone)>"Edit"</button>
                                                <button class="danger" on:click=move |_| {
                                                    leptos::task::spawn_local(async move {
                                                        let _ = api::delete_endpoint(id).await;
                                                        if let Ok(eps) = api::list_endpoints().await {
                                                            set_endpoints.set(eps);
                                                        }
                                                    });
                                                }>"Delete"</button>
                                            </div>
                                        </>
                                    }.into_any()
                                }
                            }}
                        </div>
                    }
                }).collect_view()}
            </div>
        </div>
    }
}
