//! Endpoint configs management component.

use leptos::prelude::*;

use crate::api;

/// Endpoints tab: list, create, edit endpoint configurations.
#[component]
pub fn Endpoints() -> impl IntoView {
    let (endpoints, set_endpoints) = signal::<Vec<api::EndpointConfig>>(Vec::new());
    let (error, set_error) = signal::<Option<String>>(None);
    let (new_alias, set_new_alias) = signal(String::new());
    let (new_type, set_new_type) = signal("YT_HLS".to_string());
    let (new_key, set_new_key) = signal(String::new());

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
                    <option value="YT_HLS">"YouTube HLS"</option>
                    <option value="YT_RTMP">"YouTube RTMP"</option>
                    <option value="FB">"Facebook"</option>
                    <option value="VIMEO">"Vimeo"</option>
                    <option value="INSTAGRAM">"Instagram"</option>
                    <option value="TEST_FILE">"Test File"</option>
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
                    view! {
                        <div class="endpoint-card">
                            <div class="endpoint-header">
                                <strong>{ep.alias.clone()}</strong>
                                <span class="service-type">{ep.service_type.clone()}</span>
                                <span class=move || if enabled { "badge active" } else { "badge" }>
                                    {if enabled { "Enabled" } else { "Disabled" }}
                                </span>
                                {if ep.is_fast { Some(view! { <span class="badge fast">"Fast"</span> }) } else { None }}
                            </div>
                            <div class="endpoint-actions">
                                <button class="danger" on:click=move |_| {
                                    leptos::task::spawn_local(async move {
                                        let _ = api::delete_endpoint(id).await;
                                        if let Ok(eps) = api::list_endpoints().await {
                                            set_endpoints.set(eps);
                                        }
                                    });
                                }>"Delete"</button>
                            </div>
                        </div>
                    }
                }).collect_view()}
            </div>
        </div>
    }
}
