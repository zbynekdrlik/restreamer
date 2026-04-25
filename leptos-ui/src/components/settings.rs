//! Settings view for configuring events, endpoints, and stream settings.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

/// Format a byte count as a human-readable string. Wraps `api::format_bytes`
/// for the unsigned u64 values returned by the S3 usage endpoint.
fn format_bytes(bytes: u64) -> String {
    // Saturate at i64::MAX to handle absurd values without panicking.
    api::format_bytes(bytes.min(i64::MAX as u64) as i64)
}

/// Settings page with tab navigation: Config, Templates, Events.
#[component]
pub fn SettingsView() -> impl IntoView {
    let (settings_tab, set_settings_tab) = signal("config".to_string());

    view! {
        <div class="settings-page">
            <h2>"Settings"</h2>
            <div class="settings-tabs">
                <button
                    class=move || if settings_tab.get() == "config" { "tab active" } else { "tab" }
                    on:click=move |_| set_settings_tab.set("config".to_string())
                >
                    "Config"
                </button>
                <button
                    class=move || {
                        if settings_tab.get() == "templates" { "tab active" } else { "tab" }
                    }
                    on:click=move |_| set_settings_tab.set("templates".to_string())
                >
                    "Templates"
                </button>
                <button
                    class=move || if settings_tab.get() == "events" { "tab active" } else { "tab" }
                    on:click=move |_| set_settings_tab.set("events".to_string())
                >
                    "Events"
                </button>
            </div>
            {move || match settings_tab.get().as_str() {
                "templates" => view! { <super::templates::TemplatesView /> }.into_any(),
                "events" => view! { <EventsManagement /> }.into_any(),
                _ => {
                    view! {
                        <div>
                            <ObsSettingsSection />
                            <crate::components::EndpointsView />
                        </div>
                    }
                    .into_any()
                }
            }}
        </div>
    }
}

/// OBS WebSocket configuration section.
#[component]
fn ObsSettingsSection() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let enabled = RwSignal::new(true);
    let ws_url = RwSignal::new(String::new());
    let ws_password = RwSignal::new(String::new());
    let show_password = RwSignal::new(false);
    let saving = RwSignal::new(false);
    let status_msg = RwSignal::new(Option::<String>::None);
    let loaded = RwSignal::new(false);

    // Load current config on mount
    Effect::new(move || {
        if !loaded.get() {
            spawn_local(async move {
                if let Ok(config) = api::get_config().await {
                    if let Some(obs) = config.get("obs") {
                        enabled.set(obs.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true));
                        ws_url.set(
                            obs.get("ws_url")
                                .and_then(|v| v.as_str())
                                .unwrap_or("ws://127.0.0.1:4455")
                                .to_string(),
                        );
                        ws_password.set(
                            obs.get("ws_password")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        );
                    }
                    loaded.set(true);
                }
            });
        }
    });

    let on_save = move |_| {
        saving.set(true);
        status_msg.set(None);
        let patch = serde_json::json!({
            "obs": {
                "enabled": enabled.get(),
                "ws_url": ws_url.get(),
                "ws_password": ws_password.get(),
            }
        });
        spawn_local(async move {
            match api::patch_config(&patch).await {
                Ok(_) => {
                    status_msg.set(Some("Saved".to_string()));
                    // Reload password field (will show "***" if changed)
                    if let Ok(config) = api::get_config().await {
                        if let Some(obs) = config.get("obs") {
                            ws_password.set(
                                obs.get("ws_password")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            );
                        }
                    }
                }
                Err(e) => status_msg.set(Some(format!("Error: {e}"))),
            }
            saving.set(false);
        });
    };

    view! {
        <div class="settings-section">
            <h3>
                "OBS WebSocket"
                <span class="obs-connection-badge">
                    {move || {
                        let obs = store.obs_status.get();
                        if obs.streaming {
                            "Streaming"
                        } else if obs.connected {
                            "Connected"
                        } else {
                            "Disconnected"
                        }
                    }}
                </span>
            </h3>
            <div class="obs-settings-form">
                <div class="edit-row checkboxes">
                    <label class="checkbox-label">
                        <input
                            type="checkbox"
                            prop:checked=move || enabled.get()
                            on:change=move |ev| {
                                enabled.set(event_target_checked(&ev));
                            }
                        />
                        " Enabled"
                    </label>
                </div>
                <div class="edit-row">
                    <label>"WebSocket URL"</label>
                    <input
                        type="text"
                        placeholder="ws://127.0.0.1:4455"
                        prop:value=move || ws_url.get()
                        on:input=move |ev| ws_url.set(event_target_value(&ev))
                    />
                </div>
                <div class="edit-row">
                    <label>"Password"</label>
                    <div class="key-input-wrapper">
                        <input
                            type=move || if show_password.get() { "text" } else { "password" }
                            placeholder="(optional)"
                            prop:value=move || ws_password.get()
                            on:input=move |ev| ws_password.set(event_target_value(&ev))
                        />
                        <button
                            class="toggle-key-btn"
                            on:click=move |_| show_password.update(|v| *v = !*v)
                        >
                            {move || if show_password.get() { "Hide" } else { "Show" }}
                        </button>
                    </div>
                </div>
                <div class="obs-settings-actions">
                    <button
                        class="btn-small"
                        on:click=on_save
                        disabled=move || saving.get()
                    >
                        {move || if saving.get() { "Saving..." } else { "Save" }}
                    </button>
                    {move || status_msg.get().map(|msg| view! { <span class="status-hint">{msg}</span> })}
                </div>
            </div>
        </div>
    }
}

/// Editable cache delay input for an event.
#[component]
fn CacheDelayEditor(event_id: i64, initial_delay: Option<i64>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let delay_value = RwSignal::new(initial_delay.map(|d| d.to_string()).unwrap_or_default());

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

/// Editable rescue video URL input for an event.
#[component]
fn RescueVideoEditor(event_id: i64, initial_url: Option<String>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let url_value = RwSignal::new(initial_url.unwrap_or_default());
    let upload_status = RwSignal::new(String::new());

    let on_save = move |_| {
        let val = url_value.get();
        let url = if val.trim().is_empty() {
            None
        } else {
            Some(val)
        };
        let eid = event_id;
        spawn_local(async move {
            let req = api::UpdateEventRequest {
                rescue_video_url: url,
                ..Default::default()
            };
            let _ = api::update_event(eid, &req).await;
            if let Ok(events) = api::list_events().await {
                store.events_list.set(events);
            }
        });
    };

    let on_file = move |ev: leptos::ev::Event| {
        use wasm_bindgen::JsCast;
        let target = ev
            .target()
            .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok());
        let file = target.and_then(|i| i.files()).and_then(|fl| fl.get(0));
        let Some(file) = file else {
            return;
        };
        upload_status.set("Uploading...".into());
        let eid = event_id;
        spawn_local(async move {
            match api::upload_rescue_video(file).await {
                Ok(url) => {
                    url_value.set(url.clone());
                    upload_status.set(format!("Uploaded: {url}"));
                    // Save immediately so the event picks up the new URL
                    let req = api::UpdateEventRequest {
                        rescue_video_url: Some(url),
                        ..Default::default()
                    };
                    let _ = api::update_event(eid, &req).await;
                    if let Ok(events) = api::list_events().await {
                        store.events_list.set(events);
                    }
                }
                Err(e) => upload_status.set(format!("Upload failed: {e}")),
            }
        });
    };

    view! {
        <div class="cache-edit">
            <label>"Rescue video URL:"</label>
            <input
                type="text"
                class="rescue-video-input"
                placeholder="https://s3.example.com/rescue-video.mp4"
                prop:value=move || url_value.get()
                on:input=move |ev| url_value.set(event_target_value(&ev))
            />
            <button class="btn-small" on:click=on_save>"Save"</button>
            <label class="btn-small file-upload-btn">
                "Upload"
                <input
                    type="file"
                    accept="video/mp4,video/webm,video/quicktime,video/x-matroska"
                    style="display:none"
                    on:change=on_file
                />
            </label>
            {move || {
                let s = upload_status.get();
                if s.is_empty() {
                    None
                } else {
                    Some(view! { <span class="upload-status">{s}</span> })
                }
            }}
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

/// Events management tab: list events, create from template, delete with cleanup.
#[component]
fn EventsManagement() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    // Delete confirmation modal state
    let show_delete_modal = RwSignal::new(false);
    let delete_target_id = RwSignal::new(0i64);
    let delete_target_name = RwSignal::new(String::new());

    // Clear-S3 confirmation modal state (clears chunks but keeps event)
    let show_clear_modal = RwSignal::new(false);
    let clear_target_id = RwSignal::new(0i64);
    let clear_target_name = RwSignal::new(String::new());

    // S3 usage state
    let s3_usage = RwSignal::<Option<api::S3UsageResponse>>::new(None);
    let s3_usage_error = RwSignal::<Option<String>>::new(None);

    // Busy and error state for destructive actions on event cards.
    // `busy_event_id` is `Some(id)` while a delete or clear-S3 call is
    // in flight for that event; the buttons on that card render disabled
    // with a "Deleting…"/"Clearing…" label so the operator sees progress.
    // `action_error` holds the most recent failure message from either
    // action so it can render in a banner above the list.
    let busy_event_id = RwSignal::<Option<i64>>::new(None);
    let action_error = RwSignal::<Option<String>>::new(None);

    // Template picker modal state
    let show_template_modal = RwSignal::new(false);
    let (template_error, set_template_error) = signal::<Option<String>>(None);

    // Load events, templates, and S3 usage on mount.
    Effect::new(move |_| {
        spawn_local(async move {
            if let Ok(events) = api::list_events().await {
                store.events_list.set(events);
            }
            if let Ok(templates) = api::list_templates().await {
                store.templates_list.set(templates);
            }
            match api::get_s3_usage().await {
                Ok(u) => {
                    s3_usage.set(Some(u));
                    s3_usage_error.set(None);
                }
                Err(e) => s3_usage_error.set(Some(e)),
            }
        });
    });

    let on_confirm_delete = Callback::new(move |_: ()| {
        let id = delete_target_id.get();
        busy_event_id.set(Some(id));
        action_error.set(None);
        spawn_local(async move {
            match api::delete_event(id).await {
                Ok(_) => {
                    if let Ok(events) = api::list_events().await {
                        store.events_list.set(events);
                    }
                    if let Ok(u) = api::get_s3_usage().await {
                        s3_usage.set(Some(u));
                    }
                }
                Err(e) => action_error.set(Some(format!("Delete failed: {e}"))),
            }
            busy_event_id.set(None);
        });
    });

    let on_confirm_clear = Callback::new(move |_: ()| {
        let id = clear_target_id.get();
        busy_event_id.set(Some(id));
        action_error.set(None);
        spawn_local(async move {
            match api::clear_event_s3_chunks(id).await {
                Ok(_) => {
                    if let Ok(u) = api::get_s3_usage().await {
                        s3_usage.set(Some(u));
                    }
                }
                Err(e) => action_error.set(Some(format!("Clear failed: {e}"))),
            }
            busy_event_id.set(None);
        });
    });

    let delete_message = Signal::derive(move || {
        format!(
            "Delete event \"{}\"? This will also clean up S3 chunks.",
            delete_target_name.get()
        )
    });

    let clear_message = Signal::derive(move || {
        format!(
            "Delete all S3 chunks for event \"{}\"? The event row stays \
             — only the chunks are removed.",
            clear_target_name.get()
        )
    });

    view! {
        <div class="events-management-tab">
            <h2>"Events"</h2>

            <div class="events-actions-bar">
                <button
                    class="btn-primary"
                    on:click=move |_| {
                        set_template_error.set(None);
                        show_template_modal.set(true);
                    }
                >
                    "+ New from Template"
                </button>
            </div>

            // Action error banner — surfaces the most recent failure from
            // delete/clear actions. Dismissable with the "×" button.
            {move || action_error.get().map(|err| view! {
                <div class="error-message">
                    {err}
                    <button
                        class="banner-dismiss-btn"
                        on:click=move |_| action_error.set(None)
                    >
                        "×"
                    </button>
                </div>
            })}

            // S3 storage usage banner. Shows total bucket usage and lets the
            // operator see at a glance which event is using the most space.
            // Branches use .into_any() so the if/else arms unify to a single
            // AnyView type — Leptos generates a unique type for every view!{}
            // call so the raw arms can't be unified directly.
            {move || {
                if let Some(usage) = s3_usage.get() {
                    view! {
                        <div class="s3-usage-banner">
                            <strong>"S3 storage: "</strong>
                            {format_bytes(usage.total_bytes)}
                            " ("
                            {usage.total_objects}
                            " objects)"
                        </div>
                    }
                    .into_any()
                } else if let Some(err) = s3_usage_error.get() {
                    view! {
                        <div class="s3-usage-banner error">
                            "S3 usage unavailable: "
                            {err}
                        </div>
                    }
                    .into_any()
                } else {
                    view! { <div></div> }.into_any()
                }
            }}

            <div class="items-list">
                {move || {
                    let usage_map: std::collections::HashMap<String, (u64, u64)> = s3_usage
                        .get()
                        .map(|u| {
                            u.by_event
                                .into_iter()
                                .map(|e| (e.event_name, (e.bytes, e.objects)))
                                .collect()
                        })
                        .unwrap_or_default();
                    store.events_list.get().iter().map(|evt| {
                        let id = evt.id;
                        let cache = evt.cache_delay_secs;
                        let rescue_url = evt.rescue_video_url.clone();
                        let name = evt.name.clone();
                        let recv = evt.receiving_activated;
                        let deliv = evt.delivering_activated;
                        let is_streaming = recv || deliv;
                        let created_from = evt.created_from.clone();
                        let name_for_modal = name.clone();
                        let name_for_clear = name.clone();
                        let usage_for_event = usage_map.get(&name).cloned();

                        view! {
                            <div class="settings-card">
                                <div class="card-header">
                                    <strong>{name}</strong>
                                    <div class="badges">
                                        {if recv {
                                            Some(view! { <span class="badge active">"Receiving"</span> })
                                        } else {
                                            Some(view! { <span class="badge">"Idle"</span> })
                                        }}
                                        {if deliv {
                                            Some(view! {
                                                <span class="badge active">"Delivering"</span>
                                            })
                                        } else {
                                            Some(view! { <span class="badge">"Stopped"</span> })
                                        }}
                                        {created_from.map(|src| view! {
                                            <span class="badge template-badge">
                                                {format!("from: {src}")}
                                            </span>
                                        })}
                                        {usage_for_event.map(|(bytes, objects)| view! {
                                            <span class="badge s3-badge">
                                                {format!("S3: {} ({} obj)", format_bytes(bytes), objects)}
                                            </span>
                                        })}
                                    </div>
                                </div>
                                <div class="card-body">
                                    <CacheDelayEditor event_id=id initial_delay=cache />
                                    <RescueVideoEditor event_id=id initial_url=rescue_url />
                                    <EventEndpoints event_id=id />
                                </div>
                                <div class="card-actions">
                                    <button
                                        class="btn-secondary"
                                        disabled=move || is_streaming || busy_event_id.get() == Some(id)
                                        on:click=move |_| {
                                            clear_target_id.set(id);
                                            clear_target_name.set(name_for_clear.clone());
                                            show_clear_modal.set(true);
                                        }
                                        title="Delete S3 chunks for this event but keep the event row"
                                    >
                                        {move || {
                                            if busy_event_id.get() == Some(id) {
                                                "Clearing…"
                                            } else {
                                                "Clear S3 chunks"
                                            }
                                        }}
                                    </button>
                                    <button
                                        class="btn-danger"
                                        disabled=move || is_streaming || busy_event_id.get() == Some(id)
                                        on:click=move |_| {
                                            delete_target_id.set(id);
                                            delete_target_name.set(name_for_modal.clone());
                                            show_delete_modal.set(true);
                                        }
                                    >
                                        {move || {
                                            if is_streaming {
                                                "Delete (stop stream first)"
                                            } else if busy_event_id.get() == Some(id) {
                                                "Deleting…"
                                            } else {
                                                "Delete + Cleanup"
                                            }
                                        }}
                                    </button>
                                </div>
                            </div>
                        }
                    }).collect::<Vec<_>>()
                }}
            </div>

            <crate::components::ConfirmModal
                show=show_delete_modal
                title="Delete Event"
                message=delete_message
                confirm_label="Delete + Cleanup"
                on_confirm=on_confirm_delete
            />

            <crate::components::ConfirmModal
                show=show_clear_modal
                title="Clear S3 chunks"
                message=clear_message
                confirm_label="Clear chunks"
                on_confirm=on_confirm_clear
            />

            // Template picker modal
            <Show when=move || show_template_modal.get() fallback=|| ()>
                <div class="modal-overlay" on:click=move |_| show_template_modal.set(false)>
                    <div
                        class="confirm-modal"
                        on:click=move |ev| ev.stop_propagation()
                    >
                        <h3 class="confirm-modal-title">"New Event from Template"</h3>
                        {move || template_error.get().map(|e| view! {
                            <div class="error-message">{e}</div>
                        })}
                        <div class="template-picker-list">
                            {move || {
                                let templates = store.templates_list.get();
                                if templates.is_empty() {
                                    view! {
                                        <p class="section-hint">"No templates yet. Create one in the Templates tab."</p>
                                    }.into_any()
                                } else {
                                    templates.iter().map(|t| {
                                        let tid = t.id;
                                        let tname = t.name.clone();
                                        view! {
                                            <button
                                                class="template-pick-btn"
                                                on:click=move |_| {
                                                    let tname = tname.clone();
                                                    spawn_local(async move {
                                                        match api::create_event_from_template(tid).await {
                                                            Ok(_) => {
                                                                show_template_modal.set(false);
                                                                set_template_error.set(None);
                                                                if let Ok(events) = api::list_events().await {
                                                                    store.events_list.set(events);
                                                                }
                                                            }
                                                            Err(e) => {
                                                                set_template_error.set(Some(
                                                                    format!("Failed to create event from \"{tname}\": {e}")
                                                                ));
                                                            }
                                                        }
                                                    });
                                                }
                                            >
                                                {t.name.clone()}
                                            </button>
                                        }
                                    }).collect::<Vec<_>>().into_any()
                                }
                            }}
                        </div>
                        <div class="modal-actions">
                            <button
                                class="modal-cancel-btn"
                                on:click=move |_| show_template_modal.set(false)
                            >
                                "Cancel"
                            </button>
                        </div>
                    </div>
                </div>
            </Show>
        </div>
    }
}
