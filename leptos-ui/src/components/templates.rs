//! Templates management component for creating and managing reusable event presets.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

/// Templates view: list, create, and manage event templates with endpoint assignment.
#[component]
pub fn TemplatesView() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let (new_name, set_new_name) = signal(String::new());
    let (new_cache, set_new_cache) = signal(String::new());
    let (loading, set_loading) = signal(false);
    let (error, set_error) = signal::<Option<String>>(None);

    // Load templates and endpoints on mount
    Effect::new(move |_| {
        spawn_local(async move {
            if let Ok(templates) = api::list_templates().await {
                store.templates_list.set(templates);
            }
            if let Ok(eps) = api::list_endpoints().await {
                store.endpoints_list.set(eps);
            }
        });
    });

    let on_create = move |_| {
        let name = new_name.get();
        if name.trim().is_empty() {
            return;
        }
        let cache: Option<i64> = {
            let v = new_cache.get();
            if v.trim().is_empty() {
                None
            } else {
                v.parse().ok()
            }
        };
        set_loading.set(true);
        set_error.set(None);
        spawn_local(async move {
            match api::create_template(&name, cache, None).await {
                Ok(_) => {
                    set_new_name.set(String::new());
                    set_new_cache.set(String::new());
                    if let Ok(templates) = api::list_templates().await {
                        store.templates_list.set(templates);
                    }
                }
                Err(e) => set_error.set(Some(format!("Failed to create template: {e}"))),
            }
            set_loading.set(false);
        });
    };

    view! {
        <div class="templates-tab">
            <h2>"Event Templates"</h2>
            <p class="section-hint">
                "Templates are reusable presets with pre-assigned endpoints. "
                "Create an event from a template to apply all settings at once."
            </p>

            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}

            <div class="create-form">
                <input
                    type="text"
                    placeholder="Template name"
                    prop:value=move || new_name.get()
                    on:input=move |ev| set_new_name.set(event_target_value(&ev))
                />
                <input
                    type="number"
                    placeholder="Cache delay (optional)"
                    prop:value=move || new_cache.get()
                    on:input=move |ev| set_new_cache.set(event_target_value(&ev))
                />
                <button on:click=on_create disabled=move || loading.get()>
                    {move || if loading.get() { "Creating..." } else { "Create Template" }}
                </button>
            </div>

            <div class="items-list">
                {move || {
                    store.templates_list.get().iter().map(|t| {
                        let id = t.id;
                        let name = t.name.clone();
                        let cache = t.cache_delay_secs;
                        let rescue = t.rescue_video_url.clone();
                        view! {
                            <TemplateCard
                                template_id=id
                                template_name=name
                                cache_delay_secs=cache
                                rescue_video_url=rescue
                            />
                        }
                    }).collect::<Vec<_>>()
                }}
            </div>
        </div>
    }
}

/// Per-template card with endpoint assignment.
#[component]
pub fn TemplateCard(
    template_id: i64,
    template_name: String,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let assigned = RwSignal::new(Vec::<api::EndpointConfig>::new());
    let (error, set_error) = signal::<Option<String>>(None);
    let rescue_url = RwSignal::new(rescue_video_url.unwrap_or_default());
    let upload_status = RwSignal::new(String::new());

    // Load assigned endpoints on mount
    let tid = template_id;
    spawn_local(async move {
        if let Ok(eps) = api::get_template_endpoints(tid).await {
            assigned.set(eps);
        }
    });

    let name = template_name.clone();
    let on_delete = move |_| {
        let tid = template_id;
        spawn_local(async move {
            if api::delete_template(tid).await.is_ok() {
                if let Ok(templates) = api::list_templates().await {
                    store.templates_list.set(templates);
                }
            }
        });
    };

    view! {
        <div class="settings-card">
            <div class="card-header">
                <strong>{name}</strong>
                <div class="badges">
                    {cache_delay_secs.map(|d| view! {
                        <span class="badge">{format!("Cache: {d}s")}</span>
                    })}
                </div>
                <button class="btn-danger" on:click=on_delete>"Delete"</button>
            </div>

            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}

            <div class="card-body">
                <div class="cache-edit">
                    <label>"Rescue video URL:"</label>
                    <input
                        type="text"
                        class="rescue-video-input"
                        placeholder="https://s3.example.com/rescue-video.mp4"
                        prop:value=move || rescue_url.get()
                        on:input=move |ev| rescue_url.set(event_target_value(&ev))
                    />
                    <button class="btn-small" on:click=move |_| {
                        let val = rescue_url.get();
                        let url = if val.trim().is_empty() { None } else { Some(val) };
                        let tid = template_id;
                        spawn_local(async move {
                            if let Err(e) = api::update_template(tid, None, None, url).await {
                                set_error.set(Some(format!("Update failed: {e}")));
                            } else {
                                set_error.set(None);
                                if let Ok(templates) = api::list_templates().await {
                                    store.templates_list.set(templates);
                                }
                            }
                        });
                    }>"Save"</button>
                    <label class="btn-small file-upload-btn">
                        "Upload"
                        <input
                            type="file"
                            accept="video/mp4,video/webm,video/quicktime,video/x-matroska"
                            style="display:none"
                            on:change=move |ev: leptos::ev::Event| {
                                use wasm_bindgen::JsCast;
                                let target = ev
                                    .target()
                                    .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok());
                                let file = target
                                    .and_then(|i| i.files())
                                    .and_then(|fl| fl.get(0));
                                let Some(file) = file else { return };
                                upload_status.set("Uploading...".into());
                                let tid = template_id;
                                spawn_local(async move {
                                    match api::upload_rescue_video(file).await {
                                        Ok(url) => {
                                            rescue_url.set(url.clone());
                                            upload_status.set(format!("Uploaded: {url}"));
                                            if let Err(e) = api::update_template(tid, None, None, Some(url)).await {
                                                set_error.set(Some(format!("Update failed: {e}")));
                                            } else if let Ok(templates) = api::list_templates().await {
                                                store.templates_list.set(templates);
                                            }
                                        }
                                        Err(e) => upload_status.set(format!("Upload failed: {e}")),
                                    }
                                });
                            }
                        />
                    </label>
                    {move || {
                        let s = upload_status.get();
                        if s.is_empty() { None } else {
                            Some(view! { <span class="upload-status">{s}</span> })
                        }
                    }}
                </div>
                <div class="event-endpoints">
                    <div class="assigned-endpoints">
                        {move || {
                            assigned.get().iter().map(|ep| {
                                let ep_id = ep.id;
                                let alias = ep.alias.clone();
                                let tid = template_id;
                                view! {
                                    <span class="endpoint-tag">
                                        {alias}
                                        <button class="tag-remove" on:click=move |_| {
                                            spawn_local(async move {
                                                if let Err(e) = api::detach_template_endpoint(tid, ep_id).await {
                                                    set_error.set(Some(format!("Detach failed: {e}")));
                                                } else {
                                                    set_error.set(None);
                                                    if let Ok(eps) = api::get_template_endpoints(tid).await {
                                                        assigned.set(eps);
                                                    }
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
                            let tid = template_id;
                            spawn_local(async move {
                                if let Err(e) = api::attach_template_endpoint(tid, ep_id).await {
                                    set_error.set(Some(format!("Attach failed: {e}")));
                                } else {
                                    set_error.set(None);
                                    if let Ok(eps) = api::get_template_endpoints(tid).await {
                                        assigned.set(eps);
                                    }
                                }
                            });
                        }
                    }>
                        <option value="">"+ Assign endpoint"</option>
                        {move || {
                            let all = store.endpoints_list.get();
                            let assigned_ids: Vec<i64> =
                                assigned.get().iter().map(|e| e.id).collect();
                            all.iter()
                                .filter(|ep| !assigned_ids.contains(&ep.id))
                                .map(|ep| {
                                    let id_str = ep.id.to_string();
                                    let alias = ep.alias.clone();
                                    view! { <option value={id_str}>{alias}</option> }
                                })
                                .collect::<Vec<_>>()
                        }}
                    </select>
                </div>
            </div>
        </div>
    }
}
