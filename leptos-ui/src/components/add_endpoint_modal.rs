//! "Add endpoint" picker modal shown mid-delivery to attach an additional
//! destination without stopping the stream.
//!
//! The modal is mounted at the dashboard root (not inside the endpoint
//! tree) so that tree re-renders triggered by delivery polling do not
//! unmount the modal while the operator is interacting with it.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

#[component]
pub fn AddEndpointModal(show: RwSignal<bool>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let selected_ep_id = RwSignal::new(Option::<i64>::None);
    let start_position = RwSignal::new("Live".to_string());
    let available_eps = RwSignal::new(Vec::<(i64, String, String)>::new());

    // Snapshot available endpoints when modal opens (non-reactive)
    Effect::new(move |_| {
        if show.get() {
            let all = store.endpoints_list.get_untracked();
            let active_aliases: Vec<String> = store
                .delivery
                .get_untracked()
                .endpoints
                .iter()
                .map(|e| e.alias.clone())
                .collect();
            let opts: Vec<(i64, String, String)> = all
                .iter()
                .filter(|ep| !active_aliases.contains(&ep.alias))
                .map(|ep| (ep.id, ep.alias.clone(), ep.service_type.clone()))
                .collect();
            available_eps.set(opts);
            selected_ep_id.set(None);
            start_position.set("Live".to_string());
        }
    });

    let on_add = move |_| {
        if let Some(ep_id) = selected_ep_id.get() {
            let pos = start_position.get();
            if let Some(event_id) = store.selected_event_id.get() {
                spawn_local(async move {
                    let _ = api::delivery_add_endpoint(event_id, ep_id, &pos).await;
                });
            }
            show.set(false);
        }
    };

    let on_cancel = move |_| {
        show.set(false);
    };

    let on_overlay_click = move |_| {
        show.set(false);
    };

    view! {
        <Show when=move || show.get() fallback=|| ()>
            <div class="modal-overlay" on:click=on_overlay_click>
                <div class="add-endpoint-modal" on:click=move |ev| ev.stop_propagation()>
                    <h3>"Add Endpoint"</h3>
                    <div class="modal-endpoint-list">
                        {move || {
                            available_eps.get().iter().map(|(id, alias, stype)| {
                                let ep_id = *id;
                                let is_selected = move || selected_ep_id.get() == Some(ep_id);
                                let alias = alias.clone();
                                let stype = stype.clone();
                                view! {
                                    <div
                                        class="modal-endpoint-row"
                                        class:selected=is_selected
                                        on:click=move |_| selected_ep_id.set(Some(ep_id))
                                    >
                                        <span class="modal-ep-alias">{alias}</span>
                                        <span class="modal-ep-type">{stype}</span>
                                    </div>
                                }
                            }).collect::<Vec<_>>()
                        }}
                    </div>
                    <div class="modal-position">
                        <label>"Start position:"</label>
                        <select
                            class="start-position-select"
                            on:change=move |ev| start_position.set(event_target_value(&ev))
                        >
                            <option value="Live">"Live"</option>
                            <option value="Beginning">"From Beginning"</option>
                        </select>
                    </div>
                    <div class="modal-actions">
                        <button
                            class="modal-add-btn btn-small"
                            on:click=on_add
                            disabled=move || selected_ep_id.get().is_none()
                        >
                            "Add"
                        </button>
                        <button class="modal-cancel-btn" on:click=on_cancel>
                            "Cancel"
                        </button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
