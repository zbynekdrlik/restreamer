//! "Change Key" guided modal (#68).
//!
//! Changing a YouTube/FB stream key on an endpoint that is CURRENTLY attached
//! to a live delivery is otherwise a 3-step dance across two screens: the live
//! endpoint task snapshots its config at spawn (rs-api endpoint_handle.rs), so
//! an in-place key edit while attached is silently inert — the operator has to
//! remove the endpoint from the delivery, edit the key, then re-add it at the
//! live edge. This modal performs that whole sequence from one dashboard click:
//!   1. `delivery_remove_endpoint(event_id, alias)`  — detach from the live delivery
//!   2. `update_endpoint(id, { stream_key })`         — persist the new key
//!   3. `delivery_add_endpoint(event_id, id, "Live")` — re-add at the live edge;
//!      the add re-reads the endpoint config FRESH from the DB, so it picks up
//!      the new key.
//!
//! Mounted inside the endpoint tree as a sibling of the endpoint `For` (like the
//! remove-confirm modals) so per-endpoint re-renders never unmount it.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api::{self, UpdateEndpointRequest};
use crate::store::DashboardStore;

#[component]
pub fn ChangeKeyModal(
    /// Whether the modal is shown.
    show: RwSignal<bool>,
    /// Alias of the endpoint whose key is being changed (set by the tree button).
    alias: RwSignal<Option<String>>,
) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let new_key = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let error = RwSignal::new(Option::<String>::None);

    // Reset the input each time the modal opens.
    Effect::new(move |_| {
        if show.get() {
            new_key.set(String::new());
            busy.set(false);
            error.set(None);
        }
    });

    let target_alias = Signal::derive(move || alias.get().unwrap_or_default());

    let on_confirm = move |_| {
        let key = new_key.get();
        if key.trim().is_empty() {
            return;
        }
        let Some(alias_val) = alias.get_untracked() else {
            return;
        };
        // Resolve the endpoint DB id from the configured-endpoints list.
        let ep_id = store
            .endpoints_list
            .get_untracked()
            .iter()
            .find(|e| e.alias == alias_val)
            .map(|e| e.id);
        let Some(ep_id) = ep_id else {
            error.set(Some(format!("No endpoint config found for \"{alias_val}\"")));
            return;
        };
        // Prefer the live delivery's event_id (same source the sibling
        // remove-endpoint action uses), falling back to the selected event.
        let event_id = store
            .pipeline_state
            .get_untracked()
            .event_id
            .or_else(|| store.selected_event_id.get_untracked());
        let Some(event_id) = event_id else {
            error.set(Some("No active event selected".to_string()));
            return;
        };
        busy.set(true);
        error.set(None);
        spawn_local(async move {
            // 1. detach from the live delivery
            if let Err(e) = api::delivery_remove_endpoint(event_id, &alias_val).await {
                error.set(Some(format!("Remove failed: {e}")));
                busy.set(false);
                return;
            }
            // 2. persist the new key
            let req = UpdateEndpointRequest {
                stream_key: Some(key),
                ..Default::default()
            };
            if let Err(e) = api::update_endpoint(ep_id, &req).await {
                error.set(Some(format!("Key update failed: {e}")));
                busy.set(false);
                return;
            }
            // 3. re-add at the live edge (re-reads fresh config incl. the new key)
            if let Err(e) = api::delivery_add_endpoint(event_id, ep_id, "Live").await {
                error.set(Some(format!("Re-add failed: {e}")));
                busy.set(false);
                return;
            }
            busy.set(false);
            show.set(false);
        });
    };

    let on_cancel = move |_| show.set(false);
    let on_overlay_click = move |_| {
        if !busy.get() {
            show.set(false);
        }
    };

    view! {
        <Show when=move || show.get() fallback=|| ()>
            <div class="modal-overlay" on:click=on_overlay_click>
                <div
                    class="change-key-modal"
                    data-testid="change-key-modal"
                    on:click=move |ev| ev.stop_propagation()
                >
                    <h3>"Change Key"</h3>
                    <p class="change-key-endpoint">
                        {move || format!("Endpoint: {}", target_alias.get())}
                    </p>
                    <p class="change-key-hint">
                        "Removes this endpoint from the live delivery, saves the new key, and re-adds it at the live edge."
                    </p>
                    <input
                        class="change-key-input"
                        type="text"
                        data-testid="change-key-input"
                        placeholder="New stream key"
                        prop:value=move || new_key.get()
                        on:input=move |ev| new_key.set(event_target_value(&ev))
                    />
                    {move || {
                        error
                            .get()
                            .map(|e| view! { <div class="change-key-error">{e}</div> })
                    }}
                    <div class="modal-actions">
                        <button
                            class="modal-add-btn btn-small"
                            data-testid="change-key-confirm"
                            on:click=on_confirm
                            disabled=move || busy.get() || new_key.get().trim().is_empty()
                        >
                            {move || if busy.get() { "Changing\u{2026}" } else { "Change Key" }}
                        </button>
                        <button
                            class="modal-cancel-btn"
                            on:click=on_cancel
                            disabled=move || busy.get()
                        >
                            "Cancel"
                        </button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
