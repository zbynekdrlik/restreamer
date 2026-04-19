//! Confirmation modal for removing the LAST endpoint from an active delivery.
//!
//! Because that action takes the audience offline, the operator must type
//! the event name verbatim before the "Remove anyway" button becomes
//! clickable. This is deliberately harder than the regular remove-confirm
//! modal — the last-endpoint case has no recovery without re-attaching a
//! new endpoint.

use leptos::prelude::*;

#[component]
pub fn EndpointRemoveConfirmModal(
    #[prop(into)] alias: Signal<String>,
    #[prop(into)] event_name: Signal<String>,
    #[prop(into)] visible: Signal<bool>,
    on_cancel: impl Fn() + 'static + Send + Sync + Clone,
    on_confirm: impl Fn() + 'static + Send + Sync + Clone,
) -> impl IntoView {
    let (typed, set_typed) = signal(String::new());

    // Reset the typed-match box every time the modal opens so residue
    // from a previous open doesn't carry over.
    Effect::new(move |_| {
        if visible.get() {
            set_typed.set(String::new());
        }
    });

    let match_ok =
        Memo::new(move |_| typed.get() == event_name.get() && !event_name.get().is_empty());

    // Store callbacks so the `<Show>` children closure (which must be Fn,
    // not FnOnce) can re-capture them without moving the original.
    let on_cancel_stored = StoredValue::new(on_cancel);
    let on_confirm_stored = StoredValue::new(on_confirm);

    view! {
        <Show when=move || visible.get()>
            <div class="modal__backdrop">
                <div class="endpoint-remove-modal" on:click=move |ev| ev.stop_propagation()>
                    <h3>"Remove last endpoint"</h3>
                    <p>
                        "Removing "
                        <strong>{move || alias.get()}</strong>
                        " is the last endpoint on this delivery. Audience will see NOTHING."
                    </p>
                    <p>
                        "Type the event name ("
                        <code>{move || event_name.get()}</code>
                        ") to confirm:"
                    </p>
                    <input
                        class="endpoint-remove-modal__input"
                        type="text"
                        prop:value=move || typed.get()
                        on:input=move |ev| set_typed.set(event_target_value(&ev))
                    />
                    <div class="endpoint-remove-modal__actions">
                        <button on:click=move |_| on_cancel_stored.with_value(|f| f())>"Cancel"</button>
                        <button
                            class="confirm-btn-danger"
                            prop:disabled=move || !match_ok.get()
                            on:click=move |_| on_confirm_stored.with_value(|f| f())
                        >
                            "Remove anyway"
                        </button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
