//! Reusable confirmation modal for destructive actions.

use leptos::prelude::*;

/// A confirmation modal with Cancel and a danger-styled Confirm button.
///
/// Dismisses on: Cancel click, overlay click, Escape key.
#[component]
pub fn ConfirmModal(
    show: RwSignal<bool>,
    title: &'static str,
    #[prop(into)] message: Signal<String>,
    confirm_label: &'static str,
    on_confirm: Callback<()>,
) -> impl IntoView {
    let dismiss = move || show.set(false);

    let on_overlay_click = move |_| dismiss();
    let on_cancel = move |_| dismiss();

    let on_confirm_click = move |_| {
        dismiss();
        on_confirm.run(());
    };

    let on_keydown = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Escape" {
            dismiss();
        }
    };

    view! {
        <Show when=move || show.get() fallback=|| ()>
            <div
                class="modal-overlay"
                on:click=on_overlay_click
                on:keydown=on_keydown
                tabindex="-1"
            >
                <div class="confirm-modal" on:click=move |ev| ev.stop_propagation()>
                    <h3 class="confirm-modal-title">{title}</h3>
                    <p class="confirm-modal-message">{move || message.get()}</p>
                    <div class="modal-actions">
                        <button class="confirm-btn-danger" on:click=on_confirm_click>
                            {confirm_label}
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
