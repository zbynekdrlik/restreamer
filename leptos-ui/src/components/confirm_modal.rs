//! Reusable confirmation modal for destructive actions.

use gloo_timers::callback::Timeout;
use leptos::prelude::*;

/// A confirmation modal with Cancel and a danger-styled Confirm button.
///
/// Dismisses on: Cancel click, overlay click, Escape key.
///
/// IMPORTANT: All click handlers defer the `show.set(false)` dismiss via
/// `setTimeout(0)` (gloo_timers::Timeout) so the surrounding `<Show>` does
/// not unmount the button while its `on:click` Closure is still executing.
/// Without the defer, Leptos panics with `closure invoked recursively or
/// after being dropped` because the wasm-bindgen Closure backing the
/// button is freed mid-call.
///
/// `spawn_local` is NOT enough here — Leptos's local executor polls the
/// queued future before the JS click event loop yields, so the dismiss
/// still races with the running handler. setTimeout truly defers to the
/// next macrotask.
#[component]
pub fn ConfirmModal(
    show: RwSignal<bool>,
    title: &'static str,
    #[prop(into)] message: Signal<String>,
    confirm_label: &'static str,
    on_confirm: Callback<()>,
) -> impl IntoView {
    let dismiss_deferred = move || {
        Timeout::new(0, move || {
            show.set(false);
        })
        .forget();
    };

    let on_overlay_click = move |_| dismiss_deferred();
    let on_cancel = move |_| dismiss_deferred();

    let on_confirm_click = move |_| {
        // Run the user's callback FIRST (synchronously) so it can read any
        // signals it needs while the modal is still mounted, then defer
        // the dismiss.
        on_confirm.run(());
        dismiss_deferred();
    };

    let on_keydown = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Escape" {
            dismiss_deferred();
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
