//! Reusable confirmation modal for destructive actions.

use leptos::prelude::*;

use crate::utils::defer_set_false;

/// A confirmation modal with Cancel and a danger-styled Confirm button.
///
/// Dismisses on: Cancel click, overlay click, Escape key.
///
/// All dismiss paths go through `utils::defer_set_false` which uses
/// `setTimeout(0)` so the surrounding `<Show>` does not unmount the button
/// while its `on:click` Closure is still executing. See that helper's
/// docstring for the full rationale — tl;dr: `spawn_local` is not enough
/// because Leptos's local executor drains before the JS event handler
/// returns, so the closure-lifetime panic still fires.
#[component]
pub fn ConfirmModal(
    show: RwSignal<bool>,
    title: &'static str,
    #[prop(into)] message: Signal<String>,
    confirm_label: &'static str,
    on_confirm: Callback<()>,
) -> impl IntoView {
    let on_overlay_click = move |_| defer_set_false(show);
    let on_cancel = move |_| defer_set_false(show);

    let on_confirm_click = move |_| {
        // Run the user's callback FIRST (synchronously) so it can read any
        // signals it needs while the modal is still mounted, then defer
        // the dismiss.
        on_confirm.run(());
        defer_set_false(show);
    };

    let on_keydown = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Escape" {
            defer_set_false(show);
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
