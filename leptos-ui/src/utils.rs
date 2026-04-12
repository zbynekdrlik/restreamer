//! Small helpers shared across Leptos components.

use gloo_timers::callback::Timeout;
use leptos::prelude::*;

/// Defer `signal.set(false)` to the next JS macrotask (via `setTimeout(0)`).
///
/// Use this when you need to close an overlay from inside a click handler
/// that sits on one of the overlay's own buttons. Setting the signal
/// synchronously would unmount the button while its `on:click` Closure is
/// still executing, which triggers the Leptos panic
/// `closure invoked recursively or after being dropped`.
///
/// `leptos::task::spawn_local` is NOT sufficient here — it schedules a
/// microtask on Leptos's local executor which can drain before the JS
/// event handler returns. `setTimeout(0)` truly waits for the next
/// macrotask, after the click handler has fully unwound.
///
/// The returned Timeout is deliberately leaked with `.forget()` — it fires
/// once and there is nothing to cancel afterwards.
pub fn defer_set_false(signal: RwSignal<bool>) {
    Timeout::new(0, move || {
        signal.set(false);
    })
    .forget();
}
