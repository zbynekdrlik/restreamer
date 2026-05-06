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

/// Cache-bar critical-threshold multiplier for a given endpoint
/// `service_type`. YT (RTMP / HLS) get a tight 1.1x band so any drift
/// surfaces; Facebook gets a looser 1.3x because FB ingest
/// legitimately settles ~10-15s above target due to TLS handshake +
/// RTT variance. Real divergence still trips critical for both
/// (#174 review #10 + review-of-review #8).
pub fn cache_threshold_for_service(service_type: &str) -> f64 {
    match service_type {
        "Facebook" => 1.3,
        // YT_RTMP / YT_HLS / unknown: tight gate.
        _ => 1.1,
    }
}

#[cfg(test)]
mod tests {
    use super::cache_threshold_for_service;

    #[test]
    fn facebook_gets_loose_threshold() {
        assert_eq!(cache_threshold_for_service("Facebook"), 1.3);
    }

    #[test]
    fn yt_and_unknown_get_tight_threshold() {
        assert_eq!(cache_threshold_for_service("YT_RTMP"), 1.1);
        assert_eq!(cache_threshold_for_service("YT_HLS"), 1.1);
        assert_eq!(cache_threshold_for_service(""), 1.1);
        assert_eq!(cache_threshold_for_service("Twitch"), 1.1);
    }
}
