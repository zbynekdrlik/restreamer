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

/// Buffer-bar severity class for a FAST (near-live) endpoint.
///
/// `secs` is the endpoint's observed `chunk_delay_secs`; `target_secs` is the
/// adaptive controller's current ratcheted read-delay target, when the VPS
/// reports one.
///
/// Extracted VERBATIM from `operator_dashboard`'s inline fast-bar branch so the
/// classification is testable. Behaviour is unchanged by the extraction — the
/// target is accepted but not yet consulted, which is exactly the #295 bug: the
/// absolute 8s ceiling predates the #294 ratchet and paints a correctly-held
/// buffer permanently red.
pub fn fast_buffer_class(secs: f64, target_secs: Option<f64>) -> &'static str {
    let _ = target_secs;
    if secs > 8.0 {
        "critical"
    } else if secs <= 5.0 {
        "healthy"
    } else {
        "warning"
    }
}

#[cfg(test)]
mod tests {
    use super::{cache_threshold_for_service, fast_buffer_class};

    #[test]
    fn held_ratcheted_buffer_renders_healthy_not_critical() {
        // #295 regression (live event 9333, 2026-07-19): `Control stream Kiko`
        // held a stable 30s buffer for hours — exactly the #294 design (ratchet
        // up on a real drain, then HOLD; fast_delay_shrank = 0, zero stutters).
        // The dashboard rendered it permanently RED because the fast-bar
        // thresholds still assumed a 2-5s near-live buffer and hard-coded
        // `secs > 8.0 => critical`. The operator read RED as "broken" while the
        // stream was in fact perfect — the exact opposite of the intended
        // signal.
        //
        // A fast endpoint TRACKING its ratcheted target is healthy at ANY value
        // the ratchet legitimately reached (the controller's range is 5..=120s).
        for target in [5.0, 6.0, 8.0, 10.0, 14.0, 30.0, 45.0, 120.0] {
            assert_eq!(
                fast_buffer_class(target, Some(target)),
                "healthy",
                "a fast endpoint holding its {target}s ratcheted target is \
                 working as designed and must render healthy"
            );
        }
        // The live case from the ticket, stated explicitly.
        assert_eq!(
            fast_buffer_class(30.0, Some(30.0)),
            "healthy",
            "#295: a held 30s ratcheted buffer must NOT render critical"
        );
    }

    #[test]
    fn fast_buffer_below_target_is_healthy() {
        // Closer to the live edge than the target (transient, e.g. right after
        // a lag-jump correction) is not a problem — the buffer is the ceiling
        // it works toward, not a minimum it must exceed.
        assert_eq!(fast_buffer_class(12.0, Some(30.0)), "healthy");
        assert_eq!(fast_buffer_class(0.0, Some(30.0)), "healthy");
    }

    #[test]
    fn fast_buffer_drifting_above_target_still_warns_and_alarms() {
        // The other half of the #295 acceptance: a fast endpoint genuinely
        // starving / drifting ABOVE its target must still surface. Ratios are
        // relative to the target, so they scale with the ratchet.
        let target = Some(30.0);
        assert_eq!(fast_buffer_class(30.0 * 1.5, target), "warning");
        assert_eq!(fast_buffer_class(30.0 * 2.5, target), "critical");
        // Same shape at a small target — the classification is scale-free.
        let small = Some(8.0);
        assert_eq!(fast_buffer_class(8.0 * 1.5, small), "warning");
        assert_eq!(fast_buffer_class(8.0 * 2.5, small), "critical");
    }

    #[test]
    fn fast_buffer_without_a_reported_target_falls_back_to_absolute_bands() {
        // A VPS running an rs-delivery older than #295 reports no target. Fall
        // back to the pre-#295 absolute thresholds, which are correct for an
        // UNRATCHETED fast endpoint sitting near the 5s floor.
        assert_eq!(fast_buffer_class(2.0, None), "healthy");
        assert_eq!(fast_buffer_class(5.0, None), "healthy");
        assert_eq!(fast_buffer_class(6.0, None), "warning");
        assert_eq!(fast_buffer_class(9.0, None), "critical");
        // A zero/garbage target is treated as "not reported", never as a
        // divide-by-zero that paints everything critical.
        assert_eq!(fast_buffer_class(2.0, Some(0.0)), "healthy");
    }

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
