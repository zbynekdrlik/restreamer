//! Unit tests for `cap_endpoint_delay_secs` — the per-endpoint dashboard
//! delay-number cap (FIX B, #232).
//!
//! Before this cap, the per-endpoint `chunk_delay_secs` was the raw output of
//! `get_cache_duration_secs` (sum of every sent chunk above the read position).
//! A fast/behind endpoint — or one whose `current_chunk_id` momentarily reads 0
//! — showed the whole S3 backlog (e.g. 7800s). These tests lock the bounded
//! behavior for both endpoint classes.

use crate::delivery_status::cap_endpoint_delay_secs;

#[test]
fn delayed_endpoint_caps_at_target_times_1_5() {
    // target = 120s → cap = 180s. A 7800s ghost must be clamped to 180.
    assert_eq!(cap_endpoint_delay_secs(7800.0, false, 120), 180.0);
}

#[test]
fn delayed_endpoint_passes_value_below_cap_unchanged() {
    // 90s of buffer under a 120s target (cap 180) is a legitimate value.
    assert_eq!(cap_endpoint_delay_secs(90.0, false, 120), 90.0);
}

#[test]
fn fast_endpoint_caps_at_small_constant() {
    // Fast endpoint (delivery_delay == 0): a 7800s raw value is meaningless.
    // Must clamp to the 30s fast cap, NOT to 1.5*0 = 0.
    assert_eq!(cap_endpoint_delay_secs(7800.0, true, 0), 30.0);
}

#[test]
fn fast_endpoint_passes_small_value_unchanged() {
    // A genuinely near-live fast endpoint shows its real small buffer.
    assert_eq!(cap_endpoint_delay_secs(5.0, true, 0), 5.0);
}

#[test]
fn zero_target_falls_back_to_fast_cap_even_when_not_flagged_fast() {
    // Defensive: a garbage/zero target on a non-fast endpoint must not yield a
    // 1.5*0 = 0 cap that would zero out a real value. Fall back to the fast cap
    // so the number is bounded but never collapses to 0.
    assert_eq!(cap_endpoint_delay_secs(7800.0, false, 0), 30.0);
    // Below the fast cap → unchanged.
    assert_eq!(cap_endpoint_delay_secs(12.0, false, 0), 12.0);
}

#[test]
fn negative_raw_is_floored_to_zero() {
    // get_cache_duration_secs cannot return negative, but the cap must never
    // surface a negative delay to the dashboard regardless.
    assert_eq!(cap_endpoint_delay_secs(-5.0, false, 120), 0.0);
    assert_eq!(cap_endpoint_delay_secs(-5.0, true, 0), 0.0);
}
