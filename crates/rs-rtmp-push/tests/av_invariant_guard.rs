//! #367 design test 5: the ABSOLUTE A/V invariant guard.
//!
//! INVARIANT: every stage applies ONE common transform to both tracks, so
//! for the latest tag of each track `(a_out - v_out) == (a_in - v_in)`. The
//! guard has NO baseline. An offset present from the very first chunk is a
//! violation, which is exactly the case the baseline-relative skew guards
//! (#257/#354/#359) fold away and never see (the 2026-10-01 incident).

use rs_rtmp_push::{
    AV_INVARIANT_TOLERANCE_MS, AvInvariantEvent, AvInvariantGuard, AvInvariantViolation,
};

/// A stage that keeps the relation (one shared transform) never trips,
/// whatever the transform's offset is.
#[test]
fn shared_transform_never_violates() {
    let mut g = AvInvariantGuard::default();
    // out = in + 5_000 for BOTH tracks; audio 700 ms after video in content.
    g.observe_video(1_000, 6_000);
    g.observe_audio(1_700, 6_700);
    assert_eq!(g.check(), None);
    assert_eq!(g.evaluate(), None);
    assert!(!g.is_violated());
}

/// The Thursday shape: a stage that collapsed the +700 ms content relation
/// to 0 on its output. The violation is present from the FIRST evaluation
/// (no baseline) and carries a_rel / v_rel / delta.
#[test]
fn offset_from_the_first_chunk_is_a_violation() {
    let mut g = AvInvariantGuard::default();
    g.observe_video(1_000, 5_000);
    g.observe_audio(1_700, 5_000);
    let v = AvInvariantViolation {
        a_rel_ms: 3_300,
        v_rel_ms: 4_000,
        delta_ms: -700,
    };
    assert_eq!(g.check(), Some(v));
    assert_eq!(g.evaluate(), Some(AvInvariantEvent::Violated(v)));
    assert!(g.is_violated());
    assert_eq!(g.violation_count(), 1);
    // Edge-triggered: a still-violated boundary does not re-fire.
    assert_eq!(g.evaluate(), None);
    assert_eq!(g.violation_count(), 1);
}

/// Within tolerance is fine; one ms past it is not.
#[test]
fn tolerance_boundary() {
    let mut g = AvInvariantGuard::default();
    g.observe_video(0, 0);
    g.observe_audio(0, AV_INVARIANT_TOLERANCE_MS);
    assert_eq!(g.check(), None, "exactly the tolerance is still OK");
    g.observe_audio(0, AV_INVARIANT_TOLERANCE_MS + 1);
    assert!(
        g.check().is_some(),
        "one ms past the tolerance is a violation"
    );
}

/// Once the relation is back within tolerance the latch clears with ONE
/// Restored edge.
#[test]
fn restored_edge_after_recovery() {
    let mut g = AvInvariantGuard::default();
    g.observe_video(0, 0);
    g.observe_audio(0, 1_000);
    assert!(matches!(g.evaluate(), Some(AvInvariantEvent::Violated(_))));
    g.observe_video(40, 40);
    g.observe_audio(40, 40);
    assert_eq!(
        g.evaluate(),
        Some(AvInvariantEvent::Restored { delta_ms: 0 })
    );
    assert!(!g.is_violated());
    assert_eq!(g.evaluate(), None);
}

/// A legitimate transform change (session re-anchor, reconnect) must drop
/// the old samples. Pairing an old-transform sample of one track with a
/// new-transform sample of the other would be a false violation.
#[test]
fn new_transform_drops_stale_samples() {
    let mut g = AvInvariantGuard::default();
    g.observe_video(600_000, 0);
    g.observe_audio(600_000, 0);
    g.begin_new_transform();
    // Only video seen under the new transform so far: nothing to compare.
    g.observe_video(0, 401);
    assert_eq!(g.check(), None);
    assert_eq!(g.evaluate(), None);
}

/// A full reset of a latched guard reports one Restored edge (the operator
/// banner / Discord episode must close) and clears the latch.
#[test]
fn reset_of_a_latched_guard_restores() {
    let mut g = AvInvariantGuard::default();
    g.observe_video(0, 0);
    g.observe_audio(0, 900);
    assert!(matches!(g.evaluate(), Some(AvInvariantEvent::Violated(_))));
    assert_eq!(
        g.reset(),
        Some(AvInvariantEvent::Restored { delta_ms: 900 })
    );
    assert!(!g.is_violated());
    assert_eq!(g.reset(), None, "resetting an unlatched guard is silent");
}
