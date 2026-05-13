//! Quota tracker contract: per-project sliding window, refill semantics,
//! exhaust + recover.

use crate::quota::{QuotaExhausted, QuotaTracker};
use std::time::Duration;

#[test]
fn acquire_under_budget_succeeds() {
    let q = QuotaTracker::new(100);
    for _ in 0..50 {
        assert!(q.acquire(1).is_ok());
    }
    assert_eq!(q.remaining(), 50);
}

#[test]
fn acquire_over_budget_returns_exhausted() {
    let q = QuotaTracker::new(10);
    for _ in 0..10 {
        q.acquire(1).unwrap();
    }
    match q.acquire(1) {
        Err(QuotaExhausted) => (),
        Ok(()) => panic!("expected QuotaExhausted"),
    }
}

#[test]
fn refill_restores_units_over_time() {
    // 100 units/day = 100 / 86_400 ≈ 0.00116 units/sec.
    // Use a larger budget so the test exercises refill quickly.
    let q = QuotaTracker::new(8640); // 0.1 units/sec
    for _ in 0..8640 {
        q.acquire(1).unwrap();
    }
    assert!(q.acquire(1).is_err());
    // Travel forward 20 seconds in test time.
    q.advance_for_test(Duration::from_secs(20));
    // 20s * 0.1 units/sec = 2 units refilled.
    assert!(q.acquire(1).is_ok());
    assert!(q.acquire(1).is_ok());
    assert!(q.acquire(1).is_err());
}

#[test]
fn remaining_clamps_to_budget() {
    let q = QuotaTracker::new(10);
    // Don't acquire anything; refill should not push above budget.
    q.advance_for_test(Duration::from_secs(86_400));
    assert_eq!(q.remaining(), 10);
}
