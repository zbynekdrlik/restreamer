//! `AuditThrottle` on explicit instants: nothing sleeps.

use super::*;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

const INTERVAL: Duration = Duration::from_secs(10);

#[test]
fn the_first_incident_is_written_with_nothing_held_back() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    assert_eq!(t.admit(t0, 800), Admission::Emit { suppressed: None });
    assert_eq!(t.take_pending(), None);
}

#[test]
fn a_storm_inside_the_interval_is_counted_not_written() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    assert_eq!(t.admit(t0, 800), Admission::Emit { suppressed: None });
    assert_eq!(t.admit(t0 + ms(1_000), 900), Admission::Suppress);
    assert_eq!(t.admit(t0 + ms(2_000), 3_000), Admission::Suppress);
    assert_eq!(t.admit(t0 + ms(9_999), 750), Admission::Suppress);

    // The interval is inclusive: exactly 10 s after the last row, the next
    // incident is written and carries the three held back before it.
    let Admission::Emit {
        suppressed: Some(held),
    } = t.admit(t0 + INTERVAL, 1_200)
    else {
        panic!("the row after the interval is written with the aggregate");
    };
    assert_eq!(
        held,
        Suppressed {
            count: 3,
            max: 3_000,
            total: 4_650,
            first_at: t0 + ms(1_000),
            last_at: t0 + ms(9_999),
        }
    );
    assert_eq!(t.take_pending(), None, "the aggregate was handed over once");
}

#[test]
fn the_interval_restarts_at_every_written_row() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    t.admit(t0, 1);
    assert!(matches!(t.admit(t0 + INTERVAL, 1), Admission::Emit { .. }));
    assert_eq!(
        t.admit(t0 + INTERVAL + ms(9_999), 1),
        Admission::Suppress,
        "10 s after the SECOND row, not after the first"
    );
}

#[test]
fn a_held_back_aggregate_is_flushed_once_the_interval_has_passed() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    t.admit(t0, 800);
    t.admit(t0 + ms(500), 900);
    t.admit(t0 + ms(700), 950);
    assert_eq!(t.take_due(t0 + ms(9_999)), None, "not due yet");
    let held = t.take_due(t0 + INTERVAL).expect("due at the interval");
    assert_eq!((held.count, held.max, held.total), (2, 950, 1_850));
    assert_eq!(t.take_due(t0 + 3 * INTERVAL), None, "flushed once");

    // The flush was that interval's row: the next incident waits for the
    // interval measured from the flush.
    assert_eq!(t.admit(t0 + INTERVAL + ms(1), 1), Admission::Suppress);
}

#[test]
fn nothing_held_back_means_nothing_due() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    assert_eq!(t.take_due(t0), None);
    t.admit(t0, 800);
    assert_eq!(t.take_due(t0 + 5 * INTERVAL), None);
    assert!(
        matches!(t.admit(t0 + 5 * INTERVAL, 1), Admission::Emit { .. }),
        "an empty take_due never consumes the interval"
    );
}

#[test]
fn take_pending_flushes_regardless_of_the_interval() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    t.admit(t0, 800);
    t.admit(t0 + ms(10), 300);
    let held = t.take_pending().expect("held back");
    assert_eq!((held.count, held.max, held.total), (1, 300, 300));
    assert_eq!(t.take_pending(), None);
}

#[test]
fn a_zero_interval_writes_every_row() {
    let mut t = AuditThrottle::new(Duration::ZERO);
    let t0 = Instant::now();
    for i in 0..5 {
        assert_eq!(
            t.admit(t0, i),
            Admission::Emit { suppressed: None },
            "incident {i}"
        );
    }
}

#[test]
fn suppressed_json_names_its_unit_and_span() {
    let t0 = Instant::now();
    let held = Suppressed {
        count: 4,
        max: 1_300,
        total: 3_100,
        first_at: t0,
        last_at: t0 + ms(2_500),
    };
    assert_eq!(
        held.to_json("ms"),
        serde_json::json!({
            "count": 4, "max": 1_300, "total": 3_100, "unit": "ms", "span_ms": 2_500
        })
    );
}

#[test]
fn the_total_saturates_instead_of_overflowing() {
    let mut t = AuditThrottle::new(INTERVAL);
    let t0 = Instant::now();
    t.admit(t0, 0);
    t.admit(t0 + ms(1), u64::MAX);
    t.admit(t0 + ms(2), 5);
    let held = t.take_pending().unwrap();
    assert_eq!((held.count, held.max, held.total), (2, u64::MAX, u64::MAX));
}
