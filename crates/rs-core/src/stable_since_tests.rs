//! #368: the lock-free publisher-stable cell.

use super::*;

#[test]
fn a_new_cell_has_no_publisher() {
    for cell in [StableSince::new(), StableSince::default()] {
        assert_eq!(cell.get(), None);
        assert_eq!(cell.stable_secs(), 0);
    }
}

/// A publisher that became stable before the cell existed (every API test
/// sets "stable for 120 s") reads back exactly, as does one after it.
#[test]
fn an_instant_on_either_side_of_the_anchor_reads_back_exactly() {
    let cell = StableSince::new();
    let earlier = Instant::now() - Duration::from_secs(120);
    cell.set(Some(earlier));
    assert_eq!(cell.get(), Some(earlier));
    assert_eq!(cell.stable_secs(), 120);

    let later = Instant::now() + Duration::from_millis(1_500);
    cell.set(Some(later));
    assert_eq!(cell.get(), Some(later));
    assert_eq!(
        cell.stable_secs(),
        0,
        "a future instant has not elapsed yet"
    );
}

#[test]
fn clearing_the_cell_forgets_the_publisher() {
    let cell = StableSince::new();
    cell.set(Some(Instant::now() - Duration::from_secs(30)));
    assert_eq!(cell.stable_secs(), 30);
    cell.set(None);
    assert_eq!(cell.get(), None);
    assert_eq!(cell.stable_secs(), 0);
}

/// The ingest thread writes one clone, the API reads another.
#[test]
fn clones_share_one_cell() {
    let ingest = StableSince::new();
    let api = ingest.clone();
    let at = Instant::now() - Duration::from_secs(16);
    ingest.set(Some(at));
    assert_eq!(api.get(), Some(at));
    assert_eq!(api.stable_secs(), 16);
    assert!(api.ptr_eq(&ingest));
    assert!(!api.ptr_eq(&StableSince::new()));
}

#[test]
fn offsets_are_signed_nanoseconds_from_the_anchor() {
    let anchor = Instant::now();
    assert_eq!(
        signed_offset_ns(anchor, anchor + Duration::from_nanos(1_500)),
        1_500
    );
    assert_eq!(
        signed_offset_ns(anchor, anchor - Duration::from_nanos(2_500)),
        -2_500
    );
    assert_eq!(signed_offset_ns(anchor, anchor), 0);
    assert_eq!(
        instant_at(anchor, 1_500),
        anchor + Duration::from_nanos(1_500)
    );
    assert_eq!(
        instant_at(anchor, -2_500),
        anchor - Duration::from_nanos(2_500)
    );
    assert_eq!(instant_at(anchor, 0), anchor);
}

/// A stored offset is clamped to +/-i64::MAX, so even an absurd one can
/// never be read as "no publisher".
#[test]
fn a_stored_offset_never_means_no_publisher() {
    assert_eq!(stored_offset(7), 7);
    assert_eq!(stored_offset(-7), -7);
    assert_eq!(stored_offset(i128::MAX), i64::MAX);
    assert_eq!(stored_offset(i128::MIN), -i64::MAX);
    assert_ne!(stored_offset(i128::from(i64::MIN)), NO_PUBLISHER);
}
