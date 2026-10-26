//! Cushion deficit of a BUFFERED (non-fast) endpoint (#296, #424).
//!
//! A buffered endpoint's cache cushion is a **time** cushion: it delivers
//! content that is `delivery_delay` seconds behind the live edge. Content is
//! produced and consumed at 1x real time, so once a source gap lets the
//! consumer eat into the cushion, it stays lower for the rest of the event.
//!
//! #296 rebuilt it by delivering at 0.98x of real time. That never acted
//! until #424 (the pusher's pacing caught up the sleep), and once it did,
//! YouTube flagged every refill window as `videoIngestionStarved` (health
//! bad). Owner decision 2026-10-26: the outgoing stream never runs slower
//! or faster than real time. The deficit is measured and audited
//! (`buffer_refill_started` / `_ended`, `producer_lag`) as information only.

/// Deficit (in CHUNKS) of a buffered endpoint whose live edge is `max_id` and
/// whose read/deliver pointer is `current`, for a configured cushion of
/// `target_chunks`. The cushion is `max_id - current`; the deficit is how many
/// chunks that falls SHORT of the target. Returns 0 when the cushion is at or
/// above target (the steady state — nothing to refill) or when the inputs are
/// nonsensical (negative target, pointer ahead of the live edge).
pub fn refill_deficit_chunks(max_id: i64, current: i64, target_chunks: i64) -> u64 {
    if target_chunks <= 0 {
        return 0;
    }
    // Live edge behind the read pointer is a nonsensical / transient
    // (clock-race) reading — treat it as "cannot determine a deficit", never
    // as a full deficit that would wrongly throttle. `max_id == current` (fully
    // drained AT the live edge) is legitimate and DOES yield the full deficit.
    if max_id < current {
        return 0;
    }
    let cushion = max_id - current;
    (target_chunks - cushion).max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deficit_chunks_zero_when_cushion_at_or_above_target() {
        // cushion == target → no deficit
        assert_eq!(refill_deficit_chunks(100 + 150, 100, 150), 0);
        // cushion > target (excess lag — maybe_jump's job, not refill's)
        assert_eq!(refill_deficit_chunks(100 + 300, 100, 150), 0);
        // pointer ahead of live edge (clock race) → clamped, no deficit
        assert_eq!(refill_deficit_chunks(90, 100, 150), 0);
    }

    #[test]
    fn deficit_chunks_positive_when_below_target() {
        // cushion 50, target 150 → deficit 100
        assert_eq!(refill_deficit_chunks(100 + 50, 100, 150), 100);
        // fully drained to the live edge → deficit == full target
        assert_eq!(refill_deficit_chunks(100, 100, 150), 150);
        // cushion 1 → deficit 149
        assert_eq!(refill_deficit_chunks(101, 100, 150), 149);
    }

    #[test]
    fn deficit_chunks_zero_when_target_nonpositive() {
        // fast endpoints (no delay) never have a refill target
        assert_eq!(refill_deficit_chunks(500, 100, 0), 0);
        assert_eq!(refill_deficit_chunks(500, 100, -5), 0);
    }
}
