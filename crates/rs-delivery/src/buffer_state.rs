//! Shared state types between producer and consumer tasks, plus pure
//! helpers for delivery mode decisions.
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};

/// Shared buffer state between producer and consumer for rescue mode.
pub struct BufferState {
    /// Estimated buffer duration in ms (chunks available on S3 ahead of consumer).
    /// Note: bounded by the prefetch channel capacity in practice; rescue mode
    /// uses a time-based exit instead of reading this directly.
    pub buffer_duration_ms: AtomicU64,
    /// Whether the producer is actively finding new chunks (vs stalled).
    pub producer_active: AtomicBool,
    /// Largest starvation gap (ms) observed by the consumer's keepalive since
    /// the producer last consumed it. Written with fetch_max on keepalive end,
    /// swapped to 0 by the producer, which grows the adaptive read-delay by it.
    pub starvation_gap_ms: AtomicU64,
    /// Highest chunk_id the producer has successfully `tx.send()`'d into the
    /// prefetch channel (C3 #237 FIX 1). Updated with `fetch_max` right after a
    /// successful send. On a producer respawn the channel is still open and
    /// holds up to `PREFETCH_BUFFER_SIZE` already-queued-but-not-yet-delivered
    /// chunks; resuming from `current_chunk_id + 1` (last DELIVERED) would
    /// re-fetch those queued chunks and the consumer would deliver them twice
    /// (live judder). The respawn resume point takes the MAX of last-delivered
    /// and this value so queued chunks are never re-fetched. Init to -1 so the
    /// "nothing sent yet" case clamps correctly (see endpoint_respawn FIX 3).
    pub highest_sent_chunk_id: AtomicI64,
    /// #294: the fast endpoint's ratcheted read-delay target (seconds).
    /// Persisted here — outside the per-producer-task `FastDelayController` —
    /// so a #237 producer RESPAWN resumes at the level the session's real
    /// drains already climbed to, instead of resetting to the floor and
    /// re-starving (which would reintroduce the repeated stuttering #294
    /// exists to eliminate). 0 = unset → the controller starts at its floor.
    /// Only the fast producer writes it; monotonic within a session (grows
    /// with the target, never lowered — the ratchet-up-only invariant).
    pub fast_delay_target_secs: AtomicU64,
    /// #296: how far (seconds) a BUFFERED (non-fast) endpoint's delivered
    /// cushion currently sits BELOW its configured target. 0 = at/above
    /// target (nothing to refill) — the steady state. Written by the
    /// producer's periodic live-edge HEAD-probe (`producer_lag`), read by the
    /// consumer to decide whether to deliver marginally slower than realtime
    /// (0.98x) and rebuild the cushion after a source gap (`refill` module).
    /// Mirrors `fast_delay_target_secs` in shape (an AtomicU64 on the shared
    /// state) but the opposite direction: fast endpoints RAISE a read-delay
    /// target; buffered endpoints REFILL a drained delivery cushion. Fast
    /// endpoints never write it (they use the #294 ratchet instead), so it
    /// stays 0 for them and the consumer's throttle is gated `!is_fast`.
    /// Cleared to 0 by the producer when it goes stalled (outage) so no
    /// throttle fires while there is no source to rebuild from.
    pub refill_deficit_secs: AtomicU64,
}

impl BufferState {
    pub fn new() -> Self {
        Self {
            buffer_duration_ms: AtomicU64::new(0),
            producer_active: AtomicBool::new(true),
            starvation_gap_ms: AtomicU64::new(0),
            highest_sent_chunk_id: AtomicI64::new(-1),
            fast_delay_target_secs: AtomicU64::new(0),
            refill_deficit_secs: AtomicU64::new(0),
        }
    }
}

impl Default for BufferState {
    fn default() -> Self {
        Self::new()
    }
}

/// Determine the initial delivery mode for a new endpoint based on its
/// configuration. "warmup" only applies when rescue video is configured
/// AND the endpoint is not fast AND there's a cache window to fill;
/// otherwise "normal" (or effectively "skip warmup" for fast endpoints).
pub fn initial_delivery_mode(
    has_rescue_video: bool,
    is_fast: bool,
    delivery_delay_ms: u64,
) -> String {
    if has_rescue_video && !is_fast && delivery_delay_ms > 0 {
        "warmup".to_string()
    } else {
        "normal".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn buffer_state_default_duration_zero() {
        let bs = BufferState::new();
        assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn buffer_state_tracks_duration() {
        let bs = BufferState::new();
        bs.buffer_duration_ms.store(5000, Ordering::Relaxed);
        assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 5000);
    }

    #[test]
    fn buffer_state_producer_active_default_true() {
        let bs = BufferState::new();
        assert!(bs.producer_active.load(Ordering::Relaxed));
    }

    #[test]
    fn buffer_state_producer_stall_detection() {
        let bs = BufferState::new();
        bs.producer_active.store(false, Ordering::Relaxed);
        assert!(!bs.producer_active.load(Ordering::Relaxed));
    }

    #[test]
    fn initial_mode_warmup_when_all_conditions_met() {
        assert_eq!(initial_delivery_mode(true, false, 120_000), "warmup");
    }

    #[test]
    fn initial_mode_normal_when_no_rescue_video() {
        assert_eq!(initial_delivery_mode(false, false, 120_000), "normal");
    }

    #[test]
    fn initial_mode_normal_when_fast_endpoint() {
        // Fast endpoints never enter warmup — they run near-live
        assert_eq!(initial_delivery_mode(true, true, 120_000), "normal");
    }

    #[test]
    fn initial_mode_normal_when_zero_delay() {
        // Zero delay means no cache window to fill, so no warmup
        assert_eq!(initial_delivery_mode(true, false, 0), "normal");
    }
}
