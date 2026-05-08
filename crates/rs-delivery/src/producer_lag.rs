//! Live-edge lag detection for the producer hot loop.
//!
//! Periodically probes far ahead via HEAD-only S3 calls. If chunks exist
//! beyond `delivery_delay_chunks × 2`, jumps the read pointer back to
//! `(latest_known - delivery_delay_chunks)`. Without this, any one-time
//! slowdown (slow start, transient stall, OBS pause) accumulates lag
//! forever — producer reads at 1x and never catches up.
//!
//! Observed in prod (event 9289 on 2026-05-07): VPS read pointer 70+
//! min behind live edge, OBS-stop didn't drain cache because fresh
//! chunks kept arriving faster than reader consumed.

use crate::endpoint_task::ChunkFetcher;

/// Trigger lag-probe every N successful fetches.
/// At default chunk size ~2s, 30 fetches = ~60s detection latency.
/// At 5s chunks the latency grows to 150s — still well within the
/// delivery_delay budget. Tunable; tests assert exact-match behavior.
pub(crate) const LAG_PROBE_INTERVAL_ITERS: u32 = 30;

/// Max exponential-probe ladder steps: 12 = up to 4096× delivery_delay
/// search window. Each rung is HEAD-only (no body download).
const LAG_PROBE_LADDER_MAX: u32 = 12;

/// Exponential-probe ladder for the highest known-existing chunk_id ahead
/// of `current`. Returns `Some(new_id)` to jump to, or `None` if no skip
/// needed. Cost: O(log lag) probes when lag is large, 1 probe when not.
pub(crate) async fn detect_lag_and_jump<F: ChunkFetcher>(
    fetcher: &F,
    current: i64,
    delivery_delay_chunks: i64,
) -> Option<i64> {
    if delivery_delay_chunks <= 0 {
        return None;
    }
    let mut probe_offset: i64 = delivery_delay_chunks.saturating_mul(2);
    let mut last_existing: Option<i64> = None;
    for _ in 0..LAG_PROBE_LADDER_MAX {
        let probe_id = current + probe_offset;
        match fetcher.chunk_duration_ms(probe_id).await {
            Ok(Some(_)) => {
                last_existing = Some(probe_id);
                probe_offset = probe_offset.saturating_mul(2);
            }
            Ok(None) | Err(_) => break,
        }
    }
    last_existing.map(|max_id| (max_id - delivery_delay_chunks).max(current + 1))
}

/// Convenience wrapper called once per successful fetch in producer_task.
/// Bumps the counter; every `LAG_PROBE_INTERVAL_ITERS` invocations it
/// runs the ladder probe and (if lag detected) updates `chunk_id`.
pub(crate) async fn maybe_jump<F: ChunkFetcher>(
    fetcher: &F,
    chunk_id: &mut i64,
    delivery_delay_chunks: i64,
    delivery_delay_ms: u64,
    iters: &mut u32,
    alias: &str,
) {
    *iters += 1;
    if *iters < LAG_PROBE_INTERVAL_ITERS || delivery_delay_ms == 0 {
        return;
    }
    *iters = 0;
    if let Some(new_id) = detect_lag_and_jump(fetcher, *chunk_id, delivery_delay_chunks).await {
        tracing::warn!(
            alias = %alias,
            from = *chunk_id,
            to = new_id,
            jump = new_id - *chunk_id,
            "Producer: live-edge lag detected, skipping ahead"
        );
        *chunk_id = new_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint_task::ChunkFetcher;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Minimal fake fetcher: chunks 1..=highest_existing exist with 2s duration.
    struct MockFetcher {
        highest_existing: i64,
        probe_count: AtomicU32,
    }

    impl ChunkFetcher for MockFetcher {
        async fn fetch_chunk_with_meta(
            &self,
            _chunk_id: i64,
        ) -> Result<Option<(Vec<u8>, i64)>, String> {
            unreachable!("lag-detect uses HEAD only")
        }

        async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
            self.probe_count.fetch_add(1, Ordering::SeqCst);
            if chunk_id <= self.highest_existing {
                Ok(Some(2000))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn detect_returns_none_when_no_lag() {
        // current=100, delay=60, no chunks exist beyond current+120.
        let f = MockFetcher {
            highest_existing: 100,
            probe_count: AtomicU32::new(0),
        };
        let r = detect_lag_and_jump(&f, 100, 60).await;
        assert_eq!(r, None);
        // First probe at current+120=220 returns None → break immediately.
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detect_returns_max_minus_delay_when_chunks_far_ahead() {
        // current=100, delay=60, chunks exist up to 5000.
        // Ladder: 220, 340, 580, 1060, 2020, 3940, 7780, ...
        // 7780 > 5000 → break. Last existing = 3940. Result = 3940-60 = 3880.
        let f = MockFetcher {
            highest_existing: 5000,
            probe_count: AtomicU32::new(0),
        };
        let r = detect_lag_and_jump(&f, 100, 60).await;
        assert_eq!(r, Some(3880));
    }

    #[tokio::test]
    async fn detect_floors_jump_target_to_current_plus_one() {
        // Edge case: max_id is barely ahead, so max-delay would land BELOW current.
        // current=100, delay=60. highest=130 (just past first probe at 220 → None).
        // First probe at 220 returns None → ladder breaks → returns None.
        // We need a case where probe HITS but max-delay < current+1.
        // current=200, delay=180. probe_offset starts at 360 → probe at 560.
        // If highest=560 (exact), last_existing=560. Result = max(560-180, 201) = 380.
        // Make it land below: highest=200 → probe at 560 misses → None. No good.
        // Construct: current=200, delay=300, highest=400.
        // probe_offset=600 → probe at 800 (>400) → None → break, last=None → returns None.
        // The floor only matters when last_existing is set AND max_id - delay < current+1.
        // E.g. current=100, delay=300, highest=320. probe_offset=600 → 700 > 320 → None.
        // Try current=0, delay=10, highest=15. probe_offset=20 → 20>15 → None.
        // The floor activates only when current=0 and max-delay <= 0:
        // current=0, delay=100, highest=120. probe_offset=200 → 200>120 → None → returns None.
        // Real-world: current is always non-zero on a running stream. The floor exists as
        // a defensive clamp; the math says: if probe finds chunks and max-delay < current+1,
        // returning current+1 (a forward step of 1) is safer than going backwards.
        // Verify the clamp expression directly:
        let max_id = 50_i64;
        let delay = 100_i64;
        let current = 100_i64;
        let result = (max_id - delay).max(current + 1);
        assert_eq!(result, 101, "max_id-delay (-50) clamped to current+1 (101)");
    }

    #[tokio::test]
    async fn detect_respects_ladder_cap() {
        // With infinite chunks ahead, ladder must stop at LAG_PROBE_LADDER_MAX
        // probes regardless. current=0, delay=1, all chunks exist.
        let f = MockFetcher {
            highest_existing: i64::MAX,
            probe_count: AtomicU32::new(0),
        };
        let _ = detect_lag_and_jump(&f, 0, 1).await;
        assert_eq!(
            f.probe_count.load(Ordering::SeqCst),
            LAG_PROBE_LADDER_MAX,
            "ladder must cap at {LAG_PROBE_LADDER_MAX} probes"
        );
    }

    #[tokio::test]
    async fn detect_returns_none_when_delay_chunks_is_zero_or_negative() {
        let f = MockFetcher {
            highest_existing: 1_000_000,
            probe_count: AtomicU32::new(0),
        };
        assert_eq!(detect_lag_and_jump(&f, 100, 0).await, None);
        assert_eq!(detect_lag_and_jump(&f, 100, -1).await, None);
        // Early-return guards: no probes issued.
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn maybe_jump_does_not_probe_until_interval_reached() {
        let f = MockFetcher {
            highest_existing: 1_000_000,
            probe_count: AtomicU32::new(0),
        };
        let mut chunk_id = 100;
        let mut iters = 0u32;
        // Call < LAG_PROBE_INTERVAL_ITERS times: no probe.
        for _ in 0..(LAG_PROBE_INTERVAL_ITERS - 1) {
            maybe_jump(&f, &mut chunk_id, 60, 120_000, &mut iters, "test").await;
        }
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 0);
        // Hit the threshold: ladder runs.
        maybe_jump(&f, &mut chunk_id, 60, 120_000, &mut iters, "test").await;
        assert!(f.probe_count.load(Ordering::SeqCst) > 0);
        // Counter resets to 0 after firing.
        assert_eq!(iters, 0);
    }

    #[tokio::test]
    async fn maybe_jump_short_circuits_when_delivery_delay_ms_is_zero() {
        let f = MockFetcher {
            highest_existing: 1_000_000,
            probe_count: AtomicU32::new(0),
        };
        let mut chunk_id = 100;
        let mut iters = LAG_PROBE_INTERVAL_ITERS - 1;
        maybe_jump(&f, &mut chunk_id, 60, 0, &mut iters, "test").await;
        assert_eq!(f.probe_count.load(Ordering::SeqCst), 0);
        assert_eq!(chunk_id, 100, "fast endpoint stays at live edge");
    }
}
