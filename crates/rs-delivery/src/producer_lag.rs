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
