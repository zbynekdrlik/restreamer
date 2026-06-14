//! Producer half of the per-endpoint delivery pipeline.
//!
//! Extracted from `endpoint_task.rs` so that file stays under the 1000-line
//! CI cap while `consumer_task` keeps room to grow. This is a pure move of
//! the `producer_task` async fn — no behaviour change. The producer fetches
//! chunks from S3 (via the `ChunkFetcher`) and sends them into the bounded
//! mpsc channel that the consumer drains.

use std::sync::{Arc, atomic::Ordering as AtomicOrdering};
use tokio::sync::{mpsc, watch};

use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::Stats;
use crate::endpoint_task::{
    ChunkFetcher, MAX_CHUNK_MISS_COUNT, PrefetchedChunk, S3_BACKOFF_BASE_SECS, S3_BACKOFF_MAX_SECS,
    SKIP_AHEAD_PROBE,
};
use crate::fast_delay::FastDelayController;
use crate::producer_lag::maybe_jump as maybe_jump_ahead;

/// #253: consecutive `Err(_)` S3 fetches before the producer signals stalled
/// (`producer_active=false`). Mirrors the `Ok(None) >= 3` clean-404 logic.
/// S3 errors back off exponentially (2s, 4s, …), so 3 consecutive errors is
/// ~6-14s of genuinely failing fetches — long enough to be a real outage
/// (wedged S3, connection reset) rather than a single transient blip, which
/// self-heals on the next successful fetch (the success arm resets the
/// counter and restores `producer_active=true`).
pub(crate) const MAX_CONSECUTIVE_FETCH_ERRORS: u32 = 3;

/// Producer task: fetches chunks from S3 and sends them into the bounded channel.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn producer_task<F: ChunkFetcher>(
    fetcher: F,
    tx: mpsc::Sender<PrefetchedChunk>,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    is_fast: bool,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    alias: String,
    buffer_state: Arc<BufferState>,
    audit_ring: Option<Arc<AuditRing>>,
) {
    let mut chunk_id = start_chunk_id;
    let mut consecutive_chunk_misses: u32 = 0;
    // #253: consecutive S3 fetch ERRORS (the `Err(_)` arm). Mirrors
    // `consecutive_chunk_misses` for the `Ok(None)` arm. When errors persist
    // past `MAX_CONSECUTIVE_FETCH_ERRORS`, the producer flips
    // `producer_active=false` so the consumer's `run_outage_rescue` gate
    // (which is `!producer_active`) opens on an error-shaped drain — not only
    // on the clean-404 (`Ok(None)`) drain. Pre-#253 the Err arm never touched
    // `producer_active`, so a wedged-S3 / connection-error outage left the
    // gate stuck `true` and rescue never fired.
    let mut consecutive_fetch_errors: u32 = 0;
    let mut s3_backoff_secs: u64 = S3_BACKOFF_BASE_SECS;
    // Issue #173: rate-limited audit-row emitter, owned by this task.
    let mut s3_fetch_audit = crate::endpoint_audit::S3FetchAuditLimiter::new();
    // Lag-detect state. typical_chunk_dur_ms is updated from observed
    // `duration_ms` so it tracks operator config without a hardcode.
    let mut typical_chunk_dur_ms: u64 = 1000;
    let mut iters_since_lag_probe: u32 = 0;
    // Adaptive read-delay controller — fast endpoints only. Grows the
    // read-delay on starvation (so the live-edge lag-probe leaves a buffer
    // instead of re-pinning to the edge) and shrinks slowly when healthy.
    // None for non-fast endpoints → byte-for-byte unchanged behaviour.
    let mut fast_delay = if is_fast {
        Some(FastDelayController::new(std::time::Instant::now()))
    } else {
        None
    };

    loop {
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Producer: stop signal received");
            break;
        }

        // Fetch chunk with metadata in one S3 GET
        match fetcher.fetch_chunk_with_meta(chunk_id).await {
            Ok(Some((data, duration_ms))) => {
                consecutive_chunk_misses = 0;
                consecutive_fetch_errors = 0;
                s3_backoff_secs = S3_BACKOFF_BASE_SECS;
                {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = 0;
                    if s.stall_reason.as_deref() == Some("chunk_gap") {
                        s.stall_reason = None;
                    }
                }

                let chunk = PrefetchedChunk {
                    chunk_id,
                    data,
                    duration_ms,
                };

                // Track buffer growth for rescue mode
                let current_buf = buffer_state
                    .buffer_duration_ms
                    .load(AtomicOrdering::Relaxed);
                buffer_state.buffer_duration_ms.store(
                    current_buf.saturating_add(duration_ms.max(0) as u64),
                    AtomicOrdering::Relaxed,
                );
                buffer_state
                    .producer_active
                    .store(true, AtomicOrdering::Relaxed);

                // Send into channel; blocks if buffer full (backpressure).
                // If receiver is dropped (consumer gone), stop.
                if tx.send(chunk).await.is_err() {
                    tracing::info!(alias = %alias, "Producer: consumer gone, stopping");
                    break;
                }

                chunk_id += 1;
                // EWMA + [500,5000]ms clamp guards against outlier duration_ms.
                if duration_ms > 0 {
                    let c = (duration_ms as u64).clamp(500, 5000);
                    typical_chunk_dur_ms = (3 * typical_chunk_dur_ms + c) / 4;
                }
                // Fast endpoints: read-delay is ADAPTIVE via the controller. On
                // every healthy fetch we let it opportunistically shrink one step
                // toward the floor; `delay_chunks()` is always >= 1 so the
                // live-edge lag-probe trails the edge and keeps a buffer (#232,
                // adaptive controller). Non-fast endpoints keep the prior exact
                // math: delay_ms==0 → live edge (0), else >=1 floor. RTMP push
                // stays strictly 1× — this only moves the READ pointer.
                let delivery_delay_chunks: i64 = match fast_delay.as_mut() {
                    Some(ctrl) => {
                        // Consumer-measured starvation gap (keepalive) — the
                        // trickle-regime grow signal. The probe-cycle grow
                        // below only fires after ~80s of uninterrupted misses;
                        // this one fires after ANY real keepalive gap, so the
                        // first freeze of an event grows the buffer and the
                        // next spike of that size is absorbed silently.
                        let gap_ms = buffer_state
                            .starvation_gap_ms
                            .swap(0, AtomicOrdering::Relaxed);
                        if gap_ms > 0 {
                            let gap_secs = gap_ms.div_ceil(1000);
                            if let Some((from, to)) =
                                ctrl.on_starvation(gap_secs, std::time::Instant::now())
                            {
                                crate::fast_delay_audit::emit_delay_grown(
                                    &audit_ring,
                                    &alias,
                                    from,
                                    to,
                                    gap_secs,
                                );
                            }
                        }
                        if let Some((from, to)) = ctrl.on_healthy(std::time::Instant::now()) {
                            crate::fast_delay_audit::emit_delay_shrank(
                                &audit_ring,
                                &alias,
                                from,
                                to,
                            );
                        }
                        ctrl.delay_chunks(typical_chunk_dur_ms)
                    }
                    None if delivery_delay_ms == 0 => 0,
                    None => ((delivery_delay_ms / typical_chunk_dur_ms.max(1)) as i64).max(1),
                };
                maybe_jump_ahead(
                    &fetcher,
                    &mut chunk_id,
                    delivery_delay_chunks,
                    delivery_delay_ms,
                    &mut iters_since_lag_probe,
                    &alias,
                )
                .await;
                tokio::task::yield_now().await;
            }
            Ok(None) => {
                consecutive_chunk_misses += 1;
                // A clean 404 is not a fetch error — reset the error counter
                // so an interleaved 404/error pattern doesn't double-count.
                consecutive_fetch_errors = 0;

                // Chunk gap skip-ahead logic
                if consecutive_chunk_misses >= MAX_CHUNK_MISS_COUNT {
                    // Captured BEFORE the probe loop mutates chunk_id, so the
                    // controller can measure how far ahead we had to skip
                    // (the read-side deficit) when a gap was crossed.
                    let stuck_at = chunk_id;
                    tracing::warn!(
                        alias = %alias,
                        chunk_id,
                        misses = consecutive_chunk_misses,
                        "Producer: probing ahead for chunks"
                    );

                    let mut found_ahead = false;
                    for offset in 1..=SKIP_AHEAD_PROBE {
                        let probe_id = chunk_id + offset;
                        // Use HEAD (duration check) instead of GET to avoid downloading data
                        if let Ok(Some(_)) = fetcher.chunk_duration_ms(probe_id).await {
                            tracing::info!(
                                alias = %alias,
                                from = chunk_id,
                                to = probe_id,
                                "Producer: skipping ahead to chunk"
                            );
                            chunk_id = probe_id;
                            consecutive_chunk_misses = 0;
                            let mut s = stats.lock().await;
                            s.consecutive_chunk_misses = 0;
                            s.stall_reason = None;
                            found_ahead = true;
                            break;
                        }
                    }

                    if !found_ahead {
                        let mut s = stats.lock().await;
                        s.stall_reason = Some("chunk_gap".to_string());
                        s.consecutive_chunk_misses = consecutive_chunk_misses;
                        drop(s);
                        tracing::warn!(
                            alias = %alias,
                            chunk_id,
                            "Producer: no chunks found in probe range"
                        );
                        // Reset counter so we probe again after another cycle
                        consecutive_chunk_misses = 0;
                    }

                    // Fast endpoints only: a starvation event fired (probe
                    // cycle). Grow the adaptive read-delay so the live-edge
                    // lag-probe trails the edge by enough to absorb the spike.
                    // `deficit_secs` is the media-time gap we had to skip when
                    // a forward chunk was found; 0 when the gap was unbounded
                    // (no chunk found → grow by the floor margin only). Runs
                    // ONLY on the probe cycle, never on every miss.
                    if let Some(ctrl) = fast_delay.as_mut() {
                        let deficit_secs = if found_ahead {
                            ((chunk_id - stuck_at).max(0) as u64)
                                .saturating_mul(typical_chunk_dur_ms)
                                / 1000
                        } else {
                            0
                        };
                        if let Some((from, to)) =
                            ctrl.on_starvation(deficit_secs, std::time::Instant::now())
                        {
                            crate::fast_delay_audit::emit_delay_grown(
                                &audit_ring,
                                &alias,
                                from,
                                to,
                                deficit_secs,
                            );
                        }
                    }
                } else {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = consecutive_chunk_misses;
                }

                // Signal producer stall for rescue mode detection.
                // Polls are 2s apart, so 3 misses = ~6s of genuinely no new
                // chunks on S3. Triggering sooner means rescue activates
                // faster after OBS stops, at the cost of occasional false
                // positives on transient S3 errors (which self-heal on the
                // next successful fetch — producer_active returns to true).
                if consecutive_chunk_misses >= 3 {
                    buffer_state
                        .producer_active
                        .store(false, AtomicOrdering::Relaxed);
                }

                tracing::debug!(alias = %alias, chunk_id, "Producer: chunk not found, waiting");
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
            }
            Err(e) => {
                consecutive_fetch_errors += 1;
                tracing::error!(
                    alias = %alias,
                    chunk_id,
                    backoff_secs = s3_backoff_secs,
                    consecutive_fetch_errors,
                    "Producer: S3 fetch error, retrying in {s3_backoff_secs}s: {e}"
                );
                // Issue #173: emit audit row (rate-limited per error_class).
                s3_fetch_audit.try_emit(&audit_ring, &alias, chunk_id, &e, s3_backoff_secs);
                {
                    let mut s = stats.lock().await;
                    s.last_error = Some(e);
                }
                // #253: signal producer stall for rescue-mode detection on an
                // ERROR-shaped drain (wedged S3, connection reset). The Ok(None)
                // arm already flips this on >=3 clean 404s; before this fix the
                // Err arm never touched `producer_active`, so the consumer's
                // `run_outage_rescue` gate (`!producer_active`) stayed shut and
                // rescue never fired when the outage manifested as fetch errors
                // rather than 404s (the 2026-05-30 stream.lan crash class). The
                // next successful fetch resets the counter and restores
                // `producer_active=true` (self-heal on transient errors).
                if consecutive_fetch_errors >= MAX_CONSECUTIVE_FETCH_ERRORS {
                    buffer_state
                        .producer_active
                        .store(false, AtomicOrdering::Relaxed);
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(s3_backoff_secs)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
                s3_backoff_secs = (s3_backoff_secs * 2).min(S3_BACKOFF_MAX_SECS);
            }
        }
    }

    tracing::info!(alias = %alias, "Producer task stopped");
}
