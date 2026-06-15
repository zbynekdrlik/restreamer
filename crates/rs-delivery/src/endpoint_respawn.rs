//! Producer-respawn decision logic (C3 #237), extracted from
//! `endpoint_loop` in `endpoint_task.rs` to keep that file under the
//! 1000-line file-size gate (CI `file-size` job). This is a pure move: the
//! helper performs exactly the producer-stall accounting, audit emit, and
//! resume-chunk computation that previously lived inline in the
//! `result = &mut producer` select arm, and returns a decision the loop acts
//! on. The interactions that touch the pinned `consumer` future and the
//! `spawn_producer` closure stay in `endpoint_loop` (they cannot be moved
//! without borrowing the pinned local) — only the borrow-free bookkeeping is
//! extracted, so behaviour is byte-identical.

use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::Stats;

/// Producer-respawn budget. Each respawn waits `PRODUCER_RESPAWN_BACKOFF` so a
/// producer that keeps dying immediately (e.g. a poisoned fetcher) can't
/// hot-loop. After `MAX_PRODUCER_RESPAWNS` the endpoint gives up and
/// drains/tears down (the orchestrator restarts it from scratch). A producer
/// that survives a healthy stretch resets the budget so a long event with
/// several brief source losses isn't capped.
pub(crate) const MAX_PRODUCER_RESPAWNS: u32 = 1000;
pub(crate) const PRODUCER_RESPAWN_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// What `endpoint_loop` should do after its producer task finished while the
/// consumer is still alive and no stop was signalled.
pub(crate) enum ProducerFinishedDecision {
    /// Respawn budget exhausted — drain the consumer and tear the endpoint
    /// down (the orchestrator restarts it from scratch).
    TearDownBudgetExhausted,
    /// Respawn the producer from `resume_from` after the backoff. The budget
    /// counters in the caller have already been advanced by the helper.
    Respawn { resume_from: i64 },
}

/// Handle the borrow-free bookkeeping of a producer finishing while the
/// consumer is still alive and no stop was signalled (C3 #237):
/// - signal the producer stall so the consumer's rescue gate opens (keeps a
///   watchable preview during the gap),
/// - apply the respawn budget (reset after a healthy stretch, then increment),
/// - compute the resume chunk id (one past the highest delivered-OR-queued
///   chunk, so already-queued chunks are never re-fetched — FIX 1/FIX 3),
/// - emit the `ProducerRespawned` audit row.
///
/// Mutates the caller's `respawns` / `last_respawn_at` budget counters in
/// place so the caller's `MAX_PRODUCER_RESPAWNS` accounting is preserved
/// exactly. Returns the decision the loop acts on. The actual consumer-drain
/// timeout and the backoff+respawn (which touch the pinned `consumer` future
/// and the `spawn_producer` closure) stay inline in `endpoint_loop`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn on_producer_finished(
    alias: &str,
    start_chunk_id: i64,
    stats: &Stats,
    buffer_state: &Arc<BufferState>,
    audit_ring: &Option<Arc<AuditRing>>,
    respawns: &mut u32,
    last_respawn_at: &mut tokio::time::Instant,
) -> ProducerFinishedDecision {
    // C3 (#237): producer exited while the consumer is still alive and no stop
    // was signalled (producer panic, or it broke on a transient consumer-gone
    // false positive). Signal the producer stall so the consumer's rescue gate
    // opens (keeps a watchable preview during the gap), then RESPAWN the
    // producer from one past the highest chunk that is either DELIVERED or
    // already QUEUED in the still-open channel (see the resume computation
    // below — FIX 1/FIX 3) so a returning source refills the buffer without
    // re-fetching queued chunks and rescue's 120s recovery can complete. The
    // channel stays open via the endpoint_loop-scoped `tx`, so the consumer
    // never saw a spurious recv-None.
    buffer_state
        .producer_active
        .store(false, AtomicOrdering::Relaxed);

    // FIX 2 (#237): reset the budget FIRST, then test it. The previous order
    // tested `respawns >= MAX` BEFORE the 60s healthy-stretch reset, so a death
    // that arrives after a long healthy run (which SHOULD reset the budget to 0)
    // could tear the endpoint down instead. Resetting first means a producer
    // that survived a healthy stretch always gets a fresh budget — matching the
    // documented intent in this file's header comment ("A producer that
    // survives a healthy stretch resets the budget").
    if last_respawn_at.elapsed() >= std::time::Duration::from_secs(60) {
        *respawns = 0;
    }

    if *respawns >= MAX_PRODUCER_RESPAWNS {
        tracing::error!(
            alias = %alias,
            respawns = *respawns,
            "Producer respawn budget exhausted; tearing down endpoint for orchestrator restart"
        );
        return ProducerFinishedDecision::TearDownBudgetExhausted;
    }

    *respawns += 1;
    *last_respawn_at = tokio::time::Instant::now();

    // Resume chunk id (FIX 1 + FIX 3, #237):
    //
    // FIX 3 — first-death off-by-one. `current_chunk_id` is seeded to
    // `start_chunk_id` at endpoint init (`initial_endpoint_stats`), so it cannot
    // by itself distinguish "delivered up to start_chunk_id" from "nothing
    // delivered yet". `chunks_processed` (0 until the first successful delivery)
    // is the gate: with nothing delivered, treat last-delivered as
    // `start_chunk_id - 1` so `resume_from` lands exactly on `start_chunk_id`
    // (the old `+ 1` math skipped `start_chunk_id` itself; the `max(start - 1)`
    // clamp was dead because the seed already made `current_chunk_id >=
    // start - 1`).
    //
    // FIX 1 — duplicate chunks. Up to `PREFETCH_BUFFER_SIZE` chunks may already
    // be `tx.send()`'d into the still-open channel but not yet delivered;
    // resuming from last-delivered + 1 would re-fetch those queued chunks and
    // the consumer would deliver them twice (live judder). Resume past the
    // highest SENT id as well (`highest_sent_chunk_id`, -1 when nothing sent).
    let resume_from = {
        let s = stats.lock().await;
        let delivered = if s.chunks_processed == 0 {
            start_chunk_id - 1
        } else {
            s.current_chunk_id
        };
        drop(s);
        let queued = buffer_state
            .highest_sent_chunk_id
            .load(AtomicOrdering::Relaxed);
        delivered.max(queued) + 1
    };
    if let Some(ring) = audit_ring {
        ring.push_parts(crate::audit_ring::RingRowParts {
            severity: rs_core::audit::Severity::Warn,
            source: rs_core::audit::Source::Vps,
            endpoint: Some(alias.to_string()),
            action: rs_core::audit::Action::ProducerRespawned,
            detail: serde_json::json!({
                "resume_from_chunk_id": resume_from,
                "respawn": *respawns,
            }),
        });
    }
    tracing::warn!(
        alias = %alias,
        resume_from,
        respawn = *respawns,
        "Producer finished while consumer alive; respawning after backoff"
    );
    ProducerFinishedDecision::Respawn { resume_from }
}
