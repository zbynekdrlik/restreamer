//! The source stopped on purpose (#408).
//!
//! The operator stopped the stream in OBS while OBS stayed connected. The
//! host announces the last chunk of the source (`POST /api/source`). Every
//! endpoint of the event plays its buffer out to that chunk, then ends its
//! publish and waits, instead of entering rescue: no outage clip, no outage
//! alert. A chunk after the end (OBS published again; the sequence goes on
//! per event) clears it, and the endpoints open a fresh publish.

use std::sync::{Arc, Mutex, MutexGuard};

use rs_core::audit::{Action, Severity, Source};
use rs_core::endpoint_lifecycle::ENDED_DELIVERY_MODE;
use tokio::sync::{mpsc, watch};

use crate::audit_ring::{AuditRing, RingRowParts};
use crate::backup_mirror::{EndpointPusher, PushTarget, build_endpoint_pusher};
use crate::endpoint_stats::Stats;
use crate::endpoint_task::PrefetchedChunk;

#[derive(Debug)]
struct Inner {
    ended_at: Option<i64>,
    highest_fetched: i64,
}

/// The end of the source, shared by every endpoint of one event (it lives on
/// the event's `DiskCache`).
#[derive(Debug, Clone)]
pub struct SourceEnd(Arc<Mutex<Inner>>);

impl Default for SourceEnd {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceEnd {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner {
            ended_at: None,
            highest_fetched: -1,
        })))
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The host's announcement that the source ended at `ended_at`. Refused
    /// (`false`) when a producer already fetched a later chunk: the source
    /// is live again and the announcement is stale.
    pub fn announce(&self, ended_at: i64) -> bool {
        let mut g = self.lock();
        if g.highest_fetched > ended_at {
            return false;
        }
        g.ended_at = Some(ended_at);
        true
    }

    /// The announced last chunk, while the source is stopped.
    pub fn ended_at(&self) -> Option<i64> {
        self.lock().ended_at
    }

    /// A producer fetched `chunk_id`. A chunk past the end means the source
    /// is live again: the end is cleared.
    pub fn note_fetched(&self, chunk_id: i64) {
        let mut g = self.lock();
        g.highest_fetched = g.highest_fetched.max(chunk_id);
        if past_end(g.ended_at, chunk_id) {
            g.ended_at = None;
        }
    }
}

/// The end an endpoint that delivered `last_delivered` has played out to.
pub fn reached(ended_at: Option<i64>, last_delivered: i64) -> Option<i64> {
    ended_at.filter(|&e| last_delivered >= e)
}

/// Whether `chunk_id` lies past the end of the source: a producer miss there
/// is the expected end, not a gap.
pub fn past_end(ended_at: Option<i64>, chunk_id: i64) -> bool {
    ended_at.is_some_and(|e| chunk_id > e)
}

pub fn ended_row(alias: &str, ended_at: i64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Info,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::EndpointSourceEnded,
        detail: serde_json::json!({ "ended_at_chunk_id": ended_at }),
    }
}

pub fn resumed_row(alias: &str, chunk_id: i64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Info,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::EndpointSourceResumed,
        detail: serde_json::json!({ "chunk_id": chunk_id }),
    }
}

/// The endpoint played the source out to `ended_at`: end its publish (the
/// primary and the YouTube backup), show it as ended, and wait for the next
/// chunk. Returns that chunk on a fresh pusher, or `None` on a stop or a
/// closed channel.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn idle_until_resume(
    target: &PushTarget,
    ended_at: i64,
    rx: &mut mpsc::Receiver<PrefetchedChunk>,
    stop_rx: &mut watch::Receiver<bool>,
    stats: &Stats,
    audit_ring: &Option<Arc<AuditRing>>,
    rust_pusher: &mut Option<EndpointPusher>,
) -> Option<PrefetchedChunk> {
    let alias = target.alias.as_str();
    if let Some(mut p) = rust_pusher.take() {
        p.finish().await;
    }
    stats.lock().await.delivery_mode = ENDED_DELIVERY_MODE.to_string();
    if let Some(r) = audit_ring {
        r.push_parts(ended_row(alias, ended_at));
    }
    tracing::info!(
        alias,
        ended_at,
        "source ended on purpose: publish closed, waiting for the source"
    );

    let chunk = loop {
        tokio::select! {
            c = rx.recv() => break c,
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    break None;
                }
            }
        }
    }?;

    *rust_pusher = Some(build_endpoint_pusher(target, stats).await);
    stats.lock().await.delivery_mode = "normal".to_string();
    if let Some(r) = audit_ring {
        r.push_parts(resumed_row(alias, chunk.chunk_id));
    }
    tracing::info!(
        alias,
        chunk = chunk.chunk_id,
        "source is back: fresh publish"
    );
    Some(chunk)
}

#[cfg(test)]
#[path = "source_end_tests.rs"]
mod tests;
