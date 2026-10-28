//! The warmup clip's push, with a CLEAN end (#424 go-live repeat).
//!
//! During the initial cache fill every non-fast endpoint shows the warmup
//! clip ("Vysielanie sa o chvíľu spustí…") on its primary and, for YouTube,
//! its backup ingest (#368). When the buffer target is met the consumer
//! publishes the content on fresh sessions to the SAME keys.
//!
//! Before #424 the warmup task was aborted: the primary session vanished with
//! no FCUnpublish/deleteStream, and the dropped backup mirror only aborted its
//! task without waiting, so the content publish on the backup key could open
//! while the warmup backup session was still alive. YouTube then re-settled
//! between the feeds: camera-box measured a repeat of part of a chunk plus
//! single-AAC-frame audio steps ~40 s after the switch, at the same VOD
//! position in two runs.
//!
//! [`WarmupPush`] runs the clip on its OWN stop channel. [`WarmupPush::stop`]
//! signals it and awaits the task, which `finish()`es the pusher: the
//! primary unpublishes and drains its send queue (bounded by
//! `rs_rtmp_push::FINISH_MAX_WAIT`), then the backup mirror is shut down and
//! awaited. Only when that end itself hangs past [`WARMUP_STOP_MAX_WAIT`] is
//! the task aborted, with a warning.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::endpoint_stats::Stats;
use crate::pushable::Pushable;
use crate::rescue_segments::RescueClipSource;
use crate::rust_rescue_push::{RescuePushMode, rescue_push_loop};

/// How long the warmup end may take before the task is aborted: the
/// primary's drain bound plus a margin for the backup's shutdown.
pub(crate) const WARMUP_STOP_MAX_WAIT: Duration =
    Duration::from_secs(rs_rtmp_push::FINISH_MAX_WAIT.as_secs() + 5);

/// How the warmup publish ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarmupEnd {
    /// The pusher was finished: primary unpublished + drained, backup closed.
    Finished,
    /// The end hung past [`WARMUP_STOP_MAX_WAIT`]; the task was aborted.
    Aborted,
    /// The warmup task panicked: its sessions were dropped, not finished.
    Panicked,
}

/// The running warmup clip push of one endpoint.
pub(crate) struct WarmupPush {
    stop_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl WarmupPush {
    /// Start pushing the warmup clip through `pusher` until [`Self::stop`].
    pub(crate) fn spawn<P: Pushable + Send + 'static>(
        pusher: P,
        alias: String,
        source: RescueClipSource,
        stats: Stats,
    ) -> Self {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        // producer_active stays false, so the rescue loop's refill exit can
        // never fire: the warmup probe loop alone decides when the clip ends.
        let buffer_state = Arc::new(crate::buffer_state::BufferState::new());
        buffer_state
            .producer_active
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let task = tokio::spawn(async move {
            let mut pusher = pusher;
            rescue_push_loop(
                &mut pusher,
                &alias,
                source,
                buffer_state,
                stats,
                &mut stop_rx,
                RescuePushMode::Warmup,
            )
            .await;
            tracing::info!(
                alias = %alias,
                "warmup clip stopped; ending its publish (unpublish + drain, backup shutdown) (#424)"
            );
            pusher.finish().await;
        });
        Self { stop_tx, task }
    }

    /// End the warmup publish and wait until both its sessions are gone, so
    /// the content pusher built right after never overlaps them on the same
    /// key. Bounded by [`WARMUP_STOP_MAX_WAIT`].
    pub(crate) async fn stop(self, alias: &str) -> WarmupEnd {
        let started = tokio::time::Instant::now();
        // The receiver lives in the task; an Err means the task already ended.
        let _ = self.stop_tx.send(true);
        let mut task = self.task;
        let end = match tokio::time::timeout(WARMUP_STOP_MAX_WAIT, &mut task).await {
            Ok(Ok(())) => WarmupEnd::Finished,
            Ok(Err(_join_error)) => WarmupEnd::Panicked,
            Err(_elapsed) => {
                task.abort();
                let _ = task.await;
                WarmupEnd::Aborted
            }
        };
        let waited_ms = started.elapsed().as_millis() as u64;
        match end {
            WarmupEnd::Finished => tracing::info!(
                alias,
                waited_ms,
                "warmup publish ended cleanly before the content publish (#424)"
            ),
            WarmupEnd::Aborted => tracing::warn!(
                alias,
                waited_ms,
                max_wait_ms = WARMUP_STOP_MAX_WAIT.as_millis() as u64,
                "warmup publish end hung; task aborted, its sessions dropped without unpublish (#424)"
            ),
            WarmupEnd::Panicked => tracing::error!(
                alias,
                waited_ms,
                "warmup push task panicked; its sessions were dropped without unpublish (#424)"
            ),
        }
        end
    }
}
