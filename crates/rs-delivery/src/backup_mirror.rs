//! YouTube backup ingest mirror (#368).
//!
//! YouTube documents a BACKUP ingest per stream (`backupIngestionAddress`,
//! the same key) that is fed IN PARALLEL with the primary. When the primary
//! session drops, YouTube rolls viewers over to the backup instead of
//! showing a gap. Owner decision 2026-10-16 (#368): an occasional ~3 s gap
//! when one YouTube ingest session drops is not an acceptable baseline, so
//! every YouTube endpoint streams to its backup too.
//!
//! - [`TeePusher`] is the `Pushable` every rust-push path uses. It forwards to
//!   the primary `RtmpPusher` and, for a YouTube endpoint, offers each FLV
//!   payload to a [`BackupMirror`] without ever waiting for it.
//! - [`BackupMirror`] owns its OWN pusher to the backup URL, its own backoff
//!   and reconnect, in its own task, fed through a bounded queue of
//!   [`BACKUP_QUEUE_CHUNKS`] whole chunks. A full queue drops the NEW chunk
//!   (chunks start on keyframes, so the backup skips whole chunks, never a
//!   partial one) and counts it. A slow, hung or dead backup can therefore
//!   never delay or fail the primary.
//! - A backup death writes its own `EndpointBackupPushDied` audit row, never
//!   `EndpointRtmpPushDied`: the outage notifier and the CI death gates count
//!   only the primary.
//! - [`build_endpoint_pusher`] is the ONE place a live or rescue pusher is
//!   built, so the live chunks, the keepalive freeze and the rescue clip all
//!   reach the backup as well.
//!
//! Limit: one VPS feeds both ingests, so this covers a YouTube-side session
//! close, not a VPS loss. The backup is fed at the primary's push cadence: a
//! primary stuck in a long write timeout starves the backup too.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PushError, PusherConfig, RtmpPusher};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::audit_ring::{AuditRing, RingRowParts};
use crate::endpoint_rtmp_url::{build_backup_rtmp_url, build_rtmp_url};
use crate::endpoint_stats::Stats;
use crate::pushable::Pushable;

/// Whole chunks (~2 s each) the backup may lag behind before the tee drops
/// new ones: ~8 s, enough to ride out the backup's own fast reconnect.
pub const BACKUP_QUEUE_CHUNKS: usize = 4;

/// One backup chunk push may take at most this long (the primary's
/// `WRITE_TIMEOUT_SECS`); a hung backup session is then reconnected.
const BACKUP_WRITE_TIMEOUT: Duration =
    Duration::from_secs(crate::endpoint_task::WRITE_TIMEOUT_SECS);

/// The pusher every rust-push path uses: the primary plus, for YouTube, the
/// backup mirror.
pub(crate) type EndpointPusher = TeePusher<RtmpPusher>;

/// Everything needed to build one endpoint's pusher.
#[derive(Clone)]
pub struct PushTarget {
    pub alias: String,
    pub service_type: ServiceType,
    pub stream_key: String,
    /// Mirror to the service's backup ingest when it has one (YouTube).
    pub backup_ingest: bool,
    pub audit_ring: Option<Arc<AuditRing>>,
}

impl PushTarget {
    /// The target of endpoint `ep_cfg` (whose service type the caller parsed
    /// once); its `youtube_backup_ingest` switch decides the backup.
    pub fn for_endpoint(
        ep_cfg: &crate::api::EndpointConfig,
        service_type: ServiceType,
        audit_ring: Option<Arc<AuditRing>>,
    ) -> Self {
        Self {
            alias: ep_cfg.alias.clone(),
            service_type,
            stream_key: ep_cfg.stream_key.clone(),
            backup_ingest: ep_cfg.youtube_backup_ingest,
            audit_ring,
        }
    }
}

/// The backup URL this target streams to, if any: only with
/// `backup_ingest` and only for a service that has a backup ingest.
pub fn backup_url_for(target: &PushTarget) -> Option<String> {
    if !target.backup_ingest {
        return None;
    }
    build_backup_rtmp_url(target.service_type, &target.stream_key)
}

/// THE one place a live or rescue pusher is built (#368): the primary to the
/// endpoint URL, plus the backup mirror for a YouTube target. The mirror's
/// state is published in the endpoint stats (`/api/status` `backup_ingest`).
/// Nothing connects here: both pushers dial on their first chunk.
pub(crate) async fn build_endpoint_pusher(target: &PushTarget, stats: &Stats) -> EndpointPusher {
    let primary = RtmpPusher::new(
        build_rtmp_url(target.service_type, &target.stream_key),
        PusherConfig::default(),
    );
    let backup = backup_url_for(target).map(|url| {
        tracing::info!(alias = %target.alias, "YouTube backup ingest mirror configured (#368)");
        BackupMirror::spawn(
            RtmpPusher::new(url, PusherConfig::default()),
            target.alias.clone(),
            target.audit_ring.clone(),
        )
    });
    stats.lock().await.backup_ingest = backup.as_ref().map(BackupMirror::state);
    TeePusher::new(primary, backup)
}

/// What `/api/status` shows about an endpoint's backup ingest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BackupIngestStatus {
    /// The last backup push succeeded (false before the first one and after
    /// a backup death).
    pub connected: bool,
    pub reconnect_count: u32,
    pub chunks_pushed: u64,
    /// Whole chunks the tee dropped because the backup queue was full.
    pub dropped_chunks: u64,
    pub last_error: Option<String>,
}

/// Live state of one backup mirror, shared by the tee (drops), the mirror
/// task (pushes, deaths) and the endpoint stats.
#[derive(Debug, Default)]
pub struct BackupMirrorState {
    connected: AtomicBool,
    reconnect_count: AtomicU32,
    chunks_pushed: AtomicU64,
    dropped_chunks: AtomicU64,
    last_error: std::sync::Mutex<Option<String>>,
}

impl BackupMirrorState {
    fn note_pushed(&self, reconnect_count: u32) {
        self.connected.store(true, Ordering::Relaxed);
        self.reconnect_count
            .store(reconnect_count, Ordering::Relaxed);
        self.chunks_pushed.fetch_add(1, Ordering::Relaxed);
    }

    fn note_died(&self, error: &str, reconnect_count: u32) {
        self.connected.store(false, Ordering::Relaxed);
        self.reconnect_count
            .store(reconnect_count, Ordering::Relaxed);
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.to_string());
    }

    fn note_dropped(&self) {
        self.dropped_chunks.fetch_add(1, Ordering::Relaxed);
    }

    fn note_stopped(&self) {
        self.connected.store(false, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BackupIngestStatus {
        BackupIngestStatus {
            connected: self.connected.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            chunks_pushed: self.chunks_pushed.load(Ordering::Relaxed),
            dropped_chunks: self.dropped_chunks.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

/// The backup's reconnect policy: the primary's own `reconnect_delay_ms`
/// (fast first reconnect after a healthy minute, short retry after it),
/// over the backup's own error streak and session health.
#[derive(Debug, Default)]
struct MirrorBackoff {
    consecutive_errors: u32,
    healthy_since: Option<Instant>,
    follows_fast_retry: bool,
}

impl MirrorBackoff {
    fn on_ok(&mut self, now: Instant) {
        self.consecutive_errors = 0;
        self.follows_fast_retry = false;
        self.healthy_since.get_or_insert(now);
    }

    /// The delay before the backup reconnects, `None` = stop (LocalCancel).
    fn on_error(&mut self, err: &PushError, now: Instant) -> Option<u64> {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        let healthy_ms = self.healthy_since.take().map_or(0, |since| {
            now.saturating_duration_since(since).as_millis() as u64
        });
        let delay = rs_rtmp_push::reconnect_delay_ms(
            err,
            self.consecutive_errors,
            healthy_ms,
            self.follows_fast_retry,
        )?;
        self.follows_fast_retry = delay == 0;
        Some(delay)
    }
}

/// The audit row of a backup push death. Its own action, so neither the
/// outage notifier nor the CI death gates count it as an endpoint death.
fn backup_died_row(
    alias: &str,
    error: &str,
    backoff_ms: u64,
    reconnect_count: u32,
) -> RingRowParts {
    RingRowParts {
        severity: rs_core::audit::Severity::Warn,
        source: rs_core::audit::Source::Vps,
        endpoint: Some(alias.to_string()),
        action: rs_core::audit::Action::EndpointBackupPushDied,
        detail: serde_json::json!({
            "ingest": "youtube_backup",
            "error": error,
            "backoff_ms": backoff_ms,
            "reconnect_count": reconnect_count,
        }),
    }
}

/// The backup task: pushes every queued chunk to the backup ingest, with its
/// own reconnect and backoff. Runs until the queue closes or it is aborted.
async fn run_mirror<P: Pushable>(
    mut pusher: P,
    mut rx: mpsc::Receiver<Arc<[u8]>>,
    state: Arc<BackupMirrorState>,
    alias: String,
    audit_ring: Option<Arc<AuditRing>>,
) {
    let mut backoff = MirrorBackoff::default();
    while let Some(chunk) = rx.recv().await {
        let result =
            tokio::time::timeout(BACKUP_WRITE_TIMEOUT, pusher.push_flv_bytes(&chunk)).await;
        // Drain the backup pusher's A/V invariant edges (it queues them until
        // taken). They are not audited: the endpoint's push-stage invariant
        // episode belongs to the primary (outage-notifier keying).
        let _ = pusher.take_av_invariant_events();
        let err = match result {
            Ok(Ok(())) => {
                backoff.on_ok(Instant::now());
                state.note_pushed(pusher.reconnect_count());
                continue;
            }
            Ok(Err(e)) => e,
            Err(_elapsed) => PushError::Timeout,
        };
        pusher.close().await;
        let Some(delay_ms) = backoff.on_error(&err, Instant::now()) else {
            break;
        };
        let error = err.to_string();
        let reconnect_count = pusher.reconnect_count();
        state.note_died(&error, reconnect_count);
        tracing::warn!(
            alias = %alias,
            error = %error,
            backoff_ms = delay_ms,
            "backup ingest push died; the primary is unaffected (#368)"
        );
        if let Some(ring) = &audit_ring {
            ring.push_parts(backup_died_row(&alias, &error, delay_ms, reconnect_count));
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    pusher.close().await;
    state.note_stopped();
}

/// One endpoint's backup ingest: a task with its own pusher, fed through a
/// bounded queue the tee never waits on.
pub(crate) struct BackupMirror {
    tx: mpsc::Sender<Arc<[u8]>>,
    state: Arc<BackupMirrorState>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl BackupMirror {
    pub(crate) fn spawn<P: Pushable + Send + 'static>(
        pusher: P,
        alias: String,
        audit_ring: Option<Arc<AuditRing>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(BACKUP_QUEUE_CHUNKS);
        let state = Arc::new(BackupMirrorState::default());
        let task = tokio::spawn(run_mirror(
            pusher,
            rx,
            Arc::clone(&state),
            alias,
            audit_ring,
        ));
        Self {
            tx,
            state,
            task: Some(task),
        }
    }

    /// Queue one chunk for the backup; never waits. A full queue (or a
    /// stopped mirror) drops the chunk and counts it.
    pub fn offer(&self, data: &[u8]) {
        if self.tx.try_send(Arc::from(data)).is_err() {
            self.state.note_dropped();
        }
    }

    pub fn state(&self) -> Arc<BackupMirrorState> {
        Arc::clone(&self.state)
    }

    /// Stop the mirror and wait until its session is gone, so a pusher
    /// built right after (rescue, recovery) never overlaps it on the same
    /// key.
    pub async fn shutdown(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        self.state.note_stopped();
    }
}

impl Drop for BackupMirror {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// The primary pusher plus an optional backup mirror.
///
/// - `push_flv_bytes` offers the chunk to the backup (never waiting), then
///   pushes the primary.
/// - `repush_flv_bytes` (the consumer's one re-send after a failure) goes to
///   the primary only: the backup already has that chunk.
/// - `close` is the primary's reconnect hook after a push error: it closes
///   ONLY the primary. The backup must stay up exactly while the primary
///   reconnects; that is what it is for.
/// - `shutdown` (endpoint end, rescue switch) closes both.
pub(crate) struct TeePusher<P> {
    primary: P,
    backup: Option<BackupMirror>,
}

impl<P> TeePusher<P> {
    pub fn new(primary: P, backup: Option<BackupMirror>) -> Self {
        Self { primary, backup }
    }

    #[cfg(test)]
    pub fn has_backup(&self) -> bool {
        self.backup.is_some()
    }
}

impl<P: Pushable> TeePusher<P> {
    /// Close the primary and stop the backup (endpoint end, or before a
    /// rescue pusher takes over the same key).
    pub async fn shutdown(&mut self) {
        self.primary.close().await;
        if let Some(backup) = self.backup.as_mut() {
            backup.shutdown().await;
        }
    }
}

impl<P: Pushable + Send> Pushable for TeePusher<P> {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        if let Some(backup) = &self.backup {
            backup.offer(data);
        }
        self.primary.push_flv_bytes(data).await
    }

    async fn repush_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.primary.push_flv_bytes(data).await
    }

    async fn close(&mut self) {
        self.primary.close().await;
    }

    fn reconnect_count(&self) -> u32 {
        self.primary.reconnect_count()
    }

    fn av_skew_ms(&self) -> i64 {
        self.primary.av_skew_ms()
    }

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        self.primary.take_av_invariant_events()
    }

    fn last_upstream_ack_at(&self) -> Option<std::time::Instant> {
        self.primary.last_upstream_ack_at()
    }
}

#[cfg(test)]
#[path = "backup_mirror_tests.rs"]
mod tests;
