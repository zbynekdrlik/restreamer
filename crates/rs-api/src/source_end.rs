//! A deliberate OBS stop is the end of the program, not an outage (#408).
//!
//! The VPS decides rescue only from chunk exhaustion, so before #408 an
//! operator who stopped the stream in OBS got the outage clip and outage
//! alerts on every destination once the buffer ran out. The host knows more:
//! the obs-websocket stays connected and reports the stream output STOPPED
//! after STOPPING (`obs::stopped_deliberately_after`). This watch announces
//! the last chunk on S3 to the VPS (`POST /api/source`); every endpoint plays
//! its buffer out to that chunk, then stops sending instead of entering
//! rescue. When OBS publishes again, the next chunk resumes them on the VPS
//! side.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;
use rs_core::models::InpointState;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::obs::ObsClient;

/// How long the ingest must be gone before the end is announced: the
/// chunker's final chunk is flushed, recorded and queued for upload by then.
/// A fast endpoint enters rescue no earlier than ~10 s after its last chunk,
/// so the announcement (this + one 2 s tick + the HTTP call) lands first.
pub const SETTLE: Duration = Duration::from_secs(2);

/// How often the watch looks.
const TICK: Duration = Duration::from_secs(2);

/// How close the OBS stop and the ingest disconnect must be to be one event.
/// OBS reports STOPPED within a second or two of closing its RTMP session,
/// and the watch sees the disconnect within one tick. An OBS stop from long
/// before says nothing about another publisher (CI's ffmpeg) dropping now.
pub const MATCH_WINDOW: Duration = Duration::from_secs(10);

/// What one tick sees.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Inputs {
    pub event_id: i64,
    pub obs_connected: bool,
    /// When OBS stopped on purpose, if it did (`ObsState::stopped_at`).
    pub obs_stopped_at: Option<Instant>,
    pub pending_uploads: i64,
    pub last_sent_chunk: Option<i64>,
}

/// The announcement state of one stop.
#[derive(Debug, Default)]
pub struct Tracker {
    event_id: Option<i64>,
    disconnected_since: Option<Instant>,
    /// OBS was seen connected and stopped on purpose at this disconnect.
    /// Latched: an operator who stops and then quits OBS within seconds
    /// (the websocket is gone at the announcing tick) still stopped on purpose.
    deliberate_seen: bool,
    announced: Option<i64>,
}

impl Tracker {
    /// A publisher is connected: no end, and the next disconnect settles anew.
    pub fn publisher_connected(&mut self) {
        self.disconnected_since = None;
        self.deliberate_seen = false;
        self.announced = None;
    }

    /// The chunk to announce as the end of the source now, if any, while
    /// no publisher is connected.
    ///
    /// The source has ended when the ingest has been gone for `SETTLE`, OBS
    /// was seen connected and stopped on purpose within `MATCH_WINDOW` of
    /// that disconnect, and every chunk of the event is on S3. The end is
    /// the last uploaded chunk; it is announced again when a late chunk
    /// raised it.
    pub fn due(&mut self, now: Instant, i: &Inputs) -> Option<i64> {
        if self.event_id != Some(i.event_id) {
            *self = Tracker {
                event_id: Some(i.event_id),
                ..Tracker::default()
            };
        }
        let since = *self.disconnected_since.get_or_insert(now);
        let obs_stop_caused_it = i.obs_stopped_at.is_some_and(|at| {
            at.saturating_duration_since(since)
                .max(since.saturating_duration_since(at))
                <= MATCH_WINDOW
        });
        self.deliberate_seen |= i.obs_connected && obs_stop_caused_it;
        if now.saturating_duration_since(since) < SETTLE
            || !self.deliberate_seen
            || i.pending_uploads > 0
        {
            return None;
        }
        let last = i.last_sent_chunk?;
        (self.announced != Some(last)).then_some(last)
    }

    /// The VPS accepted `chunk` as the end. Returns whether this is the
    /// first announcement of this stop (the one the audit row records).
    pub fn accepted(&mut self, chunk: i64) -> bool {
        self.announced.replace(chunk).is_none()
    }
}

/// `POST {vps}/api/source {ended_at_chunk_id}`. An old VPS without the route
/// answers 404: it keeps the pre-#408 behaviour, and the error is logged.
pub async fn announce(vps_base_url: &str, auth_token: &str, ended_at: i64) -> Result<(), String> {
    let resp = reqwest::Client::new()
        .post(format!("{vps_base_url}/api/source"))
        .bearer_auth(auth_token)
        .json(&serde_json::json!({ "ended_at_chunk_id": ended_at }))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("VPS answered {}", resp.status()))
    }
}

/// Everything the watch reads.
pub struct SourceEndWatch {
    pub pool: SqlitePool,
    pub inpoint: InpointState,
    pub obs: Arc<RwLock<Option<Arc<ObsClient>>>>,
    pub audit_tx: tokio::sync::mpsc::Sender<AuditRow>,
    /// rs-delivery's port (8000); tests point it at a mock VPS.
    pub vps_port: u16,
}

impl SourceEndWatch {
    pub async fn run(self) {
        let mut tracker = Tracker::default();
        loop {
            tokio::time::sleep(TICK).await;
            self.tick(&mut tracker, Instant::now()).await;
        }
    }

    /// One look: announce the end when it is due. Returns the chunk the VPS
    /// accepted, if any.
    pub async fn tick(&self, tracker: &mut Tracker, now: Instant) -> Option<i64> {
        // The common case, every 2 s of a live event: no DB read at all.
        if self.inpoint.is_connected() {
            tracker.publisher_connected();
            return None;
        }
        let event = db::get_streaming_event(&self.pool)
            .await
            .ok()
            .flatten()
            .filter(|e| e.delivering_activated)?;
        let obs = match self.obs.read().await.clone() {
            Some(client) => client.get_status().await,
            None => crate::obs::ObsState::default(),
        };
        let inputs = Inputs {
            event_id: event.id,
            obs_connected: obs.connected,
            obs_stopped_at: obs.stopped_at.filter(|_| obs.stopped_deliberately),
            // A failed read counts as "still uploading": never announce early.
            pending_uploads: db::get_pending_chunk_count_for_event(&self.pool, event.id)
                .await
                .unwrap_or(1),
            last_sent_chunk: db::get_last_sent_sequence_for_event(&self.pool, event.id)
                .await
                .ok()
                .flatten(),
        };
        let ended_at = tracker.due(now, &inputs)?;
        let instance = db::get_delivery_instance_by_event(&self.pool, event.id)
            .await
            .ok()
            .flatten()?;
        let base = format!("http://{}:{}", instance.ipv4, self.vps_port);
        if let Err(e) = announce(&base, &instance.auth_token, ended_at).await {
            warn!(
                event = event.id,
                ended_at, "source end: announcing to the VPS failed: {e}"
            );
            return None;
        }
        info!(
            event = event.id,
            ended_at, "source end: OBS stopped on purpose, endpoints play out to chunk {ended_at}"
        );
        if tracker.accepted(ended_at) {
            rs_core::audit::record(
                &self.audit_tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event.id),
                    instance_id: Some(instance.id),
                    endpoint: None,
                    action: Action::SourceStoppedDeliberately,
                    detail: serde_json::json!({ "ended_at_chunk_id": ended_at }),
                    ts_override: None,
                },
            );
        }
        Some(ended_at)
    }
}

#[cfg(test)]
#[path = "source_end_tests.rs"]
mod tests;
