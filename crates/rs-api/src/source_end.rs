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
    pub inpoint_connected: bool,
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
    announced: Option<i64>,
}

impl Tracker {
    /// The chunk to announce as the end of the source now, if any.
    ///
    /// The source has ended when the ingest has been gone for `SETTLE`, OBS
    /// is connected and stopped on purpose within `MATCH_WINDOW` of that
    /// disconnect, and every chunk of the event is on S3. The end is the last uploaded chunk; it is announced again when
    /// a late chunk raised it. A publisher connecting again starts over.
    pub fn due(&mut self, now: Instant, i: &Inputs) -> Option<i64> {
        let _ = (
            now,
            i,
            MATCH_WINDOW,
            &self.event_id,
            &self.disconnected_since,
        );
        None
    }

    /// The VPS accepted `chunk` as the end. Returns whether this is the
    /// first announcement of this stop (the one the audit row records).
    pub fn accepted(&mut self, chunk: i64) -> bool {
        let _ = (chunk, &self.announced);
        false
    }
}

/// `POST {vps}/api/source {ended_at_chunk_id}`. An old VPS without the route
/// answers 404: it keeps the pre-#408 behaviour, and the error is logged.
pub async fn announce(vps_base_url: &str, auth_token: &str, ended_at: i64) -> Result<(), String> {
    let _ = (vps_base_url, auth_token, ended_at);
    Ok(())
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
        let _ = (
            tracker,
            now,
            &self.pool,
            &self.inpoint,
            &self.obs,
            &self.audit_tx,
            self.vps_port,
        );
        None
    }
}

#[cfg(test)]
#[path = "source_end_tests.rs"]
mod tests;
