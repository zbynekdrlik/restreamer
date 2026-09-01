#[rustfmt::skip]
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientProfile {
    pub id: i64,
    pub user_uuid: String,
}

/// Reusable event configuration preset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTemplate {
    pub id: i64,
    pub name: String,
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEvent {
    pub id: i64,
    pub name: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
    pub cache_delay_secs: Option<i64>,
    pub created_from: Option<String>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    pub id: i64,
    pub streaming_event_id: i64,
    pub chunk_file_path: String,
    pub data_size: i64,
    pub created_at: String,
    pub md5: String,
    pub in_process: bool,
    pub sent: bool,
    pub sequence_number: i64,
    pub duration_ms: i64,
    // V17 upload telemetry
    #[serde(default)]
    pub upload_attempts: i64,
    #[serde(default)]
    pub upload_first_attempt_at: Option<i64>,
    #[serde(default)]
    pub upload_completed_at: Option<i64>,
    #[serde(default)]
    pub upload_duration_ms: Option<i64>,
    #[serde(default)]
    pub upload_last_error: Option<String>,
    #[serde(default)]
    pub upload_next_retry_at: Option<i64>,
    #[serde(default)]
    pub upload_failed_permanently: bool,
}

/// Which RTMP-push backend an endpoint uses. Default `Ffmpeg` keeps existing
/// `config.json` files behaving exactly as today; `Rust` selects the new
/// in-process pusher introduced for #103.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PusherKind {
    /// Legacy ffmpeg-subprocess push. Kept as a DB / config value for
    /// backward read compatibility with rows created before the
    /// rust-pusher migration, but no API path writes this value: the
    /// `create_endpoint_config` INSERT site explicitly sets `'rust'`
    /// and `update_endpoint` doesn't accept a `pusher` field at all.
    /// Migration v28 backfills any pre-existing `'ffmpeg'` row to
    /// `'rust'` on the next service start. Full removal of this variant
    /// and the ffmpeg-subprocess push code path is tracked in #212.
    Ffmpeg,
    /// In-process Rust RTMP pusher (rs-rtmp-push). The only supported
    /// push backend for new endpoints. `#[default]` so any config without
    /// an explicit pusher field starts on the working path.
    #[default]
    Rust,
}

/// Endpoint configuration (e.g., YouTube HLS, Facebook RTMP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub id: i64,
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    pub enabled: bool,
    pub position_last: i64,
    pub delivered_bytes: i64,
    pub is_fast: bool,
    /// Which push backend to use. `#[serde(default)]` parses legacy
    /// config.json files that omit the field; the default is `Rust`
    /// (post-#196 — was `Ffmpeg` until v0.17.0). Endpoints without an
    /// explicit `pusher` field now silently land on the working
    /// rs-rtmp-push backend.
    #[serde(default)]
    pub pusher: PusherKind,
    /// Number of chunks to pre-fetch ahead of the pusher. Resolution
    /// at endpoint init: explicit Some(K) wins; else is_fast=true => K=1
    /// (double-buffered, ~zero added delay); else K=0 (current bypass
    /// behavior). Operator may override per endpoint.
    #[serde(default)]
    pub prefetch_chunks: Option<u32>,
    /// FK into `youtube_oauth(id)`. `None` => no YT health probe.
    /// `#[serde(default)]` keeps existing config.json files parsing.
    #[serde(default)]
    pub youtube_oauth_id: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

/// Event-endpoint many-to-many link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEndpoint {
    pub event_id: i64,
    pub endpoint_id: i64,
}

/// Hetzner delivery VPS instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInstance {
    pub id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub ipv4: String,
    pub status: String,
    pub server_type: String,
    pub event_id: Option<i64>,
    pub created_at: String,
    pub last_health_at: Option<String>,
    /// Auth token for rs-delivery API (not serialized to API responses).
    #[serde(skip)]
    pub auth_token: String,
}

/// Per-endpoint status on delivery VPS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEndpointStatus {
    pub id: i64,
    pub instance_id: i64,
    pub alias: String,
    pub alive: bool,
    pub chunks_processed: i64,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub last_check_at: String,
}

/// YouTube OAuth tokens, keyed by a unique `label`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeOAuth {
    pub id: i64,
    /// Human-readable label uniquely identifying this grant
    /// (e.g. `default`, `bb`). Used by endpoint linkage and OAuth flow `?label=`.
    #[serde(default = "default_oauth_label")]
    pub label: String,
    pub access_token: String,
    pub refresh_token: String,
    pub token_uri: String,
    pub client_id: String,
    pub client_secret: String,
    pub scopes: String,
    pub expires_at: Option<String>,
    /// Captured from `liveStreams.list` items' `snippet.channelId` after
    /// the first successful probe.
    #[serde(default)]
    pub channel_id: Option<String>,
    /// RFC3339 timestamp when the Device Flow grant was completed.
    #[serde(default)]
    pub connected_at: Option<String>,
}

fn default_oauth_label() -> String {
    "default".to_string()
}

/// Real-time event broadcast over WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WsEvent {
    InpointStatus {
        state: String,
        rtmp_connected: bool,
        received_bytes: u64,
        chunk_count: u64,
    },
    EndpointStatus {
        state: String,
        pending_chunks: u64,
        active_uploads: u32,
        buffer_duration: String,
    },
    ChunkReceived {
        id: i64,
        data_size: i64,
        md5: String,
    },
    ChunkUploaded {
        chunk_id: i64,
    },
    ChunkUploadAttempt {
        chunk_id: i64,
        attempt: i64,
    },
    ChunkUploadFailed {
        chunk_id: i64,
        error: String,
        permanent: bool,
    },
    StreamingEvent {
        action: String,
        name: Option<String>,
        receiving: bool,
        delivering: bool,
    },
    DeliveryStatus {
        instance_name: String,
        status: String,
        server_ip: Option<String>,
        endpoint_count: u32,
        endpoints: Vec<DeliveryEndpointMetrics>,
    },
    Error {
        service: String,
        message: String,
    },
    ActivityFeed {
        timestamp: String,
        severity: String,
        message: String,
        source: String,
    },
    PipelineState {
        state: String,
        event_id: Option<i64>,
        event_name: Option<String>,
        target_delay_secs: u64,
        session_start: Option<String>,
        #[serde(default)]
        local_buffer_chunks: i64,
        #[serde(default)]
        s3_queue_chunks: i64,
        #[serde(default)]
        cache_duration_secs: f64,
    },
    ObsStatus {
        connected: bool,
        streaming: bool,
        recording: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream_timecode: Option<String>,
        summary: String,
    },
    AuditAppended {
        id: i64,
        ts: String,
        severity: String,
        source: String,
        event_id: Option<i64>,
        instance_id: Option<i64>,
        endpoint: Option<String>,
        action: String,
        detail: serde_json::Value,
    },
    MetricsSample {
        ts_ms: i64,
        event_id: i64,
        instance_id: i64,
        alias: String,
        chunk_delay_secs: f64,
        current_chunk_id: i64,
        chunks_processed: i64,
        alive: bool,
    },
}

/// Snapshot of YT `liveStreams.list` health for a single endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct YoutubeHealth {
    /// `status.streamStatus` (`active` | `ready` | `inactive` | ...).
    pub stream_status: String,
    /// `status.healthStatus.status` (`good` | `ok` | `bad` | `noData` | ...).
    pub health_status: String,
    /// `status.healthStatus.configurationIssues[0].type` if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_issue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_rate: Option<String>,
    /// Seconds since the data was probed.
    #[serde(default)]
    pub age_secs: i64,
    /// Set when the probe could not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// `EndpointLifecycle` + `LifecycleInput` + `compute` live in
// `crate::endpoint_lifecycle` (extracted to keep this file under the
// 1000-line CI cap). Re-exported here so `rs_core::models::EndpointLifecycle`
// paths keep resolving.
pub use crate::endpoint_lifecycle::{EndpointLifecycle, LifecycleInput};

/// Per-endpoint delivery metrics broadcast via WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEndpointMetrics {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall_reason: Option<String>,
    #[serde(default)]
    pub ffmpeg_restart_count: u32,
    /// Rust-pusher reconnect counter (issue #172). Defaults to 0 for
    /// older payloads / ffmpeg-path endpoints.
    #[serde(default)]
    pub reconnect_count: u32,
    /// Content-PTS A/V skew in ms (positive = audio behind video) for
    /// rust-pusher endpoints. The dashboard alarms on a sustained non-zero
    /// value; the #258 E2E gate asserts it stays ~0. Defaults to 0 for older
    /// payloads / ffmpeg-path endpoints (issue #257).
    #[serde(default)]
    pub av_skew_ms: i64,
    /// #295: the fast endpoint's current ratcheted read-delay target
    /// (seconds) from the #294 adaptive controller. The dashboard colours the
    /// fast buffer bar RELATIVE to this, so a correctly HELD ratcheted buffer
    /// reads healthy instead of tripping a stale absolute 8s ceiling. `None`
    /// for a non-fast endpoint or a VPS binary predating the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_delay_target_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub is_fast: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rescue_eta_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub youtube_health: Option<YoutubeHealth>,
    /// Operator-facing lifecycle (host-computed). Older payloads default to
    /// Live so the dashboard degrades gracefully.
    #[serde(default = "crate::endpoint_lifecycle::default_lifecycle")]
    pub lifecycle: EndpointLifecycle,
}

/// Service status summary returned by the /status endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceStatus {
    pub inpoint: ComponentStatus,
    pub endpoint: ComponentStatus,
    pub delivery: ComponentStatus,
    pub streaming_event: Option<StreamingEvent>,
    /// Local chunk-store disk-pressure level: "ok" | "warn" | "critical".
    /// Drives the dashboard disk-pressure banner (#231). Defaults to "ok".
    #[serde(default)]
    pub disk_pressure: String,
    /// Whether the configured `s3.region` matches the project standard
    /// (`fsn1`). Computed fresh from the LIVE config on every poll, so it
    /// stays correct even after a runtime config patch. Drives the
    /// dashboard S3-region banner (#278). Defaults to true (assume
    /// standard) so an absent/older field never false-alarms.
    #[serde(default = "default_true")]
    pub s3_region_standard: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    pub state: String,
    pub details: serde_json::Value,
}

impl Default for ComponentStatus {
    fn default() -> Self {
        Self {
            state: String::new(),
            details: serde_json::Value::Object(Default::default()),
        }
    }
}

/// Shared state tracking whether an RTMP publisher (e.g. OBS) is connected.
///
/// Uses `Arc<AtomicBool>` so clones share the same underlying state.
/// Written by `MediaReceiver` on Publish/UnPublish, read by the API `/status` handler.
///
/// In addition to the connected-flag, the struct carries two optional
/// hooks set by the runtime at construction time (absent in tests):
/// - `audit_tx` — emit RtmpConnected/Disconnected/HandshakeFailed rows
/// - `rtmp_stable_since` — Arc shared with `AppState.rtmp_stable_since`.
///   MediaReceiver writes `Some(Instant::now())` on Publish and `None` on
///   UnPublish; the `POST /delivery/start` handler reads it to gate VPS
///   creation until the ingest has been stable for
///   `RTMP_STABLE_REQUIRED_SECS`.
#[derive(Debug, Clone)]
pub struct InpointState {
    rtmp_connected: Arc<AtomicBool>,
    /// Shared handle to the `AppState.rtmp_stable_since` cell. None in
    /// stand-alone tests; Some in the runtime-wired path.
    rtmp_stable_since: Option<Arc<tokio::sync::Mutex<Option<std::time::Instant>>>>,
    /// Optional audit channel. None in tests; set by
    /// `with_audit_tx(...)` at runtime wiring time.
    audit_tx: Option<tokio::sync::mpsc::Sender<crate::audit::AuditRow>>,
    /// Connect timestamp for computing session duration on disconnect.
    connect_started_at: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    /// Live ingest-side A/V skew (ms, signed; positive = audio behind video),
    /// written by the chunker (`rs_inpoint::flv_chunker`) and read by the API
    /// `/status` handler + the `POST /delivery/start` gate + the Tauri tray's
    /// `get_status`. Every `InpointState` clone shares this `Arc`, so the one
    /// wired into the chunker and the ones held by the API/Tauri states all
    /// see the same value — no separate cross-component wiring needed (#354).
    ingest_skew_ms: Arc<AtomicI64>,
    /// Latched "ingest skew sustained over threshold" flag: `true` while the
    /// source (OBS) is desynced past `inpoint.skew_threshold_ms`. Drives the
    /// dashboard banner and gates `Start Delivering`. Shared by `Arc` across
    /// clones like `ingest_skew_ms` (#354).
    ingest_skew_active: Arc<AtomicBool>,
}

impl InpointState {
    pub fn new() -> Self {
        Self {
            rtmp_connected: Arc::new(AtomicBool::new(false)),
            rtmp_stable_since: None,
            audit_tx: None,
            connect_started_at: Arc::new(std::sync::Mutex::new(None)),
            ingest_skew_ms: Arc::new(AtomicI64::new(0)),
            ingest_skew_active: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wire the audit channel. Call once at runtime startup; all clones
    /// share the same sender. `None` means no audit rows are emitted.
    pub fn with_audit_tx(mut self, tx: tokio::sync::mpsc::Sender<crate::audit::AuditRow>) -> Self {
        self.audit_tx = Some(tx);
        self
    }

    /// Wire the shared `rtmp_stable_since` cell. Required for
    /// `POST /delivery/start` to see the publisher-stable timestamp.
    pub fn with_stable_since(
        mut self,
        cell: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    ) -> Self {
        self.rtmp_stable_since = Some(cell);
        self
    }

    pub fn audit_tx(&self) -> Option<&tokio::sync::mpsc::Sender<crate::audit::AuditRow>> {
        self.audit_tx.as_ref()
    }

    pub fn set_connected(&self, connected: bool) {
        self.rtmp_connected.store(connected, Ordering::Relaxed);
    }

    /// Mark publisher connected. Sets `rtmp_stable_since` (if wired) and
    /// records the connect instant so `mark_disconnected` can emit a
    /// duration-accurate audit row.
    pub async fn mark_connected(&self) {
        let now = std::time::Instant::now();
        self.rtmp_connected.store(true, Ordering::Relaxed);
        if let Some(cell) = &self.rtmp_stable_since {
            *cell.lock().await = Some(now);
        }
        if let Ok(mut g) = self.connect_started_at.lock() {
            *g = Some(now);
        }
    }

    /// Mark publisher disconnected. Clears `rtmp_stable_since` (if wired)
    /// and returns the session duration in seconds (None if not
    /// previously connected).
    pub async fn mark_disconnected(&self) -> Option<u64> {
        self.rtmp_connected.store(false, Ordering::Relaxed);
        if let Some(cell) = &self.rtmp_stable_since {
            *cell.lock().await = None;
        }
        let started = self
            .connect_started_at
            .lock()
            .ok()
            .and_then(|mut g| g.take());
        started.map(|s| s.elapsed().as_secs())
    }

    pub fn is_connected(&self) -> bool {
        self.rtmp_connected.load(Ordering::Relaxed)
    }

    /// Record the live ingest A/V skew (ms, signed) — called by the chunker at
    /// each chunk boundary.
    pub fn set_ingest_skew_ms(&self, ms: i64) {
        self.ingest_skew_ms.store(ms, Ordering::Relaxed);
    }

    /// Current live ingest A/V skew (ms, signed; positive = audio behind video).
    pub fn ingest_skew_ms(&self) -> i64 {
        self.ingest_skew_ms.load(Ordering::Relaxed)
    }

    /// Set the latched "ingest skew sustained over threshold" flag.
    pub fn set_ingest_skew_active(&self, active: bool) {
        self.ingest_skew_active.store(active, Ordering::Relaxed);
    }

    /// Whether ingest skew is currently latched over threshold (source desynced).
    pub fn ingest_skew_active(&self) -> bool {
        self.ingest_skew_active.load(Ordering::Relaxed)
    }
}

impl Default for InpointState {
    fn default() -> Self {
        Self::new()
    }
}

/// Upload telemetry row returned by /api/v1/uploads/recent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadChunkRow {
    pub chunk_id: i64,
    pub event_identifier: String,
    pub sequence_number: i64,
    pub size_bytes: i64,
    pub attempts: i64,
    pub duration_ms: Option<i64>,
    /// "sent" | "pending" | "retrying" | "failed"
    pub status: String,
    pub last_error: Option<String>,
    pub first_attempt_at: Option<i64>,
    pub completed_at: Option<i64>,
}

/// Chunk statistics returned by the /chunks/stats endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkStats {
    pub total_chunks: i64,
    pub pending_chunks: i64,
    pub sent_chunks: i64,
    pub in_process_chunks: i64,
    pub total_bytes: i64,
    pub buffer_duration_secs: f64,
}

#[cfg(test)]
#[path = "models_tests.rs"]
mod tests;
