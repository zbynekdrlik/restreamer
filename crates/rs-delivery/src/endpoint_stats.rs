//! Per-endpoint stats struct + initial-state constructor.
//!
//! Extracted from `endpoint_task.rs` to free line budget under the
//! 1000-line CI cap (#184 follow-up wiring needed extra lines in
//! endpoint_task.rs for the LifecycleAwarePusher integration).

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::endpoint_audit::{FfmpegRestartRecord, RtmpPushAuditRecord};

/// Stats tracked per endpoint with diagnostics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EndpointStats {
    pub bytes_processed_total: u64,
    pub duration_processed_ms: u64,
    pub current_chunk_id: i64,
    pub chunks_processed: u64,
    // Diagnostics
    pub ffmpeg_restart_count: u32,
    pub consecutive_ffmpeg_failures: u32,
    pub consecutive_chunk_misses: u32,
    pub last_error: Option<String>,
    pub stall_reason: Option<String>,
    pub ffmpeg_last_stderr: Option<String>,
    /// Per-endpoint ring buffer of recent ffmpeg restarts. Capped at
    /// RESTART_HISTORY_CAP — oldest dropped first.
    pub restart_history: std::collections::VecDeque<FfmpegRestartRecord>,
    /// Current delivery mode: "normal", "warmup", "rescue", "recovering".
    pub delivery_mode: String,
    /// ETA in seconds until rescue mode ends (warmup or buffer refill).
    pub rescue_eta_secs: Option<u64>,
    /// Reconnect counter for `PusherKind::Rust` endpoints. Mirrors
    /// `ffmpeg_restart_count` so the dashboard can use either uniformly.
    #[serde(default)]
    pub reconnect_count: u32,
    /// Current signed content-PTS A/V skew in ms for `PusherKind::Rust`
    /// endpoints (positive = audio behind video). Read from the pusher on
    /// every successful chunk push. The dashboard alarms on a sustained
    /// non-zero value and the #258 E2E gate asserts it stays ~0 (issue #257).
    #[serde(default)]
    pub av_skew_ms: i64,
    /// Per-endpoint ring buffer of recent Rust RTMP pusher reconnects.
    #[serde(default)]
    pub rtmp_push_history: std::collections::VecDeque<RtmpPushAuditRecord>,
    /// PrefetchQueue fill: depth/capacity for fast endpoints. None when
    /// the endpoint runs without a prefetch queue (K=0). Surfaced on the
    /// dashboard as a fill bar (#184).
    #[serde(default)]
    pub prefetch_fill: Option<PrefetchFill>,
    /// Last lifecycle worst-stage observation: stage label + duration.
    /// Surfaced on the dashboard as a small badge (#184).
    #[serde(default)]
    pub last_lifecycle_worst_stage: Option<LifecycleSummary>,
}

/// Snapshot of a per-endpoint PrefetchQueue.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PrefetchFill {
    pub depth: u32,
    pub capacity: u32,
}

/// Snapshot of the most recent LifecycleSampler observation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LifecycleSummary {
    pub worst_stage: String,
    pub worst_stage_ms: i64,
}

impl Default for EndpointStats {
    fn default() -> Self {
        Self {
            bytes_processed_total: 0,
            duration_processed_ms: 0,
            current_chunk_id: 0,
            chunks_processed: 0,
            ffmpeg_restart_count: 0,
            consecutive_ffmpeg_failures: 0,
            consecutive_chunk_misses: 0,
            last_error: None,
            stall_reason: None,
            ffmpeg_last_stderr: None,
            restart_history: std::collections::VecDeque::new(),
            delivery_mode: "normal".to_string(),
            rescue_eta_secs: None,
            reconnect_count: 0,
            av_skew_ms: 0,
            rtmp_push_history: std::collections::VecDeque::new(),
            prefetch_fill: None,
            last_lifecycle_worst_stage: None,
        }
    }
}

pub type Stats = Arc<Mutex<EndpointStats>>;

/// Initial EndpointStats: Default + per-endpoint overrides.
/// Explicit assignment (not struct literal) for mutation-test coverage.
#[allow(clippy::field_reassign_with_default)]
pub fn initial_endpoint_stats(start_chunk_id: i64, initial_mode: String) -> EndpointStats {
    let mut s = EndpointStats::default();
    s.current_chunk_id = start_chunk_id;
    s.delivery_mode = initial_mode;
    s
}
