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
    /// Current delivery mode: "normal", "warmup", "rescue", "recovering",
    /// or "refilling" (#296 — buffered endpoint delivering slightly slower
    /// than realtime to rebuild a below-target cushion after a source gap).
    pub delivery_mode: String,
    /// ETA in seconds until rescue mode ends (warmup or buffer refill).
    pub rescue_eta_secs: Option<u64>,
    /// Reconnect counter for rust-pusher endpoints. Mirrors
    /// `ffmpeg_restart_count` so the dashboard can use either uniformly.
    #[serde(default)]
    pub reconnect_count: u32,
    /// Current signed content-PTS A/V skew in ms for rust-pusher
    /// endpoints (positive = audio behind video). Read from the pusher on
    /// every successful chunk push. The dashboard alarms on a sustained
    /// non-zero value and the #258 E2E gate asserts it stays ~0 (issue #257).
    #[serde(default)]
    pub av_skew_ms: i64,
    /// Per-endpoint ring buffer of recent Rust RTMP pusher reconnects.
    #[serde(default)]
    pub rtmp_push_history: std::collections::VecDeque<RtmpPushAuditRecord>,
    /// Prefetch-queue fill (depth/capacity) for the dashboard fill bar (#184).
    /// Currently ALWAYS `None`: the `PrefetchQueue` that fed it was removed as
    /// dead code (#286/#334), so nothing populates this yet. Kept as a
    /// serialized field (serde `default`, so no reader breaks) until a
    /// replacement fill signal wires in.
    #[serde(default)]
    pub prefetch_fill: Option<PrefetchFill>,
    /// Last lifecycle worst-stage observation: stage label + duration.
    /// Surfaced on the dashboard as a small badge (#184).
    #[serde(default)]
    pub last_lifecycle_worst_stage: Option<LifecycleSummary>,
    /// Unix epoch ms of the last SUCCESSFUL push on this endpoint — a live
    /// chunk via the rust pusher, or a rescue-clip push during an outage.
    /// #284 disambiguation telemetry: together with `producer_active` on
    /// `/api/status` this classifies a live stall (producer starved vs
    /// pusher stalled) without log access, and the #238 crash-exhaustion
    /// E2E gate asserts it keeps advancing while rescue is active. `None`
    /// until the first successful push (and on the ffmpeg pusher path,
    /// which has no per-push success signal).
    #[serde(default)]
    pub last_push_ok_unix_ms: Option<i64>,
}

/// Snapshot of a per-endpoint prefetch-queue fill. Currently unpopulated —
/// see `EndpointStats::prefetch_fill` (the `PrefetchQueue` was removed in
/// #286/#334); retained for the dashboard fill-bar contract (#184).
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
            last_push_ok_unix_ms: None,
        }
    }
}

/// Current Unix epoch time in milliseconds. Shared by the push-success
/// bookkeeping (`last_push_ok_unix_ms`) and the `/api/status` age math so
/// both sides use the same clock.
pub fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Age in ms of an optional unix-ms timestamp relative to `now_ms`,
/// clamped at 0 (a small negative from cross-thread clock reads must not
/// surface as a bogus huge age). Used by `/api/status` for
/// `last_push_ok_age_ms` (#284).
pub fn age_ms(now_ms: i64, t_unix_ms: Option<i64>) -> Option<i64> {
    t_unix_ms.map(|t| (now_ms - t).max(0))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_ms_now_is_a_real_epoch_timestamp() {
        // 2020-09-13 in unix-ms — any real clock is far past this; a
        // mutant returning 0/1 dies here.
        assert!(unix_ms_now() > 1_600_000_000_000);
    }

    #[test]
    fn age_ms_computes_now_minus_timestamp() {
        assert_eq!(age_ms(10_000, Some(7_500)), Some(2_500));
        assert_eq!(age_ms(10_000, None), None);
    }

    #[test]
    fn age_ms_clamps_negative_to_zero() {
        // Timestamp slightly in the future (clock read race) => 0, never
        // negative.
        assert_eq!(age_ms(10_000, Some(10_050)), Some(0));
    }
}
