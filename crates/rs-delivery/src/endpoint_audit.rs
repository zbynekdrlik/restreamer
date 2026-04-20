//! Per-endpoint audit emission helpers for the VPS delivery worker.
//!
//! Isolated from `endpoint_task.rs` to keep that file under the
//! 1000-line file-size gate. Every call site below becomes a no-op when
//! `audit_ring` is `None` (tests) so the consumer loop doesn't care.

use std::sync::Arc;

use rs_core::audit::{Action, Severity, Source};

use crate::audit_ring::AuditRing;
use crate::ffmpeg_reason::ReasonClass;

/// Catchup budget for the consumer after ffmpeg restart.
///
/// During the backoff window between death and respawn, S3 kept uploading
/// chunks while the producer was blocked on channel backpressure. When
/// the consumer resumes, it must drain both the 10-chunk pre-fetch buffer
/// AND the chunks that accumulated on S3 during the backoff, otherwise the
/// cache depth stays stuck at `target + backoff_secs` forever (the
/// 2026-04-20 cascading-drift bug).
///
/// The ~0.95 factor in the catchup math (consumer gains 0.95 chunks per
/// catchup tick, because producer also advances during catchup) means this
/// budget slightly overshoots — the cache briefly dips below target after
/// catchup, then stabilises back at target as normal pacing resumes.
pub fn catchup_budget_for_backoff(backoff_secs: u64, buffer_size: usize) -> u32 {
    (backoff_secs as u32).saturating_add(buffer_size as u32)
}

/// One row in the ffmpeg restart audit log. Captures process-death
/// details so operators can diagnose patterns (e.g. all restarts after
/// exactly 65s = upstream session timeout).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FfmpegRestartRecord {
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: u64,
    /// Serialized `ReasonClass` (e.g. "youtube_rtmp_closed").
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: u64,
}

/// Cap on the per-endpoint restart history ring buffer.
pub const RESTART_HISTORY_CAP: usize = 100;

/// Per-class consecutive-death counter feeding `ffmpeg_reason::reconnect_floor`.
/// Resets to 1 when the class changes so (e.g.) `NetworkTimeout` -> `YoutubeRtmpClosed`
/// starts a fresh backoff ladder instead of carrying the old exponent.
#[derive(Debug, Clone, Copy, Default)]
pub struct EndpointRestartState {
    pub consecutive_same_class: u32,
    pub last_class: Option<ReasonClass>,
}

impl EndpointRestartState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called after each ffmpeg death. Returns updated state.
    pub fn advance(self, class: ReasonClass) -> Self {
        let count = if self.last_class == Some(class) {
            self.consecutive_same_class.saturating_add(1)
        } else {
            1
        };
        Self {
            consecutive_same_class: count,
            last_class: Some(class),
        }
    }
}

/// Audit row emitted when ffmpeg is successfully spawned for an endpoint.
/// First spawn uses `EndpointStarted`; any subsequent successful spawn
/// (after a death handled by the consumer loop) uses `EndpointAliveTransition`.
pub fn emit_spawn_success(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    service_type: &str,
    stream_key_len: usize,
    was_dead: bool,
) {
    let Some(ring) = audit_ring else { return };
    let action = if was_dead {
        Action::EndpointAliveTransition
    } else {
        Action::EndpointStarted
    };
    ring.push(
        Severity::Info,
        Source::Ffmpeg,
        Some(alias.to_string()),
        action,
        serde_json::json!({
            "service_type": service_type,
            "stream_key_len": stream_key_len,
        }),
    );
}

/// Audit row emitted when ffmpeg exits. Captures classifier output +
/// stderr tail so operators can reconstruct why each endpoint went red
/// without needing a live process attached.
#[allow(clippy::too_many_arguments)]
pub fn emit_ffmpeg_died(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    lifetime_secs: u64,
    reason: &str,
    stderr_tail: Option<&str>,
    backoff_secs: u64,
    consecutive_same_class: u32,
) {
    let Some(ring) = audit_ring else { return };
    ring.push(
        Severity::Warn,
        Source::Ffmpeg,
        Some(alias.to_string()),
        Action::EndpointFfmpegDied,
        serde_json::json!({
            "lifetime_secs": lifetime_secs,
            "reason": reason,
            "stderr_tail": stderr_tail,
            "backoff_secs": backoff_secs,
            "consecutive_same_class": consecutive_same_class,
        }),
    );
}

/// Audit row emitted when `FfmpegProcess::spawn` itself fails (missing
/// binary, permission denied, OS resource exhaustion, …). Distinct from
/// `EndpointFfmpegDied` which is about a process that ran and then exited.
pub fn emit_spawn_failed(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    consecutive_failures: u32,
    error: &str,
) {
    let Some(ring) = audit_ring else { return };
    ring.push(
        Severity::Error,
        Source::Ffmpeg,
        Some(alias.to_string()),
        Action::EndpointFfmpegRestartFailed,
        serde_json::json!({
            "consecutive_failures": consecutive_failures,
            "error": error,
        }),
    );
}

/// Audit row emitted when the per-endpoint S3 fetcher can't be constructed
/// at all (bad bucket config, invalid credentials, …). Uses the same
/// `EndpointFfmpegRestartFailed` action with a `phase` tag so operators
/// can distinguish it from in-loop spawn failures.
pub fn emit_s3_fetcher_init_failed(audit_ring: &Option<Arc<AuditRing>>, alias: &str, error: &str) {
    let Some(ring) = audit_ring else { return };
    ring.push(
        Severity::Error,
        Source::Vps,
        Some(alias.to_string()),
        Action::EndpointFfmpegRestartFailed,
        serde_json::json!({
            "phase": "s3_fetcher_init",
            "error": error,
        }),
    );
}
