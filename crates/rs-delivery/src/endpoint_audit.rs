//! Per-endpoint audit emission helpers for the VPS delivery worker.
//!
//! Isolated from `endpoint_task.rs` to keep that file under the
//! 1000-line file-size gate. Every call site below becomes a no-op when
//! `audit_ring` is `None` (tests) so the consumer loop doesn't care.

use std::sync::Arc;

use rs_core::audit::{Action, Severity, Source};

use crate::audit_ring::AuditRing;
use crate::ffmpeg_reason::ReasonClass;

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

/// Audit record emitted on every reconnect of an endpoint using
/// `PusherKind::Rust`. Mirrors `FfmpegRestartRecord` so the dashboard can
/// render either source uniformly. See spec sec 5.5.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RtmpPushAuditRecord {
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub reconnect_count: u32,
    /// Short error code from the RTMP server (e.g. "NetStream.Publish.BadName")
    /// or the error variant name for local failures.
    pub error_display: String,
    pub backoff_ms: u64,
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

/// Audit row emitted when the VPS rs-delivery cannot fetch a chunk from S3
/// (Hetzner 503/504, network blip, etc.). Issue #173 — operator could
/// previously not distinguish "all endpoints stuck because of upstream S3
/// hiccup" from "our code wedged" without digging through each endpoint's
/// last_error. Caller must apply rate-limiting per error_class so a 30 s
/// outage doesn't flood the audit log.
///
/// `error_class` is the bucketed cause ("504", "503", "timeout", "conn",
/// "other") so the dashboard can color-code without parsing free-text.
pub fn emit_s3_fetch_failed(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    chunk_id: i64,
    error_class: &str,
    error_msg: &str,
    backoff_secs: u64,
) {
    let Some(ring) = audit_ring else { return };
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(alias.to_string()),
        Action::S3FetchFailed,
        serde_json::json!({
            "chunk_id": chunk_id,
            "error_class": error_class,
            "error_msg": error_msg,
            "backoff_secs": backoff_secs,
        }),
    );
}

/// One audit row per minute per error_class for VPS S3 fetch failures
/// (issue #173). 60 s matches the host-side `crates/rs-endpoint/src/uploader.rs`
/// rate-limiter on the upload side.
pub const S3_FETCH_AUDIT_WINDOW_SECS: u64 = 60;

/// Per-class rate-limiter for S3-fetch audit rows. Owned by the
/// producer task; drop with the task. Keeps the producer hot path
/// branch-free (one HashMap lookup) while ensuring a sustained outage
/// emits one row per minute per class instead of a flood.
#[derive(Default)]
pub struct S3FetchAuditLimiter {
    last: std::collections::HashMap<&'static str, std::time::Instant>,
}

impl S3FetchAuditLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Emit an audit row for an S3 fetch failure if the rate-limit
    /// window for this error_class has elapsed (or this is the first
    /// hit). Bucketing happens via [`classify_s3_fetch_error`].
    pub fn try_emit(
        &mut self,
        audit_ring: &Option<Arc<AuditRing>>,
        alias: &str,
        chunk_id: i64,
        error_msg: &str,
        backoff_secs: u64,
    ) {
        let class = classify_s3_fetch_error(error_msg);
        let now = std::time::Instant::now();
        let should_emit = match self.last.get(class) {
            Some(t) => t.elapsed().as_secs() >= S3_FETCH_AUDIT_WINDOW_SECS,
            None => true,
        };
        if should_emit {
            emit_s3_fetch_failed(audit_ring, alias, chunk_id, class, error_msg, backoff_secs);
            self.last.insert(class, now);
        }
    }
}

/// Bucket a free-text S3 fetch error into a small set of stable classes
/// for the audit row's rate-limiter key + dashboard categorization.
/// Mirrors the pattern in `crates/rs-endpoint/src/uploader.rs::classify_upload_error`.
pub fn classify_s3_fetch_error(msg: &str) -> &'static str {
    let m = msg.to_ascii_lowercase();
    if m.contains("504") || m.contains("gateway timeout") {
        "504"
    } else if m.contains("503") || m.contains("service unavailable") {
        "503"
    } else if m.contains("timeout") || m.contains("timed out") {
        "timeout"
    } else if m.contains("connection") || m.contains("reset") || m.contains("refused") {
        "conn"
    } else {
        "other"
    }
}

/// Audit row emitted when the Rust RTMP pusher disconnects. Uses its OWN
/// `Action::EndpointRtmpPushDied` variant so the dashboard activity feed
/// can render the correct icon/label — operators watching the rust path
/// should see "rust pusher reconnected" immediately, not the misleading
/// "ffmpeg died" that would appear if both backends shared
/// `EndpointFfmpegDied` (#103 4-h soak feedback).
pub fn emit_rtmp_push_died(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    error_display: &str,
    backoff_ms: u64,
    reconnect_count: u32,
) {
    let Some(ring) = audit_ring else { return };
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(alias.to_string()),
        Action::EndpointRtmpPushDied,
        serde_json::json!({
            "backend": "rust_rtmp_push",
            "error": error_display,
            "backoff_ms": backoff_ms,
            "reconnect_count": reconnect_count,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- classify_s3_fetch_error (issue #173) ---

    #[test]
    fn classify_s3_504_gateway_timeout() {
        assert_eq!(classify_s3_fetch_error("S3 fetch error: status 504"), "504");
        assert_eq!(
            classify_s3_fetch_error("upstream returned Gateway Timeout"),
            "504"
        );
    }

    #[test]
    fn classify_s3_503_service_unavailable() {
        assert_eq!(classify_s3_fetch_error("S3 fetch error: status 503"), "503");
        assert_eq!(classify_s3_fetch_error("Service Unavailable"), "503");
    }

    #[test]
    fn classify_s3_timeout() {
        assert_eq!(classify_s3_fetch_error("operation timed out"), "timeout");
        assert_eq!(classify_s3_fetch_error("connect TIMEOUT"), "timeout");
    }

    #[test]
    fn classify_s3_connection() {
        assert_eq!(classify_s3_fetch_error("connection refused"), "conn");
        assert_eq!(classify_s3_fetch_error("connection reset by peer"), "conn");
    }

    #[test]
    fn classify_s3_other_falls_through() {
        // Any unrecognized error → "other". Mutant killer for any
        // catch-all branch flip.
        assert_eq!(
            classify_s3_fetch_error("some weird hetzner-specific phrase"),
            "other"
        );
    }

    #[test]
    fn classify_s3_504_priority_over_timeout() {
        // Boundary: a 504 message that also contains "timeout" must
        // classify as "504" (the more specific code wins). Kills a
        // mutant that swaps the branch order.
        assert_eq!(
            classify_s3_fetch_error("status 504 - request timed out at gateway"),
            "504"
        );
    }

    #[test]
    fn emit_rtmp_push_died_detailed_includes_telemetry_fields() {
        use crate::rtmp_push_telemetry::RtmpPushTelemetry;

        let ring = AuditRing::new(64);
        let mut tel = RtmpPushTelemetry::new();
        tel.note_send("Audio", 100);
        tel.note_chunk_pushed();
        let close_buf = [0x00, 0xC0, 0x00, 0x03];

        emit_rtmp_push_died_detailed(
            &Some(Arc::new(ring.clone())),
            "FB-NewLevel",
            "upstream closed connection mid-stream: unexpected end of file",
            3000,
            2840,
            &tel,
            &close_buf,
            0, // chunks_buffered_in_pipeline
        );

        let (rows, _) = ring.since(0i64);
        assert_eq!(rows.len(), 1);
        let detail = &rows[0].detail;
        assert_eq!(detail["backend"], "rust_rtmp_push");
        assert_eq!(detail["reconnect_count"], 2840);
        assert_eq!(detail["bytes_sent_since_connect"], 100);
        assert_eq!(detail["chunks_pushed"], 1);
        assert_eq!(detail["last_rtmp_message_type_sent"], "Audio");
        assert_eq!(detail["upstream_close_first_bytes_hex"], "00c00003");
        assert_eq!(detail["chunks_buffered_in_pipeline"], 0);
    }
}
