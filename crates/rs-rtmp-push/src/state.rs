//! Pusher transport state. See spec §5.1.

/// Per-`RtmpPusher` runtime state. Owns connection-lifetime data (TCP session +
/// monotonic output timestamp + reconnect counter). Retry-policy state
/// (`consecutive_errors`, `last_error_class`) lives in the *caller*
/// (`endpoint_task`) — same boundary as today's split between `FfmpegProcess`
/// and `EndpointRestartState`.
#[derive(Default)]
pub struct PusherState {
    /// Output timestamp in ms, monotonic across reconnects. Never resets.
    pub last_output_ts_ms: u64,
    /// Total reconnects since the pusher was created. Surfaced as the
    /// dashboard `reconnect_count` metric (replaces `ffmpeg_restart_count`).
    pub reconnect_count: u32,
    /// `true` while a TCP+RTMP session is open and bytes can flow. `false`
    /// after `connect()` failed or after a mid-stream error dropped the
    /// session. Lazy reconnect on next `push_flv_bytes`.
    pub connected: bool,
}

#[derive(Clone)]
pub struct PusherConfig {
    /// Per-call socket-write timeout in ms. Default 30_000 (matches today's
    /// `crates/rs-delivery/src/endpoint_task.rs::WRITE_TIMEOUT_SECS`).
    pub timeout_ms: u64,
}

impl Default for PusherConfig {
    fn default() -> Self {
        Self { timeout_ms: 30_000 }
    }
}
