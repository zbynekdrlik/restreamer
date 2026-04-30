//! Pusher transport state. See spec §5.1.

use tokio::time::Instant;

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
    /// Wall-clock anchor for `-re`-style pacing. Set on the first chunk we
    /// successfully push and reused across the whole pusher lifetime; together
    /// with `last_output_ts_ms` it lets `push_flv_bytes` sleep ONCE per chunk
    /// to keep output close to 1 x media-time when up-to-date, while still
    /// running flat-out when behind. Per-tag pacing was tried first but the
    /// 80 sleeps/sec compounded scheduler jitter and dropped output to
    /// ~0.3 x real-time (#103, run 25119429314).
    pub pacing_anchor: Option<Instant>,
    /// `true` once an AVC sequence header has been forwarded on this RTMP
    /// session. The chunker re-emits the sequence header in EVERY S3 chunk
    /// (it must, so each chunk is a self-contained FLV file for ffmpeg's
    /// `-re -f flv -i pipe:`), but a real RTMP server expects it exactly
    /// once per session — re-sending it can cause the receiver to reset
    /// its decoder or pause ingestion, and was observed to drop the rust
    /// pusher's effective output to ~0.2 x real-time (#103).
    pub avc_seq_header_sent: bool,
    /// Same as `avc_seq_header_sent` but for AAC.
    pub aac_seq_header_sent: bool,
    /// Chunker's last-seen video FLV timestamp across the pusher lifetime.
    /// Video is stamped by the chunker with wall-clock since session start
    /// (`crates/rs-inpoint/src/flv_chunker.rs::current_session_ts`), so the
    /// delta between two successive chunks' last video tag IS the real
    /// wall-clock advance from chunker's frame of reference — including
    /// the inter-chunk keyframe gap that `chunk_duration_ms` misses.
    /// Using this for pacing-target advance eliminates the ~1.7 % drain
    /// caused by `chunk_duration_ms` reporting only intra-chunk span
    /// (#103 4-h soak observed cache drop from 106s → 92s in 13 min).
    /// `None` until the first chunk with a video tag has been pushed.
    pub last_video_ts_ms: Option<u32>,
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
