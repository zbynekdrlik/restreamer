//! Pusher transport state. See spec §5.1.

use tokio::time::Instant;

/// Per-`RtmpPusher` runtime state. Owns connection-lifetime data (TCP session +
/// monotonic output timestamp + reconnect counter). Retry-policy state
/// (`consecutive_errors`, `last_error_class`) lives in the *caller*
/// (`endpoint_task`) — same boundary as today's split between `FfmpegProcess`
/// and `EndpointRestartState`.
#[derive(Default)]
pub struct PusherState {
    /// Highest output timestamp seen across all tracks. Used by the consumer
    /// task as the "reconnect-count" companion metric and to decide whether
    /// a fresh connect is a true reconnect (any media has been sent before).
    pub last_output_ts_ms: u64,
    /// Total reconnects since the pusher was created. Surfaced as the
    /// dashboard `reconnect_count` metric (replaces `ffmpeg_restart_count`).
    pub reconnect_count: u32,
    /// `true` while a TCP+RTMP session is open and bytes can flow. `false`
    /// after `connect()` failed or after a mid-stream error dropped the
    /// session. Lazy reconnect on next `push_flv_bytes`.
    pub connected: bool,
    /// Wall-clock anchor for `-re`-style pacing. Set on the first chunk we
    /// successfully push and reused across the whole pusher lifetime.
    /// Each tag's pacing target is its `output_ts` directly — same domain
    /// as `anchor.elapsed()`, so when up-to-date the pusher sleeps just
    /// long enough for wall-clock to catch up to the tag's PTS.
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
    /// FLV ts of the FIRST audio tag seen on the current RTMP session.
    /// Each subsequent audio tag's wire `output_ts` is computed as
    /// `audio_base + (tag.ts - audio_origin)`, keeping audio on its OWN
    /// continuous timeline that exactly preserves xiu's audio cadence
    /// (~21 ms between AAC frames). Resets to `None` on reconnect so the
    /// new session anchors fresh; `audio_base` carries the cumulative
    /// offset so the wire timeline stays monotonic across reconnects.
    /// Without per-track timelines, audio frames at chunk boundaries
    /// landed on the same `output_ts` as the last frame of the previous
    /// chunk → audible click at every chunk boundary (#103).
    pub audio_origin_xiu_ts: Option<u32>,
    /// Per-track output_ts base for AUDIO. Carried across reconnects so
    /// the wire timeline never goes backwards even when xiu's RTMP
    /// session resets to 0 on the upstream reconnect.
    pub audio_base_ms: u64,
    /// Highest audio `output_ts` actually sent. Used to advance
    /// `audio_base_ms` on reconnect (`audio_base_ms = max + 1`).
    pub last_audio_output_ts_ms: u64,
    /// FLV ts of the FIRST video tag seen on the current RTMP session.
    /// See `audio_origin_xiu_ts`.
    pub video_origin_xiu_ts: Option<u32>,
    /// Per-track output_ts base for VIDEO.
    pub video_base_ms: u64,
    /// Highest video `output_ts` actually sent.
    pub last_video_output_ts_ms: u64,
    /// xiu FLV ts of the previous non-seq-header AUDIO tag we processed.
    /// Used to detect chunker-side timestamp regression — when stream.lan
    /// crashes/restarts but our RTMP session to YouTube stays alive, the
    /// chunker resumes with xiu_ts ~0 even though we'd previously been
    /// pushing tags at xiu_ts ~600_000. Without re-anchoring, the next
    /// `output_ts` would be `audio_base_ms + 0` — strictly less than the
    /// last `output_ts` we sent, breaking PTS monotonicity on the wire
    /// and causing YouTube to drop the stream (#103 production test
    /// 2026-04-30: stream went `active/good` → `inactive/noData` after
    /// the crash-recovery resilience test). Detection roll forward
    /// `audio_base_ms`, clears `audio_origin_xiu_ts`, and re-anchors on
    /// the regressed tag — strictly monotonic on the wire, RTMP session
    /// preserved.
    pub last_audio_xiu_ts: Option<u32>,
    /// Same as `last_audio_xiu_ts` but for video.
    pub last_video_xiu_ts: Option<u32>,
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
