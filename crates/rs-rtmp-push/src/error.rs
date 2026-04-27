//! Error types surfaced by `RtmpPusher`. See spec §4.1 + §5.3.

use std::io;
use thiserror::Error;

/// One-of error returned by `RtmpPusher::push_flv_bytes`.
///
/// `code` and `description` on the `*Rejected` variants are the upstream-provided
/// AMF onStatus payload (e.g. `code: "NetStream.Publish.BadName"`).
#[derive(Debug, Error)]
pub enum PushError {
    #[error("RTMP handshake failed: {0}")]
    HandshakeFailed(#[source] io::Error),

    #[error("NetConnection.Connect rejected: {code} - {description}")]
    ConnectRejected { code: String, description: String },

    #[error("NetStream.Publish rejected: {code} - {description}")]
    PublishRejected { code: String, description: String },

    #[error("upstream closed connection mid-stream: {0}")]
    RemoteClosed(#[source] io::Error),

    #[error("operation timed out")]
    Timeout,

    #[error("I/O error: {0}")]
    IoError(#[source] io::Error),

    #[error("local cancel")]
    LocalCancel,

    #[error("malformed FLV input at offset {offset}: {reason}")]
    MalformedInput { offset: usize, reason: String },
}

/// Backoff floor in milliseconds for a given error variant. Mirrors today's
/// `crates/rs-delivery/src/ffmpeg_reason.rs::reconnect_floor` semantics.
///
/// The endpoint task multiplies this by `2^consecutive_errors` and caps at 300_000
/// (5 min). `PublishRejected { code: "NetStream.Publish.BadName" }` is a fixed
/// 30s floor — fast retry is pointless and exponential escalation drowns the
/// signal. `LocalCancel` returns `None` (no retry).
pub fn backoff_floor_ms(err: &PushError) -> Option<u64> {
    match err {
        PushError::HandshakeFailed(_) => Some(5_000),
        PushError::ConnectRejected { .. } => Some(30_000),
        PushError::PublishRejected { code, .. } if code == "NetStream.Publish.BadName" => {
            Some(30_000)
        }
        PushError::PublishRejected { .. } => Some(30_000),
        PushError::RemoteClosed(_) => Some(30_000),
        PushError::Timeout => Some(10_000),
        PushError::IoError(_) => Some(15_000),
        PushError::MalformedInput { .. } => Some(15_000),
        PushError::LocalCancel => None,
    }
}

/// Whether to escalate the floor exponentially on consecutive same-class
/// errors. `BadName` is fixed (operator must rotate the key); the rest follow
/// today's exponential ×2 cap-at-300s policy.
pub fn is_exponential(err: &PushError) -> bool {
    !matches!(
        err,
        PushError::PublishRejected { code, .. } if code == "NetStream.Publish.BadName"
    ) && !matches!(
        err,
        PushError::Timeout
            | PushError::IoError(_)
            | PushError::HandshakeFailed(_)
            | PushError::MalformedInput { .. }
            | PushError::LocalCancel
    )
}
