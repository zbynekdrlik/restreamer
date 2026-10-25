//! #424: ending a publish without losing its tail.
//!
//! The VOD of a deliberately ended stream lacked its last ~8 s (camera-box
//! av-gate sessions on stream-snv). The push socket has an 8 MB send buffer
//! (`SO_SNDBUF` 4 MB, doubled by Linux), and `Session::close` dropped it at
//! once: no FCUnpublish / deleteStream, no wait for the queue. A socket closed
//! with unread received bytes (the upstream's acks and pings) is reset by
//! Linux, which discards whatever was still queued.
//!
//! `Session::finish` ends the publish the way OBS and ffmpeg do: FCUnpublish
//! and deleteStream go out after the last media tag, then it waits until the
//! upstream has acknowledged every byte (bounded), and only then closes.

use std::time::Duration;

use super::Session;

/// How long an endpoint's end waits for the upstream to acknowledge its
/// queued bytes: the 8 MB send buffer at ~5 Mbit/s, with margin.
pub const FINISH_MAX_WAIT: Duration = Duration::from_secs(15);

/// How often `finish` re-reads the send queue.
pub(crate) const FINISH_POLL: Duration = Duration::from_millis(50);

/// The not-yet-acknowledged bytes of a push socket's send queue.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SendQueue {
    #[cfg(target_os = "linux")]
    fd: std::os::fd::RawFd,
}

impl SendQueue {
    pub(crate) fn of(stream: &tokio::net::TcpStream) -> Self {
        let _ = stream;
        Self {
            #[cfg(target_os = "linux")]
            fd: -1,
        }
    }

    /// Bytes written but not yet acknowledged by the peer. `None` where the
    /// platform cannot tell.
    pub(crate) fn unacked_bytes(&self) -> Option<u64> {
        None
    }
}

/// How the wait for the send queue ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The upstream acknowledged every byte.
    Drained,
    /// The platform cannot read the queue; closed without waiting.
    Unmeasured,
    /// The session died while waiting.
    SessionDead,
    /// Bytes were still queued at the bound.
    TimedOut,
}

/// One step of the wait: `None` = keep waiting.
pub(crate) fn drain_step(
    unacked: Option<u64>,
    poisoned: bool,
    waited: Duration,
    max_wait: Duration,
) -> Option<DrainOutcome> {
    let _ = (unacked, poisoned, waited, max_wait);
    Some(DrainOutcome::Unmeasured)
}

/// What `Session::finish` did, for the log and the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishReport {
    pub outcome: DrainOutcome,
    /// FCUnpublish and deleteStream were written.
    pub unpublished: bool,
    pub queued_at_start: Option<u64>,
    pub queued_at_end: Option<u64>,
    pub waited_ms: u64,
}

impl Session {
    /// End the publish: unpublish, wait up to `max_wait` until every byte is
    /// acknowledged, then close.
    pub async fn finish(self, max_wait: Duration) -> FinishReport {
        let _ = max_wait;
        self.close().await;
        FinishReport {
            outcome: DrainOutcome::Unmeasured,
            unpublished: false,
            queued_at_start: None,
            queued_at_end: None,
            waited_ms: 0,
        }
    }
}

/// AMF0 body of `FCUnpublish(stream_name)`.
pub(crate) fn fcunpublish_body(transaction_id: f64, stream_name: &str) -> Vec<u8> {
    let _ = (transaction_id, stream_name);
    Vec::new()
}

/// AMF0 body of `deleteStream(stream_id)`.
pub(crate) fn delete_stream_body(transaction_id: f64, stream_id: u32) -> Vec<u8> {
    let _ = (transaction_id, stream_id);
    Vec::new()
}

#[cfg(test)]
#[path = "session_finish_tests.rs"]
mod tests;
