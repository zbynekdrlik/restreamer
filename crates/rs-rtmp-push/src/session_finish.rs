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

use std::io;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rtmp::chunk::ChunkInfo;
use rtmp::chunk::define::csid_type;

use super::{AMF0_MARKER_NUMBER, Session, write_amf0_string};
use crate::PushError;

/// AMF0 null marker.
const AMF0_MARKER_NULL: u8 = 0x05;
/// RTMP message type of an AMF0 command.
const MSG_TYPE_AMF0_COMMAND: u8 = 20;
/// Transaction ids after `publish` (3).
const TID_FCUNPUBLISH: f64 = 4.0;
const TID_DELETE_STREAM: f64 = 5.0;

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
    /// The session keeps `stream` open for as long as it reads this.
    pub(crate) fn of(stream: &tokio::net::TcpStream) -> Self {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            Self {
                fd: stream.as_raw_fd(),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stream;
            Self {}
        }
    }

    /// A queue that cannot be read (tests over a fake transport).
    #[cfg(test)]
    pub(crate) fn unmeasured() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            fd: -1,
        }
    }

    /// Bytes written but not yet acknowledged by the peer. `None` where the
    /// platform cannot tell.
    #[allow(unsafe_code)]
    pub(crate) fn unacked_bytes(&self) -> Option<u64> {
        #[cfg(target_os = "linux")]
        {
            let mut n: libc::c_int = 0;
            // SAFETY: `fd` is the session's own socket, open while the session
            // lives; TIOCOUTQ writes one c_int (unsent plus unacknowledged).
            let rc = unsafe { libc::ioctl(self.fd, libc::TIOCOUTQ, &mut n) };
            if rc != 0 {
                return None;
            }
            u64::try_from(n).ok()
        }
        #[cfg(not(target_os = "linux"))]
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
    if poisoned {
        return Some(DrainOutcome::SessionDead);
    }
    match unacked {
        None => Some(DrainOutcome::Unmeasured),
        Some(0) => Some(DrainOutcome::Drained),
        Some(_) if waited >= max_wait => Some(DrainOutcome::TimedOut),
        Some(_) => None,
    }
}

/// How many queue reads fit in `max_wait`, plus the first one at 0.
pub(crate) fn finish_polls(max_wait: Duration) -> u128 {
    max_wait.as_millis() / FINISH_POLL.as_millis() + 1
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
    ///
    /// The commands go out after the last media tag, so the drain covers them
    /// too. The read loop keeps reading the upstream's acks meanwhile, so the
    /// close that follows a drain has nothing left to discard.
    pub async fn finish(mut self, max_wait: Duration) -> FinishReport {
        let started = tokio::time::Instant::now();
        let queued_at_start = self.send_queue.unacked_bytes();
        let unpublished = match self.unpublish().await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(error = ?e, "rtmp_push: unpublish not sent (#424)");
                false
            }
        };
        // Bounded by a count as well as by `max_wait`, so the wait ends even if
        // the clock or the queue reading misbehaves.
        let polls = finish_polls(max_wait);
        let mut outcome = DrainOutcome::TimedOut;
        for _ in 0..polls {
            let step = drain_step(
                self.send_queue.unacked_bytes(),
                self.poisoned.load(Ordering::Relaxed),
                started.elapsed(),
                max_wait,
            );
            if let Some(done) = step {
                outcome = done;
                break;
            }
            tokio::time::sleep(FINISH_POLL).await;
        }
        let report = FinishReport {
            outcome,
            unpublished,
            queued_at_start,
            queued_at_end: self.send_queue.unacked_bytes(),
            waited_ms: started.elapsed().as_millis() as u64,
        };
        match report.outcome {
            DrainOutcome::Drained | DrainOutcome::Unmeasured => {
                tracing::info!(?report, "rtmp_push: publish finished (#424)")
            }
            DrainOutcome::SessionDead | DrainOutcome::TimedOut => tracing::warn!(
                ?report,
                "rtmp_push: publish finished with bytes possibly lost (#424)"
            ),
        }
        self.close().await;
        report
    }

    async fn unpublish(&mut self) -> Result<(), PushError> {
        let fc = fcunpublish_body(TID_FCUNPUBLISH, &self.stream_name);
        self.send_command(&fc).await?;
        let del = delete_stream_body(TID_DELETE_STREAM, self.msg_stream_id);
        self.send_command(&del).await
    }

    /// An AMF0 command on the NetConnection (message stream 0).
    async fn send_command(&mut self, body: &[u8]) -> Result<(), PushError> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(PushError::RemoteClosed(io::Error::from(
                io::ErrorKind::ConnectionReset,
            )));
        }
        let mut chunk_info = ChunkInfo::new(
            csid_type::COMMAND_AMF0_AMF3,
            0,
            0,
            body.len() as u32,
            MSG_TYPE_AMF0_COMMAND,
            0,
            bytes::BytesMut::from(body),
        );
        self.packetizer
            .write_chunk(&mut chunk_info)
            .await
            .map_err(crate::error::map_write_err)
    }
}

fn write_amf0_number(buf: &mut Vec<u8>, n: f64) {
    buf.push(AMF0_MARKER_NUMBER);
    buf.extend_from_slice(&n.to_be_bytes());
}

/// AMF0 body of `FCUnpublish(stream_name)`.
pub(crate) fn fcunpublish_body(transaction_id: f64, stream_name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    write_amf0_string(&mut buf, "FCUnpublish");
    write_amf0_number(&mut buf, transaction_id);
    buf.push(AMF0_MARKER_NULL);
    write_amf0_string(&mut buf, stream_name);
    buf
}

/// AMF0 body of `deleteStream(stream_id)`.
pub(crate) fn delete_stream_body(transaction_id: f64, stream_id: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    write_amf0_string(&mut buf, "deleteStream");
    write_amf0_number(&mut buf, transaction_id);
    buf.push(AMF0_MARKER_NULL);
    write_amf0_number(&mut buf, f64::from(stream_id));
    buf
}

#[cfg(test)]
#[path = "session_finish_tests.rs"]
mod tests;
