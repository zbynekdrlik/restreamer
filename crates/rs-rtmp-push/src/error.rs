//! Error types surfaced by `RtmpPusher`. See spec §4.1 + §5.3.

use bytesio::bytes_errors::{BytesWriteError, BytesWriteErrorValue};
use bytesio::bytesio_errors::{BytesIOError, BytesIOErrorValue};
use rtmp::chunk::errors::{PackError, PackErrorValue};
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

    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),

    #[error("malformed FLV input at offset {offset}: {reason}")]
    MalformedInput { offset: usize, reason: String },

    /// Issue #257: the cross-track content-PTS A/V skew exceeded
    /// `MAX_AV_SKEW_MS` for the debounce window. Returned by `push_flv_bytes`
    /// to force a CLEAN reconnect so both tracks re-anchor from a common
    /// session start. This is a deliberate recovery signal, not a transport
    /// fault — the consumer's error arm already force-closes + reconnects,
    /// which is exactly the bounded recovery this variant requests.
    #[error("A/V skew exceeded: {skew_ms} ms (video ahead of audio)")]
    AvSkewExceeded { skew_ms: i64 },
}

/// Backoff floor in milliseconds for a given error variant. Mirrors today's
/// `crates/rs-delivery/src/ffmpeg_reason.rs::reconnect_floor` semantics.
///
/// The endpoint task multiplies this by `2^consecutive_errors` and caps at 300_000
/// (5 min). All `PublishRejected` variants share the same 30 s floor; the
/// BadName-specific behaviour (no exponential escalation) is encoded in
/// `is_exponential`, not here. `LocalCancel` returns `None` (no retry).
pub fn backoff_floor_ms(err: &PushError) -> Option<u64> {
    match err {
        PushError::HandshakeFailed(_) => Some(5_000),
        PushError::TlsHandshakeFailed(_) => Some(5_000),
        PushError::ConnectRejected { .. } => Some(30_000),
        PushError::PublishRejected { .. } => Some(30_000),
        // RemoteClosed = the upstream (YT/FB) ended the connection, not us.
        // There is no fixed period: production YouTube closes came after
        // 27 min to 500 min of session (#368); only the CI `e2e rtmp`
        // endpoint sees one every ~15 min. 30 s backoff was punishing the
        // operator: every reset added 30 s of cache overshoot before the
        // pusher even reconnected. 3 s avoids a tight reconnect loop if
        // upstream keeps rejecting; the FIRST close after a healthy minute
        // reconnects at once (`reconnect_delay_ms`, #368).
        PushError::RemoteClosed(_) => Some(3_000),
        PushError::Timeout => Some(10_000),
        PushError::IoError(_) => Some(15_000),
        PushError::MalformedInput { .. } => Some(15_000),
        // AvSkewExceeded (#257) is a deliberate recovery reconnect, not a
        // fault. A short 3 s floor reconnects almost immediately so the
        // desync clears fast; the reconnect-thrash protection lives in
        // SkewTracker's debounce + min-interval rate limit, NOT in a punishing
        // backoff. Same floor as RemoteClosed for the same reason (the reset
        // is wanted, not penalised).
        PushError::AvSkewExceeded { .. } => Some(3_000),
        PushError::LocalCancel => None,
    }
}

/// Map a `BytesIOError` produced by xiu's `TNetIO::read()` (or `read_timeout`)
/// onto the right `PushError` variant.
///
/// Issue #168: `BytesIOErrorValue::NoneReturn` is the bytesio crate's signal
/// that the peer cleanly closed the TCP/TLS connection (the underlying
/// framed-codec stream ran dry). For RTMP that is **upstream-initiated
/// disconnect** — exactly what `PushError::RemoteClosed` already encodes (3 s
/// backoff floor, non-exponential). Wrapping it in `IoError` was wrong:
/// 15 s backoff per reset bloated the cache overshoot on FB endpoints.
///
/// Apply at every READ site in session.rs that does `.read().await` and was
/// previously `.map_err(|e| PushError::IoError(io::Error::other(e.to_string())))`.
pub fn map_read_err(err: BytesIOError) -> PushError {
    match err.value {
        BytesIOErrorValue::NoneReturn => {
            PushError::RemoteClosed(io::Error::from(io::ErrorKind::UnexpectedEof))
        }
        BytesIOErrorValue::IOError(io_err) => PushError::IoError(io_err),
        BytesIOErrorValue::TimeoutError(_) => PushError::Timeout,
        // ClientClosed / NotEnoughBytes / fall-through: treat as ordinary
        // I/O error (15 s backoff). These are diagnostic-quality variants
        // that should not occur on a healthy session.
        other => PushError::IoError(io::Error::other(format!("{other:?}"))),
    }
}

/// Map an error from writing a chunk (xiu's `ChunkPacketizer::write_chunk`)
/// onto a `PushError`. A write the upstream ended (reset, broken pipe, EOF: a
/// YouTube ingest session close, e.g. the 2026-10-15 "Broken pipe" after 44
/// min) is `RemoteClosed`, as on the read side; any other write failure stays
/// `IoError`.
pub fn map_write_err(err: PackError) -> PushError {
    let io_err = match err.value {
        PackErrorValue::BytesWriteError(BytesWriteError {
            value: BytesWriteErrorValue::IO(e),
        }) => e,
        PackErrorValue::BytesWriteError(BytesWriteError {
            value: BytesWriteErrorValue::BytesIOError(BytesIOError { value }),
        }) => match value {
            BytesIOErrorValue::IOError(e) => e,
            BytesIOErrorValue::NoneReturn => io::Error::from(io::ErrorKind::UnexpectedEof),
            other => return PushError::IoError(io::Error::other(format!("write: {other}"))),
        },
        other => return PushError::IoError(io::Error::other(format!("write: {other}"))),
    };
    if peer_gone(io_err.kind()) {
        PushError::RemoteClosed(io_err)
    } else {
        PushError::IoError(io_err)
    }
}

/// The upstream ended the connection (as opposed to a local or unknown fault).
fn peer_gone(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::NotConnected
    )
}

/// #368: whether a push that failed with `err` may have left (part of) its
/// chunk undelivered upstream, so the consumer must push that SAME chunk
/// once more on the fresh session.
///
/// A transport fault (`RemoteClosed`, `IoError`, `Timeout`) can strike mid
/// chunk, and the bytes still in the 4 MB socket send buffer are lost with
/// the connection. A connect-time failure sent nothing of it at all. An
/// `AvSkewExceeded` trip is raised only AFTER the whole chunk went out, a
/// `MalformedInput` chunk would fail identically again, and `LocalCancel`
/// stops the endpoint.
pub fn may_lose_chunk(err: &PushError) -> bool {
    match err {
        PushError::RemoteClosed(_)
        | PushError::IoError(_)
        | PushError::Timeout
        | PushError::HandshakeFailed(_)
        | PushError::TlsHandshakeFailed(_)
        | PushError::ConnectRejected { .. }
        | PushError::PublishRejected { .. } => true,
        PushError::AvSkewExceeded { .. }
        | PushError::MalformedInput { .. }
        | PushError::LocalCancel => false,
    }
}

/// #368: a session that pushed media at least this long before the upstream
/// closed it earns an IMMEDIATE reconnect. One minute keeps a target that
/// accepts a connect and then drops it again from turning the zero delay
/// into a reconnect hammer.
pub const FAST_RECONNECT_MIN_HEALTHY_MS: u64 = 60_000;

/// #368: the short retry after a rejection that directly follows a fast
/// reconnect (the platform may still hold the old session for a moment).
/// Same as the `RemoteClosed` floor.
pub const SHORT_RETRY_AFTER_FAST_MS: u64 = 3_000;

/// #368: the delay before the next connect after `err`, or `None` for
/// `LocalCancel` (no retry).
///
/// - The FIRST upstream close (`RemoteClosed`, `consecutive_errors == 1`)
///   of a session that pushed media for at least
///   `FAST_RECONNECT_MIN_HEALTHY_MS` reconnects at once (0 ms). Such a close
///   is the platform ending one ingest session (YouTube, 2026-10-15), and
///   the old fixed 3 s floor was the bulk of the viewer-visible gap.
/// - A publish/connect rejection that directly follows such a fast retry
///   gets one short retry (`SHORT_RETRY_AFTER_FAST_MS`) instead of the 30 s
///   rejection floor: the platform may still hold the session that just
///   closed.
/// - Everything else keeps the pre-#368 ladder: the error's floor, doubled
///   per consecutive error for the exponential classes, capped at 5 min.
pub fn reconnect_delay_ms(
    err: &PushError,
    consecutive_errors: u32,
    healthy_session_ms: u64,
    follows_fast_retry: bool,
) -> Option<u64> {
    let floor_ms = backoff_floor_ms(err)?;
    if consecutive_errors == 1
        && healthy_session_ms >= FAST_RECONNECT_MIN_HEALTHY_MS
        && matches!(err, PushError::RemoteClosed(_))
    {
        return Some(0);
    }
    if follows_fast_retry
        && matches!(
            err,
            PushError::PublishRejected { .. } | PushError::ConnectRejected { .. }
        )
    {
        return Some(SHORT_RETRY_AFTER_FAST_MS);
    }
    if is_exponential(err) {
        let factor = 1u64 << (consecutive_errors.saturating_sub(1).min(5));
        Some(floor_ms.saturating_mul(factor).min(300_000))
    } else {
        Some(floor_ms)
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
            | PushError::TlsHandshakeFailed(_)
            // RemoteClosed = an upstream-initiated session close (no fixed
            // period, #368). Never exponential — each event is independent
            // of our behaviour, escalating wastes cache time we just got.
            | PushError::RemoteClosed(_)
            | PushError::MalformedInput { .. }
            // AvSkewExceeded (#257) = deliberate recovery reconnect. The
            // SkewTracker rate limit (min 60 s between trips) already prevents
            // thrash; escalating the backoff exponentially would only waste
            // delivery time we want back. Fixed floor.
            | PushError::AvSkewExceeded { .. }
            | PushError::LocalCancel
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    // --- map_write_err (#368: a YouTube rotation seen on the WRITE side) ---

    fn write_io(kind: io::ErrorKind) -> PackError {
        PackError {
            value: PackErrorValue::BytesWriteError(BytesWriteError {
                value: BytesWriteErrorValue::BytesIOError(BytesIOError {
                    value: BytesIOErrorValue::IOError(io::Error::from(kind)),
                }),
            }),
        }
    }

    #[test]
    fn a_write_the_upstream_ended_is_remote_closed_with_the_short_floor() {
        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::NotConnected,
        ] {
            let e = map_write_err(write_io(kind));
            assert!(matches!(e, PushError::RemoteClosed(_)), "{kind:?} -> {e:?}");
            assert_eq!(backoff_floor_ms(&e), Some(3_000), "{kind:?}");
        }
        let direct = PackError {
            value: PackErrorValue::BytesWriteError(BytesWriteError {
                value: BytesWriteErrorValue::IO(io::Error::from(io::ErrorKind::ConnectionReset)),
            }),
        };
        assert!(matches!(map_write_err(direct), PushError::RemoteClosed(_)));
        let none = PackError {
            value: PackErrorValue::BytesWriteError(BytesWriteError {
                value: BytesWriteErrorValue::BytesIOError(BytesIOError {
                    value: BytesIOErrorValue::NoneReturn,
                }),
            }),
        };
        assert!(matches!(map_write_err(none), PushError::RemoteClosed(_)));
    }

    #[test]
    fn any_other_write_failure_stays_an_io_error() {
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::OutOfMemory,
        ] {
            let e = map_write_err(write_io(kind));
            assert!(
                matches!(&e, PushError::IoError(io) if io.kind() == kind),
                "{kind:?} -> {e:?}"
            );
        }
        let header = PackError {
            value: PackErrorValue::NotExistHeader,
        };
        assert!(matches!(map_write_err(header), PushError::IoError(_)));
        let timeout = PackError {
            value: PackErrorValue::BytesWriteError(BytesWriteError {
                value: BytesWriteErrorValue::Timeout,
            }),
        };
        assert!(matches!(map_write_err(timeout), PushError::IoError(_)));
    }

    // --- backoff_floor_ms ---

    #[test]
    fn backoff_floor_handshake_failed_is_5000() {
        let e = PushError::HandshakeFailed(io::Error::other("x"));
        assert_eq!(backoff_floor_ms(&e), Some(5_000));
    }

    #[test]
    fn backoff_floor_connect_rejected_is_30000() {
        let e = PushError::ConnectRejected {
            code: "NetConnection.Connect.Rejected".into(),
            description: "y".into(),
        };
        assert_eq!(backoff_floor_ms(&e), Some(30_000));
    }

    #[test]
    fn backoff_floor_publish_rejected_bad_name_is_30000() {
        let e = PushError::PublishRejected {
            code: "NetStream.Publish.BadName".into(),
            description: String::new(),
        };
        assert_eq!(backoff_floor_ms(&e), Some(30_000));
    }

    #[test]
    fn backoff_floor_publish_rejected_other_code_is_30000() {
        let e = PushError::PublishRejected {
            code: "NetStream.Publish.OtherCode".into(),
            description: String::new(),
        };
        assert_eq!(backoff_floor_ms(&e), Some(30_000));
    }

    #[test]
    fn backoff_floor_remote_closed_is_3000() {
        let e = PushError::RemoteClosed(io::Error::new(io::ErrorKind::ConnectionReset, "x"));
        assert_eq!(backoff_floor_ms(&e), Some(3_000));
    }

    #[test]
    fn backoff_floor_timeout_is_10000() {
        assert_eq!(backoff_floor_ms(&PushError::Timeout), Some(10_000));
    }

    #[test]
    fn backoff_floor_io_error_is_15000() {
        let e = PushError::IoError(io::Error::other("x"));
        assert_eq!(backoff_floor_ms(&e), Some(15_000));
    }

    #[test]
    fn backoff_floor_malformed_input_is_15000() {
        let e = PushError::MalformedInput {
            offset: 0,
            reason: "x".into(),
        };
        assert_eq!(backoff_floor_ms(&e), Some(15_000));
    }

    #[test]
    fn backoff_floor_local_cancel_is_none() {
        assert_eq!(backoff_floor_ms(&PushError::LocalCancel), None);
    }

    #[test]
    fn backoff_floor_av_skew_exceeded_is_3000() {
        // #257: skew recovery reconnects fast; rate limit lives in SkewTracker,
        // not in a punishing backoff.
        let e = PushError::AvSkewExceeded { skew_ms: 25_500 };
        assert_eq!(backoff_floor_ms(&e), Some(3_000));
    }

    #[test]
    fn is_exponential_av_skew_exceeded_is_false() {
        let e = PushError::AvSkewExceeded { skew_ms: 25_500 };
        assert!(
            !is_exponential(&e),
            "AvSkewExceeded is a deliberate recovery reconnect; never escalate"
        );
    }

    #[test]
    fn av_skew_exceeded_display_contains_skew() {
        let e = PushError::AvSkewExceeded { skew_ms: 25_500 };
        assert!(
            e.to_string().contains("25500"),
            "Display must carry the skew value for operator triage, got {e}"
        );
    }

    // --- is_exponential ---

    #[test]
    fn is_exponential_bad_name_is_false() {
        let e = PushError::PublishRejected {
            code: "NetStream.Publish.BadName".into(),
            description: String::new(),
        };
        assert!(
            !is_exponential(&e),
            "BadName must NOT be exponential: operator must rotate the key"
        );
    }

    #[test]
    fn is_exponential_publish_rejected_other_code_is_true() {
        let e = PushError::PublishRejected {
            code: "NetStream.Publish.SomeOther".into(),
            description: String::new(),
        };
        assert!(
            is_exponential(&e),
            "non-BadName PublishRejected MUST be exponential"
        );
    }

    #[test]
    fn is_exponential_connect_rejected_is_true() {
        let e = PushError::ConnectRejected {
            code: "NetConnection.Connect.Rejected".into(),
            description: "y".into(),
        };
        assert!(is_exponential(&e));
    }

    #[test]
    fn is_exponential_remote_closed_is_false() {
        let e = PushError::RemoteClosed(io::Error::new(io::ErrorKind::ConnectionReset, "x"));
        assert!(
            !is_exponential(&e),
            "RemoteClosed = upstream-initiated rotation; never escalate"
        );
    }

    #[test]
    fn is_exponential_timeout_is_false() {
        assert!(
            !is_exponential(&PushError::Timeout),
            "Timeout uses fixed floor, not exponential"
        );
    }

    #[test]
    fn is_exponential_io_error_is_false() {
        let e = PushError::IoError(io::Error::other("x"));
        assert!(
            !is_exponential(&e),
            "IoError uses fixed floor, not exponential"
        );
    }

    #[test]
    fn is_exponential_handshake_failed_is_false() {
        let e = PushError::HandshakeFailed(io::Error::other("x"));
        assert!(
            !is_exponential(&e),
            "HandshakeFailed uses fixed floor, not exponential"
        );
    }

    #[test]
    fn is_exponential_malformed_input_is_false() {
        let e = PushError::MalformedInput {
            offset: 0,
            reason: "x".into(),
        };
        assert!(
            !is_exponential(&e),
            "MalformedInput uses fixed floor, not exponential"
        );
    }

    #[test]
    fn is_exponential_local_cancel_is_false() {
        assert!(
            !is_exponential(&PushError::LocalCancel),
            "LocalCancel returns None from backoff_floor_ms, is_exponential must be false"
        );
    }

    #[test]
    fn backoff_floor_tls_handshake_failed_is_5000() {
        let e = PushError::TlsHandshakeFailed("rustls: handshake error".into());
        assert_eq!(backoff_floor_ms(&e), Some(5_000));
    }

    #[test]
    fn is_exponential_tls_handshake_failed_is_false() {
        let e = PushError::TlsHandshakeFailed("rustls: handshake error".into());
        assert!(
            !is_exponential(&e),
            "TlsHandshakeFailed uses fixed floor, not exponential"
        );
    }

    // --- reconnect_delay_ms (#368: fast first reconnect) ---

    fn broken_pipe() -> PushError {
        PushError::RemoteClosed(io::Error::from(io::ErrorKind::BrokenPipe))
    }

    fn bad_name() -> PushError {
        PushError::PublishRejected {
            code: "NetStream.Publish.BadName".into(),
            description: String::new(),
        }
    }

    #[test]
    fn the_first_close_after_a_healthy_minute_reconnects_at_once() {
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 1, 60_000, false),
            Some(0)
        );
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 1, 2_629_000, false),
            Some(0)
        );
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 1, 59_999, false),
            Some(3_000),
            "a session under a minute keeps the 3 s floor"
        );
    }

    #[test]
    fn only_the_first_consecutive_close_is_fast() {
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 2, 60_000, false),
            Some(3_000)
        );
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 2, 60_000, true),
            Some(3_000)
        );
        assert_eq!(
            reconnect_delay_ms(&broken_pipe(), 0, 60_000, false),
            Some(3_000)
        );
    }

    #[test]
    fn only_an_upstream_close_is_fast() {
        let io = PushError::IoError(io::Error::other("x"));
        assert_eq!(reconnect_delay_ms(&io, 1, 600_000, false), Some(15_000));
        let skew = PushError::AvSkewExceeded { skew_ms: 4_500 };
        assert_eq!(reconnect_delay_ms(&skew, 1, 600_000, false), Some(3_000));
        assert_eq!(
            reconnect_delay_ms(&PushError::Timeout, 1, 600_000, false),
            Some(10_000)
        );
    }

    #[test]
    fn a_rejection_right_after_a_fast_retry_gets_a_short_retry() {
        // The platform may still hold the session that just closed; one
        // short retry instead of the 30 s rejection floor.
        assert_eq!(
            reconnect_delay_ms(&bad_name(), 2, 0, true),
            Some(SHORT_RETRY_AFTER_FAST_MS)
        );
        let other = PushError::PublishRejected {
            code: "NetStream.Publish.Busy".into(),
            description: String::new(),
        };
        assert_eq!(reconnect_delay_ms(&other, 2, 0, true), Some(3_000));
        let connect = PushError::ConnectRejected {
            code: "NetConnection.Connect.Rejected".into(),
            description: String::new(),
        };
        assert_eq!(reconnect_delay_ms(&connect, 2, 0, true), Some(3_000));
        // Not after a fast retry: the rejection floors are unchanged.
        assert_eq!(reconnect_delay_ms(&bad_name(), 2, 0, false), Some(30_000));
        assert_eq!(reconnect_delay_ms(&other, 2, 0, false), Some(60_000));
        assert_eq!(reconnect_delay_ms(&connect, 1, 0, false), Some(30_000));
        // A handshake failure after a fast retry keeps its own short floor.
        let hs = PushError::HandshakeFailed(io::Error::other("x"));
        assert_eq!(reconnect_delay_ms(&hs, 2, 0, true), Some(5_000));
    }

    #[test]
    fn reconnect_delay_keeps_the_exponential_ladder_and_its_cap() {
        let connect = PushError::ConnectRejected {
            code: "NetConnection.Connect.Rejected".into(),
            description: String::new(),
        };
        assert_eq!(reconnect_delay_ms(&connect, 2, 0, false), Some(60_000));
        assert_eq!(reconnect_delay_ms(&connect, 4, 0, false), Some(240_000));
        assert_eq!(reconnect_delay_ms(&connect, 10, 0, false), Some(300_000));
        assert_eq!(
            reconnect_delay_ms(&PushError::LocalCancel, 1, 600_000, true),
            None
        );
    }

    // --- may_lose_chunk (#368: re-send the failed chunk once) ---

    #[test]
    fn a_transport_or_connect_failure_may_lose_the_chunk() {
        let lost = [
            PushError::RemoteClosed(io::Error::from(io::ErrorKind::BrokenPipe)),
            PushError::IoError(io::Error::other("x")),
            PushError::Timeout,
            PushError::HandshakeFailed(io::Error::other("x")),
            PushError::TlsHandshakeFailed("x".into()),
            PushError::ConnectRejected {
                code: "NetConnection.Connect.Rejected".into(),
                description: String::new(),
            },
            PushError::PublishRejected {
                code: "NetStream.Publish.BadName".into(),
                description: String::new(),
            },
        ];
        for e in lost {
            assert!(may_lose_chunk(&e), "{e:?} must re-send the chunk");
        }
    }

    #[test]
    fn a_skew_trip_bad_input_or_cancel_never_resends() {
        // AvSkewExceeded is returned AFTER the whole chunk went out; a
        // malformed chunk would fail identically again; a cancel stops.
        for e in [
            PushError::AvSkewExceeded { skew_ms: 4_500 },
            PushError::MalformedInput {
                offset: 0,
                reason: "x".into(),
            },
            PushError::LocalCancel,
        ] {
            assert!(!may_lose_chunk(&e), "{e:?} must not re-send");
        }
    }

    // --- map_read_err ---
    //
    // Issue #168: "I/O error: none return" on FB endpoints was traced to
    // wait_for_publish_start / wait_for_create_stream_response wrapping
    // `BytesIOErrorValue::NoneReturn` (= peer EOF) into PushError::IoError
    // (15 s backoff) instead of PushError::RemoteClosed (3 s backoff).
    // map_read_err is the centralizing helper applied at every READ site
    // in session.rs so the variant carries through.

    #[test]
    fn map_read_err_none_return_is_remote_closed() {
        let e = BytesIOError {
            value: BytesIOErrorValue::NoneReturn,
        };
        let mapped = map_read_err(e);
        match mapped {
            PushError::RemoteClosed(_) => {}
            other => panic!("expected RemoteClosed, got {other:?}"),
        }
    }

    #[test]
    fn map_read_err_io_error_stays_io_error() {
        let e = BytesIOError {
            value: BytesIOErrorValue::IOError(io::Error::other("x")),
        };
        let mapped = map_read_err(e);
        match mapped {
            PushError::IoError(_) => {}
            other => panic!("expected IoError, got {other:?}"),
        }
    }

    #[test]
    fn map_read_err_remote_closed_has_unexpected_eof_kind() {
        // Killer: catches a mutant that produces RemoteClosed(io::Error)
        // with the wrong ErrorKind. The 15s-vs-3s decision lives in
        // backoff_floor_ms and works on the variant, but downstream
        // operator tooling reads ErrorKind for triage.
        let e = BytesIOError {
            value: BytesIOErrorValue::NoneReturn,
        };
        if let PushError::RemoteClosed(inner) = map_read_err(e) {
            assert_eq!(inner.kind(), io::ErrorKind::UnexpectedEof);
        } else {
            panic!("expected RemoteClosed");
        }
    }

    #[test]
    fn backoff_floor_for_mapped_none_return_is_3000_not_15000() {
        // Round-trip: NoneReturn -> RemoteClosed -> 3s floor (not 15s).
        // This is the production-impact assertion: 12s saved per reset
        // event, which historically inflated cache overshoot on FB.
        let e = BytesIOError {
            value: BytesIOErrorValue::NoneReturn,
        };
        let mapped = map_read_err(e);
        assert_eq!(backoff_floor_ms(&mapped), Some(3_000));
    }

    #[test]
    fn remote_closed_display_carries_ci_gate_rotation_marker() {
        // Contract lock for the FB/YT 30-min soak gate in ci.yml. That gate
        // classifies an `endpoint_rtmp_push_died` audit row as an UPSTREAM
        // ROTATION (tolerated -- FB/YT reset our ingest ~every 5-15 min and the
        // pusher reconnects in ~3 s) vs a LOCAL/pusher fault (fails the soak)
        // by substring-matching the audit `detail.error` against the literal
        // "upstream closed connection mid-stream". That string is THIS
        // variant's Display. If a refactor changes it, the gate would
        // misclassify every FB/YT rotation as a pusher regression and the
        // 30-min soak would flake on the first upstream reset again -- the exact
        // #227 fragility this test guards. Keep the marker and the ci.yml
        // classifier in sync.
        let e = PushError::RemoteClosed(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(
            e.to_string()
                .contains("upstream closed connection mid-stream"),
            "RemoteClosed Display must contain the ci.yml soak-gate rotation marker \
             'upstream closed connection mid-stream', got: {e}"
        );
    }
}
