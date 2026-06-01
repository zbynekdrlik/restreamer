//! Error types surfaced by `RtmpPusher`. See spec §4.1 + §5.3.

use bytesio::bytesio_errors::{BytesIOError, BytesIOErrorValue};
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
        // RemoteClosed = upstream (YT/FB) rotated the connection. Common
        // periodic event (~12-15 min on YT) NOT caused by us. 30 s backoff
        // was punishing the operator: every reset added 30 s of cache
        // overshoot before the pusher even reconnected. 3 s lets the
        // pusher reconnect almost immediately while still avoiding a
        // tight reconnect loop if upstream keeps rejecting.
        PushError::RemoteClosed(_) => Some(3_000),
        PushError::Timeout => Some(10_000),
        PushError::IoError(_) => Some(15_000),
        PushError::MalformedInput { .. } => Some(15_000),
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
            // RemoteClosed = upstream-initiated rotation (e.g. YT load
            // balancer churn every ~12-15 min). Never exponential — each
            // event is independent of our behaviour, escalating wastes
            // cache time we just got.
            | PushError::RemoteClosed(_)
            | PushError::MalformedInput { .. }
            | PushError::LocalCancel
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

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
}
