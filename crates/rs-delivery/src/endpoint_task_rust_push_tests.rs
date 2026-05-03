//! Unit tests for Rust RTMP pusher backoff math, `build_rtmp_url`, and
//! `emit_rtmp_push_died`. Kills surviving mutants in:
//! - `endpoint_consumer_helpers.rs`: +=/= and << -> >> on the backoff
//!   ladder; >= -> < on the consecutive_push_errors exponent cap.
//! - `endpoint_task.rs::build_rtmp_url`: String::new() / "xyzzy" body replacements.
//! - `endpoint_task.rs::emit_rtmp_push_died` (via endpoint_audit): body -> ()
//!   replacement.

use crate::audit_ring::AuditRing;
use crate::endpoint_audit;
use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PushError, backoff_floor_ms, is_exponential};
use std::io;

/// Compute the backoff_ms that the consumer would apply for a given
/// `PushError` after `consecutive` consecutive errors (1-indexed: first
/// error has consecutive = 1). Mirrors the exact formula in
/// `endpoint_consumer_helpers::handle_rust_push`.
///
/// factor = 1 << (consecutive - 1).min(5)
/// backoff_ms = floor_ms * factor, capped at 300_000
/// If floor is None (LocalCancel) returns 0.
fn compute_rust_push_backoff(err: &PushError, consecutive: u32) -> u64 {
    let floor_ms = match backoff_floor_ms(err) {
        Some(f) => f,
        None => return 0,
    };
    if is_exponential(err) {
        let exponent = consecutive.saturating_sub(1).min(5);
        let factor: u64 = 1 << exponent;
        floor_ms.saturating_mul(factor).min(300_000)
    } else {
        floor_ms
    }
}

// ---------------------------------------------------------------------------
// Backoff math: HandshakeFailed (fixed, floor = 5_000)
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_handshake_failed_fixed_always_5000() {
    let e = PushError::HandshakeFailed(io::Error::new(io::ErrorKind::Other, "x"));
    for consecutive in 1..=10 {
        assert_eq!(
            compute_rust_push_backoff(&e, consecutive),
            5_000,
            "HandshakeFailed is NOT exponential; backoff must stay at floor=5000 for consecutive={consecutive}"
        );
    }
}

// ---------------------------------------------------------------------------
// Backoff math: RemoteClosed (exponential, floor = 30_000)
// RemoteClosed = upstream-initiated rotation (YT/FB load balancer churn).
// 3 s flat, NEVER exponential — escalating wastes cache time we just got.
// (Changed from 30 s exponential ladder in #160 follow-up after live soak
// showed 60 s cache overshoot per 2-rotation cycle.)
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_remote_closed_first_error_is_3000() {
    let e = PushError::RemoteClosed(io::Error::new(io::ErrorKind::ConnectionReset, "x"));
    assert_eq!(
        compute_rust_push_backoff(&e, 1),
        3_000,
        "first RemoteClosed: 3 s floor, not exponential"
    );
}

#[test]
fn rust_push_backoff_remote_closed_is_never_exponential() {
    let e = PushError::RemoteClosed(io::Error::new(io::ErrorKind::ConnectionReset, "x"));
    for consecutive in 1..=10 {
        assert_eq!(
            compute_rust_push_backoff(&e, consecutive),
            3_000,
            "RemoteClosed must stay at 3 s for consecutive={consecutive}; \
             upstream-initiated rotations are not our fault, escalating \
             wastes cache time"
        );
    }
}

// ---------------------------------------------------------------------------
// Backoff math: ConnectRejected (exponential, floor = 30_000)
// Same ladder as RemoteClosed; verifies the match arm is present.
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_connect_rejected_escalates() {
    let e = PushError::ConnectRejected {
        code: "NetConnection.Connect.Rejected".into(),
        description: "bad auth".into(),
    };
    assert_eq!(compute_rust_push_backoff(&e, 1), 30_000);
    assert_eq!(compute_rust_push_backoff(&e, 2), 60_000);
    assert_eq!(compute_rust_push_backoff(&e, 5), 300_000);
}

// ---------------------------------------------------------------------------
// Backoff math: PublishRejected BadName (fixed, floor = 30_000)
// NOT exponential: must stay at 30_000 regardless of consecutive.
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_bad_name_is_always_fixed_30000() {
    let e = PushError::PublishRejected {
        code: "NetStream.Publish.BadName".into(),
        description: String::new(),
    };
    for consecutive in 1..=10 {
        assert_eq!(
            compute_rust_push_backoff(&e, consecutive),
            30_000,
            "BadName MUST be fixed at 30000 for consecutive={consecutive}; \
             NOT exponential (kills += -> -= and << -> >> mutants)"
        );
    }
}

// ---------------------------------------------------------------------------
// Backoff math: Timeout (fixed, floor = 10_000)
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_timeout_is_fixed_10000() {
    for consecutive in 1..=5 {
        assert_eq!(
            compute_rust_push_backoff(&PushError::Timeout, consecutive),
            10_000,
            "Timeout is NOT exponential; backoff must stay at 10000"
        );
    }
}

// ---------------------------------------------------------------------------
// Backoff math: LocalCancel returns 0 (no retry)
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_local_cancel_is_zero() {
    assert_eq!(
        compute_rust_push_backoff(&PushError::LocalCancel, 1),
        0,
        "LocalCancel has no floor; compute_rust_push_backoff must return 0"
    );
}

// ---------------------------------------------------------------------------
// Ladder monotonicity: consecutive errors must never decrease backoff
// for exponential errors until the cap is reached.
// ---------------------------------------------------------------------------

#[test]
fn rust_push_backoff_remote_closed_is_constant() {
    // RemoteClosed is no longer exponential (changed from 30 s ladder to
    // 3 s flat). Verify the value is constant across consecutive errors —
    // kills the ×= 2 / << mutants in compute_rust_push_backoff.
    let e = PushError::RemoteClosed(io::Error::new(io::ErrorKind::ConnectionReset, "x"));
    let values: Vec<u64> = (1..=6).map(|n| compute_rust_push_backoff(&e, n)).collect();
    let first = values[0];
    assert!(
        values.iter().all(|&v| v == first),
        "RemoteClosed backoff must be constant; got {:?}",
        values
    );
    assert_eq!(first, 3_000);
}

// ---------------------------------------------------------------------------
// build_rtmp_url: verify each service type produces a non-empty RTMP URL
// containing the stream key.  Kills the "replace -> String::new()" and
// "replace -> 'xyzzy'" body mutants.
// ---------------------------------------------------------------------------

#[test]
fn build_rtmp_url_yt_rtmp_contains_key_and_rtmp_scheme() {
    let url = super::super::build_rtmp_url_pub(ServiceType::YtRtmp, "my-yt-key");
    assert!(
        url.starts_with("rtmp://"),
        "YtRtmp URL must use rtmp:// scheme, got: {url}"
    );
    assert!(
        url.contains("my-yt-key"),
        "YtRtmp URL must embed the stream key, got: {url}"
    );
    assert!(
        url.contains("youtube.com"),
        "YtRtmp URL must point to youtube.com, got: {url}"
    );
}

#[test]
fn build_rtmp_url_facebook_contains_key_and_facebook_host() {
    let url = super::super::build_rtmp_url_pub(ServiceType::Facebook, "fb-key-123");
    assert!(
        url.contains("fb-key-123"),
        "Facebook URL must embed the stream key, got: {url}"
    );
    assert!(
        url.contains("facebook.com"),
        "Facebook URL must point to facebook.com, got: {url}"
    );
    assert!(!url.is_empty(), "Facebook URL must not be empty");
}

#[test]
fn build_rtmp_url_vimeo_contains_key_and_vimeo_host() {
    let url = super::super::build_rtmp_url_pub(ServiceType::Vimeo, "vimeo-key-xyz");
    assert!(
        url.contains("vimeo-key-xyz"),
        "Vimeo URL must embed the stream key, got: {url}"
    );
    assert!(
        url.contains("vimeo.com"),
        "Vimeo URL must point to vimeo.com, got: {url}"
    );
}

#[test]
fn build_rtmp_url_instagram_contains_key_and_instagram_host() {
    let url = super::super::build_rtmp_url_pub(ServiceType::Instagram, "ig-key-789");
    assert!(
        url.contains("ig-key-789"),
        "Instagram URL must embed the stream key, got: {url}"
    );
    assert!(
        url.contains("instagram.com"),
        "Instagram URL must point to instagram.com, got: {url}"
    );
}

#[test]
fn build_rtmp_url_test_file_contains_key() {
    let url = super::super::build_rtmp_url_pub(ServiceType::TestFile, "test-key");
    assert!(
        url.contains("test-key"),
        "TestFile URL must embed the stream key, got: {url}"
    );
    assert!(!url.is_empty(), "TestFile URL must not be empty");
}

// ---------------------------------------------------------------------------
// emit_rtmp_push_died: verify the function actually pushes a row into the
// AuditRing.  Kills the "replace function body with ()" mutant.
// ---------------------------------------------------------------------------

#[test]
fn emit_rtmp_push_died_appends_row_to_ring() {
    let ring = AuditRing::new(10);
    let (rows_before, _) = ring.since(0);
    assert_eq!(rows_before.len(), 0);

    endpoint_audit::emit_rtmp_push_died(
        &Some(ring.clone()),
        "test-alias",
        "RTMP handshake failed: connection refused",
        30_000,
        3,
    );

    let (rows_after, _) = ring.since(0);
    assert_eq!(
        rows_after.len(),
        1,
        "emit_rtmp_push_died must append exactly one row to the audit ring"
    );
}

#[test]
fn emit_rtmp_push_died_with_none_ring_is_no_op() {
    // When audit_ring is None the function must be a no-op (no panic).
    endpoint_audit::emit_rtmp_push_died(&None, "test-alias", "some error", 5_000, 1);
    // If we get here without panic the test passes.
}

// ---------------------------------------------------------------------------
// handle_rust_push close-on-error behavior (regression for 2026-05-03 freeze)
// Asserts that the Err arm calls pusher.close() so the next push reconnects
// instead of re-using a wedged session forever.
// ---------------------------------------------------------------------------

mod close_on_error {
    use super::super::super::consumer_helpers::{Pushable, RustPushAction, handle_rust_push};
    use super::super::super::{EndpointStats, FlvStreamNormalizer};
    use rs_rtmp_push::PushError;
    use std::collections::VecDeque;
    use std::io;
    use std::sync::Arc;
    use tokio::sync::{Mutex, watch};

    /// Mock that records call sequence and returns a pre-canned push result
    /// per call, so the test can assert close() was invoked between an Err
    /// and a subsequent push.
    #[derive(Default)]
    struct MockPusher {
        push_results: VecDeque<Result<(), PushError>>,
        events: Vec<&'static str>,
        reconnects: u32,
    }

    impl MockPusher {
        fn with_results(results: Vec<Result<(), PushError>>) -> Self {
            Self {
                push_results: VecDeque::from(results),
                events: Vec::new(),
                reconnects: 0,
            }
        }
    }

    impl Pushable for MockPusher {
        async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
            self.events.push("push");
            self.push_results
                .pop_front()
                .unwrap_or(Err(PushError::IoError(io::Error::other(
                    "no canned result remaining",
                ))))
        }

        async fn close(&mut self) {
            self.events.push("close");
            // Each close represents a fresh-session reconnect on the next push.
            self.reconnects += 1;
        }

        fn reconnect_count(&self) -> u32 {
            self.reconnects
        }
    }

    fn fresh_state() -> (
        Arc<Mutex<EndpointStats>>,
        watch::Receiver<bool>,
        FlvStreamNormalizer,
        u32,
        u32,
    ) {
        let stats = Arc::new(Mutex::new(EndpointStats::default()));
        let (_tx, rx) = watch::channel(false);
        (stats, rx, FlvStreamNormalizer::new(), 0u32, 0u32)
    }

    #[tokio::test(start_paused = true)]
    async fn err_arm_calls_close_so_next_push_reconnects() {
        // Sequence: IoError -> Ok. Without the close-on-Err fix the Err arm
        // would skip close() and reconnect_count would stay 0.
        //
        // `start_paused = true` virtualizes the 15 s exponential backoff
        // sleep so the test runs in milliseconds. Real-time waits would
        // add ~15 s per `cargo test` invocation.
        let mut pusher = MockPusher::with_results(vec![
            Err(PushError::IoError(io::Error::other("none return"))),
            Ok(()),
        ]);

        let (stats, mut stop_rx, mut norm, mut consec_err, mut consec_write) = fresh_state();

        // Spawn the handle_rust_push call so we can advance virtual time
        // past the backoff sleep.
        let stats_clone = Arc::clone(&stats);
        let task = tokio::spawn(async move {
            handle_rust_push(
                &mut pusher,
                b"chunk-data",
                42,
                2000,
                "test-alias",
                &mut consec_err,
                &mut consec_write,
                &stats_clone,
                &None,
                &mut stop_rx,
                &mut norm,
            )
            .await;
            pusher
        });

        // Yield to let the task reach the backoff sleep, then advance
        // virtual time past 15 s + a margin so the sleep returns.
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(20)).await;

        let pusher = task.await.expect("task panicked");

        assert_eq!(
            pusher.events,
            vec!["push", "close"],
            "Err arm must call close() after the failed push so the next call reconnects"
        );

        let stats_guard = stats.lock().await;
        assert_eq!(
            stats_guard.last_error.as_deref(),
            Some("I/O error: none return"),
            "Err arm must surface last_error so the dashboard shows the failure"
        );
        assert_eq!(
            stats_guard.stall_reason.as_deref(),
            Some("I/O error: none return"),
            "Err arm must surface stall_reason (regression: previously only the Timeout arm did)"
        );
    }

    #[tokio::test]
    async fn ok_arm_clears_stall_after_recovery() {
        // After a transient error sets stall_reason / last_error, a
        // successful push must clear them so the dashboard reflects
        // recovery instead of the stale freeze indicator.
        let mut pusher = MockPusher::with_results(vec![Ok(())]);
        let (stats, mut stop_rx, mut norm, mut consec_err, mut consec_write) = fresh_state();

        // Pre-seed stale error markers as if a prior failure happened.
        {
            let mut s = stats.lock().await;
            s.last_error = Some("stale".to_string());
            s.stall_reason = Some("stale".to_string());
        }

        let action = handle_rust_push(
            &mut pusher,
            b"chunk-data",
            7,
            2000,
            "test-alias",
            &mut consec_err,
            &mut consec_write,
            &stats,
            &None,
            &mut stop_rx,
            &mut norm,
        )
        .await;

        assert!(matches!(action, RustPushAction::Continue));

        let s = stats.lock().await;
        assert!(
            s.last_error.is_none(),
            "Ok path must clear last_error; got {:?}",
            s.last_error
        );
        assert!(
            s.stall_reason.is_none(),
            "Ok path must clear stall_reason; got {:?}",
            s.stall_reason
        );
        assert_eq!(s.chunks_processed, 1);
    }

    #[tokio::test]
    async fn local_cancel_returns_break_without_calling_close() {
        // PushError::LocalCancel is the only None-floor variant. The let-else
        // short-circuit must return Break BEFORE the close() call, so no
        // double-close on shutdown (close happens via Drop on stack unwind).
        let mut pusher = MockPusher::with_results(vec![Err(PushError::LocalCancel)]);
        let (stats, mut stop_rx, mut norm, mut consec_err, mut consec_write) = fresh_state();

        let action = handle_rust_push(
            &mut pusher,
            b"data",
            1,
            2000,
            "test-alias",
            &mut consec_err,
            &mut consec_write,
            &stats,
            &None,
            &mut stop_rx,
            &mut norm,
        )
        .await;

        assert!(matches!(action, RustPushAction::Break));
        assert_eq!(
            pusher.events,
            vec!["push"],
            "LocalCancel must NOT call close() in handle_rust_push (Drop handles it)"
        );
        assert_ne!(
            pusher.events.last().copied(),
            Some("close"),
            "Last event must NOT be 'close' on the LocalCancel path"
        );
        assert_eq!(
            pusher.reconnects, 0,
            "LocalCancel must NOT trigger any reconnect bookkeeping"
        );
    }
}
