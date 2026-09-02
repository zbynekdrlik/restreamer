//! #236: dead-target classifier tests, split out of
//! `endpoint_task_rust_push_tests.rs` to keep that file under the
//! 1000-line CI cap (rust-crate-hygiene.md). Included via `#[path]` as
//! `mod dead_target_classifier` inside `endpoint_task_rust_push_tests.rs`
//! -- an ordinary CHILD module, so `super::` paths below resolve
//! exactly as they did when this content lived inline (one level of
//! module nesting either way).

use super::super::super::EndpointStats;
use super::super::super::consumer_helpers::{Pushable, RustPushAction, handle_rust_push};
use crate::audit_ring::AuditRing;
use rs_core::audit::{Action, Severity};
use rs_rtmp_push::PushError;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

/// Mock pusher that plays back a queued sequence of `push_flv_bytes`
/// results, defaulting to a `RemoteClosed` (the confirmed #236
/// death-loop signature -- FB closing at/just after the RTMP handshake
/// before any FLV bytes go out) once the queue is exhausted.
///
/// `hang_on_call` (1-indexed): when set, that specific call NEVER
/// resolves `push_flv_bytes` -- it awaits `std::future::pending()`
/// forever, so the surrounding `tokio::time::timeout` in
/// `handle_rust_push` is the thing that actually completes (driving
/// the `Err(_timeout)` arm, not `Ok(Err(push_err))`). Requires
/// `#[tokio::test(start_paused = true)]`: with no runnable task and
/// the internal 30s timeout `Sleep` still registered, tokio's paused
/// clock auto-advances straight to it.
#[derive(Default)]
struct SequencedPusher {
    results: VecDeque<Result<(), PushError>>,
    reconnects: u32,
    calls: u32,
    hang_on_call: Option<u32>,
}

impl SequencedPusher {
    fn with_results(results: Vec<Result<(), PushError>>) -> Self {
        Self {
            results: VecDeque::from(results),
            reconnects: 0,
            calls: 0,
            hang_on_call: None,
        }
    }

    fn with_results_hanging_on_call(
        results: Vec<Result<(), PushError>>,
        hang_on_call: u32,
    ) -> Self {
        Self {
            results: VecDeque::from(results),
            reconnects: 0,
            calls: 0,
            hang_on_call: Some(hang_on_call),
        }
    }
}

fn remote_closed() -> PushError {
    PushError::RemoteClosed(io::Error::new(io::ErrorKind::UnexpectedEof, "x"))
}

fn handshake_failed() -> PushError {
    PushError::HandshakeFailed(io::Error::new(io::ErrorKind::ConnectionRefused, "no route"))
}

fn publish_rejected_bad_name() -> PushError {
    PushError::PublishRejected {
        code: "NetStream.Publish.BadName".to_string(),
        description: "stream key not found".to_string(),
    }
}

impl Pushable for SequencedPusher {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        self.calls += 1;
        if self.hang_on_call == Some(self.calls) {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves");
        }
        self.results
            .pop_front()
            .unwrap_or_else(|| Err(remote_closed()))
    }

    async fn close(&mut self) {
        self.reconnects += 1;
    }

    fn reconnect_count(&self) -> u32 {
        self.reconnects
    }

    fn av_skew_ms(&self) -> i64 {
        0
    }
}

/// Fixture bundle mirroring `close_on_error::fresh_state` but scoped to
/// this module (kept separate rather than reaching into the sibling
/// module -- `fresh_state` there is private to `close_on_error`).
fn fresh_state() -> (
    Arc<Mutex<EndpointStats>>,
    watch::Receiver<bool>,
    u32,
    u32,
    u32,
) {
    let stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_tx, rx) = watch::channel(false);
    (stats, rx, 0u32, 0u32, 0u32)
}

#[allow(clippy::too_many_arguments)]
async fn push_once(
    pusher: &mut SequencedPusher,
    stats: &Arc<Mutex<EndpointStats>>,
    audit_ring: &Option<Arc<AuditRing>>,
    stop_rx: &mut watch::Receiver<bool>,
    consec_err: &mut u32,
    consec_write: &mut u32,
    consec_zero_byte: &mut u32,
    tel: &mut crate::rtmp_push_telemetry::RtmpPushTelemetry,
    service_type: &str,
) -> RustPushAction {
    handle_rust_push(
        pusher,
        b"chunk-data",
        1,
        2000,
        "test-alias",
        service_type,
        consec_err,
        consec_write,
        consec_zero_byte,
        stats,
        audit_ring,
        tel,
        stop_rx,
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn below_threshold_zero_byte_deaths_do_not_classify_dead_target() {
    // 4 consecutive zero-byte deaths -- one short of
    // DEAD_TARGET_ZERO_BYTE_THRESHOLD (5). Must stay on the ordinary
    // RemoteClosed fast-recovery path: backoff stays at its 3s floor
    // and last_error/stall_reason carry the RAW error text, not the
    // DEAD_TARGET operator remedy.
    let mut pusher = SequencedPusher::default();
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..4 {
        let action = push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
        assert!(matches!(action, RustPushAction::Continue));
    }

    assert_eq!(consec_zero_byte, 4);
    let s = stats.lock().await;
    assert_eq!(
        s.last_error.as_deref(),
        Some(remote_closed().to_string()).as_deref(),
        "below threshold: last_error must stay the raw error text, no DEAD_TARGET remedy yet"
    );
    assert_eq!(
        s.rtmp_push_history.back().map(|r| r.backoff_ms),
        Some(3_000),
        "below threshold: backoff must stay at RemoteClosed's own 3s floor, not the 30s dead-target override"
    );
}

#[tokio::test(start_paused = true)]
async fn five_consecutive_zero_byte_deaths_classify_fb_dead_target_and_force_30s_backoff() {
    let ring = AuditRing::new(64);
    let audit_ring = Some(Arc::clone(&ring));
    let mut pusher = SequencedPusher::default();
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for i in 1..=5 {
        let action = push_once(
            &mut pusher,
            &stats,
            &audit_ring,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
        assert!(matches!(action, RustPushAction::Continue), "call {i}");
    }

    assert_eq!(consec_zero_byte, 5);
    let s = stats.lock().await;
    let expected = format!(
        "DEAD_TARGET: FB broadcast expired/killed -- recreate the live broadcast on Facebook (stream key stays the same) (last error: {})",
        remote_closed()
    );
    assert_eq!(
        s.last_error.as_deref(),
        Some(expected.as_str()),
        "5th consecutive zero-byte death must surface the FB dead-target remedy WITH the raw error preserved"
    );
    assert_eq!(
        s.stall_reason, s.last_error,
        "stall_reason must carry the same dead-target message (EndpointLifecycle::compute reads stall_reason)"
    );
    assert_eq!(
        s.rtmp_push_history.back().map(|r| r.backoff_ms),
        Some(30_000),
        "at the threshold, backoff must be forced to the 30s dead-target floor (kills a mutant deleting .max(DEAD_TARGET_BACKOFF_MS))"
    );

    // Exactly ONE audit row -- emitted at the threshold TRANSITION,
    // never spammed on every retry (kills a mutant flipping `==` to
    // `>=` in `just_became_dead_target`).
    let (rows, _) = ring.since(0i64);
    let dead_target_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::EndpointDeadTarget)
        .collect();
    assert_eq!(
        dead_target_rows.len(),
        1,
        "exactly one EndpointDeadTarget audit row at the threshold crossing"
    );
    assert_eq!(dead_target_rows[0].severity, Severity::Error);
}

#[tokio::test(start_paused = true)]
async fn dead_target_audit_row_never_repeats_while_still_classified() {
    // 5 deaths cross the threshold (1 row); 3 MORE consecutive
    // zero-byte deaths keep the endpoint classified dead-target but
    // must NOT emit a second row.
    let ring = AuditRing::new(64);
    let audit_ring = Some(Arc::clone(&ring));
    let mut pusher = SequencedPusher::default();
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..8 {
        push_once(
            &mut pusher,
            &stats,
            &audit_ring,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }
    assert_eq!(consec_zero_byte, 8);

    let (rows, _) = ring.since(0i64);
    let dead_target_rows = rows
        .iter()
        .filter(|r| r.action == Action::EndpointDeadTarget)
        .count();
    assert_eq!(
        dead_target_rows, 1,
        "still classified dead-target after 8 deaths -- must stay at exactly one emitted row, not one per retry"
    );
}

#[tokio::test(start_paused = true)]
async fn re_arm_after_recovery_emits_a_second_audit_row() {
    // 5 deaths -> dead-target (row 1). A success resets the counter.
    // 5 MORE deaths -> dead-target AGAIN (row 2) -- the classifier
    // must re-arm, not treat "already alerted once" as permanent.
    let ring = AuditRing::new(64);
    let audit_ring = Some(Arc::clone(&ring));
    let mut pusher = SequencedPusher::with_results(vec![
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Ok(()),
    ]);
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..6 {
        push_once(
            &mut pusher,
            &stats,
            &audit_ring,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }
    assert_eq!(consec_zero_byte, 0, "the success must reset the counter");

    // 6 calls, not 5: the FIRST error right after a success still "sees"
    // that success's >0 bytes_sent (telemetry only resets on error
    // handling, not on success -- see
    // `a_successful_push_between_deaths_prevents_dead_target_classification`),
    // so it does not itself count. Only the trailing 5 are genuine
    // zero-byte deaths.
    for _ in 0..6 {
        push_once(
            &mut pusher,
            &stats,
            &audit_ring,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }
    assert_eq!(consec_zero_byte, 5);

    let (rows, _) = ring.since(0i64);
    let dead_target_rows = rows
        .iter()
        .filter(|r| r.action == Action::EndpointDeadTarget)
        .count();
    assert_eq!(
        dead_target_rows, 2,
        "a second, independent death streak after recovery must emit its OWN audit row (re-arm)"
    );
}

#[tokio::test(start_paused = true)]
async fn five_consecutive_zero_byte_deaths_on_a_non_fb_service_get_generic_message() {
    let mut pusher = SequencedPusher::default();
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..5 {
        push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "YT_RTMP",
        )
        .await;
    }

    let s = stats.lock().await;
    let msg = s.last_error.clone().expect("last_error must be set");
    assert!(
        msg.starts_with("DEAD_TARGET: "),
        "must still classify dead-target for a non-FB service: {msg}"
    );
    assert!(
        msg.contains("YT_RTMP"),
        "generic message must name the service type: {msg}"
    );
    assert!(
        !msg.contains("Facebook"),
        "must NOT use the FB-specific remedy for a non-FB service: {msg}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_successful_push_between_deaths_prevents_dead_target_classification() {
    // 4 zero-byte deaths, then ONE connect that sends real media
    // (bytes flowed -- a transient recovery), then 5 MORE errors.
    // Never 5 CONSECUTIVE GENUINE zero-byte deaths, so this must NEVER
    // classify dead-target -- proves the counter does not carry across
    // a successful push (#236 acceptance: "a subsequent connect that
    // sends >0 bytes clears the marker"). Note the FIRST error right
    // after the success (call 6) does not itself count as zero-byte:
    // `telemetry` is only reset on error handling, not on success, so
    // it still carries the success's >0 bytes_sent at that point --
    // exactly mirroring production (a session that sent real media
    // before dying is a transient outage, not a dead target). Only
    // calls 7-10 (after call 6's error freshly reset telemetry) are
    // genuine zero-byte deaths, so the counter tops out at 4.
    let mut pusher = SequencedPusher::with_results(vec![
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Ok(()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
    ]);
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for i in 1..=10 {
        let action = push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
        assert!(matches!(action, RustPushAction::Continue), "call {i}");
    }

    assert_eq!(
        consec_zero_byte, 4,
        "only calls 7-10 (genuinely zero-byte after the success + its absorbing next error) should count"
    );
    let s = stats.lock().await;
    assert!(
        !s.last_error
            .as_deref()
            .unwrap_or_default()
            .starts_with("DEAD_TARGET: "),
        "must never classify dead-target when a success interrupts the death streak: {:?}",
        s.last_error
    );
}

// --- review finding (🔴): the classifier must be scoped to RemoteClosed
// only. Every OTHER connect-time failure (network down, bad key, ...)
// has bytes_sent()==0 too, but is NOT the #236 dead-target signature
// and must never be relabeled with the FB-broadcast remedy. ---

#[tokio::test(start_paused = true)]
async fn five_consecutive_handshake_failures_never_classify_dead_target() {
    // HandshakeFailed = DNS/TCP/TLS connect failure (network down, box
    // unreachable). Also 0 bytes sent, but the correct operator action
    // is "check the network", never "recreate the FB broadcast".
    let mut pusher =
        SequencedPusher::with_results((0..8).map(|_| Err(handshake_failed())).collect());
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..8 {
        push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }

    assert_eq!(
        consec_zero_byte, 0,
        "HandshakeFailed must NEVER increment the RemoteClosed-scoped dead-target counter"
    );
    let s = stats.lock().await;
    assert!(
        !s.last_error
            .as_deref()
            .unwrap_or_default()
            .starts_with("DEAD_TARGET: "),
        "8 consecutive HandshakeFailed must never surface the FB dead-target remedy: {:?}",
        s.last_error
    );
}

#[tokio::test(start_paused = true)]
async fn five_consecutive_bad_stream_key_rejects_keep_their_own_actionable_message() {
    // PublishRejected BadName = wrong stream key. Already actionable
    // via `last_error_is_actionable`'s "badname"/"rejected" match in
    // rs-core -- must NEVER be overwritten by the dead-target remedy
    // (the old bug: after 5 rejects the correct "wrong key" text was
    // replaced with "the key stays the same", actively wrong advice).
    let mut pusher =
        SequencedPusher::with_results((0..8).map(|_| Err(publish_rejected_bad_name())).collect());
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..8 {
        push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }

    assert_eq!(consec_zero_byte, 0);
    let s = stats.lock().await;
    let msg = s.last_error.clone().unwrap_or_default();
    assert!(
        msg.starts_with("NetStream.Publish rejected"),
        "8 consecutive BadName rejects must keep the original actionable message, never DEAD_TARGET: {msg}"
    );
    assert!(
        !msg.starts_with("DEAD_TARGET: "),
        "must never relabel a bad-key reject as a dead broadcast: {msg}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_non_remote_closed_error_between_remote_closed_deaths_resets_the_counter() {
    // Any error class OTHER than a zero-byte RemoteClosed resets the
    // streak -- 4 RemoteClosed deaths, then ONE HandshakeFailed
    // (itself proven never to increment the counter, see the test
    // above), then 4 MORE RemoteClosed deaths: never 5 consecutive
    // genuine RemoteClosed deaths in a row, so this must stay below
    // threshold.
    let mut pusher = SequencedPusher::with_results(vec![
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(handshake_failed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
        Err(remote_closed()),
    ]);
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    for _ in 0..9 {
        push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }

    assert_eq!(
        consec_zero_byte, 4,
        "the interleaved HandshakeFailed must reset the streak so only the trailing 4 RemoteClosed deaths count"
    );
    let s = stats.lock().await;
    assert!(
        !s.last_error
            .as_deref()
            .unwrap_or_default()
            .starts_with("DEAD_TARGET: "),
        "9 calls with an interleaved non-RemoteClosed error must never reach the 5-threshold: {:?}",
        s.last_error
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_timeout_between_remote_closed_deaths_resets_the_counter() {
    // review finding (🔵): a write TIMEOUT means the peer held the TCP
    // connection open for the full WRITE_TIMEOUT_SECS instead of
    // closing it immediately -- the opposite of the dead-target
    // signature. It must reset the streak. Drives the REAL
    // `Err(_timeout)` arm (not `Ok(Err(push_err))`) by hanging
    // `push_flv_bytes` on call 5 -- under `start_paused = true`,
    // tokio auto-advances straight to the internal 30s timeout Sleep
    // once nothing else is runnable.
    let mut pusher = SequencedPusher::with_results_hanging_on_call(
        vec![
            Err(remote_closed()),
            Err(remote_closed()),
            Err(remote_closed()),
            Err(remote_closed()),
        ],
        5,
    );
    let (stats, mut stop_rx, mut consec_err, mut consec_write, mut consec_zero_byte) =
        fresh_state();
    let mut tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();

    // 4 genuine RemoteClosed zero-byte deaths.
    for _ in 0..4 {
        push_once(
            &mut pusher,
            &stats,
            &None,
            &mut stop_rx,
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero_byte,
            &mut tel,
            "FB",
        )
        .await;
    }
    assert_eq!(consec_zero_byte, 4);

    // Call 5: hangs -> the outer WRITE_TIMEOUT_SECS timeout fires ->
    // Err(_timeout) arm -> must reset the counter to 0.
    push_once(
        &mut pusher,
        &stats,
        &None,
        &mut stop_rx,
        &mut consec_err,
        &mut consec_write,
        &mut consec_zero_byte,
        &mut tel,
        "FB",
    )
    .await;

    assert_eq!(
        consec_zero_byte, 0,
        "a write timeout must reset the zero-byte-death streak to 0, never silently preserve it"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.last_error.as_deref(),
        Some("rtmp_push_timeout"),
        "the Timeout arm's own message must surface unchanged, never DEAD_TARGET"
    );
}
