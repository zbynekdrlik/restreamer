//! #368: the consumer's reconnect delay after a push error. The first
//! upstream close after a healthy minute reconnects at once (it was a fixed
//! 3 s floor: the bulk of the 2026-10-15 ~3.2 s YouTube gap), and a
//! rejection right after such a fast retry gets one short retry instead of
//! the 30 s rejection floor.

use super::super::consumer_helpers::{Pushable, handle_rust_push};
use super::super::{EndpointStats, FlvStreamNormalizer};
use crate::rtmp_push_telemetry::RtmpPushTelemetry;
use rs_rtmp_push::PushError;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

struct Canned(VecDeque<Result<(), PushError>>);

/// A pusher whose session saw an upstream Acknowledgement `ack_age` ago.
struct Acked {
    results: VecDeque<Result<(), PushError>>,
    ack_at: std::time::Instant,
}

impl Pushable for Acked {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        self.results.pop_front().unwrap_or(Ok(()))
    }

    async fn close(&mut self) {}

    fn reconnect_count(&self) -> u32 {
        0
    }

    fn av_skew_ms(&self) -> i64 {
        0
    }

    async fn finish(&mut self) {
        self.close().await
    }

    fn slow_pacing(&mut self, _by: std::time::Duration) {}

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }

    fn last_upstream_ack_at(&self) -> Option<std::time::Instant> {
        Some(self.ack_at)
    }
}

impl Pushable for Canned {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        self.0
            .pop_front()
            .unwrap_or_else(|| Err(PushError::IoError(io::Error::other("no canned result"))))
    }

    async fn close(&mut self) {}

    fn reconnect_count(&self) -> u32 {
        0
    }

    fn av_skew_ms(&self) -> i64 {
        0
    }

    async fn finish(&mut self) {
        self.close().await
    }

    fn slow_pacing(&mut self, _by: std::time::Duration) {}

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

fn broken_pipe() -> PushError {
    PushError::RemoteClosed(io::Error::from(io::ErrorKind::BrokenPipe))
}

fn bad_name() -> PushError {
    PushError::PublishRejected {
        code: "NetStream.Publish.BadName".into(),
        description: "busy".into(),
    }
}

/// Push one OK chunk, let the session run `healthy_ms`, then play `errors`
/// one push each. Returns the backoff the consumer recorded per error.
async fn backoffs_after_a_healthy_session(healthy_ms: u64, errors: Vec<PushError>) -> Vec<u64> {
    let n = errors.len();
    let mut results: VecDeque<Result<(), PushError>> = VecDeque::from([Ok(())]);
    results.extend(errors.into_iter().map(Err));
    let mut pusher = Canned(results);
    let stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut norm = FlvStreamNormalizer::new();
    let (mut consec_err, mut consec_write, mut consec_zero) = (0u32, 0u32, 0u32);
    let mut tel = RtmpPushTelemetry::new_for_test_at(0);
    for i in 0..=n {
        if i == 1 {
            tel.advance_clock_for_test(Duration::from_millis(healthy_ms));
        }
        handle_rust_push(
            &mut pusher,
            b"chunk",
            i as i64,
            2000,
            "YT NLCH 4K",
            "YT_RTMP",
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero,
            &stats,
            &None,
            &mut tel,
            &mut stop_rx,
            &mut norm,
        )
        .await;
    }
    let s = stats.lock().await;
    s.rtmp_push_history.iter().map(|r| r.backoff_ms).collect()
}

#[tokio::test(start_paused = true)]
async fn a_close_after_a_healthy_minute_reconnects_without_delay() {
    assert_eq!(
        backoffs_after_a_healthy_session(60_000, vec![broken_pipe()]).await,
        vec![0]
    );
}

#[tokio::test(start_paused = true)]
async fn a_close_inside_the_first_minute_keeps_the_3s_floor() {
    assert_eq!(
        backoffs_after_a_healthy_session(59_999, vec![broken_pipe()]).await,
        vec![3_000]
    );
}

#[tokio::test(start_paused = true)]
async fn a_rejection_after_the_fast_retry_is_retried_shortly_then_the_floor_returns() {
    assert_eq!(
        backoffs_after_a_healthy_session(2_629_000, vec![broken_pipe(), bad_name(), bad_name()])
            .await,
        vec![0, 3_000, 30_000],
        "fast reconnect, one short retry of the rejection, then BadName's own 30 s"
    );
}

#[tokio::test(start_paused = true)]
async fn a_second_close_in_a_row_is_not_fast() {
    assert_eq!(
        backoffs_after_a_healthy_session(600_000, vec![broken_pipe(), broken_pipe()]).await,
        vec![0, 3_000]
    );
}

/// #368: the death row's `time_since_last_upstream_ack_ms` comes from the
/// pusher's real upstream Acknowledgements; before #368 nothing fed it, so
/// every row (2000 of them on stream.lan) said null.
#[tokio::test(start_paused = true)]
async fn the_death_row_carries_the_age_of_the_last_upstream_ack() {
    use rs_core::audit::Action;
    let ring = crate::audit_ring::AuditRing::new(16);
    let mut pusher = Acked {
        results: VecDeque::from([Err(broken_pipe())]),
        ack_at: std::time::Instant::now() - Duration::from_millis(1_500),
    };
    let stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut norm = FlvStreamNormalizer::new();
    let (mut consec_err, mut consec_write, mut consec_zero) = (0u32, 0u32, 0u32);
    let mut tel = RtmpPushTelemetry::new();
    handle_rust_push(
        &mut pusher,
        b"chunk",
        1,
        2000,
        "YT NLCH 4K",
        "YT_RTMP",
        &mut consec_err,
        &mut consec_write,
        &mut consec_zero,
        &stats,
        &Some(Arc::clone(&ring)),
        &mut tel,
        &mut stop_rx,
        &mut norm,
    )
    .await;
    let (rows, _) = ring.since(0);
    let died = rows
        .iter()
        .find(|r| r.action == Action::EndpointRtmpPushDied)
        .expect("a push death row");
    let age = died.detail["time_since_last_upstream_ack_ms"]
        .as_u64()
        .expect("the ack age must be measured, not null");
    assert!(
        (1_500..60_000).contains(&age),
        "age of the last ack: {age} ms"
    );
}

#[tokio::test(start_paused = true)]
async fn after_an_outage_rescue_a_quick_close_of_the_new_session_is_not_a_fast_reconnect() {
    use super::super::consumer_helpers::reset_after_rescue;
    // An hour-long healthy session; then an outage rescue rebuilt the pusher
    // and its new session is closed right away.
    let mut pusher = Canned(VecDeque::from([Ok(()), Err(broken_pipe())]));
    let stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut norm = FlvStreamNormalizer::new();
    let (mut consec_err, mut consec_write, mut consec_zero) = (0u32, 0u32, 0u32);
    let mut tel = RtmpPushTelemetry::new_for_test_at(0);
    let mut last_chunk_bytes = Some(Arc::new(b"freeze".to_vec()));
    for i in 0..2 {
        if i == 1 {
            tel.advance_clock_for_test(Duration::from_secs(3_600));
            reset_after_rescue(&mut norm, &mut last_chunk_bytes, &mut tel);
        }
        handle_rust_push(
            &mut pusher,
            b"chunk",
            i,
            2000,
            "YT NLCH 4K",
            "YT_RTMP",
            &mut consec_err,
            &mut consec_write,
            &mut consec_zero,
            &stats,
            &None,
            &mut tel,
            &mut stop_rx,
            &mut norm,
        )
        .await;
    }

    assert!(
        last_chunk_bytes.is_none(),
        "no stale freeze frame after a rescue"
    );
    let s = stats.lock().await;
    let backoffs: Vec<u64> = s.rtmp_push_history.iter().map(|r| r.backoff_ms).collect();
    assert_eq!(
        backoffs,
        vec![3_000],
        "the new session was never healthy: the 3 s floor, not a 0 ms reconnect"
    );
}
