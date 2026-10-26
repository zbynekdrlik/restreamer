//! #368: the consumer re-sends a chunk whose push failed, ONCE, on the fresh
//! session. Before #368 `handle_rust_push` closed the session, slept the
//! backoff and moved on to the NEXT chunk, so the ~2 s of the failed chunk
//! never reached the platform (the 2026-10-15 YT NLCH 4K stall + jump).
//!
//! The mock records every push with its bytes and tells a first push from a
//! re-send (`repush_flv_bytes`), so a test sees exactly what went where.

use super::super::consumer_helpers::{Pushable, RustPushAction, push_chunk};
use super::super::{EndpointStats, FlvStreamNormalizer};
use rs_rtmp_push::PushError;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Push(Vec<u8>),
    Repush(Vec<u8>),
    Close,
}

/// Plays back one canned result per push (first push or re-send alike);
/// `hang_on_call` (1-indexed) never resolves that push, so the consumer's
/// 30 s write timeout fires (paused clock).
#[derive(Default)]
struct ResendMock {
    results: VecDeque<Result<(), PushError>>,
    calls: Vec<Call>,
    pushes: u32,
    hang_on_call: Option<u32>,
}

impl ResendMock {
    fn with_results(results: Vec<Result<(), PushError>>) -> Self {
        Self {
            results: VecDeque::from(results),
            ..Self::default()
        }
    }

    async fn next_result(&mut self) -> Result<(), PushError> {
        self.pushes += 1;
        if self.hang_on_call == Some(self.pushes) {
            std::future::pending::<()>().await;
        }
        self.results
            .pop_front()
            .unwrap_or_else(|| Err(PushError::IoError(io::Error::other("no canned result"))))
    }
}

impl Pushable for ResendMock {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.calls.push(Call::Push(data.to_vec()));
        self.next_result().await
    }

    async fn repush_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.calls.push(Call::Repush(data.to_vec()));
        self.next_result().await
    }

    async fn close(&mut self) {
        self.calls.push(Call::Close);
    }

    fn reconnect_count(&self) -> u32 {
        0
    }

    fn av_skew_ms(&self) -> i64 {
        0
    }

    async fn finish(&mut self) {
        self.close().await
    }

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

/// Per-consumer state `push_chunk` threads through, as `consumer_task` holds it.
struct Consumer {
    stats: Arc<Mutex<EndpointStats>>,
    stop_tx: watch::Sender<bool>,
    stop_rx: watch::Receiver<bool>,
    norm: FlvStreamNormalizer,
    consec_err: u32,
    consec_write: u32,
    consec_zero: u32,
    tel: crate::rtmp_push_telemetry::RtmpPushTelemetry,
}

impl Consumer {
    fn new() -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        Self {
            stats: Arc::new(Mutex::new(EndpointStats::default())),
            stop_tx,
            stop_rx,
            norm: FlvStreamNormalizer::new(),
            consec_err: 0,
            consec_write: 0,
            consec_zero: 0,
            tel: crate::rtmp_push_telemetry::RtmpPushTelemetry::new(),
        }
    }

    async fn push(
        &mut self,
        pusher: &mut ResendMock,
        data: &[u8],
        chunk_id: i64,
    ) -> RustPushAction {
        push_chunk(
            pusher,
            data,
            chunk_id,
            2000,
            "YT NLCH 4K",
            "YT_RTMP",
            &mut self.consec_err,
            &mut self.consec_write,
            &mut self.consec_zero,
            &self.stats,
            &None,
            &mut self.tel,
            &mut self.stop_rx,
            &mut self.norm,
        )
        .await
    }
}

fn broken_pipe() -> PushError {
    PushError::RemoteClosed(io::Error::from(io::ErrorKind::BrokenPipe))
}

/// Aborts the test process when a test runs 30 s of REAL time: a mutant
/// that spins stalls the paused clock (mutation-killable-code.md). Dropping
/// it disarms it.
struct Watchdog(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

fn watchdog() -> Watchdog {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if rx.recv_timeout(std::time::Duration::from_secs(30))
            == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            eprintln!("resend test still running after 30 s of real time -- aborting");
            std::process::abort();
        }
    });
    Watchdog(tx)
}

#[tokio::test(start_paused = true)]
async fn a_chunk_whose_push_failed_is_pushed_again_on_the_fresh_session() {
    let _wd = watchdog();
    let mut pusher = ResendMock::with_results(vec![Err(broken_pipe()), Ok(())]);
    let mut c = Consumer::new();

    let action = c.push(&mut pusher, b"chunk-41", 41).await;

    assert!(matches!(action, RustPushAction::Continue));
    assert_eq!(
        pusher.calls,
        vec![
            Call::Push(b"chunk-41".to_vec()),
            Call::Close,
            Call::Repush(b"chunk-41".to_vec()),
        ],
        "the failed chunk must be re-sent (same bytes) on the fresh session"
    );
    let s = c.stats.lock().await;
    assert_eq!(s.chunks_processed, 1, "the re-send delivered the chunk");
    assert_eq!(s.current_chunk_id, 41);
}

#[tokio::test(start_paused = true)]
async fn a_chunk_is_resent_at_most_once_then_the_next_chunk_goes_out() {
    let _wd = watchdog();
    let mut pusher = ResendMock::with_results(vec![Err(broken_pipe()), Err(broken_pipe()), Ok(())]);
    let mut c = Consumer::new();

    let first = c.push(&mut pusher, b"chunk-41", 41).await;
    let second = c.push(&mut pusher, b"chunk-42", 42).await;

    assert!(matches!(first, RustPushAction::Continue));
    assert!(matches!(second, RustPushAction::Continue));
    assert_eq!(
        pusher.calls,
        vec![
            Call::Push(b"chunk-41".to_vec()),
            Call::Close,
            Call::Repush(b"chunk-41".to_vec()),
            Call::Close,
            Call::Push(b"chunk-42".to_vec()),
        ],
        "a second failure of the same chunk moves on: no third try, and the \
         next chunk is a normal first push"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_timeout_resends_the_chunk() {
    let _wd = watchdog();
    let mut pusher = ResendMock::with_results(vec![Ok(())]);
    pusher.hang_on_call = Some(1);
    let mut c = Consumer::new();

    let action = c.push(&mut pusher, b"chunk-7", 7).await;

    assert!(matches!(action, RustPushAction::Continue));
    assert_eq!(
        pusher.calls,
        vec![
            Call::Push(b"chunk-7".to_vec()),
            Call::Close,
            Call::Repush(b"chunk-7".to_vec()),
        ],
        "a hung write may have lost the chunk too: re-send it"
    );
}

#[tokio::test(start_paused = true)]
async fn a_fully_sent_or_bad_chunk_is_not_resent() {
    let _wd = watchdog();
    // AvSkewExceeded is raised after the WHOLE chunk went out; a malformed
    // chunk would only fail again.
    for err in [
        PushError::AvSkewExceeded { skew_ms: 4_500 },
        PushError::MalformedInput {
            offset: 0,
            reason: "x".into(),
        },
    ] {
        let label = format!("{err:?}");
        let mut pusher = ResendMock::with_results(vec![Err(err), Ok(())]);
        let mut c = Consumer::new();
        c.push(&mut pusher, b"chunk-1", 1).await;
        c.push(&mut pusher, b"chunk-2", 2).await;
        assert_eq!(
            pusher.calls,
            vec![
                Call::Push(b"chunk-1".to_vec()),
                Call::Close,
                Call::Push(b"chunk-2".to_vec()),
            ],
            "{label}: no re-send"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_successful_push_is_never_repeated() {
    let _wd = watchdog();
    let mut pusher = ResendMock::with_results(vec![Ok(()), Ok(())]);
    let mut c = Consumer::new();
    c.push(&mut pusher, b"chunk-1", 1).await;
    c.push(&mut pusher, b"chunk-2", 2).await;
    assert_eq!(
        pusher.calls,
        vec![
            Call::Push(b"chunk-1".to_vec()),
            Call::Push(b"chunk-2".to_vec()),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_the_backoff_skips_the_resend() {
    let _wd = watchdog();
    let mut pusher = ResendMock::with_results(vec![Err(broken_pipe()), Ok(())]);
    let mut c = Consumer::new();
    // An unseen stop resolves `stop_rx.changed()` in the backoff select.
    c.stop_tx.send(true).expect("stop");

    let action = c.push(&mut pusher, b"chunk-9", 9).await;

    assert!(matches!(action, RustPushAction::Break));
    assert_eq!(
        pusher.calls,
        vec![Call::Push(b"chunk-9".to_vec()), Call::Close],
        "a stopping endpoint must not re-send"
    );
}

/// #368 review: YouTube may still hold the old session for a moment.
fn still_held() -> PushError {
    PushError::PublishRejected {
        code: "NetStream.Publish.BadName".into(),
        description: "busy".into(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_resend_rejected_right_after_a_fast_reconnect_gets_one_more_try() {
    let _wd = watchdog();
    // An hour into a healthy session YouTube closes it mid chunk 42: the
    // 0 ms reconnect's re-send is rejected because YouTube still holds the
    // old session. One more re-send after the short retry delivers chunk 42
    // instead of dropping it.
    let mut pusher =
        ResendMock::with_results(vec![Ok(()), Err(broken_pipe()), Err(still_held()), Ok(())]);
    let mut c = Consumer::new();
    c.tel = crate::rtmp_push_telemetry::RtmpPushTelemetry::new_for_test_at(0);
    assert!(matches!(
        c.push(&mut pusher, b"chunk-41", 41).await,
        RustPushAction::Continue
    ));
    c.tel
        .advance_clock_for_test(std::time::Duration::from_secs(3_600));

    let action = c.push(&mut pusher, b"chunk-42", 42).await;

    assert!(matches!(action, RustPushAction::Continue));
    assert_eq!(
        pusher.calls,
        vec![
            Call::Push(b"chunk-41".to_vec()),
            Call::Push(b"chunk-42".to_vec()),
            Call::Close,
            Call::Repush(b"chunk-42".to_vec()),
            Call::Close,
            Call::Repush(b"chunk-42".to_vec()),
        ],
        "a re-send rejected on the fast reconnect gets one more try"
    );
    let s = c.stats.lock().await;
    let backoffs: Vec<u64> = s.rtmp_push_history.iter().map(|r| r.backoff_ms).collect();
    assert_eq!(
        backoffs,
        vec![0, 3_000],
        "the fast retry, then the short one"
    );
    assert_eq!(s.chunks_processed, 2, "chunk 42 was delivered after all");
}
