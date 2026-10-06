//! Tests for `media_receiver.rs`. Loaded via `#[cfg(test)] #[path = "media_receiver_tests.rs"] mod tests;`
//! to keep the production file under the 1000-line CI gate.

use super::*;
use std::time::Duration;
use streamhub::define::DataReceiver;

// ---------------------------------------------------------------------------
// #367 event-driven receiver: a programmable hub that answers EVERY Subscribe
// from a "current publisher" slot (accept), or rejects when no publisher is
// up. That is the real xiu hub's behaviour across a publisher restart.
// ---------------------------------------------------------------------------

/// One Subscribe request the programmable hub answered.
#[derive(Debug, Clone)]
struct SubRecord {
    at: tokio::time::Instant,
    accepted: bool,
    /// The stream the Subscribe asked for.
    identifier: StreamIdentifier,
    /// The subscriber id the request carried.
    subscriber: String,
}

type PublisherSlot = Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<FrameData>>>>;

/// Spawn a hub that serves every Subscribe from `slot` (taking the current
/// publisher's frame receiver), or rejects with `NoAppOrStreamName` when the
/// slot is empty, and logs each answer to the returned channel.
fn spawn_programmable_hub(
    hub_rx: tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
) -> (
    PublisherSlot,
    tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
) {
    let (slot, log_rx, _unsubscribed) = spawn_recording_hub(hub_rx);
    (slot, log_rx)
}

/// `spawn_programmable_hub` that also reports the subscriber id of every
/// UnSubscribe it receives.
fn spawn_recording_hub(
    mut hub_rx: tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
) -> (
    PublisherSlot,
    tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let slot: PublisherSlot = Arc::new(std::sync::Mutex::new(None));
    let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel();
    let (unsub_tx, unsub_rx) = tokio::sync::mpsc::unbounded_channel();
    let hub_slot = Arc::clone(&slot);
    tokio::spawn(async move {
        while let Some(event) = hub_rx.recv().await {
            match event {
                StreamHubEvent::Subscribe {
                    identifier,
                    info,
                    result_sender,
                } => {
                    let frames = hub_slot.lock().unwrap().take();
                    let accepted = frames.is_some();
                    let reply = match frames {
                        Some(rx) => Ok((
                            DataReceiver {
                                frame_receiver: Some(rx),
                                packet_receiver: None,
                            },
                            None,
                        )),
                        None => Err(rejected()),
                    };
                    let _ = result_sender.send(reply);
                    let _ = log_tx.send(SubRecord {
                        at: tokio::time::Instant::now(),
                        accepted,
                        identifier,
                        subscriber: info.id.to_string(),
                    });
                }
                StreamHubEvent::UnSubscribe { info, .. } => {
                    let _ = unsub_tx.send(info.id.to_string());
                }
                _ => {}
            }
        }
    });
    (slot, log_rx, unsub_rx)
}

/// The hub's answer when nothing publishes on the stream.
fn rejected() -> streamhub::errors::StreamHubError {
    streamhub::errors::StreamHubError {
        value: streamhub::errors::StreamHubErrorValue::NoAppOrStreamName,
    }
}

/// A publisher (re)connects. The hub registers its stream, THEN broadcasts
/// Publish (the streamhub `publish()` order). Returns the publisher's frame
/// sender; dropping it is the publisher disconnecting.
fn publish(
    slot: &PublisherSlot,
    event_tx: &tokio::sync::broadcast::Sender<BroadcastEvent>,
    identifier: &StreamIdentifier,
) -> tokio::sync::mpsc::UnboundedSender<FrameData> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx);
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: identifier.clone(),
        })
        .expect("receiver subscribed to broadcast events");
    tx
}

async fn next_accepted(
    log_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
    within: Duration,
) -> Option<SubRecord> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, log_rx.recv()).await {
            Ok(Some(rec)) if rec.accepted => return Some(rec),
            Ok(Some(_rejected)) => continue,
            _ => return None,
        }
    }
}

fn test_identifier() -> StreamIdentifier {
    StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: "test".to_string(),
    }
}

/// Design test 2 (#367): the Thursday lost-event chain. A Publish (OBS
/// reconnecting) arrives while the receiver sits on a STALLED subscription.
/// The pre-fix receiver never read `event_rx` there, so that Publish stayed
/// queued and was consumed later as a STALE event. The receiver then stayed
/// one event behind for good, and every later real republish was joined
/// 0-8 s late via the retry ladder (GOP-cache replay, baked-in offset).
/// The next republish must be subscribed within 100 ms and must re-anchor
/// the chunker session.
#[tokio::test(start_paused = true)]
async fn publish_during_stall_is_not_lost_and_next_republish_joins_immediately() {
    let _wd = watchdog("publish_during_stall_is_not_lost_and_next_republish_joins_immediately");
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let state = InpointState::new();
    // start_new_session() clears the ingest-skew latch, which makes the
    // re-anchor observable from outside the receiver.
    let sink = Arc::new(FlvChunkSink::new_null().with_ingest_state(state.clone(), 2_000));
    let receiver = MediaReceiver::new(event_rx, hub_tx, sink, state.clone());
    let (slot, mut log_rx) = spawn_programmable_hub(hub_rx);
    let id = test_identifier();
    let _run = tokio::spawn(receiver.run());

    // P1: first publisher, one frame, then it stalls (process freeze).
    let tx1 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("first publish must be subscribed");
    tx1.send(FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    })
    .unwrap();

    // P2 arrives 10 s into the stall: OBS reconnected on a new connection.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let tx2 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(60))
        .await
        .expect("the reconnected publisher must be subscribed");

    // P2's publisher goes away (the operator closes OBS).
    drop(tx2);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // P3: the real fresh publish. Raise the skew latch first so that the
    // re-anchor (start_new_session clears it) is observable.
    state.set_ingest_skew_active(true);
    let _tx3 = publish(&slot, &event_tx, &id);
    let published_at = tokio::time::Instant::now();
    let sub = next_accepted(&mut log_rx, Duration::from_secs(30))
        .await
        .expect("the fresh publish must eventually be subscribed");
    let lag = sub.at.duration_since(published_at);
    assert!(
        lag <= Duration::from_millis(100),
        "the fresh publish must be subscribed within 100 ms, got {lag:?}; a late join \
         replays the GOP cache and bakes an A/V offset in (#367)"
    );
    tokio::task::yield_now().await;
    assert!(
        !state.ingest_skew_active(),
        "the fresh publish must re-anchor the chunker session (start_new_session)"
    );
    drop(tx1);
}

/// Design test 3 (#367): a `RecvError::Lagged` on the hub's broadcast
/// channel used to `break` `run()`. The RTMP server reads that as a clean
/// stop, so ingest silently died until a process restart. A lag must be
/// logged and survived.
#[tokio::test(start_paused = true)]
async fn lagged_broadcast_does_not_end_run() {
    let _wd = watchdog("lagged_broadcast_does_not_end_run");
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        InpointState::new(),
    );
    let (slot, mut log_rx) = spawn_programmable_hub(hub_rx);
    let id = test_identifier();
    let run = tokio::spawn(receiver.run());

    // Overflow the 16-slot broadcast ring before the receiver reads once.
    for i in 0..40 {
        event_tx
            .send(BroadcastEvent::UnSubscribe {
                id: format!("noise-{i}"),
                result_sender: None,
            })
            .unwrap();
    }
    let _tx = publish(&slot, &event_tx, &id);

    let sub = next_accepted(&mut log_rx, Duration::from_secs(5)).await;
    assert!(
        sub.is_some(),
        "after a Lagged broadcast the receiver must keep running and subscribe to the Publish"
    );
    assert!(!run.is_finished(), "a Lagged broadcast must not end run()");
}

// ---------------------------------------------------------------------------
// Stall / disconnect / subscription-timeout behaviour, driven through run().
// (#367 re-expressed these from the removed `process_stream`/`StreamEnd`
// internals; the asserted behaviour is unchanged.)
// ---------------------------------------------------------------------------

/// Receiver + programmable hub + broadcast sender, ready to `run()`.
fn running_receiver(
    sink: Arc<FlvChunkSink>,
    state: InpointState,
) -> (
    tokio::sync::broadcast::Sender<BroadcastEvent>,
    PublisherSlot,
    tokio::sync::mpsc::UnboundedReceiver<SubRecord>,
) {
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(event_rx, hub_tx, sink, state);
    let (slot, log_rx) = spawn_programmable_hub(hub_rx);
    tokio::spawn(receiver.run());
    (event_tx, slot, log_rx)
}

/// A stalled subscription (publisher alive, no frames for FRAME_TIMEOUT) is
/// dropped and re-subscribed, never left hanging forever.
#[tokio::test(start_paused = true)]
async fn stalled_subscription_is_resubscribed_after_frame_timeout() {
    let _wd = watchdog("stalled_subscription_is_resubscribed_after_frame_timeout");
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), InpointState::new());
    let id = test_identifier();

    let tx = publish(&slot, &event_tx, &id);
    let first = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    tx.send(FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    })
    .unwrap();

    // The publisher is still up (xiu would accept a new subscription).
    let (_tx_again, rx_again) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_again);

    let again = next_accepted(&mut log_rx, Duration::from_secs(120))
        .await
        .expect("a stalled subscription must be re-subscribed");
    let gap = again.at.duration_since(first.at);
    assert!(
        gap >= FRAME_TIMEOUT,
        "re-subscribe must wait out FRAME_TIMEOUT ({FRAME_TIMEOUT:?}), happened after {gap:?}"
    );
    drop(tx);
}

/// The publisher disconnecting (frame channel closed) ends the session: the
/// inpoint reports disconnected.
#[tokio::test(start_paused = true)]
async fn publisher_disconnect_ends_the_session() {
    let _wd = watchdog("publisher_disconnect_ends_the_session");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let id = test_identifier();

    let tx = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    assert!(
        state.is_connected(),
        "Publish must mark the inpoint connected"
    );

    drop(tx);
    for _ in 0..50 {
        if !state.is_connected() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("publisher disconnect must mark the inpoint disconnected");
}

/// A Subscribe the hub never answers times out after SUBSCRIPTION_TIMEOUT and
/// is retried, instead of hanging.
#[tokio::test(start_paused = true)]
async fn unanswered_subscribe_times_out_and_is_retried() {
    let _wd = watchdog("unanswered_subscribe_times_out_and_is_retried");
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, mut hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        InpointState::new(),
    );
    // A hub that records Subscribe requests but never answers them (it keeps
    // the reply senders alive so the receiver really has to time out).
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut parked = Vec::new();
        while let Some(event) = hub_rx.recv().await {
            if let StreamHubEvent::Subscribe { result_sender, .. } = event {
                parked.push(result_sender);
                let _ = seen_tx.send(tokio::time::Instant::now());
            }
        }
    });
    tokio::spawn(receiver.run());

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: test_identifier(),
        })
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), seen_rx.recv())
        .await
        .expect("first Subscribe")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(60), seen_rx.recv())
        .await
        .expect("an unanswered Subscribe must be retried")
        .unwrap();
    assert!(
        second.duration_since(first) >= SUBSCRIPTION_TIMEOUT,
        "retry must come after SUBSCRIPTION_TIMEOUT ({SUBSCRIPTION_TIMEOUT:?})"
    );
}

/// A stall flushes the partial FLV chunk, so the frames received before the
/// freeze still reach disk/S3.
#[tokio::test(start_paused = true)]
async fn stall_flushes_the_partial_chunk() {
    let _wd = watchdog("stall_flushes_the_partial_chunk");
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(FlvChunkSink::new(
        dir.path().to_path_buf(),
        Duration::from_secs(60), // long duration -- won't auto-flush
    ));
    let (event_tx, slot, mut log_rx) = running_receiver(Arc::clone(&sink), InpointState::new());

    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    tx.send(FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]),
    })
    .unwrap();
    tx.send(FrameData::Video {
        timestamp: 100,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD][..]),
    })
    .unwrap();

    // The publisher stays up, so the re-subscribe after the stall is
    // accepted and the receiver goes quiet again.
    let (_tx_again, rx_again) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_again);

    // Stall past FRAME_TIMEOUT.
    tokio::time::sleep(FRAME_TIMEOUT + Duration::from_secs(1)).await;
    assert!(
        sink.chunk_count().await > 0,
        "FLV chunk sink should have flushed partial data on a stall"
    );
    drop(tx);
}

/// #367 (A): a CLOSED hub broadcast channel is a failure, not a clean stop:
/// `run()` returns `Err`, so `RtmpServer::run` propagates it and the
/// orchestrator restarts the server. Before, `run()` just returned and the
/// server read that as a clean shutdown.
#[tokio::test(start_paused = true)]
async fn closed_hub_channel_ends_run_with_error() {
    let _wd = watchdog("closed_hub_channel_ends_run_with_error");
    let state = InpointState::new();
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        state.clone(),
    );
    let (slot, mut log_rx) = spawn_programmable_hub(hub_rx);
    let run = tokio::spawn(receiver.run());

    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");

    drop(event_tx);
    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("run() must end when the hub channel closes")
        .expect("run() must not panic");
    assert!(
        matches!(result, Err(InpointError::Protocol(_))),
        "a closed hub channel must surface as an error, got {result:?}"
    );
    assert!(
        !state.is_connected(),
        "the active session must be ended on the way out"
    );
    drop(tx);
}

fn identifier_named(stream_name: &str) -> StreamIdentifier {
    StreamIdentifier::Rtmp {
        app_name: "live".to_string(),
        stream_name: stream_name.to_string(),
    }
}

/// One video frame, enough to mark a session as having had frames.
fn a_frame(timestamp: u32) -> FrameData {
    FrameData::Video {
        timestamp,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    }
}

/// Real-time watchdog for the paused-clock tests (#367). A mutant that makes
/// the receiver spin keeps the runtime busy, so the paused clock never
/// advances and the test would hang until cargo-mutants' 300 s timeout.
/// After 30 s of REAL time the whole test process aborts instead: a hung
/// test is a failed test. Dropped at the end of the test (also on a panic).
struct Watchdog(Option<std::sync::mpsc::Sender<()>>);

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.0.take());
    }
}

fn watchdog(test: &'static str) -> Watchdog {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if rx.recv_timeout(Duration::from_secs(30))
            == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            eprintln!("{test}: still running after 30 s of real time -- aborting");
            std::process::abort();
        }
    });
    Watchdog(Some(tx))
}

/// Overflow the 16-slot broadcast ring: the receiver's next read lags.
fn overflow(event_tx: &tokio::sync::broadcast::Sender<BroadcastEvent>) {
    for i in 0..40 {
        event_tx
            .send(BroadcastEvent::UnSubscribe {
                id: format!("noise-{i}"),
                result_sender: None,
            })
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// #367 review round 3: the re-subscribe ladder, hub bookkeeping, audit rows.
// ---------------------------------------------------------------------------

#[test]
fn retry_delay_grows_by_two_seconds_up_to_ten() {
    let secs: Vec<u64> = (1..=7).map(|r| retry_delay(r).as_secs()).collect();
    assert_eq!(secs, [2, 4, 6, 8, 10, 10, 10]);
}

#[test]
fn phase_names_label_the_logs() {
    assert_eq!(Phase::Idle.name(), "idle");
    assert_eq!(
        Phase::RetryWait {
            at: tokio::time::Instant::now()
        }
        .name(),
        "retry_wait"
    );
}

#[test]
fn frames_resuming_reports_and_resets_the_retry_count() {
    let mut s = Session::new(test_identifier());
    assert_eq!(s.frames_resumed(), None, "no retries pending");
    s.retries = 3;
    assert_eq!(s.frames_resumed(), Some(3));
    assert_eq!(
        s.retries, 0,
        "frames flowing restart the re-subscribe ladder"
    );
    assert_eq!(s.frames_resumed(), None);
}

/// Every rejected Subscribe backs off 2, 4, 6, 8, then 10 s, and after
/// MAX_RESUBSCRIBE_RETRIES attempts the session is given up: the inpoint
/// reports disconnected and no further Subscribe goes out.
#[tokio::test(start_paused = true)]
async fn rejected_subscribes_back_off_then_give_up() {
    let _wd = watchdog("rejected_subscribes_back_off_then_give_up");
    let state = InpointState::new();
    let (event_tx, _slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    // A Publish nothing serves: every Subscribe is rejected.
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: test_identifier(),
        })
        .unwrap();
    let mut at = Vec::new();
    for attempt in 0..MAX_RESUBSCRIBE_RETRIES {
        let rec = tokio::time::timeout(Duration::from_secs(60), log_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("Subscribe attempt {attempt} never came"))
            .expect("hub log open");
        assert!(!rec.accepted);
        at.push(rec.at);
    }
    let gaps: Vec<u64> = at
        .windows(2)
        .map(|w| w[1].duration_since(w[0]).as_secs())
        .collect();
    let mut expected = vec![2, 4, 6, 8];
    expected.resize(MAX_RESUBSCRIBE_RETRIES as usize - 1, 10);
    assert_eq!(gaps, expected, "the re-subscribe ladder");
    assert!(
        tokio::time::timeout(Duration::from_secs(60), log_rx.recv())
            .await
            .is_err(),
        "after MAX_RESUBSCRIBE_RETRIES the receiver gives up"
    );
    assert!(
        !state.is_connected(),
        "a given-up session reports the inpoint disconnected"
    );
}

/// A hub that cannot take requests anymore (its event receiver is gone) is
/// retried on the same ladder and then given up, never left "connected".
#[tokio::test(start_paused = true)]
async fn unreachable_hub_is_retried_then_given_up() {
    let _wd = watchdog("unreachable_hub_is_retried_then_given_up");
    let state = InpointState::new();
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    drop(hub_rx);
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        state.clone(),
    );
    tokio::spawn(receiver.run());
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: test_identifier(),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(state.is_connected(), "the Publish starts a session");
    // The whole ladder is 2 + 4 + 6 + 8 + 25 x 10 = 270 s.
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(
        !state.is_connected(),
        "an unreachable hub must be given up after the ladder"
    );
}

/// A dropped live subscription is unsubscribed from the hub (xiu never
/// prunes a dead frame sender on its own): the stalled subscriber is the one
/// removed.
#[tokio::test(start_paused = true)]
async fn a_stalled_subscription_is_unsubscribed_from_the_hub() {
    let _wd = watchdog("a_stalled_subscription_is_unsubscribed_from_the_hub");
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(
        event_rx,
        hub_tx,
        Arc::new(FlvChunkSink::new_null()),
        InpointState::new(),
    );
    let (slot, mut log_rx, mut unsubscribed) = spawn_recording_hub(hub_rx);
    tokio::spawn(receiver.run());

    let tx = publish(&slot, &event_tx, &test_identifier());
    let first = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    tx.send(a_frame(0)).unwrap();
    let (_tx_again, rx_again) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_again);

    let gone = tokio::time::timeout(FRAME_TIMEOUT + Duration::from_secs(5), unsubscribed.recv())
        .await
        .expect("a stalled subscription must be unsubscribed from the hub")
        .expect("hub open");
    assert_eq!(
        gone, first.subscriber,
        "the stalled subscriber is the one removed"
    );
    drop(tx);
}

/// The session start and end are audited (RtmpConnected with its trigger,
/// RtmpDisconnected with its reason): the operator's ingest timeline.
#[tokio::test(start_paused = true)]
async fn session_start_and_end_are_audited() {
    let _wd = watchdog("session_start_and_end_are_audited");
    use rs_core::audit::{Action, Source};
    let (audit_tx, mut audit_rx) = tokio::sync::mpsc::channel(64);
    let state = InpointState::new().with_audit_tx(audit_tx);
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());

    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut rows = Vec::new();
    while let Ok(row) = audit_rx.try_recv() {
        rows.push(row);
    }
    let connected = rows
        .iter()
        .find(|r| r.action == Action::RtmpConnected)
        .expect("the session start is audited");
    assert_eq!(connected.source, Source::Inpoint);
    assert_eq!(connected.detail["trigger"], "publish");
    let disconnected = rows
        .iter()
        .find(|r| r.action == Action::RtmpDisconnected)
        .expect("the session end is audited");
    assert_eq!(disconnected.detail["reason"], "publisher_closed");
}

/// A re-subscribe after frames flowed re-anchors the chunker session: xiu
/// gives no session id, so a new publisher can never be ruled out.
#[tokio::test(start_paused = true)]
async fn resubscribe_after_frames_reanchors_the_chunker_session() {
    let _wd = watchdog("resubscribe_after_frames_reanchors_the_chunker_session");
    let state = InpointState::new();
    let sink = Arc::new(FlvChunkSink::new_null().with_ingest_state(state.clone(), 2_000));
    let (event_tx, slot, mut log_rx) = running_receiver(sink, state.clone());

    let tx = publish(&slot, &event_tx, &test_identifier());
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("publish must be subscribed");
    tx.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // start_new_session() clears the ingest-skew latch: raise it to observe.
    state.set_ingest_skew_active(true);
    let (_tx_again, rx_again) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_again);

    next_accepted(&mut log_rx, FRAME_TIMEOUT + Duration::from_secs(5))
        .await
        .expect("the stalled subscription must be re-subscribed");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !state.ingest_skew_active(),
        "a re-subscribe after frames flowed must re-anchor the chunker session"
    );
    drop(tx);
}

#[path = "media_receiver_takeover_tests.rs"]
mod takeover;

#[path = "media_receiver_gap_tests.rs"]
mod gaps;
