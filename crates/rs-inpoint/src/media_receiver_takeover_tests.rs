//! #367 receiver tests: a deferred Publish, a Publish lost in a broadcast
//! lag, and the probes that take them over. Child of
//! `media_receiver_tests.rs` (`#[path]`): the harness lives there.

use super::super::*;
use super::*;
use std::time::Duration;

/// Review finding (#367): a Publish of a DIFFERENT stream must not preempt a
/// healthy live stream. That would leave the live publisher orphaned once
/// the other one leaves. It is remembered instead, and picked up when the
/// current stream ends.
#[tokio::test(start_paused = true)]
async fn other_stream_publish_waits_for_the_live_stream_to_end() {
    let _wd = watchdog("other_stream_publish_waits_for_the_live_stream_to_end");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let tx_a = publish(&slot, &event_tx, &live);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the live stream must be subscribed");
    tx_a.send(FrameData::Video {
        timestamp: 0,
        data: bytes::BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]),
    })
    .unwrap();

    // Another stream starts publishing while A is healthy.
    let _tx_b = publish(&slot, &event_tx, &other);
    assert!(
        next_accepted(&mut log_rx, Duration::from_secs(1))
            .await
            .is_none(),
        "a different stream's Publish must not preempt the healthy live stream"
    );

    // A ends: the remembered Publish of B is picked up right away.
    drop(tx_a);
    let ended_at = tokio::time::Instant::now();
    let sub = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the pending stream must be subscribed once the live one ends");
    assert_eq!(sub.identifier, other);
    assert!(sub.at.duration_since(ended_at) <= Duration::from_millis(100));
}

/// Review finding (#367): a `Lagged` broadcast can swallow a Publish. While
/// Idle, the receiver must probe the last known stream instead of waiting
/// forever for an event that was dropped.
#[tokio::test(start_paused = true)]
async fn lagged_while_idle_probes_the_last_stream() {
    let _wd = watchdog("lagged_while_idle_probes_the_last_stream");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let id = test_identifier();

    // First session, then the publisher leaves: the receiver is Idle.
    let tx1 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("first publish must be subscribed");
    drop(tx1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The publisher comes back, but its Publish is lost in a broadcast lag:
    // the Publish is sent FIRST, then 40 noise events overflow the 16-slot ring.
    let _tx2 = publish(&slot, &event_tx, &id);
    overflow(&event_tx);
    let sub = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("after a Lagged broadcast while Idle the receiver must probe the last stream");
    assert_eq!(sub.identifier, id, "the probe targets the last stream");
    tokio::task::yield_now().await;
    assert!(
        state.is_connected(),
        "a probe that finds the stream publishing starts its session"
    );
}

/// Review finding (#367): a Publish of another stream deferred behind a live
/// stream must be taken over the moment that stream STALLS. A stalled stream
/// is no longer live; waiting for its whole re-subscribe ladder (minutes)
/// before looking at the other publisher leaves ingest dark meanwhile.
#[tokio::test(start_paused = true)]
async fn deferred_publish_is_taken_over_when_the_live_stream_stalls() {
    let _wd = watchdog("deferred_publish_is_taken_over_when_the_live_stream_stalls");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let tx_a = publish(&slot, &event_tx, &live);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the live stream must be subscribed");
    tx_a.send(a_frame(0)).unwrap();
    let last_frame_at = tokio::time::Instant::now();

    // B publishes while A is healthy: deferred.
    let _tx_b = publish(&slot, &event_tx, &other);

    // A freezes (no frames for FRAME_TIMEOUT): B must be taken over at once.
    let sub = next_accepted(&mut log_rx, FRAME_TIMEOUT + Duration::from_secs(5))
        .await
        .expect("the deferred stream must be subscribed once the live one stalls");
    assert_eq!(
        sub.identifier, other,
        "the stalled live stream's slot must go to the deferred publisher"
    );
    let late = sub.at.duration_since(last_frame_at + FRAME_TIMEOUT);
    assert!(
        late <= Duration::from_millis(100),
        "the deferred stream must be taken over at the stall, got {late:?} after it"
    );
    drop(tx_a);
}

/// Review finding (#367): a deferred Publish can be STALE by the time the
/// live stream ends (that publisher already left). Taking it over must be a
/// probe: one Subscribe the hub rejects, then Idle. Never an inpoint reported
/// "connected" with a re-subscribe ladder behind a stream that is gone.
#[tokio::test(start_paused = true)]
async fn stale_deferred_publish_is_probed_and_not_reported_connected() {
    let _wd = watchdog("stale_deferred_publish_is_probed_and_not_reported_connected");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let tx_a = publish(&slot, &event_tx, &live);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the live stream must be subscribed");
    tx_a.send(a_frame(0)).unwrap();

    // B publishes while A is healthy (deferred), then leaves again.
    let tx_b = publish(&slot, &event_tx, &other);
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx_b);
    *slot.lock().unwrap() = None;

    // A ends: the deferred Publish of B is stale now.
    drop(tx_a);
    let mut b_subscribes = 0;
    let mut probed = Vec::new();
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_secs(60), log_rx.recv()).await {
            Ok(Some(rec)) => {
                if rec.identifier == other {
                    b_subscribes += 1;
                }
                probed.push(rec.identifier);
            }
            _ => break,
        }
    }
    assert_eq!(
        b_subscribes, 1,
        "a stale deferred Publish must be probed once, not retried"
    );
    assert_eq!(
        probed,
        vec![other, live],
        "the takeover probe, ONE fallback probe of the ended live stream, then silence"
    );
    assert!(
        !state.is_connected(),
        "a probe that finds nothing publishing must not report the inpoint connected"
    );
}

/// Review finding (#367): a broadcast lag while a stream is LIVE can swallow
/// that stream's own reconnect Publish (OBS reconnected on a new connection;
/// xiu closes the old connection's frames afterwards). Once the old session
/// ends the receiver must probe the stream, or ingest stays dark although
/// the publisher is up.
#[tokio::test(start_paused = true)]
async fn publish_lost_in_a_lag_while_streaming_is_found_when_the_session_ends() {
    let _wd = watchdog("publish_lost_in_a_lag_while_streaming_is_found_when_the_session_ends");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let id = test_identifier();

    let tx_old = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("first publish must be subscribed");
    tx_old.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The reconnect's Publish is lost: 40 noise events overflow the
    // 16-slot ring behind it.
    let _tx_new = publish(&slot, &event_tx, &id);
    overflow(&event_tx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // xiu closes the old connection's frame channel.
    drop(tx_old);
    let ended_at = tokio::time::Instant::now();
    let sub = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the publisher whose Publish was lost in a lag must be found");
    assert_eq!(sub.identifier, id);
    assert!(
        sub.at.duration_since(ended_at) <= Duration::from_millis(100),
        "the probe must go out as soon as the old session ends"
    );
    tokio::task::yield_now().await;
    assert!(state.is_connected(), "the found publisher starts a session");
}

/// Review round 3 (#367): a re-Publish of the LIVE stream while another
/// stream's Publish is deferred keeps the deferred one. It is taken over once
/// the republished stream ends.
#[tokio::test(start_paused = true)]
async fn deferred_publish_survives_a_republish_of_the_live_stream() {
    let _wd = watchdog("deferred_publish_survives_a_republish_of_the_live_stream");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let tx_a = publish(&slot, &event_tx, &live);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the live stream must be subscribed");
    tx_a.send(a_frame(0)).unwrap();
    let _tx_b = publish(&slot, &event_tx, &other);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A reconnects (same stream): it supersedes its old subscription at once.
    let tx_a2 = publish(&slot, &event_tx, &live);
    let sub = next_accepted(&mut log_rx, Duration::from_secs(1))
        .await
        .expect("the live stream's republish is subscribed at once");
    assert_eq!(sub.identifier, live);
    tx_a2.send(a_frame(40)).unwrap();
    // B's publisher is still up.
    let (_tx_b2, rx_b2) = tokio::sync::mpsc::unbounded_channel();
    *slot.lock().unwrap() = Some(rx_b2);

    drop(tx_a2);
    drop(tx_a);
    let sub = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the deferred stream is taken over once the republished one ends");
    assert_eq!(sub.identifier, other);
}

/// A Publish of the SAME stream while it is streaming (OBS reconnected on a
/// new connection) supersedes the old subscription at once: never deferred.
#[tokio::test(start_paused = true)]
async fn same_stream_republish_while_streaming_supersedes_at_once() {
    let _wd = watchdog("same_stream_republish_while_streaming_supersedes_at_once");
    let state = InpointState::new();
    let (event_tx, slot, mut log_rx) =
        running_receiver(Arc::new(FlvChunkSink::new_null()), state.clone());
    let id = test_identifier();

    let tx1 = publish(&slot, &event_tx, &id);
    next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("first publish must be subscribed");
    tx1.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let _tx2 = publish(&slot, &event_tx, &id);
    let published_at = tokio::time::Instant::now();
    let sub = next_accepted(&mut log_rx, Duration::from_secs(5))
        .await
        .expect("the republish of the live stream must be subscribed");
    assert!(
        sub.at.duration_since(published_at) <= Duration::from_millis(100),
        "a republish of the live stream joins at once"
    );
    drop(tx1);
}

/// A hub whose Subscribe requests the test answers by hand.
fn spawn_manual_hub(
    mut hub_rx: tokio::sync::mpsc::UnboundedReceiver<StreamHubEvent>,
) -> tokio::sync::mpsc::UnboundedReceiver<(StreamIdentifier, oneshot::Sender<SubscribeReply>)> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(event) = hub_rx.recv().await {
            if let StreamHubEvent::Subscribe {
                identifier,
                result_sender,
                ..
            } = event
            {
                let _ = tx.send((identifier, result_sender));
            }
        }
    });
    rx
}

/// Review round 3 (#367): a lag while a probe is in flight must not send a
/// second, concurrent probe. The lag is covered by ONE more probe once the
/// first one is answered; then the receiver stays idle.
#[tokio::test(start_paused = true)]
async fn a_lag_during_a_probe_is_covered_after_it() {
    let _wd = watchdog("a_lag_during_a_probe_is_covered_after_it");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let id = test_identifier();
    let within = Duration::from_secs(5);

    // A first session that ends: the receiver is Idle and knows the stream.
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: id.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the Publish is subscribed")
        .unwrap();
    let frames_tx = accept(reply);
    frames_tx.send(a_frame(0)).unwrap();
    drop(frames_tx);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!state.is_connected(), "the first session ended");

    // Lag 1 while Idle: one probe goes out.
    overflow(&event_tx);
    let (probed, probe_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("a lag while idle sends a probe")
        .unwrap();
    assert_eq!(probed, id);

    // Lag 2 while that probe is in flight: no concurrent second probe.
    overflow(&event_tx);
    assert!(
        tokio::time::timeout(within, requests.recv()).await.is_err(),
        "no second probe while one is in flight"
    );

    // The probe finds nothing: the second lag is covered by ONE more probe.
    let _ = probe_reply.send(Err(rejected()));
    let (again, again_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the lag during the probe is covered after it")
        .unwrap();
    assert_eq!(again, id);
    let _ = again_reply.send(Err(rejected()));
    assert!(
        tokio::time::timeout(Duration::from_secs(60), requests.recv())
            .await
            .is_err(),
        "then the receiver stays idle"
    );
    assert!(!state.is_connected());
}

/// Receiver + hand-answered hub, ready to `run()`.
fn manual_receiver(
    state: InpointState,
) -> (
    tokio::sync::broadcast::Sender<BroadcastEvent>,
    tokio::sync::mpsc::UnboundedReceiver<(StreamIdentifier, oneshot::Sender<SubscribeReply>)>,
) {
    let (event_tx, event_rx) = tokio::sync::broadcast::channel(16);
    let (hub_tx, hub_rx) = tokio::sync::mpsc::unbounded_channel();
    let receiver = MediaReceiver::new(event_rx, hub_tx, Arc::new(FlvChunkSink::new_null()), state);
    let requests = spawn_manual_hub(hub_rx);
    tokio::spawn(receiver.run());
    (event_tx, requests)
}

/// Accept a Subscribe with a fresh frame channel; returns its sender.
fn accept(reply: oneshot::Sender<SubscribeReply>) -> tokio::sync::mpsc::UnboundedSender<FrameData> {
    let (frames_tx, frames_rx) = tokio::sync::mpsc::unbounded_channel();
    let _ = reply.send(Ok((
        DataReceiver {
            frame_receiver: Some(frames_rx),
            packet_receiver: None,
        },
        None,
    )));
    frames_tx
}

/// Third review (#367): a deferred Publish is taken over at the live
/// stream's stall, but that publisher may already be gone. A stalled live
/// publisher can still be registered at the hub (a 30 s network stall on the
/// same connection) and resume WITHOUT a new Publish. When the takeover probe
/// finds nothing, the receiver must look at the stalled live stream again
/// instead of going dark.
#[tokio::test(start_paused = true)]
async fn a_stale_deferred_publish_falls_back_to_the_stalled_live_stream() {
    let _wd = watchdog("a_stale_deferred_publish_falls_back_to_the_stalled_live_stream");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: live.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the live stream is subscribed")
        .unwrap();
    let frames_a = accept(reply);
    frames_a.send(a_frame(0)).unwrap();
    // Let A reach Streaming before B publishes.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // B publishes while A is live: deferred.
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();

    // A stalls: B is taken over as a probe ... and B is already gone.
    let (probed, b_reply) =
        tokio::time::timeout(FRAME_TIMEOUT + Duration::from_secs(5), requests.recv())
            .await
            .expect("the deferred stream is probed at the stall")
            .unwrap();
    assert_eq!(probed, other);
    let _ = b_reply.send(Err(rejected()));

    // The stalled live stream is still registered: the receiver falls back.
    let (again, a_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("a stale takeover must fall back to the stalled live stream")
        .unwrap();
    assert_eq!(again, live);
    assert!(
        !state.is_connected(),
        "a probe in flight is not a session: the inpoint is not connected yet"
    );
    let frames_a2 = accept(a_reply);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(state.is_connected(), "the live stream is attached again");
    drop(frames_a);
    drop(frames_a2);
}

/// Third review (#367): a lag that arrives while a Subscribe is IN FLIGHT
/// can hide a Publish newer than the attachment the Subscribe brings back.
/// Accepting that Subscribe covers only the lags from before it was sent;
/// once the session ends, the newer lag still gets its probe.
#[tokio::test(start_paused = true)]
async fn a_lag_while_subscribing_is_probed_after_the_session() {
    let _wd = watchdog("a_lag_while_subscribing_is_probed_after_the_session");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let id = test_identifier();
    let within = Duration::from_secs(5);

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: id.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the Publish is subscribed")
        .unwrap();
    // A lag while that Subscribe is in flight, THEN the hub accepts it.
    overflow(&event_tx);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let frames = accept(reply);
    frames.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The session ends: the lag seen during the Subscribe gets its probe.
    drop(frames);
    let (probed, _probe_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("a lag seen while subscribing must be probed once the session ends")
        .unwrap();
    assert_eq!(probed, id);
}

/// Fourth review (#367): the fallback probe targets the same stream a lag
/// probe would, so sending it covers an earlier lag too. A lag while the
/// live stream streamed, then a stale takeover, costs the takeover probe and
/// ONE fallback probe, not a third, identical lag probe after them.
#[tokio::test(start_paused = true)]
async fn a_fallback_probe_also_covers_an_earlier_lag() {
    let _wd = watchdog("a_fallback_probe_also_covers_an_earlier_lag");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: live.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the live stream is subscribed")
        .unwrap();
    let frames_a = accept(reply);
    frames_a.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // B is deferred (read before the lag), then a lag while A streams.
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    overflow(&event_tx);

    // A stalls: B is taken over and found gone, the fallback finds A gone.
    let mut probed = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(FRAME_TIMEOUT + within, requests.recv()).await {
            Ok(Some((id, reply))) => {
                probed.push(id);
                let _ = reply.send(Err(rejected()));
            }
            _ => break,
        }
    }
    assert_eq!(
        probed,
        vec![other, live],
        "the takeover probe, then ONE fallback probe that also covers the lag"
    );
    drop(frames_a);
}

/// Fourth review (#367): a takeover probe the hub never answers times out
/// after SUBSCRIPTION_TIMEOUT and fails like a rejected one: it falls back
/// to the stalled live stream.
#[tokio::test(start_paused = true)]
async fn a_timed_out_takeover_probe_falls_back_like_a_rejected_one() {
    let _wd = watchdog("a_timed_out_takeover_probe_falls_back_like_a_rejected_one");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: live.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the live stream is subscribed")
        .unwrap();
    let frames_a = accept(reply);
    frames_a.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();

    // A stalls: B is taken over, and the hub never answers B's probe.
    let (probed, b_reply) = tokio::time::timeout(FRAME_TIMEOUT + within, requests.recv())
        .await
        .expect("the deferred stream is probed at the stall")
        .unwrap();
    assert_eq!(probed, other);
    let sent_at = tokio::time::Instant::now();
    let (again, a_reply) = tokio::time::timeout(SUBSCRIPTION_TIMEOUT + within, requests.recv())
        .await
        .expect("a timed-out takeover probe falls back to the live stream")
        .unwrap();
    assert_eq!(again, live);
    assert!(
        sent_at.elapsed() >= SUBSCRIPTION_TIMEOUT,
        "the fallback goes out only once the probe timed out"
    );
    drop((b_reply, a_reply, frames_a));
}

/// Fifth review (#367): two stream keys DO reach stream.lan's inpoint (OBS
/// `live/obs-e2e-test`, the CI ffmpeg `live/ci-e2e-test`). When a takeover
/// of B is ACCEPTED and B's session later ends, the stalled stream A it
/// superseded must be looked at again: A's publisher can still be registered
/// and resume without a new Publish.
#[tokio::test(start_paused = true)]
async fn the_end_of_an_accepted_takeover_reprobes_the_superseded_stream() {
    let _wd = watchdog("the_end_of_an_accepted_takeover_reprobes_the_superseded_stream");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    event_tx
        .send(BroadcastEvent::Publish {
            identifier: live.clone(),
        })
        .unwrap();
    let (_, reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the live stream is subscribed")
        .unwrap();
    let frames_a = accept(reply);
    frames_a.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();

    // A stalls: B is taken over, and B is live.
    let (probed, b_reply) = tokio::time::timeout(FRAME_TIMEOUT + within, requests.recv())
        .await
        .expect("the deferred stream is probed at the stall")
        .unwrap();
    assert_eq!(probed, other);
    let frames_b = accept(b_reply);
    frames_b.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(state.is_connected(), "B's session started");

    // B ends: the superseded A is probed once.
    drop(frames_b);
    let (again, _a_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the end of B must re-probe the stream B superseded")
        .unwrap();
    assert_eq!(again, live);
    drop(frames_a);
}
