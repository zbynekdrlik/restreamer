//! #367 receiver tests: streams the receiver LEFT without seeing them end
//! are probed once when it is idle again. Child of
//! `media_receiver_takeover_tests.rs` (`#[path]`): the hand-answered hub
//! helpers live there, the harness one level higher.

use super::*;
use std::time::Duration;

/// Publish `identifier` and accept its Subscribe; returns the frame sender
/// after one frame went through (the stream is Streaming).
async fn stream(
    event_tx: &tokio::sync::broadcast::Sender<BroadcastEvent>,
    requests: &mut HubRequests,
    identifier: &StreamIdentifier,
) -> tokio::sync::mpsc::UnboundedSender<FrameData> {
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: identifier.clone(),
        })
        .unwrap();
    let (subscribed, reply) = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .expect("the Publish is subscribed")
        .unwrap();
    assert_eq!(&subscribed, identifier);
    let frames = accept(reply);
    frames.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    frames
}

/// Every Subscribe for `within`, each answered with a rejection.
async fn rejected_probes(requests: &mut HubRequests, within: Duration) -> Vec<StreamIdentifier> {
    let mut probed = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(within, requests.recv()).await {
            Ok(Some((id, reply))) => {
                probed.push(id);
                let _ = reply.send(Err(rejected()));
            }
            _ => break,
        }
    }
    probed
}

/// Sixth review (#367): a Publish of another stream that arrives AFTER the
/// live stream stalled is not deferred; it supersedes the stalled session
/// directly. The stalled stream can still be registered and resume without
/// a new Publish, so once the new stream ends it is probed once.
#[tokio::test(start_paused = true)]
async fn a_stream_superseded_by_a_direct_publish_is_probed_once_idle() {
    let _wd = watchdog("a_stream_superseded_by_a_direct_publish_is_probed_once_idle");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-c");

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    // A stalls; C publishes while A waits to re-subscribe.
    tokio::time::sleep(FRAME_TIMEOUT + Duration::from_secs(1)).await;
    let frames_c = stream(&event_tx, &mut requests, &other).await;

    drop(frames_c);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![live],
        "once C ends, the stalled A it superseded is probed exactly once"
    );
    drop(frames_a);
}

/// Sixth review (#367): a remembered stream that gets its OWN session again
/// is forgotten; the takeover probe it abandoned is remembered instead.
#[tokio::test(start_paused = true)]
async fn a_stream_with_its_own_session_again_is_forgotten() {
    let _wd = watchdog("a_stream_with_its_own_session_again_is_forgotten");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();
    // A stalls: B is taken over ...
    let (probed, b_reply) = tokio::time::timeout(FRAME_TIMEOUT + within, requests.recv())
        .await
        .expect("the deferred stream is probed at the stall")
        .unwrap();
    assert_eq!(probed, other);
    // ... and A republishes while B's probe is in flight.
    let frames_a2 = stream(&event_tx, &mut requests, &live).await;
    drop(b_reply);

    // A ends: A had its own session, so only the abandoned B is probed.
    drop(frames_a2);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![other],
        "A is forgotten once it has its own session; the abandoned B probe is not"
    );
    drop(frames_a);
}

/// Sixth review (#367): a lag while the taken-over B streams can hide B's
/// own reconnect. When the probe of the superseded A is ACCEPTED, that lag
/// must not be lost: once A ends, B is probed.
#[tokio::test(start_paused = true)]
async fn an_accepted_probe_does_not_swallow_a_lag_of_another_stream() {
    let _wd = watchdog("an_accepted_probe_does_not_swallow_a_lag_of_another_stream");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: other.clone(),
        })
        .unwrap();
    let (_, b_reply) = tokio::time::timeout(FRAME_TIMEOUT + within, requests.recv())
        .await
        .expect("the deferred stream is probed at the stall")
        .unwrap();
    let frames_b = accept(b_reply);
    frames_b.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A lag while B streams, then B ends: the superseded A is probed and
    // this time it is up.
    overflow(&event_tx);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(frames_b);
    let (again, a_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the superseded A is probed")
        .unwrap();
    assert_eq!(again, live);
    let frames_a2 = accept(a_reply);
    frames_a2.send(a_frame(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(state.is_connected(), "A is attached again");

    // A ends: the lag seen while B streamed still gets its probe of B.
    drop(frames_a2);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![other],
        "the lag that could hide B's reconnect is probed after A"
    );
    drop(frames_a);
}

/// Seventh review (#367): a pending lag belongs to the LAST stream; ANY
/// session start of another stream must keep it (as a remembered stream),
/// not only an accepted probe. Here a Publish abandons a remembered probe
/// and starts its stream's session directly.
#[tokio::test(start_paused = true)]
async fn a_session_start_of_another_stream_keeps_a_pending_lag() {
    let _wd = watchdog("a_session_start_of_another_stream_keeps_a_pending_lag");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let within = Duration::from_secs(5);

    // B streams and stalls; A publishes and supersedes it (B remembered).
    let frames_b = stream(&event_tx, &mut requests, &other).await;
    tokio::time::sleep(FRAME_TIMEOUT + Duration::from_secs(1)).await;
    let frames_a = stream(&event_tx, &mut requests, &live).await;

    // A lag while A streams, then A's connection closes.
    overflow(&event_tx);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(frames_a);
    let (probed, b_reply) = tokio::time::timeout(within, requests.recv())
        .await
        .expect("the remembered B is probed")
        .unwrap();
    assert_eq!(probed, other);
    // B publishes while that probe is in flight: its own session starts.
    let frames_b2 = stream(&event_tx, &mut requests, &other).await;
    drop(b_reply);

    // B ends: the lag that could hide A's reconnect is still probed.
    drop(frames_b2);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![live],
        "the pending lag of A survives B's session start"
    );
    drop(frames_b);
}

/// Seventh review (#367): a probe of a remembered stream covers its entry;
/// a stream just probed and found gone is not probed again.
#[tokio::test(start_paused = true)]
async fn a_probe_covers_its_remembered_entry() {
    let _wd = watchdog("a_probe_covers_its_remembered_entry");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    // A streams and stalls; B supersedes it directly (A remembered).
    let frames_a = stream(&event_tx, &mut requests, &live).await;
    tokio::time::sleep(FRAME_TIMEOUT + Duration::from_secs(1)).await;
    let frames_b = stream(&event_tx, &mut requests, &other).await;
    // A publishes again while B streams: deferred.
    event_tx
        .send(BroadcastEvent::Publish {
            identifier: live.clone(),
        })
        .unwrap();

    // B stalls: A is taken over (B remembered). Both are gone.
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![live, other],
        "the takeover probe of A covers A's remembered entry: A is not probed twice"
    );
    drop((frames_a, frames_b));
}

/// Seventh review (#367): a deferred Publish overwritten by a newer one of
/// a third stream is remembered, not dropped.
#[tokio::test(start_paused = true)]
async fn an_overwritten_deferred_publish_is_remembered() {
    let _wd = watchdog("an_overwritten_deferred_publish_is_remembered");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");
    let third = identifier_named("third-c");

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    for deferred in [&other, &third] {
        event_tx
            .send(BroadcastEvent::Publish {
                identifier: deferred.clone(),
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A's publisher closes (seen ending: not remembered): C is taken over,
    // then the overwritten B is probed.
    drop(frames_a);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        vec![third, other],
        "the newest deferred Publish first, then the one it overwrote"
    );
}

/// Eighth review (#367): an UnPublish (streamhub 0.2.4 never sends one; the
/// manifest allows any 0.2.x) ends only the session of the stream it names.
/// An UnPublish of ANOTHER stream must leave the live one alone.
#[tokio::test(start_paused = true)]
async fn an_unpublish_of_another_stream_leaves_the_live_one_alone() {
    let _wd = watchdog("an_unpublish_of_another_stream_leaves_the_live_one_alone");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    event_tx
        .send(BroadcastEvent::UnPublish {
            identifier: other.clone(),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        state.is_connected(),
        "an UnPublish of another stream must not end the live session"
    );

    event_tx
        .send(BroadcastEvent::UnPublish {
            identifier: live.clone(),
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !state.is_connected(),
        "an UnPublish of the live stream ends its session"
    );
    drop(frames_a);
}

/// Eighth review (#367): an UnPublish of a DEFERRED stream drops the
/// deferral: once the live stream ends, nothing is taken over.
#[tokio::test(start_paused = true)]
async fn an_unpublish_drops_a_matching_deferred_publish() {
    let _wd = watchdog("an_unpublish_drops_a_matching_deferred_publish");
    let state = InpointState::new();
    let (event_tx, mut requests) = manual_receiver(state.clone());
    let live = identifier_named("live-a");
    let other = identifier_named("other-b");

    let frames_a = stream(&event_tx, &mut requests, &live).await;
    for event in [
        BroadcastEvent::Publish {
            identifier: other.clone(),
        },
        BroadcastEvent::UnPublish {
            identifier: other.clone(),
        },
    ] {
        event_tx.send(event).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    drop(frames_a);
    assert_eq!(
        rejected_probes(&mut requests, Duration::from_secs(60)).await,
        Vec::<StreamIdentifier>::new(),
        "an unpublished deferred stream is not taken over"
    );
}
