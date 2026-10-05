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
