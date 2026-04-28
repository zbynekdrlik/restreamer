//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.

use std::sync::Arc;
use std::time::Duration;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use streamhub::StreamsHub;
use streamhub::define::{
    BroadcastEvent, FrameData, NotifyInfo, StreamHubEvent, SubDataType, SubscribeType,
    SubscriberInfo,
};
use streamhub::utils::{RandomDigitCount, Uuid};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Spin up a real xiu RTMP server bound to `127.0.0.1:0`.
///
/// Returns the `rtmp://` URL the pusher should connect to (stream key
/// `live/test`) and a `JoinHandle` that keeps the server alive for the
/// duration of the test.
///
/// Port-discovery strategy: bind a `TcpListener` to get an ephemeral port,
/// capture the address, then drop the listener so xiu can bind to the same
/// address.  There is a small TOCTOU window between the drop and xiu's
/// `TcpListener::bind`; on a loopback interface this is negligible.  If it
/// ever bites (see issue #148), set `RTMP_TEST_PORT` to a fixed port number
/// as an escape hatch.
async fn spawn_xiu_server() -> (String, tokio::task::JoinHandle<()>) {
    // Discover an available ephemeral port.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let a = listener.local_addr().expect("local_addr");
        drop(listener); // release so xiu can bind
        a
    };

    // Build the StreamsHub and xiu RtmpServer.
    //
    // set_rtmp_push_enabled(true) is required so that the hub emits
    // BroadcastEvent::Publish when the client connects - without this the
    // hub's broadcast channel stays silent and the ClientSession on the
    // server side does not advance past WaitStateChange.
    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let event_sender = hub.get_hub_event_sender();

    let address = addr.to_string();

    let handle = tokio::spawn(async move {
        let mut rtmp_server = rtmp::rtmp::RtmpServer::new(address, event_sender, 0, None);

        // Run hub and server concurrently; either finishing ends the task.
        tokio::select! {
            _ = hub.run() => {}
            result = rtmp_server.run() => {
                if let Err(e) = result {
                    log::debug!("xiu test server stopped: {e}");
                }
            }
        }
    });

    let url = format!("rtmp://{}/live/test", addr);
    (url, handle)
}

#[tokio::test]
async fn handshake_completes_with_local_xiu_server() {
    let (url, _server) = spawn_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // Empty bytes -> no media tags to send.  `push_flv_bytes` should still
    // do the lazy connect (handshake + NetConnection.connect + createStream
    // + publish) and return Ok if the server accepts the publish.
    let result = tokio::time::timeout(Duration::from_secs(5), pusher.push_flv_bytes(&[]))
        .await
        .expect("push_flv_bytes did not return within 5s");

    assert!(
        result.is_ok(),
        "expected push_flv_bytes(&[]) to complete handshake+publish; got {:?}",
        result
    );

    // After successful publish, the pusher reports zero reconnects and zero
    // output TS (no media has been sent yet).
    assert_eq!(pusher.reconnect_count(), 0);
    assert_eq!(pusher.last_output_ts_ms(), 0);
}

// ---------------------------------------------------------------------------
// Recording harness types
// ---------------------------------------------------------------------------

/// A single captured FLV tag as it arrived at the xiu server.
///
/// `body` holds the raw FLV tag body bytes -- the bytes AFTER the 11-byte tag
/// header and BEFORE the 4-byte PreviousTagSize trailer.  This is exactly the
/// payload that `sha256_flv_bodies` reads from the source file, so the two
/// SHA-256 digests can be compared directly.
///
/// `timestamp_ms` is included for debugging / future assertions; it is not
/// consumed by the SHA-256 helpers.
#[allow(dead_code)]
struct RecordedTag {
    /// FLV tag type byte: 8 = audio, 9 = video.
    tag_type: u8,
    /// Composition timestamp in milliseconds from the FLV tag header.
    timestamp_ms: u32,
    /// Tag body bytes (after the 11-byte header, before PreviousTagSize).
    body: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Recording server helper
// ---------------------------------------------------------------------------

/// Spin up a real xiu RTMP server and register a streamhub subscriber that
/// captures every audio/video frame the publisher sends.
///
/// The function blocks until the subscriber is fully registered with the hub
/// (i.e., the caller receives a frame receiver from the hub).  This guarantees
/// that when the caller invokes `push_flv_bytes`, the subscriber is already in
/// place and no media frames are lost to a subscriber-registration race.
///
/// Design:
///   1. Spawn the xiu `RtmpServer` + hub in a background task.
///   2. Obtain a `BroadcastEvent` receiver from the hub BEFORE `hub.run()`.
///   3. Obtain a dedicated `event_sender` for the subscriber.
///   4. In a second background task:
///      a. Wait for `BroadcastEvent::Publish` (signals the publisher's publish
///         command was accepted by xiu).
///      b. Send a `StreamHubEvent::Subscribe` and receive the frame channel.
///      c. Signal readiness back to the caller via a `oneshot`.
///      d. Drain `FrameData` frames into `recorded` until the channel closes.
///   5. Return to the caller only after the `oneshot` fires (subscriber ready).
///
/// Returns:
/// - `url`          -- `rtmp://127.0.0.1:<port>/live/test` for the pusher
/// - `recorded`     -- shared accumulator; filled by the subscriber task
/// - `_server`      -- `JoinHandle` keeping the server + hub alive
/// - `sub_ready_rx` -- fires when the subscriber has obtained its frame receiver
async fn spawn_recording_xiu_server() -> (
    String,
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    // Discover an available ephemeral port (same TOCTOU caveat as spawn_xiu_server).
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let a = listener.local_addr().expect("local_addr");
        drop(listener);
        a
    };

    let recorded: Arc<Mutex<Vec<RecordedTag>>> = Arc::new(Mutex::new(Vec::new()));

    // Build hub with rtmp_push_enabled so BroadcastEvent::Publish fires when
    // the pusher's publish command is accepted.
    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);

    // Dedicated event sender for the subscriber task (obtained before hub.run()).
    let event_sender_for_server = hub.get_hub_event_sender();
    let event_sender_for_sub = hub.get_hub_event_sender();

    // Broadcast receiver: fires when the publisher connects (Publish event).
    // Must be obtained before hub.run() starts consuming events.
    let mut broadcast_rx = hub.get_client_event_consumer();

    // Oneshot used to signal that the subscriber has obtained its frame receiver
    // and is ready to accept media.  The caller awaits this before pushing data.
    let (sub_ready_tx, sub_ready_rx) = tokio::sync::oneshot::channel::<()>();

    let address = addr.to_string();
    let url = format!("rtmp://{}/live/test", addr);

    let recorded_for_sub = Arc::clone(&recorded);

    // Subscriber task: waits for Publish, subscribes, signals ready, then drains.
    tokio::spawn(async move {
        // Step 1: wait for BroadcastEvent::Publish with a 10-second timeout.
        let identifier = loop {
            match tokio::time::timeout(Duration::from_secs(10), broadcast_rx.recv()).await {
                Ok(Ok(BroadcastEvent::Publish { identifier })) => {
                    eprintln!(
                        "[recorder] BroadcastEvent::Publish received for {:?}",
                        identifier
                    );
                    break identifier;
                }
                Ok(Ok(_)) => continue, // other broadcast events -- skip
                Ok(Err(_)) => {
                    eprintln!("[recorder] broadcast channel closed before Publish");
                    return;
                }
                Err(_) => {
                    eprintln!("[recorder] timed out waiting for BroadcastEvent::Publish");
                    return;
                }
            }
        };

        // Step 2: Subscribe to the stream via the hub event channel.
        // Retry briefly in case the hub event loop hasn't ticked yet.
        let mut frame_rx = loop {
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            let sub_info = SubscriberInfo {
                id: Uuid::new(RandomDigitCount::Six),
                sub_type: SubscribeType::RtmpPull,
                sub_data_type: SubDataType::Frame,
                notify_info: NotifyInfo {
                    request_url: String::new(),
                    remote_addr: String::from("test-recorder"),
                },
            };

            if event_sender_for_sub
                .send(StreamHubEvent::Subscribe {
                    identifier: identifier.clone(),
                    info: sub_info,
                    result_sender: result_tx,
                })
                .is_err()
            {
                eprintln!("[recorder] hub event channel closed; cannot subscribe");
                return;
            }

            match tokio::time::timeout(Duration::from_millis(500), result_rx).await {
                Ok(Ok(Ok((data_receiver, _stat)))) => {
                    if let Some(rx) = data_receiver.frame_receiver {
                        eprintln!("[recorder] subscribed successfully; frame receiver ready");
                        break rx;
                    }
                    // Hub returned a packet-only receiver -- retry.
                    eprintln!("[recorder] no frame_receiver in response; retrying...");
                }
                Ok(Ok(Err(e))) => {
                    eprintln!("[recorder] subscribe error: {e:?}; retrying...");
                }
                _ => {
                    eprintln!("[recorder] subscribe timeout or channel error; retrying...");
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // Step 3: Signal the caller that the subscriber is ready.  From this
        // point onward, any media the pusher sends will be captured.  If the
        // send fails the caller already timed out -- we still drain below.
        let _ = sub_ready_tx.send(());

        // Step 4: Drain FrameData into the recorded accumulator until the
        // publisher disconnects and the channel closes.
        while let Some(frame) = frame_rx.recv().await {
            match frame {
                FrameData::Audio { timestamp, data } => {
                    eprintln!("[recorder] audio frame ts={}", timestamp);
                    recorded_for_sub.lock().await.push(RecordedTag {
                        tag_type: 8,
                        timestamp_ms: timestamp,
                        body: data.to_vec(),
                    });
                }
                FrameData::Video { timestamp, data } => {
                    eprintln!("[recorder] video frame ts={}", timestamp);
                    recorded_for_sub.lock().await.push(RecordedTag {
                        tag_type: 9,
                        timestamp_ms: timestamp,
                        body: data.to_vec(),
                    });
                }
                _ => {} // skip MetaData, MediaInfo
            }
        }
        eprintln!("[recorder] frame channel closed; drain complete");
    });

    // Server task: runs xiu RtmpServer and hub concurrently.
    let handle = tokio::spawn(async move {
        let mut rtmp_server =
            rtmp::rtmp::RtmpServer::new(address, event_sender_for_server, 0, None);

        tokio::select! {
            _ = hub.run() => {}
            result = rtmp_server.run() => {
                if let Err(e) = result {
                    log::debug!("xiu recording test server stopped: {e}");
                }
            }
        }
    });

    // The subscriber task and server task are now both running.  The server is
    // ready to accept TCP connections immediately; the subscriber task will
    // establish its frame channel once the pusher's publish command is accepted.
    // We do NOT wait for sub_ready_rx here -- that wait is done in the test
    // body AFTER push_flv_bytes initiates the publish handshake.

    (url, recorded, handle, sub_ready_rx)
}

// ---------------------------------------------------------------------------
// SHA-256 helpers
// ---------------------------------------------------------------------------

/// Walk `bytes` as raw FLV (header + tags) and feed every audio (type 8) and
/// video (type 9) tag body into separate SHA-256 digests.
///
/// Returns `(audio_hex, video_hex)`.  Script tags (type 18) are skipped.
fn sha256_flv_bodies(bytes: &[u8]) -> (String, String) {
    use sha2::{Digest, Sha256};

    let mut audio = Sha256::new();
    let mut video = Sha256::new();

    // FLV header is 9 bytes; immediately followed by PreviousTagSize0 (4 bytes
    // = 0x00000000), so the first real tag starts at offset 13.
    let mut offset = 9 + 4;

    while offset + 11 <= bytes.len() {
        let tag_type = bytes[offset];
        let data_size = ((bytes[offset + 1] as usize) << 16)
            | ((bytes[offset + 2] as usize) << 8)
            | (bytes[offset + 3] as usize);

        let body_start = offset + 11;
        let body_end = body_start + data_size;
        if body_end > bytes.len() {
            break;
        }

        match tag_type {
            8 => audio.update(&bytes[body_start..body_end]),
            9 => video.update(&bytes[body_start..body_end]),
            _ => {} // skip script / metadata tags
        }

        // Advance past body + 4-byte PreviousTagSize.
        offset = body_end + 4;
    }

    (
        format!("{:x}", audio.finalize()),
        format!("{:x}", video.finalize()),
    )
}

/// Feed every `RecordedTag` body from the server-side capture into separate
/// SHA-256 digests and return `(audio_hex, video_hex)`.
fn sha256_recorded_bodies(recorded: &[RecordedTag]) -> (String, String) {
    use sha2::{Digest, Sha256};

    let mut audio = Sha256::new();
    let mut video = Sha256::new();

    for tag in recorded {
        match tag.tag_type {
            8 => audio.update(&tag.body),
            9 => video.update(&tag.body),
            _ => {}
        }
    }

    (
        format!("{:x}", audio.finalize()),
        format!("{:x}", video.finalize()),
    )
}

// ---------------------------------------------------------------------------
// Byte-identity test: every audio/video tag body must arrive unmodified
// ---------------------------------------------------------------------------

/// Assert that every audio and video tag body byte that the pusher reads from
/// the source FLV arrives unmodified at the xiu server side.
///
/// Timing contract (no race conditions):
///   1. `spawn_recording_xiu_server` starts both the xiu server task and the
///      subscriber task.  The subscriber task blocks on `BroadcastEvent::Publish`.
///   2. The test starts the pusher.  The pusher performs the full RTMP negotiate
///      sequence (handshake + connect + createStream + publish).  When the
///      server's `onStatus(NetStream.Publish.Start)` is sent, xiu internally
///      fires `BroadcastEvent::Publish` on the hub's broadcast channel.
///   3. The subscriber task receives `BroadcastEvent::Publish`, immediately sends
///      a `StreamHubEvent::Subscribe`, gets the frame receiver, and signals the
///      test via `sub_ready_rx`.
///   4. The test awaits `sub_ready_rx` (5-second timeout) before sending any
///      media tags.  By the time the first audio/video chunk is written, the
///      subscriber is already registered in the hub -- no frames are dropped.
///   5. After `push_flv_bytes` returns, we wait 2 seconds for the subscriber
///      task to drain any in-flight frames, then compare SHA-256 digests.
#[tokio::test]
async fn media_payload_byte_identical_to_source() {
    let source_bytes = std::fs::read("tests/data/short.flv").expect("read short.flv");

    let (url, recorded, _server, sub_ready_rx) = spawn_recording_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // Step 1: perform the RTMP negotiate (handshake + connect + publish).
    // This triggers BroadcastEvent::Publish inside xiu, which the subscriber
    // task is waiting for.  We call push_flv_bytes with an empty slice here
    // so the publish handshake completes but no media is sent yet.
    //
    // NOTE: push_flv_bytes with an empty slice parses zero tags and returns
    // immediately after the lazy connect -- it does NOT send any media chunks.
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake must succeed");

    eprintln!("[test] publish handshake complete; waiting for subscriber ready signal...");

    // Step 2: wait for the subscriber task to confirm it has obtained the
    // frame receiver from the hub.  This eliminates the race between xiu's
    // BroadcastEvent::Publish and our Subscribe request.
    tokio::time::timeout(Duration::from_secs(5), sub_ready_rx)
        .await
        .expect("subscriber task did not signal ready within 5s")
        .expect("sub_ready channel dropped before signal");

    eprintln!("[test] subscriber ready; sending media...");

    // Step 3: now send the actual media payload.  The subscriber is registered
    // so no frames will be dropped.
    pusher
        .push_flv_bytes(&source_bytes)
        .await
        .expect("push_flv_bytes");

    eprintln!("[test] push_flv_bytes complete; waiting for drain...");

    // Give the subscriber task time to drain the last frames from the channel.
    // The channel closes once the pusher drops the session (TCP disconnect),
    // which happens when `pusher` is dropped at end of scope.  We wait here
    // while the pusher is still alive, then allow the drain to complete.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let recorded_guard = recorded.lock().await;
    eprintln!("[test] recorded {} tags", recorded_guard.len());
    assert!(
        !recorded_guard.is_empty(),
        "no tags reached the server; check eprintln output above"
    );

    let (src_audio_sha, src_video_sha) = sha256_flv_bodies(&source_bytes);
    let (rec_audio_sha, rec_video_sha) = sha256_recorded_bodies(&recorded_guard);

    assert_eq!(rec_audio_sha, src_audio_sha, "audio body bytes diverged");
    assert_eq!(rec_video_sha, src_video_sha, "video body bytes diverged");
}
