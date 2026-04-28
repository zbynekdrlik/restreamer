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
/// `body` holds the raw FLV tag body bytes — the bytes AFTER the 11-byte tag
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

/// Spin up a real xiu RTMP server and a companion subscription task that
/// captures every audio/video frame the publisher sends via the streamhub
/// `FrameData` channel.
///
/// Using the real xiu `RtmpServer` + streamhub subscribe avoids the protocol
/// fragility of a hand-rolled minimal server and eliminates the race condition
/// between subscriber registration and the first media frames: we wait for the
/// `BroadcastEvent::Publish` signal before attempting to subscribe, and retry
/// until the hub registers the stream.
///
/// Returns:
/// - `url`      — `rtmp://127.0.0.1:<port>/live/test` for the pusher
/// - `recorded` — shared accumulator; drained by the subscriber task
/// - `_server`  — `JoinHandle` keeping the server + hub alive
async fn spawn_recording_xiu_server() -> (
    String,
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
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

    // Two independent senders: one for the RtmpServer, one for the subscriber task.
    let event_sender_for_server = hub.get_hub_event_sender();
    let event_sender_for_sub = hub.get_hub_event_sender();

    // Broadcast receiver fires on Publish / UnPublish events.
    let mut broadcast_rx = hub.get_client_event_consumer();

    let address = addr.to_string();
    let url = format!("rtmp://{}/live/test", addr);

    let recorded_for_sub = Arc::clone(&recorded);

    let handle = tokio::spawn(async move {
        let mut rtmp_server =
            rtmp::rtmp::RtmpServer::new(address, event_sender_for_server, 0, None);

        // Spawn the subscriber task inside the same Tokio task group so it
        // shares the same runtime as hub.run() and rtmp_server.run().
        let recorded_clone = Arc::clone(&recorded_for_sub);
        tokio::spawn(async move {
            // Block until the xiu server emits BroadcastEvent::Publish.
            // This guarantees the stream is registered in the hub before we try
            // to subscribe (no retry needed for the Publish race).
            let identifier = loop {
                match broadcast_rx.recv().await {
                    Ok(BroadcastEvent::Publish { identifier }) => break identifier,
                    Ok(_) => continue,
                    Err(_) => return, // broadcast channel closed — server gone
                }
            };

            // Subscribe to the stream via the hub event channel.  The hub
            // processes Subscribe synchronously in its event loop; because we
            // already received Publish the stream is registered so the first
            // attempt should succeed.  Retry with a short delay in case the hub
            // event loop hasn't ticked yet.
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
                    // Hub event channel closed.
                    return;
                }

                match tokio::time::timeout(Duration::from_millis(500), result_rx).await {
                    Ok(Ok(Ok((data_receiver, _stat)))) => {
                        if let Some(rx) = data_receiver.frame_receiver {
                            break rx;
                        }
                        // No frame receiver — hub sent a packet-only receiver; retry.
                    }
                    _ => {
                        // Timeout or error — retry after a brief pause.
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            };

            // Drain FrameData into the recorded accumulator until the publisher
            // disconnects and the channel closes.
            while let Some(frame) = frame_rx.recv().await {
                match frame {
                    FrameData::Audio { timestamp, data } => {
                        recorded_clone.lock().await.push(RecordedTag {
                            tag_type: 8,
                            timestamp_ms: timestamp,
                            body: data.to_vec(),
                        });
                    }
                    FrameData::Video { timestamp, data } => {
                        recorded_clone.lock().await.push(RecordedTag {
                            tag_type: 9,
                            timestamp_ms: timestamp,
                            body: data.to_vec(),
                        });
                    }
                    _ => {} // skip MetaData, MediaInfo
                }
            }
        });

        // Run hub and server concurrently; either finishing ends the task.
        tokio::select! {
            _ = hub.run() => {}
            result = rtmp_server.run() => {
                if let Err(e) = result {
                    log::debug!("xiu recording test server stopped: {e}");
                }
            }
        }
    });

    (url, recorded, handle)
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
/// The test uses a real xiu `RtmpServer` + streamhub subscription to capture
/// `FrameData` frames.  The subscriber task waits for `BroadcastEvent::Publish`
/// before subscribing, eliminating the race between subscriber registration and
/// the first media frames.
#[tokio::test]
async fn media_payload_byte_identical_to_source() {
    let source_bytes = std::fs::read("tests/data/short.flv").expect("read short.flv");

    let (url, recorded, _server) = spawn_recording_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    pusher
        .push_flv_bytes(&source_bytes)
        .await
        .expect("push_flv_bytes");

    // Give the subscriber task time to drain the last frames from the channel.
    // The channel closes once the pusher drops the session (TCP disconnect),
    // which happens when `pusher` is dropped at end of scope.  We wait here
    // while the pusher is still alive, then allow the drain to complete.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let recorded_guard = recorded.lock().await;
    assert!(!recorded_guard.is_empty(), "no tags reached the server");

    let (src_audio_sha, src_video_sha) = sha256_flv_bodies(&source_bytes);
    let (rec_audio_sha, rec_video_sha) = sha256_recorded_bodies(&recorded_guard);

    assert_eq!(rec_audio_sha, src_audio_sha, "audio body bytes diverged");
    assert_eq!(rec_video_sha, src_video_sha, "video body bytes diverged");
}
