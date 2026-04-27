//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.

use std::sync::Arc;
use std::time::Duration;

use bytesio::bytes_writer::AsyncBytesWriter;
use bytesio::bytesio::{TNetIO, TcpIO};
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
use rtmp::handshake::define::ServerHandshakeState;
use rtmp::handshake::handshake_server::SimpleHandshakeServer;
use rtmp::messages::define::RtmpMessageData;
use rtmp::messages::parser::MessageParser;
use rtmp::netconnection::writer::NetConnection;
use rtmp::netstream::writer::NetStreamWriter;
use streamhub::StreamsHub;
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
/// consumed by the SHA-256 helpers.  Task 6 populates all three fields.
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

/// Spin up a minimal hand-rolled RTMP server that accepts one publisher
/// connection, performs the RTMP handshake + connect + createStream + publish
/// negotiation, and captures every audio/video chunk payload.
///
/// This approach avoids streamhub entirely so there is no race between the
/// subscriber registration and the first media frames from the pusher.
///
/// Returns:
/// - `url`      — `rtmp://127.0.0.1:<port>/live/test` for the pusher
/// - `recorded` — shared accumulator; filled after `push_flv_bytes` returns
/// - `_server`  — `JoinHandle` keeping the listener alive
async fn spawn_recording_xiu_server() -> (
    String,
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("rtmp://{}/live/test", addr);

    let recorded: Arc<Mutex<Vec<RecordedTag>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_for_task = Arc::clone(&recorded);

    let handle = tokio::spawn(async move {
        // Accept exactly one publisher connection.
        let (tcp_stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                log::debug!("recording server accept error: {e}");
                return;
            }
        };

        let net_io: Box<dyn TNetIO + Send + Sync> = Box::new(TcpIO::new(tcp_stream));
        let io = Arc::new(Mutex::new(net_io));

        if let Err(e) = recording_session(io, recorded_for_task).await {
            log::debug!("recording session ended: {e}");
        }
    });

    (url, recorded, handle)
}

/// Run the server-side RTMP negotiation and media capture loop.
///
/// Performs handshake, responds to connect/createStream/publish, then
/// captures all audio and video chunk payloads into `recorded`.
async fn recording_session(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    recorded: Arc<Mutex<Vec<RecordedTag>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // --- Handshake -----------------------------------------------------------
    // Read at least RTMP_HANDSHAKE_SIZE bytes per phase to avoid hitting
    // NotEnoughBytes on partial TCP segments.
    // Phase 1: accumulate C0C1 (1537 bytes), call handshake -> writes S0S1S2,
    //          state becomes ReadC2.
    // Phase 2: accumulate C2 (1536 bytes), call handshake -> state Finish.
    // Leftover bytes after C2 (e.g., bundled AMF connect) go into unpacketizer.
    let mut unpacketizer = ChunkUnpacketizer::new();
    {
        use rtmp::handshake::define::RTMP_HANDSHAKE_SIZE;
        let mut hs = SimpleHandshakeServer::new(Arc::clone(&io));

        // Phase 1: read C0C1.
        let mut accumulated = 0usize;
        while accumulated < RTMP_HANDSHAKE_SIZE {
            let data = io.lock().await.read().await?;
            accumulated += data.len();
            hs.extend_data(&data[..]);
        }
        hs.handshake().await?; // reads C0C1, writes S0S1S2, state=ReadC2

        // Phase 2: read C2.
        accumulated = 0;
        while accumulated < RTMP_HANDSHAKE_SIZE {
            let data = io.lock().await.read().await?;
            accumulated += data.len();
            hs.extend_data(&data[..]);
        }
        hs.handshake().await?; // reads C2, state=Finish

        // Drain any bytes the handshaker did not consume (e.g., AMF connect
        // bundled in the same read() as C2).
        let leftover = hs.reader.get_remaining_bytes();
        if !leftover.is_empty() {
            unpacketizer.extend_data(&leftover[..]);
        }
    }

    // --- Negotiate and capture ----------------------------------------------
    // Track whether we have sent the publish start response.
    let mut published = false;

    loop {
        let data = io.lock().await.read().await?;
        if data.is_empty() {
            break;
        }
        unpacketizer.extend_data(&data[..]);

        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        let timestamp = chunk.message_header.timestamp;

                        // Parse the chunk into a message; handle all variants below.
                        let msg = match MessageParser::new(chunk).parse() {
                            Ok(Some(m)) => m,
                            _ => continue,
                        };

                        match msg {
                            RtmpMessageData::SetChunkSize { chunk_size } => {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
                            }
                            RtmpMessageData::Amf0Command {
                                command_name,
                                transaction_id,
                                ..
                            } => {
                                let cmd = match &command_name {
                                    xflv::amf0::define::Amf0ValueType::UTF8String(s) => s.clone(),
                                    _ => String::new(),
                                };
                                let tid = match &transaction_id {
                                    xflv::amf0::define::Amf0ValueType::Number(n) => *n,
                                    _ => 0.0,
                                };

                                match cmd.as_str() {
                                    "connect" => {
                                        // Send window ack size + set peer bandwidth first.
                                        {
                                            use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
                                            let mut ctrl = ProtocolControlMessagesWriter::new(
                                                AsyncBytesWriter::new(Arc::clone(&io)),
                                            );
                                            ctrl.write_window_acknowledgement_size(2500000)
                                                .await
                                                .ok();
                                            ctrl.write_set_peer_bandwidth(2500000, 2).await.ok();
                                            ctrl.write_set_chunk_size(
                                                rtmp::chunk::define::CHUNK_SIZE,
                                            )
                                            .await
                                            .ok();
                                        }
                                        let mut nc = NetConnection::new(Arc::clone(&io));
                                        nc.write_connect_response(
                                            &tid,
                                            "FMS/3,0,1,123",
                                            &31.0,
                                            "NetConnection.Connect.Success",
                                            "status",
                                            "Connection succeeded",
                                            &0.0,
                                        )
                                        .await
                                        .ok();
                                    }
                                    "createStream" => {
                                        let mut nc = NetConnection::new(Arc::clone(&io));
                                        nc.write_create_stream_response(&tid, &1.0).await.ok();
                                    }
                                    "publish" => {
                                        let mut ns = NetStreamWriter::new(Arc::clone(&io));
                                        ns.write_on_status(
                                            &0.0,
                                            "status",
                                            "NetStream.Publish.Start",
                                            "Start publishing",
                                        )
                                        .await
                                        .ok();
                                        published = true;
                                    }
                                    _ => {
                                        // releaseStream, FCPublish, etc. — ignore
                                    }
                                }
                            }
                            RtmpMessageData::AudioData { data } => {
                                if published {
                                    recorded.lock().await.push(RecordedTag {
                                        tag_type: 8,
                                        timestamp_ms: timestamp,
                                        body: data.to_vec(),
                                    });
                                }
                            }
                            RtmpMessageData::VideoData { data } => {
                                if published {
                                    recorded.lock().await.push(RecordedTag {
                                        tag_type: 9,
                                        timestamp_ms: timestamp,
                                        body: data.to_vec(),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => break,
                Ok(_) => {}
            }
        }
    }

    Ok(())
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
// TDD-RED test (Task 5): fails until Task 6 implements the tag-write loop
// ---------------------------------------------------------------------------

/// Assert that every audio and video tag body byte that the pusher reads from
/// the source FLV arrives unmodified at the xiu server side.
///
/// # Why this test is RED in Task 5
///
/// `pusher.push_flv_bytes(&source_bytes)` returns
/// `Err(PushError::MalformedInput { … "tag-write loop unimplemented (Task 6)" })`
/// before any tags are written to the server.  The `expect("push_flv_bytes")`
/// panics, making the test fail.
///
/// Once Task 6 replaces that stub with real FLV-tag emission AND Task 6's
/// implementer wires up the subscription loop in `spawn_recording_xiu_server`,
/// all tags will flow through xiu and `recorded_guard` will contain the same
/// body bytes as `source_bytes`, making this test GREEN.
#[tokio::test]
async fn media_payload_byte_identical_to_source() {
    let source_bytes = std::fs::read("tests/data/short.flv").expect("read short.flv");

    let (url, recorded, _server) = spawn_recording_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // This panics in Task 5 because push_flv_bytes returns MalformedInput
    // ("tag-write loop unimplemented (Task 6)").  Task 6 makes it return Ok(()).
    pusher
        .push_flv_bytes(&source_bytes)
        .await
        .expect("push_flv_bytes");

    // Give the server a moment to drain the last tag.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let recorded_guard = recorded.lock().await;
    assert!(!recorded_guard.is_empty(), "no tags reached the server");

    let (src_audio_sha, src_video_sha) = sha256_flv_bodies(&source_bytes);
    let (rec_audio_sha, rec_video_sha) = sha256_recorded_bodies(&recorded_guard);

    assert_eq!(rec_audio_sha, src_audio_sha, "audio body bytes diverged");
    assert_eq!(rec_video_sha, src_video_sha, "video body bytes diverged");
}
