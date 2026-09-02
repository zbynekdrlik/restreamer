//! Shared test helpers for `local_xiu_loopback` integration tests.
//!
//! Contains server harnesses, SHA-256 helpers, and FLV generators used by
//! the tests in `local_xiu_loopback.rs`.

// Helpers in this module are shared across multiple test binaries
// (local_xiu_loopback, local_tls_loopback, fb_mock_server). Each binary
// only uses a subset, so unused-per-binary dead_code warnings are
// expected and not actionable.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rtmp::session::server_session::ServerSession;
use streamhub::StreamsHub;
use streamhub::define::{
    BroadcastEvent, FrameData, NotifyInfo, StreamHubEvent, StreamHubEventSender, SubDataType,
    SubscribeType, SubscriberInfo,
};
use streamhub::utils::{RandomDigitCount, Uuid};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Loopback port reservation + shared xiu accept loop (issue #148)
// ---------------------------------------------------------------------------

/// Reserve a loopback ephemeral port by binding `127.0.0.1:0` and returning the
/// listener HELD, together with the port it owns.
///
/// The returned listener owns the port continuously — there is NO
/// pick-then-release-then-rebind window a concurrent `bind(0)` could steal
/// (#148). Callers move the listener straight into [`run_xiu_accept_loop`]: the
/// socket that reserves the port is the socket that accepts, so two servers can
/// never be handed the same live port and no stray client can reach a port
/// whose intended server failed to bind. This removed the `spawn_recording_xiu_server_tls`
/// self cross-wire (the plain server's freed port being handed to the test's
/// own TLS bridge listener → `InvalidContentType`).
pub async fn reserved_loopback_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback ephemeral listener");
    let port = listener.local_addr().expect("local_addr").port();
    (listener, port)
}

/// Accept RTMP connections on a HELD listener and drive each connection with
/// xiu's `ServerSession`.
///
/// This replaces `rtmp::rtmp::RtmpServer::run()` — which binds by address
/// string and cannot adopt an existing socket — so the harness can keep the
/// reserved listener from the moment of port discovery (no close→rebind TOCTOU,
/// #148). It mirrors xiu's own accept loop exactly: one `ServerSession::new(
/// stream, sender, gop_num, None)` spawned per accepted connection. Returns
/// (ending the server task) when `accept()` errors, matching xiu's behaviour.
async fn run_xiu_accept_loop(
    listener: TcpListener,
    event_sender: StreamHubEventSender,
    gop_num: usize,
) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                log::debug!("xiu test accept loop stopped: {e}");
                break;
            }
        };
        let mut session = ServerSession::new(stream, event_sender.clone(), gop_num, None);
        tokio::spawn(async move {
            if let Err(e) = session.run().await {
                log::debug!("xiu test session ended: {e}");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// RecordedTag
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
pub struct RecordedTag {
    /// FLV tag type byte: 8 = audio, 9 = video.
    pub tag_type: u8,
    /// Composition timestamp in milliseconds from the FLV tag header.
    pub timestamp_ms: u32,
    /// Tag body bytes (after the 11-byte header, before PreviousTagSize).
    pub body: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Simple xiu server (no recording subscriber)
// ---------------------------------------------------------------------------

/// Spin up a real xiu RTMP server bound to `127.0.0.1:0`.
///
/// Returns the `rtmp://` URL the pusher should connect to (stream key
/// `live/test`) and a `JoinHandle` that keeps the server alive for the
/// duration of the test.
///
/// Port-discovery strategy (#148): reserve the ephemeral port with a HELD
/// listener ([`reserved_loopback_listener`]) and hand that exact socket to the
/// accept loop ([`run_xiu_accept_loop`], over xiu's `ServerSession`). The
/// socket that reserves the port is the socket that accepts, so there is no
/// close→rebind window and the port cannot be stolen by a concurrent `bind(0)`.
pub async fn spawn_xiu_server() -> (String, tokio::task::JoinHandle<()>) {
    // Reserve the ephemeral port with a held listener (no TOCTOU window).
    let (listener, port) = reserved_loopback_listener().await;

    // Build the StreamsHub.
    //
    // set_rtmp_push_enabled(true) is required so that the hub emits
    // BroadcastEvent::Publish when the client connects - without this the
    // hub's broadcast channel stays silent and the ClientSession on the
    // server side does not advance past WaitStateChange.
    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let event_sender = hub.get_hub_event_sender();

    let handle = tokio::spawn(async move {
        // Run hub and accept loop concurrently; either finishing ends the task.
        tokio::select! {
            _ = hub.run() => {}
            _ = run_xiu_accept_loop(listener, event_sender, 0) => {}
        }
    });

    let url = format!("rtmp://127.0.0.1:{port}/live/test");
    (url, handle)
}

// ---------------------------------------------------------------------------
// Recording server helper
// ---------------------------------------------------------------------------

/// Core recording-server harness: adopt a HELD listener, run a real xiu RTMP
/// server (via [`run_xiu_accept_loop`]) over it, and register a streamhub
/// subscriber that captures every audio/video frame the publisher sends.
///
/// Taking an already-bound `listener` is what removes the #148 TOCTOU: the
/// port is reserved by the exact socket that accepts, so there is no
/// pick-then-release window. `spawn_recording_xiu_server` (fresh ephemeral
/// port) and `spawn_recording_xiu_server_at` (caller-supplied addr) are thin
/// wrappers over this core.
///
/// The subscriber task blocks the returned `sub_ready_rx` until it holds a
/// frame receiver, so no media frames are lost to a subscriber-registration
/// race.
///
/// Returns:
/// - `recorded`     -- shared accumulator; filled by the subscriber task
/// - `_server`      -- `JoinHandle` keeping the server + hub alive
/// - `sub_ready_rx` -- fires when the subscriber has obtained its frame receiver
pub async fn spawn_recording_xiu_server_on(
    listener: TcpListener,
) -> (
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let recorded: Arc<Mutex<Vec<RecordedTag>>> = Arc::new(Mutex::new(Vec::new()));

    // Build hub with rtmp_push_enabled so BroadcastEvent::Publish fires when
    // the pusher's publish command is accepted.
    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);

    // Dedicated event senders (obtained before hub.run()): one for the accept
    // loop's ServerSessions, one for the subscriber task.
    let event_sender_for_server = hub.get_hub_event_sender();
    let event_sender_for_sub = hub.get_hub_event_sender();

    // Broadcast receiver: fires when the publisher connects (Publish event).
    // Must be obtained before hub.run() starts consuming events.
    let mut broadcast_rx = hub.get_client_event_consumer();

    // Oneshot used to signal that the subscriber has obtained its frame receiver
    // and is ready to accept media.  The caller awaits this before pushing data.
    let (sub_ready_tx, sub_ready_rx) = tokio::sync::oneshot::channel::<()>();

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

    // Server task: runs the accept loop (over the HELD listener) and hub
    // concurrently; either finishing ends the task.
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = hub.run() => {}
            _ = run_xiu_accept_loop(listener, event_sender_for_server, 0) => {}
        }
    });

    // The subscriber task and server task are now both running.  The server is
    // ready to accept TCP connections immediately; the subscriber task will
    // establish its frame channel once the pusher's publish command is accepted.
    // We do NOT wait for sub_ready_rx here -- that wait is done in the test
    // body AFTER push_flv_bytes initiates the publish handshake.

    (recorded, handle, sub_ready_rx)
}

/// Spin up a recording xiu RTMP server on a fresh ephemeral loopback port.
///
/// Reserves the port with a HELD listener (no TOCTOU window, #148) and delegates
/// to [`spawn_recording_xiu_server_on`].
///
/// Returns `(url, recorded, _server, sub_ready_rx)` where `url` is
/// `rtmp://127.0.0.1:<port>/live/test` for the pusher.
pub async fn spawn_recording_xiu_server() -> (
    String,
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (listener, port) = reserved_loopback_listener().await;
    let url = format!("rtmp://127.0.0.1:{port}/live/test");
    let (recorded, handle, sub_ready_rx) = spawn_recording_xiu_server_on(listener).await;
    (url, recorded, handle, sub_ready_rx)
}

// ---------------------------------------------------------------------------
// Recording server bound to a specific addr
// ---------------------------------------------------------------------------

/// Like `spawn_recording_xiu_server` but binds the server to `addr` instead of
/// discovering a fresh ephemeral port.  Used by the reconnect test to spin up a
/// replacement server on the exact same port after the first server is killed.
///
/// The listener is bound HERE and held (loud failure on bind error) before the
/// server task starts — the caller is responsible for ensuring the port is free
/// before calling (e.g. by aborting the previous server task and waiting briefly
/// so the OS releases the TCP listener). To spin the FIRST server up race-free,
/// reserve the port with [`reserved_loopback_listener`] and pass the held
/// listener to [`spawn_recording_xiu_server_on`] instead.
pub async fn spawn_recording_xiu_server_at(
    addr: std::net::SocketAddr,
) -> (
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("bind recording-at listener");
    spawn_recording_xiu_server_on(listener).await
}

// ---------------------------------------------------------------------------
// SHA-256 helpers
// ---------------------------------------------------------------------------

/// Walk `bytes` as raw FLV (header + tags) and feed every audio (type 8) and
/// video (type 9) tag body into separate SHA-256 digests.
///
/// Returns `(audio_hex, video_hex)`.  Script tags (type 18) are skipped.
pub fn sha256_flv_bodies(bytes: &[u8]) -> (String, String) {
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
pub fn sha256_recorded_bodies(recorded: &[RecordedTag]) -> (String, String) {
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
// Synthetic FLV generator
// ---------------------------------------------------------------------------

/// Build an in-memory FLV byte stream containing audio tags spaced 100 ms
/// apart from `ts_start` to `ts_end` (inclusive).
///
/// The FLV header and PreviousTagSize0 are prepended so `RtmpPusher::push_flv_bytes`
/// parses it correctly.  Each audio tag body is 4 bytes of synthetic payload.
pub fn synthetic_audio_flv(ts_start_ms: u32, ts_end_ms: u32) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();

    // FLV header: signature "FLV", version 1, flags 0x04 (audio only), header size 9
    out.extend_from_slice(&[b'F', b'L', b'V', 1, 0x04, 0, 0, 0, 9]);
    // PreviousTagSize0 = 0
    out.extend_from_slice(&[0u8; 4]);

    let mut ts = ts_start_ms;
    loop {
        // 4-byte synthetic body: two fixed bytes plus two TS-derived bytes so
        // each tag is unique and detectable in the recording.
        let body: [u8; 4] = [0xAB, 0xCD, (ts >> 8) as u8, ts as u8];
        let body_size = body.len() as u32;

        // Tag header (11 bytes):
        //   [0]     tag type = 8 (audio)
        //   [1..3]  data size (3 bytes, big-endian)
        //   [4..6]  timestamp lower 24 bits (big-endian)
        //   [7]     timestamp upper 8 bits (extended)
        //   [8..10] stream id = 0 (3 bytes)
        let ts_low = ts & 0x00FF_FFFF;
        let ts_high = (ts >> 24) as u8;

        out.push(8u8); // audio tag type
        out.push((body_size >> 16) as u8);
        out.push((body_size >> 8) as u8);
        out.push(body_size as u8);
        out.push((ts_low >> 16) as u8);
        out.push((ts_low >> 8) as u8);
        out.push(ts_low as u8);
        out.push(ts_high);
        out.extend_from_slice(&[0u8; 3]); // stream id

        out.extend_from_slice(&body);

        // PreviousTagSize = 11 (header) + body_size
        let prev_size = 11u32 + body_size;
        out.extend_from_slice(&prev_size.to_be_bytes());

        if ts >= ts_end_ms {
            break;
        }
        ts = ts.saturating_add(100);
        if ts > ts_end_ms {
            ts = ts_end_ms;
        }
    }

    out
}

// ---------------------------------------------------------------------------
// TLS bridge harness (spec §11.2)
// ---------------------------------------------------------------------------

/// Spawn the existing recording xiu RTMP server on plain TCP, then bind a TLS
/// listener that bridges decrypted bytes through to it via
/// `tokio::io::copy_bidirectional`. xiu's `ServerSession::new` accepts
/// `TcpStream` concretely (cannot consume `TlsStream`), so the bridge is the
/// simplest way to validate rtmps:// without forking xiu.
///
/// Returns:
/// - `rtmps_url`     -- `rtmps://127.0.0.1:<tls_port>/live/test`
/// - `recorded`      -- shared accumulator filled by the underlying recording subscriber
/// - `ca_cert_der`   -- the self-signed CA cert; the test client must trust it
/// - `_server`       -- `JoinHandle` keeping the plain xiu server + hub alive
/// - `sub_ready_rx`  -- fires when the recording subscriber has its frame receiver
pub async fn spawn_recording_xiu_server_tls() -> (
    String,
    std::sync::Arc<tokio::sync::Mutex<Vec<RecordedTag>>>,
    rcgen::CertifiedKey,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    // rustls 0.23 panics on `ServerConfig::builder()` unless a process-level
    // CryptoProvider is installed. Install ring idempotently before the build.
    rs_rtmp_push::tls::testing::ensure_default_crypto_provider();

    // 1. Spawn the plain xiu server we already have.
    let (plain_url, recorded, server_handle, sub_ready_rx) = spawn_recording_xiu_server().await;
    // plain_url = "rtmp://127.0.0.1:<plain_port>/live/test"
    let plain_authority = plain_url
        .strip_prefix("rtmp://")
        .and_then(|s| s.split('/').next())
        .expect("plain URL has authority");
    let plain_addr: std::net::SocketAddr = plain_authority
        .parse()
        .expect("plain authority parses as SocketAddr");

    // 2. Generate a self-signed cert valid for 127.0.0.1.
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
        .expect("rcgen self-signed cert");
    let cert_der: CertificateDer<'static> = certified.cert.der().clone();
    let key_pem: Vec<u8> = certified.key_pair.serialize_der();
    let key_der: PrivateKeyDer<'static> = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pem));

    // 3. Build the TLS acceptor.
    let server_cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("rustls server config");
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    // 4. Bind the TLS listener on an ephemeral port.
    let tls_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let tls_addr = tls_listener.local_addr().expect("tls local_addr");
    let rtmps_url = format!("rtmps://127.0.0.1:{}/live/test", tls_addr.port());

    // 5. Bridge task: TLS in, plain TCP out. One spawned task per connection.
    tokio::spawn(async move {
        loop {
            let (tcp, _) = match tls_listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let mut tls = match acceptor.accept(tcp).await {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("[bridge] tls accept error: {e}");
                        return;
                    }
                };
                let mut plain = match tokio::net::TcpStream::connect(plain_addr).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("[bridge] plain connect error: {e}");
                        return;
                    }
                };
                // `copy_bidirectional` propagates half-close: when one side
                // shuts down, the other side's copy task gets EOF and the
                // join completes. Two independent `copy` calls in `join!`
                // would leak per-connection tasks until both halves see EOF.
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
            });
        }
    });

    (rtmps_url, recorded, certified, server_handle, sub_ready_rx)
}
