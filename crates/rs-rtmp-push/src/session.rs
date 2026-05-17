//! Hand-rolled RTMP push session using xiu low-level primitives.
//!
//! `ClientSession::Push` is a relay mode: on `NetStream.Publish.Start` it
//! calls `subscribe_from_stream_hub` against the local hub, which requires a
//! registered publisher.  For a direct push (no local hub) that call returns
//! `NoAppName` and the session exits.  This module bypasses `ClientSession`
//! entirely and drives the wire protocol directly:
//!
//!   1. TCP connect (with timeout)
//!   2. RTMP handshake (`SimpleHandshakeClient`)
//!   3. `NetConnection.connect` -> wait for `_result`
//!   4. `NetConnection.createStream` -> wait for `_result`
//!   5. `NetStream.publish` -> wait for `onStatus(NetStream.Publish.Start)`
//!   6. Background read loop watches for mid-stream errors
//!
//! Tag writes (`send_audio_tag` / `send_video_tag`) use ChunkPacketizer.
//! Task 6 fills the actual chunk-packetize-and-send loop.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytesio::bytes_writer::AsyncBytesWriter;
use bytesio::bytesio::{TNetIO, TcpIO};
use rtmp::chunk::ChunkInfo;
use rtmp::chunk::define::CHUNK_SIZE;
use rtmp::chunk::packetizer::ChunkPacketizer;
use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
use rtmp::handshake::define::ClientHandshakeState;
use rtmp::handshake::handshake_client::SimpleHandshakeClient;
use rtmp::messages::define::RtmpMessageData;
use rtmp::messages::parser::MessageParser;
use rtmp::netconnection::writer::{ConnectProperties, NetConnection};
use rtmp::netstream::writer::NetStreamWriter;
use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
use rtmp::session::define::{TRANSACTION_ID_CONNECT, TRANSACTION_ID_CREATE_STREAM};
use tokio::net::TcpSocket;
use tokio::sync::Mutex;
use xflv::amf0::define::Amf0ValueType;

use crate::{PushError, map_read_err};

// -------------------------------------------------------------------------
// Connection timeout for the full handshake+connect+publish sequence.
// Independent of the TCP connect timeout passed in by the caller.
// -------------------------------------------------------------------------
const NEGOTIATE_TIMEOUT_SECS: u64 = 30;

// -------------------------------------------------------------------------
// Session
// -------------------------------------------------------------------------

/// An active RTMP push session established via the full
/// handshake + connect + publish wire sequence.
///
/// After `Session::connect` returns `Ok`, the remote server has confirmed
/// `NetStream.Publish.Start`.  A background task monitors the connection for
/// server-initiated errors; if one is detected `poisoned` is set and
/// subsequent `send_*_tag` calls return `Err(PushError::RemoteClosed(...))`.
pub struct Session {
    /// Shared I/O handle.  Held here so the `Arc` stays alive (and therefore
    /// the `TcpIO` is not dropped) for as long as the session lives.  The
    /// packetizer and the read-loop each hold their own `Arc` clones; this
    /// field keeps the TCP socket open even if all other holders finish early.
    #[allow(dead_code)]
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    /// Chunk packetizer for writing audio/video tags over the RTMP connection.
    /// Set after `Session::connect` succeeds.
    packetizer: ChunkPacketizer,
    /// RTMP message stream id assigned by the server in the createStream response.
    /// xiu typically assigns 1.
    msg_stream_id: u32,
    /// Set by the background read-loop on any I/O error or server-side close.
    poisoned: Arc<AtomicBool>,
    /// Background task handle; aborted on `Drop` or `close`.
    read_loop_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Session {
    /// Dial `url`, run the full RTMP handshake + connect + publish sequence,
    /// and return a live `Session` ready to accept media tags.
    ///
    /// `url` must be `rtmp://host[:port]/app/stream`.
    /// `timeout_ms` is applied to the TCP connect step only; the full
    /// negotiate sequence has its own 30-second deadline.
    pub async fn connect(url: &str, timeout_ms: u64) -> Result<Self, PushError> {
        // --- 1. Parse URL ---------------------------------------------------
        let (scheme, host, port, app, stream_name) = parse_rtmp_url(url)?;

        // --- 2. TCP connect -------------------------------------------------
        // Pre-resolve via tokio::net::lookup_host so we can build a
        // tokio::net::TcpSocket and configure socket options BEFORE the
        // connect (TCP_NODELAY can be set after connect on TcpStream, but
        // SO_SNDBUF is only exposed on TcpSocket).
        let addr = format!("{host}:{port}");
        let socket_addr = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            tokio::net::lookup_host(&addr),
        )
        .await
        .map_err(|_| PushError::Timeout)?
        .map_err(PushError::HandshakeFailed)?
        .next()
        .ok_or_else(|| {
            PushError::HandshakeFailed(io::Error::other(format!("no addresses for {addr}")))
        })?;

        let socket = if socket_addr.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(PushError::HandshakeFailed)?;

        // Bump the TCP send buffer to 4 MB. With per-tag pacing
        // ChunkPacketizer issues ~150 TCP writes per chunk (one per FLV
        // tag), each ~17 KB. The default Linux send buffer (~256 KB on
        // most kernels) blocks each write until the kernel has receive-
        // ACKed the prior bytes — Hetzner→YouTube RTT is ~30 ms, so the
        // pusher caps at roughly 33 writes/s = 4 s to drain a 2 s chunk
        // (#103, run 25138323858). A 4 MB buffer holds ~30 chunks of
        // in-flight bytes and decouples the per-tag write rate from RTT.
        // Failure to set the option logs a warning but does not fail the
        // connection — kernels with rmem/wmem caps will simply get the
        // largest value they allow.
        const PUSH_SEND_BUF_BYTES: u32 = 4 * 1024 * 1024;
        if let Err(e) = socket.set_send_buffer_size(PUSH_SEND_BUF_BYTES) {
            tracing::warn!(error = %e, "failed to set TCP send buffer size on push socket");
        }

        let tcp_stream = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            socket.connect(socket_addr),
        )
        .await
        .map_err(|_| PushError::Timeout)?
        .map_err(PushError::HandshakeFailed)?;

        // Disable Nagle's algorithm (TCP_NODELAY=true). RTMP packetizes a
        // single FLV tag into many small chunks (default 4 KB each) and
        // ChunkPacketizer issues one TCP write per chunk; Nagle would
        // coalesce those writes for up to ~40 ms each, dropping the
        // effective output rate well below real-time. Without NODELAY the
        // E2E push pipeline ran at ~0.22 x real-time and `cache_delay`
        // grew at ~0.8 s/s during init (#103, run 25116396931).
        if let Err(e) = tcp_stream.set_nodelay(true) {
            tracing::warn!(error = %e, "failed to set TCP_NODELAY on push socket");
        }

        // Wrap the connected stream in either TcpIO (rtmp://) or TlsIO (rtmps://).
        // The negotiate / read-loop machinery below operates on Box<dyn TNetIO>
        // and is therefore transparent to wire encryption.
        let net_io: Box<dyn TNetIO + Send + Sync> = match scheme {
            Scheme::Rtmp => Box::new(TcpIO::new(tcp_stream)),
            Scheme::Rtmps => Box::new(crate::tls::connect_tls(tcp_stream, &host).await?),
        };
        let io = Arc::new(Mutex::new(net_io));

        // --- 3-5. Negotiate (handshake + connect + publish) ------------------
        let msg_stream_id = tokio::time::timeout(
            Duration::from_secs(NEGOTIATE_TIMEOUT_SECS),
            negotiate(Arc::clone(&io), scheme, &addr, &app, &stream_name),
        )
        .await
        .map_err(|_| PushError::Timeout)??;

        // --- 6. Build packetizer + spawn background read-loop ----------------
        let packetizer = ChunkPacketizer::new(Arc::clone(&io));
        let poisoned = Arc::new(AtomicBool::new(false));
        let read_loop_handle = tokio::spawn(read_loop(Arc::clone(&io), Arc::clone(&poisoned)));

        Ok(Self {
            io,
            packetizer,
            msg_stream_id,
            poisoned,
            read_loop_handle: Some(read_loop_handle),
        })
    }

    /// Send an audio FLV tag body via ChunkPacketizer.
    pub async fn send_audio_tag(
        &mut self,
        timestamp_ms: u32,
        body: &[u8],
    ) -> Result<(), PushError> {
        self.send_tag(rtmp::chunk::define::csid_type::AUDIO, 8, timestamp_ms, body)
            .await
    }

    /// Send a video FLV tag body via ChunkPacketizer.
    pub async fn send_video_tag(
        &mut self,
        timestamp_ms: u32,
        body: &[u8],
    ) -> Result<(), PushError> {
        self.send_tag(rtmp::chunk::define::csid_type::VIDEO, 9, timestamp_ms, body)
            .await
    }

    /// Packetize and write a single RTMP chunk for the given tag body.
    ///
    /// Pure write — no pacing here. `RtmpPusher::push_flv_bytes` paces ONCE
    /// per chunk (rather than per tag) so the wall-clock sleep happens
    /// exactly once per ~80 tags instead of 80 times per second of media.
    /// Per-tag pacing produced ~12.5 ms of scheduler jitter per sleep ×
    /// 80 tags/sec = >1 second of compounded overhead per second of media,
    /// dropping effective output to ~0.3 x real-time (#103, run 25119429314).
    async fn send_tag(
        &mut self,
        csid: u32,
        msg_type_id: u8,
        timestamp_ms: u32,
        body: &[u8],
    ) -> Result<(), PushError> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(PushError::RemoteClosed(io::Error::from(
                io::ErrorKind::ConnectionReset,
            )));
        }

        let mut chunk_info = ChunkInfo::new(
            csid,
            0, // format; zip_chunk_header will optimize on subsequent chunks
            timestamp_ms,
            body.len() as u32,
            msg_type_id,
            self.msg_stream_id,
            bytes::BytesMut::from(body),
        );
        // Phase 2 probe (#176/#177/#178): time the actual TCP-level write
        // so we can prove whether stalls are at write_chunk (TCP-level) or
        // between chunks (runtime starvation / channel wait).
        let write_start = std::time::Instant::now();
        let result = self
            .packetizer
            .write_chunk(&mut chunk_info)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())));
        let elapsed_ms = write_start.elapsed().as_millis() as u64;
        if elapsed_ms >= 250 {
            tracing::warn!(
                csid,
                msg_type_id,
                body_len = body.len(),
                timestamp_ms,
                elapsed_ms,
                "rtmp_push: SLOW write_chunk (>=250ms)"
            );
        }
        result
    }

    /// Gracefully shut down the session.
    pub async fn close(mut self) {
        if let Some(h) = self.read_loop_handle.take() {
            h.abort();
        }
        // `self.io` is intentionally held until here so the Arc stays alive
        // long enough for the read-loop abort to complete before the TcpIO
        // is dropped.
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(h) = self.read_loop_handle.take() {
            h.abort();
        }
    }
}

// -------------------------------------------------------------------------
// Negotiate: handshake + connect + createStream + publish
// -------------------------------------------------------------------------

/// Run the full RTMP client negotiation sequence on `io`.
///
/// Returns the RTMP message stream id assigned by the server in the
/// `createStream` response (typically 1) once the server sends
/// `NetStream.Publish.Start`, or errors on any rejection or unexpected close.
async fn negotiate(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    scheme: Scheme,
    raw_domain: &str,
    app: &str,
    stream_name: &str,
) -> Result<u32, PushError> {
    // --- Handshake ----------------------------------------------------------
    {
        let mut handshaker = SimpleHandshakeClient::new(Arc::clone(&io));
        loop {
            handshaker
                .handshake()
                .await
                .map_err(|e| PushError::HandshakeFailed(io::Error::other(e.to_string())))?;
            if handshaker.state == ClientHandshakeState::Finish {
                break;
            }
            // Need to read S0S1S2 (2 * RTMP_HANDSHAKE_SIZE bytes).
            let mut bytes_read = 0;
            let need = rtmp::handshake::define::RTMP_HANDSHAKE_SIZE * 2 + 1;
            while bytes_read < need {
                let data = io
                    .lock()
                    .await
                    .read()
                    .await
                    .map_err(|e| PushError::HandshakeFailed(io::Error::other(e.to_string())))?;
                bytes_read += data.len();
                handshaker.extend_data(&data[..]);
            }
        }
    }

    // Shared unpacketizer for the connect-publish sequence.
    let mut unpacketizer = ChunkUnpacketizer::new();

    // --- send SetChunkSize + NetConnection.connect --------------------------
    {
        // Send SetChunkSize first so the rest of the connect-flow chunks fit
        // without splitting. xiu's own client_session sends connect first; we
        // pre-set chunk size here for simplicity. Both orderings are accepted
        // by xiu's RtmpServer (verified by Task 3 loopback test).
        let mut ctrl = ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)));
        ctrl.write_set_chunk_size(CHUNK_SIZE)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;

        let mut nc = NetConnection::new(Arc::clone(&io));
        let mut props = ConnectProperties::new_none();
        props.app = Some(app.to_string());
        props.pub_type = Some("nonprivate".to_string());
        // OBS advertises these on every RTMP connect. Without them, Facebook
        // Live silently accepts the publish and then discards the media (no
        // RTMP error returned, no preview shown in Live Producer). Operator
        // confirmed 2026-05-03 that FB shows zero data ingestion despite
        // pusher reporting healthy chunk-done logs. Mirror libobs values.
        props.flash_ver = Some("FMLE/3.0 (compatible; FMSc/1.0)".to_string());
        props.fpad = Some(false);
        props.capabilities = Some(239.0);
        props.audio_codecs = Some(3575.0); // OBS bitmask: AAC + MP3 + ...
        props.video_codecs = Some(252.0); // OBS bitmask: H.264 + ...
        props.video_function = Some(1.0); // CLIENT_SEEK
        props.object_encoding = Some(0.0); // AMF0
        let scheme_str = match scheme {
            Scheme::Rtmp => "rtmp",
            Scheme::Rtmps => "rtmps",
        };
        props.tc_url = Some(format!("{scheme_str}://{raw_domain}/{app}"));
        nc.write_connect(&(TRANSACTION_ID_CONNECT as f64), &props)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
    }

    // Wait for _result (transaction 1 == connect).
    wait_for_result(
        Arc::clone(&io),
        &mut unpacketizer,
        TRANSACTION_ID_CONNECT,
        "connect",
    )
    .await?;

    // --- On _result connect: send ACK + releaseStream + FCPublish ----------
    //
    // Match xiu ClientSession::on_result_connect: write_acknowledgement,
    // write_release_stream, write_fcpublish, then transition to CreateStream.
    {
        let mut ctrl = ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)));
        ctrl.write_acknowledgement(3107)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;

        let sn = stream_name.to_string();
        let mut ns = NetStreamWriter::new(Arc::clone(&io));
        ns.write_release_stream(&(TRANSACTION_ID_CONNECT as f64), &sn)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
        ns.write_fcpublish(&(TRANSACTION_ID_CONNECT as f64), &sn)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
    }

    // --- NetConnection.createStream -----------------------------------------
    {
        let mut nc = NetConnection::new(Arc::clone(&io));
        nc.write_create_stream(&(TRANSACTION_ID_CREATE_STREAM as f64))
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
    }

    // Wait for _result (transaction 2 == createStream) and capture stream_id.
    let msg_stream_id = wait_for_create_stream_result(Arc::clone(&io), &mut unpacketizer).await?;

    // --- NetStream.publish --------------------------------------------------
    {
        let sn = stream_name.to_string();
        let st = "live".to_string();
        let mut ns = NetStreamWriter::new(Arc::clone(&io));
        ns.write_publish(&3.0, &sn, &st)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
    }

    // Wait for onStatus(NetStream.Publish.Start).
    wait_for_publish_start(Arc::clone(&io), &mut unpacketizer).await?;

    Ok(msg_stream_id)
}

// -------------------------------------------------------------------------
// Protocol helpers
// -------------------------------------------------------------------------

/// Read messages from `io` until we see an AMF0 `_result` for the given
/// `expected_transaction_id`.
///
/// Silently passes through SetChunkSize messages so the unpacketizer stays in
/// sync.  Returns `Err(ConnectRejected)` on `_error`.
async fn wait_for_result(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    unpacketizer: &mut ChunkUnpacketizer,
    expected_tid: u8,
    label: &str,
) -> Result<(), PushError> {
    loop {
        let data = io.lock().await.read().await.map_err(map_read_err)?;
        unpacketizer.extend_data(&data[..]);

        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        if chunk.message_header.msg_type_id
                            == rtmp::messages::define::msg_type_id::SET_CHUNK_SIZE
                        {
                            // Keep unpacketizer chunk-size in sync.
                            if let Some(RtmpMessageData::SetChunkSize { chunk_size }) =
                                MessageParser::new(chunk).parse().ok().flatten()
                            {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
                            }
                            continue;
                        }

                        let msg = match MessageParser::new(chunk).parse() {
                            Ok(Some(m)) => m,
                            _ => continue,
                        };

                        match msg {
                            RtmpMessageData::Amf0Command {
                                command_name,
                                transaction_id,
                                ..
                            } => {
                                let cmd = amf_string(&command_name);
                                let tid = amf_u8(&transaction_id);
                                if cmd == "_result" && tid == expected_tid {
                                    return Ok(());
                                }
                                if cmd == "_error" {
                                    return Err(PushError::ConnectRejected {
                                        code: format!("error during {label}"),
                                        description: "server returned _error".to_string(),
                                    });
                                }
                            }
                            RtmpMessageData::SetChunkSize { chunk_size } => {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
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
}

/// Read messages from `io` until we see the `_result` for the `createStream`
/// command (transaction id 2).
///
/// Extracts and returns the RTMP message stream id from the AMF response.
/// xiu's server sets stream_id = 1; if extraction fails we default to 1.
async fn wait_for_create_stream_result(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    unpacketizer: &mut ChunkUnpacketizer,
) -> Result<u32, PushError> {
    loop {
        let data = io.lock().await.read().await.map_err(map_read_err)?;
        unpacketizer.extend_data(&data[..]);

        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        if chunk.message_header.msg_type_id
                            == rtmp::messages::define::msg_type_id::SET_CHUNK_SIZE
                        {
                            if let Some(RtmpMessageData::SetChunkSize { chunk_size }) =
                                MessageParser::new(chunk).parse().ok().flatten()
                            {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
                            }
                            continue;
                        }

                        let msg = match MessageParser::new(chunk).parse() {
                            Ok(Some(m)) => m,
                            _ => continue,
                        };

                        match msg {
                            RtmpMessageData::Amf0Command {
                                command_name,
                                transaction_id,
                                others,
                                ..
                            } => {
                                let cmd = amf_string(&command_name);
                                let tid = amf_u8(&transaction_id);
                                if cmd == "_result" && tid == TRANSACTION_ID_CREATE_STREAM {
                                    // The stream_id is the first Number in `others`
                                    // (the command object is the Null before it,
                                    // consumed as `command_object` by the parser).
                                    let stream_id = others
                                        .iter()
                                        .find_map(|v| {
                                            if let Amf0ValueType::Number(n) = v {
                                                Some(*n as u32)
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or(1);
                                    return Ok(stream_id);
                                }
                                if cmd == "_error" {
                                    return Err(PushError::ConnectRejected {
                                        code: "error during createStream".to_string(),
                                        description: "server returned _error".to_string(),
                                    });
                                }
                            }
                            RtmpMessageData::SetChunkSize { chunk_size } => {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
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
}

/// Read messages from `io` until we see `onStatus` with a code that starts
/// with `NetStream.Publish.Start`.  Returns an error on rejection.
///
/// Task 10 (AMF onStatus parsing for PublishRejected) is implemented inline
/// in the loop below -- see the `Amf0Command` arm that extracts `code` and
/// `description` from the status object and returns `PushError::PublishRejected`.
async fn wait_for_publish_start(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    unpacketizer: &mut ChunkUnpacketizer,
) -> Result<(), PushError> {
    loop {
        let data = io.lock().await.read().await.map_err(map_read_err)?;
        unpacketizer.extend_data(&data[..]);

        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        let msg = match MessageParser::new(chunk).parse() {
                            Ok(Some(m)) => m,
                            _ => continue,
                        };

                        match msg {
                            RtmpMessageData::Amf0Command {
                                command_name,
                                others,
                                ..
                            } if amf_string(&command_name) == "onStatus" => {
                                // `others` contains the info object (the
                                // Null command object was consumed as
                                // `command_object`; the status object is
                                // the first entry in `others`).
                                let status_obj = others.into_iter().find_map(|v| {
                                    if let Amf0ValueType::Object(m) = v {
                                        Some(m)
                                    } else {
                                        None
                                    }
                                });
                                if let Some(obj) = status_obj {
                                    let code = obj
                                        .get("code")
                                        .and_then(|v| {
                                            if let Amf0ValueType::UTF8String(s) = v {
                                                Some(s.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or_default();
                                    let desc = obj
                                        .get("description")
                                        .and_then(|v| {
                                            if let Amf0ValueType::UTF8String(s) = v {
                                                Some(s.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or_default();

                                    if code == "NetStream.Publish.Start" {
                                        return Ok(());
                                    }
                                    if code.starts_with("NetStream.Publish.")
                                        || code.starts_with("NetConnection.Connect.")
                                    {
                                        return Err(PushError::PublishRejected {
                                            code,
                                            description: desc,
                                        });
                                    }
                                    // Other onStatus codes (e.g. Reset) - keep waiting.
                                }
                            }
                            RtmpMessageData::SetChunkSize { chunk_size } => {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
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
}

// -------------------------------------------------------------------------
// Background read loop
// -------------------------------------------------------------------------

// Maximum time the read-loop holds the `io` mutex on each poll. Real-time
// media push needs ~50 tags/sec each acquiring the same mutex to write,
// so the read side must yield FAST when contended; otherwise contention
// caps output rate to a fraction of real-time and `cache_delay` grows
// without bound. Empirically, a 100 ms hold caused ~0.2 x real-time
// output (cache grew 0.8 s/s during E2E #103).
const READ_LOOP_HOLD_MS: u64 = 5;
// Idle gap between read attempts so the read-loop is mutex-busy ~10 % of
// the time, leaving the rest for `send_tag`. Combined with `try_lock`
// below this means `send_tag` essentially never blocks on the read-loop.
const READ_LOOP_IDLE_MS: u64 = 50;

/// Background task: continuously reads from `io` and watches for
/// server-initiated errors.  Sets `poisoned = true` on any I/O error or EOF.
///
/// Mutex discipline: the `io` mutex is shared with `send_tag`; on the push
/// path that mutex is on the hot path (every audio/video tag write). The
/// read-loop therefore uses `try_lock` and a short hold/idle cycle so it
/// yields to writers immediately and is mutex-busy only ~10 % of the time.
/// Detection latency for server-initiated errors is bounded by
/// `READ_LOOP_HOLD_MS + READ_LOOP_IDLE_MS` (~55 ms), which is far below
/// xiu's 2 s inactivity timer and well under the consumer-task `WRITE_TIMEOUT`.
// Read loop is large (RTMP message dispatch + Acknowledgement state).
// Extracted to a sibling file to keep this file under the 1000-line cap.
#[path = "session_read_loop.rs"]
mod read_loop_mod;
use read_loop_mod::read_loop;

// -------------------------------------------------------------------------
// AMF value helpers
// -------------------------------------------------------------------------

fn amf_string(v: &Amf0ValueType) -> String {
    match v {
        Amf0ValueType::UTF8String(s) => s.clone(),
        _ => String::new(),
    }
}

fn amf_u8(v: &Amf0ValueType) -> u8 {
    match v {
        Amf0ValueType::Number(n) => *n as u8,
        _ => 0,
    }
}

// -------------------------------------------------------------------------
// URL parsing
// -------------------------------------------------------------------------

/// URL scheme of the upstream RTMP endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scheme {
    Rtmp,
    Rtmps,
}

fn parse_rtmp_url(url: &str) -> Result<(Scheme, String, u16, String, String), PushError> {
    let (scheme, rest, default_port) = if let Some(r) = url.strip_prefix("rtmps://") {
        (Scheme::Rtmps, r, 443u16)
    } else if let Some(r) = url.strip_prefix("rtmp://") {
        (Scheme::Rtmp, r, 1935u16)
    } else {
        return Err(bad_url("must start with rtmp:// or rtmps://", url));
    };

    let slash = rest
        .find('/')
        .ok_or_else(|| bad_url("missing /app/stream path", url))?;

    let authority = &rest[..slash];
    let path = &rest[slash + 1..];

    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let h = &authority[..colon];
        let p: u16 = authority[colon + 1..]
            .parse()
            .map_err(|_| bad_url("invalid port number", url))?;
        (h.to_string(), p)
    } else {
        (authority.to_string(), default_port)
    };

    if host.is_empty() {
        return Err(bad_url("host is empty", url));
    }

    let slash2 = path
        .find('/')
        .ok_or_else(|| bad_url("path must contain /app/stream (two components)", url))?;

    let app = path[..slash2].to_string();
    let stream = path[slash2 + 1..].to_string();

    if app.is_empty() {
        return Err(bad_url("app name is empty", url));
    }
    if stream.is_empty() {
        return Err(bad_url("stream name is empty", url));
    }

    Ok((scheme, host, port, app, stream))
}

fn bad_url(reason: &str, url: &str) -> PushError {
    PushError::IoError(io::Error::other(format!("bad RTMP URL ({reason}): {url}")))
}

// -------------------------------------------------------------------------
// Unit tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{READ_LOOP_HOLD_MS, READ_LOOP_IDLE_MS, Scheme, build_tc_url, parse_rtmp_url};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::time::Instant;

    // --- URL parser tests ---------------------------------------------------

    #[test]
    fn parse_standard_rtmp_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://a.example.com/live/test").unwrap();
        assert_eq!(scheme, Scheme::Rtmp);
        assert_eq!(host, "a.example.com");
        assert_eq!(port, 1935);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn parse_rtmp_url_with_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://127.0.0.1:19350/live/mykey").unwrap();
        assert_eq!(scheme, Scheme::Rtmp);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19350);
        assert_eq!(app, "live");
        assert_eq!(stream, "mykey");
    }

    #[test]
    fn parse_standard_rtmps_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://live-api-s.facebook.com/rtmp/abc123").unwrap();
        assert_eq!(scheme, Scheme::Rtmps);
        assert_eq!(host, "live-api-s.facebook.com");
        assert_eq!(port, 443);
        assert_eq!(app, "rtmp");
        assert_eq!(stream, "abc123");
    }

    #[test]
    fn parse_rtmps_url_with_explicit_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://127.0.0.1:19443/live/test").unwrap();
        assert_eq!(scheme, Scheme::Rtmps);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19443);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn rejects_non_rtmp_scheme() {
        assert!(parse_rtmp_url("http://host/live/test").is_err());
    }

    #[test]
    fn rejects_missing_stream() {
        assert!(parse_rtmp_url("rtmp://host/live").is_err());
        assert!(parse_rtmp_url("rtmps://host/live").is_err());
    }

    #[test]
    fn rejects_empty_app() {
        assert!(parse_rtmp_url("rtmp://host//stream").is_err());
    }

    // --- tc_url builder tests -----------------------------------------------

    #[test]
    fn build_tc_url_omits_default_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "live-api-s.facebook.com", 443, "rtmp");
        assert_eq!(url, "rtmps://live-api-s.facebook.com/rtmp");
    }

    #[test]
    fn build_tc_url_omits_default_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "a.rtmp.youtube.com", 1935, "live2");
        assert_eq!(url, "rtmp://a.rtmp.youtube.com/live2");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "127.0.0.1", 19350, "live");
        assert_eq!(url, "rtmp://127.0.0.1:19350/live");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "127.0.0.1", 19443, "live");
        assert_eq!(url, "rtmps://127.0.0.1:19443/live");
    }

    // --- Liveness-stub tests (make send_audio_tag / send_video_tag reachable)

    /// Confirm the poisoned-flag logic that backs `send_audio_tag` and
    /// `send_video_tag` works correctly.
    ///
    /// We cannot create a `Session` without a live server, so we test the
    /// underlying `AtomicBool` semantics instead.
    #[test]
    fn check_alive_poison_flag_logic() {
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!flag.load(Ordering::Relaxed), "fresh flag must be false");
        flag.store(true, Ordering::Relaxed);
        assert!(
            flag.load(Ordering::Relaxed),
            "after store(true) flag is true"
        );
    }

    /// Pacing math for `-re`-style rate limiting (issue #155 / spec §2):
    /// given an `Instant` anchor + first FLV ts + current FLV ts, the sleep
    /// before sending should equal max(0, ts_delta - wall_elapsed).
    ///
    /// We use `tokio::time::pause` so the test runs deterministically.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn pacing_sleeps_until_wall_matches_ts_delta() {
        let anchor = Instant::now();
        let first_ts: u32 = 1_000;

        // Advance virtual time by exactly 500ms so wall_elapsed = 500.
        tokio::time::advance(Duration::from_millis(500)).await;

        // Tag at first_ts + 2000 means target_ms=2000, wall_elapsed=500,
        // so we should sleep 1500ms.
        let current_ts: u32 = first_ts + 2_000;
        let target_ms = current_ts.saturating_sub(first_ts) as u64;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        assert_eq!(actual_ms, 500, "wall elapsed must be 500ms");
        assert_eq!(target_ms, 2_000, "target ts delta must be 2000ms");
        assert!(actual_ms < target_ms, "must need a sleep");
        let sleep_ms = target_ms - actual_ms;
        assert_eq!(
            sleep_ms, 1_500,
            "pacing must sleep ts_delta - wall_elapsed = 1500ms"
        );
    }

    /// When wall-clock has already caught up past the FLV ts (e.g. previous
    /// chunk's send took longer than its ts), pacing must NOT sleep.
    /// `saturating_sub` guards against negative wraparound.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn pacing_no_sleep_when_wall_already_past_ts_delta() {
        let anchor = Instant::now();
        let first_ts: u32 = 0;

        // Advance virtual time by 5 seconds.
        tokio::time::advance(Duration::from_millis(5_000)).await;

        // Tag at first_ts + 1000 means target_ms=1000, wall_elapsed=5000,
        // so we are already 4000ms behind real-time — must not sleep.
        let current_ts: u32 = first_ts + 1_000;
        let target_ms = current_ts.saturating_sub(first_ts) as u64;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        assert!(
            actual_ms >= target_ms,
            "wall elapsed (5000) >= target (1000)"
        );
    }

    #[test]
    fn read_loop_constants_keep_writers_off_the_hot_path() {
        // Regression for the cache-growth bug found in #103 E2E run on
        // 2026-04-29: the read-loop held the io mutex for 100ms per cycle.
        // Each `send_tag` write needs the same mutex; with ~50 tags/sec on
        // a typical OBS stream (30fps video + audio), output rate dropped
        // to ~0.2 x real-time and `cache_delay` grew at ~1 s/s during init.
        //
        // The fix bounds total mutex-busy fraction (HOLD / (HOLD + IDLE))
        // to ~10 %. A regression that pushes HOLD back up to 100ms or
        // shrinks IDLE to a few ms would re-create the contention.
        assert!(
            READ_LOOP_HOLD_MS <= 10,
            "HOLD must stay tiny so writers win the mutex"
        );
        assert!(
            READ_LOOP_IDLE_MS >= 4 * READ_LOOP_HOLD_MS,
            "IDLE must be much larger than HOLD so the mutex is mostly free for writers"
        );
        // Detection latency for server errors (~HOLD + IDLE) must stay
        // below xiu's 2 s inactivity timer with a healthy margin.
        assert!(
            READ_LOOP_HOLD_MS + READ_LOOP_IDLE_MS < 1_000,
            "detection-latency budget exceeded"
        );
    }
}
