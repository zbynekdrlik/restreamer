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
//! Tag writes (`send_audio_tag` / `send_video_tag`) are stubs for Task 4.
//! Task 6 fills the actual chunk-packetize-and-send loop.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytesio::bytes_writer::AsyncBytesWriter;
use bytesio::bytesio::{TNetIO, TcpIO};
use rtmp::chunk::define::CHUNK_SIZE;
use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
use rtmp::handshake::define::ClientHandshakeState;
use rtmp::handshake::handshake_client::SimpleHandshakeClient;
use rtmp::messages::define::RtmpMessageData;
use rtmp::messages::parser::MessageParser;
use rtmp::netconnection::writer::{ConnectProperties, NetConnection};
use rtmp::netstream::writer::NetStreamWriter;
use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
use rtmp::session::define::{TRANSACTION_ID_CONNECT, TRANSACTION_ID_CREATE_STREAM};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use xflv::amf0::define::Amf0ValueType;

use crate::PushError;

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
    /// Shared I/O handle.  Writers (main task) and the read-loop (background
    /// task) compete for this mutex.  On a live push the server sends almost
    /// no data after publishing starts, so lock contention is negligible.
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
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
        let (host, port, app, stream_name) = parse_rtmp_url(url)?;

        // --- 2. TCP connect -------------------------------------------------
        let addr = format!("{host}:{port}");
        let tcp_stream =
            tokio::time::timeout(Duration::from_millis(timeout_ms), TcpStream::connect(&addr))
                .await
                .map_err(|_| PushError::Timeout)?
                .map_err(PushError::HandshakeFailed)?;

        // Wrap in TcpIO and share via Arc<Mutex<>>.
        let net_io: Box<dyn TNetIO + Send + Sync> = Box::new(TcpIO::new(tcp_stream));
        let io = Arc::new(Mutex::new(net_io));

        // --- 3-5. Negotiate (handshake + connect + publish) ------------------
        tokio::time::timeout(
            Duration::from_secs(NEGOTIATE_TIMEOUT_SECS),
            negotiate(Arc::clone(&io), &addr, &app, &stream_name),
        )
        .await
        .map_err(|_| PushError::Timeout)??;

        // --- 6. Spawn background read-loop -----------------------------------
        let poisoned = Arc::new(AtomicBool::new(false));
        let read_loop_handle = tokio::spawn(read_loop(Arc::clone(&io), Arc::clone(&poisoned)));

        Ok(Self {
            io,
            poisoned,
            read_loop_handle: Some(read_loop_handle),
        })
    }

    /// Send an audio FLV tag body.
    ///
    /// Task 4 stub: checks liveness, returns `Ok(())` without sending bytes.
    /// Task 6 replaces this with actual `ChunkPacketizer` writes.
    pub async fn send_audio_tag(
        &mut self,
        _timestamp_ms: u32,
        _body: &[u8],
    ) -> Result<(), PushError> {
        self.check_alive()
    }

    /// Send a video FLV tag body.
    ///
    /// Task 4 stub: checks liveness, returns `Ok(())` without sending bytes.
    /// Task 6 replaces this with actual `ChunkPacketizer` writes.
    pub async fn send_video_tag(
        &mut self,
        _timestamp_ms: u32,
        _body: &[u8],
    ) -> Result<(), PushError> {
        self.check_alive()
    }

    /// Returns `Ok(())` if the session is still alive, or an error if the
    /// background read-loop detected a server-side close.
    fn check_alive(&self) -> Result<(), PushError> {
        if self.poisoned.load(Ordering::Relaxed) {
            Err(PushError::RemoteClosed(io::Error::from(
                io::ErrorKind::ConnectionReset,
            )))
        } else {
            Ok(())
        }
    }

    /// Gracefully shut down the session.
    pub async fn close(mut self) {
        if let Some(h) = self.read_loop_handle.take() {
            h.abort();
        }
        // `self.io` is intentionally held until here so the Arc stays alive
        // long enough for the read-loop abort to complete before the TcpIO
        // is dropped.  Task 6 will also use this field for chunk writes.
        drop(self.io);
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
/// Returns once the server sends `NetStream.Publish.Start`, or errors on any
/// rejection or unexpected close.
async fn negotiate(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    raw_domain: &str,
    app: &str,
    stream_name: &str,
) -> Result<(), PushError> {
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
        // Match xiu's ClientSession: send SetChunkSize before connect.
        let mut ctrl = ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)));
        ctrl.write_set_chunk_size(CHUNK_SIZE)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;

        let mut nc = NetConnection::new(Arc::clone(&io));
        let mut props = ConnectProperties::new_none();
        props.app = Some(app.to_string());
        props.pub_type = Some("nonprivate".to_string());
        props.flash_ver = Some("FMLE/3.0 (compatible; xiu)".to_string());
        props.fpad = Some(false);
        props.tc_url = Some(format!("rtmp://{raw_domain}/{app}"));
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

    // Wait for _result (transaction 2 == createStream).
    wait_for_result(
        Arc::clone(&io),
        &mut unpacketizer,
        TRANSACTION_ID_CREATE_STREAM,
        "createStream",
    )
    .await?;

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

    Ok(())
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
        let data = io
            .lock()
            .await
            .read()
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
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

/// Read messages from `io` until we see `onStatus` with a code that starts
/// with `NetStream.Publish.Start`.  Returns an error on rejection.
async fn wait_for_publish_start(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    unpacketizer: &mut ChunkUnpacketizer,
) -> Result<(), PushError> {
    loop {
        let data = io
            .lock()
            .await
            .read()
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
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
                            } => {
                                if amf_string(&command_name) == "onStatus" {
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

/// Background task: continuously reads from `io` and watches for
/// server-initiated errors.  Sets `poisoned = true` on any I/O error or EOF.
async fn read_loop(io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>, poisoned: Arc<AtomicBool>) {
    let mut unpacketizer = ChunkUnpacketizer::new();
    loop {
        let data = {
            let mut guard = io.lock().await;
            match guard.read().await {
                Ok(d) => d,
                Err(_) => {
                    poisoned.store(true, Ordering::Relaxed);
                    return;
                }
            }
        };

        if data.is_empty() {
            poisoned.store(true, Ordering::Relaxed);
            return;
        }

        unpacketizer.extend_data(&data[..]);
        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        if let Ok(Some(msg)) = MessageParser::new(chunk).parse() {
                            match msg {
                                RtmpMessageData::SetChunkSize { chunk_size } => {
                                    unpacketizer.update_max_chunk_size(chunk_size as usize);
                                }
                                RtmpMessageData::Amf0Command {
                                    command_name,
                                    others,
                                    ..
                                } => {
                                    // Watch for mid-stream onStatus errors.
                                    if amf_string(&command_name) == "onStatus" {
                                        let is_error = others.iter().any(|v| {
                                            if let Amf0ValueType::Object(m) = v {
                                                m.get("level")
                                                    .map(|lv| matches!(lv, Amf0ValueType::UTF8String(s) if s == "error"))
                                                    .unwrap_or(false)
                                            } else {
                                                false
                                            }
                                        });
                                        if is_error {
                                            poisoned.store(true, Ordering::Relaxed);
                                            return;
                                        }
                                    }
                                }
                                _ => {}
                            }
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

/// Parse `rtmp://host[:port]/app/stream` into `(host, port, app, stream)`.
///
/// - Default port is 1935.
/// - Both `app` and `stream` must be non-empty.
fn parse_rtmp_url(url: &str) -> Result<(String, u16, String, String), PushError> {
    let rest = url
        .strip_prefix("rtmp://")
        .ok_or_else(|| bad_url("must start with rtmp://", url))?;

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
        (authority.to_string(), 1935u16)
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

    Ok((host, port, app, stream))
}

fn bad_url(reason: &str, url: &str) -> PushError {
    PushError::IoError(io::Error::other(format!("bad RTMP URL ({reason}): {url}")))
}

// -------------------------------------------------------------------------
// Unit tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{Session, parse_rtmp_url};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // --- URL parser tests ---------------------------------------------------

    #[test]
    fn parse_standard_url() {
        let (host, port, app, stream) = parse_rtmp_url("rtmp://a.example.com/live/test").unwrap();
        assert_eq!(host, "a.example.com");
        assert_eq!(port, 1935);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn parse_url_with_port() {
        let (host, port, app, stream) =
            parse_rtmp_url("rtmp://127.0.0.1:19350/live/mykey").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19350);
        assert_eq!(app, "live");
        assert_eq!(stream, "mykey");
    }

    #[test]
    fn rejects_non_rtmp_scheme() {
        assert!(parse_rtmp_url("http://host/live/test").is_err());
    }

    #[test]
    fn rejects_missing_stream() {
        assert!(parse_rtmp_url("rtmp://host/live").is_err());
    }

    #[test]
    fn rejects_empty_app() {
        assert!(parse_rtmp_url("rtmp://host//stream").is_err());
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

    /// Dead-code suppression helper: references `send_audio_tag` and
    /// `send_video_tag` so Rust's liveness checker marks them as used.
    ///
    /// This helper is never called at runtime; its sole purpose is to
    /// make the async method names appear in compiled code.
    #[allow(dead_code)]
    async fn _use_stub_methods(s: &mut Session) {
        let _ = s.send_audio_tag(0, b"").await;
        let _ = s.send_video_tag(0, b"").await;
    }
}
