//! Hand-rolled publish-rejecting RTMP server for the `publish_rejected` test
//! (issue #149).
//!
//! Split out of `common/mod.rs` to keep that file under the repo's
//! 1000-line-per-file CI cap.

// Only `local_xiu_loopback` uses this helper; the other test binaries that
// pull in `common` compile it unused (same per-binary situation `mod.rs`
// documents for its own helpers).
#![allow(dead_code)]

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// A minimal RTMP server that completes the standard handshake / connect /
/// createStream sequence and then REJECTS the `publish` command by sending an
/// `onStatus` message with `code = "NetStream.Publish.BadName"`.
///
/// This lets us verify that `RtmpPusher` surfaces `PushError::PublishRejected`
/// with the correct code. The real xiu `RtmpServer` cannot drive this test:
/// its `ServerSession` only ever writes `NetStream.Publish.Start`, and its
/// `auth` hook rejects by returning `Err` (connection close), which surfaces
/// as an I/O error rather than a `NetStream.Publish.*` onStatus.
///
/// # Why this differs from the version removed in PR #103 (issue #149)
///
/// The earlier `run_rejecting_server` desynced the client's `ChunkUnpacketizer`
/// ("pack error" / "none return") so the rejection onStatus was never parsed.
/// The root cause was fidelity to xiu's real `ServerSession` accept sequence:
///
///   1. It used `SimpleHandshakeServer` and did **not** call
///      `get_remaining_bytes()` after the handshake finished. The pusher sends
///      `SetChunkSize` + `connect` immediately after C2, so on loopback those
///      bytes routinely arrive coalesced into the same read that carried C2.
///      Those leftover bytes were dropped instead of being fed into the message
///      unpacketizer, so the client's `connect` was lost and the exchange hung.
///   2. It skipped `window_acknowledgement_size` / `set_peer_bandwidth` and
///      `stream_begin`, diverging from the byte sequence the client's read path
///      is proven to parse (see `media_payload_byte_identical_to_source`, which
///      exercises xiu's real server against this exact client).
///
/// This version uses the **same** xiu server-side writers in the **same**
/// accept sequence as `rtmp-0.6.5/src/session/server_session.rs`, with the
/// complex `HandshakeServer` (which auto-falls back to simple for the pusher's
/// `SimpleHandshakeClient`, exactly as the real server does) and feeds its
/// `get_remaining_bytes()` into the unpacketizer. The publish response is the
/// one deliberate divergence: an `onStatus` rejection (`level = "error"`,
/// `code = "NetStream.Publish.BadName"`) instead of the success
/// `NetStream.Publish.Start`. The publish transaction id is echoed as xiu does;
/// the client ignores the onStatus level and transaction id either way.
///
/// The function accepts exactly one incoming connection, processes it to the
/// point of the publish rejection, and then returns. Designed to run inside a
/// `tokio::spawn` so the test can join (or abort) it.
pub async fn run_rejecting_server(listener: TcpListener) -> Result<(), String> {
    use bytesio::bytes_writer::AsyncBytesWriter;
    use bytesio::bytesio::{TNetIO, TcpIO};
    use rtmp::chunk::define::CHUNK_SIZE;
    use rtmp::chunk::errors::UnpackErrorValue;
    use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
    use rtmp::handshake::define::{RTMP_HANDSHAKE_SIZE, ServerHandshakeState};
    use rtmp::handshake::handshake_server::HandshakeServer;
    use rtmp::messages::define::RtmpMessageData;
    use rtmp::messages::parser::MessageParser;
    use rtmp::netconnection::writer::NetConnection;
    use rtmp::netstream::writer::NetStreamWriter;
    use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
    use rtmp::session::define::{
        CAPABILITIES, FMSVER, LEVEL, OBJENCODING_AMF0, PEER_BANDWIDTH, STREAM_ID,
        WINDOW_ACKNOWLEDGEMENT_SIZE, peer_bandwidth_limit_type,
    };
    use xflv::amf0::define::Amf0ValueType;

    let (stream, _peer) = listener
        .accept()
        .await
        .map_err(|e| format!("accept: {e:?}"))?;

    // Wrap the TcpStream in the bytesio TNetIO adapter that xiu APIs expect.
    let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> =
        Arc::new(Mutex::new(Box::new(TcpIO::new(stream))));

    // --- Phase 1: RTMP handshake (mirror ServerSession::handshake) ----------
    let mut handshaker = HandshakeServer::new(Arc::clone(&io));
    loop {
        // Read at least one full handshake block before advancing, exactly as
        // xiu's ServerSession::handshake does.
        let mut bytes_len = 0usize;
        while bytes_len < RTMP_HANDSHAKE_SIZE {
            let data = io
                .lock()
                .await
                .read()
                .await
                .map_err(|e| format!("read hs: {e:?}"))?;
            bytes_len += data.len();
            handshaker.extend_data(&data[..]);
        }
        handshaker
            .handshake()
            .await
            .map_err(|e| format!("hs: {e:?}"))?;
        if let ServerHandshakeState::Finish = handshaker.state() {
            break;
        }
    }

    // CRITICAL (issue #149): feed any bytes that arrived coalesced with C2
    // (typically the client's SetChunkSize + connect) into the message
    // unpacketizer. Dropping these is what desynced the removed harness.
    let mut unpacketizer = ChunkUnpacketizer::new();
    let left = handshaker.get_remaining_bytes();
    if !left.is_empty() {
        unpacketizer.extend_data(&left[..]);
    }

    // Mirror xiu ServerSession::send_set_chunk_size: announce our outgoing
    // chunk size (CHUNK_SIZE) so both sides agree. The server-side command
    // writers (NetConnection / NetStreamWriter) packetize at CHUNK_SIZE by
    // default, so this matches what we actually write.
    {
        let mut ctrl = ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)));
        ctrl.write_set_chunk_size(CHUNK_SIZE)
            .await
            .map_err(|e| format!("write_set_chunk_size: {e:?}"))?;
    }

    // --- Phase 2: parse RTMP messages; respond to connect / createStream and
    //             REJECT publish with onStatus(NetStream.Publish.BadName). -----
    loop {
        // Drain everything currently buffered BEFORE blocking on another read.
        // The client's `connect` may already sit in the handshake leftover, so
        // reading first would deadlock (the client is waiting on our _result).
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
                                transaction_id,
                                ..
                            } => {
                                let cmd = match &command_name {
                                    Amf0ValueType::UTF8String(s) => s.as_str(),
                                    _ => "",
                                };
                                let tid = match &transaction_id {
                                    Amf0ValueType::Number(n) => *n,
                                    _ => 0.0,
                                };

                                match cmd {
                                    "connect" => {
                                        // Mirror ServerSession::on_connect.
                                        let mut ctrl = ProtocolControlMessagesWriter::new(
                                            AsyncBytesWriter::new(Arc::clone(&io)),
                                        );
                                        ctrl.write_window_acknowledgement_size(
                                            WINDOW_ACKNOWLEDGEMENT_SIZE,
                                        )
                                        .await
                                        .map_err(|e| format!("win_ack: {e:?}"))?;
                                        ctrl.write_set_peer_bandwidth(
                                            PEER_BANDWIDTH,
                                            peer_bandwidth_limit_type::DYNAMIC,
                                        )
                                        .await
                                        .map_err(|e| format!("set_peer_bw: {e:?}"))?;

                                        let mut nc = NetConnection::new(Arc::clone(&io));
                                        nc.write_connect_response(
                                            &tid,
                                            FMSVER,
                                            &CAPABILITIES,
                                            "NetConnection.Connect.Success",
                                            LEVEL,
                                            "Connection Succeeded.",
                                            &OBJENCODING_AMF0,
                                        )
                                        .await
                                        .map_err(|e| format!("connect_response: {e:?}"))?;
                                    }
                                    "createStream" => {
                                        // Mirror ServerSession::on_create_stream.
                                        let mut nc = NetConnection::new(Arc::clone(&io));
                                        nc.write_create_stream_response(&tid, &STREAM_ID)
                                            .await
                                            .map_err(|e| {
                                                format!("create_stream_response: {e:?}")
                                            })?;
                                    }
                                    "publish" => {
                                        // The one deliberate divergence from the
                                        // success path: reject instead of
                                        // NetStream.Publish.Start. A rejecting
                                        // server sends no stream_begin. Echo the
                                        // publish transaction id as xiu does.
                                        let mut ns = NetStreamWriter::new(Arc::clone(&io));
                                        ns.write_on_status(
                                            &tid,
                                            "error",
                                            "NetStream.Publish.BadName",
                                            "Publish rejected by test harness.",
                                        )
                                        .await
                                        .map_err(|e| format!("write_on_status: {e:?}"))?;
                                        // Done -- the pusher parses the rejection
                                        // and returns PushError::PublishRejected.
                                        return Ok(());
                                    }
                                    // releaseStream, FCPublish, _checkbw, etc. --
                                    // the client does not wait on responses to
                                    // these, so ignore silently (as xiu does).
                                    _ => {}
                                }
                            }
                            RtmpMessageData::SetChunkSize { chunk_size } => {
                                unpacketizer.update_max_chunk_size(chunk_size as usize);
                            }
                            _ => {}
                        }
                    }
                }
                // `read_chunks` only ever yields `Chunks` on success; any other
                // `Ok` is unreachable in practice.
                Ok(_) => break,
                Err(e) => {
                    // `CannotParse` is xiu's *sticky* desync signal -- surface it
                    // as a harness bug rather than letting the exchange stall
                    // into the test's 5 s timeout (which would misreport a
                    // harness desync as a production death-loop, the exact
                    // #103/#149 misattribution this re-add exists to end).
                    // `EmptyChunks` just means "need more bytes" -> read more.
                    if let UnpackErrorValue::CannotParse = e.value {
                        return Err(format!("unpacketizer desync (CannotParse): {e:?}"));
                    }
                    break;
                }
            }
        }

        // Block for the next batch of client bytes.
        let data = io
            .lock()
            .await
            .read()
            .await
            .map_err(|e| format!("read msg: {e:?}"))?;
        unpacketizer.extend_data(&data[..]);
    }
}
