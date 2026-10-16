//! Hand-rolled RTMP server that ACCEPTS the publish and then sends the
//! upstream control messages YouTube sends mid-session: a UserControl
//! PingRequest (event 6) and an Acknowledgement (#368).
//!
//! xiu's real server never sends either, so the success-path xiu helpers in
//! `mod.rs` cannot drive this test. The accept sequence mirrors
//! `rejecting_server.rs` (and so xiu's `ServerSession`) exactly; only the
//! publish answer (`NetStream.Publish.Start`) and the control messages after
//! it differ. See `.claude/rules/rtmp-test-harness.md`.

#![allow(dead_code)]

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

/// UserControl event types (RTMP spec 7.1.7).
const EVENT_PING_RESPONSE: u16 = 7;

/// Accept ONE client, accept its publish, and once the client's first data
/// message (its onMetaData) arrived send TWO PingRequests (`ping_ts`, then
/// `ping_ts + 1`) and an `Acknowledgement(ack_seq)`. Two pings, so a test can
/// tell a real answer counter from a constant. The first PingResponse the
/// client sends back is reported on `pong_tx`. Returns when the client closes.
pub async fn run_control_server(
    listener: TcpListener,
    ping_ts: u32,
    ack_seq: u32,
    pong_tx: oneshot::Sender<u32>,
) -> Result<(), String> {
    use bytesio::bytes_writer::AsyncBytesWriter;
    use bytesio::bytesio::{TNetIO, TcpIO};
    use rtmp::chunk::define::CHUNK_SIZE;
    use rtmp::chunk::errors::UnpackErrorValue;
    use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
    use rtmp::handshake::define::{RTMP_HANDSHAKE_SIZE, ServerHandshakeState};
    use rtmp::handshake::handshake_server::HandshakeServer;
    use rtmp::messages::define::{RtmpMessageData, msg_type_id};
    use rtmp::messages::parser::MessageParser;
    use rtmp::netconnection::writer::NetConnection;
    use rtmp::netstream::writer::NetStreamWriter;
    use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
    use rtmp::session::define::{
        CAPABILITIES, FMSVER, LEVEL, OBJENCODING_AMF0, PEER_BANDWIDTH, STREAM_ID,
        WINDOW_ACKNOWLEDGEMENT_SIZE, peer_bandwidth_limit_type,
    };
    use rtmp::user_control_messages::writer::EventMessagesWriter;
    use xflv::amf0::define::Amf0ValueType;

    let (stream, _peer) = listener
        .accept()
        .await
        .map_err(|e| format!("accept: {e:?}"))?;
    let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> =
        Arc::new(Mutex::new(Box::new(TcpIO::new(stream))));

    // --- Handshake (mirror ServerSession::handshake) ------------------------
    let mut handshaker = HandshakeServer::new(Arc::clone(&io));
    loop {
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
    let mut unpacketizer = ChunkUnpacketizer::new();
    let left = handshaker.get_remaining_bytes();
    if !left.is_empty() {
        unpacketizer.extend_data(&left[..]);
    }
    ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)))
        .write_set_chunk_size(CHUNK_SIZE)
        .await
        .map_err(|e| format!("write_set_chunk_size: {e:?}"))?;

    let mut pong_tx = Some(pong_tx);
    let mut sent_control = false;
    loop {
        loop {
            match unpacketizer.read_chunks() {
                Ok(UnpackResult::Chunks(chunks)) => {
                    for chunk in chunks {
                        let type_id = chunk.message_header.msg_type_id;
                        // The client's PingResponse: xiu's parser cannot read
                        // event 7, so read the raw payload.
                        if type_id == msg_type_id::USER_CONTROL_EVENT {
                            let p = &chunk.payload;
                            if p.len() >= 6
                                && u16::from_be_bytes([p[0], p[1]]) == EVENT_PING_RESPONSE
                            {
                                let ts = u32::from_be_bytes([p[2], p[3], p[4], p[5]]);
                                if let Some(tx) = pong_tx.take() {
                                    let _ = tx.send(ts);
                                }
                            }
                            continue;
                        }
                        // The client's first data message (onMetaData) proves
                        // its session (and its read loop) is up: now ping it.
                        if type_id == msg_type_id::DATA_AMF0 && !sent_control {
                            sent_control = true;
                            for ts in [ping_ts, ping_ts.wrapping_add(1)] {
                                EventMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)))
                                    .write_ping_request(ts)
                                    .await
                                    .map_err(|e| format!("write_ping_request: {e:?}"))?;
                            }
                            ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(
                                &io,
                            )))
                            .write_acknowledgement(ack_seq)
                            .await
                            .map_err(|e| format!("write_acknowledgement: {e:?}"))?;
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
                                        NetConnection::new(Arc::clone(&io))
                                            .write_connect_response(
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
                                        NetConnection::new(Arc::clone(&io))
                                            .write_create_stream_response(&tid, &STREAM_ID)
                                            .await
                                            .map_err(|e| {
                                                format!("create_stream_response: {e:?}")
                                            })?;
                                    }
                                    "publish" => {
                                        NetStreamWriter::new(Arc::clone(&io))
                                            .write_on_status(
                                                &tid,
                                                "status",
                                                "NetStream.Publish.Start",
                                                "",
                                            )
                                            .await
                                            .map_err(|e| format!("write_on_status: {e:?}"))?;
                                    }
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
                Ok(_) => break,
                Err(e) => {
                    if let UnpackErrorValue::CannotParse = e.value {
                        return Err(format!("unpacketizer desync (CannotParse): {e:?}"));
                    }
                    break;
                }
            }
        }
        let data = match io.lock().await.read().await {
            Ok(d) if !d.is_empty() => d,
            // The client closed: the test is over.
            _ => return Ok(()),
        };
        unpacketizer.extend_data(&data[..]);
    }
}
