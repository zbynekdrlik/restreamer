//! Background read-loop for an active RTMP push session.
//!
//! Extracted from `session.rs` to keep that file under the 1000-line cap.
//! Included via `#[path]` as `mod read_loop_mod` inside `session.rs`.
//!
//! Responsibilities:
//! - Watch for server-initiated errors (poison the session on EOF / I/O error
//!   / mid-stream `onStatus` error).
//! - Update the chunk unpacketizer's max chunk size when the server sends
//!   a `SetChunkSize` message.
//! - Track total bytes received from the server and send back
//!   `Acknowledgement` messages every `WindowAcknowledgementSize` bytes per
//!   RTMP spec §6.2.7. Without these, YT/FB load balancers conclude the
//!   connection is unresponsive and close it (~12-15 min in production).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytesio::bytes_writer::AsyncBytesWriter;
use bytesio::bytesio::TNetIO;
use rtmp::chunk::unpacketizer::{ChunkUnpacketizer, UnpackResult};
use rtmp::messages::define::RtmpMessageData;
use rtmp::messages::parser::MessageParser;
use rtmp::protocol_control_messages::writer::ProtocolControlMessagesWriter;
use tokio::sync::Mutex;
use xflv::amf0::define::Amf0ValueType;

use super::{READ_LOOP_HOLD_MS, READ_LOOP_IDLE_MS, amf_string};

pub(super) async fn read_loop(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    poisoned: Arc<AtomicBool>,
) {
    let mut unpacketizer = ChunkUnpacketizer::new();
    // RTMP Acknowledgement state per spec §6.2.7. Default 2.5 MB until
    // server overrides via WindowAcknowledgementSize.
    let mut window_size: u32 = 2_500_000;
    let mut bytes_received: u32 = 0;
    let mut last_ack_seq: u32 = 0;

    loop {
        // Sleep FIRST so the read-loop is mutex-busy at most ~9 % of the
        // time — `send_tag` is on the hot path and must win quickly.
        tokio::time::sleep(Duration::from_millis(READ_LOOP_IDLE_MS)).await;

        let result = match io.try_lock() {
            Ok(mut guard) => {
                tokio::time::timeout(Duration::from_millis(READ_LOOP_HOLD_MS), guard.read()).await
            }
            Err(_) => continue,
        };

        let data = match result {
            Err(_timeout) => continue,
            Ok(Ok(d)) => d,
            Ok(Err(_)) => {
                poisoned.store(true, Ordering::Relaxed);
                return;
            }
        };

        if data.is_empty() {
            poisoned.store(true, Ordering::Relaxed);
            return;
        }

        bytes_received = bytes_received.wrapping_add(data.len() as u32);

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
                                RtmpMessageData::WindowAcknowledgementSize { size } => {
                                    window_size = size;
                                }
                                RtmpMessageData::SetPeerBandwidth { properties } => {
                                    let new_window = properties.window_size;
                                    window_size = new_window;
                                    let mut ctrl = ProtocolControlMessagesWriter::new(
                                        AsyncBytesWriter::new(Arc::clone(&io)),
                                    );
                                    let _ =
                                        ctrl.write_window_acknowledgement_size(new_window).await;
                                }
                                RtmpMessageData::Amf0Command {
                                    command_name,
                                    others,
                                    ..
                                } if amf_string(&command_name) == "onStatus" => {
                                    let is_error = others.iter().any(|v| {
                                        let Amf0ValueType::Object(m) = v else {
                                            return false;
                                        };
                                        m.get("level")
                                            .map(|lv| matches!(lv, Amf0ValueType::UTF8String(s) if s == "error"))
                                            .unwrap_or(false)
                                    });
                                    if is_error {
                                        poisoned.store(true, Ordering::Relaxed);
                                        return;
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

        // RTMP §6.2.7: send Acknowledgement once we've received
        // `window_size` bytes since the last ack. Without this YT/FB
        // load balancers consider the connection unresponsive after
        // ~12-15 min and close it (issue #164).
        if bytes_received.wrapping_sub(last_ack_seq) >= window_size {
            let mut ctrl =
                ProtocolControlMessagesWriter::new(AsyncBytesWriter::new(Arc::clone(&io)));
            if ctrl.write_acknowledgement(bytes_received).await.is_ok() {
                last_ack_seq = bytes_received;
            }
        }
    }
}
