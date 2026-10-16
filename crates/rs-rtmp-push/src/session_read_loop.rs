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

/// #368: incoming upstream control the read loop records, shared with the
/// `Session` so the pusher can report it (`RtmpPusher::last_upstream_ack_at`).
#[derive(Debug, Default)]
pub(crate) struct UpstreamSignals {
    last_ack_at: std::sync::Mutex<Option<std::time::Instant>>,
    pings_answered: std::sync::atomic::AtomicU64,
}

impl UpstreamSignals {
    /// The upstream acknowledged our bytes (RTMP Acknowledgement, type 3).
    pub(crate) fn note_ack(&self) {
        *self.last_ack_at.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(std::time::Instant::now());
    }

    pub(crate) fn last_ack_at(&self) -> Option<std::time::Instant> {
        *self.last_ack_at.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// We answered one UserControl PingRequest.
    pub(crate) fn note_ping_answered(&self) {
        self.pings_answered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn pings_answered(&self) -> u64 {
        self.pings_answered.load(Ordering::Relaxed)
    }
}

/// #368: the timestamp of a UserControl PingRequest (event 6), or `None`
/// for any other message. Skeleton: never a ping.
pub(crate) fn ping_request_timestamp(msg_type_id: u8, payload: &[u8]) -> Option<u32> {
    let _ = (msg_type_id, payload);
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A UserControl payload: 2-byte event type + 4-byte event data.
    fn event(kind: u16, data: u32) -> Vec<u8> {
        let mut p = kind.to_be_bytes().to_vec();
        p.extend_from_slice(&data.to_be_bytes());
        p
    }

    #[test]
    fn a_ping_request_yields_its_timestamp() {
        assert_eq!(
            ping_request_timestamp(4, &event(6, 0xDEAD_BEEF)),
            Some(0xDEAD_BEEF)
        );
        assert_eq!(ping_request_timestamp(4, &event(6, 0)), Some(0));
    }

    #[test]
    fn other_events_and_messages_are_not_pings() {
        // StreamBegin, a PingResponse, SetBufferLength.
        for kind in [0u16, 7, 3] {
            assert_eq!(
                ping_request_timestamp(4, &event(kind, 9)),
                None,
                "event {kind}"
            );
        }
        // The same bytes in an Acknowledgement / SetChunkSize are not a ping.
        for type_id in [3u8, 1, 5] {
            assert_eq!(
                ping_request_timestamp(type_id, &event(6, 9)),
                None,
                "type {type_id}"
            );
        }
    }

    #[test]
    fn a_short_payload_is_not_a_ping() {
        let full = event(6, 0x0102_0304);
        assert_eq!(ping_request_timestamp(4, &full[..5]), None);
        assert_eq!(ping_request_timestamp(4, &full[..2]), None);
        assert_eq!(ping_request_timestamp(4, &[]), None);
        // A trailing byte does not hide the ping.
        let mut long = full.clone();
        long.push(0xFF);
        assert_eq!(ping_request_timestamp(4, &long), Some(0x0102_0304));
    }

    #[test]
    fn upstream_signals_record_acks_and_answered_pings() {
        let s = UpstreamSignals::default();
        assert_eq!(s.last_ack_at(), None);
        assert_eq!(s.pings_answered(), 0);
        let before = std::time::Instant::now();
        s.note_ack();
        assert!(s.last_ack_at().is_some_and(|t| t >= before));
        s.note_ping_answered();
        s.note_ping_answered();
        assert_eq!(s.pings_answered(), 2);
    }
}
