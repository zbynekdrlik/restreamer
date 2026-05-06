// Phase 1 telemetry struct for the rust_rtmp_push backend.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.3.
// Issue #176.

#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

/// Per-session telemetry counters for one RTMP push connection.
/// Reset on each connect by constructing a fresh value.
pub struct RtmpPushTelemetry {
    connect_at: Instant,
    bytes_sent: u64,
    last_upstream_ack_at: Option<Instant>,
    last_message_type_sent: Option<&'static str>,
    chunks_pushed: u32,
    /// Test-only override: when Some, overrides Instant::now() for snapshot math.
    test_clock: Option<Instant>,
}

impl RtmpPushTelemetry {
    pub fn new() -> Self {
        Self {
            connect_at: Instant::now(),
            bytes_sent: 0,
            last_upstream_ack_at: None,
            last_message_type_sent: None,
            chunks_pushed: 0,
            test_clock: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_at(_offset_ms: u64) -> Self {
        let now = Instant::now();
        Self {
            connect_at: now,
            bytes_sent: 0,
            last_upstream_ack_at: None,
            last_message_type_sent: None,
            chunks_pushed: 0,
            test_clock: Some(now),
        }
    }

    #[cfg(test)]
    pub(crate) fn advance_clock_for_test(&mut self, by: Duration) {
        let cur = self.test_clock.expect("test clock not set");
        self.test_clock = Some(cur + by);
    }

    fn now(&self) -> Instant {
        self.test_clock.unwrap_or_else(Instant::now)
    }

    pub fn note_send(&mut self, msg_type: &'static str, n_bytes: u64) {
        self.last_message_type_sent = Some(msg_type);
        self.bytes_sent = self.bytes_sent.saturating_add(n_bytes);
    }

    pub fn note_upstream_ack(&mut self) {
        self.last_upstream_ack_at = Some(self.now());
    }

    pub fn note_chunk_pushed(&mut self) {
        self.chunks_pushed = self.chunks_pushed.saturating_add(1);
    }

    pub fn snapshot(&self, close_buf: &[u8]) -> serde_json::Value {
        let now = self.now();
        let time_since_connect_ms =
            now.saturating_duration_since(self.connect_at).as_millis() as u64;
        let time_since_last_upstream_ack_ms = self
            .last_upstream_ack_at
            .map(|t| now.saturating_duration_since(t).as_millis() as u64);

        let truncated = if close_buf.len() > 64 {
            &close_buf[..64]
        } else {
            close_buf
        };
        let mut hex = String::with_capacity(truncated.len() * 2);
        for b in truncated {
            hex.push_str(&format!("{:02x}", b));
        }

        serde_json::json!({
            "bytes_sent_since_connect": self.bytes_sent,
            "time_since_connect_ms": time_since_connect_ms,
            "time_since_last_upstream_ack_ms": time_since_last_upstream_ack_ms,
            "last_rtmp_message_type_sent": self.last_message_type_sent,
            "chunks_pushed": self.chunks_pushed,
            "upstream_close_first_bytes_hex": hex,
        })
    }
}

impl Default for RtmpPushTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn snapshot_with_no_ack_reports_null_time_since_ack() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.advance_clock_for_test(Duration::from_millis(1500));
        t.note_send("Audio", 1024);
        t.note_chunk_pushed();
        let v = t.snapshot(&[0u8; 8]);
        assert_eq!(v["bytes_sent_since_connect"], 1024);
        assert_eq!(v["time_since_connect_ms"], 1500);
        assert_eq!(
            v["time_since_last_upstream_ack_ms"],
            serde_json::Value::Null
        );
        assert_eq!(v["last_rtmp_message_type_sent"], "Audio");
        assert_eq!(v["chunks_pushed"], 1);
    }

    #[test]
    fn snapshot_after_ack_reports_age_since_ack() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.advance_clock_for_test(Duration::from_millis(500));
        t.note_upstream_ack();
        t.advance_clock_for_test(Duration::from_millis(750));
        let v = t.snapshot(&[]);
        assert_eq!(v["time_since_last_upstream_ack_ms"], 750);
    }

    #[test]
    fn snapshot_hex_encodes_close_buffer() {
        let t = RtmpPushTelemetry::new_for_test_at(0);
        let v = t.snapshot(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(v["upstream_close_first_bytes_hex"], "deadbeef");
    }

    #[test]
    fn snapshot_truncates_close_buffer_to_64_bytes() {
        let t = RtmpPushTelemetry::new_for_test_at(0);
        let buf = vec![0xAA; 200];
        let v = t.snapshot(&buf);
        let hex = v["upstream_close_first_bytes_hex"].as_str().unwrap();
        assert_eq!(hex.len(), 128); // 64 bytes * 2 hex chars
    }
}
