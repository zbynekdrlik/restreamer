// Phase 1 telemetry struct for the rust_rtmp_push backend.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.3.
// Issue #176.

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
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        let v = t.snapshot(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(v["upstream_close_first_bytes_hex"], "deadbeef");
    }

    #[test]
    fn snapshot_truncates_close_buffer_to_64_bytes() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        let buf = vec![0xAA; 200];
        let v = t.snapshot(&buf);
        let hex = v["upstream_close_first_bytes_hex"].as_str().unwrap();
        assert_eq!(hex.len(), 128); // 64 bytes * 2 hex chars
    }
}
