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
    /// #368: the push that ended the PREVIOUS connect failed in a way that
    /// may have lost its chunk upstream, so the first push of THIS connect
    /// re-sends that same chunk (`endpoint_consumer_helpers::push_chunk`).
    resend_pending: bool,
    /// #368: this connect was made without any delay after a healthy
    /// session closed (`rs_rtmp_push::reconnect_delay_ms` returned 0).
    follows_fast_retry: bool,
    /// #368: when the first chunk of this connect was pushed; the session
    /// counts as healthy from then on.
    first_send_at: Option<Instant>,
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
            resend_pending: false,
            follows_fast_retry: false,
            first_send_at: None,
            test_clock: None,
        }
    }

    /// Telemetry for the connect that follows a failed push. `resend_pending`
    /// = the failed chunk must be pushed again on it (#368);
    /// `follows_fast_retry` = this connect was made without any delay.
    pub fn after_failure(resend_pending: bool, follows_fast_retry: bool) -> Self {
        Self {
            resend_pending,
            follows_fast_retry,
            ..Self::new()
        }
    }

    /// #368: whether this connect was made immediately (0 ms) after a
    /// healthy session closed.
    pub fn follows_fast_retry(&self) -> bool {
        self.follows_fast_retry
    }

    /// #368: how long this connect has been HEALTHY (pushing media): ms since
    /// its first successful chunk, 0 before any.
    pub fn healthy_session_ms(&self) -> u64 {
        self.first_send_at
            .map(|t| self.now().saturating_duration_since(t).as_millis() as u64)
            .unwrap_or(0)
    }

    /// Whether the failed chunk still has to be re-sent on this connect.
    pub fn resend_pending(&self) -> bool {
        self.resend_pending
    }

    /// Consume the pending re-send: `true` exactly once after a failure that
    /// may have lost the chunk.
    pub fn take_resend_pending(&mut self) -> bool {
        std::mem::take(&mut self.resend_pending)
    }

    /// Drop a pending re-send: a chunk is re-sent at most once (#368).
    pub fn clear_resend_pending(&mut self) {
        self.resend_pending = false;
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
            resend_pending: false,
            follows_fast_retry: false,
            first_send_at: None,
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
        let now = self.now();
        self.first_send_at.get_or_insert(now);
        self.last_message_type_sent = Some(msg_type);
        self.bytes_sent = self.bytes_sent.saturating_add(n_bytes);
    }

    /// An upstream RTMP Acknowledgement arrived now.
    pub fn note_upstream_ack(&mut self) {
        self.note_upstream_ack_at(self.now());
    }

    /// #368: the pusher's read loop saw the latest upstream Acknowledgement
    /// at `at` (`Pushable::last_upstream_ack_at`); the consumer copies it in
    /// after every push, so the death row's `time_since_last_upstream_ack_ms`
    /// is measured instead of always null.
    pub fn note_upstream_ack_at(&mut self, at: Instant) {
        self.last_upstream_ack_at = Some(at);
    }

    pub fn note_chunk_pushed(&mut self) {
        self.chunks_pushed = self.chunks_pushed.saturating_add(1);
    }

    /// Total bytes sent on THIS connect (since the telemetry was last reset
    /// by constructing a fresh value on reconnect). #236: the dead-target
    /// classifier in `endpoint_consumer_helpers::handle_rust_push` reads
    /// this BEFORE the post-error telemetry reset to tell "this session
    /// never sent any media before the peer closed it" (dead target, e.g.
    /// an expired FB broadcast) apart from "media was flowing, then the
    /// peer dropped it" (transient outage -- unchanged fast-recovery path).
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    pub fn snapshot(&self, close_buf: &[u8]) -> serde_json::Value {
        let now = self.now();
        let time_since_connect_ms =
            now.saturating_duration_since(self.connect_at).as_millis() as u64;
        let time_since_last_upstream_ack_ms = self
            .last_upstream_ack_at
            .map(|t| now.saturating_duration_since(t).as_millis() as u64);

        let mut map = serde_json::Map::new();
        map.insert(
            "bytes_sent_since_connect".into(),
            serde_json::json!(self.bytes_sent),
        );
        map.insert(
            "time_since_connect_ms".into(),
            serde_json::json!(time_since_connect_ms),
        );
        map.insert(
            "time_since_last_upstream_ack_ms".into(),
            serde_json::json!(time_since_last_upstream_ack_ms),
        );
        // Omit last_rtmp_message_type_sent when None -- field is not yet wired
        // from production (Phase 2, #177); emitting null would mislead operators
        // into thinking "no message sent" when in fact we just don't know.
        if let Some(msg_type) = self.last_message_type_sent {
            map.insert(
                "last_rtmp_message_type_sent".into(),
                serde_json::json!(msg_type),
            );
        }
        map.insert(
            "chunks_pushed".into(),
            serde_json::json!(self.chunks_pushed),
        );
        map.insert(
            "follows_fast_retry".into(),
            serde_json::json!(self.follows_fast_retry),
        );
        // Omit upstream_close_first_bytes_hex when the close buffer is empty --
        // an empty string would look like "no bytes received" which is ambiguous
        // (the buffer may simply not have been plumbed yet, Phase 2, #177).
        if !close_buf.is_empty() {
            let truncated = if close_buf.len() > 64 {
                &close_buf[..64]
            } else {
                close_buf
            };
            let mut hex = String::with_capacity(truncated.len() * 2);
            for b in truncated {
                hex.push_str(&format!("{:02x}", b));
            }
            map.insert(
                "upstream_close_first_bytes_hex".into(),
                serde_json::json!(hex),
            );
        }
        serde_json::Value::Object(map)
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

    // #368: the re-send flag of the connect after a failure.
    #[test]
    fn after_failure_carries_the_resend_flag_and_take_consumes_it_once() {
        let mut t = RtmpPushTelemetry::after_failure(true, false);
        assert!(t.resend_pending());
        assert!(t.take_resend_pending());
        assert!(!t.resend_pending(), "take must consume the flag");
        assert!(!t.take_resend_pending());
        assert!(!RtmpPushTelemetry::after_failure(false, false).resend_pending());
        assert!(!RtmpPushTelemetry::new().resend_pending());
        assert_eq!(
            RtmpPushTelemetry::after_failure(true, false).bytes_sent(),
            0,
            "a failure starts a fresh connect"
        );
    }

    #[test]
    fn after_failure_carries_the_fast_retry_flag() {
        assert!(RtmpPushTelemetry::after_failure(false, true).follows_fast_retry());
        assert!(!RtmpPushTelemetry::after_failure(true, false).follows_fast_retry());
        assert!(!RtmpPushTelemetry::new().follows_fast_retry());
        let v = RtmpPushTelemetry::after_failure(false, true).snapshot(&[]);
        assert_eq!(
            v["follows_fast_retry"], true,
            "the death row says whether that connect followed a fast retry"
        );
        let v = RtmpPushTelemetry::new().snapshot(&[]);
        assert_eq!(v["follows_fast_retry"], false);
    }

    #[test]
    fn healthy_session_ms_counts_from_the_first_pushed_chunk() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.advance_clock_for_test(Duration::from_millis(5_000));
        assert_eq!(t.healthy_session_ms(), 0, "nothing pushed yet");
        t.note_send("flv_bytes", 100);
        t.advance_clock_for_test(Duration::from_millis(61_000));
        t.note_send("flv_bytes", 100);
        t.advance_clock_for_test(Duration::from_millis(500));
        assert_eq!(t.healthy_session_ms(), 61_500);
    }

    #[test]
    fn clear_resend_pending_drops_the_flag() {
        let mut t = RtmpPushTelemetry::after_failure(true, false);
        t.clear_resend_pending();
        assert!(!t.resend_pending());
    }

    // #236: bytes_sent() accessor -- the dead-target classifier in
    // endpoint_consumer_helpers::handle_rust_push reads this directly.
    #[test]
    fn bytes_sent_is_zero_on_a_fresh_connect() {
        let t = RtmpPushTelemetry::new_for_test_at(0);
        assert_eq!(t.bytes_sent(), 0);
    }

    #[test]
    fn bytes_sent_accumulates_across_multiple_sends() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.note_send("Video", 100);
        t.note_send("Audio", 50);
        assert_eq!(t.bytes_sent(), 150);
    }
}
