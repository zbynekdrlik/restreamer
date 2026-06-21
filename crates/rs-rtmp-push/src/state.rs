//! Pusher transport state. See spec §5.1.

use tokio::time::Instant;

/// Per-`RtmpPusher` runtime state. Owns connection-lifetime data (TCP session +
/// monotonic output timestamp + reconnect counter). Retry-policy state
/// (`consecutive_errors`, `last_error_class`) lives in the *caller*
/// (`endpoint_task`) — same boundary as today's split between `FfmpegProcess`
/// and `EndpointRestartState`.
#[derive(Default)]
pub struct PusherState {
    /// Highest output timestamp seen across all tracks. Used by the consumer
    /// task as the "reconnect-count" companion metric and to decide whether
    /// a fresh connect is a true reconnect (any media has been sent before).
    pub last_output_ts_ms: u64,
    /// Total reconnects since the pusher was created. Surfaced as the
    /// dashboard `reconnect_count` metric (replaces `ffmpeg_restart_count`).
    pub reconnect_count: u32,
    /// `true` while a TCP+RTMP session is open and bytes can flow. `false`
    /// after `connect()` failed or after a mid-stream error dropped the
    /// session. Lazy reconnect on next `push_flv_bytes`.
    pub connected: bool,
    /// Wall-clock anchor for `-re`-style pacing. Set on the first chunk we
    /// successfully push and reused across the whole pusher lifetime.
    /// Each tag's pacing target is its `output_ts` directly — same domain
    /// as `anchor.elapsed()`, so when up-to-date the pusher sleeps just
    /// long enough for wall-clock to catch up to the tag's PTS.
    pub pacing_anchor: Option<Instant>,
    /// `true` once an AVC sequence header has been forwarded on this RTMP
    /// session. The chunker re-emits the sequence header in EVERY S3 chunk
    /// (it must, so each chunk is a self-contained FLV file for ffmpeg's
    /// `-re -f flv -i pipe:`), but a real RTMP server expects it exactly
    /// once per session — re-sending it can cause the receiver to reset
    /// its decoder or pause ingestion, and was observed to drop the rust
    /// pusher's effective output to ~0.2 x real-time (#103).
    pub avc_seq_header_sent: bool,
    /// Same as `avc_seq_header_sent` but for AAC.
    pub aac_seq_header_sent: bool,
    /// FLV ts of the FIRST audio tag seen on the current RTMP session.
    /// Each subsequent audio tag's wire `output_ts` is computed as
    /// `audio_base + (tag.ts - audio_origin)`, keeping audio on its OWN
    /// continuous timeline that exactly preserves xiu's audio cadence
    /// (~21 ms between AAC frames). Resets to `None` on reconnect so the
    /// new session anchors fresh; `audio_base` carries the cumulative
    /// offset so the wire timeline stays monotonic across reconnects.
    /// Without per-track timelines, audio frames at chunk boundaries
    /// landed on the same `output_ts` as the last frame of the previous
    /// chunk → audible click at every chunk boundary (#103).
    pub audio_origin_xiu_ts: Option<u32>,
    /// Per-track output_ts base for AUDIO. Carried across reconnects so
    /// the wire timeline never goes backwards even when xiu's RTMP
    /// session resets to 0 on the upstream reconnect.
    pub audio_base_ms: u64,
    /// Highest audio `output_ts` actually sent. Used to advance
    /// `audio_base_ms` on reconnect (`audio_base_ms = max + 1`).
    pub last_audio_output_ts_ms: u64,
    /// FLV ts of the FIRST video tag seen on the current RTMP session.
    /// See `audio_origin_xiu_ts`.
    pub video_origin_xiu_ts: Option<u32>,
    /// Per-track output_ts base for VIDEO.
    pub video_base_ms: u64,
    /// Highest video `output_ts` actually sent.
    pub last_video_output_ts_ms: u64,
    /// Times this pusher has detected upstream-chunker timestamp regression
    /// (`tag.xiu_ts < last_*_xiu_ts`) and re-anchored. Mirrors
    /// `reconnect_count` for visibility — useful for alerting on
    /// stream.lan crashes / chunker resets that the operator might
    /// otherwise miss (the RTMP-to-YouTube session stays alive through
    /// these events, so reconnect_count alone wouldn't move).
    pub regression_reanchor_count: u32,
    /// xiu FLV ts of the previous non-seq-header AUDIO tag we processed.
    /// Used to detect chunker-side timestamp regression — when stream.lan
    /// crashes/restarts but our RTMP session to YouTube stays alive, the
    /// chunker resumes with xiu_ts ~0 even though we'd previously been
    /// pushing tags at xiu_ts ~600_000. Without re-anchoring, the next
    /// `output_ts` would be `audio_base_ms + 0` — strictly less than the
    /// last `output_ts` we sent, breaking PTS monotonicity on the wire
    /// and causing YouTube to drop the stream (#103 production test
    /// 2026-04-30: stream went `active/good` → `inactive/noData` after
    /// the crash-recovery resilience test). Detection roll forward
    /// `audio_base_ms`, clears `audio_origin_xiu_ts`, and re-anchors on
    /// the regressed tag — strictly monotonic on the wire, RTMP session
    /// preserved.
    pub last_audio_xiu_ts: Option<u32>,
    /// Same as `last_audio_xiu_ts` but for video.
    pub last_video_xiu_ts: Option<u32>,
}

/// Which track tripped a backward / large-forward timestamp jump and is
/// requesting a re-anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Track {
    Audio,
    Video,
}

impl PusherState {
    /// Re-anchor on a chunker-side timestamp anomaly (backward regression or a
    /// large forward jump) detected on `tripped`.
    ///
    /// **Symmetric re-anchor (issue #257):** when EITHER track trips, BOTH
    /// tracks re-anchor to a single shared base on the same tag instant
    /// (`max(last_audio_output, last_video_output) + 1`) and BOTH origins are
    /// cleared so the next tag on each track re-pins from the new common base.
    ///
    /// Why symmetric: the previous per-track re-anchor bumped only the tripping
    /// track's base, leaving the other track on its old base. If the two tracks
    /// had drifted to unequal `last_*_output_ts_ms` (independent reconnect /
    /// rescue→resume / republish boundaries — see #255 / #249), re-anchoring one
    /// track FROZE that inter-track offset into the wire timeline. Re-anchoring
    /// both to the shared base collapses the offset to zero at the re-anchor
    /// instant, which is the only point where audio and video content are known
    /// to be coincident (the chunker flushes both on the same boundary).
    ///
    /// The shared base is `max + 1` (not `min + 1`) so the wire timeline stays
    /// strictly monotonic on BOTH tracks — neither can step backward past a
    /// timestamp already sent.
    pub fn reanchor(&mut self, tripped: Track) {
        let shared_base = self
            .last_audio_output_ts_ms
            .max(self.last_video_output_ts_ms)
            .saturating_add(1);
        self.audio_base_ms = shared_base;
        self.video_base_ms = shared_base;
        self.audio_origin_xiu_ts = None;
        self.video_origin_xiu_ts = None;
        self.regression_reanchor_count = self.regression_reanchor_count.saturating_add(1);
        // `tripped` retained in the signature for call-site clarity / logging;
        // both tracks re-anchor regardless of which one detected the anomaly.
        let _ = tripped;
    }
}

#[derive(Clone)]
pub struct PusherConfig {
    /// Per-call socket-write timeout in ms. Default 30_000 (matches today's
    /// `crates/rs-delivery/src/endpoint_task.rs::WRITE_TIMEOUT_SECS`).
    pub timeout_ms: u64,
}

impl Default for PusherConfig {
    fn default() -> Self {
        Self { timeout_ms: 30_000 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Symmetric re-anchor (#257): when AUDIO trips, BOTH bases move to the
    /// shared `max(last_audio_output, last_video_output) + 1` and BOTH origins
    /// clear — so a drifted inter-track offset collapses instead of freezing.
    #[test]
    fn reanchor_audio_moves_both_bases_to_shared_max_plus_one() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 630_000,
            last_video_output_ts_ms: 600_000,
            audio_base_ms: 630_000,
            video_base_ms: 600_000,
            audio_origin_xiu_ts: Some(1),
            video_origin_xiu_ts: Some(2),
            ..PusherState::default()
        };
        state.reanchor(Track::Audio);
        assert_eq!(state.audio_base_ms, 630_001, "audio base = max+1");
        assert_eq!(
            state.video_base_ms, 630_001,
            "video base must ALSO move to the shared max+1 (symmetric)"
        );
        assert!(state.audio_origin_xiu_ts.is_none());
        assert!(state.video_origin_xiu_ts.is_none());
        assert_eq!(state.regression_reanchor_count, 1);
    }

    /// Symmetric re-anchor when VIDEO trips: same shared base for both tracks.
    /// Here video is the higher track, so max+1 derives from it.
    #[test]
    fn reanchor_video_moves_both_bases_to_shared_max_plus_one() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 500_000,
            last_video_output_ts_ms: 800_000,
            audio_base_ms: 500_000,
            video_base_ms: 800_000,
            ..PusherState::default()
        };
        state.reanchor(Track::Video);
        assert_eq!(state.audio_base_ms, 800_001);
        assert_eq!(state.video_base_ms, 800_001);
        assert_eq!(state.regression_reanchor_count, 1);
    }

    /// The shared base is `max + 1` (NOT `min + 1`) so neither track's wire
    /// timeline can step backward past a timestamp already sent.
    #[test]
    fn reanchor_uses_max_not_min_for_monotonicity() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 100,
            last_video_output_ts_ms: 999_999,
            ..PusherState::default()
        };
        state.reanchor(Track::Audio);
        assert_eq!(
            state.audio_base_ms, 1_000_000,
            "must use the LARGER of the two last-outputs + 1, never the smaller"
        );
        assert_eq!(state.video_base_ms, 1_000_000);
    }
}
