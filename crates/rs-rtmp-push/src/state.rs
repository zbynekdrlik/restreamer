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
    /// #367: the INPUT (chunk) ts that maps to `base_ms` on the wire, SHARED
    /// by both tracks. Every media tag goes through ONE common transform:
    /// `wire = base_ms + (tag.ts - origin_ts)`, so the wire A/V relation is
    /// exactly the content relation the chunker produced. `None` after a
    /// reconnect / re-anchor; the next tag re-pins it with `robust_pin`: the
    /// minimum ts of the chunk's remaining media tags ON the local timeline,
    /// so no tag of that chunk maps below `base_ms` and one corrupt ts can
    /// never drag the origin.
    ///
    /// Before #367 each track re-pinned its OWN origin on its first tag, so a
    /// chunk whose audio started 700 ms after its keyframe sent both at the
    /// same wire instant: the wire offset depended on pusher history, not on
    /// the content. Cross-chunk continuity (the #103 click fix) is unchanged:
    /// the origin is per mapping, never per chunk.
    pub origin_ts: Option<u32>,
    /// #367: the shared wire base for BOTH tracks. On a reconnect / re-anchor
    /// it moves to `max(last_audio_output, last_video_output) + 1`, so
    /// neither track's wire timeline can step back (#103 / #257).
    pub base_ms: u64,
    /// Highest audio `output_ts` actually sent.
    pub last_audio_output_ts_ms: u64,
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
    /// `output_ts` would be `base_ms + 0` — strictly less than the last
    /// `output_ts` we sent, breaking PTS monotonicity on the wire and
    /// causing YouTube to drop the stream (#103 production test
    /// 2026-04-30: stream went `active/good` → `inactive/noData` after the
    /// crash-recovery resilience test). Detection re-anchors BOTH tracks
    /// (`reanchor`) on the regressed tag — strictly monotonic on the wire,
    /// RTMP session preserved.
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
    /// Start a NEW shared wire mapping for both tracks (a fresh RTMP session
    /// or a re-anchor): the base moves to one past the highest output ts sent
    /// on either track, the shared origin is cleared so the next tag re-pins
    /// it, and the per-track "last input ts" trackers are cleared so the new
    /// mapping does not immediately trip the jump detector again.
    pub fn begin_new_mapping(&mut self) {
        self.base_ms = self
            .last_audio_output_ts_ms
            .max(self.last_video_output_ts_ms)
            .saturating_add(1);
        self.origin_ts = None;
        self.last_audio_xiu_ts = None;
        self.last_video_xiu_ts = None;
    }

    /// Re-anchor on a chunker-side timestamp anomaly (backward regression or a
    /// large forward jump) detected on `tripped`.
    ///
    /// **Symmetric (issue #257), one shared transform (issue #367):** when
    /// EITHER track trips, BOTH tracks move to the single shared base
    /// `max(last_audio_output, last_video_output) + 1` and the shared origin
    /// is cleared, so the next tag re-pins ONE origin for both tracks. The
    /// wire relation after the re-anchor is therefore exactly the content
    /// relation of the new tags (#257 collapsed it to 0 instead, which was
    /// still a pusher-made offset whenever the new chunk's tracks did not
    /// start together).
    ///
    /// The shared base is `max + 1` (not `min + 1`) so the wire timeline stays
    /// strictly monotonic on BOTH tracks — neither can step backward past a
    /// timestamp already sent.
    pub fn reanchor(&mut self, tripped: Track) {
        self.begin_new_mapping();
        self.regression_reanchor_count = self.regression_reanchor_count.saturating_add(1);
        // `tripped` retained in the signature for call-site clarity / logging;
        // both tracks re-anchor regardless of which one detected the anomaly.
        let _ = tripped;
    }

    /// Map one media tag's INPUT ts onto the wire with the shared transform,
    /// pinning the shared origin to `pin_ts` if no mapping is active.
    /// `pin_ts` is the pusher's `robust_pin`: the minimum input ts of the
    /// chunk's remaining media tags on the local timeline, so none of them
    /// maps below `base_ms`. (An off-timeline outlier is clamped by the
    /// pusher and never reaches this.)
    pub fn wire_ts(&mut self, input_ts: u32, pin_ts: u32) -> u64 {
        let origin = *self.origin_ts.get_or_insert(pin_ts);
        self.base_ms + u64::from(input_ts.saturating_sub(origin))
    }

    /// Like [`Self::wire_ts`] but never pins the origin (codec sequence
    /// headers carry ts 0 and must not anchor the mapping).
    pub fn wire_ts_unpinned(&self, input_ts: u32, pin_ts: u32) -> u64 {
        let origin = self.origin_ts.unwrap_or(pin_ts);
        self.base_ms + u64::from(input_ts.saturating_sub(origin))
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

    /// Symmetric re-anchor (#257) with one shared transform (#367): when
    /// AUDIO trips, the ONE shared base moves to `max(last outputs) + 1`, the
    /// shared origin clears, and so do both per-track jump trackers.
    #[test]
    fn reanchor_moves_shared_base_to_max_plus_one_and_clears_origin() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 630_000,
            last_video_output_ts_ms: 600_000,
            base_ms: 1,
            origin_ts: Some(42),
            last_audio_xiu_ts: Some(630_000),
            last_video_xiu_ts: Some(600_000),
            ..PusherState::default()
        };
        state.reanchor(Track::Audio);
        assert_eq!(state.base_ms, 630_001, "shared base = max + 1");
        assert!(state.origin_ts.is_none(), "shared origin must re-pin");
        assert!(state.last_audio_xiu_ts.is_none());
        assert!(
            state.last_video_xiu_ts.is_none(),
            "the OTHER track's jump tracker must clear too, or its first new tag \
             trips a second re-anchor"
        );
        assert_eq!(state.regression_reanchor_count, 1);
    }

    /// Same when VIDEO trips: video is the higher track here, so max + 1
    /// derives from it.
    #[test]
    fn reanchor_on_video_uses_the_same_shared_base() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 500_000,
            last_video_output_ts_ms: 800_000,
            ..PusherState::default()
        };
        state.reanchor(Track::Video);
        assert_eq!(state.base_ms, 800_001);
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
            state.base_ms, 1_000_000,
            "must use the LARGER of the two last-outputs + 1, never the smaller"
        );
    }

    /// A new mapping (reconnect) does not count as a re-anchor.
    #[test]
    fn begin_new_mapping_does_not_count_as_reanchor() {
        let mut state = PusherState {
            last_audio_output_ts_ms: 60_000,
            last_video_output_ts_ms: 60_033,
            origin_ts: Some(40_000),
            ..PusherState::default()
        };
        state.begin_new_mapping();
        assert_eq!(state.base_ms, 60_034);
        assert!(state.origin_ts.is_none());
        assert_eq!(state.regression_reanchor_count, 0);
    }

    /// #367: ONE origin for both tracks. A mapping pinned at the chunk's
    /// minimum (the keyframe at 1_000) keeps audio 700 ms after it on the
    /// wire -- the per-track origins mapped both first tags to the base.
    #[test]
    fn wire_ts_maps_both_tracks_through_one_origin() {
        let mut state = PusherState {
            base_ms: 5_000,
            ..PusherState::default()
        };
        let v = state.wire_ts(1_000, 1_000);
        let a = state.wire_ts(1_700, 1_000);
        assert_eq!(v, 5_000);
        assert_eq!(a, 5_700, "wire relation must equal the content relation");
        assert_eq!(state.origin_ts, Some(1_000));
    }

    /// The origin is per MAPPING, never per chunk: consecutive chunks keep a
    /// continuous wire timeline per track (the #103 click fix), even though a
    /// later chunk's pin would be different.
    #[test]
    fn wire_ts_is_continuous_across_chunks() {
        let mut state = PusherState::default();
        let chunk_n: Vec<u64> = [40_000_u32, 40_021, 41_979]
            .iter()
            .map(|&ts| state.wire_ts(ts, 40_000))
            .collect();
        let chunk_n1: Vec<u64> = [42_000_u32, 42_021]
            .iter()
            .map(|&ts| state.wire_ts(ts, 42_000))
            .collect();
        assert_eq!(chunk_n, vec![0, 21, 1_979]);
        assert_eq!(
            chunk_n1,
            vec![2_000, 2_021],
            "the next chunk must continue the same mapping (no per-chunk rebase)"
        );
    }

    /// A codec sequence header (ts 0 in every chunk) must never pin the
    /// shared origin.
    #[test]
    fn wire_ts_unpinned_never_pins_the_origin() {
        let state = PusherState {
            base_ms: 10,
            ..PusherState::default()
        };
        assert_eq!(state.wire_ts_unpinned(0, 5_000), 10);
        assert!(state.origin_ts.is_none());
    }

    /// With a mapping active, an unpinned tag goes through the SAME shared
    /// transform as any media tag.
    #[test]
    fn wire_ts_unpinned_uses_the_active_mapping() {
        let state = PusherState {
            base_ms: 10,
            origin_ts: Some(1_000),
            ..PusherState::default()
        };
        assert_eq!(state.wire_ts_unpinned(1_500, 5_000), 510);
        assert_eq!(state.origin_ts, Some(1_000), "unchanged");
    }
}
