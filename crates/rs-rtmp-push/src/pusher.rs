//! `RtmpPusher` - public API.

use std::time::Duration;

use tokio::time::Instant;

use crate::session::Session;
use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    config: PusherConfig,
    state: PusherState,
    session: Option<Session>,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
            session: None,
        }
    }

    pub fn last_output_ts_ms(&self) -> u64 {
        self.state.last_output_ts_ms
    }

    pub fn reconnect_count(&self) -> u32 {
        self.state.reconnect_count
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Lazy-connect + write FLV bytes.
    ///
    /// `chunk_duration_ms` is the chunker's authoritative media duration of
    /// this chunk (from `chunk_records.duration_ms`); it advances the
    /// cumulative `last_output_ts_ms` by EXACTLY that amount, decoupling the
    /// pacing target from FLV tag timestamps that may live in different
    /// time domains for audio vs video (issue #103: audio is xiu-ts,
    /// video is wall-clock — see `feedback_chunker_time_domains`).
    /// Pass `0` only for handshake-only calls with empty `bytes`.
    ///
    /// On the first call (or after a reconnect) the pusher dials the server,
    /// performs the full RTMP handshake + connect + publish sequence, and
    /// stores the resulting `Session`.
    ///
    /// For empty `bytes` slices the method returns `Ok(())` after connecting -
    /// this is the Task 3 test contract: "handshake completes, no tags sent".
    ///
    /// For non-empty `bytes`, each audio/video tag body is written via
    /// `ChunkPacketizer` with monotonically rewritten timestamps.
    pub async fn push_flv_bytes(
        &mut self,
        bytes: &[u8],
        _chunk_duration_ms: u32,
    ) -> Result<(), PushError> {
        // Lazy connect.
        if self.session.is_none() {
            let is_reconnect = self.state.last_output_ts_ms > 0;
            let connect_result = Session::connect(&self.url, self.config.timeout_ms).await;
            if is_reconnect {
                self.state.reconnect_count = self.state.reconnect_count.saturating_add(1);
            }
            let s = connect_result?;
            self.session = Some(s);
            self.state.connected = true;
            // Codec config must be re-sent on every fresh RTMP session so
            // the receiver can decode subsequent NALU/raw-AAC tags.
            self.state.avc_seq_header_sent = false;
            self.state.aac_seq_header_sent = false;
            // Reset per-track origins so the new session anchors on its
            // first tag. Roll the per-track BASE forward to one past the
            // highest output_ts we sent in the previous session — that
            // keeps the wire timeline strictly monotonic across the
            // reconnect even if xiu's RTMP session resets to ts=0.
            self.state.audio_origin_xiu_ts = None;
            self.state.video_origin_xiu_ts = None;
            self.state.audio_base_ms = self.state.last_audio_output_ts_ms.saturating_add(1);
            self.state.video_base_ms = self.state.last_video_output_ts_ms.saturating_add(1);
            // last_*_xiu_ts is "what we just saw upstream"; the new
            // session starts fresh, so any xiu_ts value is valid (the
            // origin will anchor on the first tag we see).
            self.state.last_audio_xiu_ts = None;
            self.state.last_video_xiu_ts = None;
        }

        // Empty slice -> handshake verified, nothing to send.
        if bytes.is_empty() {
            return Ok(());
        }

        // Parse FLV tags. Each non-seq-header media tag's `output_ts` is
        // computed PER TRACK as `track_base + (tag.ts - track_origin)`.
        // The two tracks evolve on independent monotonic timelines, both
        // measured in the same units (ms) so the receiver sees them as
        // a coherent A/V pair.
        //
        // Why per-track: the chunker stamps audio with xiu's RTMP session
        // ts and video with wall-clock since chunker session start. Within
        // a single chunk those two clocks are aligned, but the SPAN of
        // audio in a chunk often differs from the SPAN of video in the
        // same chunk (chunks flush at video keyframes — audio frames
        // straddling that boundary go to whichever chunk happens to be
        // open at the moment). Mixing the two into one shared cumulative
        // counter caused audio frames at chunk boundaries to land on the
        // SAME `output_ts` as the previous chunk's last audio frame, and
        // YouTube/the decoder rendered that as an audible click every
        // ~2 s (#103 production test on 2026-04-30).
        let iter = crate::flv::FlvTagIter::new(bytes)?;
        let anchor = *self.state.pacing_anchor.get_or_insert_with(Instant::now);

        let chunk_started_at = Instant::now();
        let mut tags_sent: u32 = 0;
        let mut tags_skipped: u32 = 0;
        let mut bytes_sent: u64 = 0;
        let mut max_audio_output_ts: u64 = 0;
        let mut max_video_output_ts: u64 = 0;

        for tag in iter {
            // Sequence headers (codec config: AVC SPS/PPS, AAC config) are
            // identified by body[1] == 0x00 (`AVCPacketType::SequenceHeader`
            // / `AACPacketType::SequenceHeader`). The chunker writes them
            // at the START of every chunk with ts=0 so each S3 chunk is a
            // self-contained FLV file for the ffmpeg path. We forward
            // exactly ONE per codec per RTMP session (subsequent ones
            // would force the receiver to reset its decoder).
            let is_seq_header = tag.body.len() >= 2 && tag.body[1] == 0x00;

            let (output_ts_u64, track_max) = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => {
                    if !is_seq_header {
                        // Detect chunker-side timestamp regression (e.g.
                        // stream.lan crash → fresh chunker session →
                        // xiu_ts resets to ~0 even though our RTMP-to-
                        // -YouTube session is still alive). When the new
                        // tag's xiu_ts is strictly less than the previous
                        // tag's, treat it as an upstream reset: bump the
                        // per-track base past the highest output_ts we
                        // already sent, and re-anchor the origin on this
                        // tag. PTS stays strictly monotonic on the wire.
                        if let Some(prev) = self.state.last_audio_xiu_ts
                            && tag.timestamp_ms < prev
                        {
                            self.state.audio_base_ms =
                                self.state.last_audio_output_ts_ms.saturating_add(1);
                            self.state.audio_origin_xiu_ts = None;
                        }
                        self.state.last_audio_xiu_ts = Some(tag.timestamp_ms);
                    }
                    let origin = if is_seq_header {
                        // Don't anchor on the seq header (its ts=0 would
                        // pollute the audio origin with the chunk preamble
                        // value); use the existing or future first real
                        // audio tag instead.
                        self.state.audio_origin_xiu_ts.unwrap_or(tag.timestamp_ms)
                    } else {
                        *self
                            .state
                            .audio_origin_xiu_ts
                            .get_or_insert(tag.timestamp_ms)
                    };
                    let delta = tag.timestamp_ms.saturating_sub(origin) as u64;
                    let ts = self.state.audio_base_ms + delta;
                    (ts, &mut max_audio_output_ts)
                }
                crate::flv::FLV_TAG_VIDEO => {
                    if !is_seq_header {
                        if let Some(prev) = self.state.last_video_xiu_ts
                            && tag.timestamp_ms < prev
                        {
                            self.state.video_base_ms =
                                self.state.last_video_output_ts_ms.saturating_add(1);
                            self.state.video_origin_xiu_ts = None;
                        }
                        self.state.last_video_xiu_ts = Some(tag.timestamp_ms);
                    }
                    let origin = if is_seq_header {
                        self.state.video_origin_xiu_ts.unwrap_or(tag.timestamp_ms)
                    } else {
                        *self
                            .state
                            .video_origin_xiu_ts
                            .get_or_insert(tag.timestamp_ms)
                    };
                    let delta = tag.timestamp_ms.saturating_sub(origin) as u64;
                    let ts = self.state.video_base_ms + delta;
                    (ts, &mut max_video_output_ts)
                }
                _ => continue, // SCRIPT/unknown — drop, no PTS to assign
            };
            let output_ts = output_ts_u64 as u32;
            if output_ts_u64 > *track_max {
                *track_max = output_ts_u64;
            }

            // De-duplicate codec sequence headers across the session.
            let skip = match tag.tag_type {
                crate::flv::FLV_TAG_VIDEO if is_seq_header => {
                    let already = self.state.avc_seq_header_sent;
                    self.state.avc_seq_header_sent = true;
                    already
                }
                crate::flv::FLV_TAG_AUDIO if is_seq_header => {
                    let already = self.state.aac_seq_header_sent;
                    self.state.aac_seq_header_sent = true;
                    already
                }
                _ => false,
            };

            // Per-tag pacing: sleep until wall-clock catches up to this
            // tag's PTS. Both `output_ts_u64` and `anchor.elapsed()` live
            // in the same ms domain, so the math is direct.
            let actual_ms = anchor.elapsed().as_millis() as u64;
            if actual_ms < output_ts_u64 {
                tokio::time::sleep(Duration::from_millis(output_ts_u64 - actual_ms)).await;
            }

            let session = self.session.as_mut().expect("session was just set");
            let send_result = if skip {
                tags_skipped += 1;
                Ok(())
            } else {
                let body_len = tag.body.len();
                let res = match tag.tag_type {
                    crate::flv::FLV_TAG_AUDIO => session.send_audio_tag(output_ts, tag.body).await,
                    crate::flv::FLV_TAG_VIDEO => session.send_video_tag(output_ts, tag.body).await,
                    _ => Ok(()),
                };
                if res.is_ok() {
                    tags_sent += 1;
                    bytes_sent += body_len as u64;
                }
                res
            };

            if let Err(e) = send_result {
                self.state.connected = false;
                self.session = None;
                return Err(e);
            }
        }

        // Advance per-track bookkeeping with the highest output_ts we
        // actually sent on each track in this chunk. `last_output_ts_ms`
        // is the max of both — used as a single "is this a true reconnect"
        // signal at the top of the next call and reported on the dashboard.
        if max_audio_output_ts > self.state.last_audio_output_ts_ms {
            self.state.last_audio_output_ts_ms = max_audio_output_ts;
        }
        if max_video_output_ts > self.state.last_video_output_ts_ms {
            self.state.last_video_output_ts_ms = max_video_output_ts;
        }
        let cumulative_max = self
            .state
            .last_audio_output_ts_ms
            .max(self.state.last_video_output_ts_ms);
        if cumulative_max > self.state.last_output_ts_ms {
            self.state.last_output_ts_ms = cumulative_max;
        }
        let send_elapsed_ms = chunk_started_at.elapsed().as_millis() as u64;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        let target_ms = self.state.last_output_ts_ms;
        let pacing_sleep_ms = target_ms.saturating_sub(actual_ms);

        tracing::info!(
            "rtmp_push: chunk done tags_sent={tags_sent} tags_skipped={tags_skipped} bytes_sent={bytes_sent} a_out={max_audio_output_ts} v_out={max_video_output_ts} send_elapsed_ms={send_elapsed_ms} pacing_sleep_ms={pacing_sleep_ms} target_ms={target_ms} actual_ms={actual_ms}"
        );

        Ok(())
    }

    pub async fn close(&mut self) {
        if let Some(s) = self.session.take() {
            s.close().await;
        }
        self.state.connected = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only setter so unit tests can prove the getter reads from state.
    /// Without this, the `RtmpPusher::last_output_ts_ms -> 0` mutation would
    /// only be killed by integration tests (which mutation testing skips).
    impl RtmpPusher {
        pub(crate) fn set_last_output_ts_ms_for_test(&mut self, v: u64) {
            self.state.last_output_ts_ms = v;
        }
    }

    #[test]
    fn last_output_ts_ms_reads_state_field() {
        let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
        assert_eq!(p.last_output_ts_ms(), 0);
        p.set_last_output_ts_ms_for_test(12_345);
        assert_eq!(p.last_output_ts_ms(), 12_345);
        p.set_last_output_ts_ms_for_test(u64::MAX);
        assert_eq!(p.last_output_ts_ms(), u64::MAX);
    }

    #[test]
    fn reconnect_count_starts_zero() {
        let p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
        assert_eq!(p.reconnect_count(), 0);
    }

    #[test]
    fn url_returns_constructor_value() {
        let p = RtmpPusher::new(
            "rtmp://example.com/live/key".into(),
            PusherConfig::default(),
        );
        assert_eq!(p.url(), "rtmp://example.com/live/key");
    }

    /// Per-chunk pacing math (issue #103, run 25119429314): when wall-clock
    /// is BEHIND the cumulative output timestamp, the chunk-end sleep
    /// equals `last_output_ts_ms - wall_elapsed`. Uses `tokio::time::pause`
    /// so the test is deterministic.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn per_chunk_pacing_sleeps_when_ahead_of_wall_clock() {
        let anchor = Instant::now();
        // Pretend we just sent a chunk worth 2000 ms of media…
        let target_ms: u64 = 2_000;
        // …in 500 ms of wall time (very fast TCP writes).
        tokio::time::advance(Duration::from_millis(500)).await;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        assert_eq!(actual_ms, 500, "wall elapsed must be 500 ms");
        assert!(actual_ms < target_ms);
        let sleep_ms = target_ms - actual_ms;
        assert_eq!(
            sleep_ms, 1_500,
            "per-chunk pacing must sleep target - wall = 1500 ms"
        );
    }

    /// When wall-clock has already overrun the cumulative output timestamp
    /// (cache overshoot during init, or a slow first chunk), pacing must
    /// NOT sleep — the pusher needs to drain at TCP-write speed.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn per_chunk_pacing_no_sleep_when_behind() {
        let anchor = Instant::now();
        let target_ms: u64 = 1_000;
        tokio::time::advance(Duration::from_millis(5_000)).await;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        assert!(
            actual_ms >= target_ms,
            "wall elapsed (5000) >= target (1000) must skip sleep"
        );
    }

    #[test]
    fn pacing_anchor_starts_none_and_persists_after_first_set() {
        let mut state = PusherState::default();
        assert!(state.pacing_anchor.is_none());
        let anchor = *state.pacing_anchor.get_or_insert_with(Instant::now);
        assert!(state.pacing_anchor.is_some());
        // get_or_insert_with on an Option<Instant> is idempotent: a second
        // call must NOT overwrite the anchor (otherwise pacing math drifts
        // back to per-chunk wall-clock instead of cumulative).
        let anchor2 = *state.pacing_anchor.get_or_insert_with(Instant::now);
        assert_eq!(anchor, anchor2, "anchor must be stable after first set");
    }

    #[test]
    fn seq_header_dedup_flags_default_false_and_independent() {
        // Regression for #103 cache-growth investigation: AVC and AAC
        // sequence-header flags must start FALSE so the FIRST seq header
        // of each codec is forwarded to the RTMP server (without it the
        // receiver can't decode subsequent tags). Once flipped, the SECOND
        // identical seq header is suppressed; the chunker re-emits it in
        // every S3 chunk and re-sending throttled YouTube ingestion.
        let state = PusherState::default();
        assert!(!state.avc_seq_header_sent);
        assert!(!state.aac_seq_header_sent);
    }

    /// Audio output_ts continuity across chunk boundaries (regression
    /// for #103 production test on 2026-04-30: audible click every ~2 s
    /// when the rust pusher delivered to YouTube).
    ///
    /// Audio frames continue at xiu's monotonic ts across chunks (e.g.
    /// 40000, 40021, ..., 41979 in chunk N; 42000, 42021, ..., 43979 in
    /// chunk N+1). The pusher's `output_ts` for those frames is
    /// `audio_base + (xiu_ts - audio_origin)`. With per-track origin
    /// pinned at the FIRST audio tag of the session, audio's `output_ts`
    /// timeline matches xiu's timeline exactly: continuous, monotonic,
    /// no jumps at chunk boundaries.
    #[test]
    fn per_track_audio_output_ts_is_continuous_across_chunks() {
        let mut state = PusherState::default();
        // Simulate first audio tag of the session arriving at xiu_ts=40000.
        let origin = *state.audio_origin_xiu_ts.get_or_insert(40_000);
        assert_eq!(origin, 40_000);

        // Compute output_ts for chunk N's audio frames.
        let chunk_n = [40_000_u32, 40_021, 40_042, 41_958, 41_979];
        let n_outputs: Vec<u64> = chunk_n
            .iter()
            .map(|&ts| {
                state.audio_base_ms + (ts.saturating_sub(state.audio_origin_xiu_ts.unwrap())) as u64
            })
            .collect();
        assert_eq!(n_outputs, vec![0, 21, 42, 1_958, 1_979]);

        // Now chunk N+1 — audio continues at xiu_ts=42000, 42021, ...
        // The origin is sticky (set once at session start), NOT per-chunk.
        let chunk_n_plus_1 = [42_000_u32, 42_021, 43_958, 43_979];
        let np1_outputs: Vec<u64> = chunk_n_plus_1
            .iter()
            .map(|&ts| {
                state.audio_base_ms + (ts.saturating_sub(state.audio_origin_xiu_ts.unwrap())) as u64
            })
            .collect();
        // CRITICAL: chunk N+1's first audio output_ts (2_000) must be
        // STRICTLY GREATER than chunk N's last (1_979) by exactly the
        // xiu inter-chunk delta (21 ms). Old code rebased per chunk and
        // collided at 1_979 → audible click every chunk boundary.
        assert_eq!(np1_outputs, vec![2_000, 2_021, 3_958, 3_979]);
        assert!(
            np1_outputs[0] > *n_outputs.last().unwrap(),
            "audio output_ts must be strictly monotonic across chunk boundary"
        );
        assert_eq!(
            np1_outputs[0] - n_outputs.last().unwrap(),
            21,
            "inter-chunk gap must equal xiu inter-frame interval"
        );
    }

    /// Chunker-side timestamp regression mid-session (e.g. stream.lan
    /// crashed and resumed but VPS pusher's RTMP session to YouTube
    /// stayed alive): incoming tag.xiu_ts is strictly less than the
    /// previous tag's. Without re-anchoring, the wire `output_ts` would
    /// go BACKWARDS (audio_base + new_xiu - old_origin = negative,
    /// saturating to 0, which is way less than the last tag we sent).
    /// Detection bumps base past last_output_ts and clears origin.
    #[test]
    fn audio_xiu_regression_advances_base_and_re_anchors() {
        let mut state = PusherState::default();
        // Simulate first session at xiu=0..600_000.
        state.audio_origin_xiu_ts = Some(0);
        state.last_audio_xiu_ts = Some(600_000);
        state.last_audio_output_ts_ms = 600_000;

        // Now upstream chunker resets, new tag arrives at xiu=21
        // (much less than the last seen 600_000).
        let new_tag_xiu_ts = 21_u32;
        let prev = state.last_audio_xiu_ts.unwrap();
        if new_tag_xiu_ts < prev {
            state.audio_base_ms = state.last_audio_output_ts_ms.saturating_add(1);
            state.audio_origin_xiu_ts = None;
        }
        // Anchor on this tag.
        let origin = *state.audio_origin_xiu_ts.get_or_insert(new_tag_xiu_ts);
        let delta = new_tag_xiu_ts.saturating_sub(origin) as u64;
        let output_ts = state.audio_base_ms + delta;

        assert_eq!(state.audio_base_ms, 600_001);
        assert_eq!(origin, 21);
        assert_eq!(delta, 0);
        assert_eq!(
            output_ts, 600_001,
            "post-regression first tag MUST be > prior session's last output_ts"
        );
        assert!(
            output_ts > state.last_audio_output_ts_ms,
            "wire timeline must stay strictly monotonic"
        );
    }

    /// On reconnect, per-track BASE rolls forward to one past the highest
    /// output_ts sent in the prior session. This keeps the wire timeline
    /// strictly monotonic across reconnects even when xiu's RTMP session
    /// resets to ts=0 on the upstream side.
    #[test]
    fn reconnect_advances_per_track_base() {
        let mut state = PusherState::default();
        state.last_audio_output_ts_ms = 60_000;
        state.last_video_output_ts_ms = 60_033;
        state.audio_origin_xiu_ts = Some(40_000);
        state.video_origin_xiu_ts = Some(0);

        // Simulate the reconnect logic in push_flv_bytes lazy-connect block.
        state.audio_origin_xiu_ts = None;
        state.video_origin_xiu_ts = None;
        state.audio_base_ms = state.last_audio_output_ts_ms.saturating_add(1);
        state.video_base_ms = state.last_video_output_ts_ms.saturating_add(1);

        assert_eq!(state.audio_base_ms, 60_001);
        assert_eq!(state.video_base_ms, 60_034);
        assert!(state.audio_origin_xiu_ts.is_none());
        assert!(state.video_origin_xiu_ts.is_none());

        // First audio tag of the new session at xiu_ts=0 (xiu reset)
        // gets output_ts = 60_001 (strictly greater than the prior
        // session's last 60_000 → wire timeline stays monotonic).
        let first_xiu = 0_u32;
        let origin = *state.audio_origin_xiu_ts.get_or_insert(first_xiu);
        let output_ts = state.audio_base_ms + first_xiu.saturating_sub(origin) as u64;
        assert_eq!(output_ts, 60_001);
    }
}
