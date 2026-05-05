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

/// Minimum wallclock interval between consecutive tag writes during
/// catch-up. Caps the burst push rate so a post-RemoteClosed recovery
/// doesn't overwhelm the upstream's TCP receive buffer (issue #171
/// follow-up — v0.3.92 unbounded catch-up cascaded into BytesWriteError
/// loops on 4 YT_RTMP endpoints during the 2026-05-04 soak).
///
/// 5 ms = max ~200 tags/sec wallclock. 12 Mbit RTMP at ~110 tags/sec
/// gives ~1.8x real-time burst, which empties a 30 s gap in ~17 s of
/// wallclock without saturating TCP.
pub const MIN_TAG_INTERVAL_MS: u64 = 5;

/// Pure pacing helper. Returns ms to sleep before pushing the next tag.
///
/// Three regimes:
/// 1. `wall_elapsed < output_ts` — output AHEAD of wall: real-time pacing,
///    sleep until wall aligns.
/// 2. `wall_elapsed - output_ts > CATCHUP_THRESHOLD_MS` — actively in
///    catch-up territory after a gap: enforce `min_tag_interval_ms`
///    between writes so the burst doesn't drown TCP.
/// 3. Otherwise (output near wall) — push immediately. Normal real-time
///    pushes don't pay the per-tag spacing tax (kept the steady-state
///    behavior identical to v0.3.91 to keep TLS / xiu loopback tests
///    happy on Windows runners).
pub const CATCHUP_THRESHOLD_MS: u64 = 100;

pub fn next_tag_sleep_ms(
    output_ts_ms: u64,
    wall_elapsed_ms: u64,
    last_tag_write_elapsed_ms: Option<u64>,
    min_tag_interval_ms: u64,
) -> u64 {
    if wall_elapsed_ms < output_ts_ms {
        return output_ts_ms - wall_elapsed_ms;
    }
    let lag = wall_elapsed_ms - output_ts_ms;
    if lag > CATCHUP_THRESHOLD_MS {
        if let Some(elapsed) = last_tag_write_elapsed_ms
            && elapsed < min_tag_interval_ms
        {
            return min_tag_interval_ms - elapsed;
        }
    }
    0
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
    /// On the first call (or after a reconnect) the pusher dials the server,
    /// performs the full RTMP handshake + connect + publish sequence, and
    /// stores the resulting `Session`.
    ///
    /// For empty `bytes` slices the method returns `Ok(())` after connecting -
    /// this is the Task 3 test contract: "handshake completes, no tags sent".
    ///
    /// For non-empty `bytes`, each audio/video tag body is written via
    /// `ChunkPacketizer` with per-track monotonically rewritten timestamps.
    /// Each track (audio xiu-ts vs video wall-clock — see
    /// `feedback_chunker_time_domains`) carries its own cumulative
    /// `output_ts` so chunk boundaries don't introduce cross-track
    /// collisions or audible clicks (#103).
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
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
            // Roll the per-track BASE forward to `last_output_ts + 1`.
            // Wire monotonicity preserved (every tag's output_ts is
            // strictly greater than the previous one). When wall has
            // run ahead during a RemoteClosed gap, output_ts starts
            // BEHIND wall → per-tag pacing skips real-time sleep AND
            // enforces `MIN_TAG_INTERVAL_MS` per-tag spacing instead
            // → bounded catch-up burst. The previous unbounded form
            // (v0.3.92) caused TCP write cascades on YT (issue #171
            // follow-up).
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
                        // tag's, treat it as an upstream reset and re-
                        // anchor: bump the per-track base to match wall
                        // clock (so subsequent pacing doesn't overshoot
                        // and freeze the pusher) while staying strictly
                        // greater than the highest output_ts already
                        // sent (so the wire timeline never goes
                        // backwards). The previous "+ 1" formulation
                        // froze the pusher in #103 resilience-test (the
                        // pusher's catch-up burst would advance output_ts
                        // by chunk_duration_ms × N chunks while
                        // anchor.elapsed only advanced by N × ~200 ms,
                        // and pacing then slept for the difference,
                        // exceeding the 30 s consumer-task write
                        // timeout).
                        if let Some(prev) = self.state.last_audio_xiu_ts
                            && tag.timestamp_ms < prev
                        {
                            self.state.audio_base_ms =
                                self.state.last_audio_output_ts_ms.saturating_add(1);
                            self.state.audio_origin_xiu_ts = None;
                            self.state.regression_reanchor_count =
                                self.state.regression_reanchor_count.saturating_add(1);
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
                            self.state.regression_reanchor_count =
                                self.state.regression_reanchor_count.saturating_add(1);
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

            // Per-tag pacing: real-time when output_ts ahead of wall;
            // bounded burst (cap at MIN_TAG_INTERVAL_MS) when output_ts
            // is behind wall (catch-up after a RemoteClosed gap).
            // See `next_tag_sleep_ms` doc + issue #171 follow-up.
            let actual_ms = anchor.elapsed().as_millis() as u64;
            let last_write_elapsed_ms = self
                .state
                .last_tag_write_at
                .map(|t| t.elapsed().as_millis() as u64);
            let sleep_ms = next_tag_sleep_ms(
                output_ts_u64,
                actual_ms,
                last_write_elapsed_ms,
                MIN_TAG_INTERVAL_MS,
            );
            if sleep_ms > 0 {
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
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
                    self.state.last_tag_write_at = Some(Instant::now());
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
        // Per-tag pacing already drained inside the loop — by chunk end the
        // residual is normally 0–10 ms. Renamed from `pacing_sleep_ms` to
        // make clear that the pusher does NOT sleep this long here; the
        // value just shows how far ahead/behind wall-clock the chunk
        // ended up.
        let pacing_residual_ms = target_ms.saturating_sub(actual_ms);
        let regression_reanchor_count = self.state.regression_reanchor_count;

        tracing::info!(
            "rtmp_push: chunk done tags_sent={tags_sent} tags_skipped={tags_skipped} bytes_sent={bytes_sent} a_out={max_audio_output_ts} v_out={max_video_output_ts} send_elapsed_ms={send_elapsed_ms} pacing_residual_ms={pacing_residual_ms} target_ms={target_ms} actual_ms={actual_ms} reanchor={regression_reanchor_count}"
        );

        Ok(())
    }

    /// Number of times this pusher has detected an upstream chunker
    /// timestamp regression and re-anchored its per-track base. Mirrors
    /// `reconnect_count()` for visibility — alerts can fire on a non-zero
    /// value to investigate stream.lan crashes / chunker resets that
    /// the operator might otherwise miss.
    pub fn regression_reanchor_count(&self) -> u32 {
        self.state.regression_reanchor_count
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

    /// Chunker-side timestamp regression mid-session: re-anchor base
    /// to `last_output_ts + 1` so per-tag pacing can burst-push the
    /// gap (bounded by MIN_TAG_INTERVAL_MS) without leaving permanent
    /// cache delay. See issue #171 follow-up.
    #[test]
    fn audio_xiu_regression_re_anchors_to_last_output_plus_one() {
        let mut state = PusherState::default();
        state.audio_origin_xiu_ts = Some(0);
        state.last_audio_xiu_ts = Some(600_000);
        state.last_audio_output_ts_ms = 600_000;

        let new_tag_xiu_ts = 21_u32;
        let prev = state.last_audio_xiu_ts.unwrap();
        if new_tag_xiu_ts < prev {
            state.audio_base_ms = state.last_audio_output_ts_ms.saturating_add(1);
            state.audio_origin_xiu_ts = None;
        }

        assert_eq!(
            state.audio_base_ms, 600_001,
            "base anchors on wire timeline (last+1), bounded burst handles the gap"
        );
    }

    /// On reconnect, per-track BASE rolls forward to `last_output_ts + 1`.
    /// Bounded burst (MIN_TAG_INTERVAL_MS in next_tag_sleep_ms) caps
    /// the catch-up rate so TCP doesn't drown.
    #[test]
    fn reconnect_advances_per_track_base_to_last_output_plus_one() {
        let mut state = PusherState::default();
        state.last_audio_output_ts_ms = 60_000;
        state.last_video_output_ts_ms = 60_033;
        state.audio_origin_xiu_ts = Some(40_000);
        state.video_origin_xiu_ts = Some(0);

        state.audio_origin_xiu_ts = None;
        state.video_origin_xiu_ts = None;
        state.audio_base_ms = state.last_audio_output_ts_ms.saturating_add(1);
        state.video_base_ms = state.last_video_output_ts_ms.saturating_add(1);

        assert_eq!(state.audio_base_ms, 60_001);
        assert_eq!(state.video_base_ms, 60_034);

        let first_xiu = 0_u32;
        let origin = *state.audio_origin_xiu_ts.get_or_insert(first_xiu);
        let output_ts = state.audio_base_ms + first_xiu.saturating_sub(origin) as u64;
        assert_eq!(output_ts, 60_001);
    }

    // --- next_tag_sleep_ms (bounded catch-up) ---

    #[test]
    fn next_tag_sleep_real_time_when_output_ahead_of_wall() {
        // Output 1000 ms ahead of wall → sleep 1000 ms.
        assert_eq!(next_tag_sleep_ms(2000, 1000, None, 5), 1000);
    }

    #[test]
    fn next_tag_sleep_zero_on_first_tag_when_behind_wall() {
        // Output behind wall, no prior write → push immediately.
        assert_eq!(next_tag_sleep_ms(500, 5000, None, 5), 0);
    }

    #[test]
    fn next_tag_sleep_enforces_min_interval_during_catchup() {
        // Output behind wall, previous write was 2 ms ago, MIN=5 →
        // sleep 3 ms. Caps burst rate at ~200 tags/sec.
        assert_eq!(next_tag_sleep_ms(500, 5000, Some(2), 5), 3);
    }

    #[test]
    fn next_tag_sleep_zero_during_catchup_after_min_elapsed() {
        // Output behind wall, previous write was 10 ms ago > MIN=5 →
        // push immediately.
        assert_eq!(next_tag_sleep_ms(500, 5000, Some(10), 5), 0);
    }

    #[test]
    fn next_tag_sleep_min_interval_boundary() {
        // Exactly at min interval → no extra sleep (kills off-by-one mutant).
        assert_eq!(next_tag_sleep_ms(500, 5000, Some(5), 5), 0);
    }

    #[test]
    fn next_tag_sleep_real_time_takes_priority_over_min_interval() {
        // Even with a recent write, real-time pacing wins when output
        // is ahead of wall (don't fall behind real-time just because
        // we wrote recently).
        assert_eq!(next_tag_sleep_ms(2000, 1000, Some(1), 5), 1000);
    }

    #[test]
    fn next_tag_sleep_zero_in_steady_state_within_catchup_threshold() {
        // Steady-state: output near wall (lag < CATCHUP_THRESHOLD_MS).
        // The min-interval tax is NOT applied here — normal real-time
        // pushes proceed without the catch-up spacing. Kills any
        // mutant that drops the threshold check and applies the tax
        // unconditionally (which broke the TLS loopback test on
        // Windows runners with the original v0.3.94 attempt).
        assert_eq!(
            next_tag_sleep_ms(99, 100, Some(1), 5),
            0,
            "lag of 1 ms is below CATCHUP_THRESHOLD_MS — no min-interval"
        );
        assert_eq!(
            next_tag_sleep_ms(0, 100, Some(1), 5),
            0,
            "lag of 100 ms exactly is at threshold — still no min-interval"
        );
    }

    #[test]
    fn next_tag_sleep_min_interval_kicks_in_just_past_threshold() {
        // Boundary: lag of 101 > threshold 100 → catch-up active →
        // min-interval enforced. Recent write (1 ms ago, < 5 min) →
        // sleep 4 ms.
        assert_eq!(
            next_tag_sleep_ms(0, 101, Some(1), 5),
            4,
            "lag of 101 ms is just past threshold — min-interval applies"
        );
    }
}
