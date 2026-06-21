//! `RtmpPusher` - public API.

use std::time::Duration;

use tokio::time::Instant;

use crate::session::Session;
use crate::skew::{SkewDecision, SkewTracker};
use crate::state::Track;
use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    config: PusherConfig,
    state: PusherState,
    session: Option<Session>,
    /// Cross-track A/V-skew detector (issue #257). Reset on each fresh RTMP
    /// session so skew is measured from the new common epoch.
    skew: SkewTracker,
}

/// Catch-up factor expressed as percent of real-time. 120 = at most 1.2×
/// real-time. Conservative enough that a 5 s rotation gap drains in ~25 s
/// wallclock without bursting upstream's TCP receive buffer (the failure
/// mode of v0.3.92's unbounded burst on YT and v0.3.94's 5 ms-per-tag
/// cap that still pushed 7× on YT and killed FB at chunk 3 — see
/// issue #171). 100 disables catch-up entirely (real-time only).
pub const CATCHUP_FACTOR_PCT: u64 = 120;

/// Forward-jump threshold for FLV tag timestamps. Anything past this
/// is treated as chunker-side glitch and triggers a re-anchor on the
/// wire timeline. Single source of truth for both audio + video tracks.
const MAX_TAG_TS_JUMP_MS: u32 = 30_000;

/// Pure pacing helper. Returns the wallclock ms to sleep at the END of a
/// push_flv_bytes call so the chunk's push rate is capped at
/// `catchup_factor_pct/100` × real-time.
///
/// Math: `target_wall = chunk_media_ms × 100 / catchup_factor_pct`. If
/// `actual_wall_ms < target_wall_ms`, sleep the residual; otherwise no
/// sleep.
///
/// Steady-state (per-tag pacing already paced at real-time, so
/// actual_wall ≈ chunk_media): the residual collapses to 0 and no
/// extra sleep happens. Only catch-up bursts (actual_wall ≪ chunk_media
/// because per-tag pacing skipped) trigger the rate cap.
///
/// `catchup_factor_pct == 0` disables the cap entirely (returns 0). 100
/// = exact real-time. >100 = allowed to push faster than real-time
/// (catch up).
pub fn chunk_pacing_sleep_ms(
    chunk_media_ms: u64,
    actual_wall_ms: u64,
    catchup_factor_pct: u64,
) -> u64 {
    if catchup_factor_pct == 0 {
        return 0;
    }
    let target_wall_ms = chunk_media_ms.saturating_mul(100) / catchup_factor_pct;
    target_wall_ms.saturating_sub(actual_wall_ms)
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
            session: None,
            skew: SkewTracker::default(),
        }
    }

    pub fn last_output_ts_ms(&self) -> u64 {
        self.state.last_output_ts_ms
    }

    pub fn reconnect_count(&self) -> u32 {
        self.state.reconnect_count
    }

    /// Current signed content-PTS A/V skew in ms (positive = audio behind
    /// video). Surfaced to per-endpoint telemetry so the dashboard and the
    /// #258 E2E gate can read it and alarm on a desync (issue #257).
    pub fn av_skew_ms(&self) -> i64 {
        self.skew.last_skew_ms()
    }

    /// Times this pusher has tripped the A/V-skew guard and forced a recovery
    /// reconnect (issue #257). Companion to `reconnect_count` for alerting.
    pub fn av_skew_trip_count(&self) -> u32 {
        self.skew.trip_count()
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
            // Roll the per-track BASE forward to the LATER of:
            //   1. one ms past the highest output_ts already sent
            //      (preserves wire monotonicity)
            //   2. wall-clock since pusher session start
            //      (keeps pacing from over-shooting after a long gap;
            //      the consumer's per-tag pacing math compares each
            //      tag's `output_ts` directly to `anchor.elapsed()`,
            //      and freezes the pusher if `output_ts` ever runs
            //      far ahead of wall — see the #103 resilience-test
            //      regression).
            // Issue #171: drop the wall-clock floor on reconnect base.
            // After a RemoteClosed gap, `last_output+1` lets per-tag
            // pacing skip sleep (output < wall), bursting buffered
            // chunks until output catches wall. The unbounded burst
            // problem (v0.3.92 cascaded YT TCP, v0.3.94 5ms-cap-killed
            // FB) is now mitigated by the chunk-end rate cap (see
            // `chunk_pacing_sleep_ms` and CATCHUP_FACTOR_PCT below).
            self.state.audio_base_ms = self.state.last_audio_output_ts_ms.saturating_add(1);
            self.state.video_base_ms = self.state.last_video_output_ts_ms.saturating_add(1);
            // last_*_xiu_ts is "what we just saw upstream"; the new
            // session starts fresh, so any xiu_ts value is valid (the
            // origin will anchor on the first tag we see).
            self.state.last_audio_xiu_ts = None;
            self.state.last_video_xiu_ts = None;
            // A fresh RTMP session re-anchors BOTH tracks from a common
            // start, so the A/V-skew detector must measure from the new
            // shared epoch (issue #257). This is also how the bounded
            // recovery converges: after the AvSkewExceeded reconnect, the
            // skew baseline resets and a transient desync clears.
            self.skew.reset_tracks();
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
        // Capture the per-track output_ts at chunk start so the chunk-end
        // rate cap (issue #171) knows how much MEDIA was sent, vs how
        // much wallclock elapsed during the push. Burst delivery during
        // catch-up advances chunk_media_ms much faster than wallclock,
        // which is the case the cap brakes.
        let chunk_start_audio_out = self.state.last_audio_output_ts_ms;
        let chunk_start_video_out = self.state.last_video_output_ts_ms;
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
                        // Detect both BACKWARD (regression) and large
                        // FORWARD jumps in tag.timestamp_ms. A forward
                        // jump of more than MAX_TAG_TS_JUMP_MS is treated
                        // as a chunker-side timestamp glitch (#176/#178:
                        // observed 720s forward jump in a single video
                        // tag → pacing slept 12min → 30s write timeout
                        // → upstream connection reset). Re-anchor on the
                        // wire timeline so output_ts steps by 1ms instead
                        // of the bad delta, while preserving monotonicity.
                        if let Some(prev) = self.state.last_audio_xiu_ts {
                            let backward = tag.timestamp_ms < prev;
                            let forward_jump =
                                tag.timestamp_ms.saturating_sub(prev) > MAX_TAG_TS_JUMP_MS;
                            if backward || forward_jump {
                                // Symmetric re-anchor (#257): re-anchor BOTH
                                // tracks to a shared base so a republish /
                                // reconnect boundary can't freeze an
                                // inter-track offset into the wire timeline.
                                self.state.reanchor(Track::Audio);
                                self.skew.reset_tracks();
                                tracing::warn!(
                                    prev_xiu_ts = prev,
                                    new_xiu_ts = tag.timestamp_ms,
                                    direction = if backward { "backward" } else { "forward" },
                                    shared_base = self.state.audio_base_ms,
                                    "rtmp_push: AUDIO tag.timestamp_ms anomaly -- symmetric re-anchor of BOTH tracks to shared base"
                                );
                            }
                        }
                        self.state.last_audio_xiu_ts = Some(tag.timestamp_ms);
                        // Feed the cross-track skew detector with the INPUT
                        // (chunker-stamped) PTS, before the per-track output
                        // re-anchor rewrites it (#257).
                        self.skew.observe_audio(tag.timestamp_ms);
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
                        // Detect BACKWARD + large FORWARD jumps. See
                        // matching audio block above for rationale.
                        if let Some(prev) = self.state.last_video_xiu_ts {
                            let backward = tag.timestamp_ms < prev;
                            let forward_jump =
                                tag.timestamp_ms.saturating_sub(prev) > MAX_TAG_TS_JUMP_MS;
                            if backward || forward_jump {
                                // Symmetric re-anchor (#257): see matching
                                // audio block above.
                                self.state.reanchor(Track::Video);
                                self.skew.reset_tracks();
                                tracing::warn!(
                                    prev_xiu_ts = prev,
                                    new_xiu_ts = tag.timestamp_ms,
                                    direction = if backward { "backward" } else { "forward" },
                                    shared_base = self.state.video_base_ms,
                                    "rtmp_push: VIDEO tag.timestamp_ms anomaly -- symmetric re-anchor of BOTH tracks to shared base"
                                );
                            }
                        }
                        self.state.last_video_xiu_ts = Some(tag.timestamp_ms);
                        // Feed the cross-track skew detector with the INPUT PTS
                        // (#257), pre per-track output re-anchor.
                        self.skew.observe_video(tag.timestamp_ms);
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
                crate::flv::FLV_TAG_SCRIPT => {
                    // Forward FLV script tag (typically `@setDataFrame onMetaData`)
                    // straight through to the RTMP server with timestamp 0, no
                    // pacing, no PTS bookkeeping. FB Live Producer silently
                    // rejects video without an onMetaData announcement first.
                    // Send before any audio/video tags from this chunk.
                    if let Some(session) = self.session.as_mut() {
                        let body_len = tag.body.len();
                        match session.send_data_tag(0, tag.body).await {
                            Ok(()) => {
                                tags_sent += 1;
                                bytes_sent += body_len as u64;
                            }
                            Err(e) => {
                                self.state.connected = false;
                                return Err(e);
                            }
                        }
                    }
                    continue;
                }
                _ => continue, // unknown — drop, no PTS to assign
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
            //
            // Defensive cap (issue #176/#178): if a tag carries a corrupt
            // timestamp far in the future (observed 14m output_ts at 2m
            // wall-clock = 12-minute pacing sleep), clamp the sleep to
            // PACING_SLEEP_CAP_MS so a single bad tag does not stall the
            // entire push (which then trips the consumer-side 30s write
            // timeout and force-closes 5+ endpoint sessions simultaneously
            // when the bad tag arrives via shared chunk supply).
            const PACING_SLEEP_CAP_MS: u64 = 5_000;
            let actual_ms = anchor.elapsed().as_millis() as u64;
            if actual_ms < output_ts_u64 {
                let raw_sleep_ms = output_ts_u64 - actual_ms;
                let clamped = raw_sleep_ms.min(PACING_SLEEP_CAP_MS);
                if raw_sleep_ms >= 2_000 {
                    tracing::warn!(
                        tag_type = tag.tag_type,
                        output_ts = output_ts_u64,
                        actual_ms,
                        raw_sleep_ms,
                        clamped_to_ms = clamped,
                        last_audio_output_ts_ms = self.state.last_audio_output_ts_ms,
                        last_video_output_ts_ms = self.state.last_video_output_ts_ms,
                        audio_base_ms = self.state.audio_base_ms,
                        video_base_ms = self.state.video_base_ms,
                        "rtmp_push: LONG per-tag pacing sleep (>=2s) -- output_ts ahead of wall by {raw_sleep_ms}ms; clamped to {clamped}ms"
                    );
                }
                tokio::time::sleep(Duration::from_millis(clamped)).await;
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
        // Per-tag pacing already drained inside the loop — by chunk end the
        // residual is normally 0–10 ms. Renamed from `pacing_sleep_ms` to
        // make clear that the pusher does NOT sleep this long here; the
        // value just shows how far ahead/behind wall-clock the chunk
        // ended up.
        let pacing_residual_ms = target_ms.saturating_sub(actual_ms);
        let regression_reanchor_count = self.state.regression_reanchor_count;

        // Issue #257 — cross-track A/V-skew guard. Evaluate the content-PTS
        // skew at the chunk boundary. A sustained over-threshold skew (the
        // 2026-06-19 audio-behind-video desync, which the per-track output
        // re-anchor would otherwise hide on the wire) trips a CLEAN
        // reconnect: drop the session and return AvSkewExceeded so the
        // consumer force-closes and the next push re-anchors BOTH tracks from
        // a common session start. Strict 1× — recovery is ONLY a reconnect +
        // re-anchor, never a speed-up. Debounced + rate-limited inside
        // SkewTracker so a persistent upstream skew cannot thrash reconnects.
        let skew_decision = self.skew.evaluate_chunk(actual_ms);
        let av_skew_ms = self.skew.last_skew_ms();
        if skew_decision == SkewDecision::TripRecovery {
            tracing::error!(
                av_skew_ms,
                max_av_skew_ms = crate::skew::MAX_AV_SKEW_MS,
                a_out = max_audio_output_ts,
                v_out = max_video_output_ts,
                trip_count = self.skew.trip_count(),
                "rtmp_push: A/V SKEW EXCEEDED -- forcing clean reconnect to re-anchor both tracks (#257)"
            );
            self.state.connected = false;
            self.session = None;
            return Err(PushError::AvSkewExceeded {
                skew_ms: av_skew_ms,
            });
        }

        // Issue #171 — chunk-end rate cap. Caps push rate at
        // CATCHUP_FACTOR_PCT/100 × real-time so a post-RemoteClosed
        // burst drains buffered chunks GENTLY without overrunning
        // upstream's TCP receive buffer. Steady-state pushes (where
        // per-tag pacing already paced at real-time) see a 0 ms
        // residual and no extra sleep.
        let chunk_start_max = chunk_start_audio_out.max(chunk_start_video_out);
        let chunk_end_max = max_audio_output_ts.max(max_video_output_ts);
        let chunk_media_ms = chunk_end_max.saturating_sub(chunk_start_max);
        let chunk_cap_sleep_ms =
            chunk_pacing_sleep_ms(chunk_media_ms, send_elapsed_ms, CATCHUP_FACTOR_PCT);
        if chunk_cap_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(chunk_cap_sleep_ms)).await;
        }

        tracing::info!(
            "rtmp_push: chunk done tags_sent={tags_sent} tags_skipped={tags_skipped} bytes_sent={bytes_sent} a_out={max_audio_output_ts} v_out={max_video_output_ts} av_skew_ms={av_skew_ms} send_elapsed_ms={send_elapsed_ms} pacing_residual_ms={pacing_residual_ms} target_ms={target_ms} actual_ms={actual_ms} reanchor={regression_reanchor_count}"
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

    /// Chunker-side timestamp regression mid-session: re-anchor
    /// `audio_base_ms` to `last_output_ts + 1`. Wire-monotonic, sets
    /// output BEHIND wall so per-tag pacing burst-pushes the gap; the
    /// burst is capped by the chunk-end rate cap (issue #171).
    #[test]
    fn audio_xiu_regression_re_anchors_to_last_output_plus_one() {
        let mut state = PusherState {
            audio_origin_xiu_ts: Some(0),
            last_audio_xiu_ts: Some(600_000),
            last_audio_output_ts_ms: 600_000,
            ..PusherState::default()
        };

        let new_tag_xiu_ts = 21_u32;
        let prev = state.last_audio_xiu_ts.unwrap();
        if new_tag_xiu_ts < prev {
            state.audio_base_ms = state.last_audio_output_ts_ms.saturating_add(1);
            state.audio_origin_xiu_ts = None;
        }

        assert_eq!(
            state.audio_base_ms, 600_001,
            "base anchors on wire timeline; chunk-end rate cap bounds the catch-up burst"
        );
    }

    /// Issue #257 symmetric re-anchor RED→GREEN. When the two tracks have
    /// drifted to UNEQUAL `last_*_output_ts_ms` (independent reconnect /
    /// rescue→resume / republish boundaries) and ONE track trips a backward
    /// jump, the per-track re-anchor froze the inter-track offset. The
    /// symmetric re-anchor must collapse the offset to ~0 by re-anchoring BOTH
    /// tracks to a single shared base at the re-anchor instant.
    ///
    /// RED: `reanchor(Track::Audio)` moves only `audio_base_ms`, leaving
    /// `video_base_ms` on its old value → the next coincident A/V tags produce
    /// a non-zero `(a_out − v_out)` offset. GREEN: both bases move to the
    /// shared base → coincident tags produce a ~0 offset.
    #[test]
    fn symmetric_reanchor_collapses_drifted_offset_to_zero() {
        // `Track` is in scope via `use super::*`.
        // Audio track is 30_000 ms AHEAD of video on the wire (drift from an
        // earlier independent reconnect). This is the pre-jump steady offset
        // that a per-track re-anchor would freeze.
        let mut state = PusherState {
            last_audio_output_ts_ms: 630_000,
            last_video_output_ts_ms: 600_000,
            audio_base_ms: 630_000,
            video_base_ms: 600_000,
            audio_origin_xiu_ts: Some(630_000),
            video_origin_xiu_ts: Some(600_000),
            last_audio_xiu_ts: Some(630_000),
            last_video_xiu_ts: Some(600_000),
            ..PusherState::default()
        };

        // Audio trips a backward regression (chunker reset → xiu_ts ~0).
        state.reanchor(Track::Audio);

        // After the re-anchor, the FIRST coincident audio+video tag of the new
        // chunk both arrive at the SAME chunker input ts (the chunker flushed
        // both on the same boundary → they are content-coincident). Their wire
        // output_ts must therefore be (nearly) equal.
        let new_input_ts = 0_u32;
        let a_origin = *state.audio_origin_xiu_ts.get_or_insert(new_input_ts);
        let v_origin = *state.video_origin_xiu_ts.get_or_insert(new_input_ts);
        let a_out = state.audio_base_ms + (new_input_ts.saturating_sub(a_origin)) as u64;
        let v_out = state.video_base_ms + (new_input_ts.saturating_sub(v_origin)) as u64;

        let offset = a_out as i64 - v_out as i64;
        assert_eq!(
            offset, 0,
            "symmetric re-anchor must collapse the drifted A/V offset to ~0; \
             per-track re-anchor would leave it frozen at +30_000 ms (a_out={a_out} v_out={v_out})"
        );
    }

    /// On reconnect, per-track BASE = `last_output_ts + 1`. Output
    /// starts behind wall after a gap → per-tag pacing skips sleep →
    /// burst push, then chunk-end rate cap caps overall rate at
    /// CATCHUP_FACTOR_PCT/100 × real-time.
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

    // --- chunk_pacing_sleep_ms (issue #171 chunk-end rate cap) ---

    #[test]
    fn chunk_pacing_steady_state_no_extra_sleep() {
        // Per-tag pacing already paced at real-time → actual_wall ≈
        // chunk_media. target_wall = 2000*100/120 = 1666 < actual 2000
        // → 0 sleep. Steady-state path unaffected.
        assert_eq!(chunk_pacing_sleep_ms(2000, 2000, 120), 0);
    }

    #[test]
    fn chunk_pacing_catchup_burst_is_bounded() {
        // Catch-up: per-tag pacing skipped, all 2000 ms media pushed in
        // 50 ms wallclock. Cap at 1.2× real-time → target_wall = 1666 ms,
        // sleep 1616 ms. Net: chunk push takes 1666 ms wallclock, push
        // rate = 2000/1666 = 1.2× real-time exactly.
        assert_eq!(chunk_pacing_sleep_ms(2000, 50, 120), 1616);
    }

    #[test]
    fn chunk_pacing_disabled_at_zero() {
        // Zero factor disables the cap entirely (regression escape
        // hatch). Returns 0 regardless of inputs.
        assert_eq!(chunk_pacing_sleep_ms(2000, 50, 0), 0);
    }

    #[test]
    fn chunk_pacing_factor_100_is_strict_real_time() {
        // 100% = exactly real-time. burst 50ms wallclock for 2000ms
        // media → sleep 1950 to bring total to 2000ms.
        assert_eq!(chunk_pacing_sleep_ms(2000, 50, 100), 1950);
    }

    #[test]
    fn chunk_pacing_factor_500_aggressive_burst() {
        // 5x burst allowed: target_wall = 2000/5 = 400, sleep 350 from
        // actual 50. Used to verify factor scales correctly.
        assert_eq!(chunk_pacing_sleep_ms(2000, 50, 500), 350);
    }

    #[test]
    fn chunk_pacing_actual_at_target_no_sleep() {
        // Boundary: actual_wall == target_wall → sleep 0 (kills off-by-one
        // mutant flipping <= to <).
        assert_eq!(chunk_pacing_sleep_ms(2400, 2000, 120), 0);
    }

    #[test]
    fn chunk_pacing_empty_chunk_no_sleep() {
        // Edge case: chunk with no media (e.g. just seq headers).
        // chunk_media_ms = 0 → target_wall = 0 → sleep 0.
        assert_eq!(chunk_pacing_sleep_ms(0, 0, 120), 0);
        assert_eq!(chunk_pacing_sleep_ms(0, 100, 120), 0);
    }
}
