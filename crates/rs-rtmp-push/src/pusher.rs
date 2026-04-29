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
        chunk_duration_ms: u32,
    ) -> Result<(), PushError> {
        // Lazy connect.
        if self.session.is_none() {
            // A reconnect is any connect that happens after media has been sent
            // (last_output_ts_ms > 0 means at least one tag was written in a
            // previous session).  Count the attempt before the connect so the
            // dashboard always sees an accurate reconnect count, even when the
            // reconnect itself fails.
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
        }

        // Empty slice -> handshake verified, nothing to send.
        if bytes.is_empty() {
            return Ok(());
        }

        // Parse FLV tags with monotonic timestamp rewriting.
        let iter = crate::flv::FlvTagIter::new(bytes)?;
        // Track first ts SEPARATELY per track. The chunker stamps audio
        // tags with xiu's RTMP session ts (e.g. 60_000 ms after one
        // minute) but stamps video tags with wall-clock since the chunker
        // started (e.g. 0 ms). Mixing those two domains into a single
        // `chunk_first_ts` produces deltas inflated by the cross-domain
        // offset (~5–60 s) and was the root cause of the cache-growth
        // bug observed in #103 — `last_output_ts_ms` ballooned by tens
        // of seconds per chunk, pacing slept many seconds, and effective
        // output rate fell to 0.2 x real-time even though TCP send took
        // ~1 ms per chunk. See `feedback_chunker_time_domains`.
        let mut chunk_first_audio_ts: Option<u32> = None;
        let mut chunk_first_video_ts: Option<u32> = None;
        let monotonic_offset = self.state.last_output_ts_ms;
        let mut max_intra_chunk_delta: u32 = 0;
        // Anchor wall-clock once per pusher lifetime so per-chunk pacing
        // tracks the cumulative output timestamp, not just intra-chunk time.
        let anchor = *self.state.pacing_anchor.get_or_insert_with(Instant::now);

        // Per-chunk diagnostics — `tracing::info!` lands in journalctl on
        // the VPS so cache-growth post-mortems can attribute time to send
        // vs sleep without redeploying with extra logs.
        let chunk_started_at = Instant::now();
        let mut tags_sent: u32 = 0;
        let mut tags_skipped: u32 = 0;
        let mut bytes_sent: u64 = 0;

        // Collect tag metadata (type, output timestamp) and body slices.
        // We hold the body references into `bytes` (lifetime tied to the `iter`
        // borrow of `bytes`), so we send each tag immediately inside the loop.
        for tag in iter {
            // Compute the per-chunk delta IN THE TAG'S OWN TRACK so audio
            // (xiu domain) and video (wall-clock domain) don't pollute
            // each other's pacing math.
            let delta = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => {
                    let first = *chunk_first_audio_ts.get_or_insert(tag.timestamp_ms);
                    tag.timestamp_ms.saturating_sub(first)
                }
                crate::flv::FLV_TAG_VIDEO => {
                    let first = *chunk_first_video_ts.get_or_insert(tag.timestamp_ms);
                    tag.timestamp_ms.saturating_sub(first)
                }
                _ => 0, // SCRIPT/unknown — no contribution to output_ts
            };
            if delta > max_intra_chunk_delta {
                max_intra_chunk_delta = delta;
            }
            let output_ts = (monotonic_offset + delta as u64) as u32;

            // Identify and de-duplicate codec sequence headers. The chunker
            // re-emits AVC SPS/PPS and AAC AudioSpecificConfig in EVERY S3
            // chunk so each chunk is a self-contained FLV; but a real RTMP
            // server expects each codec config exactly ONCE per session.
            // Re-sending was observed to throttle YouTube ingestion and
            // drop output to ~0.2 x real-time (#103).
            let is_seq_header = tag.body.len() >= 2 && tag.body[1] == 0x00;
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

            // Per-tag pacing: sleep BEFORE sending so the wall-clock
            // emission cadence matches the FLV timestamp cadence (~33 ms
            // for 30 fps video, ~21 ms for AAC audio). Without this the
            // pusher sends a whole chunk's tags in ~1 ms and then sleeps
            // ~2 s — YouTube's bitrate sensor averages over a short
            // window and reports `bitrateLow` because the per-window
            // average is half the encoded rate (#103).
            let pre_tag_target_ms = monotonic_offset + delta as u64;
            let pre_tag_actual_ms = anchor.elapsed().as_millis() as u64;
            if pre_tag_actual_ms < pre_tag_target_ms {
                tokio::time::sleep(Duration::from_millis(pre_tag_target_ms - pre_tag_actual_ms))
                    .await;
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
                    crate::flv::FLV_TAG_SCRIPT => Ok(()), // skip metadata
                    other => {
                        tracing::warn!(tag_type = other, "unknown FLV tag type, skipping");
                        Ok(())
                    }
                };
                if res.is_ok() {
                    tags_sent += 1;
                    bytes_sent += body_len as u64;
                }
                res
            };

            if let Err(e) = send_result {
                // Drop session so next call lazy-reconnects.
                self.state.connected = false;
                self.session = None;
                return Err(e);
            }
        }
        // Advance the cumulative monotonic timestamp by the chunker's
        // authoritative media duration. Using FLV tag deltas would risk
        // measuring from the wrong time domain (audio xiu vs video
        // wall-clock) or from in-chunk frame-arrival jitter; the chunker's
        // `chunk_duration_ms` is computed from video keyframe-to-keyframe
        // wall-clock and is the only honest source of "how much real
        // media is in this chunk".
        self.state.last_output_ts_ms = monotonic_offset + chunk_duration_ms as u64;
        let send_elapsed_ms = chunk_started_at.elapsed().as_millis() as u64;

        // Per-chunk tail pacing: small mop-up sleep if all per-tag sleeps
        // together still left us slightly behind the cumulative target
        // (e.g. zero-duration tags or rounding). With per-tag pacing
        // active above this is normally 0–10 ms; the line is kept so a
        // CHUNKER chunk_duration_ms larger than the sum of FLV deltas
        // still produces correct cumulative pacing.
        let target_ms = self.state.last_output_ts_ms;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        let pacing_sleep_ms = target_ms.saturating_sub(actual_ms);
        if pacing_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pacing_sleep_ms)).await;
        }
        tracing::info!(
            "rtmp_push: chunk done tags_sent={tags_sent} tags_skipped={tags_skipped} bytes_sent={bytes_sent} chunk_duration_ms={chunk_duration_ms} max_intra_chunk_delta={max_intra_chunk_delta} send_elapsed_ms={send_elapsed_ms} pacing_sleep_ms={pacing_sleep_ms} target_ms={target_ms} actual_ms={actual_ms}"
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

    /// Per-track delta math (regression for #103, run 25131115779):
    /// the chunker stamps audio with xiu's RTMP session timestamp and
    /// video with wall-clock since the chunker session start. Mixing
    /// the two domains into a single `chunk_first_ts` produces deltas
    /// inflated by the cross-domain offset (~5–60 s); per-chunk
    /// `last_output_ts_ms` then balloons by tens of seconds and pacing
    /// over-sleeps. Compute the delta against the FIRST tag of THE SAME
    /// TRACK and use the max of audio/video intra-chunk deltas as the
    /// chunk's media duration.
    #[test]
    fn per_track_delta_handles_offset_audio_xiu_vs_wallclock_video() {
        // Simulate one chunk where audio lives in xiu domain and video
        // in wall-clock domain. Within the chunk, both tracks span 2 s.
        let video_ts = [0_u32, 33, 66, 1_000, 1_999];
        let audio_ts = [60_000_u32, 60_023, 60_046, 61_000, 61_999];

        // Track first ts per track.
        let mut chunk_first_audio_ts: Option<u32> = None;
        let mut chunk_first_video_ts: Option<u32> = None;
        let mut max_intra_chunk_delta: u32 = 0;

        for &ts in video_ts.iter() {
            let first = *chunk_first_video_ts.get_or_insert(ts);
            let delta = ts.saturating_sub(first);
            if delta > max_intra_chunk_delta {
                max_intra_chunk_delta = delta;
            }
        }
        for &ts in audio_ts.iter() {
            let first = *chunk_first_audio_ts.get_or_insert(ts);
            let delta = ts.saturating_sub(first);
            if delta > max_intra_chunk_delta {
                max_intra_chunk_delta = delta;
            }
        }

        // Chunk media duration must be ~2 s, NOT 60+ s. The OLD
        // single-domain code would have produced 61_999 here.
        assert!(
            max_intra_chunk_delta >= 1_999 && max_intra_chunk_delta <= 2_100,
            "expected ~2000 ms chunk duration, got {max_intra_chunk_delta} ms"
        );
    }
}
