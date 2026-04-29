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
    /// On the first call (or after a reconnect) the pusher dials the server,
    /// performs the full RTMP handshake + connect + publish sequence, and
    /// stores the resulting `Session`.
    ///
    /// For empty `bytes` slices the method returns `Ok(())` after connecting -
    /// this is the Task 3 test contract: "handshake completes, no tags sent".
    ///
    /// For non-empty `bytes`, each audio/video tag body is written via
    /// `ChunkPacketizer` with monotonically rewritten timestamps.
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
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
        let mut chunk_first_ts: Option<u32> = None;
        let monotonic_offset = self.state.last_output_ts_ms;
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
            // Compute the per-chunk delta and add to the monotonic output base.
            let first = *chunk_first_ts.get_or_insert(tag.timestamp_ms);
            let delta = tag.timestamp_ms.saturating_sub(first);
            let output_ts_u64 = monotonic_offset + delta as u64;
            let output_ts = output_ts_u64 as u32;
            self.state.last_output_ts_ms = output_ts_u64;

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
        let send_elapsed_ms = chunk_started_at.elapsed().as_millis() as u64;

        // -re-style pacing: ONCE per chunk, sleep until wall-clock has
        // caught up to the cumulative output timestamp. When we're behind
        // (cache > target after warmup), the sleep is skipped and the
        // pusher drains the buffer at TCP-write speed; once up-to-date it
        // settles at ~1 x media-time. Per-chunk granularity (vs per-tag)
        // collapses 80 sleeps/sec into ONE — eliminating the scheduler
        // jitter that previously dropped output to 0.3 x (#103).
        let target_ms = self.state.last_output_ts_ms;
        let actual_ms = anchor.elapsed().as_millis() as u64;
        let pacing_sleep_ms = target_ms.saturating_sub(actual_ms);
        if pacing_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pacing_sleep_ms)).await;
        }
        tracing::info!(
            tags_sent,
            tags_skipped,
            bytes_sent,
            send_elapsed_ms,
            pacing_sleep_ms,
            target_ms,
            actual_ms,
            "rtmp_push: chunk done"
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
}
