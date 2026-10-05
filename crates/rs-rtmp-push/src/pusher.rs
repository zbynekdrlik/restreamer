//! `RtmpPusher` - public API.

use std::time::Duration;

use tokio::time::Instant;

use crate::av_invariant::{AvInvariantEvent, AvInvariantGuard};
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
    /// #367 absolute A/V invariant guard (no baseline): the wire relation of
    /// the latest audio/video tags must equal their content (chunk)
    /// relation. Fed with the u32 ts ACTUALLY written to the wire.
    av_guard: AvInvariantGuard,
    /// Guard edges not yet taken by the consumer (which turns them into
    /// `AvInvariantViolated` / `AvInvariantRestored` audit rows).
    av_events: Vec<AvInvariantEvent>,
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

/// `true` for an AVC / AAC codec sequence header (`body[1] == 0x00`).
fn is_media_seq_header(tag: &crate::flv::FlvTag<'_>) -> bool {
    tag.body.len() >= 2 && tag.body[1] == 0x00
}

/// For each tag index `i`, the minimum input ts of the MEDIA tags (audio /
/// video, codec sequence headers excluded) at index `>= i`, or `None` when
/// no media tag follows. A shared mapping re-pinned at tag `i` uses it as
/// its origin, so no remaining tag of the chunk maps below the shared base
/// (#367).
fn media_pin_suffix(tags: &[crate::flv::FlvTag<'_>]) -> Vec<Option<u32>> {
    let mut out = vec![None; tags.len()];
    let mut running: Option<u32> = None;
    for (i, tag) in tags.iter().enumerate().rev() {
        let is_media = matches!(
            tag.tag_type,
            crate::flv::FLV_TAG_AUDIO | crate::flv::FLV_TAG_VIDEO
        );
        if is_media && !is_media_seq_header(tag) {
            running = Some(running.map_or(tag.timestamp_ms, |m| m.min(tag.timestamp_ms)));
        }
        out[i] = running;
    }
    out
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
            session: None,
            skew: SkewTracker::default(),
            av_guard: AvInvariantGuard::default(),
            av_events: Vec::new(),
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

    /// Drain the A/V invariant guard edges recorded since the last call
    /// (#367). The consumer audits each one.
    pub fn take_av_invariant_events(&mut self) -> Vec<AvInvariantEvent> {
        std::mem::take(&mut self.av_events)
    }

    /// Times this pusher's wire A/V relation broke the absolute invariant
    /// (#367). Expected to stay 0: the shared origin + base make the wire
    /// relation equal the content relation by construction.
    pub fn av_invariant_violation_count(&self) -> u32 {
        self.av_guard.violation_count()
    }

    /// Classify one media tag's INPUT ts against its track's previous one and
    /// keep the trackers. Returns `true` when the tag is an isolated outlier
    /// that must be clamped instead of mapped.
    ///
    /// A BACKWARD step (the chunker started a new session after a stream.lan
    /// restart / republish, while the RTMP session to YouTube stays alive,
    /// #103) or a FORWARD jump > `MAX_TAG_TS_JUMP_MS` (#176/#178: a 720 s jump
    /// made pacing sleep 12 min and trip the 30 s write timeout) starts a NEW
    /// shared mapping: both tracks re-anchor to one base (#257) and one origin
    /// (#367), so the wire stays monotonic and the A/V relation stays the
    /// content's.
    ///
    /// #367: with ONE origin for both tracks, a single corrupt tag must not be
    /// taken for a new timeline. If the rest of the chunk does not follow it
    /// (`rest_min` is more than `MAX_TAG_TS_JUMP_MS` away), it is an isolated
    /// outlier: the mapping stays, the trackers keep the previous ts, and the
    /// caller clamps it onto its track's wire timeline. Otherwise, re-pinning
    /// the shared origin below it would put it far ahead on the wire and
    /// freeze pacing. The old per-track re-pin hid this by accident.
    fn track_input_ts(
        &mut self,
        track: Track,
        input_ts: u32,
        pin: u32,
        rest_min: Option<u32>,
    ) -> bool {
        let prev = match track {
            Track::Audio => self.state.last_audio_xiu_ts,
            Track::Video => self.state.last_video_xiu_ts,
        };
        if let Some(prev) = prev {
            let backward = input_ts < prev;
            let forward_jump = input_ts.saturating_sub(prev) > MAX_TAG_TS_JUMP_MS;
            if backward || forward_jump {
                if rest_min.is_some_and(|m| m.abs_diff(input_ts) > MAX_TAG_TS_JUMP_MS) {
                    tracing::warn!(
                        track = ?track,
                        prev_xiu_ts = prev,
                        outlier_xiu_ts = input_ts,
                        rest_of_chunk_min_ts = rest_min,
                        "rtmp_push: isolated tag.timestamp_ms outlier -- clamped onto the \
                         current wire timeline, shared mapping unchanged (#367)"
                    );
                    return true;
                }
                self.state.reanchor(track);
                self.skew.reset_tracks();
                self.av_guard.begin_new_transform();
                tracing::warn!(
                    track = ?track,
                    prev_xiu_ts = prev,
                    new_xiu_ts = input_ts,
                    direction = if backward { "backward" } else { "forward" },
                    shared_base = self.state.base_ms,
                    shared_origin_pin = pin,
                    "rtmp_push: tag.timestamp_ms anomaly -- symmetric re-anchor of BOTH tracks \
                     onto a new shared mapping"
                );
            }
        }
        // Feed the cross-track skew detector with the INPUT (chunker-stamped)
        // PTS, before the wire mapping rewrites it (#257).
        match track {
            Track::Audio => {
                self.state.last_audio_xiu_ts = Some(input_ts);
                self.skew.observe_audio(input_ts);
            }
            Track::Video => {
                self.state.last_video_xiu_ts = Some(input_ts);
                self.skew.observe_video(input_ts);
            }
        }
        false
    }

    /// Chunk-end evaluation of the absolute A/V invariant (#367): log the
    /// edge loudly and queue it for the consumer's audit row.
    pub(crate) fn evaluate_av_invariant(&mut self) {
        let Some(event) = self.av_guard.evaluate() else {
            return;
        };
        match event {
            AvInvariantEvent::Violated(v) => tracing::warn!(
                stage = "push",
                a_rel_ms = v.a_rel_ms,
                v_rel_ms = v.v_rel_ms,
                delta_ms = v.delta_ms,
                tolerance_ms = crate::av_invariant::AV_INVARIANT_TOLERANCE_MS,
                "rtmp_push: A/V INVARIANT VIOLATED -- the wire A/V relation differs from the \
                 chunk's content relation (#367)"
            ),
            AvInvariantEvent::Restored { delta_ms } => tracing::info!(
                stage = "push",
                delta_ms,
                "rtmp_push: A/V invariant restored -- wire relation equals content relation (#367)"
            ),
        }
        self.av_events.push(event);
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
    /// `ChunkPacketizer` with its timestamp rewritten by ONE transform shared
    /// by both tracks (`PusherState::wire_ts`, #367): the wire A/V relation
    /// is exactly the chunk's content relation, and each track's wire
    /// timeline stays monotonic and continuous across chunk boundaries
    /// (#103).
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
            // Start a new SHARED wire mapping (#367): one base for both
            // tracks, one past the highest output_ts sent on either track
            // in the previous session (wire monotonic even if xiu's RTMP
            // session resets to ts=0), and one origin re-pinned by the
            // first chunk -- so the wire A/V relation after the reconnect
            // is the content relation, never pusher history.
            //
            // History of the base choice. It used to be the LATER of:
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
            // `begin_new_mapping` also clears last_*_xiu_ts ("what we just
            // saw upstream"): the new session starts fresh, so any input ts
            // is valid.
            self.state.begin_new_mapping();
            // A new mapping is a new transform: never pair an old-mapping
            // sample of one track with a new-mapping sample of the other.
            self.av_guard.begin_new_transform();
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

        // Parse FLV tags. Each media tag's `output_ts` is
        // `base_ms + (tag.ts - origin_ts)` with ONE base and ONE origin
        // shared by both tracks (#367). The chunker stamps both tracks in
        // the publisher's source-ts domain, so this keeps the wire A/V
        // relation exactly equal to the content relation.
        //
        // The origin is per MAPPING (fresh session / re-anchor), never per
        // chunk: each track's wire ts stays continuous across chunk
        // boundaries. The #103 click came from a per-CHUNK rebase that put
        // a chunk's first audio frame on the previous chunk's last audio
        // output_ts. Per-TRACK origins (the #103 fix) were rejected for #367
        // because they made the wire offset depend on which track's first
        // tag a mapping happened to see first.
        let tags: Vec<crate::flv::FlvTag<'_>> = crate::flv::FlvTagIter::new(bytes)?.collect();
        let pin_from = media_pin_suffix(&tags);
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

        for (i, tag) in tags.into_iter().enumerate() {
            // The shared origin a mapping re-pinned at this tag would get:
            // the minimum input ts of the media tags still to be sent, so
            // none of them maps below the shared base.
            let pin = pin_from[i].unwrap_or(tag.timestamp_ms);
            // Sequence headers (codec config: AVC SPS/PPS, AAC config) are
            // identified by body[1] == 0x00 (`AVCPacketType::SequenceHeader`
            // / `AACPacketType::SequenceHeader`). The chunker writes them
            // at the START of every chunk with ts=0 so each S3 chunk is a
            // self-contained FLV file for the ffmpeg path. We forward
            // exactly ONE per codec per RTMP session (subsequent ones
            // would force the receiver to reset its decoder).
            let is_seq_header = tag.body.len() >= 2 && tag.body[1] == 0x00;

            // Tags after this one: an anomalous tag the rest of the chunk does
            // not follow is an isolated outlier (#367).
            let rest_min = pin_from.get(i + 1).copied().flatten();

            let (output_ts_u64, track_max, outlier) = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => {
                    let outlier = !is_seq_header
                        && self.track_input_ts(Track::Audio, tag.timestamp_ms, pin, rest_min);
                    // Never pin the shared origin on a codec sequence header
                    // (the chunker writes it with ts 0 in every chunk).
                    let ts = if outlier {
                        self.state
                            .last_audio_output_ts_ms
                            .max(max_audio_output_ts)
                            .saturating_add(1)
                    } else if is_seq_header {
                        self.state.wire_ts_unpinned(tag.timestamp_ms, pin)
                    } else {
                        self.state.wire_ts(tag.timestamp_ms, pin)
                    };
                    (ts, &mut max_audio_output_ts, outlier)
                }
                crate::flv::FLV_TAG_VIDEO => {
                    let outlier = !is_seq_header
                        && self.track_input_ts(Track::Video, tag.timestamp_ms, pin, rest_min);
                    let ts = if outlier {
                        self.state
                            .last_video_output_ts_ms
                            .max(max_video_output_ts)
                            .saturating_add(1)
                    } else if is_seq_header {
                        self.state.wire_ts_unpinned(tag.timestamp_ms, pin)
                    } else {
                        self.state.wire_ts(tag.timestamp_ms, pin)
                    };
                    (ts, &mut max_video_output_ts, outlier)
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
            // #367: feed the absolute invariant guard with the ts that goes on
            // the wire (the u32, so even a wrap would be caught). A clamped
            // outlier is deliberately off the transform: never a sample.
            if !is_seq_header && !outlier {
                match tag.tag_type {
                    crate::flv::FLV_TAG_AUDIO => self
                        .av_guard
                        .observe_audio(i64::from(tag.timestamp_ms), i64::from(output_ts)),
                    crate::flv::FLV_TAG_VIDEO => self
                        .av_guard
                        .observe_video(i64::from(tag.timestamp_ms), i64::from(output_ts)),
                    _ => {}
                }
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
                        base_ms = self.state.base_ms,
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
        // #367: the absolute (no-baseline) invariant -- evaluated first so a
        // violation is queued for audit even when the skew guard trips below.
        self.evaluate_av_invariant();
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

    // --- local window / robust pin (#367 shared-origin pin) ---

    fn tag(tag_type: u8, timestamp_ms: u32, body: &'static [u8]) -> crate::flv::FlvTag<'static> {
        crate::flv::FlvTag {
            tag_type,
            timestamp_ms,
            body,
        }
    }

    /// Media tags at the given ts (alternating video / audio), no headers.
    fn media(ts: &[u32]) -> Vec<crate::flv::FlvTag<'static>> {
        use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_VIDEO};
        ts.iter()
            .enumerate()
            .map(|(k, &t)| {
                if k % 2 == 0 {
                    tag(FLV_TAG_VIDEO, t, &[0x27, 0x01])
                } else {
                    tag(FLV_TAG_AUDIO, t, &[0xAF, 0x01])
                }
            })
            .collect()
    }

    #[test]
    fn local_median_ignores_headers_and_script_tags() {
        use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_SCRIPT, FLV_TAG_VIDEO};
        let tags = [
            tag(FLV_TAG_SCRIPT, 0, &[0x02, 0x00]),
            tag(FLV_TAG_VIDEO, 0, &[0x17, 0x00]), // AVC seq header
            tag(FLV_TAG_AUDIO, 0, &[0xAF, 0x00]), // AAC seq header
            tag(FLV_TAG_VIDEO, 1_000, &[0x17, 0x01]),
            tag(FLV_TAG_AUDIO, 990, &[0xAF, 0x01]),
            tag(FLV_TAG_VIDEO, 1_040, &[0x27, 0x01]),
        ];
        assert_eq!(local_median(&tags, 0), Some(1_000));
        assert_eq!(local_median(&tags, 5), Some(1_040));
        assert_eq!(
            local_median(&tags[..3], 0),
            None,
            "no media tag -> no median"
        );
    }

    #[test]
    fn local_median_is_robust_to_one_glitch() {
        let tags = media(&[600_000, 5, 600_020, 600_040, 600_060]);
        assert_eq!(local_median(&tags, 0), Some(600_020));
    }

    #[test]
    fn robust_pin_ignores_a_low_glitch_and_keeps_interleave() {
        // 599_990: audio interleaved slightly before the keyframe -> kept;
        // 5: corrupt -> excluded from the pin.
        let tags = media(&[600_000, 599_990, 600_040, 5, 600_080, 600_060]);
        assert_eq!(robust_pin(&tags, 0), Some(599_990));
    }

    #[test]
    fn robust_pin_of_a_two_cluster_chunk_follows_the_local_cluster() {
        // A genuine 39 s gap inside the chunk: the pin at the head belongs to
        // the head's cluster, never to the later one.
        let mut ts: Vec<u32> = (0..10).map(|k| k * 20).collect();
        ts.extend((0..10).map(|k| 40_000 + k * 20));
        let tags = media(&ts);
        assert_eq!(robust_pin(&tags, 0), Some(0));
        assert_eq!(robust_pin(&tags, 10), Some(40_000));
    }

    // --- #367 push-side absolute A/V invariant guard ---

    /// Design test 5 (push): a constructed violation (the wire relation 0
    /// while the content relation is +700 ms, the per-track-origin bug) is
    /// reported ONCE through the pusher's chunk-end evaluation and queued for
    /// the consumer to audit.
    #[test]
    fn pusher_reports_a_constructed_wire_invariant_violation_once() {
        use crate::av_invariant::{AvInvariantEvent, AvInvariantViolation};
        let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
        p.av_guard.observe_video(1_000, 5_000);
        p.av_guard.observe_audio(1_700, 5_000);
        p.evaluate_av_invariant();
        p.evaluate_av_invariant(); // still violated: no second event
        assert_eq!(
            p.take_av_invariant_events(),
            vec![AvInvariantEvent::Violated(AvInvariantViolation {
                a_rel_ms: 3_300,
                v_rel_ms: 4_000,
                delta_ms: -700,
            })]
        );
        assert!(
            p.take_av_invariant_events().is_empty(),
            "taking the events drains the queue"
        );
        assert_eq!(p.av_invariant_violation_count(), 1);
    }

    // --- track_input_ts (#367 outlier vs new timeline) ---

    fn pusher_at(prev_audio: u32, prev_video: u32) -> RtmpPusher {
        let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
        p.state.last_audio_xiu_ts = Some(prev_audio);
        p.state.last_video_xiu_ts = Some(prev_video);
        p.state.origin_ts = Some(0);
        p
    }

    /// A single BACKWARD-glitched tag the rest of the chunk does not follow
    /// is clamped: no re-anchor, the mapping and the trackers stay put.
    #[test]
    fn isolated_backward_outlier_keeps_the_mapping() {
        let mut p = pusher_at(600_000, 600_010);
        let tags = media(&[5, 600_020, 600_040, 600_060]);
        assert!(p.track_input_ts(Track::Video, &tags, 0));
        assert_eq!(p.regression_reanchor_count(), 0);
        assert_eq!(p.state.origin_ts, Some(0), "mapping unchanged");
        assert_eq!(
            p.state.last_video_xiu_ts,
            Some(600_010),
            "the tracker keeps the previous ts so the next normal tag is no jump"
        );
    }

    /// A backward step the rest of the chunk FOLLOWS is a new timeline (new
    /// chunker session): re-anchor both tracks onto a new mapping.
    #[test]
    fn followed_backward_step_starts_a_new_mapping() {
        let mut p = pusher_at(600_000, 600_010);
        let tags = media(&[0, 0, 40, 20]);
        assert!(!p.track_input_ts(Track::Video, &tags, 0));
        assert_eq!(p.regression_reanchor_count(), 1);
        assert!(
            p.state.origin_ts.is_none(),
            "the next tag re-pins the origin"
        );
        assert_eq!(p.state.last_video_xiu_ts, Some(0));
        assert!(
            p.state.last_audio_xiu_ts.is_none(),
            "the other track's tracker clears with the new mapping"
        );
    }

    /// An anomalous LAST media tag of a chunk (nothing after it to compare)
    /// re-anchors, as before #367.
    #[test]
    fn anomalous_last_tag_reanchors() {
        let mut p = pusher_at(1_000, 1_000);
        let tags = media(&[900_000]);
        assert!(!p.track_input_ts(Track::Video, &tags, 0));
        assert_eq!(p.regression_reanchor_count(), 1);
    }

    /// The HEAD tag of a track in a new mapping (no tracker yet) that its
    /// neighbourhood disagrees with is an outlier, not a pin source.
    #[test]
    fn head_tag_outlier_is_detected_without_a_tracker() {
        let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
        let tags = media(&[720_020, 0, 40, 20, 80, 60]);
        assert!(p.track_input_ts(Track::Video, &tags, 0));
        assert!(p.state.last_video_xiu_ts.is_none());
        assert!(
            !p.track_input_ts(Track::Audio, &tags, 1),
            "a normal head tag maps"
        );
        assert_eq!(p.state.last_audio_xiu_ts, Some(0));
    }
}
