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

/// Defensive cap on one tag's pacing sleep (issue #176/#178): a tag with a
/// corrupt timestamp far in the future (observed: 14 min output_ts at 2 min
/// wall-clock = a 12-minute sleep) would otherwise stall the entire push,
/// trip the consumer-side 30 s write timeout, and force-close 5+ endpoint
/// sessions at once when the bad tag arrives via shared chunk supply.
const PACING_SLEEP_CAP_MS: u64 = 5_000;

/// A raw per-tag pacing sleep at least this long is logged as LONG.
const LONG_PACING_SLEEP_MS: u64 = 2_000;

/// How long one tag waits for wall-clock to catch up with its output ts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TagSleep {
    /// Output ts minus wall-clock elapsed.
    raw_ms: u64,
    /// `raw_ms` capped at `PACING_SLEEP_CAP_MS`.
    sleep_ms: u64,
}

impl TagSleep {
    fn is_long(self) -> bool {
        self.raw_ms >= LONG_PACING_SLEEP_MS
    }
}

/// Pure per-tag pacing: `None` when the tag is already due (wall-clock at or
/// past its output ts), else the capped sleep.
fn tag_sleep(actual_ms: u64, output_ts: u64) -> Option<TagSleep> {
    if actual_ms >= output_ts {
        return None;
    }
    let raw_ms = output_ts - actual_ms;
    Some(TagSleep {
        raw_ms,
        sleep_ms: raw_ms.min(PACING_SLEEP_CAP_MS),
    })
}

/// `true` for an AVC / AAC codec sequence header (`body[1] == 0x00`).
fn is_media_seq_header(tag: &crate::flv::FlvTag<'_>) -> bool {
    tag.body.len() >= 2 && tag.body[1] == 0x00
}

/// `true` for an audio / video tag carrying media (not a codec sequence
/// header, not a script tag).
fn is_media(tag: &crate::flv::FlvTag<'_>) -> bool {
    matches!(
        tag.tag_type,
        crate::flv::FLV_TAG_AUDIO | crate::flv::FLV_TAG_VIDEO
    ) && !is_media_seq_header(tag)
}

/// Media tags the local timeline window looks at (#367).
const LOCAL_WINDOW_TAGS: usize = 9;

/// Median input ts of the next (up to) `LOCAL_WINDOW_TAGS` media tags from
/// index `from`, or `None` when no media tag follows (#367). A single corrupt
/// timestamp cannot move a median, so this says where the stream's timeline
/// really is around `from`.
fn local_median(tags: &[crate::flv::FlvTag<'_>], from: usize) -> Option<u32> {
    let mut ts: Vec<u32> = tags
        .iter()
        .skip(from)
        .filter(|t| is_media(t))
        .take(LOCAL_WINDOW_TAGS)
        .map(|t| t.timestamp_ms)
        .collect();
    if ts.is_empty() {
        return None;
    }
    ts.sort_unstable();
    Some(ts[ts.len() / 2])
}

/// Whether `ts` belongs to the timeline around `median`.
fn near(ts: u32, median: u32) -> bool {
    ts.abs_diff(median) <= MAX_TAG_TS_JUMP_MS
}

/// Origin for a new shared mapping pinned at index `from` (#367): the minimum
/// input ts of the chunk's remaining media tags ON the local timeline. That
/// keeps audio/video interleave (a tag slightly before the keyframe maps at
/// or above the base), while a corrupt far-off timestamp can never drag the
/// origin (a plain minimum would shift the whole chunk on the wire).
fn robust_pin(tags: &[crate::flv::FlvTag<'_>], from: usize) -> Option<u32> {
    let median = local_median(tags, from)?;
    tags.iter()
        .skip(from)
        .filter(|t| is_media(t) && near(t.timestamp_ms, median))
        .map(|t| t.timestamp_ms)
        .min()
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

    /// Classify media tag `tags[i]` against its track's previous input ts and
    /// keep the trackers. Returns `true` when the tag is an isolated OUTLIER
    /// that must be clamped instead of mapped.
    ///
    /// A BACKWARD step (the chunker started a new session after a stream.lan
    /// restart / republish while the RTMP session to YouTube stays alive,
    /// #103) or a FORWARD jump > `MAX_TAG_TS_JUMP_MS` (#176/#178: a 720 s jump
    /// made pacing sleep 12 min and trip the 30 s write timeout) starts a NEW
    /// shared mapping: both tracks re-anchor to one base (#257) and one origin
    /// (#367), so the wire stays monotonic and the A/V relation stays the
    /// content's.
    ///
    /// #367: with ONE origin for both tracks, a single corrupt tag must never
    /// be taken for a new timeline. Pinning or re-pinning the shared mapping
    /// on it would move every tag of the chunk on the wire and freeze pacing
    /// (the old per-track re-pin hid this by accident). So the chunk decides:
    /// - a jumped tag the rest of the chunk does not follow (it is not near
    ///   the local median of the tags AFTER it) is an outlier;
    /// - the HEAD tag of a track in a new mapping (no tracker to compare with)
    ///   is an outlier when it is not near the local median of its own
    ///   neighbourhood.
    ///
    /// An outlier leaves the mapping and the trackers untouched; the caller
    /// clamps it onto its track's wire timeline.
    ///
    /// Known limit: in a NEW mapping, a genuine head cluster of fewer than
    /// about 5 media tags followed by a > 30 s gap loses the 9-tag median to
    /// the later cluster, so those real tags are clamped too. It needs a
    /// reconnect's first chunk with a 30 s+ hole inside it; judging the head
    /// against its immediate successor instead would let two consecutive
    /// corrupt head tags pin the origin, which is the worse failure.
    fn track_input_ts(&mut self, track: Track, tags: &[crate::flv::FlvTag<'_>], i: usize) -> bool {
        let input_ts = tags[i].timestamp_ms;
        let prev = match track {
            Track::Audio => self.state.last_audio_xiu_ts,
            Track::Video => self.state.last_video_xiu_ts,
        };
        match prev {
            Some(prev) => {
                let backward = input_ts < prev;
                let forward_jump = input_ts.saturating_sub(prev) > MAX_TAG_TS_JUMP_MS;
                if backward || forward_jump {
                    let rest = local_median(tags, i + 1);
                    if rest.is_some_and(|m| !near(input_ts, m)) {
                        tracing::warn!(
                            track = ?track,
                            prev_xiu_ts = prev,
                            outlier_xiu_ts = input_ts,
                            rest_of_chunk_median_ts = rest,
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
                        "rtmp_push: tag.timestamp_ms anomaly -- symmetric re-anchor of BOTH \
                         tracks onto a new shared mapping"
                    );
                }
            }
            None => {
                let around = local_median(tags, i);
                if around.is_some_and(|m| !near(input_ts, m)) {
                    tracing::warn!(
                        track = ?track,
                        outlier_xiu_ts = input_ts,
                        local_median_ts = around,
                        "rtmp_push: head tag of a new mapping is off its chunk's timeline -- \
                         clamped, never pins the shared origin (#367)"
                    );
                    return true;
                }
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

    /// Map audio/video tag `tags[i]` onto the wire (#367) and return its wire
    /// ts.
    ///
    /// ONE transform for both tracks: `wire = base_ms + (tag.ts - origin_ts)`.
    /// A new mapping (fresh session / re-anchor) pins its origin with
    /// `robust_pin`. A codec sequence header never pins it (the chunker writes
    /// it with ts 0 in every chunk). The absolute invariant guard is fed with
    /// the u32 that goes on the wire, so even a wrap would be caught. A
    /// clamped outlier is deliberately off the transform and is never a
    /// sample.
    fn map_media_tag(
        &mut self,
        tags: &[crate::flv::FlvTag<'_>],
        i: usize,
        is_seq_header: bool,
    ) -> u64 {
        let tag = &tags[i];
        let track = if tag.tag_type == crate::flv::FLV_TAG_AUDIO {
            Track::Audio
        } else {
            Track::Video
        };
        if !is_seq_header && self.track_input_ts(track, tags, i) {
            let last = match track {
                Track::Audio => self.state.last_audio_output_ts_ms,
                Track::Video => self.state.last_video_output_ts_ms,
            };
            return last.saturating_add(1);
        }
        let pin = match self.state.origin_ts {
            Some(_) => tag.timestamp_ms, // unused: the mapping is pinned
            None => robust_pin(tags, i).unwrap_or(tag.timestamp_ms),
        };
        if is_seq_header {
            return self.state.wire_ts_unpinned(tag.timestamp_ms, pin);
        }
        let ts = self.state.wire_ts(tag.timestamp_ms, pin);
        let (input, wire) = (i64::from(tag.timestamp_ms), i64::from(ts as u32));
        match track {
            Track::Audio => self.av_guard.observe_audio(input, wire),
            Track::Video => self.av_guard.observe_video(input, wire),
        }
        ts
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

    /// Per-session state reset after a fresh RTMP connect.
    ///
    /// Codec config must be re-sent on every fresh RTMP session so the
    /// receiver can decode subsequent NALU/raw-AAC tags. A NEW shared wire
    /// mapping starts (#367): one base for both tracks, one past the highest
    /// output_ts sent on either track (the wire stays monotonic even if xiu's
    /// RTMP session resets to ts=0), and one origin re-pinned by the first
    /// chunk. The wire A/V relation after a reconnect is therefore the content
    /// relation, never pusher history.
    ///
    /// History of the base choice: it used to be the LATER of (1) one ms past
    /// the highest output_ts already sent and (2) wall-clock since the pusher
    /// session started (a #103 resilience-test fix). #171 dropped the
    /// wall-clock floor: after a RemoteClosed gap, `last_output+1` lets
    /// per-tag pacing burst buffered chunks until output catches wall, and the
    /// chunk-end rate cap (`chunk_pacing_sleep_ms`, CATCHUP_FACTOR_PCT) bounds
    /// that burst.
    fn on_fresh_session(&mut self) {
        self.state.connected = true;
        self.state.avc_seq_header_sent = false;
        self.state.aac_seq_header_sent = false;
        // Also clears last_*_xiu_ts: the new session starts fresh.
        self.state.begin_new_mapping();
        // A new mapping is a new transform: never pair an old-mapping sample
        // of one track with a new-mapping sample of the other.
        self.av_guard.begin_new_transform();
        // The relative skew detector measures from the new session (#257).
        // Since #367 a reconnect no longer changes the wire A/V relation (it
        // is the content's by construction), so this resets the baseline; it
        // does not "fix" a content-side STEP.
        self.skew.reset_tracks();
    }

    /// De-duplicate codec sequence headers across an RTMP session: `true`
    /// when this one must be skipped (#103: re-sending made the receiver
    /// reset its decoder).
    fn skip_repeated_seq_header(&mut self, tag_type: u8, is_seq_header: bool) -> bool {
        if !is_seq_header {
            return false;
        }
        let sent = match tag_type {
            crate::flv::FLV_TAG_VIDEO => &mut self.state.avc_seq_header_sent,
            crate::flv::FLV_TAG_AUDIO => &mut self.state.aac_seq_header_sent,
            _ => return false,
        };
        std::mem::replace(sent, true)
    }

    /// Per-tag pacing: sleep until wall-clock catches up to this tag's PTS
    /// (`tag_sleep`). Both `output_ts` and `anchor.elapsed()` live in the
    /// same ms domain.
    async fn pace_tag(&self, anchor: Instant, tag_type: u8, output_ts: u64) {
        let actual_ms = anchor.elapsed().as_millis() as u64;
        let Some(sleep) = tag_sleep(actual_ms, output_ts) else {
            return;
        };
        if sleep.is_long() {
            tracing::warn!(
                tag_type,
                output_ts,
                actual_ms,
                raw_sleep_ms = sleep.raw_ms,
                clamped_to_ms = sleep.sleep_ms,
                last_audio_output_ts_ms = self.state.last_audio_output_ts_ms,
                last_video_output_ts_ms = self.state.last_video_output_ts_ms,
                base_ms = self.state.base_ms,
                "rtmp_push: LONG per-tag pacing sleep (>=2s) -- output_ts ahead of wall by {}ms; \
                 clamped to {}ms",
                sleep.raw_ms,
                sleep.sleep_ms
            );
        }
        tokio::time::sleep(Duration::from_millis(sleep.sleep_ms)).await;
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
    /// by both tracks (`map_media_tag`, #367): the wire A/V relation is
    /// exactly the chunk's content relation, and each track's wire timeline
    /// stays monotonic and continuous across chunk boundaries (#103).
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
            self.on_fresh_session();
        }

        // Empty slice -> handshake verified, nothing to send.
        if bytes.is_empty() {
            return Ok(());
        }

        // The origin is per MAPPING (fresh session / re-anchor), never per
        // chunk, so each track's wire ts stays continuous across chunk
        // boundaries. The #103 click came from a per-CHUNK rebase. Per-TRACK
        // origins (the #103 fix) were rejected for #367 because they made the
        // wire offset depend on which track's first tag a mapping saw first.
        let tags: Vec<crate::flv::FlvTag<'_>> = crate::flv::FlvTagIter::new(bytes)?.collect();
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

        for i in 0..tags.len() {
            let tag = &tags[i];
            // Sequence headers (codec config: AVC SPS/PPS, AAC config) are
            // identified by body[1] == 0x00. The chunker writes them at the
            // START of every chunk with ts=0 so each S3 chunk is a
            // self-contained FLV file for the ffmpeg path.
            let is_seq_header = is_media_seq_header(tag);

            let (output_ts_u64, track_max) = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => (
                    self.map_media_tag(&tags, i, is_seq_header),
                    &mut max_audio_output_ts,
                ),
                crate::flv::FLV_TAG_VIDEO => (
                    self.map_media_tag(&tags, i, is_seq_header),
                    &mut max_video_output_ts,
                ),
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

            let skip = self.skip_repeated_seq_header(tag.tag_type, is_seq_header);
            self.pace_tag(anchor, tag.tag_type, output_ts_u64).await;

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

            // #124 cancel-safety: advance per-track output bookkeeping per
            // SUCCESSFULLY sent tag. The rescue keepalive bridge races this
            // `push_flv_bytes` future against `rx.recv()` / stop and drops it
            // mid-chunk on recovery; without a per-tag advance the pusher's
            // `last_*_output_ts_ms` stays at the pre-push value while tags
            // were already sent on the wire, so the NEXT push re-anchors from
            // a stale (too-low) base and emits BACKWARD wire timestamps for
            // content already delivered — a non-monotonic-PTS glitch exactly
            // at the clean-recovery moment #124 exists to make clean. The
            // post-loop max update below is now redundant on the happy path.
            // #367: it also keeps a MID-chunk re-anchor's shared base past
            // what this chunk already sent.
            if !skip {
                match tag.tag_type {
                    crate::flv::FLV_TAG_AUDIO => {
                        self.state.last_audio_output_ts_ms =
                            self.state.last_audio_output_ts_ms.max(output_ts_u64);
                    }
                    crate::flv::FLV_TAG_VIDEO => {
                        self.state.last_video_output_ts_ms =
                            self.state.last_video_output_ts_ms.max(output_ts_u64);
                    }
                    _ => {}
                }
                self.state.last_output_ts_ms = self.state.last_output_ts_ms.max(output_ts_u64);
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

        // #367: the absolute (no-baseline) invariant -- evaluated first so a
        // violation is queued for audit even when the skew guard trips below.
        self.evaluate_av_invariant();
        // Issue #257 — cross-track A/V-skew guard (relative to its baseline;
        // it watches for a source-side DRIFT). A sustained over-threshold
        // skew trips a CLEAN reconnect: drop the session and return
        // AvSkewExceeded so the consumer force-closes. Strict 1× — recovery
        // is ONLY a reconnect, never a speed-up. Debounced + rate-limited
        // inside SkewTracker so a persistent upstream skew cannot thrash
        // reconnects.
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
    /// timestamp anomaly and started a new shared wire mapping (#367). Mirrors
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
#[path = "pusher_tests.rs"]
mod tests;
