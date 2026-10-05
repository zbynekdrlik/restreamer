//! The per-tag ingest path of the FLV chunker (#367): classify each tag's
//! publisher source ts (`src_track`), hold a far-backward tag until the next
//! one shows what it was, and stamp both tracks `src - session_origin`.
//!
//! A far backward step means either a NEW publisher reusing the stream
//! identifier (its Publish never reached the receiver), or ONE corrupt
//! timestamp. The two only differ in what comes next, so the tag is held:
//! - the next tag of EITHER track is also far behind its track: a new
//!   timeline. The session re-anchors for BOTH tracks (the old partial chunk
//!   is flushed) and the held tag heads the new session, so a new
//!   publisher's first keyframe is not lost;
//! - the next tag of the held tag's own track is back on the old timeline:
//!   the held tag was a lone glitch. It is written stamped at the track's
//!   last ts (kept for decoding, like jitter) and nothing re-anchors.
//!
//! At most ONE tag is held: a second far-backward tag always decides for a
//! new timeline. A session re-anchor (`clear_session_epoch`) drops it.

use bytes::BytesMut;
use rs_rtmp_push::AvInvariantEvent;
use tracing::{debug, warn};

use super::{
    FLV_TAG_AUDIO, FLV_TAG_VIDEO, FlvChunkSink, FlvChunkSinkInner, MAX_BUFFER_SIZE,
    PendingChunkWrite,
};
use crate::ingest_report::{BoundaryReport, publish_boundary, publish_reanchor};
use crate::src_track::{SrcStep, Track};

/// A far-backward tag waiting for the next tag to decide what it was.
pub(super) struct HeldTag {
    track: Track,
    src_ts: u32,
    /// The track's last ts when the tag was held: its stamp if it turns out
    /// to be a lone glitch.
    prev: u32,
    data: BytesMut,
}

/// A backward-jump session re-anchor, finished outside the lock.
pub(super) struct Reanchored {
    /// The old session's partial chunk, if it had any data.
    flushed: Option<PendingChunkWrite>,
    /// A latched invariant violation of the old session, now closed.
    invariant: Option<AvInvariantEvent>,
}

/// What ingesting one tag left to finish outside the lock.
#[derive(Default)]
pub(super) struct TagEffects {
    reanchored: Option<Reanchored>,
    boundaries: Vec<BoundaryReport>,
    pending: Vec<PendingChunkWrite>,
}

impl FlvChunkSink {
    /// Ingest one media tag (never a sequence header) under the lock.
    pub(super) fn ingest_tag(
        inner: &mut FlvChunkSinkInner,
        track: Track,
        src_ts: u32,
        data: &BytesMut,
    ) -> TagEffects {
        let mut fx = TagEffects::default();
        let step = inner.src_track(track).classify(src_ts);
        if let SrcStep::FarBackward { prev } = step {
            match inner.held.take() {
                None => {
                    warn!(
                        ?track,
                        src_ts,
                        prev_src_ts = prev,
                        "flv_chunker: source ts jumped far backward -- holding the tag until the \
                         next one shows a new publisher or a lone glitch (#367)"
                    );
                    inner.held = Some(HeldTag {
                        track,
                        src_ts,
                        prev,
                        data: data.clone(),
                    });
                }
                Some(held) => {
                    fx.reanchored = Some(Self::reanchor_session(inner, track, prev, src_ts));
                    // The held tag came first: it heads the new session.
                    Self::process_tag(
                        inner,
                        held.track,
                        held.src_ts,
                        SrcStep::Continue,
                        &held.data,
                        &mut fx,
                    );
                    Self::process_tag(inner, track, src_ts, SrcStep::Continue, data, &mut fx);
                }
            }
            return fx;
        }

        if let Some(held) = inner.held.take_if(|h| h.track == track) {
            warn!(
                ?track,
                glitch_src_ts = held.src_ts,
                stamped_as = held.prev,
                "flv_chunker: the held far-backward tag was a lone glitch -- kept, stamped at the \
                 track's last ts, no re-anchor (#367)"
            );
            let clamp = SrcStep::ClampTiny { to: held.prev };
            Self::process_tag(inner, held.track, held.src_ts, clamp, &held.data, &mut fx);
        }
        match step {
            SrcStep::Continue | SrcStep::FarBackward { .. } => {}
            SrcStep::FarForward => warn!(
                ?track,
                src_ts,
                "flv_chunker: far forward source-ts step -- written as is but not counted into \
                 the chunk duration; the next tag shows whether it was a lone glitch (#367)"
            ),
            SrcStep::ClampTiny { to } => warn!(
                ?track,
                src_ts,
                stamped_as = to,
                "flv_chunker: tiny backward source-ts step (jitter) -- stamped at the track's \
                 last ts, no re-anchor (#367)"
            ),
            SrcStep::AfterGlitch { glitch } => warn!(
                ?track,
                src_ts,
                glitch_src_ts = glitch,
                "flv_chunker: previous tag was a lone forward source-ts glitch -- back on the \
                 timeline, no re-anchor (#367)"
            ),
        }
        Self::process_tag(inner, track, src_ts, step, data, &mut fx);
        fx
    }

    /// Outside-the-lock half of ingesting a tag, in index order: the old
    /// session's partial chunk of a re-anchor first, then the boundaries'
    /// banner/audit, then the chunks this tag closed.
    pub(super) async fn finish_tag(&self, fx: TagEffects) {
        if let Some(r) = fx.reanchored {
            if let Some(pending) = r.flushed {
                self.commit_chunk(pending).await;
            }
            publish_reanchor(self.ingest_state.as_ref(), r.invariant);
        }
        for boundary in fx.boundaries {
            publish_boundary(self.ingest_state.as_ref(), self.skew_threshold_ms, boundary);
        }
        for pending in fx.pending {
            self.commit_chunk(pending).await;
        }
    }

    fn process_tag(
        inner: &mut FlvChunkSinkInner,
        track: Track,
        src_ts: u32,
        step: SrcStep,
        data: &BytesMut,
        fx: &mut TagEffects,
    ) {
        match track {
            Track::Video => Self::process_video(inner, src_ts, step, data, fx),
            Track::Audio => Self::process_audio(inner, src_ts, step, data),
        }
    }

    /// Write one video tag stamped `src - session_origin` (#367): the SAME
    /// transform audio gets, so the publisher's A/V relationship survives
    /// whatever the arrival pattern is. Opens a new chunk on a keyframe once
    /// the chunk duration has passed.
    fn process_video(
        inner: &mut FlvChunkSinkInner,
        src_ts: u32,
        step: SrcStep,
        data: &BytesMut,
        fx: &mut TagEffects,
    ) {
        let is_keyframe = !data.is_empty() && (data[0] >> 4) == 1;
        // A chunk -- and a session -- always starts on a keyframe. Drop
        // non-keyframes before that, BEFORE anchoring the session origin.
        if inner.chunk_start.is_none() && !is_keyframe {
            return;
        }
        let ts = Self::video_out_ts(inner, Self::stamped_src(step, src_ts));
        inner.video_src.record(src_ts, step);

        let boundary = is_keyframe
            && inner
                .chunk_start
                .is_some_and(|s| s.elapsed() >= inner.chunk_duration);
        // #354: a chunk boundary is where the ingest skew monitor is
        // evaluated. Evaluate the CLOSING chunk BEFORE this keyframe's ts is
        // observed (below), so the boundary reflects exactly the frames that
        // belonged to the chunk being flushed.
        if boundary {
            fx.pending.extend(Self::extract_chunk(inner));
            fx.boundaries.push(Self::evaluate_boundary(inner));
            Self::write_chunk_header(inner, ts);
        } else if inner.chunk_start.is_none() {
            // First keyframe -- start the chunk.
            Self::write_chunk_header(inner, ts);
        }

        // chunk_first_ts / chunk_last_ts / duration_ms derive from VIDEO
        // tags only (#146). The jumped tag of a far forward step (most likely
        // a lone glitch) does not extend the duration; a real sustained jump
        // does from the next tag on. The chunk's first ts is its EARLIEST
        // video ts: the successors of a glitched keyframe that opened the
        // chunk walk back below it.
        inner.chunk_first_ts = inner.chunk_first_ts.min(ts);
        if !matches!(step, SrcStep::FarForward) {
            inner.chunk_last_ts = ts;
        }
        Self::write_tag(inner, FLV_TAG_VIDEO, ts, data);
        // Observe the SAME stamped ts the pusher's SkewTracker will see
        // downstream, so ingest and VPS agree on the number (#354).
        inner.skew_monitor.observe_video(ts);
        // A clamped tag (jitter, a lone glitch) is deliberately off the
        // transform.
        if !matches!(step, SrcStep::ClampTiny { .. }) {
            inner
                .av_invariant
                .observe_video(i64::from(src_ts), i64::from(ts));
        }

        // Force-flush an oversized buffer. Not after a boundary: that one
        // just emptied it. This is ALSO a real chunk boundary for the skew
        // monitor (#354): a pathological stream that keeps hitting the 50 MB
        // force-flush (e.g. a misconfigured chunk_duration) must still
        // advance its debounce counter.
        if inner.buffer.len() >= MAX_BUFFER_SIZE && !boundary {
            warn!(
                "FLV chunk buffer exceeded {}MB limit, force-flushing",
                MAX_BUFFER_SIZE / (1024 * 1024)
            );
            fx.pending.extend(Self::extract_chunk(inner));
            fx.boundaries.push(Self::evaluate_boundary(inner));
        }
    }

    /// Write one audio tag stamped `src - session_origin`: the SAME shared
    /// origin as video, the source ts of the session's first keyframe
    /// (#367). The xiu inter-tag deltas (AAC cadence: 1024 samples, 21.3 ms
    /// at 48 kHz) are untouched, so the #142 chipmunk fix holds. Audio never
    /// touches `chunk_last_ts` (VIDEO-only chunk duration, #146).
    fn process_audio(inner: &mut FlvChunkSinkInner, src_ts: u32, step: SrcStep, data: &BytesMut) {
        inner.audio_src.record(src_ts, step);
        // Audio is written only inside a chunk, i.e. once the session's
        // first keyframe has anchored the shared origin.
        let origin = match (inner.chunk_start, inner.session_origin_src) {
            (Some(_), Some(origin)) => origin,
            _ => return,
        };
        let Some(audio_out) = Self::stamped_src(step, src_ts).checked_sub(origin) else {
            // Content earlier than the session's first keyframe has no place
            // on the session timeline -- same as audio before that keyframe.
            debug!(
                src_ts,
                session_origin_src = origin,
                "flv_chunker: dropping audio that precedes the session origin keyframe"
            );
            return;
        };
        Self::write_tag(inner, FLV_TAG_AUDIO, audio_out, data);
        // Observe the SAME ts the pusher's SkewTracker sees downstream, so
        // ingest and VPS skew agree (#354).
        inner.skew_monitor.observe_audio(audio_out);
        // A clamped tag (jitter, a lone glitch) is deliberately off the
        // transform.
        if !matches!(step, SrcStep::ClampTiny { .. }) {
            inner
                .av_invariant
                .observe_audio(i64::from(src_ts), i64::from(audio_out));
        }
    }

    /// The source ts a tag is stamped with: a clamped step is stamped at the
    /// track's last ts (monotonic per track); anything else as is.
    fn stamped_src(step: SrcStep, src_ts: u32) -> u32 {
        match step {
            SrcStep::ClampTiny { to } => to,
            _ => src_ts,
        }
    }

    /// #367: re-anchor the session for BOTH tracks after a confirmed
    /// backward source jump: extract the partial chunk (it belongs to the
    /// old publisher) and clear the session epoch.
    fn reanchor_session(
        inner: &mut FlvChunkSinkInner,
        track: Track,
        prev: u32,
        src_ts: u32,
    ) -> Reanchored {
        let flushed = Self::extract_chunk(inner);
        let old_origin = inner.session_origin_src;
        let invariant = Self::clear_session_epoch(inner);
        warn!(
            ?track,
            prev_src_ts = prev,
            new_src_ts = src_ts,
            old_session_origin_src = ?old_origin,
            chunk_index = inner.chunk_index,
            flushed_partial_chunk = flushed.is_some(),
            "flv_chunker: source ts stayed far behind -- a new publisher on the same identifier; \
             re-anchoring BOTH tracks on a new shared session origin (#367)"
        );
        Reanchored { flushed, invariant }
    }
}
