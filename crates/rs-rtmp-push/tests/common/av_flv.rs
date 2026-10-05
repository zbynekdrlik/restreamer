//! Synthetic A/V FLV chunks with traceable tags for the #367 wire-relation
//! tests. Included per test binary with
//! `#[path = "common/av_flv.rs"] mod av_flv;` (NOT through `common/mod.rs`, so
//! binaries that do not need it are unaffected).

#![allow(dead_code)]

use crate::common::RecordedTag;

pub const FLV_AUDIO: u8 = 8;
pub const FLV_VIDEO: u8 = 9;

/// Tag body tagged with a session marker and the tag's CONTENT ts, so the
/// recording can be searched for the exact tag of a content instant even when
/// its FLV timestamp was deliberately corrupted.
pub fn body(tag_type: u8, keyframe: bool, session: u8, ts: u32) -> Vec<u8> {
    let t = ts.to_be_bytes();
    match tag_type {
        FLV_VIDEO => {
            let frame = if keyframe { 0x17 } else { 0x27 };
            vec![frame, 0x01, 0, 0, 0, session, t[0], t[1], t[2], t[3]]
        }
        _ => vec![0xAF, 0x01, session, t[0], t[1], t[2], t[3]],
    }
}

/// One tag of a synthetic chunk: (FLV ts, tag type, body).
pub type Tag = (u32, u8, Vec<u8>);

/// Video every 40 ms from `video_from` (the first one a keyframe) and audio
/// every 20 ms from `audio_from`, both up to `to`, in content order (video
/// first at equal ts, so the keyframe opens the chunk).
pub fn av_tags(video_from: u32, audio_from: u32, to: u32, session: u8) -> Vec<Tag> {
    let mut tags: Vec<Tag> = Vec::new();
    let mut v = video_from;
    while v <= to {
        tags.push((v, FLV_VIDEO, body(FLV_VIDEO, v == video_from, session, v)));
        v += 40;
    }
    let mut a = audio_from;
    while a <= to {
        tags.push((a, FLV_AUDIO, body(FLV_AUDIO, false, session, a)));
        a += 20;
    }
    tags.sort_by_key(|(ts, ty, _)| (*ts, if *ty == FLV_AUDIO { 1 } else { 0 }));
    tags
}

/// Serialize tags into an FLV byte stream (header + tags + trailers).
pub fn to_flv(tags: &[Tag]) -> Vec<u8> {
    let mut out = vec![b'F', b'L', b'V', 1, 0x05, 0, 0, 0, 9, 0, 0, 0, 0];
    for (ts, tag_type, body) in tags {
        let size = body.len() as u32;
        out.push(*tag_type);
        out.extend_from_slice(&size.to_be_bytes()[1..]);
        out.extend_from_slice(&(ts & 0x00FF_FFFF).to_be_bytes()[1..]);
        out.push((ts >> 24) as u8);
        out.extend_from_slice(&[0, 0, 0]);
        out.extend_from_slice(body);
        out.extend_from_slice(&(11 + size).to_be_bytes());
    }
    out
}

/// One FLV chunk as the chunker produces it (see `av_tags`).
pub fn av_chunk(video_from: u32, audio_from: u32, to: u32, session: u8) -> Vec<u8> {
    to_flv(&av_tags(video_from, audio_from, to, session))
}

/// Corrupt the FLV ts of the tag of `tag_type` whose CONTENT ts is `at`.
pub fn glitch(tags: &mut [Tag], tag_type: u8, at: u32, glitch_ts: u32) {
    let tag = tags
        .iter_mut()
        .find(|(ts, ty, _)| *ty == tag_type && *ts == at)
        .unwrap_or_else(|| panic!("no tag of type {tag_type} at {at}"));
    tag.0 = glitch_ts;
}

/// Wire ts of the recorded tag carrying content ts `ts` of `session`.
pub fn wire_ts(recorded: &[RecordedTag], tag_type: u8, session: u8, ts: u32) -> u32 {
    let t = ts.to_be_bytes();
    recorded
        .iter()
        .find(|r| {
            let tail = &r.body[r.body.len().saturating_sub(5)..];
            r.tag_type == tag_type && tail == [session, t[0], t[1], t[2], t[3]]
        })
        .map(|r| r.timestamp_ms)
        .unwrap_or_else(|| panic!("tag type {tag_type} session {session} ts {ts} not recorded"))
}

/// Wire ts of every recorded tag of `tag_type`, in arrival order.
pub fn wire_track(recorded: &[RecordedTag], tag_type: u8) -> Vec<u32> {
    recorded
        .iter()
        .filter(|r| r.tag_type == tag_type)
        .map(|r| r.timestamp_ms)
        .collect()
}

/// Push one chunk, failing the test (instead of hanging) if the pusher
/// freezes in pacing for more than 10 s, as it does when a wire ts runs far
/// ahead of wall clock.
pub async fn push_within(pusher: &mut rs_rtmp_push::RtmpPusher, chunk: &[u8], what: &str) {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pusher.push_flv_bytes(chunk),
    )
    .await
    .unwrap_or_else(|_| panic!("{what}: the pusher froze in pacing (> 10 s)"))
    .unwrap_or_else(|e| panic!("{what}: push failed: {e:?}"));
}
