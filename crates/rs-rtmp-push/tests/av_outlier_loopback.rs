//! #367 robustness of the SHARED wire mapping (one origin + one base for both
//! tracks) against corrupt or discontinuous chunk timestamps, on the real xiu
//! server.
//!
//! With one shared origin, a single bad timestamp that is allowed to pin or
//! re-pin the mapping moves EVERY tag of the chunk on the wire. The old
//! per-track re-pin happened to hide these cases. A wire ts far ahead of wall
//! clock freezes the pusher in pacing (5 s per tag, plus the chunk-end rate
//! cap) until the consumer's 30 s write timeout fires. A wire ts that goes
//! backward breaks the receiver.

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;

/// Connected pusher + its recording xiu server.
async fn connected() -> (
    RtmpPusher,
    std::sync::Arc<tokio::sync::Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
) {
    let (url, recorded, server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");
    (pusher, recorded, server)
}

fn assert_monotonic(track: &[u32], what: &str) {
    assert!(
        track.windows(2).all(|w| w[1] >= w[0]),
        "{what}: wire ts must never go backward: {track:?}"
    );
}

/// The HEAD tag of a track in a NEW mapping (after a re-anchor the per-track
/// trackers are empty, so the jump detector has nothing to compare against)
/// carries a corrupt ts 720 s ahead. It must be clamped, not mapped 720 s
/// ahead through the shared origin.
#[tokio::test]
async fn glitched_head_tag_of_a_new_mapping_is_clamped() {
    let (mut pusher, recorded, _server) = connected().await;
    push_within(
        &mut pusher,
        &av_chunk(600_000, 600_000, 600_400, 0x0A),
        "chunk A",
    )
    .await;

    // New chunker session: audio opens the chunk at 0 (re-anchor), and the
    // first VIDEO tag (the keyframe at 20) carries a corrupt ts.
    let mut tags = av_tags(20, 0, 1_000, 0x0B);
    glitch(&mut tags, FLV_VIDEO, 20, 20 + 720_000);
    push_within(&mut pusher, &to_flv(&tags), "glitched head").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    let video = wire_track(&rec, FLV_VIDEO);
    assert_monotonic(&video, "video");
    assert!(
        video.iter().all(|&ts| ts < 10_000),
        "the glitched head tag must not jump the wire: {video:?}"
    );
    let v = wire_ts(&rec, FLV_VIDEO, 0x0B, 420);
    let a = wire_ts(&rec, FLV_AUDIO, 0x0B, 420);
    assert_eq!(v, a, "coincident content stays coincident");
}

/// The FIRST chunk of a mapping contains one audio tag whose ts is corrupt
/// and far BELOW the rest. If it pins the shared origin (a plain minimum),
/// the whole chunk lands 600 s ahead on the wire. It must be clamped instead,
/// with the origin pinned by the chunk's bulk.
#[tokio::test]
async fn low_glitch_does_not_drag_the_shared_origin() {
    let (mut pusher, recorded, _server) = connected().await;
    let mut tags = av_tags(600_000, 600_000, 601_000, 0x0C);
    glitch(&mut tags, FLV_AUDIO, 600_600, 5);
    push_within(&mut pusher, &to_flv(&tags), "low glitch").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    for (track, name) in [(FLV_VIDEO, "video"), (FLV_AUDIO, "audio")] {
        let wire = wire_track(&rec, track);
        assert_monotonic(&wire, name);
        assert!(
            wire.iter().all(|&ts| ts < 10_000),
            "{name}: the chunk must not be shifted ahead by a low glitch: {wire:?}"
        );
    }
    let v = wire_ts(&rec, FLV_VIDEO, 0x0C, 600_800);
    let a = wire_ts(&rec, FLV_AUDIO, 0x0C, 600_800);
    assert_eq!(v, a, "coincident content stays coincident");
}

/// A GENUINE content gap of 39 s on BOTH tracks inside one chunk re-anchors
/// mid-chunk. The new shared base must start past what this chunk has
/// ALREADY sent (not just past the previous chunk), or the wire goes
/// backward. The gap must also not become a 39 s pacing sleep.
#[tokio::test]
async fn mid_chunk_gap_reanchor_never_goes_backward() {
    let (mut pusher, recorded, _server) = connected().await;
    let mut tags = av_tags(0, 0, 1_000, 0x0D);
    tags.extend(av_tags(40_000, 40_000, 41_000, 0x0D));
    push_within(&mut pusher, &to_flv(&tags), "gap chunk").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    for (track, name) in [(FLV_VIDEO, "video"), (FLV_AUDIO, "audio")] {
        assert_monotonic(&wire_track(&rec, track), name);
    }
    let v = wire_ts(&rec, FLV_VIDEO, 0x0D, 40_400);
    let a = wire_ts(&rec, FLV_AUDIO, 0x0D, 40_400);
    assert_eq!(v, a, "coincident content after the gap stays coincident");
}
