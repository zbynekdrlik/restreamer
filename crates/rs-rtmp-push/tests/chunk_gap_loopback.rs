//! #437: a forward chunk-id gap must never become a pause on the wire.
//!
//! A skipped ~2 s chunk is a +2 s step in the source ts. That is far below
//! the 30 s re-anchor threshold, so before #437 the pusher mapped it straight
//! onto the wire: the next chunk's first tag was ~2 s ahead of wall clock and
//! per-tag pacing slept until it was due. YouTube got ~2 s with no data, and
//! a Low-latency player showed a freeze (PP live 25.11.2026, evt16). The
//! consumer now calls `RtmpPusher::begin_new_mapping` before such a chunk:
//! the wire continues at the last output + 1 and the content jump is a clean
//! cut with no wire gap.
//!
//! Runs against the real xiu server, which records the wire ts it receives.

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::{Duration, Instant};

/// Wire ts of one track in arrival order must never step forward by more
/// than one frame of that track plus a small margin.
fn max_forward_step(track: &[u32]) -> u32 {
    track
        .windows(2)
        .map(|w| w[1].saturating_sub(w[0]))
        .max()
        .unwrap_or(0)
}

#[tokio::test]
async fn a_chunk_gap_with_a_new_mapping_is_continuous_on_the_wire_and_never_paced() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    // Chunk N: content 0..480 ms.
    push_within(&mut pusher, &av_chunk(0, 0, 480, 1), "chunk N").await;
    // Chunk N+1 (content 500..2480) is skipped. Chunk N+2 opens at 2500:
    // a +2 s source step, exactly the skipped chunk.
    pusher.begin_new_mapping();
    let started = Instant::now();
    push_within(&mut pusher, &av_chunk(2_500, 2_500, 2_980, 1), "chunk N+2").await;
    let took = started.elapsed();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A pusher that paced the skipped 2 s needs ~2.5 s for this 480 ms chunk.
    assert!(
        took < Duration::from_millis(1_500),
        "the chunk after a gap must be paced at its own duration, not the gap's: took {took:?}"
    );
    let rec = recorded.lock().await;
    let video = wire_track(&rec, FLV_VIDEO);
    let audio = wire_track(&rec, FLV_AUDIO);
    assert!(
        max_forward_step(&video) <= 41,
        "video wire ts must stay continuous across the gap: {video:?}"
    );
    assert!(
        max_forward_step(&audio) <= 41,
        "audio wire ts must stay continuous across the gap: {audio:?}"
    );
    // The A/V content relation of the new chunk survives the new mapping.
    let v0 = wire_ts(&rec, FLV_VIDEO, 1, 2_500);
    let a0 = wire_ts(&rec, FLV_AUDIO, 1, 2_500);
    assert_eq!(v0, a0, "same content instant, same wire instant");
    assert!(
        v0 > wire_ts(&rec, FLV_VIDEO, 1, 480),
        "the wire never steps back"
    );
}

/// Without a new mapping (a contiguous next chunk) nothing changes: the
/// wire continues with the content's own spacing.
#[tokio::test]
async fn a_contiguous_chunk_keeps_its_content_spacing() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    push_within(&mut pusher, &av_chunk(0, 0, 480, 2), "chunk N").await;
    push_within(&mut pusher, &av_chunk(520, 500, 980, 2), "chunk N+1").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    assert_eq!(
        wire_ts(&rec, FLV_VIDEO, 2, 520) - wire_ts(&rec, FLV_VIDEO, 2, 480),
        40,
        "a contiguous chunk is mapped through the same transform"
    );
}
