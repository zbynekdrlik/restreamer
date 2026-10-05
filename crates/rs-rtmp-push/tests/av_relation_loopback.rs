//! #367 design test 6: the pusher must keep the WIRE A/V relation equal to
//! the CONTENT (chunk tag) relation across a re-anchor and across a
//! reconnect.
//!
//! The pre-fix pusher re-pinned a per-track origin on the first tag of EACH
//! track after a reconnect or re-anchor. When a chunk started with video and
//! its audio only began 700 ms later, both first tags mapped to the same wire
//! instant: the 700 ms content relation collapsed to 0 on the wire, so the
//! wire offset depended on pusher history instead of the content. One shared
//! origin and one shared base for both tracks keep the content relation.
//!
//! Runs against the real xiu server (`common::spawn_recording_xiu_server*`),
//! which records the wire timestamps the receiver actually sees.

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;
use tokio::net::TcpListener;

/// Re-anchor path: same RTMP session, the chunker starts a new session
/// (backward content ts). The new chunk opens with a video keyframe at 0 and
/// its audio starts at 700. On the wire, audio must still start 700 ms after
/// the keyframe.
#[tokio::test]
async fn reanchor_keeps_wire_av_relation_equal_to_content_relation() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    // Session A: aligned A/V far into a stream.
    push_within(
        &mut pusher,
        &av_chunk(600_000, 600_000, 600_400, 0x0A),
        "session A",
    )
    .await;
    // Session B: backward jump -> re-anchor; audio starts 700 ms after video.
    push_within(&mut pusher, &av_chunk(0, 700, 1_000, 0x0B), "session B").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    let v0 = wire_ts(&rec, FLV_VIDEO, 0x0B, 0);
    let a0 = wire_ts(&rec, FLV_AUDIO, 0x0B, 700);
    assert_eq!(
        i64::from(a0) - i64::from(v0),
        700,
        "after a re-anchor the wire A/V relation must equal the content relation \
         (+700 ms); got video_wire={v0} audio_wire={a0}"
    );
    assert!(
        pusher.regression_reanchor_count() >= 1,
        "the backward content jump must have re-anchored"
    );
    // Design test 5 (push, silent side): the shared transform keeps the
    // absolute invariant through the re-anchor.
    assert_eq!(
        pusher.av_invariant_violation_count(),
        0,
        "the wire A/V invariant guard must stay silent across a re-anchor"
    );
}

/// Reconnect path: the RTMP session drops and the pusher reconnects. The
/// first chunk after the reconnect opens with a keyframe at 2_440 and its
/// audio starts at 3_140. On the new session's wire, audio must still start
/// 700 ms after the keyframe.
#[tokio::test]
async fn reconnect_keeps_wire_av_relation_equal_to_content_relation() {
    let addr = {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe listener");
        let a = probe.local_addr().expect("probe local_addr");
        drop(probe);
        a
    };
    let url = format!("rtmp://{addr}/live/rel");

    let (_recorded_a, server_a, sub_ready_a) = spawn_recording_xiu_server_at(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake with server A");
    tokio::time::timeout(Duration::from_secs(5), sub_ready_a)
        .await
        .expect("subscriber A did not signal within 5s")
        .expect("sub_ready_a channel dropped");
    push_within(
        &mut pusher,
        &av_chunk(2_000, 2_000, 2_400, 0x0A),
        "server A",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The session drops; a replacement server comes up on the same port.
    server_a.abort();
    pusher.close().await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (recorded_b, _server_b, sub_ready_b) = spawn_recording_xiu_server_at(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake with server B (reconnect)");
    tokio::time::timeout(Duration::from_secs(5), sub_ready_b)
        .await
        .expect("subscriber B did not signal within 5s")
        .expect("sub_ready_b channel dropped");

    push_within(
        &mut pusher,
        &av_chunk(2_440, 3_140, 3_600, 0x0B),
        "first chunk after reconnect",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded_b.lock().await;
    let v0 = wire_ts(&rec, FLV_VIDEO, 0x0B, 2_440);
    let a0 = wire_ts(&rec, FLV_AUDIO, 0x0B, 3_140);
    assert_eq!(
        i64::from(a0) - i64::from(v0),
        700,
        "after a reconnect the wire A/V relation must equal the content relation \
         (+700 ms); got video_wire={v0} audio_wire={a0}"
    );
    assert_eq!(
        pusher.reconnect_count(),
        1,
        "exactly one reconnect expected"
    );
    assert_eq!(
        pusher.av_invariant_violation_count(),
        0,
        "the wire A/V invariant guard must stay silent across a reconnect"
    );
}

/// #367 robustness of the shared mapping: ONE corrupt tag whose ts jumped
/// 720 s ahead (the #176/#178 shape) while the rest of the chunk stays on
/// the old timeline is an isolated outlier. It must not move the shared
/// mapping: with one origin for both tracks, a re-anchor pinned below it
/// would put it 720 s ahead on the wire and freeze the pusher in pacing.
/// The old per-track re-pin hid this by accident.
#[tokio::test]
async fn isolated_forward_glitch_does_not_move_the_shared_mapping() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    let mut tags = av_tags(0, 0, 1_000, 0x0C);
    glitch(&mut tags, FLV_VIDEO, 400, 400 + 720_000);
    let chunk = to_flv(&tags);
    push_within(&mut pusher, &chunk, "isolated glitch").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rec = recorded.lock().await;
    let video: Vec<u32> = rec
        .iter()
        .filter(|r| r.tag_type == FLV_VIDEO)
        .map(|r| r.timestamp_ms)
        .collect();
    assert!(
        video.windows(2).all(|w| w[1] >= w[0]),
        "video wire ts must stay monotonic: {video:?}"
    );
    assert!(
        video.iter().all(|&ts| ts < 5_000),
        "the glitch must not jump the wire timeline: {video:?}"
    );
    // The clean tags after the glitch keep the content relation (coincident).
    let v = wire_ts(&rec, FLV_VIDEO, 0x0C, 800);
    let a = wire_ts(&rec, FLV_AUDIO, 0x0C, 800);
    assert_eq!(
        v, a,
        "coincident content after the glitch must stay coincident"
    );
}
