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

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;
use tokio::net::TcpListener;

const FLV_AUDIO: u8 = 8;
const FLV_VIDEO: u8 = 9;

/// Tag body tagged with a session marker and the tag's content ts, so the
/// recording can be searched for the exact tag of a content instant.
fn body(tag_type: u8, keyframe: bool, session: u8, ts: u32) -> Vec<u8> {
    let t = ts.to_be_bytes();
    match tag_type {
        FLV_VIDEO => {
            let frame = if keyframe { 0x17 } else { 0x27 };
            vec![frame, 0x01, 0, 0, 0, session, t[0], t[1], t[2], t[3]]
        }
        _ => vec![0xAF, 0x01, session, t[0], t[1], t[2], t[3]],
    }
}

/// One FLV chunk as the chunker produces it: video every 40 ms from
/// `video_from` (the first one a keyframe) and audio every 20 ms from
/// `audio_from`, both up to `to`, interleaved in content order.
fn av_chunk(video_from: u32, audio_from: u32, to: u32, session: u8) -> Vec<u8> {
    let mut tags: Vec<(u32, u8, Vec<u8>)> = Vec::new();
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
    // Content order; video first at equal ts (the keyframe opens the chunk).
    tags.sort_by_key(|(ts, ty, _)| (*ts, if *ty == FLV_AUDIO { 1 } else { 0 }));

    let mut out = vec![b'F', b'L', b'V', 1, 0x05, 0, 0, 0, 9, 0, 0, 0, 0];
    for (ts, tag_type, body) in tags {
        let size = body.len() as u32;
        out.push(tag_type);
        out.extend_from_slice(&size.to_be_bytes()[1..]);
        out.extend_from_slice(&(ts & 0x00FF_FFFF).to_be_bytes()[1..]);
        out.push((ts >> 24) as u8);
        out.extend_from_slice(&[0, 0, 0]);
        out.extend_from_slice(&body);
        out.extend_from_slice(&(11 + size).to_be_bytes());
    }
    out
}

/// Wire ts of the recorded tag carrying content ts `ts` of `session`.
fn wire_ts(recorded: &[RecordedTag], tag_type: u8, session: u8, ts: u32) -> u32 {
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
    pusher
        .push_flv_bytes(&av_chunk(600_000, 600_000, 600_400, 0x0A))
        .await
        .expect("push session A chunk");
    // Session B: backward jump -> re-anchor; audio starts 700 ms after video.
    pusher
        .push_flv_bytes(&av_chunk(0, 700, 1_000, 0x0B))
        .await
        .expect("push session B chunk");
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
    pusher
        .push_flv_bytes(&av_chunk(2_000, 2_000, 2_400, 0x0A))
        .await
        .expect("push chunk to server A");
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

    pusher
        .push_flv_bytes(&av_chunk(2_440, 3_140, 3_600, 0x0B))
        .await
        .expect("push first chunk after reconnect");
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
