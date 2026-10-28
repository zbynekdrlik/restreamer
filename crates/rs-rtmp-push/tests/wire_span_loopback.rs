//! #424: `RtmpPusher::last_wire_span` reports the outgoing timestamps the
//! last push really put on the wire (the VPS per-chunk push log reads it).

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher, WireSpan};
use std::time::Duration;

#[tokio::test]
async fn the_wire_span_is_what_each_push_put_on_the_wire() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    assert_eq!(pusher.last_wire_span(), None, "nothing pushed yet");
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");
    assert_eq!(pusher.last_wire_span(), None, "a handshake sends no media");

    push_within(&mut pusher, &av_chunk(0, 0, 480, 1), "chunk 1").await;
    let first = pusher.last_wire_span().expect("chunk 1 span");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rec = recorded.lock().await;
    assert_eq!(
        first,
        WireSpan {
            first_ts_ms: u64::from(rec[0].timestamp_ms),
            last_ts_ms: u64::from(rec.iter().map(|r| r.timestamp_ms).max().unwrap()),
        },
        "chunk 1: the first tag sent and the highest one"
    );
    let chunk1_tags = rec.len();
    drop(rec);

    push_within(&mut pusher, &av_chunk(500, 500, 980, 1), "chunk 2").await;
    let second = pusher.last_wire_span().expect("chunk 2 span");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rec = recorded.lock().await;
    assert_eq!(
        second,
        WireSpan {
            first_ts_ms: u64::from(rec[chunk1_tags].timestamp_ms),
            last_ts_ms: u64::from(rec.iter().map(|r| r.timestamp_ms).max().unwrap()),
        },
        "chunk 2 has its own span, not chunk 1's"
    );
    assert!(
        second.first_ts_ms > first.last_ts_ms,
        "{first:?} {second:?}"
    );
    assert_eq!(
        second.first_ts_ms,
        u64::from(wire_ts(&rec, FLV_VIDEO, 1, 500)),
        "chunk 2 opens with its keyframe"
    );
}
