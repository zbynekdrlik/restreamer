//! #424: the #296 slow refill must slow the WIRE, not only the consumer.
//!
//! The pusher paces every tag against a wall-clock anchor. The refill used to
//! sleep ~40 ms between chunks; the next chunk then went out with no pacing
//! sleep until it had caught up with the wall, so the delivery rate stayed
//! 1.0x and a drained cushion was never rebuilt (it stayed "refilling" for
//! whole 3-hour events on stream-pp).
//!
//! Runs against the real xiu server and real time: the pacing sleeps are what
//! is measured.

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;
use tokio::time::Instant;

/// Four 1-second chunks (content 0..3980 ms) with a 300 ms slowdown after
/// each of the first three: the last tag is due 900 ms later than the content
/// alone would put it.
#[tokio::test]
async fn slow_pacing_delays_every_following_tag() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    let started = Instant::now();
    for i in 0..4u32 {
        let from = i * 1000;
        push_within(&mut pusher, &av_chunk(from, from, from + 980, 1), "chunk").await;
        if i < 3 {
            pusher.slow_pacing(Duration::from_millis(300));
        }
    }
    let elapsed = started.elapsed();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rec = recorded.lock().await;
    // The last video tag (content 3960 ms) reached the server.
    wire_ts(&rec, FLV_VIDEO, 1, 3_960);

    // The last tag (content 3980 ms) is paced to anchor + 3980 + 3 x 300.
    // 100 ms of slack for the connect-to-first-tag offset of the anchor;
    // without the slowdown the push ends at ~3980 ms.
    assert!(
        elapsed >= Duration::from_millis(4_780),
        "4 chunks with 3 x 300 ms of slowdown took only {elapsed:?}: the \
         pacer caught the slowdown up"
    );
}
