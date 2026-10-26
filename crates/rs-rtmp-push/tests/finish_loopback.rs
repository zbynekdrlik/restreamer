//! #424: `RtmpPusher::finish` ends a publish without losing its tail.
//!
//! Against the real xiu server: every media tag pushed before `finish`
//! arrives, FCUnpublish and deleteStream go out, and (on Linux) `finish`
//! returns only after the upstream acknowledged every byte.

mod common;
use common::*;

#[path = "common/av_flv.rs"]
mod av_flv;
use av_flv::*;

use rs_rtmp_push::{DrainOutcome, PusherConfig, RtmpPusher};
use std::time::Duration;

#[tokio::test]
async fn finish_unpublishes_and_waits_for_the_send_queue() {
    let (url, recorded, _server, sub_ready) = spawn_recording_xiu_server().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());
    pusher.push_flv_bytes(&[]).await.expect("handshake failed");
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber did not signal within 5s")
        .expect("sub_ready channel dropped");

    push_within(&mut pusher, &av_chunk(0, 0, 980, 1), "chunk").await;
    let report = tokio::time::timeout(
        Duration::from_secs(20),
        pusher.finish(Duration::from_secs(15)),
    )
    .await
    .expect("finish is bounded")
    .expect("a session was open");

    assert!(report.unpublished, "{report:?}");
    #[cfg(target_os = "linux")]
    {
        assert_eq!(report.outcome, DrainOutcome::Drained, "{report:?}");
        assert_eq!(report.queued_at_end, Some(0), "{report:?}");
    }
    #[cfg(not(target_os = "linux"))]
    assert_eq!(report.outcome, DrainOutcome::Unmeasured, "{report:?}");

    tokio::time::sleep(Duration::from_millis(300)).await;
    let rec = recorded.lock().await;
    wire_ts(&rec, FLV_VIDEO, 1, 960);
    wire_ts(&rec, FLV_AUDIO, 1, 980);
    drop(rec);
    assert!(
        pusher.finish(Duration::from_secs(1)).await.is_none(),
        "a finished pusher has no session left"
    );
}
