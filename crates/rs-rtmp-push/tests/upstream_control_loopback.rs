//! #368: the pusher answers the upstream's UserControl PingRequest and
//! records its Acknowledgements, against an in-process RTMP server that
//! sends both mid-session (`common/control_server.rs`).
//!
//! Before #368 xiu's `parse_event` rejected event 6 and the read loop dropped
//! the error, so a PingRequest was never answered (librtmp/OBS and ffmpeg
//! answer it), and incoming Acknowledgements were dropped, leaving the death
//! row's `time_since_last_upstream_ack_ms` permanently null.

mod common;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn a_ping_request_is_answered_and_an_ack_is_recorded() {
    const PING_TS: u32 = 0x1234_5678;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let (pong_tx, pong_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        if let Err(e) =
            common::control_server::run_control_server(listener, PING_TS, 4_096, pong_tx).await
        {
            eprintln!("[control-server] error: {e}");
        }
    });

    let mut pusher = RtmpPusher::new(format!("rtmp://{addr}/live2/key"), PusherConfig::default());
    tokio::time::timeout(Duration::from_secs(5), pusher.push_flv_bytes(&[]))
        .await
        .expect("connect did not finish within 5 s")
        .expect("the server accepts the publish");
    assert!(
        pusher.last_upstream_ack_at().is_none(),
        "no Acknowledgement has arrived yet"
    );

    // The server pings right after the client's onMetaData; the answer must
    // come back within a second, with the SAME timestamp.
    let pong = tokio::time::timeout(Duration::from_secs(1), pong_rx)
        .await
        .expect("no PingResponse within 1 s of the PingRequest")
        .expect("server dropped the pong channel");
    assert_eq!(
        pong, PING_TS,
        "PingResponse must echo the request timestamp"
    );

    // The Acknowledgement was sent right after the ping: it must be recorded.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while pusher.last_upstream_ack_at().is_none() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        pusher.last_upstream_ack_at().is_some(),
        "an upstream Acknowledgement must be recorded within 1 s"
    );
    // The server sent two PingRequests: both must be answered and counted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while pusher.pings_answered() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        pusher.pings_answered(),
        2,
        "both PingRequests must be answered"
    );

    pusher.close().await;
    assert!(
        pusher.last_upstream_ack_at().is_none(),
        "a closed session has no upstream acks"
    );
    assert_eq!(
        pusher.pings_answered(),
        0,
        "a closed pusher has no session, so no answered pings"
    );
    server.abort();
}
