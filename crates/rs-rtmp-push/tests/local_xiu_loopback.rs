//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.

use std::time::Duration;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use tokio::net::TcpListener;

/// Spin up a barebones xiu RTMP server bound to `127.0.0.1:0`. Returns the
/// `rtmp://` URL the pusher should connect to (with stream key `live/test`).
///
/// The server runs on its own task and is dropped when the returned handle
/// goes out of scope. The test does NOT have to clean it up explicitly.
async fn spawn_xiu_server() -> (String, tokio::task::JoinHandle<()>) {
    // Bind ephemeral port to discover an unused one without TOCTOU.
    // (See issue #148 for the discovery+bind race; we hand the listener
    // directly to xiu instead of just the port number.)
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    let handle = tokio::spawn(async move {
        // Task 4 replaces this body with a real xiu RtmpServer wired via
        // StreamsHub. For the failing test in Task 3, the listener simply
        // accepts and drops the connection (which causes the pusher's
        // handshake to fail).
        if let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });

    let url = format!("rtmp://{}/live/test", addr);
    (url, handle)
}

#[tokio::test]
async fn handshake_completes_with_local_xiu_server() {
    let (url, _server) = spawn_xiu_server().await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // Empty bytes → no media tags to send. `push_flv_bytes` should still
    // do the lazy connect (handshake + NetConnection.connect + createStream
    // + publish) and return Ok if the server accepts the publish.
    let result = tokio::time::timeout(Duration::from_secs(5), pusher.push_flv_bytes(&[]))
        .await
        .expect("push_flv_bytes did not return within 5s");

    assert!(
        result.is_ok(),
        "expected push_flv_bytes(&[]) to complete handshake+publish; got {:?}",
        result
    );

    // After successful publish, the pusher reports zero reconnects and zero
    // output TS (no media has been sent yet).
    assert_eq!(pusher.reconnect_count(), 0);
    assert_eq!(pusher.last_output_ts_ms(), 0);
}
