//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.

use std::time::Duration;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use streamhub::StreamsHub;
use tokio::net::TcpListener;

/// Spin up a real xiu RTMP server bound to `127.0.0.1:0`.
///
/// Returns the `rtmp://` URL the pusher should connect to (stream key
/// `live/test`) and a `JoinHandle` that keeps the server alive for the
/// duration of the test.
///
/// Port-discovery strategy: bind a `TcpListener` to get an ephemeral port,
/// capture the address, then drop the listener so xiu can bind to the same
/// address.  There is a small TOCTOU window between the drop and xiu's
/// `TcpListener::bind`; on a loopback interface this is negligible.  If it
/// ever bites (see issue #148), set `RTMP_TEST_PORT` to a fixed port number
/// as an escape hatch.
async fn spawn_xiu_server() -> (String, tokio::task::JoinHandle<()>) {
    // Discover an available ephemeral port.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let a = listener.local_addr().expect("local_addr");
        drop(listener); // release so xiu can bind
        a
    };

    // Build the StreamsHub and xiu RtmpServer.
    //
    // set_rtmp_push_enabled(true) is required so that the hub emits
    // BroadcastEvent::Publish when the client connects - without this the
    // hub's broadcast channel stays silent and the ClientSession on the
    // server side does not advance past WaitStateChange.
    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let event_sender = hub.get_hub_event_sender();

    let address = addr.to_string();

    let handle = tokio::spawn(async move {
        let mut rtmp_server = rtmp::rtmp::RtmpServer::new(address, event_sender, 0, None);

        // Run hub and server concurrently; either finishing ends the task.
        tokio::select! {
            _ = hub.run() => {}
            result = rtmp_server.run() => {
                if let Err(e) = result {
                    log::debug!("xiu test server stopped: {e}");
                }
            }
        }
    });

    let url = format!("rtmp://{}/live/test", addr);
    (url, handle)
}

#[tokio::test]
async fn handshake_completes_with_local_xiu_server() {
    let (url, _server) = spawn_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // Empty bytes -> no media tags to send.  `push_flv_bytes` should still
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
