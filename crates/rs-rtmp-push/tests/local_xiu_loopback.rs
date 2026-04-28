//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.
//!
//! Shared helpers (server harnesses, SHA-256 utilities, FLV generator, and
//! the hand-rolled rejecting server) live in `common/mod.rs`.

mod common;
use common::*;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::time::Duration;
use tokio::net::TcpListener;

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

/// Assert that every audio and video tag body byte that the pusher reads from
/// the source FLV arrives unmodified at the xiu server side.
///
/// Timing contract (no race conditions):
///   1. `spawn_recording_xiu_server` starts both the xiu server task and the
///      subscriber task.  The subscriber task blocks on `BroadcastEvent::Publish`.
///   2. The test starts the pusher.  The pusher performs the full RTMP negotiate
///      sequence (handshake + connect + createStream + publish).  When the
///      server's `onStatus(NetStream.Publish.Start)` is sent, xiu internally
///      fires `BroadcastEvent::Publish` on the hub's broadcast channel.
///   3. The subscriber task receives `BroadcastEvent::Publish`, immediately sends
///      a `StreamHubEvent::Subscribe`, gets the frame receiver, and signals the
///      test via `sub_ready_rx`.
///   4. The test awaits `sub_ready_rx` (5-second timeout) before sending any
///      media tags.  By the time the first audio/video chunk is written, the
///      subscriber is already registered in the hub -- no frames are dropped.
///   5. After `push_flv_bytes` returns, we wait 2 seconds for the subscriber
///      task to drain any in-flight frames, then compare SHA-256 digests.
#[tokio::test]
async fn media_payload_byte_identical_to_source() {
    let source_bytes = std::fs::read("tests/data/short.flv").expect("read short.flv");

    let (url, recorded, _server, sub_ready_rx) = spawn_recording_xiu_server().await;

    // Give the server a moment to bind and start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    // Step 1: perform the RTMP negotiate (handshake + connect + publish).
    // This triggers BroadcastEvent::Publish inside xiu, which the subscriber
    // task is waiting for.  We call push_flv_bytes with an empty slice here
    // so the publish handshake completes but no media is sent yet.
    //
    // NOTE: push_flv_bytes with an empty slice parses zero tags and returns
    // immediately after the lazy connect -- it does NOT send any media chunks.
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake must succeed");

    eprintln!("[test] publish handshake complete; waiting for subscriber ready signal...");

    // Step 2: wait for the subscriber task to confirm it has obtained the
    // frame receiver from the hub.  This eliminates the race between xiu's
    // BroadcastEvent::Publish and our Subscribe request.
    tokio::time::timeout(Duration::from_secs(5), sub_ready_rx)
        .await
        .expect("subscriber task did not signal ready within 5s")
        .expect("sub_ready channel dropped before signal");

    eprintln!("[test] subscriber ready; sending media...");

    // Step 3: now send the actual media payload.  The subscriber is registered
    // so no frames will be dropped.
    pusher
        .push_flv_bytes(&source_bytes)
        .await
        .expect("push_flv_bytes");

    eprintln!("[test] push_flv_bytes complete; waiting for drain...");

    // Give the subscriber task time to drain the last frames from the channel.
    // The channel closes once the pusher drops the session (TCP disconnect),
    // which happens when `pusher` is dropped at end of scope.  We wait here
    // while the pusher is still alive, then allow the drain to complete.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let recorded_guard = recorded.lock().await;
    eprintln!("[test] recorded {} tags", recorded_guard.len());
    assert!(
        !recorded_guard.is_empty(),
        "no tags reached the server; check eprintln output above"
    );

    let (src_audio_sha, src_video_sha) = sha256_flv_bodies(&source_bytes);
    let (rec_audio_sha, rec_video_sha) = sha256_recorded_bodies(&recorded_guard);

    assert_eq!(rec_audio_sha, src_audio_sha, "audio body bytes diverged");
    assert_eq!(rec_video_sha, src_video_sha, "video body bytes diverged");
}

// publish_rejected_on_invalid_stream_key was removed in PR #103: the
// hand-rolled rejecting server in common/run_rejecting_server produces
// AMF "pack error" / "none return" inside xiu's MessageParser, so the
// test fails for an infrastructure reason instead of asserting the real
// PublishRejected path. The PublishRejected path itself is implemented
// (see crates/rs-rtmp-push/src/session.rs::wait_for_publish_start).
// Tracked: issue #149 -- re-add with a working AMF harness.

/// Assert that output timestamps remain monotonic across a pusher reconnect
/// AND that `reconnect_count()` increments from 0 to 1.
///
/// Scenario (Approach C -- sequential port-reuse):
///   1. Discover ephemeral port P and bind server A there.
///   2. Construct `RtmpPusher` pointed at `rtmp://127.0.0.1:P/live/rec`.
///   3. Push `chunk1` (TS 0..=500) to server A.
///   4. Abort server A; call `pusher.close()` to drop the stale session.
///   5. Wait briefly so the OS finishes releasing port P.
///   6. Bind server B at the SAME port P.
///   7. Push `chunk2` (TS 0..=500 again) to server B.
///      The pusher sees `session == None` and reconnects.
///   8. Assert combined wire timestamps (server A then server B) are monotonic.
///   9. Assert `reconnect_count() == 1` (Task 8 implements the increment; this
///      assertion is the TDD RED for Task 7 -- it will fail until Task 8 lands).
///
/// Port-reuse note: after aborting server A the OS transitions the listening
/// socket through TIME_WAIT.  We sleep 400 ms before binding server B to give
/// the kernel time to release the port.  On Linux loopback `SO_REUSEADDR` makes
/// immediate rebind possible, but we rely on the sleep for portability.
#[tokio::test]
async fn monotonic_ts_across_reconnect() {
    // --- Step 1: discover ephemeral port ------------------------------------
    let addr = {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe listener");
        let a = probe.local_addr().expect("probe local_addr");
        drop(probe);
        a
    };

    let url = format!("rtmp://{}/live/rec", addr);

    // --- Step 2: spin up server A and the pusher ----------------------------
    let (recorded_a, server_a, sub_ready_a) = spawn_recording_xiu_server_at(addr).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut pusher = RtmpPusher::new(url.clone(), PusherConfig::default());

    // Handshake with server A (empty push to trigger publish so the subscriber
    // task can register its frame receiver).
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake with server A");

    eprintln!("[reconnect-test] handshake A done; waiting for subscriber A...");

    tokio::time::timeout(Duration::from_secs(5), sub_ready_a)
        .await
        .expect("subscriber A did not signal within 5s")
        .expect("sub_ready_a channel dropped");

    eprintln!("[reconnect-test] subscriber A ready; pushing chunk1...");

    // --- Step 3: push chunk1 (TS 0..=500) to server A ----------------------
    let chunk1 = synthetic_audio_flv(0, 500);
    pusher
        .push_flv_bytes(&chunk1)
        .await
        .expect("push chunk1 to server A");

    // Allow the subscriber task on server A to drain in-flight frames.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let last_ts_a = {
        let guard = recorded_a.lock().await;
        eprintln!("[reconnect-test] server A recorded {} tags", guard.len());
        guard.last().map(|t| t.timestamp_ms).unwrap_or(0)
    };

    eprintln!("[reconnect-test] last wire TS from server A: {}", last_ts_a);

    // --- Step 4: kill server A and drop the pusher session ------------------
    server_a.abort();
    pusher.close().await;

    eprintln!("[reconnect-test] server A aborted; session closed; sleeping before rebind...");

    // --- Step 5: wait for OS to release the port ----------------------------
    tokio::time::sleep(Duration::from_millis(400)).await;

    // --- Step 6: spin up server B at the same port --------------------------
    let (recorded_b, _server_b, sub_ready_b) = spawn_recording_xiu_server_at(addr).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    eprintln!("[reconnect-test] server B started; pushing chunk2 (triggers reconnect)...");

    // --- Step 7: push chunk2 (TS 0..=500 internally; wire TS must continue) -
    // The pusher finds session == None (closed in step 4) and reconnects.
    // After Task 8, this increments reconnect_count from 0 to 1.
    let chunk2 = synthetic_audio_flv(0, 500);

    // Trigger the reconnect and the subscriber registration on server B.
    pusher
        .push_flv_bytes(&[])
        .await
        .expect("handshake with server B (reconnect)");

    eprintln!("[reconnect-test] handshake B done; waiting for subscriber B...");

    tokio::time::timeout(Duration::from_secs(5), sub_ready_b)
        .await
        .expect("subscriber B did not signal within 5s")
        .expect("sub_ready_b channel dropped");

    eprintln!("[reconnect-test] subscriber B ready; pushing chunk2 media...");

    pusher
        .push_flv_bytes(&chunk2)
        .await
        .expect("push chunk2 to server B");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // --- Step 8: assert combined timestamps are monotonic -------------------
    let guard_a = recorded_a.lock().await;
    let guard_b = recorded_b.lock().await;

    eprintln!(
        "[reconnect-test] server A: {} tags, server B: {} tags",
        guard_a.len(),
        guard_b.len()
    );

    assert!(
        !guard_a.is_empty(),
        "no tags reached server A; check eprintln output"
    );
    assert!(
        !guard_b.is_empty(),
        "no tags reached server B; check eprintln output"
    );

    // Collect all wire timestamps in order: server A first, then server B.
    let all_ts: Vec<u32> = guard_a
        .iter()
        .map(|t| t.timestamp_ms)
        .chain(guard_b.iter().map(|t| t.timestamp_ms))
        .collect();

    let mut last = 0u32;
    for ts in &all_ts {
        assert!(
            *ts >= last,
            "timestamp regressed: {} < {} (combined wire TS must be monotonic across reconnect)",
            ts,
            last
        );
        last = *ts;
    }

    eprintln!(
        "[reconnect-test] monotonic TS check passed (first={}, last={})",
        all_ts.first().copied().unwrap_or(0),
        all_ts.last().copied().unwrap_or(0)
    );

    // --- Step 9: assert reconnect_count incremented (TDD RED until Task 8) --
    // Task 8 adds: if is_reconnect { self.state.reconnect_count += 1; }
    // Until then, reconnect_count stays 0 and this assertion fails.
    assert_eq!(
        pusher.reconnect_count(),
        1,
        "expected reconnect_count == 1 after one session drop + reconnect; got {}",
        pusher.reconnect_count()
    );
}
