//! Unit tests for the TEST_FILE loopback sink (#192).
//!
//! This module is compiled into BOTH the rs-delivery lib and bin test
//! processes. The bin process holds unit tests that assume
//! `127.0.0.1:1935` is REFUSED, so every sink here binds an EPHEMERAL
//! loopback port (`127.0.0.1:0`). The real-port tests live in
//! `tests/test_file_sink_e2e.rs`, which runs as its own process.

use super::*;

use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);

/// Poll a running slot's counters until `ok` holds. The wait is bounded, so
/// a broken sink fails fast and never hangs the suite.
async fn wait_counters(
    slot: &TestFileSinkSlot,
    what: &str,
    ok: impl Fn(&SinkCountersSnapshot) -> bool,
) -> SinkCountersSnapshot {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let c = slot
            .status()
            .await
            .unwrap_or_else(|| panic!("sink not running while waiting for {what}"))
            .counters;
        if ok(&c) {
            return c;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {WAIT:?} waiting for {what}; counters={c:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn sink_is_wanted_only_when_a_test_file_endpoint_is_configured() {
    assert!(
        wants_test_file_sink(["YT_RTMP", "TEST_FILE"]),
        "a set holding a TEST_FILE endpoint needs the sink"
    );
    assert!(
        wants_test_file_sink(["TEST_FILE"]),
        "a lone TEST_FILE endpoint needs the sink"
    );
    assert!(
        !wants_test_file_sink(["YT_RTMP", "FB", "VIMEO", "INSTAGRAM"]),
        "real platform endpoints never need the loopback sink"
    );
    assert!(
        !wants_test_file_sink(std::iter::empty::<&str>()),
        "an empty endpoint set needs no sink"
    );
    // Same exact-match rule as `rs_ffmpeg::ServiceType::from_str`: the sink
    // only serves what `build_rtmp_url` really routes to 127.0.0.1:1935.
    assert!(
        !wants_test_file_sink(["test_file", "TEST_FILE ", "TESTFILE"]),
        "near-miss spellings are not TEST_FILE"
    );
}

#[tokio::test]
async fn slot_refuses_a_non_loopback_or_invalid_bind_address() {
    for addr in ["0.0.0.0:0", "[::]:0", "not-an-address"] {
        let slot = TestFileSinkSlot::new(addr);
        slot.reconcile(async { true }).await;
        assert!(
            slot.status().await.is_none(),
            "the TEST_FILE sink must never bind {addr:?} -- it must not be reachable from outside the VPS"
        );
    }
}

#[tokio::test]
async fn slot_starts_one_loopback_sink_and_stops_it() {
    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    assert!(slot.status().await.is_none(), "a fresh slot runs no sink");

    slot.reconcile(async { true }).await;
    let first = slot
        .status()
        .await
        .expect("wanted=true must start the sink");
    assert!(
        first.local_addr.ip().is_loopback(),
        "bound to loopback, got {}",
        first.local_addr
    );
    assert_ne!(first.local_addr.port(), 0, "a real port was bound");
    assert_eq!(first.counters, SinkCountersSnapshot::default());

    // Idempotent: a second wanted=true keeps the SAME sink (no rebind).
    slot.reconcile(async { true }).await;
    let again = slot.status().await.expect("still running");
    assert_eq!(
        again.local_addr, first.local_addr,
        "must not rebind a running sink"
    );

    slot.reconcile(async { false }).await;
    // The port is free the moment reconcile returns (shutdown awaits the
    // accept task), not "eventually": rebind it synchronously, before any
    // await could let an aborted-but-not-yet-dropped listener go away.
    drop(
        std::net::TcpListener::bind(first.local_addr)
            .expect("reconcile(false) must release the port before it returns"),
    );
    assert!(
        slot.status().await.is_none(),
        "wanted=false must stop the sink"
    );

    // And it can come back after a stop.
    slot.reconcile(async { true }).await;
    assert!(
        slot.status().await.is_some(),
        "the sink restarts when wanted again"
    );
    slot.reconcile(async { false }).await;
}

#[tokio::test]
async fn sink_counts_connections_and_a_non_publisher_is_never_an_unpublish() {
    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    slot.reconcile(async { true }).await;
    let addr = slot.status().await.expect("sink running").local_addr;

    // A raw TCP client that never speaks RTMP.
    let client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("the sink must accept TCP on its loopback port");
    let c = wait_counters(&slot, "the connection to be accepted", |c| {
        c.connections_accepted == 1 && c.active_connections == 1
    })
    .await;
    assert_eq!(c.publishes, 0, "no publish happened");

    drop(client);
    let c = wait_counters(&slot, "the connection to close", |c| {
        c.active_connections == 0
    })
    .await;
    assert_eq!(c.connections_accepted, 1);
    assert_eq!(
        c.unpublishes, 0,
        "a connection that never published must not count as an unpublish"
    );
    assert_eq!(c.tags_received, 0);
    assert_eq!(c.bytes_received, 0);
    slot.reconcile(async { false }).await;
}

#[tokio::test]
async fn dropping_the_slot_releases_the_port() {
    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    slot.reconcile(async { true }).await;
    let addr = slot.status().await.expect("sink running").local_addr;

    // No explicit stop: dropping the owner (e.g. the AppState going away)
    // must still tear the listener down instead of leaking the accept task.
    drop(slot);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_err() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a dropped slot left a listener on {addr}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Two publishers on the SAME `live/<key>` at once are both accepted. The
/// delivery's warmup / outage-rescue pushers reuse the endpoint's key, so a
/// sink that rejected the second one (xiu `StreamsHub` answers `Exists`
/// until the first session times out) would turn a rescue entry into
/// refused pushes. This pins the per-connection responder design.
#[tokio::test]
async fn two_publishers_on_the_same_key_are_both_accepted() {
    use rs_rtmp_push::{PusherConfig, RtmpPusher};

    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    slot.reconcile(async { true }).await;
    let addr = slot.status().await.expect("sink running").local_addr;
    let url = format!("rtmp://{addr}/live/ci-fast");
    let clip = crate::rescue_default::DEFAULT_RESCUE_FLV;

    let mut first = RtmpPusher::new(url.clone(), PusherConfig::default());
    let mut second = RtmpPusher::new(url.clone(), PusherConfig::default());
    let (a, b) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(first.push_flv_bytes(clip), second.push_flv_bytes(clip))
    })
    .await
    .expect("both pushes must finish");
    assert!(a.is_ok(), "first publisher on {url} refused: {a:?}");
    assert!(
        b.is_ok(),
        "second publisher on the same {url} refused: {b:?}"
    );

    let c = wait_counters(&slot, "both publishes", |c| c.publishes == 2).await;
    // (No `active_connections == 2` check here: xiu drops a publisher idle
    // for 2 s, so on a slow runner one may already be gone.)
    assert_eq!(c.connections_accepted, 2, "{c:?}");
    assert!(c.bytes_received > 0, "{c:?}");

    first.close().await;
    second.close().await;
    let c = wait_counters(&slot, "both publishers to leave", |c| {
        c.active_connections == 0
    })
    .await;
    assert_eq!(c.unpublishes, 2, "{c:?}");
    slot.reconcile(async { false }).await;
}

/// Pins the documented sink characteristic: xiu's `ServerSession` hard-codes
/// a 2 s client read timeout, so a publisher that goes quiet (e.g. a fast
/// endpoint waiting out `FAST_KEEPALIVE_TRIGGER_SECS` before its freeze
/// frame) is DISCONNECTED -- where YouTube would hold the session -- and the
/// pusher's reconnect on the same key is accepted. If an xiu upgrade changes
/// this, this test flips and the playbook note must be updated with it.
#[tokio::test]
async fn an_idle_publisher_is_dropped_and_a_new_one_on_the_same_key_is_accepted() {
    use rs_rtmp_push::{PusherConfig, RtmpPusher};

    let slot = TestFileSinkSlot::new("127.0.0.1:0");
    slot.reconcile(async { true }).await;
    let addr = slot.status().await.expect("sink running").local_addr;
    let url = format!("rtmp://{addr}/live/ci-fast");
    let clip = crate::rescue_default::DEFAULT_RESCUE_FLV;

    let mut idle = RtmpPusher::new(url.clone(), PusherConfig::default());
    let pushed = tokio::time::timeout(Duration::from_secs(30), idle.push_flv_bytes(clip))
        .await
        .expect("push must finish");
    assert!(pushed.is_ok(), "first publisher refused: {pushed:?}");
    // Stay connected but send nothing.
    let c = wait_counters(&slot, "the silent publisher to be dropped", |c| {
        c.active_connections == 0
    })
    .await;
    assert_eq!(c.unpublishes, 1, "{c:?}");

    let mut fresh = RtmpPusher::new(url.clone(), PusherConfig::default());
    let pushed = tokio::time::timeout(Duration::from_secs(30), fresh.push_flv_bytes(clip))
        .await
        .expect("push must finish");
    assert!(
        pushed.is_ok(),
        "reconnect on the same key refused: {pushed:?}"
    );
    let c = wait_counters(&slot, "the second publish", |c| c.publishes == 2).await;
    assert_eq!(c.connections_accepted, 2, "{c:?}");
    fresh.close().await;
    drop(idle);
    slot.reconcile(async { false }).await;
}
