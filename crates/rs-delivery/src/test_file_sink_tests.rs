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
