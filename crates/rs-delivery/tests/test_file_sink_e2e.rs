//! #192 — the TEST_FILE sink on the REAL production address `127.0.0.1:1935`.
//!
//! Under the Rust pusher a TEST_FILE endpoint (and its rescue loop) dials
//! `build_rtmp_url(TestFile, key)` = `rtmp://127.0.0.1:1935/live/<key>`. Before
//! #192 nothing on the delivery VPS listened there, so every push died with
//! "connection refused". These tests start the sink through its production
//! entry point (`TestFileSinkSlot::production()` + `reconcile`) and push with
//! the real `RtmpPusher` to the real URL.
//!
//! This file is its OWN test process on purpose. rs-delivery's bin unit tests
//! assume 1935 is REFUSED (e.g. `rescue_endpoint_loop_tests`), so a live
//! listener there must never share their process. Tests in THIS binary still
//! run in parallel threads, so each one holds `PRODUCTION_PORT` for its whole
//! body.

use std::net::SocketAddr;
use std::sync::LazyLock;
use std::time::Duration;

use rs_delivery::endpoint_rtmp_url::build_rtmp_url;
use rs_delivery::rescue_default::DEFAULT_RESCUE_FLV;
use rs_delivery::test_file_sink::{SinkCountersSnapshot, TEST_FILE_SINK_ADDR, TestFileSinkSlot};
use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use tokio::sync::Mutex;

/// Serializes the tests that bind the fixed production port.
static PRODUCTION_PORT: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const WAIT: Duration = Duration::from_secs(20);

/// One parsed FLV tag (type 8 = audio, 9 = video, 18 = script data).
struct FlvTag<'a> {
    tag_type: u8,
    timestamp_ms: u32,
    /// The raw tag bytes: 11-byte header + body + 4-byte PreviousTagSize.
    raw: &'a [u8],
    body: &'a [u8],
}

/// Split a well-formed FLV into its header (incl. PreviousTagSize0) and tags.
fn flv_split(flv: &[u8]) -> (&[u8], Vec<FlvTag<'_>>) {
    assert!(flv.starts_with(b"FLV"), "not an FLV");
    let data_offset = u32::from_be_bytes([flv[5], flv[6], flv[7], flv[8]]) as usize;
    let header_end = data_offset + 4;
    let mut tags = Vec::new();
    let mut pos = header_end;
    while pos + 11 <= flv.len() {
        let h = &flv[pos..pos + 11];
        let size = u32::from_be_bytes([0, h[1], h[2], h[3]]) as usize;
        let timestamp_ms = u32::from_be_bytes([h[7], h[4], h[5], h[6]]);
        let end = pos + 11 + size + 4;
        assert!(end <= flv.len(), "truncated FLV tag at byte {pos}");
        tags.push(FlvTag {
            tag_type: h[0] & 0x1f,
            timestamp_ms,
            raw: &flv[pos..end],
            body: &flv[pos + 11..pos + 11 + size],
        });
        pos = end;
    }
    (&flv[..header_end], tags)
}

/// The leading `max_ts_ms` of a real FLV: a live-chunk-sized slice that keeps
/// the sequence headers (they sit at timestamp 0).
fn flv_prefix(flv: &[u8], max_ts_ms: u32) -> Vec<u8> {
    let (header, tags) = flv_split(flv);
    let mut out = header.to_vec();
    for tag in tags.iter().filter(|t| t.timestamp_ms <= max_ts_ms) {
        out.extend_from_slice(tag.raw);
    }
    out
}

/// Bytes of the audio/video tag BODIES the sink must see for one push of
/// `flv`. Sequence headers are excluded: the pusher sends them once per
/// session, so a second push of the same clip legitimately skips them.
fn media_body_bytes(flv: &[u8]) -> u64 {
    let (_, tags) = flv_split(flv);
    tags.iter()
        .filter(|t| {
            let is_avc_seq = t.tag_type == 9 && t.body[0] & 0x0f == 7 && t.body[1] == 0;
            let is_aac_seq = t.tag_type == 8 && t.body[0] >> 4 == 10 && t.body[1] == 0;
            matches!(t.tag_type, 8 | 9) && !is_avc_seq && !is_aac_seq
        })
        .map(|t| t.body.len() as u64)
        .sum()
}

/// Poll the running sink's counters until `ok` holds. The wait is bounded.
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
            .unwrap_or_else(|| panic!("TEST_FILE sink is not running while waiting for {what}"))
            .counters;
        if ok(&c) {
            return c;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {WAIT:?} waiting for {what}; counters={c:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Push `flv` once; a refusal fails the test with the production symptom.
async fn push_ok(pusher: &mut RtmpPusher, url: &str, flv: &[u8]) {
    let pushed = tokio::time::timeout(WAIT, pusher.push_flv_bytes(flv))
        .await
        .unwrap_or_else(|_| panic!("push to {url} hung for {WAIT:?}"));
    if let Err(e) = pushed {
        panic!("TEST_FILE push to {url} was not accepted: {e} -- nothing listens there (#192)");
    }
}

/// (i) The Rust pusher's push to a TEST_FILE endpoint URL is accepted.
#[tokio::test]
async fn rust_pusher_to_a_test_file_endpoint_is_accepted_by_the_sink() {
    let _port = PRODUCTION_PORT.lock().await;
    let slot = TestFileSinkSlot::production();
    slot.reconcile(async { true }).await;

    // A ~1 s live-chunk-sized slice of a real H.264/AAC FLV.
    let chunk = flv_prefix(DEFAULT_RESCUE_FLV, 1_000);
    let want = media_body_bytes(&chunk);
    assert!(want > 0, "the 1 s slice carries media");

    let url = build_rtmp_url(ServiceType::TestFile, "ci-fast");
    let mut pusher = RtmpPusher::new(url.clone(), PusherConfig::default());
    push_ok(&mut pusher, &url, &chunk).await;

    let c = wait_counters(&slot, "the chunk's media to reach the sink", |c| {
        c.bytes_received >= want
    })
    .await;
    assert_eq!(c.connections_accepted, 1, "{c:?}");
    assert_eq!(c.publishes, 1, "the push published exactly once: {c:?}");
    assert!(c.tags_received > 0, "{c:?}");

    pusher.close().await;
    let c = wait_counters(&slot, "the publisher to disconnect", |c| {
        c.active_connections == 0
    })
    .await;
    assert_eq!(
        c.unpublishes, 1,
        "a publisher that went away counts as exactly one unpublish: {c:?}"
    );
    slot.reconcile(async { false }).await;
}

/// (ii) The sink binds loopback only -- exactly where the pusher dials.
#[tokio::test]
async fn the_sink_binds_loopback_only() {
    let addr: SocketAddr = TEST_FILE_SINK_ADDR
        .parse()
        .expect("TEST_FILE_SINK_ADDR is a socket address");
    assert!(addr.ip().is_loopback(), "{addr} must be a loopback address");
    assert_eq!(addr.port(), 1935);
    assert_eq!(
        build_rtmp_url(ServiceType::TestFile, "k"),
        format!("rtmp://{TEST_FILE_SINK_ADDR}/live/k"),
        "the TEST_FILE pusher must dial exactly where the sink listens"
    );

    let _port = PRODUCTION_PORT.lock().await;
    let slot = TestFileSinkSlot::production();
    slot.reconcile(async { true }).await;
    let status = slot
        .status()
        .await
        .expect("the production TEST_FILE sink must be running");
    assert_eq!(
        status.local_addr, addr,
        "bound to exactly {TEST_FILE_SINK_ADDR}, never a wildcard address"
    );

    // A wildcard bind address is refused, never bound. (Port 0, so a missing
    // loopback check would really bind -- not fail on the busy 1935.)
    let public = TestFileSinkSlot::new("0.0.0.0:0");
    public.reconcile(async { true }).await;
    assert!(
        public.status().await.is_none(),
        "the sink must refuse a non-loopback bind address"
    );

    // Stopping releases the port the moment reconcile returns: rebind it
    // synchronously (no await in between), then a fresh connect is refused.
    slot.reconcile(async { false }).await;
    drop(
        std::net::TcpListener::bind(addr)
            .expect("reconcile(false) must release 127.0.0.1:1935 before it returns"),
    );
    assert!(slot.status().await.is_none());
    let reconnect = tokio::time::timeout(WAIT, tokio::net::TcpStream::connect(addr))
        .await
        .expect("a connect to a closed port must not hang");
    assert!(
        reconnect.is_err(),
        "after the sink stops nothing may listen on {addr}"
    );
}

/// (iv) A rescue-clip push to a TEST_FILE endpoint succeeds. The rescue loop
/// pushes the clip back-to-back on ONE session, so do the same.
#[tokio::test]
async fn rescue_clip_push_to_a_test_file_endpoint_succeeds() {
    let _port = PRODUCTION_PORT.lock().await;
    let slot = TestFileSinkSlot::production();
    slot.reconcile(async { true }).await;

    let url = build_rtmp_url(ServiceType::TestFile, "ci-fast");
    let mut pusher = RtmpPusher::new(url.clone(), PusherConfig::default());
    push_ok(&mut pusher, &url, DEFAULT_RESCUE_FLV).await;
    push_ok(&mut pusher, &url, DEFAULT_RESCUE_FLV).await;

    let want = 2 * media_body_bytes(DEFAULT_RESCUE_FLV);
    let c = wait_counters(&slot, "both rescue-clip pushes to reach the sink", |c| {
        c.bytes_received >= want
    })
    .await;
    assert_eq!(
        c.publishes, 1,
        "both clip pushes ride ONE publish session, like the rescue loop: {c:?}"
    );
    assert_eq!(c.connections_accepted, 1, "{c:?}");

    pusher.close().await;
    wait_counters(&slot, "the publisher to disconnect", |c| {
        c.active_connections == 0
    })
    .await;
    slot.reconcile(async { false }).await;
}
