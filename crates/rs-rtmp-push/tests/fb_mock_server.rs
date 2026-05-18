//! Integration test: hand-rolled mock RTMP server validates the AMF
//! `NetConnection.connect` command that `rs-rtmp-push` sends.
//!
//! This is the integration-level regression guard for issue #215 (Facebook
//! Live "Invalid URL" publish rejection). Even if real-FB E2E coverage is
//! skipped or flaky, this test fails fast (<5 s) if anyone breaks the AMF
//! compliance the rust pusher must maintain:
//!
//!   1. `tcUrl` must NOT carry the default port suffix (`:1935/` for rtmp,
//!      `:443/` for rtmps) -- libobs/ffmpeg convention; FB strict.
//!   2. `swfUrl` must be present.
//!   3. `pageUrl` must be present.
//!
//! Strategy: the mock TCP server accepts the RTMP handshake (C0+C1+C2 ->
//! S0+S1+S2), reads the chunked `connect` command from the pusher, then
//! does a byte-string scan for the AMF0 UTF8 keys `tcUrl`, `swfUrl`,
//! `pageUrl` to extract their string values. No xiu AMF parser dependency
//! -- the byte-sniff approach is robust against AMF library API churn
//! (`Amf0Reader` / `ChunkUnpacketizer` types are not crate-public on the
//! integration-test boundary anyway).
//!
//! After the inspection is captured the mock simply drops the TCP
//! connection. The pusher's `push_flv_bytes` errors out with some
//! `PushError` variant -- the test doesn't care, it only asserts on
//! the captured inspection.

use std::io;
use std::time::Duration;

use rs_rtmp_push::{PusherConfig, RtmpPusher};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

// RTMP handshake constant (matches xiu rtmp::handshake::define::RTMP_HANDSHAKE_SIZE).
// Re-declared here to keep the test independent of xiu's private const re-exports.
const RTMP_HANDSHAKE_SIZE: usize = 1536;

/// What the mock captured from the pusher's NetConnection.connect command.
#[derive(Debug)]
struct ConnectionInspection {
    tc_url: Option<String>,
    swf_url: Option<String>,
    page_url: Option<String>,
    /// Set to Some(reason) if the mock rejected the connection (handshake
    /// failure, premature EOF, etc.) before AMF inspection completed.
    rejected: Option<String>,
}

/// Spawn the mock RTMP server on `127.0.0.1:0`. Returns the bound port and
/// a oneshot receiver that fires once with the inspection result.
async fn spawn_mock() -> io::Result<(u16, oneshot::Receiver<ConnectionInspection>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = oneshot::channel::<ConnectionInspection>();

    tokio::spawn(async move {
        // Accept exactly one connection then exit.
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.send(ConnectionInspection {
                    tc_url: None,
                    swf_url: None,
                    page_url: None,
                    rejected: Some(format!("accept failed: {e}")),
                });
                return;
            }
        };
        let inspection = run_mock(stream).await;
        let _ = tx.send(inspection);
    });

    Ok((port, rx))
}

/// Drive the mock-server side of the RTMP handshake + AMF capture.
async fn run_mock(mut stream: TcpStream) -> ConnectionInspection {
    // -- 1. Handshake: read C0+C1 (1 + 1536), write S0+S1+S2, read C2 (1536).
    let mut c0 = [0u8; 1];
    if let Err(e) = stream.read_exact(&mut c0).await {
        return reject(format!("read C0 failed: {e}"));
    }
    if c0[0] != 0x03 {
        return reject(format!("unexpected RTMP version byte: 0x{:02x}", c0[0]));
    }

    let mut c1 = vec![0u8; RTMP_HANDSHAKE_SIZE];
    if let Err(e) = stream.read_exact(&mut c1).await {
        return reject(format!("read C1 failed: {e}"));
    }

    // S0 = 0x03
    if let Err(e) = stream.write_all(&[0x03u8]).await {
        return reject(format!("write S0 failed: {e}"));
    }
    // S1 = 1536 bytes (timestamp + zero + random/zero). SimpleHandshakeClient
    // does not validate digest in simple-mode, so all-zero is accepted.
    let s1 = vec![0u8; RTMP_HANDSHAKE_SIZE];
    if let Err(e) = stream.write_all(&s1).await {
        return reject(format!("write S1 failed: {e}"));
    }
    // S2 = echo of C1.
    if let Err(e) = stream.write_all(&c1).await {
        return reject(format!("write S2 failed: {e}"));
    }
    if let Err(e) = stream.flush().await {
        return reject(format!("flush S0S1S2 failed: {e}"));
    }

    let mut c2 = vec![0u8; RTMP_HANDSHAKE_SIZE];
    if let Err(e) = stream.read_exact(&mut c2).await {
        return reject(format!("read C2 failed: {e}"));
    }

    // -- 2. Read incoming RTMP chunked data until the AMF0 `connect`
    //       command body has been transmitted. The pusher sends:
    //         a) SetChunkSize (msg_type_id=1, 4 bytes payload)
    //         b) AMF0 connect command (msg_type_id=20)
    //       Both are framed as RTMP chunks. We don't fully parse the chunk
    //       headers; instead we keep reading bytes from the socket into a
    //       buffer until we can locate the AMF0 string "connect" followed
    //       by the three keys tcUrl/swfUrl/pageUrl. A short bounded read
    //       loop with a per-read timeout ensures determinism.
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return ConnectionInspection {
                tc_url: scan_amf_string_value(&buf, "tcUrl"),
                swf_url: scan_amf_string_value(&buf, "swfUrl"),
                page_url: scan_amf_string_value(&buf, "pageUrl"),
                rejected: Some("AMF capture deadline expired".to_string()),
            };
        }

        // Try to extract all three values; if all present, stop reading.
        let tc = scan_amf_string_value(&buf, "tcUrl");
        let swf = scan_amf_string_value(&buf, "swfUrl");
        let page = scan_amf_string_value(&buf, "pageUrl");
        if tc.is_some() && swf.is_some() && page.is_some() {
            return ConnectionInspection {
                tc_url: tc,
                swf_url: swf,
                page_url: page,
                rejected: None,
            };
        }

        let mut chunk = [0u8; 1024];
        let read_fut = stream.read(&mut chunk);
        let timed = tokio::time::timeout(Duration::from_millis(500), read_fut).await;
        match timed {
            Ok(Ok(0)) => {
                // EOF -- pusher closed unexpectedly. Return whatever we
                // managed to capture; assertion will surface the gap.
                return ConnectionInspection {
                    tc_url: scan_amf_string_value(&buf, "tcUrl"),
                    swf_url: scan_amf_string_value(&buf, "swfUrl"),
                    page_url: scan_amf_string_value(&buf, "pageUrl"),
                    rejected: Some("pusher closed before connect completed".to_string()),
                };
            }
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(e)) => {
                return ConnectionInspection {
                    tc_url: scan_amf_string_value(&buf, "tcUrl"),
                    swf_url: scan_amf_string_value(&buf, "swfUrl"),
                    page_url: scan_amf_string_value(&buf, "pageUrl"),
                    rejected: Some(format!("read failed: {e}")),
                };
            }
            Err(_) => {
                // Short per-read timeout: loop and re-evaluate against the
                // bigger deadline.
                continue;
            }
        }
    }
}

fn reject(reason: String) -> ConnectionInspection {
    ConnectionInspection {
        tc_url: None,
        swf_url: None,
        page_url: None,
        rejected: Some(reason),
    }
}

/// Locate an AMF0 object-property whose key is `key` (e.g. `"tcUrl"`) inside
/// `buf` and return its UTF8 string value, or `None` if not found.
///
/// AMF0 object-property layout:
///   key:   2-byte BE length + raw UTF8 bytes (no type marker on keys)
///   value: 1-byte type marker (0x02 for UTF8String) +
///          2-byte BE length + raw UTF8 bytes
///
/// We search for the byte sequence (key-length-BE-u16 || key-bytes ||
/// 0x02 || value-length-BE-u16) and slice the value out. This is robust to
/// chunked transport because the entire NetConnection.connect AMF body is
/// well under one RTMP chunk (CHUNK_SIZE=4096) and arrives contiguously in
/// the test's read buffer.
fn scan_amf_string_value(buf: &[u8], key: &str) -> Option<String> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len();
    if key_len > u16::MAX as usize {
        return None;
    }
    let mut needle: Vec<u8> = Vec::with_capacity(2 + key_len + 1);
    needle.push(((key_len >> 8) & 0xff) as u8);
    needle.push((key_len & 0xff) as u8);
    needle.extend_from_slice(key_bytes);
    // 0x02 = AMF0 UTF8String type marker
    needle.push(0x02);

    // Linear search for the needle. The AMF body is small (<1 KB) so a
    // naive scan is fine.
    let mut i = 0usize;
    while i + needle.len() + 2 <= buf.len() {
        if buf[i..i + needle.len()] == needle[..] {
            let val_len_hi = buf[i + needle.len()] as usize;
            let val_len_lo = buf[i + needle.len() + 1] as usize;
            let val_len = (val_len_hi << 8) | val_len_lo;
            let val_start = i + needle.len() + 2;
            let val_end = val_start + val_len;
            if val_end <= buf.len() {
                if let Ok(s) = std::str::from_utf8(&buf[val_start..val_end]) {
                    return Some(s.to_string());
                }
            }
            // Needle matched but value extends past buffer (truncated chunk) or
            // UTF-8 decode failed — advance past this occurrence and keep
            // scanning. This makes the "robust to chunked transport" doc comment
            // accurate: a false-match mid-buffer does not abort the entire scan.
            i += 1;
            continue;
        }
        i += 1;
    }
    None
}

// -------------------------------------------------------------------------
// Test
// -------------------------------------------------------------------------

/// `RtmpPusher` must emit a CONNECT AMF whose `tcUrl` omits the default-port
/// suffix (none for the kernel-assigned 127.0.0.1 port -- port is non-default
/// so it DOES appear, just not `:1935/`), and whose `swfUrl` + `pageUrl`
/// are both present. These are the three FB-compliance rules from #215.
#[tokio::test(flavor = "multi_thread")]
async fn rust_pusher_sends_fb_compliant_connect_amf() {
    let (port, inspection_rx) = spawn_mock().await.expect("bind mock");

    // Give the listener a moment to be ready to accept().
    tokio::time::sleep(Duration::from_millis(20)).await;

    let url = format!("rtmp://127.0.0.1:{port}/rtmp/test-key");

    let mut pusher = RtmpPusher::new(url.clone(), PusherConfig::default());

    // Drive the pusher with an empty FLV body so it performs lazy connect
    // and stops there. The mock will drop the TCP connection after AMF
    // capture; the pusher will return some PushError -- that's fine, we
    // only assert on the inspection.
    let push_fut = pusher.push_flv_bytes(&[]);
    let _ = tokio::time::timeout(Duration::from_secs(5), push_fut).await;

    // Receive the inspection result (bounded wait so the test fails fast
    // if the mock hangs).
    let inspection = tokio::time::timeout(Duration::from_secs(2), inspection_rx)
        .await
        .expect("inspection oneshot did not fire within 2s")
        .expect("inspection oneshot sender was dropped");

    // Guard: the mock must NOT have rejected before completing AMF capture.
    // If it did, the field asserts below might still pass on partial data while
    // the integration test should actually fail (e.g. three fields seen but the
    // third triggers the reject path before all three are stored).
    assert!(
        inspection.rejected.is_none(),
        "mock rejected before capture: {:?}",
        inspection.rejected
    );

    // The mock must have completed at least the AMF capture before any
    // bail-out reason it might have recorded. We check captures FIRST so
    // a partial capture surfaces the missing field directly.
    let tc_url = inspection
        .tc_url
        .as_deref()
        .unwrap_or_else(|| panic!("tcUrl missing from connect AMF; mock={inspection:?}"));
    let swf_url = inspection
        .swf_url
        .as_deref()
        .unwrap_or_else(|| panic!("swfUrl missing from connect AMF; mock={inspection:?}"));
    let page_url = inspection
        .page_url
        .as_deref()
        .unwrap_or_else(|| panic!("pageUrl missing from connect AMF; mock={inspection:?}"));

    // Rule 1 (FB strict): tcUrl must not carry a default-port suffix.
    // Kernel-assigned port is never 1935 or 443, so this also implicitly
    // asserts the actual ephemeral port was included as expected by the
    // libobs/ffmpeg `tcUrl` convention.
    assert!(
        !tc_url.contains(":1935/"),
        "tcUrl must omit default rtmp port :1935 (#215); got {tc_url}"
    );
    assert!(
        !tc_url.contains(":443/"),
        "tcUrl must omit default rtmps port :443 (#215); got {tc_url}"
    );

    // Cross-check expected shape against the URL we dialed.
    let expected = format!("rtmp://127.0.0.1:{port}/rtmp");
    assert_eq!(tc_url, expected, "tcUrl shape mismatch (host/app/port)");

    // Rule 2 (FB strict): swfUrl present and non-empty.
    assert!(
        !swf_url.is_empty(),
        "swfUrl must be present and non-empty (#215)"
    );
    // Rule 3 (FB strict): pageUrl present and non-empty.
    assert!(
        !page_url.is_empty(),
        "pageUrl must be present and non-empty (#215)"
    );

    // Sanity: libobs convention mirrors swfUrl + pageUrl = tcUrl.
    assert_eq!(swf_url, tc_url, "swfUrl is expected to mirror tcUrl");
    assert_eq!(page_url, tc_url, "pageUrl is expected to mirror tcUrl");
}
