//! Integration test: rtmps:// pusher against a self-signed TLS RTMP server.
//!
//! Bridge harness (`tests/common/spawn_recording_xiu_server_tls`) accepts TLS
//! on an ephemeral 127.0.0.1 port, decrypts, and `copy_bidirectional` to a
//! plain xiu RtmpServer. The test client trusts the same self-signed CA via
//! `rs_rtmp_push::tls::testing::set_tls_client_config_for_tests`.
//!
//! See spec `docs/superpowers/specs/2026-05-01-rs-rtmp-push-rtmps-tls-design.md`.

#[path = "common/mod.rs"]
mod common;

use common::{RecordedTag, spawn_recording_xiu_server_tls};

use rs_rtmp_push::tls::testing::set_tls_client_config_for_tests;
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::pki_types::CertificateDer;

/// Build a `ClientConfig` whose trust anchor is exactly the test CA.
/// Installs rustls's `ring` crypto provider idempotently so `ClientConfig::builder()`
/// does not panic on a missing process-level provider.
fn test_client_config(ca_der: CertificateDer<'static>) -> Arc<ClientConfig> {
    rs_rtmp_push::tls::testing::ensure_default_crypto_provider();
    let mut roots = RootCertStore::empty();
    roots.add(ca_der).expect("add test CA");
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

#[tokio::test]
async fn rtmps_handshake_completes_and_media_payload_byte_identical() {
    let _ = tracing_subscriber::fmt::try_init();

    let (rtmps_url, recorded, certified, _server, sub_ready) =
        spawn_recording_xiu_server_tls().await;

    // Trust the self-signed CA in this process.
    let cert_der: CertificateDer<'static> = certified.cert.der().clone();
    set_tls_client_config_for_tests(test_client_config(cert_der));

    let mut pusher = RtmpPusher::new(rtmps_url.clone(), PusherConfig::default());

    // Step 1: empty push to drive handshake + publish. Mirrors the plaintext
    // `media_payload_byte_identical_to_source` test ordering. Without this,
    // the subscriber never sees `BroadcastEvent::Publish` and `sub_ready` is
    // never signalled (deadlock).
    tokio::time::timeout(Duration::from_secs(10), pusher.push_flv_bytes(&[]))
        .await
        .expect("rtmps handshake did not return within 10s")
        .expect("rtmps handshake must succeed");

    // Step 2: now the publish has reached the hub; wait for the recording
    // subscriber to register before pushing media.
    tokio::time::timeout(Duration::from_secs(5), sub_ready)
        .await
        .expect("subscriber task did not signal ready within 5s")
        .expect("sub_ready channel dropped before signal");

    // Step 3: push the real canned FLV.
    let canned = build_canned_flv();
    let canned_bodies_sha = sha256_flv_bodies(&canned);

    tokio::time::timeout(Duration::from_secs(10), pusher.push_flv_bytes(&canned))
        .await
        .expect("rtmps push did not return within 10s")
        .expect("rtmps push must succeed");

    // Drain a moment so the recording subscriber finishes accumulating.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let recorded_lock = recorded.lock().await;
    let recorded_bodies_sha = sha256_recorded_bodies(&recorded_lock);

    assert!(
        !recorded_lock.is_empty(),
        "expected at least one media tag captured by the recording subscriber"
    );
    assert_eq!(
        recorded_bodies_sha, canned_bodies_sha,
        "byte-identical body SHA-256 over rtmps:// must match plaintext FLV source"
    );
}

// ---------------------------------------------------------------------------
// FLV helpers (kept local to this test file -- if more rtmps tests grow they
// can move to tests/common/mod.rs)
// ---------------------------------------------------------------------------

/// Build a small canned FLV stream with: header + AAC seq hdr + 1 audio tag +
/// AVC seq hdr + 1 video tag. Matches the structure of the existing
/// plaintext byte-identical test.
fn build_canned_flv() -> Vec<u8> {
    // FLV file header (9 bytes): "FLV" + 0x01 (ver) + 0x05 (audio+video) + 0x00000009 (header len)
    let mut buf = vec![0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];
    // PreviousTagSize0 (4 bytes)
    buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

    // AAC sequence header tag: tag_type=8, body=[0xAF, 0x00, 0x12, 0x10] (4 bytes)
    push_flv_tag(&mut buf, 8, 0, &[0xAF, 0x00, 0x12, 0x10]);
    // 1 audio media tag: tag_type=8, body=[0xAF, 0x01, 0xDE, 0xAD, 0xBE, 0xEF]
    push_flv_tag(&mut buf, 8, 23, &[0xAF, 0x01, 0xDE, 0xAD, 0xBE, 0xEF]);

    // AVC sequence header tag: tag_type=9, body=[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x42, 0xC0]
    push_flv_tag(
        &mut buf,
        9,
        0,
        &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x42, 0xC0],
    );
    // 1 video media tag: tag_type=9, body=[0x27, 0x01, 0x00, 0x00, 0x00, 0xCA, 0xFE, 0xBA, 0xBE]
    push_flv_tag(
        &mut buf,
        9,
        40,
        &[0x27, 0x01, 0x00, 0x00, 0x00, 0xCA, 0xFE, 0xBA, 0xBE],
    );

    buf
}

fn push_flv_tag(buf: &mut Vec<u8>, tag_type: u8, ts_ms: u32, body: &[u8]) {
    let body_len = body.len() as u32;
    // Tag header (11 bytes): type, body size (3), timestamp (3), timestamp_ext (1), stream_id (3)
    buf.push(tag_type);
    buf.push(((body_len >> 16) & 0xFF) as u8);
    buf.push(((body_len >> 8) & 0xFF) as u8);
    buf.push((body_len & 0xFF) as u8);
    buf.push(((ts_ms >> 16) & 0xFF) as u8);
    buf.push(((ts_ms >> 8) & 0xFF) as u8);
    buf.push((ts_ms & 0xFF) as u8);
    buf.push(((ts_ms >> 24) & 0xFF) as u8); // ts ext
    buf.extend_from_slice(&[0x00, 0x00, 0x00]); // stream id
    buf.extend_from_slice(body);
    // PreviousTagSize (4 bytes) = 11 + body_len
    let prev = 11u32 + body_len;
    buf.extend_from_slice(&prev.to_be_bytes());
}

fn sha256_flv_bodies(flv: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut i: usize = 9 + 4; // skip FLV header + PreviousTagSize0
    while i + 11 <= flv.len() {
        let tag_type = flv[i];
        let body_len =
            ((flv[i + 1] as usize) << 16) | ((flv[i + 2] as usize) << 8) | (flv[i + 3] as usize);
        let body_start = i + 11;
        let body_end = body_start + body_len;
        if body_end + 4 > flv.len() {
            break;
        }
        if tag_type == 8 || tag_type == 9 {
            hasher.update(&flv[body_start..body_end]);
        }
        i = body_end + 4;
    }
    hasher.finalize().into()
}

fn sha256_recorded_bodies(recorded: &[RecordedTag]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for t in recorded {
        if t.tag_type == 8 || t.tag_type == 9 {
            hasher.update(&t.body);
        }
    }
    hasher.finalize().into()
}
