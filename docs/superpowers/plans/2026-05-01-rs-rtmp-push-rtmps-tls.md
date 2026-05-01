# rs-rtmp-push rtmps:// TLS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `rtmps://` TLS support to `rs-rtmp-push` so Facebook / Vimeo / Instagram endpoints can use the in-process Rust pusher.

**Architecture:** New `crates/rs-rtmp-push/src/tls.rs` defines `TlsIO` (parallel to bytesio's `TcpIO`) implementing the existing `TNetIO` trait. `Session::connect` detects the URL scheme and either constructs `TcpIO` (rtmp://, byte-identical to today) or wraps the connected `TcpStream` in `tokio_rustls::client::TlsStream` and constructs `TlsIO`. Production uses webpki-roots; tests inject a custom CA via a `tls::testing::set_tls_client_config_for_tests` override.

**Tech Stack:** rustls 0.23 (ring crypto), tokio-rustls 0.26, webpki-roots 0.26, rcgen 0.13 (test-only).

**Spec:** `docs/superpowers/specs/2026-05-01-rs-rtmp-push-rtmps-tls-design.md` (commit 5b4e79a).

**Issue:** #156. All implementation commits (Tasks 3+) reference `(#156)`.

---

## Context

PR 1 of #103 (rs-rtmp-push, merged today as PR #150) shipped a plain-TCP RTMP client. Facebook/Vimeo/Instagram require TLS. This PR is the prerequisite for PR 2 of #103 (flip FB-Zbynek to `pusher='rust'`).

**Branch state:** dev = main = v0.3.74 (PR #150 merged 2026-05-01, restreamer-v0.3.74 published). This PR bumps to 0.3.75.

**Repo path constraints:**
- Local checks: `cargo fmt --all --check` ONLY. NO `cargo build`, `cargo test`, `cargo clippy` locally.
- TDD: failing test commit BEFORE implementation commit.
- One commit per task, never batch.
- File size: each new `.rs` file MUST stay <1000 lines. `session.rs` is 952 today; expected ≈ 962 after this PR.
- Mutation testing: `rs-rtmp-push` MUST stay in the matrix (NOT in `--exclude-re`).
- Behavior preservation: `rtmp://` path is byte-identical. New code dead unless scheme == `rtmps`.

---

### Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml` (root workspace, line 25)
- Modify: `src-tauri/Cargo.toml` (line 3)
- Modify: `src-tauri/tauri.conf.json` (line 4)
- Modify: `leptos-ui/Cargo.toml` (line 3)

- [ ] **Step 1: Bump version 0.3.74 → 0.3.75 in all four files**

`Cargo.toml` line 25: `version = "0.3.74"` → `version = "0.3.75"`
`src-tauri/Cargo.toml` line 3: `version = "0.3.74"` → `version = "0.3.75"`
`src-tauri/tauri.conf.json` line 4: `"version": "0.3.74",` → `"version": "0.3.75",`
`leptos-ui/Cargo.toml` line 3: `version = "0.3.74"` → `version = "0.3.75"`

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.75"
```

---

### Task 2: Add deps + scaffold `tls.rs`

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]` block, after line 75 `sha2 = "0.10"`)
- Modify: `crates/rs-rtmp-push/Cargo.toml`
- Create: `crates/rs-rtmp-push/src/tls.rs`
- Modify: `crates/rs-rtmp-push/src/lib.rs`
- Modify: `crates/rs-rtmp-push/src/error.rs`

- [ ] **Step 1: Add workspace deps**

In `Cargo.toml`, add to `[workspace.dependencies]` (place just after the existing `rustls = { version = "0.23", features = ["ring"] }` line — line 38):

```toml
# TLS for rtmps:// pusher (#156)
tokio-rustls = "0.26"
webpki-roots = "0.26"
rcgen = "0.13"
```

(Note: `rustls` is already declared at workspace line 38 with `features = ["ring"]` — do not add it again.)

- [ ] **Step 2: Add crate-level deps**

In `crates/rs-rtmp-push/Cargo.toml`, replace the existing `[dependencies]` and `[dev-dependencies]` blocks with:

```toml
[dependencies]
rtmp = { workspace = true }
bytesio = "0.3"
xflv = "0.4"
tokio = { workspace = true, features = ["full"] }
thiserror = { workspace = true }
tracing = { workspace = true }
log = { workspace = true }
bytes = { workspace = true }
async-trait = "0.1"
futures = { workspace = true }
tokio-util = { workspace = true }
# rtmps:// TLS support (#156)
rustls = { workspace = true }
tokio-rustls = { workspace = true }
webpki-roots = { workspace = true }

[dev-dependencies]
streamhub = { workspace = true }
tokio = { workspace = true, features = ["full", "test-util"] }
sha2 = { workspace = true }
tracing-subscriber = { workspace = true }
rcgen = { workspace = true }
```

- [ ] **Step 3: Add `PushError::TlsHandshakeFailed` variant**

In `crates/rs-rtmp-push/src/error.rs`, add this variant inside the `pub enum PushError` block (after `LocalCancel,` on line 31, before the closing brace):

```rust
    #[error("TLS handshake failed: {0}")]
    TlsHandshakeFailed(String),
```

Also extend the `backoff_floor_ms` match (lines 45-54) to handle the new variant. After `PushError::HandshakeFailed(_) => Some(5_000),`, add:

```rust
        PushError::TlsHandshakeFailed(_) => Some(5_000),
```

And in the `is_exponential` exclusion list (lines 65-71), add `PushError::TlsHandshakeFailed(_)` to the `!matches!` non-exponential list. The updated body of `is_exponential` becomes:

```rust
pub fn is_exponential(err: &PushError) -> bool {
    !matches!(
        err,
        PushError::PublishRejected { code, .. } if code == "NetStream.Publish.BadName"
    ) && !matches!(
        err,
        PushError::Timeout
            | PushError::IoError(_)
            | PushError::HandshakeFailed(_)
            | PushError::TlsHandshakeFailed(_)
            | PushError::MalformedInput { .. }
            | PushError::LocalCancel
    )
}
```

Also add unit tests at the end of `error.rs::tests` mod, before the closing brace of the module:

```rust
    #[test]
    fn backoff_floor_tls_handshake_failed_is_5000() {
        let e = PushError::TlsHandshakeFailed("rustls: handshake error".into());
        assert_eq!(backoff_floor_ms(&e), Some(5_000));
    }

    #[test]
    fn is_exponential_tls_handshake_failed_is_false() {
        let e = PushError::TlsHandshakeFailed("rustls: handshake error".into());
        assert!(
            !is_exponential(&e),
            "TlsHandshakeFailed uses fixed floor, not exponential"
        );
    }
```

- [ ] **Step 4: Create `crates/rs-rtmp-push/src/tls.rs`**

Create the file with this exact content (~150 lines):

```rust
//! TLS-wrapped RTMP I/O for rtmps:// endpoints (Facebook / Vimeo / Instagram).
//!
//! Implements `bytesio::TNetIO` over `Framed<TlsStream<TcpStream>, BytesCodec>`
//! so the existing negotiate / read-loop / packetizer machinery in `session.rs`
//! is transparent to the wire encryption.
//!
//! Production code consumes `tls_client_config()` which is built once per
//! process from `webpki-roots`. Tests inject their own self-signed CA via
//! `testing::set_tls_client_config_for_tests` (called once per test binary).
//!
//! See `docs/superpowers/specs/2026-05-01-rs-rtmp-push-rtmps-tls-design.md`.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use bytesio::bytesio::{NetType, TNetIO};
use bytesio::bytesio_errors::{BytesIOError, BytesIOErrorValue};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_util::codec::{BytesCodec, Framed};

use crate::PushError;

type TlsClientStream = tokio_rustls::client::TlsStream<TcpStream>;

/// `TNetIO` implementation over a `tokio_rustls` client TLS stream.
/// Mirrors `bytesio::TcpIO` exactly except for the underlying transport.
pub struct TlsIO {
    stream: Framed<TlsClientStream, BytesCodec>,
}

impl TlsIO {
    pub fn new(stream: TlsClientStream) -> Self {
        Self {
            stream: Framed::new(stream, BytesCodec::new()),
        }
    }
}

#[async_trait]
impl TNetIO for TlsIO {
    fn get_net_type(&self) -> NetType {
        NetType::TCP
    }

    async fn write(&mut self, bytes: Bytes) -> Result<(), BytesIOError> {
        self.stream.send(bytes).await?;
        Ok(())
    }

    async fn read_timeout(&mut self, duration: Duration) -> Result<BytesMut, BytesIOError> {
        match tokio::time::timeout(duration, self.read()).await {
            Ok(d) => d,
            Err(err) => Err(BytesIOError {
                value: BytesIOErrorValue::TimeoutError(err),
            }),
        }
    }

    async fn read(&mut self) -> Result<BytesMut, BytesIOError> {
        match self.stream.next().await {
            Some(Ok(b)) => Ok(b),
            Some(Err(err)) => Err(BytesIOError {
                value: BytesIOErrorValue::IOError(err),
            }),
            None => Err(BytesIOError {
                value: BytesIOErrorValue::NoneReturn,
            }),
        }
    }
}

// -------------------------------------------------------------------------
// Client config (production = webpki-roots; tests = override)
// -------------------------------------------------------------------------

static TLS_CONFIG_OVERRIDE: OnceLock<Arc<ClientConfig>> = OnceLock::new();
static TLS_CONFIG_DEFAULT: OnceLock<Arc<ClientConfig>> = OnceLock::new();

/// Returns the rustls `ClientConfig` to use for outbound rtmps:// connections.
/// First call wins; subsequent calls return the cached `Arc`.
pub fn tls_client_config() -> Arc<ClientConfig> {
    if let Some(o) = TLS_CONFIG_OVERRIDE.get() {
        return o.clone();
    }
    TLS_CONFIG_DEFAULT.get_or_init(build_default_config).clone()
}

fn build_default_config() -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(cfg)
}

// -------------------------------------------------------------------------
// Connect helper
// -------------------------------------------------------------------------

/// Wrap an already-connected `TcpStream` in a TLS client stream, performing
/// the rustls handshake. Returns a `TlsIO` ready for the RTMP negotiate
/// sequence. SNI is taken from `host`.
pub async fn connect_tls(tcp: TcpStream, host: &str) -> Result<TlsIO, PushError> {
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|e| PushError::TlsHandshakeFailed(format!("invalid SNI '{host}': {e}")))?;
    let connector = TlsConnector::from(tls_client_config());
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| PushError::TlsHandshakeFailed(e.to_string()))?;
    Ok(TlsIO::new(tls))
}

// -------------------------------------------------------------------------
// Test override
// -------------------------------------------------------------------------

/// Test-only entry points. `#[doc(hidden)]` so it does not appear in public
/// rustdoc; production code never calls these.
#[doc(hidden)]
pub mod testing {
    use super::*;

    /// Install a custom rustls `ClientConfig` (e.g. a `RootCertStore` containing
    /// only a self-signed test CA). Must be called once per test binary BEFORE
    /// any rtmps:// connect attempt. Subsequent calls in the same process are
    /// silently ignored.
    pub fn set_tls_client_config_for_tests(cfg: Arc<ClientConfig>) {
        let _ = TLS_CONFIG_OVERRIDE.set(cfg);
    }
}
```

- [ ] **Step 5: Add `pub mod tls;` in `lib.rs`**

In `crates/rs-rtmp-push/src/lib.rs`, change line 9-13 from:

```rust
mod error;
mod flv;
mod pusher;
mod session;
mod state;
```

to:

```rust
mod error;
mod flv;
mod pusher;
mod session;
mod state;
pub mod tls;
```

- [ ] **Step 6: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/rs-rtmp-push/Cargo.toml crates/rs-rtmp-push/src/tls.rs crates/rs-rtmp-push/src/lib.rs crates/rs-rtmp-push/src/error.rs
git commit -m "feat(rtmp-push): scaffold tls.rs + add rustls deps (#156)"
```

---

### Task 3: TDD — failing rtmps loopback test

**Files:**
- Modify: `crates/rs-rtmp-push/tests/common/mod.rs` (add `spawn_recording_xiu_server_tls` helper at end)
- Create: `crates/rs-rtmp-push/tests/local_tls_loopback.rs`

- [ ] **Step 1: Add the TLS-bridge harness to `tests/common/mod.rs`**

At the end of `crates/rs-rtmp-push/tests/common/mod.rs` (after the existing `spawn_recording_xiu_server` and any helpers), append:

```rust
// ---------------------------------------------------------------------------
// TLS bridge harness (spec §11.2)
// ---------------------------------------------------------------------------

/// Spawn the existing recording xiu RTMP server on plain TCP, then bind a TLS
/// listener that bridges decrypted bytes through to it via
/// `tokio::io::copy_bidirectional`. xiu's `ServerSession::new` accepts
/// `TcpStream` concretely (cannot consume `TlsStream`), so the bridge is the
/// simplest way to validate rtmps:// without forking xiu.
///
/// Returns:
/// - `rtmps_url`     -- `rtmps://127.0.0.1:<tls_port>/live/test`
/// - `recorded`      -- shared accumulator filled by the underlying recording subscriber
/// - `ca_cert_der`   -- the self-signed CA cert; the test client must trust it
/// - `_server`       -- `JoinHandle` keeping the plain xiu server + hub alive
/// - `sub_ready_rx`  -- fires when the recording subscriber has its frame receiver
pub async fn spawn_recording_xiu_server_tls() -> (
    String,
    std::sync::Arc<tokio::sync::Mutex<Vec<RecordedTag>>>,
    rcgen::CertifiedKey,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    // 1. Spawn the plain xiu server we already have.
    let (plain_url, recorded, server_handle, sub_ready_rx) = spawn_recording_xiu_server().await;
    // plain_url = "rtmp://127.0.0.1:<plain_port>/live/test"
    let plain_authority = plain_url
        .strip_prefix("rtmp://")
        .and_then(|s| s.split('/').next())
        .expect("plain URL has authority");
    let plain_addr: std::net::SocketAddr = plain_authority
        .parse()
        .expect("plain authority parses as SocketAddr");

    // 2. Generate a self-signed cert valid for 127.0.0.1.
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
        .expect("rcgen self-signed cert");
    let cert_der: CertificateDer<'static> = certified.cert.der().clone();
    let key_pem: Vec<u8> = certified.key_pair.serialize_der();
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pem));

    // 3. Build the TLS acceptor.
    let server_cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("rustls server config");
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    // 4. Bind the TLS listener on an ephemeral port.
    let tls_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let tls_addr = tls_listener.local_addr().expect("tls local_addr");
    let rtmps_url = format!("rtmps://127.0.0.1:{}/live/test", tls_addr.port());

    // 5. Bridge task: TLS in, plain TCP out. One spawned task per connection.
    tokio::spawn(async move {
        loop {
            let (tcp, _) = match tls_listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let tls = match acceptor.accept(tcp).await {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("[bridge] tls accept error: {e}");
                        return;
                    }
                };
                let plain = match tokio::net::TcpStream::connect(plain_addr).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("[bridge] plain connect error: {e}");
                        return;
                    }
                };
                let (mut tls_r, mut tls_w) = tokio::io::split(tls);
                let (mut plain_r, mut plain_w) = tokio::io::split(plain);
                let _ = tokio::join!(
                    tokio::io::copy(&mut tls_r, &mut plain_w),
                    tokio::io::copy(&mut plain_r, &mut tls_w),
                );
            });
        }
    });

    (rtmps_url, recorded, certified, server_handle, sub_ready_rx)
}
```

Note: this helper depends on the existing `spawn_recording_xiu_server` and `RecordedTag` already declared at the top of `tests/common/mod.rs`. The new `rcgen` import is satisfied by Task 2's `[dev-dependencies]` addition. The `tokio_rustls` import is also from Task 2.

- [ ] **Step 2: Create `crates/rs-rtmp-push/tests/local_tls_loopback.rs`**

Create with this exact content:

```rust
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

use common::{spawn_recording_xiu_server_tls, RecordedTag};

use rs_rtmp_push::tls::testing::set_tls_client_config_for_tests;
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::pki_types::CertificateDer;

/// Build a `ClientConfig` whose trust anchor is exactly the test CA.
fn test_client_config(ca_der: CertificateDer<'static>) -> Arc<ClientConfig> {
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

    // Wait for the recording subscriber to be ready before pushing.
    sub_ready.await.expect("subscriber ready");

    // Build a tiny canned FLV: AAC sequence header + 1 audio media tag +
    // AVC sequence header + 1 video media tag.
    let canned = build_canned_flv();
    let canned_bodies_sha = sha256_flv_bodies(&canned);

    // Push via rtmps://. RtmpPusher::push_flv_bytes drives Session::connect
    // which (after Task 4) detects the rtmps:// scheme and uses TlsIO.
    let mut pusher = RtmpPusher::new(rtmps_url.clone(), PusherConfig::default());
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        pusher.push_flv_bytes(&canned),
    )
    .await
    .expect("push_flv_bytes did not return within 10s");
    result.expect("rtmps push must succeed");

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
    push_flv_tag(&mut buf, 9, 0, &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x42, 0xC0]);
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
```

- [ ] **Step 3: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-rtmp-push/tests/common/mod.rs crates/rs-rtmp-push/tests/local_tls_loopback.rs
git commit -m "test(rtmp-push): assert rtmps:// handshake against self-signed server (#156)"
```

This commit MUST compile cleanly. The test will FAIL on CI because:
1. `parse_rtmp_url` (in session.rs, line 763) still requires `rtmp://` prefix and returns `IoError("bad RTMP URL ...")` for `rtmps://`.
2. `RtmpPusher::push_flv_bytes` propagates that error.

Failure is the expected and required state for this commit.

---

### Task 4: Implement scheme branch + TlsIO + connect_tls

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs` (lines 87-175 connect, lines 763-852 parser + tests)

- [ ] **Step 1: Replace `parse_rtmp_url` (lines 763-804) with a scheme-aware version**

In `crates/rs-rtmp-push/src/session.rs`, replace lines 763-804 (the existing `fn parse_rtmp_url`) with:

```rust
/// URL scheme of the upstream RTMP endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scheme {
    Rtmp,
    Rtmps,
}

fn parse_rtmp_url(
    url: &str,
) -> Result<(Scheme, String, u16, String, String), PushError> {
    let (scheme, rest, default_port) = if let Some(r) = url.strip_prefix("rtmps://") {
        (Scheme::Rtmps, r, 443u16)
    } else if let Some(r) = url.strip_prefix("rtmp://") {
        (Scheme::Rtmp, r, 1935u16)
    } else {
        return Err(bad_url("must start with rtmp:// or rtmps://", url));
    };

    let slash = rest
        .find('/')
        .ok_or_else(|| bad_url("missing /app/stream path", url))?;

    let authority = &rest[..slash];
    let path = &rest[slash + 1..];

    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let h = &authority[..colon];
        let p: u16 = authority[colon + 1..]
            .parse()
            .map_err(|_| bad_url("invalid port number", url))?;
        (h.to_string(), p)
    } else {
        (authority.to_string(), default_port)
    };

    if host.is_empty() {
        return Err(bad_url("host is empty", url));
    }

    let slash2 = path
        .find('/')
        .ok_or_else(|| bad_url("path must contain /app/stream (two components)", url))?;

    let app = path[..slash2].to_string();
    let stream = path[slash2 + 1..].to_string();

    if app.is_empty() {
        return Err(bad_url("app name is empty", url));
    }
    if stream.is_empty() {
        return Err(bad_url("stream name is empty", url));
    }

    Ok((scheme, host, port, app, stream))
}
```

- [ ] **Step 2: Update existing parse_rtmp_url unit tests (lines 814-852 mod tests)**

In the `#[cfg(test)] mod tests` block of session.rs, the current tests destructure 4 elements. Replace the four parse-related tests with:

```rust
    #[test]
    fn parse_standard_rtmp_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://a.example.com/live/test").unwrap();
        assert_eq!(scheme, super::Scheme::Rtmp);
        assert_eq!(host, "a.example.com");
        assert_eq!(port, 1935);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn parse_rtmp_url_with_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://127.0.0.1:19350/live/mykey").unwrap();
        assert_eq!(scheme, super::Scheme::Rtmp);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19350);
        assert_eq!(app, "live");
        assert_eq!(stream, "mykey");
    }

    #[test]
    fn parse_standard_rtmps_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://live-api-s.facebook.com/rtmp/abc123").unwrap();
        assert_eq!(scheme, super::Scheme::Rtmps);
        assert_eq!(host, "live-api-s.facebook.com");
        assert_eq!(port, 443);
        assert_eq!(app, "rtmp");
        assert_eq!(stream, "abc123");
    }

    #[test]
    fn parse_rtmps_url_with_explicit_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://127.0.0.1:19443/live/test").unwrap();
        assert_eq!(scheme, super::Scheme::Rtmps);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19443);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn rejects_non_rtmp_scheme() {
        assert!(parse_rtmp_url("http://host/live/test").is_err());
    }

    #[test]
    fn rejects_missing_stream() {
        assert!(parse_rtmp_url("rtmp://host/live").is_err());
        assert!(parse_rtmp_url("rtmps://host/live").is_err());
    }
```

The mod-level `use super::{...parse_rtmp_url};` line (line 816) is unchanged.

- [ ] **Step 3: Update `Session::connect` (lines 87-175) to branch on scheme**

In `crates/rs-rtmp-push/src/session.rs`, the existing `Session::connect` body has two callsites that need updating.

**At line 89**, change:

```rust
        let (host, port, app, stream_name) = parse_rtmp_url(url)?;
```

to:

```rust
        let (scheme, host, port, app, stream_name) = parse_rtmp_url(url)?;
```

**At line 152**, change:

```rust
        // Wrap in TcpIO and share via Arc<Mutex<>>.
        let net_io: Box<dyn TNetIO + Send + Sync> = Box::new(TcpIO::new(tcp_stream));
        let io = Arc::new(Mutex::new(net_io));
```

to:

```rust
        // Wrap the connected stream in either TcpIO (rtmp://) or TlsIO (rtmps://).
        // The negotiate / read-loop machinery below operates on Box<dyn TNetIO>
        // and is therefore transparent to wire encryption.
        let net_io: Box<dyn TNetIO + Send + Sync> = match scheme {
            Scheme::Rtmp => Box::new(TcpIO::new(tcp_stream)),
            Scheme::Rtmps => Box::new(crate::tls::connect_tls(tcp_stream, &host).await?),
        };
        let io = Arc::new(Mutex::new(net_io));
```

- [ ] **Step 4: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "feat(rtmp-push): rtmps:// support via tokio-rustls TlsIO (#156)"
```

After this commit, on CI:
- The test `rtmps_handshake_completes_and_media_payload_byte_identical` from Task 3 PASSES.
- All existing `rtmp://` tests in `tests/local_xiu_loopback.rs` PASS unchanged (rtmp:// path is byte-identical).
- The mutation-testing job covers the new `crates/rs-rtmp-push/src/tls.rs` module since `rs-rtmp-push` is not in `--exclude-re`.

---

### Task 5: Push, monitor CI, create PR, post-deploy verify (orchestrator only)

**This task is performed by the orchestrator, NOT a subagent.**

- [ ] **Step 1: Run local checks**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor the dev CI run with a single background poll**

Per `~/devel/airuleset/modules/core/ci-monitoring.md` — ONE correct pattern:

```bash
gh run list --branch dev --limit 1 --json databaseId,status,conclusion
# capture the run id, then:
sleep 600 && gh run view <run-id> --json status,conclusion,jobs
```

If failures, `gh run view <run-id> --log-failed`, fix root cause, ONE more push, repeat. The relevant gates are:
- `Test (ubuntu-latest)` and `Test (windows-latest)` — must include `rtmps_handshake_completes_and_media_payload_byte_identical` passing.
- `Lint (fmt + clippy)` — clippy MUST pass on the new tls.rs.
- `Mutation Testing` — `rs-rtmp-push::tls` mutants must be killed by the new test (and by the `error.rs` unit tests added in Task 2).
- `Coverage` — must not regress from the v0.3.74 baseline.
- `E2E OBS-to-YouTube Test` — proves rtmp:// path is unchanged (the live YT_RTMP rust pusher endpoint).
- `E2E Streaming Test`, `Frontend E2E (Playwright)` — green.
- `Auto-release tag` — fires only on merge to main; not relevant for the dev push.

- [ ] **Step 4: Create PR from dev to main**

```bash
gh pr create --base main --head dev \
  --title "feat(rtmp-push): rtmps:// TLS support for FB/Vimeo/IG (#156)" \
  --body "$(cat <<'EOF'
## Summary
- Add `crates/rs-rtmp-push/src/tls.rs` with `TlsIO` (impl `bytesio::TNetIO`) and `connect_tls()` helper
- `Session::connect` detects `rtmp://` vs `rtmps://`; rtmps wraps the connected `TcpStream` in `tokio_rustls::client::TlsStream`
- Production trust anchor: webpki-roots Mozilla CA bundle (pure Rust, no OS dep)
- New `PushError::TlsHandshakeFailed(String)` variant + 5s backoff floor, non-exponential
- Test harness `spawn_recording_xiu_server_tls` bridges TLS → plain xiu RtmpServer via `copy_bidirectional` because xiu's `ServerSession` is not generic over the underlying stream
- Self-signed CA injected into the rustls `ClientConfig` via `rs_rtmp_push::tls::testing::set_tls_client_config_for_tests`
- New integration test `rtmps_handshake_completes_and_media_payload_byte_identical` proves byte-identical media payload across rtmps:// vs source FLV

## Behavior preservation
The `rtmp://` path is byte-identical to v0.3.74. The TLS branch is dead code unless `scheme == "rtmps"`. The only currently-live rust-pusher endpoint (YT_RTMP) is unaffected.

## Out of scope (deferred)
- Flipping FB / Vimeo / Instagram to `pusher='rust'` -- that is PR 2 of #103, gated on the 4-h soak in #159.
- mTLS, custom CA bundles, ALPN negotiation.

Closes #156.

## Test plan
- [x] `Test (ubuntu-latest)` and `Test (windows-latest)` green (includes new rtmps loopback test)
- [x] `Lint (fmt + clippy)` green
- [x] `Mutation Testing` -- no surviving mutants in `tls.rs`
- [x] `E2E OBS-to-YouTube Test` -- proves rtmp:// path unchanged
- [x] `E2E Streaming Test` -- full pipeline green
- [x] `Frontend E2E (Playwright)` -- dashboard renders v0.3.75
- [x] post-deploy verify on stream.lan via Playwright

EOF
)"
```

- [ ] **Step 5: Monitor PR CI run to clean + mergeable**

Same single-poll pattern. After all checks green, verify:

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<NUMBER> \
  --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Must show `mergeable: true` AND `mergeable_state: "clean"`. UNSTABLE/BLOCKED is NOT ready — investigate and fix.

- [ ] **Step 6: Post-deploy verify on stream.lan**

After CI's `Deploy to stream.lan` job succeeds, open the dashboard via Playwright:

```javascript
await mcp__plugin_playwright_playwright__browser_navigate("http://10.77.9.204:8910/");
await mcp__plugin_playwright_playwright__browser_snapshot();
// Read the version label from the header. Must show v0.3.75.
```

Confirm:
1. Version label reads `v0.3.75` (or `v0.3.75-...`).
2. Existing endpoints with `pusher='ffmpeg'` show `chunks_processed > 0`.
3. The single `pusher='rust'` endpoint (YT_RTMP) shows `chunks_processed > 0` (proves `rtmp://` path unchanged).
4. Browser console: zero errors.

- [ ] **Step 7: Send completion report**

Use the EXACT template from `~/devel/airuleset/modules/core/completion-report.md`. Must include:
- `✅ CI: green`
- `✅ /plan-check: 5/5 fulfilled`
- `✅ /review: clean — 0 🔴 0 🟡 0 🔵`
- `✅ Deploy: stream.lan dashboard shows v0.3.75 (matches backend)`
- Goal + What changed in plain language
- 🌐 Dev / Prod URLs
- Full PR link
- No "Remaining / Future" section. Do NOT mention #159 (4-h soak) or PR 2 in the report — those are tracked as separate issues.

---

### Verification

1. **Test gates green**: `rtmps_handshake_completes_and_media_payload_byte_identical` passes on Linux + Windows.
2. **Mutation testing**: no surviving mutants in `crates/rs-rtmp-push/src/tls.rs`.
3. **rtmp:// behavior preserved**: existing E2E (OBS → YT_RTMP) green.
4. **Dashboard**: v0.3.75 visible on stream.lan after deploy.
5. **PR mergeable + clean**.
