# Pure-Rust RTMP Push Implementation Plan (PR 1 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the `rs-rtmp-push` crate, the `PusherKind` config flag, and the `endpoint_task` branch — all behind `#[default] PusherKind::Ffmpeg` so existing endpoints have **zero behavior change**.

**Architecture:** New crate exposes `RtmpPusher::push_flv_bytes(&mut self, &[u8])` which lazily opens a TCP+RTMP connection via xiu `ClientSession` (Push mode) on the first call and rewrites every FLV tag's timestamp to `last_output_ts_ms + (tag.ts - chunk_first_ts)` so the output stream is monotonic across reconnects. Caller (`endpoint_task`) drives backoff, exactly as it drives ffmpeg restarts today.

**Tech Stack:** Rust 2024, xiu `rtmp = "0.6"` (already a dep), `streamhub = "0.2"`, hand-rolled FLV tag iterator (FLV spec is stable, ~80 LOC, avoids xflv coupling), tokio async, `thiserror` for error enum, `tracing` (with `log` bridge for xiu's log calls).

**Spec:** `docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md` (commit b8836a3).

**Scope:** PR 1 only. PRs 2-4 (config flips + agent-driven soak + ffmpeg deletion) are operational follow-ups documented in spec §6 — they are NOT in this plan.

**Issue:** [#103](https://github.com/zbynekdrlik/restreamer/issues/103). Already open since 2026-04-11. Tasks 3+ reference `#103` in commit messages.

---

### Task 1: Version bump 0.3.73 → 0.3.74

**Files:**
- Modify: `Cargo.toml` (workspace `version` field, line ~24)
- Modify: `src-tauri/Cargo.toml` (`version` field)
- Modify: `src-tauri/tauri.conf.json` (`version` field)
- Modify: `leptos-ui/Cargo.toml` (`version` field)
- Modify: `Cargo.lock` (sync via `cargo update -p restreamer` — orchestrator handles this; subagent does NOT compile)

- [ ] **Step 1: Bump version in `Cargo.toml`**

Find the line `version = "0.3.73"` in the workspace package section and change to:
```toml
version = "0.3.74"
```

- [ ] **Step 2: Bump version in `src-tauri/Cargo.toml`**

Find the line `version = "0.3.73"` and change to:
```toml
version = "0.3.74"
```

- [ ] **Step 3: Bump version in `src-tauri/tauri.conf.json`**

Find the JSON field `"version": "0.3.73"` and change to:
```json
"version": "0.3.74"
```

- [ ] **Step 4: Bump version in `leptos-ui/Cargo.toml`**

Find the line `version = "0.3.73"` and change to:
```toml
version = "0.3.74"
```

- [ ] **Step 5: Sync `Cargo.lock`**

The subagent does NOT run cargo commands locally. Instead, the subagent updates the lockfile by editing the literal version string everywhere it appears for our packages. Run:
```bash
sed -i 's/^version = "0.3.73"$/version = "0.3.74"/' Cargo.lock
```
(Cargo.lock has many `version = "X"` lines; this updates exactly the lines whose value is currently `0.3.73` — only our own packages match.)

Verify with:
```bash
grep -c '^version = "0.3.74"$' Cargo.lock
```
Expected: count matches the number of workspace packages (currently `restreamer`, `rs-api`, `rs-cloud`, `rs-core`, `rs-delivery`, `rs-endpoint`, `rs-ffmpeg`, `rs-inpoint`, `rs-runtime`, `rs-service`, `rs-youtube`, `rs-ts-normalize` plus the new `rs-rtmp-push` once added in Task 2 — but at this point Task 2 hasn't run yet, so 12 lines).

- [ ] **Step 6: Verify formatting**

```bash
cargo fmt --all --check
```
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.74"
```

---

### Task 2: Scaffold `rs-rtmp-push` crate

**Files:**
- Create: `crates/rs-rtmp-push/Cargo.toml`
- Create: `crates/rs-rtmp-push/src/lib.rs`
- Create: `crates/rs-rtmp-push/src/error.rs`
- Create: `crates/rs-rtmp-push/src/state.rs`
- Modify: `Cargo.toml` (workspace `members` list)

- [ ] **Step 1: Create `crates/rs-rtmp-push/Cargo.toml`**

```toml
[package]
name = "rs-rtmp-push"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
rtmp = { workspace = true }
streamhub = { workspace = true }
tokio = { workspace = true, features = ["full"] }
thiserror = { workspace = true }
tracing = { workspace = true }
log = { workspace = true }
bytes = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["full", "test-util"] }
sha2 = "0.10"
tracing-subscriber = { workspace = true }
```

(If any of those workspace fields don't exist yet, the subagent does NOT add them to the workspace `[workspace.dependencies]` — they are already there from existing crates. If `bytes` or `sha2` is missing from the workspace, the subagent adds the version directly to this crate's `[dependencies]` table — `bytes = "1"`, `sha2 = "0.10"`. Do not modify the workspace's dep table from inside this crate's `Cargo.toml`.)

- [ ] **Step 2: Add the crate to workspace `members`**

Open `Cargo.toml` (root). Find the `[workspace] members = [...]` array. Add `"crates/rs-rtmp-push"` alphabetically (between `rs-inpoint` and `rs-runtime`):
```toml
members = [
    "crates/rs-api",
    "crates/rs-cloud",
    "crates/rs-core",
    "crates/rs-delivery",
    "crates/rs-endpoint",
    "crates/rs-ffmpeg",
    "crates/rs-inpoint",
    "crates/rs-rtmp-push",
    "crates/rs-runtime",
    "crates/rs-service",
    "crates/rs-ts-normalize",
    "crates/rs-youtube",
]
```
(Subagent: open the existing file and verify the order matches; if existing list isn't alphabetical, just insert in the same style as the existing list.)

- [ ] **Step 3: Create `crates/rs-rtmp-push/src/error.rs`**

```rust
//! Error types surfaced by `RtmpPusher`. See spec §4.1 + §5.3.

use std::io;
use thiserror::Error;

/// One-of error returned by `RtmpPusher::push_flv_bytes`.
///
/// `code` and `description` on the `*Rejected` variants are the upstream-provided
/// AMF onStatus payload (e.g. `code: "NetStream.Publish.BadName"`).
#[derive(Debug, Error)]
pub enum PushError {
    #[error("RTMP handshake failed: {0}")]
    HandshakeFailed(#[source] io::Error),

    #[error("NetConnection.Connect rejected: {code} - {description}")]
    ConnectRejected { code: String, description: String },

    #[error("NetStream.Publish rejected: {code} - {description}")]
    PublishRejected { code: String, description: String },

    #[error("upstream closed connection mid-stream: {0}")]
    RemoteClosed(#[source] io::Error),

    #[error("operation timed out")]
    Timeout,

    #[error("I/O error: {0}")]
    IoError(#[source] io::Error),

    #[error("local cancel")]
    LocalCancel,

    #[error("malformed FLV input at offset {offset}: {reason}")]
    MalformedInput { offset: usize, reason: String },
}

/// Backoff floor in milliseconds for a given error variant. Mirrors today's
/// `crates/rs-delivery/src/ffmpeg_reason.rs::reconnect_floor` semantics.
///
/// The endpoint task multiplies this by `2^consecutive_errors` and caps at 300_000
/// (5 min). `PublishRejected { code: "NetStream.Publish.BadName" }` is a fixed
/// 30s floor — fast retry is pointless and exponential escalation drowns the
/// signal. `LocalCancel` returns `None` (no retry).
pub fn backoff_floor_ms(err: &PushError) -> Option<u64> {
    match err {
        PushError::HandshakeFailed(_) => Some(5_000),
        PushError::ConnectRejected { .. } => Some(30_000),
        PushError::PublishRejected { code, .. } if code == "NetStream.Publish.BadName" => Some(30_000),
        PushError::PublishRejected { .. } => Some(30_000),
        PushError::RemoteClosed(_) => Some(30_000),
        PushError::Timeout => Some(10_000),
        PushError::IoError(_) => Some(15_000),
        PushError::MalformedInput { .. } => Some(15_000),
        PushError::LocalCancel => None,
    }
}

/// Whether to escalate the floor exponentially on consecutive same-class
/// errors. `BadName` is fixed (operator must rotate the key); the rest follow
/// today's exponential ×2 cap-at-300s policy.
pub fn is_exponential(err: &PushError) -> bool {
    !matches!(
        err,
        PushError::PublishRejected { code, .. } if code == "NetStream.Publish.BadName"
    ) && !matches!(
        err,
        PushError::Timeout | PushError::IoError(_) | PushError::HandshakeFailed(_) | PushError::MalformedInput { .. } | PushError::LocalCancel
    )
}
```

- [ ] **Step 4: Create `crates/rs-rtmp-push/src/state.rs`**

```rust
//! Pusher transport state. See spec §5.1.

/// Per-`RtmpPusher` runtime state. Owns connection-lifetime data (TCP session +
/// monotonic output timestamp + reconnect counter). Retry-policy state
/// (`consecutive_errors`, `last_error_class`) lives in the *caller*
/// (`endpoint_task`) — same boundary as today's split between `FfmpegProcess`
/// and `EndpointRestartState`.
#[derive(Default)]
pub struct PusherState {
    /// Output timestamp in ms, monotonic across reconnects. Never resets.
    pub last_output_ts_ms: u64,
    /// Total reconnects since the pusher was created. Surfaced as the
    /// dashboard `reconnect_count` metric (replaces `ffmpeg_restart_count`).
    pub reconnect_count: u32,
    /// `true` while a TCP+RTMP session is open and bytes can flow. `false`
    /// after `connect()` failed or after a mid-stream error dropped the
    /// session. Lazy reconnect on next `push_flv_bytes`.
    pub connected: bool,
}

#[derive(Clone)]
pub struct PusherConfig {
    /// Per-call socket-write timeout in ms. Default 30_000 (matches today's
    /// `crates/rs-delivery/src/endpoint_task.rs::WRITE_TIMEOUT_SECS`).
    pub timeout_ms: u64,
}

impl Default for PusherConfig {
    fn default() -> Self {
        Self { timeout_ms: 30_000 }
    }
}
```

- [ ] **Step 5: Create `crates/rs-rtmp-push/src/lib.rs`**

```rust
//! In-process RTMP push client backed by xiu `ClientSession` (Push mode).
//!
//! Replaces the ffmpeg subprocess that today pipes FLV chunks to YouTube/FB.
//! See `docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md` for
//! the full design.

#![forbid(unsafe_code)]

mod error;
mod flv;
mod pusher;
mod session;
mod state;

pub use error::{PushError, backoff_floor_ms, is_exponential};
pub use pusher::RtmpPusher;
pub use state::{PusherConfig, PusherState};
```

(Modules `flv`, `pusher`, `session` are added in later tasks. The subagent creates empty stubs now so this compiles:

`crates/rs-rtmp-push/src/flv.rs`:
```rust
//! Hand-rolled FLV tag iterator. Filled in Task 6.
```

`crates/rs-rtmp-push/src/session.rs`:
```rust
//! xiu ClientSession adapter. Filled in Task 4.
```

`crates/rs-rtmp-push/src/pusher.rs`:
```rust
//! `RtmpPusher` — public API. Filled in Tasks 4, 6, 8, 10.

use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    #[allow(dead_code)]
    config: PusherConfig,
    state: PusherState,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
        }
    }

    pub fn last_output_ts_ms(&self) -> u64 {
        self.state.last_output_ts_ms
    }

    pub fn reconnect_count(&self) -> u32 {
        self.state.reconnect_count
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Lazy-connect + write FLV bytes. Filled in Tasks 4 + 6 + 8 + 10.
    /// Stub returns `LocalCancel` so the type signature is exercisable but no
    /// behavior is implemented.
    pub async fn push_flv_bytes(&mut self, _bytes: &[u8]) -> Result<(), PushError> {
        Err(PushError::LocalCancel)
    }

    pub async fn close(&mut self) {
        self.state.connected = false;
    }
}
```)

- [ ] **Step 6: Verify formatting**

```bash
cargo fmt --all --check
```
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/rs-rtmp-push/
git commit -m "feat(rtmp-push): scaffold crate with PushError enum (#103)"
```

---

### Task 3: TDD failing test — handshake completes against local xiu server

**Files:**
- Create: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs`

- [ ] **Step 1: Write the failing test**

```rust
//! Integration tests against a locally-spun xiu `RtmpServer`.
//!
//! Each test starts a fresh server bound to `127.0.0.1:0` (ephemeral port),
//! creates an `RtmpPusher` pointed at it, exercises the API, and asserts
//! against either the wire-captured tags or the server's session state.

use std::time::Duration;

use rs_rtmp_push::{PushError, PusherConfig, RtmpPusher};
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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    let handle = tokio::spawn(async move {
        // The implementer fills this in in Task 4 by following xiu's
        // crates/rs-inpoint/src/rtmp_server.rs as a reference for hub +
        // RtmpServer setup. For now the listener simply accepts and drops
        // the connection (which causes the pusher's handshake to fail).
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
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        pusher.push_flv_bytes(&[]),
    )
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
```

- [ ] **Step 2: Confirm the test would fail (do NOT compile locally)**

The subagent does not run `cargo test`. Instead, it confirms the test logic:
- The current `push_flv_bytes` stub returns `PushError::LocalCancel` (Task 2 step 5). The assertion `result.is_ok()` will FAIL with that stub.
- Additionally, the bare-listener server (Task 3 step 1's `spawn_xiu_server`) will not complete the RTMP handshake, so even after Task 4 the test will only pass with a real xiu server.

The next task (Task 4) replaces both: the stub becomes a real implementation, and `spawn_xiu_server` becomes a real xiu RtmpServer that completes the handshake.

- [ ] **Step 3: Commit (failing test only)**

```bash
git add crates/rs-rtmp-push/tests/local_xiu_loopback.rs
git commit -m "test(rtmp-push): assert handshake completes against local xiu server (#103)"
```

---

### Task 4: Implement handshake + connect + publish

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs` (replace stub)
- Modify: `crates/rs-rtmp-push/src/pusher.rs` (replace stub)
- Modify: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs` (replace `spawn_xiu_server` body with a real xiu server)

**Reference for xiu API:** Read `crates/rs-inpoint/src/rtmp_server.rs` to see how `StreamsHub` + `RtmpServer` are wired on the input side. The server side of the test harness in Task 4 step 3 mirrors that pattern. For the *client* side (the pusher itself), read xiu's source at `~/.cargo/registry/src/index.crates.io-*/rtmp-0.6.5/src/session/client_session.rs` and `~/.cargo/registry/src/index.crates.io-*/rtmp-0.6.5/src/relay/push_client.rs` for the public API.

- [ ] **Step 1: Implement `crates/rs-rtmp-push/src/session.rs`**

The `Session` wrapper owns the xiu `ClientSession` plus the inbound TCP stream. It exposes:
- `Session::connect(url) -> Result<Session, PushError>` — opens TCP, runs handshake, sends NetConnection.connect, sends createStream, sends publish. Returns the live session.
- `Session::send_audio_tag(timestamp_ms, body) -> Result<(), PushError>`
- `Session::send_video_tag(timestamp_ms, body) -> Result<(), PushError>`
- `Session::close(self)` — drops the TCP socket cleanly.

```rust
//! xiu ClientSession adapter — opens TCP, runs RTMP handshake/connect/
//! publish, then accepts media tags from the pusher.

use std::time::Duration;

use rtmp::session::client_session::{ClientSession, ClientSessionType};
use streamhub::{StreamsHub, define::StreamHubEventSender};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::PushError;

/// Parsed `rtmp://host:port/app/stream` URL.
struct ParsedUrl {
    host: String,
    port: u16,
    app: String,
    stream: String,
}

fn parse_rtmp_url(url: &str) -> Result<ParsedUrl, PushError> {
    // Format: rtmp[s]://host[:port]/app[/stream...]
    // For YT: rtmp://a.rtmp.youtube.com/live2/STREAM_KEY
    // For FB: rtmps://live-api-s.facebook.com:443/rtmp/STREAM_KEY
    let stripped = url
        .strip_prefix("rtmp://")
        .or_else(|| url.strip_prefix("rtmps://"))
        .ok_or_else(|| PushError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("URL missing rtmp:// or rtmps:// prefix: {url}"),
        )))?;

    let mut parts = stripped.splitn(2, '/');
    let authority = parts.next().ok_or_else(|| PushError::IoError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "URL missing authority",
    )))?;
    let path = parts.next().ok_or_else(|| PushError::IoError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "URL missing /app/stream path",
    )))?;

    let (host, port) = match authority.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|e| PushError::IoError(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())))?),
        None => (authority.to_string(), 1935),
    };

    let mut path_parts = path.splitn(2, '/');
    let app = path_parts.next().ok_or_else(|| PushError::IoError(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "URL missing app",
    )))?.to_string();
    let stream = path_parts.next().unwrap_or("").to_string();
    if stream.is_empty() {
        return Err(PushError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "URL missing stream key",
        )));
    }

    Ok(ParsedUrl { host, port, app, stream })
}

/// A live connected RTMP session ready to receive media tags. Internally
/// drives a xiu `ClientSession` in `Push` mode, plus a private `StreamsHub`
/// that we own (we are the only publisher and there are no subscribers; the
/// hub is just the event channel xiu's session expects).
pub struct Session {
    hub: StreamsHub,
    event_sender: StreamHubEventSender,
    session_handle: tokio::task::JoinHandle<Result<(), rtmp::session::errors::SessionError>>,
}

impl Session {
    /// Open TCP, run handshake, send NetConnection.connect + createStream +
    /// publish. Returns when the publish has been ACKed by the upstream.
    pub async fn connect(url: &str, timeout_ms: u64) -> Result<Self, PushError> {
        let parsed = parse_rtmp_url(url)?;

        let stream = timeout(
            Duration::from_millis(timeout_ms),
            TcpStream::connect((parsed.host.as_str(), parsed.port)),
        )
        .await
        .map_err(|_| PushError::Timeout)?
        .map_err(PushError::HandshakeFailed)?;

        // Hub owns the streamhub event bus that ClientSession publishes to.
        // For our pusher use-case, the hub has no subscribers — it just sinks
        // events. We run the hub on a background task.
        let mut hub = StreamsHub::new(None);
        let event_sender = hub.get_hub_event_sender();
        tokio::spawn(async move {
            hub.run().await;
        });

        // gop_num=0: no GOP cache (we are pushing a continuous live stream,
        // not relaying with replay).
        let mut client_session = ClientSession::new(
            stream,
            ClientSessionType::Push,
            format!("{}:{}", parsed.host, parsed.port),
            parsed.app.clone(),
            parsed.stream.clone(),
            event_sender.clone(),
            0,
        );

        // ClientSession::run() drives handshake → connect → createStream →
        // publish. It returns when the session ends (success path: never;
        // error path: returns Err(SessionError)).
        //
        // We need to know when the publish has been ACKed before returning
        // from this function. xiu's session emits a streamhub `Publish` event
        // at that point — but waiting for it requires subscribing to the hub
        // BEFORE calling run(). For PR 1 we accept a coarser signal: spawn
        // the run loop, sleep up to `timeout_ms`, then check whether the
        // session is still alive. If yes, publish succeeded; if no, examine
        // the JoinError for the underlying SessionError.
        //
        // This is intentionally simple — Task 10 (AMF onStatus parsing)
        // tightens the signal to react within ms instead of seconds.
        let session_handle = tokio::spawn(async move { client_session.run().await });

        // Give the handshake/connect/publish ~3s to complete on a fast LAN.
        // The full timeout_ms covers the TCP connect above; this is just the
        // RTMP round-trips.
        tokio::time::sleep(Duration::from_millis(500)).await;
        if session_handle.is_finished() {
            return Err(map_join_error(session_handle.await));
        }

        Ok(Self {
            hub: StreamsHub::new(None), // unused after spawn; kept for symmetry
            event_sender,
            session_handle,
        })
    }

    /// Send an FLV audio tag. Body is the raw FLV audio body (1-byte AudioTagHeader
    /// + AAC sequence header or AAC raw frame).
    pub async fn send_audio_tag(&mut self, timestamp_ms: u32, body: &[u8]) -> Result<(), PushError> {
        self.send_tag(8 /* FLV TagType audio */, timestamp_ms, body).await
    }

    /// Send an FLV video tag. Body is the raw FLV video body (1-byte VideoTagHeader
    /// + AVCDecoderConfigurationRecord or NALUs).
    pub async fn send_video_tag(&mut self, timestamp_ms: u32, body: &[u8]) -> Result<(), PushError> {
        self.send_tag(9 /* FLV TagType video */, timestamp_ms, body).await
    }

    async fn send_tag(&mut self, tag_type: u8, timestamp_ms: u32, body: &[u8]) -> Result<(), PushError> {
        // The xiu hub's event sender accepts FrameData messages addressed to
        // the stream identifier we published. The ClientSession's run-loop
        // reads them and packetizes into RTMP chunks.
        //
        // The implementer fills this in by reading
        // streamhub::define::FrameData (in ~/.cargo/registry/src/.../streamhub-0.2.x)
        // and finding the variant for raw audio/video frames. The body is
        // wrapped in `bytes::BytesMut`.
        //
        // If the session has died (run-loop returned), this returns
        // PushError::RemoteClosed.
        if self.session_handle.is_finished() {
            return Err(PushError::RemoteClosed(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "RTMP session ended",
            )));
        }

        // EXACT FrameData construction is xiu-specific. The implementer:
        // 1. Imports streamhub::define::{FrameData, NotifyInfo, ...}
        // 2. Builds a FrameData::Audio { timestamp: timestamp_ms, data: body.into() }
        //    or FrameData::Video equivalent.
        // 3. Sends via self.event_sender (a tokio::sync::mpsc-style sender).
        //
        // Suppress the unused warning until then.
        let _ = (tag_type, timestamp_ms, body, &self.event_sender);
        // NOTE TO IMPLEMENTER: If this stub stays here, Task 5's test fails
        // (no media reaches the wire). Replace with real FrameData send.

        Ok(())
    }

    /// Drop the TCP socket and abort the run-loop.
    pub async fn close(self) {
        self.session_handle.abort();
        // hub is dropped, sender is dropped — clean shutdown.
        drop(self.event_sender);
        drop(self.hub);
    }
}

fn map_join_error(
    result: Result<Result<(), rtmp::session::errors::SessionError>, tokio::task::JoinError>,
) -> PushError {
    match result {
        Ok(Ok(())) => PushError::RemoteClosed(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "session ended cleanly",
        )),
        Ok(Err(e)) => classify_session_error(e),
        Err(je) if je.is_cancelled() => PushError::LocalCancel,
        Err(je) => PushError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            je.to_string(),
        )),
    }
}

/// Map a xiu `SessionError` to our `PushError`. For PR 1 most variants land in
/// `IoError`; Task 10 narrows specific AMF rejection messages into
/// `ConnectRejected`/`PublishRejected`.
fn classify_session_error(e: rtmp::session::errors::SessionError) -> PushError {
    let s = e.to_string();
    if s.contains("handshake") {
        PushError::HandshakeFailed(std::io::Error::new(std::io::ErrorKind::Other, s))
    } else {
        PushError::IoError(std::io::Error::new(std::io::ErrorKind::Other, s))
    }
}
```

(Note to implementer: the `FrameData` send in `send_tag` is the xiu-specific bit. The struct definition is in `streamhub-0.2.x` — read it before filling in. The plan can't be more specific without reproducing xiu source verbatim.)

- [ ] **Step 2: Replace `push_flv_bytes` stub in `crates/rs-rtmp-push/src/pusher.rs`**

```rust
//! `RtmpPusher` — public API.

use crate::session::Session;
use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    config: PusherConfig,
    state: PusherState,
    session: Option<Session>,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self { url, config, state: PusherState::default(), session: None }
    }

    pub fn last_output_ts_ms(&self) -> u64 {
        self.state.last_output_ts_ms
    }

    pub fn reconnect_count(&self) -> u32 {
        self.state.reconnect_count
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Lazy-connect + write FLV bytes.
    ///
    /// On first call (or after a previous error dropped `self.session`),
    /// opens TCP + RTMP handshake + publish via xiu `ClientSession`. Then
    /// for each FLV tag in `bytes`, rewrites the timestamp to be monotonic
    /// across reconnects (Task 6) and sends to xiu.
    ///
    /// Empty `bytes` performs only the connect step (used by Task 3's test).
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        if self.session.is_none() {
            let s = Session::connect(&self.url, self.config.timeout_ms).await?;
            self.session = Some(s);
            self.state.connected = true;
        }

        if bytes.is_empty() {
            return Ok(());
        }

        // Tag-write loop is filled in Task 6.
        Err(PushError::LocalCancel)
    }

    pub async fn close(&mut self) {
        if let Some(s) = self.session.take() {
            s.close().await;
        }
        self.state.connected = false;
    }
}
```

- [ ] **Step 3: Replace `spawn_xiu_server` in `crates/rs-rtmp-push/tests/local_xiu_loopback.rs`**

The harness must run a real xiu RtmpServer (not just a bare TcpListener) so the pusher's handshake actually completes. Mirror `crates/rs-inpoint/src/rtmp_server.rs`:

```rust
use rtmp::rtmp::RtmpServer;
use streamhub::StreamsHub;
use tokio::net::TcpListener;

async fn spawn_xiu_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let event_sender = hub.get_hub_event_sender();
    let event_consumer = hub.get_client_event_consumer();
    tokio::spawn(async move { hub.run().await; });

    // gop_num = 0 (no GOP cache for tests).
    let server = RtmpServer::new_with_listener(listener, event_sender, 0, None, event_consumer);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    (format!("rtmp://{}/live/test", addr), handle)
}
```

(`RtmpServer::new_with_listener` may not be the exact API name — the implementer reads the rtmp crate's `src/rtmp.rs` to find the constructor that accepts an existing `TcpListener` instead of a `SocketAddr`. If only the SocketAddr-taking constructor exists, capture the listener's `local_addr()` first, drop the listener, and pass the address — accepting a small TOCTOU window for tests that don't run in parallel.)

- [ ] **Step 4: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/rs-rtmp-push/
git commit -m "feat(rtmp-push): handshake + connect + publish via xiu ClientSession (#103)"
```

---

### Task 5: TDD failing test — media payload byte equivalence

**Files:**
- Modify: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs` (add new test + helper)
- Create: `crates/rs-rtmp-push/tests/data/short.flv` (canned input — see Step 1)

- [ ] **Step 1: Generate the canned FLV test file**

The test needs a small (~1s) self-contained FLV file with one AAC sequence header + a few AAC raw frames + one AVC sequence header + a few AVC NALUs. Generation is **not** part of the subagent task — the orchestrator generates it once and commits it. To generate it (orchestrator, manual):

```bash
ffmpeg -y \
  -f lavfi -i "testsrc=duration=1:size=320x240:rate=30" \
  -f lavfi -i "sine=frequency=440:duration=1" \
  -c:v libx264 -preset ultrafast -tune zerolatency -g 30 \
  -c:a aac -b:a 96k -ar 44100 \
  -f flv crates/rs-rtmp-push/tests/data/short.flv
```

(The orchestrator runs this once and commits the binary file. Subagent only references the path.)

- [ ] **Step 2: Write the failing test**

Add this test to `crates/rs-rtmp-push/tests/local_xiu_loopback.rs`:

```rust
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Wire-level recorder: replaces the bare `spawn_xiu_server` for tests that
/// need to inspect what the pusher actually wrote on the wire.
///
/// Returns:
/// - the rtmp:// URL the pusher should connect to
/// - an Arc<Mutex<Vec<RecordedTag>>> the test inspects after pushing
/// - the server task handle
struct RecordedTag {
    tag_type: u8,
    timestamp_ms: u32,
    body: Vec<u8>,
}

async fn spawn_recording_xiu_server() -> (
    String,
    Arc<Mutex<Vec<RecordedTag>>>,
    tokio::task::JoinHandle<()>,
) {
    use rtmp::rtmp::RtmpServer;
    use streamhub::StreamsHub;
    use streamhub::define::FrameData;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let hub_sender = hub.get_hub_event_sender();
    let event_consumer = hub.get_client_event_consumer();

    // Subscribe a recorder that captures all FrameData published to "live/test".
    let recorded: Arc<Mutex<Vec<RecordedTag>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_clone = Arc::clone(&recorded);

    // The actual subscription wiring is xiu-specific — the implementer
    // reads streamhub::StreamsHub::subscribe (or equivalent) and registers
    // a consumer that pushes each FrameData::Audio/FrameData::Video into
    // recorded_clone.
    //
    // Simplified contract: when run_loop sees an Audio frame with timestamp T
    // and bytes B, it appends RecordedTag { tag_type: 8, timestamp_ms: T, body: B }.

    tokio::spawn(async move { hub.run().await; });

    let server = RtmpServer::new_with_listener(listener, hub_sender, 0, None, event_consumer);
    let handle = tokio::spawn(async move { let _ = server.run().await; });

    (format!("rtmp://{}/live/test", addr), recorded, handle)
}

#[tokio::test]
async fn media_payload_byte_identical_to_source() {
    let source_bytes = std::fs::read("tests/data/short.flv").expect("read short.flv");

    let (url, recorded, _server) = spawn_recording_xiu_server().await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    pusher.push_flv_bytes(&source_bytes).await.expect("push_flv_bytes");

    // Give the server a moment to drain the last tag.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let recorded_guard = recorded.lock().await;
    assert!(!recorded_guard.is_empty(), "no tags reached the server");

    // Extract source FLV media bodies and compare SHA256 of concatenated
    // audio bodies and concatenated video bodies. Wire-recorded bodies must
    // match exactly (the timestamps are rewritten by the pusher and are NOT
    // part of this assertion — see monotonic_ts_across_reconnect for that).
    let (src_audio_sha, src_video_sha) = sha256_flv_bodies(&source_bytes);
    let (rec_audio_sha, rec_video_sha) = sha256_recorded_bodies(&recorded_guard);

    assert_eq!(rec_audio_sha, src_audio_sha, "audio body bytes diverged");
    assert_eq!(rec_video_sha, src_video_sha, "video body bytes diverged");
}

fn sha256_flv_bodies(bytes: &[u8]) -> (String, String) {
    // Iterate FLV tags using the same flv module the pusher uses.
    // (Helper is implemented inside crates/rs-rtmp-push/src/flv.rs in Task 6;
    // the test re-imports it via `use rs_rtmp_push::*;` once Task 6 makes it pub.
    // For now, the test uses a private inline FLV walker — kept short because
    // the format is stable.)
    let mut audio = Sha256::new();
    let mut video = Sha256::new();

    // Skip 9-byte header + 4-byte PreviousTagSize0.
    let mut offset = 9 + 4;
    while offset + 11 <= bytes.len() {
        let tag_type = bytes[offset];
        let data_size = ((bytes[offset + 1] as usize) << 16)
            | ((bytes[offset + 2] as usize) << 8)
            | (bytes[offset + 3] as usize);
        let body_start = offset + 11;
        let body_end = body_start + data_size;
        if body_end > bytes.len() {
            break;
        }
        let body = &bytes[body_start..body_end];
        match tag_type {
            8 => audio.update(body),
            9 => video.update(body),
            _ => {} // skip script tags
        }
        offset = body_end + 4; // skip PreviousTagSize
    }

    (format!("{:x}", audio.finalize()), format!("{:x}", video.finalize()))
}

fn sha256_recorded_bodies(recorded: &[RecordedTag]) -> (String, String) {
    let mut audio = Sha256::new();
    let mut video = Sha256::new();
    for tag in recorded {
        match tag.tag_type {
            8 => audio.update(&tag.body),
            9 => video.update(&tag.body),
            _ => {}
        }
    }
    (format!("{:x}", audio.finalize()), format!("{:x}", video.finalize()))
}
```

- [ ] **Step 3: Confirm the test would fail**

`pusher.push_flv_bytes(&source_bytes)` returns `Err(PushError::LocalCancel)` (Task 4 stub at the tag-write loop). The test fails on `expect("push_flv_bytes")`.

- [ ] **Step 4: Commit (failing test only)**

```bash
git add crates/rs-rtmp-push/tests/local_xiu_loopback.rs crates/rs-rtmp-push/tests/data/short.flv
git commit -m "test(rtmp-push): assert media-payload byte equivalence (#103)"
```

---

### Task 6: Implement FLV demux + tag write + monotonic TS

**Files:**
- Modify: `crates/rs-rtmp-push/src/flv.rs` (replace stub with hand-rolled iterator)
- Modify: `crates/rs-rtmp-push/src/pusher.rs` (replace tag-write stub)
- Modify: `crates/rs-rtmp-push/src/lib.rs` (export `flv` module so tests can reuse it; optional)

- [ ] **Step 1: Implement `crates/rs-rtmp-push/src/flv.rs`**

```rust
//! Hand-rolled FLV tag iterator. The format is stable
//! (Adobe Flash Video File Format Specification v10) so a 80-LOC reader
//! avoids coupling to xflv's API surface.

use crate::PushError;

#[derive(Debug)]
pub struct FlvTag<'a> {
    pub tag_type: u8,        // 8 = audio, 9 = video, 18 = script
    pub timestamp_ms: u32,
    pub body: &'a [u8],
}

/// Iterate FLV tags from a self-contained FLV file (header + tags + previous-size markers).
///
/// On malformed input, returns `MalformedInput` with the byte offset where parsing failed.
pub struct FlvTagIter<'a> {
    bytes: &'a [u8],
    offset: usize,
    error: Option<PushError>,
}

impl<'a> FlvTagIter<'a> {
    /// Construct an iterator. Validates the 9-byte FLV header.
    pub fn new(bytes: &'a [u8]) -> Result<Self, PushError> {
        if bytes.len() < 9 + 4 {
            return Err(PushError::MalformedInput {
                offset: 0,
                reason: format!("FLV must be at least 13 bytes, got {}", bytes.len()),
            });
        }
        if &bytes[0..3] != b"FLV" {
            return Err(PushError::MalformedInput {
                offset: 0,
                reason: format!("expected 'FLV' signature, got {:?}", &bytes[0..3]),
            });
        }
        // Header is 9 bytes; the trailing 4-byte PreviousTagSize0 is always 0.
        Ok(Self { bytes, offset: 9 + 4, error: None })
    }

    pub fn into_error(self) -> Option<PushError> {
        self.error
    }
}

impl<'a> Iterator for FlvTagIter<'a> {
    type Item = FlvTag<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.offset + 11 > self.bytes.len() {
            return None;
        }

        let tag_type = self.bytes[self.offset];
        let data_size = ((self.bytes[self.offset + 1] as usize) << 16)
            | ((self.bytes[self.offset + 2] as usize) << 8)
            | (self.bytes[self.offset + 3] as usize);
        let ts_low = ((self.bytes[self.offset + 4] as u32) << 16)
            | ((self.bytes[self.offset + 5] as u32) << 8)
            | (self.bytes[self.offset + 6] as u32);
        let ts_high = self.bytes[self.offset + 7] as u32;
        let timestamp_ms = (ts_high << 24) | ts_low;
        // self.bytes[self.offset + 8..11] is StreamID (always 0).

        let body_start = self.offset + 11;
        let body_end = body_start + data_size;
        if body_end > self.bytes.len() {
            self.error = Some(PushError::MalformedInput {
                offset: self.offset,
                reason: format!(
                    "tag declares {} body bytes but only {} remain",
                    data_size,
                    self.bytes.len() - body_start
                ),
            });
            return None;
        }

        let body = &self.bytes[body_start..body_end];
        // Advance: body_end + 4-byte PreviousTagSize.
        self.offset = body_end + 4;

        Some(FlvTag { tag_type, timestamp_ms, body })
    }
}

/// Recognized FLV tag types we forward to RTMP. Anything else (script
/// metadata, unknown) is dropped with a tracing::warn.
pub const FLV_TAG_AUDIO: u8 = 8;
pub const FLV_TAG_VIDEO: u8 = 9;
pub const FLV_TAG_SCRIPT: u8 = 18;
```

- [ ] **Step 2: Implement the tag-write loop in `crates/rs-rtmp-push/src/pusher.rs`**

Replace `push_flv_bytes`:

```rust
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        if self.session.is_none() {
            let s = Session::connect(&self.url, self.config.timeout_ms).await?;
            self.session = Some(s);
            self.state.connected = true;
        }

        if bytes.is_empty() {
            return Ok(());
        }

        let session = self.session.as_mut().expect("session was just set");

        let iter = crate::flv::FlvTagIter::new(bytes)?;
        let mut chunk_first_ts: Option<u32> = None;
        let monotonic_offset = self.state.last_output_ts_ms;

        for tag in iter {
            let first = *chunk_first_ts.get_or_insert(tag.timestamp_ms);
            // Output TS = monotonic_offset + (tag_ts - chunk_first_ts).
            // Cast to u64 first to avoid u32 wrap on long sessions; xiu's
            // RTMP timestamp field is u32 on the wire — we will need to
            // use the RTMP "extended timestamp" mechanism for streams that
            // exceed ~49 days, but for now (max session ~12h) plain u32 is
            // safe.
            let delta = tag.timestamp_ms.saturating_sub(first);
            let output_ts_u64 = monotonic_offset + delta as u64;
            let output_ts = output_ts_u64 as u32;
            self.state.last_output_ts_ms = output_ts_u64;

            match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => {
                    session.send_audio_tag(output_ts, tag.body).await?;
                }
                crate::flv::FLV_TAG_VIDEO => {
                    session.send_video_tag(output_ts, tag.body).await?;
                }
                crate::flv::FLV_TAG_SCRIPT => {
                    // Skip script/metadata tags. ffmpeg today does too with
                    // -flvflags no_duration_filesize.
                }
                other => {
                    tracing::warn!(tag_type = other, "unknown FLV tag type, skipping");
                }
            }
        }

        Ok(())
    }
```

- [ ] **Step 3: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-rtmp-push/
git commit -m "feat(rtmp-push): write FLV tags with monotonic timestamp rewriting (#103)"
```

---

### Task 7: TDD failing test — monotonic TS across reconnect

**Files:**
- Modify: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs` (add new test)

- [ ] **Step 1: Write the failing test**

Append to `local_xiu_loopback.rs`:

```rust
use std::time::Duration;

#[tokio::test]
async fn monotonic_ts_across_reconnect() {
    // The test server records every incoming tag's wire-level timestamp.
    // The pusher pushes one chunk (TS internal 0..1000), then we drop the
    // TCP connection, then push a second chunk with the same internal
    // timestamps. The wire-recorded timestamps from the second chunk MUST
    // continue past the first chunk's last timestamp — never reset to 0.

    let (url, recorded, server_handle) = spawn_recording_xiu_server().await;
    let mut pusher = RtmpPusher::new(url.clone(), PusherConfig::default());

    let chunk1 = synthetic_flv_chunk(0, 1000);    // tags at TS 0, 100, 200, ..., 1000
    let chunk2 = synthetic_flv_chunk(0, 1000);    // tags at TS 0, 100, ..., 1000

    pusher.push_flv_bytes(&chunk1).await.expect("push chunk1");

    // Force a reconnect by aborting the server task and restarting it on
    // the SAME ephemeral port. The pusher's session detects the closed
    // socket on the next tag write and lazily reconnects.
    server_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // (The implementer wires a re-bind helper here. For PR 1, simplest is
    // to start a second `spawn_recording_xiu_server` on a fresh port and
    // create a NEW `RtmpPusher` with the same `last_output_ts_ms` carried
    // over — but that defeats the test. The correct fix: make the recording
    // server re-bindable on the original port. The implementer figures out
    // the cleanest way; the assertion below stays the same.)

    let (url2, recorded2, _server2) = spawn_recording_xiu_server().await;
    // Second pusher SHARES state via reuse: take the original pusher's
    // last_output_ts_ms and reconnect_count. We test the field semantics
    // by inspecting `last_output_ts_ms()` between chunks.
    assert!(pusher.last_output_ts_ms() >= 1000,
        "expected last_output_ts_ms >= 1000 after chunk1, got {}",
        pusher.last_output_ts_ms());

    // Re-point the pusher to the new server URL and push chunk2.
    // (Pusher does NOT support changing URLs after construction in PR 1 —
    // for the test, we instead set up the original server to come back on
    // a fresh listener but route into the same `recorded` Vec. The
    // implementer wires this; the test assertion below is the contract.)
    pusher.push_flv_bytes(&chunk2).await.expect("push chunk2");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let all_tags = recorded.lock().await.clone();
    let all_tags2 = recorded2.lock().await.clone();
    let combined: Vec<&RecordedTag> = all_tags.iter().chain(all_tags2.iter()).collect();
    assert!(combined.len() >= 2, "expected at least 2 recorded tags");

    // Assertion: timestamps are monotonically non-decreasing.
    let mut last = 0u32;
    for tag in &combined {
        assert!(
            tag.timestamp_ms >= last,
            "TS regressed: {} < {} (tag_type={})",
            tag.timestamp_ms,
            last,
            tag.tag_type,
        );
        last = tag.timestamp_ms;
    }

    // Assertion: chunk2's first tag should land on the wire with TS >= chunk1's
    // last TS. The second-chunk first tag's wire TS must be > 0 (if it were 0,
    // the bug we are fixing would still be present).
    let chunk2_first_wire_ts = all_tags2.first().map(|t| t.timestamp_ms);
    assert!(
        chunk2_first_wire_ts.is_some_and(|ts| ts > 0),
        "chunk2's first wire TS must be > 0 (carries forward from chunk1); got {:?}",
        chunk2_first_wire_ts,
    );

    // Reconnect counter must reflect exactly one reconnect.
    assert_eq!(pusher.reconnect_count(), 1,
        "expected exactly 1 reconnect, got {}", pusher.reconnect_count());
}

/// Build a minimal FLV file with audio tags at timestamps `ts_start..=ts_end`
/// stepped every 100ms. Bodies are deterministic 4-byte payloads.
fn synthetic_flv_chunk(ts_start: u32, ts_end: u32) -> Vec<u8> {
    use std::io::Write;
    let mut out = Vec::new();
    // FLV header: 'F' 'L' 'V' version=1 flags=0x05(audio+video) headerSize=9
    out.write_all(&[b'F', b'L', b'V', 1, 0x05, 0, 0, 0, 9]).unwrap();
    // PreviousTagSize0 = 0
    out.write_all(&[0u8; 4]).unwrap();

    let mut ts = ts_start;
    while ts <= ts_end {
        let body = [0xAB, 0xCD, 0xEF, ts as u8];
        let body_size = body.len() as u32;
        // Tag header: tag_type=8 (audio), data_size (3 bytes BE),
        //   timestamp_low (3 bytes BE), timestamp_high (1 byte), stream_id (3 bytes BE = 0)
        let ts_low = ts & 0x00FF_FFFF;
        let ts_high = (ts >> 24) as u8;
        out.push(8);
        out.push((body_size >> 16) as u8);
        out.push((body_size >> 8) as u8);
        out.push(body_size as u8);
        out.push((ts_low >> 16) as u8);
        out.push((ts_low >> 8) as u8);
        out.push(ts_low as u8);
        out.push(ts_high);
        out.write_all(&[0u8; 3]).unwrap(); // stream_id
        out.write_all(&body).unwrap();
        // PreviousTagSize = 11 + body_size.
        let prev_size = 11u32 + body_size;
        out.write_all(&prev_size.to_be_bytes()).unwrap();
        ts += 100;
    }

    out
}
```

- [ ] **Step 2: Confirm the test would fail**

Currently `RtmpPusher` does NOT preserve `last_output_ts_ms` across a session drop — when `Session::connect` is called the second time, the pusher state's `last_output_ts_ms` IS preserved (Task 6 implementation does `monotonic_offset = self.state.last_output_ts_ms` before each chunk and updates it as it goes), but **`reconnect_count` is never incremented** (no code path bumps it). The test's `assert_eq!(pusher.reconnect_count(), 1, ...)` will fail.

(The implementer subagent additionally must verify the test harness's reconnect mechanic — the comment in Step 1 acknowledges the harness needs implementer attention. If the implementer determines the harness-level reconnect can't be cleanly simulated in PR 1, the implementer files a follow-up issue and writes a unit-only version of this test that exercises `RtmpPusher::reconnect_count` and `last_output_ts_ms` directly via a mock `Session` that drops on demand. The contract — monotonic TS + counter increment — must still be tested somehow.)

- [ ] **Step 3: Commit (failing test only)**

```bash
git add crates/rs-rtmp-push/tests/local_xiu_loopback.rs
git commit -m "test(rtmp-push): assert monotonic TS across reconnect (#103)"
```

---

### Task 8: Implement reconnect with monotonic TS

**Files:**
- Modify: `crates/rs-rtmp-push/src/pusher.rs`

- [ ] **Step 1: Increment `reconnect_count` on every reconnect**

In `push_flv_bytes`, when a tag-send fails with an error that drops the session (handshake failure, remote close, IO error), we want to:
1. Drop `self.session` (so the next call lazy-reconnects).
2. Increment `self.state.reconnect_count` (so dashboards reflect the event).
3. Return the error.

Wrap the tag-send block in a closure-shaped error handler:

```rust
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        if self.session.is_none() {
            // Connection attempts: increment reconnect_count after the first
            // successful connect (i.e. when state.last_output_ts_ms > 0,
            // we are reconnecting; when it's still 0, this is the initial
            // connect and not a reconnect).
            let is_reconnect = self.state.last_output_ts_ms > 0;
            let s = match Session::connect(&self.url, self.config.timeout_ms).await {
                Ok(s) => s,
                Err(e) => {
                    if is_reconnect {
                        self.state.reconnect_count = self.state.reconnect_count.saturating_add(1);
                    }
                    return Err(e);
                }
            };
            if is_reconnect {
                self.state.reconnect_count = self.state.reconnect_count.saturating_add(1);
            }
            self.session = Some(s);
            self.state.connected = true;
        }

        if bytes.is_empty() {
            return Ok(());
        }

        let iter = crate::flv::FlvTagIter::new(bytes)?;
        let mut chunk_first_ts: Option<u32> = None;
        let monotonic_offset = self.state.last_output_ts_ms;

        for tag in iter {
            let first = *chunk_first_ts.get_or_insert(tag.timestamp_ms);
            let delta = tag.timestamp_ms.saturating_sub(first);
            let output_ts_u64 = monotonic_offset + delta as u64;
            let output_ts = output_ts_u64 as u32;
            self.state.last_output_ts_ms = output_ts_u64;

            let send_result = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => {
                    self.session
                        .as_mut()
                        .expect("session was just set")
                        .send_audio_tag(output_ts, tag.body)
                        .await
                }
                crate::flv::FLV_TAG_VIDEO => {
                    self.session
                        .as_mut()
                        .expect("session was just set")
                        .send_video_tag(output_ts, tag.body)
                        .await
                }
                crate::flv::FLV_TAG_SCRIPT => Ok(()),
                other => {
                    tracing::warn!(tag_type = other, "unknown FLV tag type, skipping");
                    Ok(())
                }
            };

            if let Err(e) = send_result {
                // Drop the session so the next call lazy-reconnects.
                self.state.connected = false;
                if let Some(s) = self.session.take() {
                    s.close().await;
                }
                return Err(e);
            }
        }

        Ok(())
    }
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-rtmp-push/src/pusher.rs
git commit -m "feat(rtmp-push): preserve monotonic output timestamp across reconnect (#103)"
```

---

### Task 9: TDD failing test — PublishRejected on invalid stream key

**Files:**
- Modify: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs` (add new test)

- [ ] **Step 1: Write the failing test**

Append to `local_xiu_loopback.rs`:

```rust
#[tokio::test]
async fn publish_rejected_on_invalid_stream_key() {
    // Spin up a xiu server configured to REJECT any publish that uses the
    // stream name "bad-key". The pusher targets that name and must surface
    // PushError::PublishRejected { code: "NetStream.Publish.BadName", .. }
    // within 5 seconds.

    let (url, _server) = spawn_rejecting_xiu_server("bad-key").await;
    let mut pusher = RtmpPusher::new(url, PusherConfig::default());

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        pusher.push_flv_bytes(&[]),
    )
    .await
    .expect("push_flv_bytes did not return within 5s");

    match result {
        Err(PushError::PublishRejected { code, .. }) => {
            assert_eq!(code, "NetStream.Publish.BadName",
                "expected NetStream.Publish.BadName, got code={}", code);
        }
        other => panic!("expected PushError::PublishRejected, got {:?}", other),
    }
}

/// Starts a xiu RtmpServer that explicitly rejects publishes for the named
/// stream by responding with NetStream.Publish.BadName.
async fn spawn_rejecting_xiu_server(reject_stream_name: &str) -> (String, tokio::task::JoinHandle<()>) {
    use rtmp::rtmp::RtmpServer;
    use streamhub::StreamsHub;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    // The implementer fills this in by reading xiu's session::server_session
    // (which handles publish requests) and either:
    // (a) writing a thin xiu-internal interceptor that injects a rejection
    //     onStatus when the publish stream_name matches, or
    // (b) configuring StreamsHub with a publish-validation callback (if the
    //     hub exposes one).
    //
    // If neither is straightforward, the implementer instead writes a
    // raw-TCP test stub that completes the RTMP handshake/connect manually
    // and replies to the publish command with the NetStream.Publish.BadName
    // onStatus AMF message. This is ~50 LOC since the protocol is small.
    let _ = reject_stream_name;

    let mut hub = StreamsHub::new(None);
    hub.set_rtmp_push_enabled(true);
    let event_sender = hub.get_hub_event_sender();
    let event_consumer = hub.get_client_event_consumer();
    tokio::spawn(async move { hub.run().await; });

    let server = RtmpServer::new_with_listener(listener, event_sender, 0, None, event_consumer);
    let handle = tokio::spawn(async move { let _ = server.run().await; });

    (format!("rtmp://{}/live/{}", addr, reject_stream_name), handle)
}
```

- [ ] **Step 2: Confirm the test would fail**

Today the pusher's `Session::connect` returns `Ok` whenever the underlying `ClientSession::run` doesn't immediately error. A xiu server that responds with `NetStream.Publish.BadName` would emit an `onStatus` AMF message but xiu's `ClientSession` may NOT translate that into an early-return error — it might just stay in the WaitStateChange state. The `tokio::time::timeout(5s, push_flv_bytes(&[]))` therefore TIMES OUT, and the test panics with "push_flv_bytes did not return within 5s".

(Even if the harness short-circuit doesn't happen, the test fails because Task 4's Session::connect doesn't surface `PublishRejected` — it surfaces `Ok` or `IoError`.)

- [ ] **Step 3: Commit (failing test only)**

```bash
git add crates/rs-rtmp-push/tests/local_xiu_loopback.rs
git commit -m "test(rtmp-push): assert PublishRejected surfaces NetStream.Publish.BadName (#103)"
```

---

### Task 10: Implement AMF onStatus parsing

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs` (parse onStatus messages)
- Modify: `crates/rs-rtmp-push/src/error.rs` (helper to build `PublishRejected` from AMF)

The xiu `ClientSession` already reads incoming RTMP messages (see `MessageParser` in `~/.cargo/registry/src/.../rtmp-0.6.5/src/messages/parser.rs`). The Connect ACK and Publish ACK both arrive as `onStatus` AMF command messages with a properties IndexMap that contains `code` (e.g. `"NetStream.Publish.Start"` for success, `"NetStream.Publish.BadName"` for "stream key invalid") and `description` (free-form text).

For PR 1, the cleanest path is:

- The pusher's `Session::connect` runs `client_session.run()` on a tokio task (Task 4 already does this).
- Before spawning, install an event tap on the streamhub event consumer (`event_consumer = hub.get_client_event_consumer()`). xiu's session emits `BroadcastEvent::Publish` for our identifier when the upstream ACKs our publish — that is the success signal.
- Before spawning, also install a status-error sink. xiu does not currently emit a structured "publish rejected" event — the implementer adds one by patching ClientSession's status-message handling, OR by reading the raw socket into a sniffer wrapper.

Because patching xiu is out of scope for a single PR, the implementer takes the **socket-sniff approach**:
1. Wrap the `TcpStream` in a `SniffingTcpStream` that mirrors all bytes read from the upstream into a tokio mpsc channel.
2. The pusher runs `client_session.run()` on the wrapped stream. xiu reads bytes normally; the sniffer also gets a copy.
3. A dedicated parser task consumes the sniffed bytes through `MessageParser` + `ChunkUnpacketizer`, watching for `onStatus` command messages whose `code` field starts with `NetStream.Publish.`. On the first `NetStream.Publish.Start`, fire a "publish ACKed" tokio oneshot. On the first `NetStream.Publish.*` whose code != `Start`, fire a "publish rejected" oneshot with the AMF `code` + `description`.
4. `Session::connect` waits on both oneshots with `tokio::select!`. On `publish ACKed` → return `Ok(self)`. On `publish rejected` → cancel the run task and return `Err(PushError::PublishRejected { code, description })`. On timeout → return `Err(PushError::Timeout)`.

(If the sniffing wrapper is too invasive, an alternative is to stop using `ClientSession::run()` for the connect phase and instead drive the protocol bytes manually using xiu's lower-level writers — see spec §1 brainstorm option B. The implementer chooses; either path satisfies the test.)

- [ ] **Step 1: Add `PushError::PublishRejected`-from-AMF helper to `error.rs`**

```rust
/// Build a `PublishRejected` from an AMF onStatus `code`. The description is
/// optional and may be empty if the upstream omits it.
pub fn rejected_from_status(code: String, description: Option<String>) -> PushError {
    PushError::PublishRejected {
        code,
        description: description.unwrap_or_default(),
    }
}
```

- [ ] **Step 2: Implement the AMF onStatus tap in `session.rs`**

The Step description above is the implementation contract. The implementer fills in the sniffing wrapper or the manual-protocol path. After this step, `Session::connect` returns:
- `Ok(Session)` when `NetStream.Publish.Start` is received within `timeout_ms`.
- `Err(PushError::PublishRejected { code, description })` when any other `NetStream.Publish.*` status code arrives.
- `Err(PushError::Timeout)` when neither arrives within `timeout_ms`.

- [ ] **Step 3: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-rtmp-push/src/
git commit -m "feat(rtmp-push): surface AMF NetStream.Publish status as PushError (#103)"
```

---

### Task 11: Add `PusherKind` config + integrate into `endpoint_task`

**Files:**
- Modify: `crates/rs-core/src/config.rs` (add `PusherKind` enum + `Endpoint.pusher` field)
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (branch on `PusherKind` in consumer)
- Modify: `crates/rs-delivery/src/endpoint_audit.rs` (add `RtmpPushAuditRecord` variant)
- Modify: `crates/rs-delivery/Cargo.toml` (add `rs-rtmp-push` dep)

- [ ] **Step 1: Add `PusherKind` to `crates/rs-core/src/config.rs`**

Above the `Endpoint` struct, add:

```rust
/// Which RTMP-push backend an endpoint uses. Default `Ffmpeg` keeps existing
/// `config.json` files behaving exactly as today; `Rust` selects the new
/// in-process pusher introduced for #103.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PusherKind {
    #[default]
    Ffmpeg,
    Rust,
}
```

In the `Endpoint` struct, add the field (subagent finds the existing struct and inserts at the bottom):

```rust
    #[serde(default)]
    pub pusher: PusherKind,
```

- [ ] **Step 2: Add the dep to `crates/rs-delivery/Cargo.toml`**

In the `[dependencies]` section of `crates/rs-delivery/Cargo.toml`, add:

```toml
rs-rtmp-push = { path = "../rs-rtmp-push" }
```

- [ ] **Step 3: Add the audit variant to `crates/rs-delivery/src/endpoint_audit.rs`**

Below the existing `FfmpegRestartRecord`:

```rust
/// Audit record emitted on every reconnect of an endpoint using
/// `PusherKind::Rust`. Mirrors `FfmpegRestartRecord` so the dashboard can
/// render either source uniformly. See spec §5.5.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RtmpPushAuditRecord {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub reconnect_count: u32,
    pub last_error_code: Option<String>,    // e.g. "NetStream.Publish.BadName"
    pub last_error_message: String,         // human-readable
    pub last_output_ts_ms: u64,
    pub lifetime_ms: u64,
    pub backoff_ms: u64,
}
```

- [ ] **Step 4: Branch on `PusherKind` in `crates/rs-delivery/src/endpoint_task.rs`**

The consumer's writer call (today around line 750: `p.write(...).await`) becomes a match. Add a fresh helper at the top of the file:

```rust
use rs_rtmp_push::{PushError, PusherConfig, RtmpPusher};
use rs_core::config::PusherKind;
```

Replace the `FfmpegProcess`-typed local with an enum:

```rust
enum Writer {
    Ffmpeg(rs_ffmpeg::FfmpegProcess),
    Rust(RtmpPusher),
}

impl Writer {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), WriteError> {
        match self {
            Writer::Ffmpeg(p) => p.write(bytes).await.map_err(WriteError::Ffmpeg),
            Writer::Rust(r) => r.push_flv_bytes(bytes).await.map_err(WriteError::Rust),
        }
    }

    fn is_alive(&self) -> bool {
        match self {
            Writer::Ffmpeg(p) => p.is_alive(),
            Writer::Rust(r) => r.reconnect_count() == 0 || /* internal session present */ true,
        }
    }
}

#[derive(Debug)]
enum WriteError {
    Ffmpeg(/* existing ffmpeg error type */),
    Rust(PushError),
}
```

(The actual `WriteError::Ffmpeg` variant uses whatever error type `FfmpegProcess::write` returns today — implementer reads `crates/rs-ffmpeg/src/lib.rs:207` to confirm.)

In the consumer task setup (around line 287/448), construct the writer based on `endpoint.config.pusher`:

```rust
let mut writer = match endpoint.config.pusher {
    PusherKind::Ffmpeg => {
        Writer::Ffmpeg(rs_ffmpeg::FfmpegProcess::spawn(/* existing args */)?)
    }
    PusherKind::Rust => {
        Writer::Rust(RtmpPusher::new(
            endpoint.config.url.clone(),
            PusherConfig::default(),
        ))
    }
};
```

In the write loop, replace `p.write(...)` with `writer.write(...)`. The existing reconnect/audit/backoff logic above the writer call stays unchanged.

When `Writer::Rust` returns `Err(WriteError::Rust(e))`, emit a `RtmpPushAuditRecord` (alongside the existing `FfmpegRestartRecord` emit path). Use `rs_rtmp_push::backoff_floor_ms(&e)` and `is_exponential(&e)` for the backoff math:

```rust
fn rust_pusher_backoff(err: &PushError, consecutive_errors: u32) -> Duration {
    let floor_ms = rs_rtmp_push::backoff_floor_ms(err).unwrap_or(0);
    let multiplier = if rs_rtmp_push::is_exponential(err) {
        1u64 << consecutive_errors.min(5) // cap exponent at 5 → max 32×
    } else {
        1
    };
    let total = floor_ms.saturating_mul(multiplier).min(300_000);
    Duration::from_millis(total)
}
```

- [ ] **Step 5: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 6: Commit**

```bash
git add crates/rs-core/src/config.rs crates/rs-delivery/
git commit -m "feat(delivery): add PusherKind config and rs-rtmp-push integration (#103)"
```

---

### Task 12: Playwright E2E for Rust pusher path

**Files:**
- Create: `e2e/rust-pusher.spec.ts`

- [ ] **Step 1: Write the Playwright test**

```typescript
import { test, expect } from '@playwright/test';

test('rust pusher delivers chunks and shows zero reconnects over 5min window', async ({ page }) => {
  // Arrange: harness sets up an event with one endpoint flagged pusher: "rust".
  // Test config lives in e2e/test-config.json and is loaded by the local
  // Rust API at test startup via /api/v1/test/load-config.
  // (The implementer wires the load-config helper if it doesn't already exist.)

  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('http://127.0.0.1:8910/');

  // Trigger the test event via API — same as audio-cadence test.
  const startResponse = await page.request.post('http://127.0.0.1:8910/api/v1/delivery/start', {
    data: { event_id: 'e2e-rust-pusher', /* ... endpoints from test-config.json ... */ },
  });
  expect(startResponse.ok()).toBeTruthy();

  // Wait up to 90s for chunks_processed > 0 on the endpoint.
  await expect(async () => {
    const status = await page.request.get('http://127.0.0.1:8910/api/v1/delivery/status?event_id=e2e-rust-pusher');
    const json = await status.json();
    const ep = json.endpoint_details?.[0];
    expect(ep?.chunks_processed).toBeGreaterThan(0);
  }).toPass({ timeout: 90_000, intervals: [3_000] });

  // Watch for 5 minutes; reconnect_count must stay zero.
  const startedAt = Date.now();
  const fiveMinutes = 5 * 60 * 1000;
  while (Date.now() - startedAt < fiveMinutes) {
    const status = await page.request.get('http://127.0.0.1:8910/api/v1/delivery/status?event_id=e2e-rust-pusher');
    const json = await status.json();
    const ep = json.endpoint_details?.[0];
    expect(ep?.reconnect_count, 'reconnect_count should remain 0').toBe(0);
    await page.waitForTimeout(15_000);
  }

  // Audit log must have at least one BytesPushed-equivalent record (we emit
  // per-chunk audit; assert at least one rtmp_push_progress event exists).
  const audit = await page.request.get('http://127.0.0.1:8910/api/v1/audit?event_id=e2e-rust-pusher&limit=200');
  const auditJson = await audit.json();
  const progressEvents = (auditJson.events ?? []).filter((e: any) =>
    e.event_type === 'rtmp_push_progress' || e.event_type === 'chunk_pushed'
  );
  expect(progressEvents.length).toBeGreaterThan(0);

  // Stop.
  await page.request.post('http://127.0.0.1:8910/api/v1/delivery/stop', {
    data: { event_id: 'e2e-rust-pusher' },
  });

  // Browser console: zero errors/warnings (per browser-console-zero-errors).
  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 2: Commit**

```bash
git add e2e/rust-pusher.spec.ts
git commit -m "test(e2e): Playwright spec for Rust pusher path (#103)"
```

---

### Task 13: Wire `e2e-obs-youtube-test` to use Rust pusher

**Files:**
- Modify: `.github/workflows/ci.yml` (the `e2e-obs-youtube-test` job's setup step)

- [ ] **Step 1: Find the existing test config setup in `e2e-obs-youtube-test`**

The job builds a JSON event payload that the local API consumes via `/api/v1/delivery/start`. Search for `event_id` in the YouTube job's PowerShell. Locate where the endpoint object is constructed (it has `name`, `url`, etc.).

- [ ] **Step 2: Add `pusher: "rust"` to the YouTube endpoint definition**

In the PowerShell that constructs the YouTube endpoint, add a property:

```powershell
$ytEndpoint = [PSCustomObject]@{
    name = "yt-e2e"
    url = "$env:YOUTUBE_RTMP_URL"
    # ... existing fields ...
    pusher = "rust"  # GATE: PR 1 e2e exercises the new pusher
}
```

(The exact existing object construction differs; implementer matches the existing style.)

- [ ] **Step 3: Add the reconnect-zero gate after the existing chunks-processed gate**

After the existing "GATE: chunks_processed > 0 within 90s" step (added in PR #146/#147), add:

```yaml
      - name: 'GATE: rust pusher reconnect_count stays at 0'
        run: |
          $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/status?event_id=$env:E2E_EVENT_ID"
          $reconnects = $resp.endpoint_details[0].reconnect_count
          if ($reconnects -ne 0) {
            throw "Rust pusher reconnected $reconnects times during the E2E window -- expected 0. Investigate before PR merge."
          }
          Write-Host "GATE PASSED: reconnect_count = 0"
        shell: pwsh
```

(ASCII-only PowerShell, no em-dashes, per `feedback_no_unicode_in_ci_scripts`.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: e2e-obs-youtube-test uses rust pusher with reconnect-zero gate (#103)"
```

---

### Task 14: Confirm `rs-rtmp-push` is in the mutation-testing matrix

**Files:**
- Modify: `.github/workflows/ci.yml` (the mutation-testing job)

- [ ] **Step 1: Find the cargo-mutants step**

Search `.github/workflows/ci.yml` for `cargo mutants`. The flag list typically looks like:

```yaml
        run: |
          cargo mutants --in-diff pr.diff --timeout 300 --build-timeout 600 --jobs 1 --output mutants-out \
            --exclude-re 'rescue::' \
            --exclude-re 'run_rescue_loop' \
            --exclude-re 'run_warmup_loop'
```

- [ ] **Step 2: Verify rs-rtmp-push is NOT excluded; add a comment locking it in**

Above the `--exclude-re` lines, add:

```yaml
          # rs-rtmp-push MUST stay in the mutation-testing matrix per spec §7.6
          # of docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md.
          # Do NOT add `--exclude-re 'rs_rtmp_push::'` or any equivalent.
```

If the existing flags list happens to already exclude `rs_rtmp_push::` (it doesn't today, but check), DELETE that line.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(mutants): keep rs-rtmp-push in mutation-testing matrix (#103)"
```

---

### Task 15 (orchestrator-only): Push, monitor CI, PR, post-deploy verify

**This task is NOT dispatched to a subagent. The orchestrator handles it directly.**

- [ ] **Step 1: Local pre-push check**

```bash
cargo fmt --all --check
```

If non-zero, run `cargo fmt --all` and commit as `style: cargo fmt`. Then re-check.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI**

```bash
RUN_ID=$(gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId')
# Single sleep+view pattern per ci-monitoring rule. NO loops, NO CronCreate.
sleep 600 && gh run view "$RUN_ID" --json status,conclusion,jobs
```

If failures: `gh run view "$RUN_ID" --log-failed`, fix in ONE commit, push ONCE, re-monitor.

- [ ] **Step 4: Verify all CI gates green**

The PR-1 acceptance is *no behavior change for existing endpoints*. CI must show:
- All existing tests pass (the Ffmpeg path is unchanged).
- New `rs-rtmp-push` unit + integration tests pass.
- New Playwright `rust-pusher.spec.ts` passes.
- `e2e-obs-youtube-test` passes WITH `pusher: "rust"` and `reconnect_count = 0`.
- `cargo-mutants` job processes `rs-rtmp-push` (not excluded). Surviving mutants → tighten assertions, push fix, re-run.

- [ ] **Step 5: Create PR**

```bash
gh pr create --title "feat: add rs-rtmp-push crate (#103, PR 1 of 4)" --body "$(cat <<'EOF'
## Summary
First PR of a 4-PR rollout to replace the ffmpeg subprocess in rs-delivery
with an in-process pure-Rust RTMP pusher (xiu ClientSession, Push mode).
This PR is **behavior-preserving for existing endpoints** — the new path
is opt-in via a new per-endpoint `"pusher": "rust"` config field; default
remains `"ffmpeg"`.

The fix that this groundwork enables: monotonic output timestamps across
reconnects. Every ffmpeg restart today resets PTS=0, triggering a
catch-up storm that produced 524 ffmpeg restarts after a 9.5h overnight
stream on FB-Zbynek (#103).

Subsequent PRs (operational, not in this scope):
- PR 2: flip FB-Zbynek to `pusher: "rust"`, run agent-driven 4h+ soak.
- PR 3: flip remaining endpoints, second soak.
- PR 4: delete `rs-ffmpeg` crate.

Refs spec: `docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md`

Refs #103.

## Test plan
- [ ] All existing CI gates green (no behavior change for ffmpeg path)
- [ ] `rs-rtmp-push` unit tests pass (handshake, monotonic-TS, byte-equiv, publish-rejection)
- [ ] Playwright `rust-pusher.spec.ts` passes
- [ ] `e2e-obs-youtube-test` passes with `pusher: "rust"` + `reconnect_count = 0` GATE
- [ ] `cargo-mutants` includes `rs_rtmp_push::` (no exclude); surviving mutants addressed
- [ ] Post-deploy on stream.lan: existing endpoints (still on ffmpeg) keep working unchanged

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Monitor PR CI to mergeable + clean**

```bash
PR_NUM=$(gh pr view --json number --jq .number)
sleep 600 && gh pr view "$PR_NUM" --json mergeable,mergeStateStatus,statusCheckRollup
```

The PR is ready ONLY when `mergeable: "MERGEABLE"` AND `mergeStateStatus: "CLEAN"`. Anything else (UNSTABLE, BEHIND, BLOCKED, DIRTY) → fix the cause first.

- [ ] **Step 7: Post-deploy verification on stream.lan**

After CI deploys v0.3.74 to stream.lan:

1. Open dashboard via Playwright: `mcp__plugin_playwright_playwright__browser_navigate` to the streamsnv URL.
2. Read version label from the DOM. Assert it shows `v0.3.74-dev.N` matching the pushed commit count.
3. Confirm existing endpoints — all currently configured as `pusher: "ffmpeg"` — show their normal status (no error banners, no chunks-processed regression).
4. Audit log: confirm no `RtmpPushAuditRecord` events (no endpoint is using the rust pusher yet — PR 1 is groundwork only).
5. Capture a dashboard screenshot for the completion report.

- [ ] **Step 8: Send completion report**

Per `~/devel/airuleset/modules/core/completion-report.md`. Include:
- All audit lines (`✅ CI: green`, `✅ /plan-check: N/N fulfilled`, `✅ /review: clean — 0 🔴 0 🟡 0 🔵`, `✅ Deploy: stream.lan dashboard shows v0.3.74-dev.N, existing ffmpeg endpoints unaffected`).
- Goal: "Land the in-process Rust RTMP pusher behind a per-endpoint `pusher: "rust"` flag, default ffmpeg, so existing live endpoints behave exactly as today and PR 2 can flip FB-Zbynek for soak validation."
- What changed: "New `rs-rtmp-push` crate plus `Endpoint.pusher` config field; ffmpeg path unchanged. No endpoints flipped to rust in this PR — that is PR 2."
- Dashboard URLs (every env × every service per CLAUDE.md).
- PR URL with title and number.
- ❓ Question only if a real decision is pending. If everything is clean, omit.

---

### Verification (full plan)

1. **Spec coverage:** Every spec section (§4-§7) has an implementing task in this plan. §6 (PRs 2-4) is intentionally not in this plan; it is operational follow-up.
2. **Behavior-preserving:** PR 1 changes zero behavior for existing endpoints. `pusher: "ffmpeg"` is the serde default; existing `config.json` files parse unchanged.
3. **TDD:** Tasks 3, 5, 7, 9 commit failing tests BEFORE the corresponding implementation tasks 4, 6, 8, 10.
4. **One commit per task:** Subagent-driven development dispatches one subagent per task; each commits once.
5. **CI push discipline:** Local checks limited to `cargo fmt --all --check`. No `cargo build`, `cargo test`, `cargo clippy` locally.
6. **Mutation testing:** §7.6 of the spec is enforced by Task 14 (no exclude for `rs_rtmp_push::`).
