# FB rust RTMPS push fix + CI E2E gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `rs-rtmp-push` so Facebook accepts `NetStream.publish` for `rtmps://live-api-s.facebook.com:443/rtmp/<key>`, flip FB endpoints back to `pusher='rust'` via migration v29, gate the fix with a CI E2E job pushing to a real FB stream key (closes #215).

**Architecture:** Three coupled code changes in `crates/rs-rtmp-push/src/session.rs` (drop default port from `tcUrl`, add `swfUrl`/`pageUrl` AMF fields, log outgoing AMF for future debug), one DB migration (`v29` flipping `service_type='FB'` from `ffmpeg` to `rust`), one new mock RTMP server integration test, and one new CI E2E job (`e2e-fb-push`) that pushes 60s to real FB and asserts publish success.

**Tech Stack:** Rust 2024 edition, `sqlx` SQLite, `xiu/rtmp` 0.6.5 RTMP client, `tokio`, GitHub Actions (Ubuntu runner), `ffmpeg`.

**Spec:** `docs/superpowers/specs/2026-05-17-fb-rust-rtmps-fix-and-e2e-design.md` (commit `eb1e9d7e`).

---

## Context

Issue #215: FB rust pusher consistently rejected by Facebook with `Publish Rejected: Invalid URL` despite TCP+TLS+CONNECT succeeding. Workaround live on streamsnv + streampp reverted FB rows to `pusher='ffmpeg'` via direct SQL on 2026-05-17. This plan closes #215.

Out of scope (referenced only):
- #216 streampp YT live-event regression — separate investigation
- #212 remove `PusherKind::Ffmpeg` entirely — gated on #213 4h soak + 14 days clean post-merge
- #213 4h sustained-stability soak — not a code task here

**Operator prerequisite (NOT a code task):** After Task 10 opens the PR, BEFORE `e2e-fb-push` can pass green, operator must seed the GitHub secret:

```bash
# One-time setup. FB persistent stream keys are Always Active (per feedback_fb_keys_persistent.md).
# Operator creates dedicated FB Page (or uses existing test Page).
# Live Producer → Streaming Software → Persistent Stream Key → Copy.
gh secret set FB_TEST_STREAM_KEY --body "<FB persistent stream key>"
```

The plan's Task 9 ships the CI job that consumes this secret; the secret itself is supplied out-of-band.

---

## File Structure

**Files modified:**
- `Cargo.toml` (root workspace) — version bump
- `src-tauri/Cargo.toml` — version bump
- `src-tauri/tauri.conf.json` — version bump
- `leptos-ui/Cargo.toml` — version bump
- `crates/rs-rtmp-push/src/session.rs` — add `build_tc_url` + `build_connect_props` helpers, add `tracing::debug!` AMF dump, change `negotiate` signature to take `host: &str, port: u16` instead of `raw_domain: &str`
- `crates/rs-core/src/db/migrations.rs` — bump `MAX_SCHEMA_VERSION` to 29, add `migrate_v29`, add dispatcher entry, add migration test
- `.github/workflows/ci.yml` — add `e2e-fb-push` job, wire into `e2e-gate`

**Files created:**
- `crates/rs-rtmp-push/tests/fb_mock_server.rs` — integration test with mock RTMP server validating AMF compliance

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml` (root workspace)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump workspace version 0.17.0 → 0.18.0 in `Cargo.toml`**

Read `Cargo.toml`. Find the line `version = "0.17.0"` in the `[workspace.package]` section. Replace with `version = "0.18.0"`.

- [ ] **Step 2: Bump version in `src-tauri/Cargo.toml`**

Find `version = "0.17.0"` and replace with `version = "0.18.0"`.

- [ ] **Step 3: Bump version in `src-tauri/tauri.conf.json`**

Find `"version": "0.17.0"` and replace with `"version": "0.18.0"`.

- [ ] **Step 4: Bump version in `leptos-ui/Cargo.toml`**

Find `version = "0.17.0"` and replace with `version = "0.18.0"`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.18.0 (#215)"
```

---

### Task 1: RED — `build_tc_url` unit test

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs` (add unit test in existing `#[cfg(test)] mod tests` block)

- [ ] **Step 1: Read existing test module location**

Open `crates/rs-rtmp-push/src/session.rs`. The test module starts at line 770 (`#[cfg(test)] mod tests {`). Tests for `parse_rtmp_url` start at line 780. New tests for `build_tc_url` go in the same module.

- [ ] **Step 2: Add failing unit test for `build_tc_url`**

In `crates/rs-rtmp-push/src/session.rs`, locate the `use super::{...}` line near the top of the test module (currently line 772: `use super::{READ_LOOP_HOLD_MS, READ_LOOP_IDLE_MS, Scheme, parse_rtmp_url};`). Replace it with:

```rust
    use super::{READ_LOOP_HOLD_MS, READ_LOOP_IDLE_MS, Scheme, build_tc_url, parse_rtmp_url};
```

After the existing URL parser tests (immediately after `rejects_non_rtmp_scheme` test which ends around line 827), add a new section header and four tests:

```rust
    // --- tc_url builder tests -----------------------------------------------

    #[test]
    fn build_tc_url_omits_default_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "live-api-s.facebook.com", 443, "rtmp");
        assert_eq!(url, "rtmps://live-api-s.facebook.com/rtmp");
    }

    #[test]
    fn build_tc_url_omits_default_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "a.rtmp.youtube.com", 1935, "live2");
        assert_eq!(url, "rtmp://a.rtmp.youtube.com/live2");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "127.0.0.1", 19350, "live");
        assert_eq!(url, "rtmp://127.0.0.1:19350/live");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "127.0.0.1", 19443, "live");
        assert_eq!(url, "rtmps://127.0.0.1:19443/live");
    }
```

- [ ] **Step 3: Verify test compiles RED (function not yet defined)**

Do NOT run `cargo test` locally per project Tier-2 CLAUDE.md. The controller (orchestrator) runs `cargo check --workspace` between batches. The expected failure at this point is `error[E0432]: unresolved import 'super::build_tc_url'` — the function doesn't exist yet.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "test(rtmp-push): RED build_tc_url unit tests for FB default-port handling (#215)"
```

---

### Task 2: GREEN — implement `build_tc_url` + replace inline `tc_url` in `negotiate`

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs`

- [ ] **Step 1: Add the `build_tc_url` helper function**

In `crates/rs-rtmp-push/src/session.rs`, locate the `parse_rtmp_url` function (starts at line 715). Add a new helper IMMEDIATELY ABOVE `parse_rtmp_url` (around line 714, right after `fn bad_url(...)` definition or before `parse_rtmp_url`). Insert:

```rust
/// Build the `tcUrl` AMF property for `NetConnection.connect`.
///
/// Per libobs / ffmpeg convention, the port is omitted from `tcUrl` when
/// it equals the scheme default (443 for rtmps, 1935 for rtmp). Facebook
/// Live ingest validates `tcUrl` strictly and rejects publish with
/// "Invalid URL" when the literal `:443` port suffix appears (#215).
pub(crate) fn build_tc_url(scheme: Scheme, host: &str, port: u16, app: &str) -> String {
    let scheme_str = match scheme {
        Scheme::Rtmp => "rtmp",
        Scheme::Rtmps => "rtmps",
    };
    let default_port = match scheme {
        Scheme::Rtmp => 1935,
        Scheme::Rtmps => 443,
    };
    if port == default_port {
        format!("{scheme_str}://{host}/{app}")
    } else {
        format!("{scheme_str}://{host}:{port}/{app}")
    }
}
```

- [ ] **Step 2: Change `negotiate` signature to take `host` + `port` separately**

In `crates/rs-rtmp-push/src/session.rs`, locate the `negotiate` function (starts at line 283). Replace its signature:

OLD (lines 283-289):
```rust
async fn negotiate(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    scheme: Scheme,
    raw_domain: &str,
    app: &str,
    stream_name: &str,
) -> Result<u32, PushError> {
```

NEW:
```rust
async fn negotiate(
    io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>>,
    scheme: Scheme,
    host: &str,
    port: u16,
    app: &str,
    stream_name: &str,
) -> Result<u32, PushError> {
```

- [ ] **Step 3: Replace inline `tc_url` construction with `build_tc_url` call**

Within `negotiate`, locate line 347-351 (the scheme_str + tc_url block):

OLD:
```rust
        let scheme_str = match scheme {
            Scheme::Rtmp => "rtmp",
            Scheme::Rtmps => "rtmps",
        };
        props.tc_url = Some(format!("{scheme_str}://{raw_domain}/{app}"));
```

NEW:
```rust
        props.tc_url = Some(build_tc_url(scheme, host, port, app));
```

The local `scheme_str` binding is no longer needed; remove it. (If any later code in the same `negotiate` function references `scheme_str`, leave it — but currently no such reference exists after line 351 within the connect block.)

- [ ] **Step 4: Update the caller in `Session::connect`**

In `crates/rs-rtmp-push/src/session.rs`, locate line 161-164:

OLD:
```rust
        let msg_stream_id = tokio::time::timeout(
            Duration::from_secs(NEGOTIATE_TIMEOUT_SECS),
            negotiate(Arc::clone(&io), scheme, &addr, &app, &stream_name),
        )
```

NEW:
```rust
        let msg_stream_id = tokio::time::timeout(
            Duration::from_secs(NEGOTIATE_TIMEOUT_SECS),
            negotiate(Arc::clone(&io), scheme, &host, port, &app, &stream_name),
        )
```

The `addr` local is only used for `lookup_host` (line 99) and to format the no-addresses error (line 106). Both uses remain valid. No need to delete `addr`.

- [ ] **Step 5: Verify the unit tests from Task 1 now expect to pass**

The orchestrator will run `cargo check --workspace` and `cargo test --no-run` after this task. Compilation success means the four `build_tc_url_*` tests defined in Task 1 will pass when executed in CI.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "fix(rtmp-push): GREEN build_tc_url helper drops default port from tcUrl (#215)"
```

---

### Task 3: RED — `build_connect_props` unit test

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs`

- [ ] **Step 1: Add the import for the not-yet-existing helper**

In `crates/rs-rtmp-push/src/session.rs`, find the test module `use super::{...}` line (updated in Task 1 to include `build_tc_url`). Update it to also import `build_connect_props`:

```rust
    use super::{
        READ_LOOP_HOLD_MS, READ_LOOP_IDLE_MS, Scheme, build_connect_props, build_tc_url,
        parse_rtmp_url,
    };
```

- [ ] **Step 2: Add failing unit tests for `build_connect_props`**

In `crates/rs-rtmp-push/src/session.rs` inside the `mod tests { ... }` block, after the `build_tc_url` test section added in Task 1, append:

```rust
    // --- build_connect_props tests ------------------------------------------

    #[test]
    fn connect_props_for_fb_sets_swf_url_and_page_url_without_port() {
        let props = build_connect_props(Scheme::Rtmps, "live-api-s.facebook.com", 443, "rtmp");
        assert_eq!(props.app.as_deref(), Some("rtmp"));
        assert_eq!(
            props.tc_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
        assert_eq!(
            props.swf_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
        assert_eq!(
            props.page_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
    }

    #[test]
    fn connect_props_preserves_legacy_fields() {
        let props = build_connect_props(Scheme::Rtmp, "a.rtmp.youtube.com", 1935, "live2");
        assert_eq!(
            props.flash_ver.as_deref(),
            Some("FMLE/3.0 (compatible; FMSc/1.0)")
        );
        assert_eq!(props.fpad, Some(false));
        assert_eq!(props.capabilities, Some(239.0));
        assert_eq!(props.audio_codecs, Some(3575.0));
        assert_eq!(props.video_codecs, Some(252.0));
        assert_eq!(props.video_function, Some(1.0));
        assert_eq!(props.object_encoding, Some(0.0));
        assert_eq!(props.pub_type.as_deref(), Some("nonprivate"));
    }
```

- [ ] **Step 3: Verify test fails RED**

Expected failure: `error[E0432]: unresolved import 'super::build_connect_props'`. The function doesn't exist yet — Task 4 introduces it.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "test(rtmp-push): RED build_connect_props sets swfUrl/pageUrl + preserves legacy fields (#215)"
```

---

### Task 4: GREEN — extract `build_connect_props` + populate `swf_url` + `page_url`

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs`

- [ ] **Step 1: Add the `build_connect_props` helper**

In `crates/rs-rtmp-push/src/session.rs`, IMMEDIATELY AFTER the `build_tc_url` function added in Task 2 (and before `parse_rtmp_url`), add:

```rust
/// Construct the AMF `ConnectProperties` for `NetConnection.connect`.
///
/// Mirrors libobs `obs-outputs/rtmp-stream.c` values: `flashVer`, `fpad`,
/// `capabilities`, `audioCodecs`, `videoCodecs`, `videoFunction`,
/// `objectEncoding`. Adds `swfUrl` + `pageUrl` matching `tcUrl` because
/// Facebook Live validates these fields on some publish paths (#215).
pub(crate) fn build_connect_props(
    scheme: Scheme,
    host: &str,
    port: u16,
    app: &str,
) -> ConnectProperties {
    let tc_url = build_tc_url(scheme, host, port, app);
    let mut props = ConnectProperties::new_none();
    props.app = Some(app.to_string());
    props.pub_type = Some("nonprivate".to_string());
    props.flash_ver = Some("FMLE/3.0 (compatible; FMSc/1.0)".to_string());
    props.fpad = Some(false);
    props.capabilities = Some(239.0);
    props.audio_codecs = Some(3575.0);
    props.video_codecs = Some(252.0);
    props.video_function = Some(1.0);
    props.object_encoding = Some(0.0);
    props.swf_url = Some(tc_url.clone());
    props.page_url = Some(tc_url.clone());
    props.tc_url = Some(tc_url);
    props
}
```

- [ ] **Step 2: Replace inline `ConnectProperties` block in `negotiate`**

In `crates/rs-rtmp-push/src/session.rs`, within `negotiate` (line 283+), locate the connect block (lines 331-354). The current block constructs `props` inline and calls `nc.write_connect(...)`. Replace it.

OLD (lines 331-354):
```rust
        let mut nc = NetConnection::new(Arc::clone(&io));
        let mut props = ConnectProperties::new_none();
        props.app = Some(app.to_string());
        props.pub_type = Some("nonprivate".to_string());
        // OBS advertises these on every RTMP connect. Without them, Facebook
        // Live silently accepts the publish and then discards the media (no
        // RTMP error returned, no preview shown in Live Producer). Operator
        // confirmed 2026-05-03 that FB shows zero data ingestion despite
        // pusher reporting healthy chunk-done logs. Mirror libobs values.
        props.flash_ver = Some("FMLE/3.0 (compatible; FMSc/1.0)".to_string());
        props.fpad = Some(false);
        props.capabilities = Some(239.0);
        props.audio_codecs = Some(3575.0); // OBS bitmask: AAC + MP3 + ...
        props.video_codecs = Some(252.0); // OBS bitmask: H.264 + ...
        props.video_function = Some(1.0); // CLIENT_SEEK
        props.object_encoding = Some(0.0); // AMF0
        let scheme_str = match scheme {
            Scheme::Rtmp => "rtmp",
            Scheme::Rtmps => "rtmps",
        };
        props.tc_url = Some(format!("{scheme_str}://{raw_domain}/{app}"));
        nc.write_connect(&(TRANSACTION_ID_CONNECT as f64), &props)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
```

NEW (replaces the block above):
```rust
        let mut nc = NetConnection::new(Arc::clone(&io));
        // libobs-mirrored ConnectProperties incl. swfUrl + pageUrl. Facebook
        // Live rejects publish when tcUrl carries the default port suffix
        // or when swfUrl/pageUrl are absent on some ingest paths (#215).
        let props = build_connect_props(scheme, host, port, app);
        nc.write_connect(&(TRANSACTION_ID_CONNECT as f64), &props)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
```

If Task 2 left a stale `scheme_str` binding earlier in the function, remove it now — `build_connect_props` owns scheme→string conversion internally via `build_tc_url`.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "fix(rtmp-push): GREEN build_connect_props populates swfUrl + pageUrl for FB (#215)"
```

---

### Task 5: Diagnostic AMF dump (observability, no TDD pair)

**Files:**
- Modify: `crates/rs-rtmp-push/src/session.rs`

- [ ] **Step 1: Add `tracing::debug!` AMF dump immediately before `write_connect`**

In `crates/rs-rtmp-push/src/session.rs`, within `negotiate`, the connect block now reads (after Task 4):

```rust
        let mut nc = NetConnection::new(Arc::clone(&io));
        let props = build_connect_props(scheme, host, port, app);
        nc.write_connect(&(TRANSACTION_ID_CONNECT as f64), &props)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
```

Insert the trace line BETWEEN `let props = ...;` and `nc.write_connect(...)`. New block:

```rust
        let mut nc = NetConnection::new(Arc::clone(&io));
        let props = build_connect_props(scheme, host, port, app);
        tracing::debug!(
            target: "rs_rtmp_push::connect",
            host = %host,
            port = port,
            app = %app,
            ?props,
            "sending NetConnection.connect"
        );
        nc.write_connect(&(TRANSACTION_ID_CONNECT as f64), &props)
            .await
            .map_err(|e| PushError::IoError(io::Error::other(e.to_string())))?;
```

`ConnectProperties` derives `Debug` (verified — xiu `rtmp` 0.6.5). The `?props` formatter prints all set fields. Target `rs_rtmp_push::connect` allows filtering via `RUST_LOG=rs_rtmp_push::connect=debug` without flooding from other modules.

- [ ] **Step 2: Commit**

```bash
git add crates/rs-rtmp-push/src/session.rs
git commit -m "feat(rtmp-push): debug-log outgoing AMF connect props for FB diagnosis (#215)"
```

---

### Task 6: Mock FB RTMP server integration test

**Files:**
- Create: `crates/rs-rtmp-push/tests/fb_mock_server.rs`

- [ ] **Step 1: Create the integration test file**

Create `crates/rs-rtmp-push/tests/fb_mock_server.rs` with the full content below. This test runs a TCP-based mock RTMP server that does a minimal handshake, reads the AMF `connect` payload, asserts FB-required fields, then either accepts (sends `_result`) or rejects. The rs-rtmp-push `Session::connect` is run against the mock; success is observed via `Session::connect` returning `Ok`.

Because the existing `Session::connect` ALWAYS sets up a full TLS path when given `rtmps://`, the mock test uses `rtmp://` (plain TCP) on localhost. The fix being tested (`build_tc_url` dropping default port, `build_connect_props` adding `swfUrl` + `pageUrl`) is scheme-independent — the assertion logic exercises the same code path that runs against FB ingest.

```rust
//! Integration test: rs-rtmp-push against a mock RTMP server that mimics
//! Facebook Live ingest's AMF validation rules.
//!
//! Validates:
//!   - tcUrl OMITS the default port (no `:1935` for rtmp, no `:443` for rtmps)
//!   - swfUrl AMF property is present
//!   - pageUrl AMF property is present
//!
//! These are the gaps that caused FB to reject `NetStream.Publish` (#215).
//!
//! The mock parses the AMF0 `connect` command, inspects the properties
//! object, and either replies with `_result` (accept) or closes the socket
//! after a short delay (reject). The rs-rtmp-push `Session::connect` should
//! succeed against the accepting mock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use rtmp::amf0::amf0_reader::Amf0Reader;
use rtmp::amf0::define::Amf0ValueType;
use rtmp::chunk::define::CHUNK_SIZE;
use rtmp::chunk::unpacketizer::ChunkUnpacketizer;
use rtmp::handshake::handshake_server::SimpleHandshakeServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

#[derive(Debug, Default)]
struct ConnectInspection {
    tc_url: Option<String>,
    swf_url: Option<String>,
    page_url: Option<String>,
}

/// Result the mock decided after reading the CONNECT command.
#[derive(Debug)]
struct MockOutcome {
    inspection: ConnectInspection,
    accepted: bool,
    reject_reason: Option<&'static str>,
}

/// Spawn the mock on `127.0.0.1:0` (kernel-assigned port). Returns the bound
/// port and a oneshot receiver carrying the inspection result + outcome.
async fn spawn_mock() -> (u16, tokio::sync::oneshot::Receiver<MockOutcome>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // --- Handshake (server side) -----------------------------------
        let mut handshaker = SimpleHandshakeServer::new(Arc::new(tokio::sync::Mutex::new(())));
        // Use a minimal manual handshake reader/writer here is non-trivial;
        // the simplest path is to drain the 3073-byte client handshake
        // bytes (C0+C1+C2) and respond with 3073 bytes of S0+S1+S2 of zeros.
        // RTMP servers in the wild accept zero-filled handshake payloads
        // when no encryption is negotiated.
        let _ = handshaker; // silence unused
        let mut c0c1 = [0u8; 1 + 1536];
        socket.read_exact(&mut c0c1).await.unwrap();
        // S0 = 0x03, S1 = 1536 zero bytes, S2 = echo of C1
        let mut s0s1s2 = vec![0u8; 1 + 1536 + 1536];
        s0s1s2[0] = 0x03;
        s0s1s2[1 + 1536..].copy_from_slice(&c0c1[1..]);
        socket.write_all(&s0s1s2).await.unwrap();
        let mut c2 = [0u8; 1536];
        socket.read_exact(&mut c2).await.unwrap();

        // --- Read chunks until we see NetConnection.connect ------------
        let mut unpacketizer = ChunkUnpacketizer::new();
        let mut connect_buf: Option<BytesMut> = None;
        let mut tmp = [0u8; 4096];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while connect_buf.is_none() && std::time::Instant::now() < deadline {
            let n = match timeout(Duration::from_millis(500), socket.read(&mut tmp)).await {
                Ok(Ok(n)) if n > 0 => n,
                _ => continue,
            };
            unpacketizer.extend_data(&tmp[..n]);
            while let Ok(Some(msg)) = unpacketizer.read_chunk() {
                // command messages: msg_type_id == 20 (AMF0) or 17 (AMF3)
                if msg.message_header.msg_type_id == 20 {
                    connect_buf = Some(msg.raw_data.clone());
                    break;
                }
            }
        }

        let raw = connect_buf.expect("did not receive CONNECT within 5s");

        // --- Parse AMF0: command_name, transaction_id, properties_obj --
        let mut reader = Amf0Reader::new(raw);
        let command_name = reader.read().unwrap();
        let _txn = reader.read().unwrap();
        let properties = reader.read().unwrap();

        assert!(
            matches!(&command_name, Amf0ValueType::UTF8String(s) if s == "connect"),
            "expected command_name='connect', got {command_name:?}"
        );

        let mut inspection = ConnectInspection::default();
        if let Amf0ValueType::Object(map) = &properties {
            if let Some(Amf0ValueType::UTF8String(s)) = map.get("tcUrl") {
                inspection.tc_url = Some(s.clone());
            }
            if let Some(Amf0ValueType::UTF8String(s)) = map.get("swfUrl") {
                inspection.swf_url = Some(s.clone());
            }
            if let Some(Amf0ValueType::UTF8String(s)) = map.get("pageUrl") {
                inspection.page_url = Some(s.clone());
            }
        }

        // --- Apply FB-mock validation rules ----------------------------
        let mut reject_reason: Option<&'static str> = None;
        if let Some(tc) = inspection.tc_url.as_deref() {
            if tc.contains(":1935/") || tc.contains(":443/") {
                reject_reason = Some("tcUrl contains default port suffix");
            }
        } else {
            reject_reason = Some("tcUrl missing");
        }
        if reject_reason.is_none() && inspection.swf_url.is_none() {
            reject_reason = Some("swfUrl missing");
        }
        if reject_reason.is_none() && inspection.page_url.is_none() {
            reject_reason = Some("pageUrl missing");
        }

        let accepted = reject_reason.is_none();

        if accepted {
            // Send a minimal AMF0 _result back so Session::connect returns Ok.
            // For RED-GREEN testing, we send a hand-rolled chunk: type=20
            // (command), command_name="_result", txn=1.0, properties=Object{},
            // information=Object{}.
            // Real format: chunk basic header (fmt=0, csid=3), msg header
            // (timestamp=0, length, type=20, stream=0), AMF0 payload.
            // For the assertion path we only care whether rs-rtmp-push
            // completes connect successfully; xiu's client tolerates a
            // simplified _result envelope. The minimal byte sequence below
            // matches what xiu's own test fixtures use.
            let payload = build_amf0_result();
            let mut chunk = Vec::with_capacity(12 + payload.len());
            chunk.push(0x03); // fmt=0, csid=3
            chunk.extend_from_slice(&[0u8; 3]); // timestamp
            let len = payload.len() as u32;
            chunk.push((len >> 16) as u8);
            chunk.push((len >> 8) as u8);
            chunk.push(len as u8);
            chunk.push(20); // command AMF0
            chunk.extend_from_slice(&[0u8; 4]); // stream id (LE)
            chunk.extend_from_slice(&payload);
            let _ = socket.write_all(&chunk).await;
        }

        // Send the outcome to the test thread BEFORE closing the socket.
        let _ = tx.send(MockOutcome {
            inspection,
            accepted,
            reject_reason,
        });

        // Give the client a moment to read _result before the socket closes.
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    (port, rx)
}

/// Build a minimal AMF0 `_result` payload for the connect transaction.
/// Format: UTF8 "_result", Number 1.0 (txn id), Object{} props, Object{} info.
fn build_amf0_result() -> Vec<u8> {
    let mut out = Vec::new();
    // UTF8 "_result"
    out.push(0x02);
    let s = b"_result";
    out.push(0x00);
    out.push(s.len() as u8);
    out.extend_from_slice(s);
    // Number 1.0
    out.push(0x00);
    out.extend_from_slice(&1.0f64.to_be_bytes());
    // Object{} (end marker only)
    out.push(0x03);
    out.push(0x00);
    out.push(0x00);
    out.push(0x09);
    // Object{} (end marker only)
    out.push(0x03);
    out.push(0x00);
    out.push(0x00);
    out.push(0x09);
    out
}

#[tokio::test]
async fn rust_pusher_sends_fb_compliant_connect_amf() {
    let (port, outcome_rx) = spawn_mock().await;
    let url = format!("rtmp://127.0.0.1:{port}/rtmp/test-stream-key");

    // Session::connect drives the FULL handshake + connect + publish flow.
    // We only need the CONNECT phase for this test; the mock closes the
    // socket after sending _result, which will manifest as a downstream
    // error AFTER Session::connect captures the outcome.
    let _ = tokio::time::timeout(
        Duration::from_secs(8),
        rs_rtmp_push::Session::connect(&url, 5000),
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_secs(3), outcome_rx)
        .await
        .expect("mock did not report within 3s")
        .expect("mock dropped sender");

    assert_eq!(
        outcome.reject_reason, None,
        "mock rejected: reason={:?}, tcUrl={:?}, swfUrl={:?}, pageUrl={:?}",
        outcome.reject_reason,
        outcome.inspection.tc_url,
        outcome.inspection.swf_url,
        outcome.inspection.page_url
    );
    assert!(outcome.accepted);
    assert_eq!(
        outcome.inspection.tc_url.as_deref(),
        Some("rtmp://127.0.0.1:{port}/rtmp"
            .replace("{port}", &port.to_string())
            .as_str())
            .or(Some(&format!("rtmp://127.0.0.1:{port}/rtmp")))
            .map(|s| s as &str),
        "tcUrl mismatch"
    );
    assert!(
        outcome.inspection.swf_url.is_some(),
        "swfUrl must be set per libobs/FB compat"
    );
    assert!(
        outcome.inspection.page_url.is_some(),
        "pageUrl must be set per libobs/FB compat"
    );
}
```

The format-and-compare for `tc_url` near the bottom is awkward — replace it with a cleaner version. The assertion block at the end of the test function should read:

```rust
    let expected_tc_url = format!("rtmp://127.0.0.1:{port}/rtmp");
    assert_eq!(
        outcome.inspection.tc_url.as_deref(),
        Some(expected_tc_url.as_str()),
        "tcUrl mismatch (note: port {port} is non-default for rtmp so it MUST appear)"
    );
```

Use the cleaner version. Why port appears: scheme is `rtmp`, default port is 1935, but the mock uses kernel-assigned port (any non-1935) so the port suffix should appear. This validates that `build_tc_url` correctly INCLUDES non-default ports.

- [ ] **Step 2: Add `bytes` and `tokio` dev-dependencies if missing**

Inspect `crates/rs-rtmp-push/Cargo.toml`. If `[dev-dependencies]` does not already include `bytes` and `tokio` (with `macros` + `rt-multi-thread` features), add them. Check the workspace `[dependencies]` first — `tokio` is almost certainly already a regular dep, in which case `[dev-dependencies]` inherits via `tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }` if the test needs additional features.

If `bytes` and `rtmp` are already regular deps of `rs-rtmp-push`, they're available in `tests/` automatically (Rust integration-test convention). Just confirm.

If anything is missing, add to `crates/rs-rtmp-push/Cargo.toml` `[dev-dependencies]`:

```toml
[dev-dependencies]
bytes = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "time", "io-util", "net", "sync"] }
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-rtmp-push/tests/fb_mock_server.rs crates/rs-rtmp-push/Cargo.toml
git commit -m "test(rtmp-push): integration mock validates FB-compliant CONNECT AMF (#215)"
```

---

### Task 7: RED — migration v29 test

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs`

- [ ] **Step 1: Locate the existing migration test block**

In `crates/rs-core/src/db/migrations.rs`, find the existing `#[cfg(test)] mod tests` block at the bottom of the file. Tests for v28 exist there as a reference pattern. New v29 tests go in the same module.

- [ ] **Step 2: Add failing tests for migration v29**

Append to the `#[cfg(test)] mod tests` module:

```rust
    #[tokio::test]
    async fn v29_flips_fb_endpoints_from_ffmpeg_to_rust() {
        let pool = test_pool().await;
        // Run all migrations up through current MAX (29 after this PR).
        run_migrations(&pool).await.unwrap();

        // Insert one FB row on ffmpeg, one YT row on ffmpeg, one Vimeo row on ffmpeg.
        sqlx::query(
            "INSERT INTO endpoint_configs (alias, service_type, stream_key, pusher, enabled) \
             VALUES ('fb-test', 'FB', 'fb-key', 'ffmpeg', 1), \
                    ('yt-test', 'YT_RTMP', 'yt-key', 'ffmpeg', 1), \
                    ('vimeo-test', 'VIMEO', 'v-key', 'ffmpeg', 1)"
        )
        .execute(&pool)
        .await
        .unwrap();

        // Rewind schema_version so v29 re-runs.
        sqlx::query("UPDATE schema_version SET version = 28")
            .execute(&pool)
            .await
            .unwrap();

        // Re-run migrations through dispatcher (the contract under test).
        run_migrations(&pool).await.unwrap();

        let fb: String = sqlx::query_scalar(
            "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-test'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let yt: String = sqlx::query_scalar(
            "SELECT pusher FROM endpoint_configs WHERE alias = 'yt-test'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let vimeo: String = sqlx::query_scalar(
            "SELECT pusher FROM endpoint_configs WHERE alias = 'vimeo-test'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(fb, "rust", "v29 must flip FB ffmpeg→rust");
        assert_eq!(yt, "ffmpeg", "v29 must NOT touch non-FB rows");
        assert_eq!(vimeo, "ffmpeg", "v29 must NOT touch non-FB rows");
    }

    #[tokio::test]
    async fn v29_is_idempotent() {
        let pool = test_pool().await;
        run_migrations(&pool).await.unwrap();

        sqlx::query(
            "INSERT INTO endpoint_configs (alias, service_type, stream_key, pusher, enabled) \
             VALUES ('fb-test', 'FB', 'fb-key', 'ffmpeg', 1)"
        )
        .execute(&pool)
        .await
        .unwrap();

        // First v29 run.
        sqlx::query("UPDATE schema_version SET version = 28")
            .execute(&pool)
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();

        // Second v29 run (idempotency check).
        sqlx::query("UPDATE schema_version SET version = 28")
            .execute(&pool)
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();

        let fb: String = sqlx::query_scalar(
            "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-test'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fb, "rust", "v29 idempotent — still rust after re-run");
    }

    #[tokio::test]
    async fn v29_does_not_touch_fb_rows_already_on_rust() {
        let pool = test_pool().await;
        run_migrations(&pool).await.unwrap();

        sqlx::query(
            "INSERT INTO endpoint_configs (alias, service_type, stream_key, pusher, enabled) \
             VALUES ('fb-already-rust', 'FB', 'fb-key', 'rust', 1)"
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("UPDATE schema_version SET version = 28")
            .execute(&pool)
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();

        let fb: String = sqlx::query_scalar(
            "SELECT pusher FROM endpoint_configs WHERE alias = 'fb-already-rust'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fb, "rust", "v29 only matches WHERE pusher='ffmpeg'");
    }
```

If `test_pool()` is not the existing helper name in the migrations test module, replace with whatever the existing v28 test uses (read the file to confirm — common names are `setup_test_pool`, `new_test_pool`, `test_pool`, or inline `SqlitePool::connect("sqlite::memory:")`).

- [ ] **Step 3: Verify RED**

Expected failure: dispatcher will panic on `unreachable!("unhandled migration version 29")` because `MAX_SCHEMA_VERSION` is still 28 and there's no `29 => migrate_v29(...)` arm. The tests will not compile until Task 8 adds them.

Note: because `run_migrations` won't try to run v29 until `MAX_SCHEMA_VERSION` is bumped, the actual RED here is "test compiles but v29-specific assertions fail because v29 didn't run". The dispatcher entry `29 => migrate_v29(&mut tx).await?` must exist for v29 to run at all.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/migrations.rs
git commit -m "test(db): RED migration v29 flips FB ffmpeg→rust + idempotent (#215)"
```

---

### Task 8: GREEN — implement migration v29

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs`

- [ ] **Step 1: Bump `MAX_SCHEMA_VERSION` to 29**

In `crates/rs-core/src/db/migrations.rs`, change line 16:

OLD:
```rust
pub const MAX_SCHEMA_VERSION: i32 = 28;
```

NEW:
```rust
pub const MAX_SCHEMA_VERSION: i32 = 29;
```

- [ ] **Step 2: Add the `migrate_v29` function**

In `crates/rs-core/src/db/migrations.rs`, IMMEDIATELY AFTER `migrate_v28` (currently ends at line 858), add:

```rust

/// Migration v29: flip `service_type='FB'` rows from `pusher='ffmpeg'` to
/// `pusher='rust'`. Reverses the v28 over-broad blanket flip that included
/// FB endpoints alongside YT, which broke FB delivery (#215) because the
/// rust RTMP push handshake was rejected by Facebook with "Invalid URL".
///
/// v29 is the scoped re-flip that runs AFTER the rust pusher's CONNECT AMF
/// is fixed (build_tc_url drops default port, build_connect_props adds
/// swfUrl + pageUrl in this same PR).
///
/// Idempotent: the WHERE clause only matches rows still on `ffmpeg`; rows
/// already on `rust` are not touched.
async fn migrate_v29(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    let result = sqlx::query(
        "UPDATE endpoint_configs SET pusher = 'rust' \
         WHERE pusher = 'ffmpeg' AND service_type = 'FB'",
    )
    .execute(&mut **tx)
    .await?;
    let rows = result.rows_affected();
    if rows > 0 {
        tracing::info!(
            rows_affected = rows,
            "v29: flipped FB endpoints 'ffmpeg' → 'rust' pusher"
        );
    } else {
        tracing::debug!("v29: no-op (no FB rows on 'ffmpeg')");
    }
    Ok(())
}
```

- [ ] **Step 3: Wire the dispatcher entry**

In `crates/rs-core/src/db/migrations.rs`, locate the `match version { ... }` block (currently lines 318-347). Add an arm for v29 IMMEDIATELY AFTER the v28 arm (line 346):

OLD:
```rust
            28 => migrate_v28(&mut tx).await?,
            _ => unreachable!("unhandled migration version {version}"),
```

NEW:
```rust
            28 => migrate_v28(&mut tx).await?,
            29 => migrate_v29(&mut tx).await?,
            _ => unreachable!("unhandled migration version {version}"),
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/migrations.rs
git commit -m "fix(db): GREEN migration v29 flips FB endpoints ffmpeg→rust (#215)"
```

---

### Task 9: Add `e2e-fb-push` CI job + wire into `e2e-gate`

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Locate the existing `e2e-obs-youtube` job as template**

In `.github/workflows/ci.yml`, the `e2e-obs-youtube` job header is at line 1786 (`name: E2E OBS-to-YouTube Test`). The job body extends roughly through the artifact download, restreamer start, ffmpeg push, and assertion-via-API pattern. Use this as the structural template.

- [ ] **Step 2: Add the `e2e-fb-push` job**

Find the END of the `e2e-obs-youtube` job (immediately before the next top-level job under `jobs:`). Insert a new job. The exact job key is `e2e-fb-push` so that `e2e-gate` (line 4902) can reference it via `needs.e2e-fb-push.result`.

Add the YAML block below into `.github/workflows/ci.yml` at the right indentation (2 spaces for top-level job keys under `jobs:`):

```yaml
  e2e-fb-push:
    name: E2E FB RTMPS Push (rust pusher)
    runs-on: ubuntu-latest
    needs: rust-ci
    if: ${{ needs.rust-ci.result != 'failure' }}
    timeout-minutes: 8
    env:
      FB_TEST_STREAM_KEY: ${{ secrets.FB_TEST_STREAM_KEY }}
    steps:
      - uses: actions/checkout@v4

      - name: Install ffmpeg and sqlite3
        run: |
          sudo apt-get update
          sudo apt-get install -y ffmpeg sqlite3 jq

      - name: Download restreamer artifact from rust-ci
        uses: actions/download-artifact@v4
        with:
          name: restreamer-linux
          path: ./bin

      - name: Verify FB_TEST_STREAM_KEY secret is set
        run: |
          if [ -z "$FB_TEST_STREAM_KEY" ]; then
            echo "FAIL: FB_TEST_STREAM_KEY secret not set."
            echo "Operator must run: gh secret set FB_TEST_STREAM_KEY --body '<FB persistent stream key>'"
            exit 1
          fi
          echo "FB_TEST_STREAM_KEY secret present (length: ${#FB_TEST_STREAM_KEY})"

      - name: Start restreamer service
        run: |
          chmod +x ./bin/restreamer
          ./bin/restreamer --headless > restreamer.log 2>&1 &
          echo $! > restreamer.pid
          for i in $(seq 1 30); do
            if curl -sf http://127.0.0.1:8910/api/v1/status > /dev/null; then
              echo "restreamer healthy after ${i}s"
              break
            fi
            sleep 1
          done
          curl -sf http://127.0.0.1:8910/api/v1/status || { cat restreamer.log; exit 1; }

      - name: Configure FB endpoint with rust pusher
        run: |
          RESPONSE=$(curl -s -X POST http://127.0.0.1:8910/api/v1/endpoints \
            -H 'Content-Type: application/json' \
            -d "{\"alias\":\"fb-ci-test\",\"service_type\":\"FB\",\"stream_key\":\"$FB_TEST_STREAM_KEY\",\"pusher\":\"rust\",\"enabled\":true}")
          echo "endpoint create response: $RESPONSE"
          echo "$RESPONSE" | jq -e '.id' > /dev/null || { echo "FAIL: endpoint create did not return id"; exit 1; }

      - name: Push 60s test stream into restreamer ingest
        timeout-minutes: 3
        run: |
          ffmpeg -nostdin -hide_banner -loglevel warning \
            -re -f lavfi -i "testsrc2=size=1280x720:rate=30" \
            -f lavfi -i "sine=frequency=440" \
            -c:v libx264 -preset veryfast -tune zerolatency -b:v 2500k -g 60 \
            -c:a aac -b:a 128k -ar 44100 -ac 2 \
            -t 60 -f flv rtmp://127.0.0.1:1935/live/CI

      - name: Wait for final FB push stats to settle
        run: sleep 8

      - name: Assert FB push succeeded
        run: |
          STATUS=$(curl -s http://127.0.0.1:8910/api/v1/endpoints/status)
          echo "$STATUS" | jq '.'
          FB=$(echo "$STATUS" | jq '.endpoints[] | select(.alias=="fb-ci-test")')
          [ -n "$FB" ] || { echo "FAIL: no fb-ci-test endpoint in status"; exit 1; }

          ALIVE=$(echo "$FB" | jq -r '.alive')
          CHUNKS=$(echo "$FB" | jq -r '.chunks_pushed // 0')
          BYTES=$(echo "$FB" | jq -r '.bytes_sent_since_connect // 0')

          DIED=$(curl -s "http://127.0.0.1:8910/api/v1/audit?action=rtmp_push_died&label=fb-ci-test&limit=10" \
            | jq '.events | length')

          echo "alive=$ALIVE chunks=$CHUNKS bytes=$BYTES died_events=$DIED"

          [ "$ALIVE" = "true" ] || { echo "FAIL: endpoint not alive after 60s push"; exit 1; }
          [ "$CHUNKS" -gt 25 ] || { echo "FAIL: chunks_pushed=$CHUNKS (expected >25 for 60s push)"; exit 1; }
          [ "$BYTES" -gt 1000000 ] || { echo "FAIL: bytes_sent_since_connect=$BYTES (expected >1MB)"; exit 1; }
          [ "$DIED" -eq 0 ] || { echo "FAIL: $DIED rtmp_push_died events recorded"; exit 1; }

          echo "PASS: FB rust push succeeded — chunks=$CHUNKS bytes=$BYTES no_deaths"

      - name: Capture restreamer log on failure
        if: failure()
        run: cat restreamer.log

      - name: Teardown endpoint
        if: always()
        run: |
          ID=$(curl -s http://127.0.0.1:8910/api/v1/endpoints | jq '.[] | select(.alias=="fb-ci-test") | .id')
          if [ -n "$ID" ]; then
            curl -s -X DELETE "http://127.0.0.1:8910/api/v1/endpoints/$ID"
          fi
          if [ -f restreamer.pid ]; then
            kill "$(cat restreamer.pid)" 2>/dev/null || true
          fi
```

- [ ] **Step 3: Wire `e2e-fb-push` into `e2e-gate`**

In `.github/workflows/ci.yml`, locate the `e2e-gate` job at line 4902. Find its `needs:` list (immediately under the `name:` line) and its assertion step.

For the `needs:` list, add `e2e-fb-push`:

OLD example (actual content may differ slightly — match the existing pattern):
```yaml
  e2e-gate:
    name: E2E Gate
    runs-on: ubuntu-latest
    needs:
      - e2e-streaming
      - e2e-obs-youtube
    if: ${{ always() }}
```

NEW:
```yaml
  e2e-gate:
    name: E2E Gate
    runs-on: ubuntu-latest
    needs:
      - e2e-streaming
      - e2e-obs-youtube
      - e2e-fb-push
    if: ${{ always() }}
```

For the assertion step (the existing `Verify all E2E results` or equivalent step body), add the `e2e-fb-push` result check. Mirror the pattern already used for `e2e-obs-youtube` (note `!= 'failure'` — see `feedback_ci_live_events.md` + project CLAUDE.md, NOT `== 'success'`):

Within the assertion step, locate the existing logic that fails the gate when an E2E job's `result` is `failure`. Add a parallel block for `e2e-fb-push`:

```yaml
          if [ "${{ needs.e2e-fb-push.result }}" = "failure" ]; then
            echo "e2e-fb-push FAILED"
            EXIT=1
          fi
```

The block goes alongside the existing `e2e-streaming` and `e2e-obs-youtube` blocks. Final `exit $EXIT` already exists at the end of the step.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add e2e-fb-push job and wire into e2e-gate (#215)"
```

---

### Task 10: ORCHESTRATOR-ONLY — pre-push gate, push, monitor CI, PR, post-deploy verify, completion report

**This task is NOT for a subagent. The plan controller (orchestrator) executes it personally.**

- [ ] **Step 1: Pre-push local gate**

```bash
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --no-run --workspace
```

All four must pass. If any fail, fix locally, amend the relevant task's commit (only via NEW commit — never `git commit --amend` per `commit-conventions.md`), then re-run.

- [ ] **Step 2: Run the new tests (Tier 2 fast-iterate authorizes local test execution for verification)**

```bash
cargo test -p rs-rtmp-push --test fb_mock_server -- --nocapture
cargo test -p rs-rtmp-push --lib -- session::tests:: --test-threads=1
cargo test -p rs-core --lib -- migrations::tests::v29_ --test-threads=1
```

Expected: all green. If RED, investigate locally before push (per CLAUDE.md "PR #194 had 9 CI roundtrips").

- [ ] **Step 3: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 4: Monitor CI to terminal state**

```bash
gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId'
# Capture the run id, then:
gh run view <run-id> --json status,conclusion,jobs
```

Wait until terminal. ALL jobs green, including the new `e2e-fb-push` (which requires operator to have seeded `FB_TEST_STREAM_KEY`).

If `e2e-fb-push` fails with "FB_TEST_STREAM_KEY secret not set" — STOP and request operator to run:

```bash
gh secret set FB_TEST_STREAM_KEY --body "<FB persistent stream key from dedicated FB Page>"
```

Then `gh run rerun <run-id> --failed`.

If `e2e-fb-push` fails with `chunks=0` or `rtmp_push_died` events — the AMF fix is incomplete. Pull the diagnostic AMF log from the run (added in Task 5), diff against libobs reference, iterate. Same PR.

- [ ] **Step 5: Create the PR**

```bash
gh pr create --title "fix: FB rust RTMPS push + CI E2E gate (#215)" --body "$(cat <<'EOF'
## Summary

Closes #215. FB endpoints on rust pusher were rejected by Facebook Live with "Publish Rejected: Invalid URL" because:

1. `tcUrl` included the default `:443` port suffix — libobs/ffmpeg omit it. Fixed by `build_tc_url` helper.
2. `swfUrl` and `pageUrl` AMF fields were missing — libobs always sends them. Fixed by `build_connect_props` helper.
3. No diagnostic surface — added `tracing::debug!` dump of outgoing AMF props for future regression debug.

Migration v29 re-flips `service_type='FB'` rows from `ffmpeg` back to `rust` after the fix lands. FB-scoped, idempotent.

New CI job `e2e-fb-push` pushes 60s to real FB ingest using `FB_TEST_STREAM_KEY` secret. Three-assertion gate (alive, chunks>25, bytes>1MB, zero death events). Wired into `e2e-gate`.

Mock RTMP server integration test in `crates/rs-rtmp-push/tests/fb_mock_server.rs` regression-guards the AMF compliance at unit-test speed.

`PusherKind::Ffmpeg` code path is preserved (ffmpeg subprocess removal is gated on #213 4h soak + 14 days clean post-merge, tracked in #212).

## Test plan

- [x] Unit: `cargo test -p rs-rtmp-push --lib session::tests::build_tc_url_*`
- [x] Unit: `cargo test -p rs-rtmp-push --lib session::tests::connect_props_*`
- [x] Integration: `cargo test -p rs-rtmp-push --test fb_mock_server`
- [x] Migration: `cargo test -p rs-core --lib migrations::tests::v29_*`
- [ ] CI E2E: `e2e-fb-push` green against real FB
- [ ] CI E2E: existing `e2e-obs-youtube` unchanged green (YT regression check)
- [ ] CI E2E: existing `e2e-streaming` + `frontend-e2e` unchanged green
- [ ] Post-deploy: streamsnv + streampp run migration v29; FB rows show `pusher='rust'`
- [ ] Post-deploy: operator triggers test stream → FB Live Producer shows preview within 10s
- [ ] 24h: zero `rtmp_push_died` events on FB endpoints in production audit

## Out of scope

- #216 streampp YT live-event regression (separate investigation)
- #212 remove `PusherKind::Ffmpeg` entirely (gated on #213 + 14d clean)
- RTMPS migration for YT (not requested)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Verify PR is mergeable + clean**

```bash
PR=$(gh pr view --json number --jq '.number')
gh api "repos/zbynekdrlik/restreamer/pulls/$PR" --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `{ "mergeable": true, "mergeable_state": "clean" }`.

If `behind`: `git pull --rebase origin main` is banned (rewrites history per `commit-conventions.md`). Use merge: `git fetch origin && git merge origin/main && git push origin dev`.
If `dirty` (conflicts): resolve in a new commit on dev.
If `blocked`: investigate which check is failing per `ci-monitoring.md`. Fix root cause.

- [ ] **Step 7: Post-deploy verification on streamsnv via win-stream-snv MCP**

After the deploy job in CI ships v0.18.0 to stream.lan (streamsnv):

```
mcp__win-stream-snv__ListProcesses with filter "Restreamer" — verify alive
mcp__win-stream-snv__Shell — Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status
mcp__win-stream-snv__Shell — Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/endpoints | Where service_type -eq 'FB' | Select alias,pusher
```

Assert: all FB endpoints show `pusher: "rust"` post-deploy (migration v29 ran).

- [ ] **Step 8: Post-deploy verification on streampp via win-streampp MCP**

```
mcp__win-streampp__ListProcesses with filter "Restreamer" — verify alive
mcp__win-streampp__Shell — Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status
mcp__win-streampp__Shell — Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/endpoints | Where service_type -eq 'FB' | Select alias,pusher
```

Same assertion: FB endpoints on `rust`.

- [ ] **Step 9: Read deployed version from live dashboard (per version-on-dashboard.md)**

Open `http://10.77.9.204:8910/` (streamsnv) and `http://100.71.235.121:8910/` (streampp) in Playwright. Read version label from DOM. Assert it matches `v0.18.0`. Both frontend and backend `/api/version` must match.

- [ ] **Step 10: Send completion report (per `completion-report.md` exact template)**

```
## ✅ Work Complete

**Audits & deploy:**
✅ CI: green
✅ /plan-check: 10/10 fulfilled
✅ /review: clean — 0 🔴 0 🟡 0 🔵
✅ Deploy: streamsnv + streampp show v0.18.0 in DOM (matches backend /api/version); all FB endpoints pusher='rust' post-migration
✅ Regression test: crates/rs-rtmp-push/tests/fb_mock_server.rs:<line> — RED on <test_sha>, GREEN on <fix_sha>

**E2E test coverage:**
| Feature/Fix | E2E Test File | What It Verifies |
|---|---|---|
| FB rust handshake | crates/rs-rtmp-push/tests/fb_mock_server.rs | Mock RTMP server validates tcUrl-no-default-port + swfUrl/pageUrl present |
| FB rust real push | .github/workflows/ci.yml e2e-fb-push | 60s push to real FB ingest; alive + chunks>25 + bytes>1MB + zero deaths |

---

**Goal:** Make Facebook Live accept rust pusher's RTMP publish (#215) so FB endpoints can return to the rust delivery path that YouTube already uses, and gate the fix in CI so a future regression cannot reach production.

**What changed:** rust pusher now sends FB-compliant CONNECT AMF (no default port in tcUrl, plus swfUrl + pageUrl). Migration v29 re-flips FB endpoints from ffmpeg to rust on next deploy. New CI job pushes to real FB and fails the build if the handshake breaks.

🌐 Dev:  http://10.77.9.204:8910/
🌐 Prod: http://100.71.235.121:8910/

**[restreamer] PR #<N>: fix: FB rust RTMPS push + CI E2E gate (#215)**
<full PR URL> — mergeable, clean
```

---

## Self-Review Notes

### Spec coverage

| Spec requirement | Plan task |
|---|---|
| `tc_url` drops default port (Architecture A) | Tasks 1+2 |
| Add `swfUrl` + `pageUrl` AMF (Architecture B) | Tasks 3+4 |
| Diagnostic AMF debug log (Architecture C) | Task 5 |
| Migration v29 FB-scoped, idempotent (Architecture D) | Tasks 7+8 |
| `PusherKind::Ffmpeg` stays compiled (Architecture E) | No code change — confirmed by Tasks 1-9 touching only `session.rs`, `migrations.rs`, `ci.yml` |
| Mock FB RTMP server unit-speed test | Task 6 |
| CI `e2e-fb-push` job | Task 9 |
| Wire into `e2e-gate` | Task 9 |
| `FB_TEST_STREAM_KEY` operator prerequisite | Plan header (out-of-band) |
| Post-deploy verification streamsnv + streampp | Task 10 steps 7-9 |
| Completion report per template | Task 10 step 10 |

All spec sections covered.

### Type consistency check

- `build_tc_url(scheme: Scheme, host: &str, port: u16, app: &str) -> String` — used identically in tests (Tasks 1, 3) and impl (Task 2) and `build_connect_props` body (Task 4).
- `build_connect_props(scheme: Scheme, host: &str, port: u16, app: &str) -> ConnectProperties` — same signature in test (Task 3) and impl (Task 4) and the `negotiate` call site (Task 4 step 2).
- `negotiate` signature change: `host: &str, port: u16` replaces `raw_domain: &str` — call site updated in Task 2 step 4.
- `migrate_v29` follows the same `async fn ... tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()>` shape as `migrate_v28`.

All consistent.

### Placeholder scan

No `TBD`, `TODO`, "implement later", or "similar to Task N" references. Every code step contains the literal code. Operator-prerequisite secret-seeding is explicit (callout in plan header AND Task 9 step 2 verification AND Task 10 step 4 retry hook).

### Bundling-gate check (per `autonomous-batch-issue-development.md`)

- Estimated LoC: ~250 (sessions.rs +50, migrations.rs +40, fb_mock_server.rs +180, ci.yml +110) ≈ 380. Slightly over the 300 LoC soft target but a SINGLE feature with cohesive scope (FB rust delivery fix). Per single-feature-single-PR rule, ships as one PR.
- No DB schema column changes (migration is data-only UPDATE on existing column).
- No public API break.
- No security boundary change.
- No cross-cutting refactor (changes scoped to one crate's session.rs + one crate's migrations.rs + one CI file).

Verdict: ONE PR. Confirmed.
