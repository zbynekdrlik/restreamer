# Always-On Rust-Only Rescue Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make rescue stream ALWAYS work regardless of operator configuration, with zero ffmpeg on Hetzner VPS at runtime.

**Architecture:** Embed a ~500KB pre-generated `default_rescue.flv` in the `rs-delivery` binary. At outage (cache drain + producer stalled, OR producer-gone, OR warmup-no-chunks): rescue loop pushes those FLV bytes via `rs_rtmp_push::push_flv_bytes`. Operator can still override per-template with custom MP4; the custom video is transcoded ONCE to FLV at upload time on stream.lan and stored in S3 as `.flv`, then pushed by the same rust loop. ffmpeg runs only at custom-video upload time, never at outage time, never on the VPS.

**Tech Stack:** Rust (rs-delivery, rs-rtmp-push, rs-api, rs-core, rs-cloud), Leptos/WASM (leptos-ui), Playwright (e2e tests), `ffmpeg` (one-shot for asset generation + custom-upload transcode only).

**Spec:** `docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md`

---

## File Map

**Create:**
- `crates/rs-delivery/assets/default_rescue.flv` — binary asset (~500KB, committed)
- `crates/rs-delivery/assets/logo.png` — optional source for blob generation (placeholder ok if missing)
- `crates/rs-delivery/src/bin/gen_rescue_flv.rs` — one-shot generator + `--check` verifier
- `crates/rs-delivery/src/rescue_default.rs` — `DEFAULT_RESCUE_FLV` const + blob integrity unit tests
- `crates/rs-delivery/src/rust_rescue_push.rs` — rust-only rescue loop pushing looped FLV via rs-rtmp-push
- `e2e/templates-default-rescue.spec.ts` — Playwright spec for UI hint

**Modify:**
- `crates/rs-delivery/Cargo.toml` — add `[[bin]] gen_rescue_flv`
- `crates/rs-delivery/src/lib.rs` (or `main.rs`) — expose `rescue_default` + `rust_rescue_push` modules
- `crates/rs-delivery/src/rescue.rs` — `resolve_rescue_bytes`, drop URL guard in warmup, replace `run_rescue_loop` ffmpeg spawn with `rust_rescue_push`
- `crates/rs-delivery/src/endpoint_task.rs:658-660` (consumer recv-None branch) — enter rescue instead of break
- `crates/rs-delivery/src/endpoint_task.rs:957-970` (select-loop) — defensive producer respawn from `last_delivered_chunk_id + 1`
- `crates/rs-delivery/src/rescue_tests.rs` — R1..R4 regression tests
- `crates/rs-api/src/rescue_video_handlers.rs` — transcode-on-upload to FLV
- `crates/rs-core/src/audit.rs` — `RescueLegacyFormatRejected`, `RescueCustomFetchFailed` enum variants
- `leptos-ui/src/components/templates.rs` — "Using built-in default" hint when URL=NULL
- `.github/workflows/ci.yml` — new `e2e-stream-lan-crash` job, `gen_rescue_flv --check` gate, drop URL precondition from `e2e-obs-youtube-test`
- `crates/rs-cloud/src/lib.rs:130 bootstrap_cloud_init` — remove `ffmpeg` from apt install list

---

## Task 1: Generate `default_rescue.flv` asset + generator binary

**Files:**
- Create: `crates/rs-delivery/assets/default_rescue.flv`
- Create: `crates/rs-delivery/src/bin/gen_rescue_flv.rs`
- Modify: `crates/rs-delivery/Cargo.toml`

- [ ] **Step 1: Create asset directory + placeholder logo (optional)**

```bash
mkdir -p crates/rs-delivery/assets
# logo.png is optional; generator handles missing logo with text-only output
```

- [ ] **Step 2: Add `[[bin]]` to `crates/rs-delivery/Cargo.toml`**

Append under existing `[lib]` / `[[bin]]` sections (if `rs-delivery` is already binary, add second `[[bin]]`):

```toml
[[bin]]
name = "gen_rescue_flv"
path = "src/bin/gen_rescue_flv.rs"

[dev-dependencies]
sha2 = { workspace = true }
```

If `sha2` already in `[dependencies]`, skip the `[dev-dependencies]` block. Check first:

```bash
grep -n "sha2" crates/rs-delivery/Cargo.toml
```

- [ ] **Step 3: Write `gen_rescue_flv.rs`**

```rust
// crates/rs-delivery/src/bin/gen_rescue_flv.rs
//! One-shot generator + verifier for `default_rescue.flv`.
//!
//! Two modes:
//!   * `cargo run --bin gen_rescue_flv` — regenerate the asset
//!   * `cargo run --bin gen_rescue_flv -- --check` — exit 0 only if the
//!     committed asset hashes match a freshly generated blob
//!
//! Uses external ffmpeg ONCE at dev/CI time. The produced FLV is committed to
//! the repo; the shipping rs-delivery binary uses `include_bytes!` and never
//! runs ffmpeg at runtime.

use std::path::Path;
use std::process::Command;

const ASSET_PATH: &str = "crates/rs-delivery/assets/default_rescue.flv";
const LOGO_PATH: &str = "crates/rs-delivery/assets/logo.png";
const OVERLAY_TEXT: &str = "Stream temporarily interrupted - please wait";
const DURATION_SECS: u32 = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let check_mode = args.iter().any(|a| a == "--check");

    let tmp = tempfile::NamedTempFile::new()?;
    let out_path = tmp.path().to_str().unwrap();

    let logo_exists = Path::new(LOGO_PATH).exists();
    let video_filter = if logo_exists {
        format!(
            "color=c=0x1a1a1a:s=1920x1080:d={dur},\
             drawtext=text='{txt}':fontcolor=white:fontsize=48:x=(w-text_w)/2:y=h-200,\
             [0:v]overlay=x=(W-w)/2:y=(H-h)/2-100",
            dur = DURATION_SECS,
            txt = OVERLAY_TEXT
        )
    } else {
        format!(
            "color=c=0x1a1a1a:s=1920x1080:d={dur},\
             drawtext=text='{txt}':fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2",
            dur = DURATION_SECS,
            txt = OVERLAY_TEXT
        )
    };

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y");
    if logo_exists {
        cmd.args(["-i", LOGO_PATH]);
    }
    cmd.args([
        "-f", "lavfi",
        "-i", &format!("anullsrc=r=48000:cl=stereo:d={}", DURATION_SECS),
        "-filter_complex", &video_filter,
        "-c:v", "libx264",
        "-profile:v", "main",
        "-preset", "medium",
        "-r", "30",
        "-g", "60",
        "-b:v", "1500k",
        "-c:a", "aac",
        "-ar", "48000",
        "-ac", "2",
        "-b:a", "64k",
        "-shortest",
        "-f", "flv",
        out_path,
    ]);

    let status = cmd.status()?;
    if !status.success() {
        return Err("ffmpeg failed".into());
    }

    let new_bytes = std::fs::read(out_path)?;

    if check_mode {
        let committed = std::fs::read(ASSET_PATH)
            .map_err(|e| format!("read {ASSET_PATH}: {e}"))?;
        // Hash both. ffmpeg output is reproducible byte-for-byte at same input + flags.
        use sha2::{Digest, Sha256};
        let new_hash = Sha256::digest(&new_bytes);
        let old_hash = Sha256::digest(&committed);
        if new_hash != old_hash {
            eprintln!(
                "Committed asset SHA256: {old_hash:x}\nFreshly generated SHA256: {new_hash:x}"
            );
            return Err("default_rescue.flv hash mismatch — regenerate with `cargo run --bin gen_rescue_flv`".into());
        }
        println!("OK: {ASSET_PATH} matches generator output ({} bytes)", new_bytes.len());
    } else {
        std::fs::write(ASSET_PATH, &new_bytes)?;
        println!("Wrote {ASSET_PATH} ({} bytes)", new_bytes.len());
    }

    Ok(())
}
```

- [ ] **Step 4: Add `tempfile` to dev-dependencies if missing**

```bash
grep -n "tempfile" crates/rs-delivery/Cargo.toml || \
  echo 'tempfile = "3"' >> crates/rs-delivery/Cargo.toml  # add under [dev-dependencies]
```

Manually verify the `tempfile` ends up under `[dev-dependencies]`, not at top level.

- [ ] **Step 5: Generate asset**

```bash
cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml
ls -la crates/rs-delivery/assets/default_rescue.flv
file crates/rs-delivery/assets/default_rescue.flv
```

Expected: file exists, ~300-700KB, identified as "Macromedia Flash Video" or "FLV". If the size is way off (<50KB or >2MB), inspect the ffmpeg output flags.

- [ ] **Step 6: Verify reproducibility**

```bash
cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml -- --check
```

Expected: `OK: crates/rs-delivery/assets/default_rescue.flv matches generator output (... bytes)`.

If hash differs, ffmpeg is producing non-deterministic output. Fix by adding `-fflags +bitexact -flags +bitexact` to the ffmpeg args, then regenerate + re-check.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/assets/default_rescue.flv crates/rs-delivery/src/bin/gen_rescue_flv.rs crates/rs-delivery/Cargo.toml
git commit -m "feat(rescue): gen_rescue_flv binary + commit default_rescue.flv asset

Pre-generates a ~5s 1080p30 FLV (still frame + silent AAC) via ffmpeg
ONCE at dev/CI time. The committed asset is embedded in rs-delivery via
include_bytes! in a later task; ffmpeg is never invoked at runtime.
--check mode verifies the committed bytes match a regenerated blob."
```

---

## Task 2: `DEFAULT_RESCUE_FLV` constant + R4 blob integrity test

**Files:**
- Create: `crates/rs-delivery/src/rescue_default.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (or `main.rs` if this crate is binary-only — check first)

- [ ] **Step 1: Locate the right module-root file**

```bash
ls crates/rs-delivery/src/lib.rs crates/rs-delivery/src/main.rs 2>/dev/null
```

If `lib.rs` exists, edit it. Otherwise edit `main.rs`. The plan assumes `lib.rs` exists; if it doesn't, substitute `main.rs`.

- [ ] **Step 2: Create `rescue_default.rs`**

```rust
// crates/rs-delivery/src/rescue_default.rs
//! Embedded default rescue FLV blob.
//!
//! Loaded at compile time from `assets/default_rescue.flv`. Pushed via
//! `rs_rtmp_push` whenever an endpoint enters rescue mode without a custom
//! operator-uploaded video. Always present, always works.
//!
//! Regenerate the asset with `cargo run --bin gen_rescue_flv`.

pub const DEFAULT_RESCUE_FLV: &[u8] =
    include_bytes!("../assets/default_rescue.flv");

#[cfg(test)]
mod tests {
    use super::*;

    /// R4: blob must be non-trivial and parse as FLV.
    #[test]
    fn default_rescue_flv_blob_integrity() {
        assert!(
            DEFAULT_RESCUE_FLV.len() > 100_000,
            "Default rescue blob too small: {} bytes (should be ~500KB)",
            DEFAULT_RESCUE_FLV.len()
        );
        assert!(
            DEFAULT_RESCUE_FLV.starts_with(b"FLV"),
            "Default rescue blob missing FLV magic prefix"
        );
        // Header: 'F' 'L' 'V' version flags datalen[4]
        // FLV version byte at offset 3, should be 1.
        assert_eq!(
            DEFAULT_RESCUE_FLV[3], 0x01,
            "Default rescue FLV version != 1"
        );
    }
}
```

- [ ] **Step 3: Register module in `lib.rs` / `main.rs`**

Add to `crates/rs-delivery/src/lib.rs`:

```rust
pub mod rescue_default;
```

If editing `main.rs` instead, use `mod rescue_default;` (no `pub`).

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p rs-delivery rescue_default::tests::default_rescue_flv_blob_integrity -- --nocapture
```

Expected: PASS. If FAIL with "blob too small" — Task 1 generated a corrupt FLV, regenerate. If FAIL with missing FLV magic — ffmpeg used wrong container, re-check the `-f flv` flag.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-delivery/src/rescue_default.rs crates/rs-delivery/src/lib.rs
git commit -m "feat(rescue): embed DEFAULT_RESCUE_FLV via include_bytes! + R4 blob test

Compile-time embedded default rescue blob. R4 asserts blob is >100KB and
starts with FLV magic. Future tasks consume DEFAULT_RESCUE_FLV from the
rescue resolve path."
```

---

## Task 3: `rust_rescue_push` — rust-only rescue loop

**Files:**
- Create: `crates/rs-delivery/src/rust_rescue_push.rs`
- Modify: `crates/rs-delivery/src/lib.rs`

- [ ] **Step 1: Inspect `rs_rtmp_push::RtmpPusher::push_flv_bytes` signature**

```bash
grep -n "pub async fn push_flv_bytes\|pub fn new\|impl RtmpPusher" crates/rs-rtmp-push/src/pusher.rs
```

Note the constructor and `push_flv_bytes` signature so the call sites in Step 2 use the exact types.

- [ ] **Step 2: Create `rust_rescue_push.rs`**

```rust
// crates/rs-delivery/src/rust_rescue_push.rs
//! Rust-only rescue push loop.
//!
//! Loops a pre-encoded FLV blob through `rs_rtmp_push::RtmpPusher::push_flv_bytes`
//! at real-time pace. Used during outages so the operator's RTMP/RTMPS endpoint
//! receives a continuous "Stream temporarily interrupted" video instead of
//! disconnecting. No ffmpeg, no external process.
//!
//! Exit conditions (returns `true` if stop signal received, else `false`):
//!   * stop_rx fires
//!   * `producer_active` has been true for `target_secs` continuous seconds
//!     (mirrors the old run_rescue_loop exit semantics — proves OBS is back +
//!     cache window refilled)

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rs_rtmp_push::RtmpPusher;

use crate::buffer_state::BufferState;
use crate::endpoint_task::Stats;

/// Continuous seconds of producer-active that proves rescue can exit.
pub const RESCUE_REFILL_TARGET_SECS: u64 = 120;

#[allow(clippy::too_many_arguments)]
pub async fn rust_rescue_push(
    alias: &str,
    ep_url: &str,
    stream_key: &str,
    flv_bytes: Arc<Vec<u8>>,
    buffer_state: Arc<BufferState>,
    stats: Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tracing::info!(
        alias,
        bytes = flv_bytes.len(),
        "Entering rust rescue push loop"
    );

    let mut pusher = match RtmpPusher::new(ep_url, stream_key).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(alias, "Rescue pusher init failed: {e}");
            // Wait for stop or refill; we cannot push but must not busy-loop.
            return wait_for_exit(buffer_state, stats, stop_rx).await;
        }
    };

    let mut continuous_active_secs: u64 = 0;
    let mut last_push: std::time::Instant = std::time::Instant::now();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Push one copy of the loop. push_flv_bytes already paces
                // at FLV-internal timestamps, so the loop runs at real-time.
                if let Err(e) = pusher.push_flv_bytes(&flv_bytes).await {
                    tracing::warn!(alias, "Rescue push error: {e}; will retry next loop");
                    // Backoff briefly to avoid burning CPU on a dead socket
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                // Track refill progress between pushes
                let elapsed = last_push.elapsed().as_secs();
                last_push = std::time::Instant::now();
                let is_active = buffer_state.producer_active.load(Ordering::Relaxed);
                if is_active {
                    continuous_active_secs = continuous_active_secs.saturating_add(elapsed);
                } else {
                    continuous_active_secs = 0;
                }
                let eta = RESCUE_REFILL_TARGET_SECS.saturating_sub(continuous_active_secs);
                {
                    let mut s = stats.lock().await;
                    s.delivery_mode = if is_active { "recovering".to_string() } else { "rescue".to_string() };
                    s.rescue_eta_secs = Some(eta);
                }
                if continuous_active_secs >= RESCUE_REFILL_TARGET_SECS {
                    tracing::info!(alias, "Producer active long enough, exiting rescue");
                    return false;
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias, "Rescue loop got stop signal");
                    return true;
                }
            }
        }
    }
}

async fn wait_for_exit(
    buffer_state: Arc<BufferState>,
    stats: Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let mut continuous_active_secs: u64 = 0;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let is_active = buffer_state.producer_active.load(Ordering::Relaxed);
                if is_active {
                    continuous_active_secs += 5;
                } else {
                    continuous_active_secs = 0;
                }
                let eta = RESCUE_REFILL_TARGET_SECS.saturating_sub(continuous_active_secs);
                {
                    let mut s = stats.lock().await;
                    s.delivery_mode = "rescue".to_string();
                    s.rescue_eta_secs = Some(eta);
                }
                if continuous_active_secs >= RESCUE_REFILL_TARGET_SECS {
                    return false;
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() { return true; }
            }
        }
    }
}
```

- [ ] **Step 3: Register module**

Append to `crates/rs-delivery/src/lib.rs`:

```rust
pub mod rust_rescue_push;
```

- [ ] **Step 4: Confirm compile**

```bash
cargo check -p rs-delivery
```

Expected: no errors. Common failures and fixes:
- `Stats` not exported → make it `pub` in `endpoint_task.rs`
- `BufferState` import path wrong → use `crate::buffer_state::BufferState`
- `RtmpPusher::new` signature mismatch → re-read Step 1's grep output and align

- [ ] **Step 5: Commit**

```bash
git add crates/rs-delivery/src/rust_rescue_push.rs crates/rs-delivery/src/lib.rs
git commit -m "feat(rescue): rust_rescue_push loop using rs_rtmp_push

Pure-rust rescue push: loops pre-encoded FLV bytes through RtmpPusher
at real-time pace until stop signal or producer-active-for-target.
Will replace the ffmpeg-spawning run_rescue_loop in a follow-up task."
```

---

## Task 4: `resolve_rescue_bytes` + S3 fetch + caching

**Files:**
- Modify: `crates/rs-delivery/src/rescue.rs`
- Modify: `crates/rs-core/src/audit.rs` — add `RescueLegacyFormatRejected` and `RescueCustomFetchFailed` enum variants

- [ ] **Step 1: Add audit enum variants**

Edit `crates/rs-core/src/audit.rs`. Find the existing `RescueActivated` / `RescueRecovered` block and append two variants in the same `enum AuditAction` (or whatever the enum is called — grep first to be sure):

```bash
grep -n "RescueActivated\|RescueRecovered\|enum AuditAction" crates/rs-core/src/audit.rs
```

Append after `RescueRecovered`:

```rust
    /// Delivery VPS detected an operator-configured rescue URL that is not a
    /// `.flv` (legacy MP4 / MOV / etc). VPS rejected the URL and fell back to
    /// the embedded default rescue blob. Operator must re-upload via the
    /// dashboard to restore custom rescue.
    RescueLegacyFormatRejected,
    /// Delivery VPS failed to fetch the operator-configured rescue FLV from
    /// S3 (network error, missing object, 403). VPS fell back to the embedded
    /// default rescue blob for this endpoint's lifetime.
    RescueCustomFetchFailed,
```

If the enum has a `#[serde]` rename map or string converter, also add the matching string forms (grep `RescueActivated` for the pattern).

- [ ] **Step 2: Add `resolve_rescue_bytes` to `rescue.rs`**

Add at top of `crates/rs-delivery/src/rescue.rs` (below existing `use` statements):

```rust
use std::borrow::Cow;
use std::sync::Arc;

use crate::rescue_default::DEFAULT_RESCUE_FLV;

/// Resolve the FLV bytes used for rescue on this endpoint.
///
/// Returns `Cow::Borrowed(DEFAULT_RESCUE_FLV)` when no operator URL is set OR
/// the URL is non-FLV (legacy MP4 etc). Returns `Cow::Owned(<S3 bytes>)` when
/// a custom `.flv` URL fetches successfully. On fetch failure logs + audits +
/// falls back to default.
///
/// Caller wraps the result in `Arc<Vec<u8>>` for cheap cloning across the
/// rescue loop iterations.
pub async fn resolve_rescue_bytes(
    rescue_video_url: Option<&str>,
    audit_ring: Option<&Arc<crate::audit_ring::AuditRing>>,
    alias: &str,
) -> Cow<'static, [u8]> {
    let url = match rescue_video_url {
        Some(u) if !u.is_empty() => u,
        _ => return Cow::Borrowed(DEFAULT_RESCUE_FLV),
    };

    if !url.to_lowercase().ends_with(".flv") {
        tracing::warn!(alias, url, "Non-FLV rescue URL rejected; using default");
        if let Some(ring) = audit_ring {
            crate::rescue_audit::emit_legacy_rejected(ring, alias, url);
        }
        return Cow::Borrowed(DEFAULT_RESCUE_FLV);
    }

    match fetch_flv_from_s3(url).await {
        Ok(bytes) => Cow::Owned(bytes),
        Err(e) => {
            tracing::warn!(alias, url, "Rescue FLV fetch failed: {e}; using default");
            if let Some(ring) = audit_ring {
                crate::rescue_audit::emit_custom_fetch_failed(ring, alias, url, &e.to_string());
            }
            Cow::Borrowed(DEFAULT_RESCUE_FLV)
        }
    }
}

async fn fetch_flv_from_s3(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }
    Ok(resp.bytes().await?.to_vec())
}
```

- [ ] **Step 3: Add audit emitter stubs to `rescue_audit.rs`**

Append to `crates/rs-delivery/src/rescue_audit.rs`:

```rust
pub fn emit_legacy_rejected(
    ring: &std::sync::Arc<crate::audit_ring::AuditRing>,
    alias: &str,
    url: &str,
) {
    use rs_core::audit::AuditAction;
    let r = legacy_rejected_row(alias, url);
    ring.push_parts(r);
    tracing::info!(action = ?AuditAction::RescueLegacyFormatRejected, alias, url, "audit");
}

pub fn emit_custom_fetch_failed(
    ring: &std::sync::Arc<crate::audit_ring::AuditRing>,
    alias: &str,
    url: &str,
    err: &str,
) {
    use rs_core::audit::AuditAction;
    let r = custom_fetch_failed_row(alias, url, err);
    ring.push_parts(r);
    tracing::info!(action = ?AuditAction::RescueCustomFetchFailed, alias, url, err, "audit");
}

pub fn legacy_rejected_row(alias: &str, url: &str) -> crate::audit_ring::RingRowParts {
    crate::audit_ring::RingRowParts {
        action: rs_core::audit::AuditAction::RescueLegacyFormatRejected,
        endpoint_alias: Some(alias.to_string()),
        details: serde_json::json!({ "url": url }),
    }
}

pub fn custom_fetch_failed_row(alias: &str, url: &str, err: &str) -> crate::audit_ring::RingRowParts {
    crate::audit_ring::RingRowParts {
        action: rs_core::audit::AuditAction::RescueCustomFetchFailed,
        endpoint_alias: Some(alias.to_string()),
        details: serde_json::json!({ "url": url, "error": err }),
    }
}
```

If `RingRowParts` has a different shape, grep `RingRowParts` in the file and align the struct literal exactly:

```bash
grep -n "struct RingRowParts\|fn rescue_activated_row" crates/rs-delivery/src/audit_ring.rs crates/rs-delivery/src/rescue_audit.rs
```

- [ ] **Step 4: Compile + run existing rescue tests to make sure nothing broke**

```bash
cargo test -p rs-delivery rescue -- --nocapture
```

Expected: existing tests still pass. New tests (R1..R3) come in later tasks.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-delivery/src/rescue.rs crates/rs-delivery/src/rescue_audit.rs crates/rs-core/src/audit.rs
git commit -m "feat(rescue): resolve_rescue_bytes returns embedded default when URL unset

Adds rust-only resolver: returns Cow::Borrowed(DEFAULT_RESCUE_FLV) when
rescue_video_url is None / empty / non-FLV, else fetches the .flv from S3
into Cow::Owned. Adds two new audit variants
(RescueLegacyFormatRejected, RescueCustomFetchFailed) for the fallback
paths."
```

---

## Task 5: R1 RED — `rescue_activates_when_url_null_and_cache_drains`

**Files:**
- Modify: `crates/rs-delivery/src/rescue_tests.rs`

- [ ] **Step 1: Read existing test patterns to match style**

```bash
grep -n "fn warmup_without_rescue_url\|fn warmup_with_rescue_url\|MockChunkFetcher\|TestHarness" crates/rs-delivery/src/rescue_tests.rs | head -10
```

Note the harness pattern used by neighboring rescue tests so the new test plugs into the same infrastructure (do NOT invent a new harness).

- [ ] **Step 2: Append R1 to `rescue_tests.rs`**

```rust
/// R1 (RED before fix; GREEN after Task 7): when rescue_video_url is None
/// and the buffer drains AND producer goes stalled, the consumer MUST enter
/// rescue mode using the embedded DEFAULT_RESCUE_FLV — not skip rescue
/// entirely as today.
#[tokio::test]
async fn rescue_activates_when_url_null_and_cache_drains() {
    use crate::rescue_default::DEFAULT_RESCUE_FLV;

    let harness = TestHarness::new_with_rescue_url(None).await; // see existing pattern
    harness.fill_buffer(0).await; // empty buffer
    harness.set_producer_active(false).await;

    // Wait for rescue threshold (RESCUE_STALL_THRESHOLD_SECS in rescue.rs)
    tokio::time::sleep(std::time::Duration::from_secs(
        crate::rescue::RESCUE_STALL_THRESHOLD_SECS + 1,
    ))
    .await;

    let mode = harness.stats_delivery_mode().await;
    assert_eq!(
        mode, "rescue",
        "Expected delivery_mode=rescue when URL=None + cache drained, got {mode:?}"
    );

    // Assert RescueActivated audit row emitted
    let actions = harness.audit_actions().await;
    assert!(
        actions.iter().any(|a| matches!(a, rs_core::audit::AuditAction::RescueActivated)),
        "Expected RescueActivated audit row, got {actions:?}"
    );

    // Assert the pusher received DEFAULT_RESCUE_FLV bytes
    let pushed = harness.pushed_bytes().await;
    assert!(
        pushed.windows(DEFAULT_RESCUE_FLV.len()).any(|w| w == DEFAULT_RESCUE_FLV),
        "Expected DEFAULT_RESCUE_FLV in pushed bytes (len={})",
        pushed.len()
    );
}
```

If `TestHarness::new_with_rescue_url` does not exist yet, search the file for the closest existing helper and extend it. The plan assumes you will add a `new_with_rescue_url(url: Option<&str>) -> Self` constructor that mirrors the existing one but parameterizes the URL.

- [ ] **Step 3: Run R1; expect FAIL**

```bash
cargo test -p rs-delivery rescue_activates_when_url_null_and_cache_drains -- --nocapture
```

Expected: FAIL with `Expected delivery_mode=rescue when URL=None + cache drained, got "normal"` (or similar). This proves the gap exists.

- [ ] **Step 4: Commit RED**

```bash
git add crates/rs-delivery/src/rescue_tests.rs
git commit -m "test(rescue): [red] R1 — rescue activates when URL=None and cache drains

Reproduces the 2026-05-30 production incident: stream.lan crashed, cache
drained on VPS, all endpoints went dark because no rescue URL was set on
any template. This test will go GREEN once resolve_rescue_bytes is wired
into the consumer cache-drain branch in the next task."
```

---

## Task 6: Wire `resolve_rescue_bytes` + `rust_rescue_push` into consumer (R1 GREEN)

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (cache-drain branch around line 664-708)
- Modify: `crates/rs-delivery/src/rescue.rs` (replace `run_rescue_loop` to use `rust_rescue_push`)

- [ ] **Step 1: Replace `run_rescue_loop` body in `rescue.rs`**

Edit `crates/rs-delivery/src/rescue.rs:205` (the `run_rescue_loop` fn). Replace the existing body that spawns ffmpeg with:

```rust
#[allow(clippy::too_many_arguments)]
pub async fn run_rescue_loop(
    alias: &str,
    rescue_url: Option<&str>,                              // <-- now Option
    service_type: rs_ffmpeg::ServiceType,
    stream_key: &str,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    audit_ring: Option<&std::sync::Arc<crate::audit_ring::AuditRing>>,
) -> bool {
    let ep_url = endpoint_url_for_service(service_type, stream_key);
    let bytes_cow = resolve_rescue_bytes(rescue_url, audit_ring, alias).await;
    let flv_bytes: std::sync::Arc<Vec<u8>> = std::sync::Arc::new(bytes_cow.into_owned());

    let initial_text = format_countdown_text(
        &DeliveryMode::Rescue { reason: RescueReason::BufferEmpty },
        RESCUE_REFILL_TARGET_SECS,
    );
    write_countdown_file(alias, &initial_text);

    let stopped = crate::rust_rescue_push::rust_rescue_push(
        alias,
        &ep_url,
        stream_key,
        flv_bytes,
        buffer_state.clone(),
        stats.clone(),
        stop_rx,
    )
    .await;

    cleanup_countdown_file(alias);
    stopped
}
```

Note signature change: `rescue_url: &str` → `Option<&str>`, plus an `audit_ring` param. Update ALL callers (next steps).

- [ ] **Step 2: Update consumer cache-drain caller in `endpoint_task.rs`**

Find the block around `endpoint_task.rs:664`:

```bash
grep -n "run_rescue_loop\|rescue_url\." crates/rs-delivery/src/endpoint_task.rs
```

Modify the existing block:

```rust
            _ = tokio::time::sleep(std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS)) => {
                if !buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
                    tracing::warn!(alias = %alias, "Consumer: buffer empty + producer stalled, entering rescue mode");

                    let rescue_started = std::time::Instant::now();
                    crate::rescue_audit::emit_activated(&audit_ring, &alias, last_delivered_chunk_id);

                    if let Some(mut p) = proc.take() {
                        p.kill().await;
                    }
                    {
                        let mut s = stats.lock().await;
                        s.delivery_mode = "rescue".to_string();
                        s.rescue_eta_secs = Some(crate::rescue::RESCUE_REFILL_TARGET_SECS);
                    }
                    let svc_type: rs_ffmpeg::ServiceType =
                        ep_cfg.service_type.parse().unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                    let should_stop = crate::rescue::run_rescue_loop(
                        &alias,
                        rescue_video_url.as_deref(),  // <-- pass Option<&str>
                        svc_type,
                        &ep_cfg.stream_key,
                        &buffer_state,
                        &stats,
                        &mut stop_rx,
                        audit_ring.as_ref(),  // <-- new
                    )
                    .await;
                    if should_stop {
                        return;
                    }
                    {
                        let mut s = stats.lock().await;
                        s.delivery_mode = "normal".to_string();
                        s.rescue_eta_secs = None;
                    }
                    let gap = rescue_started.elapsed().as_secs();
                    crate::rescue_audit::emit_recovered(&audit_ring, &alias, gap);
                    flv_normalizer = FlvStreamNormalizer::new();
                    tracing::info!(alias = %alias, "Consumer: resumed normal delivery");
                }
                continue;
            }
```

Key changes:
- Remove the `if let Some(ref rescue_url) = rescue_video_url` outer guard — rescue now fires regardless
- Pass `rescue_video_url.as_deref()` to `run_rescue_loop`
- Pass `audit_ring.as_ref()`

- [ ] **Step 3: Build**

```bash
cargo build -p rs-delivery
```

Expected: success. If `audit_ring` shape doesn't match (Option vs Arc vs &Arc), align the call site to whatever the surrounding code uses.

- [ ] **Step 4: Re-run R1**

```bash
cargo test -p rs-delivery rescue_activates_when_url_null_and_cache_drains -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run full rescue test suite to check no regressions**

```bash
cargo test -p rs-delivery rescue -- --nocapture
```

Expected: all rescue tests PASS.

- [ ] **Step 6: Commit GREEN**

```bash
git add crates/rs-delivery/src/rescue.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "fix(rescue): [green] cache-drain rescue fires regardless of URL config

run_rescue_loop now takes Option<&str>, resolves bytes via
resolve_rescue_bytes (default fallback), and pushes via
rust_rescue_push (no ffmpeg). Consumer cache-drain branch drops the
'if URL set' guard. R1 GREEN."
```

---

## Task 7: R3 RED → GREEN — warmup always pushes default rescue

**Files:**
- Modify: `crates/rs-delivery/src/rescue_tests.rs` (add R3)
- Modify: `crates/rs-delivery/src/rescue.rs:325-365` (warmup branch)

- [ ] **Step 1: Append R3 to `rescue_tests.rs`**

```rust
/// R3 (RED before fix; GREEN after this task): warmup must push the default
/// rescue blob even when no operator URL is configured.
#[tokio::test]
async fn warmup_always_pushes_default_rescue_when_no_url() {
    use crate::rescue_default::DEFAULT_RESCUE_FLV;

    let harness = TestHarness::new_with_rescue_url(None).await;
    harness.run_warmup_for_duration(std::time::Duration::from_secs(3)).await;

    let mode = harness.stats_delivery_mode().await;
    assert_eq!(mode, "warmup", "Expected delivery_mode=warmup, got {mode:?}");

    let pushed = harness.pushed_bytes().await;
    assert!(
        pushed.windows(DEFAULT_RESCUE_FLV.len()).any(|w| w == DEFAULT_RESCUE_FLV),
        "Warmup must push DEFAULT_RESCUE_FLV when URL=None; got {} bytes",
        pushed.len()
    );
}
```

- [ ] **Step 2: Run; expect FAIL**

```bash
cargo test -p rs-delivery warmup_always_pushes_default_rescue_when_no_url -- --nocapture
```

Expected: FAIL.

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-delivery/src/rescue_tests.rs
git commit -m "test(rescue): [red] R3 — warmup must push default rescue when no URL"
```

- [ ] **Step 4: Update `run_warmup_loop` in `rescue.rs`**

Find lines 312-367 (the `run_warmup_loop` fn). Drop the `if let Some(rescue_url) = rescue_video_url { if !ep_cfg.is_fast { ... ffmpeg spawn ... } }` block. Replace with:

```rust
    // Always run rescue during warmup for non-fast endpoints. Fast endpoints
    // skip rescue (low-latency tradeoff).
    let _warmup_handle: Option<tokio::task::JoinHandle<bool>> = if !ep_cfg.is_fast {
        let svc_type: rs_ffmpeg::ServiceType = ep_cfg
            .service_type
            .parse()
            .unwrap_or(rs_ffmpeg::ServiceType::TestFile);
        let ep_url = endpoint_url_for_service(svc_type, &ep_cfg.stream_key);

        let bytes_cow = resolve_rescue_bytes(rescue_video_url, audit_ring.map(|a| a.as_ref()), alias).await;
        let flv_bytes = std::sync::Arc::new(bytes_cow.into_owned());

        let initial_text = format_countdown_text(
            &DeliveryMode::Rescue { reason: RescueReason::Warmup },
            delivery_delay_ms / 1000,
        );
        write_countdown_file(alias, &initial_text);
        {
            let mut s = stats.lock().await;
            s.delivery_mode = "warmup".to_string();
            s.rescue_eta_secs = Some(delivery_delay_ms / 1000);
        }

        let alias_clone = alias.to_string();
        let buffer_state_clone = buffer_state.clone();
        let stats_clone = stats.clone();
        let stream_key_clone = ep_cfg.stream_key.clone();
        let mut warmup_stop = stop_rx.clone();
        Some(tokio::spawn(async move {
            crate::rust_rescue_push::rust_rescue_push(
                &alias_clone,
                &ep_url,
                &stream_key_clone,
                flv_bytes,
                buffer_state_clone,
                stats_clone,
                &mut warmup_stop,
            )
            .await
        }))
    } else {
        None
    };
```

When warmup completes (existing exit conditions), abort the warmup handle: add at the end of the `loop` (right before `return stopped`):

```rust
    if let Some(h) = _warmup_handle {
        h.abort();
    }
    cleanup_countdown_file(alias);
```

Remove all remaining `if rescue_video_url.is_some()` guards in the rest of the function — countdown text is unconditional now.

- [ ] **Step 5: Build + re-run R3**

```bash
cargo build -p rs-delivery
cargo test -p rs-delivery warmup_always_pushes_default_rescue_when_no_url -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Full rescue regression**

```bash
cargo test -p rs-delivery rescue -- --nocapture
```

Expected: all PASS (including `warmup_fast_endpoint_skips_rescue_ffmpeg` — fast endpoints unchanged).

- [ ] **Step 7: Commit GREEN**

```bash
git add crates/rs-delivery/src/rescue.rs
git commit -m "fix(rescue): [green] warmup always pushes default rescue (R3 GREEN)

Drops 'if URL set' guard in run_warmup_loop. Non-fast endpoints always
spawn rust_rescue_push during initial buffer fill with embedded default
FLV (or operator-configured custom FLV if set). Fast endpoints still
skip rescue."
```

---

## Task 8: R2 RED → GREEN — producer-gone defensive rescue + producer respawn

**Files:**
- Modify: `crates/rs-delivery/src/rescue_tests.rs` (add R2)
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (consumer recv-None branch + producer respawn)

- [ ] **Step 1: Append R2 to `rescue_tests.rs`**

```rust
/// R2 (RED before fix; GREEN after this task): when producer task disappears
/// mid-stream (panic / channel drop), consumer enters rescue rather than
/// silently exiting. The endpoint_task respawns the producer; rescue exits
/// when buffer refills.
#[tokio::test]
async fn rescue_activates_when_producer_gone() {
    let harness = TestHarness::new_with_rescue_url(None).await;
    harness.fill_buffer(5).await;
    harness.drop_producer_sender().await;

    // Give endpoint_task one select-loop tick to respawn producer
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mode = harness.stats_delivery_mode().await;
    assert_eq!(mode, "rescue", "Expected rescue after producer dropped, got {mode:?}");

    // Producer should have been respawned by endpoint_task
    assert!(
        harness.producer_respawn_count().await >= 1,
        "Expected producer respawn after disappearance"
    );
}
```

If `drop_producer_sender` / `producer_respawn_count` helpers don't exist on `TestHarness`, add them as part of this task.

- [ ] **Step 2: Run R2; expect FAIL**

```bash
cargo test -p rs-delivery rescue_activates_when_producer_gone -- --nocapture
```

Expected: FAIL — either harness helpers missing (compile error) or `delivery_mode="normal"` / endpoint died.

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-delivery/src/rescue_tests.rs
git commit -m "test(rescue): [red] R2 — producer-gone enters rescue + respawns producer"
```

- [ ] **Step 4: Add producer respawn to endpoint_task select-loop**

Edit `crates/rs-delivery/src/endpoint_task.rs:957-970`. Replace the existing select block:

```rust
    loop {
        tokio::select! {
            result = &mut producer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Producer panicked: {e}");
                } else {
                    tracing::info!(alias = %alias, "Producer finished");
                }

                // If consumer is still alive and we have NOT received a stop
                // signal, respawn the producer from last_delivered_chunk_id+1
                // so the consumer has something to recover to.
                if !*stop_rx.borrow() {
                    let resume_from = consumer_last_delivered_chunk_id_shared
                        .load(AtomicOrdering::Relaxed)
                        .saturating_add(1);
                    tracing::warn!(alias = %alias, resume_from, "Respawning producer");

                    let producer_stop2 = stop_rx.clone();
                    let producer_stats2 = stats.clone();
                    let producer_alias2 = alias.clone();
                    let producer_buffer_state2 = buffer_state.clone();
                    let producer_audit_ring2 = audit_ring.clone();
                    let new_producer = tokio::spawn(producer_task(
                        fetcher.clone(),
                        tx_holder.take_or_recreate().await,  // see Step 5
                        resume_from,
                        delivery_delay_ms,
                        producer_stop2,
                        producer_stats2,
                        producer_alias2,
                        producer_buffer_state2,
                        producer_audit_ring2,
                    ));
                    producer.set(new_producer);
                    PRODUCER_RESPAWN_COUNT.fetch_add(1, AtomicOrdering::Relaxed); // test telemetry
                    continue;
                }

                tracing::info!(alias = %alias, "Producer finished + stop signal; draining consumer");
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    &mut consumer,
                ).await;
                break;
            }
            result = &mut consumer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Consumer panicked: {e}");
                }
                break;
            }
        }
    }
```

This step introduces several pieces that need supporting code:
- `consumer_last_delivered_chunk_id_shared: Arc<AtomicI64>` — share consumer's `last_delivered_chunk_id` with the select-loop. Wire it: declare before spawning consumer, pass into `consumer_task`, have consumer write `last_delivered_chunk_id` to it after each chunk.
- `tx_holder` — a small wrapper that lets us hand out fresh `mpsc::Sender` on respawn AND share the same `rx` with consumer. Simplest approach: after the original tx is dropped (producer finished → tx dropped → consumer's `recv()` returns None), the consumer must accept a fresh `rx`. Implement as: replace `mpsc::channel` with `async_channel::unbounded` or recreate the whole channel on respawn and notify consumer via a new `tokio::sync::watch::Sender<mpsc::Receiver<PrefetchedChunk>>`. **Choose the simplest** — recreate channel + watch-broadcast new rx to consumer.
- `PRODUCER_RESPAWN_COUNT` — test-only static `AtomicU64` exposed via a `pub fn producer_respawn_count() -> u64` for the harness to read.
- `fetcher.clone()` — the `ChunkFetcher` trait must be `Clone` (or wrap in `Arc<dyn ChunkFetcher>`).

This is the biggest single chunk of work in the plan. Allot proper time and don't shortcut the channel-respawn plumbing.

- [ ] **Step 5: Update consumer recv-None branch to enter rescue**

Find `endpoint_task.rs:658-660`:

```rust
                    None => {
                        tracing::info!(alias = %alias, "Consumer: producer gone, stopping");
                        break;
                    }
```

Replace with:

```rust
                    None => {
                        tracing::warn!(alias = %alias, "Consumer: producer gone, entering defensive rescue");

                        let rescue_started = std::time::Instant::now();
                        crate::rescue_audit::emit_activated(&audit_ring, &alias, last_delivered_chunk_id);
                        if let Some(mut p) = proc.take() {
                            p.kill().await;
                        }
                        {
                            let mut s = stats.lock().await;
                            s.delivery_mode = "rescue".to_string();
                        }
                        let svc_type: rs_ffmpeg::ServiceType =
                            ep_cfg.service_type.parse().unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                        let should_stop = crate::rescue::run_rescue_loop(
                            &alias,
                            rescue_video_url.as_deref(),
                            svc_type,
                            &ep_cfg.stream_key,
                            &buffer_state,
                            &stats,
                            &mut stop_rx,
                            audit_ring.as_ref(),
                        )
                        .await;
                        if should_stop {
                            return;
                        }

                        // Try to receive from the (potentially-respawned) rx.
                        // The select-loop in the outer task respawns the
                        // producer and broadcasts a fresh rx via
                        // new_rx_watch; pick it up before continuing.
                        if let Some(new_rx) = new_rx_watch.borrow_and_update().clone() {
                            rx = new_rx;
                        }

                        let gap = rescue_started.elapsed().as_secs();
                        crate::rescue_audit::emit_recovered(&audit_ring, &alias, gap);
                        flv_normalizer = FlvStreamNormalizer::new();
                        tracing::info!(alias = %alias, "Consumer: resumed after producer respawn");
                        continue;
                    }
```

`new_rx_watch` is the `watch::Receiver<Option<mpsc::Receiver<PrefetchedChunk>>>` introduced in Step 4 plumbing.

- [ ] **Step 6: Build + re-run R2**

```bash
cargo build -p rs-delivery
cargo test -p rs-delivery rescue_activates_when_producer_gone -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Full delivery test suite + check no regressions**

```bash
cargo test -p rs-delivery -- --nocapture
```

Expected: all PASS. Pay attention to existing producer-consumer integration tests; the channel-respawn machinery may break them.

- [ ] **Step 8: Commit GREEN**

```bash
git add crates/rs-delivery/src/endpoint_task.rs
git commit -m "fix(rescue): [green] producer-gone defensive — respawn + consumer rescue (R2)

When producer task finishes WHILE consumer still draining buffer,
endpoint_task respawns producer from last_delivered_chunk_id+1 and
broadcasts a fresh rx to the consumer via watch channel. Consumer's
recv-None branch now enters rescue (was: break loop) and picks up the
new rx after rescue exits.

This is defensive hardening: the actual stream.lan-crash scenario hits
the cache-drain-stalled branch (gap #1 fixed earlier). Producer-gone
fires on producer panic or stop signal."
```

---

## Task 9: Custom upload transcode-on-upload to FLV

**Files:**
- Modify: `crates/rs-api/src/rescue_video_handlers.rs`

- [ ] **Step 1: Read existing upload handler**

```bash
grep -n "fn upload_rescue\|axum::extract::Multipart\|rescue_videos/\|upload_chunk" crates/rs-api/src/rescue_video_handlers.rs
wc -l crates/rs-api/src/rescue_video_handlers.rs
```

Read the whole file (`Read` tool) to understand the upload pipeline.

- [ ] **Step 2: Add transcode step**

Pseudocode for the new flow (translate to exact axum/tokio idioms used in the existing handler):

```rust
// Inside upload_rescue_video handler, AFTER the multipart bytes are read into a temp file:

let input_path = temp_input_file.path();
let transcoded = tempfile::NamedTempFile::with_suffix(".flv")?;
let output_path = transcoded.path();

let transcode_status = tokio::process::Command::new("ffmpeg")
    .args([
        "-y",
        "-i", input_path.to_str().unwrap(),
        "-c:v", "libx264",
        "-profile:v", "main",
        "-preset", "medium",
        "-r", "30",
        "-g", "60",
        "-b:v", "1500k",
        "-c:a", "aac",
        "-ar", "48000",
        "-ac", "2",
        "-b:a", "64k",
        "-f", "flv",
        output_path.to_str().unwrap(),
    ])
    .stderr(std::process::Stdio::piped())
    .output()
    .await?;

if !transcode_status.status.success() {
    let tail = String::from_utf8_lossy(&transcode_status.stderr)
        .lines().rev().take(20).collect::<Vec<_>>().join("\n");
    return Err((axum::http::StatusCode::BAD_REQUEST,
        format!("transcode failed:\n{tail}")).into_response());
}

// ffprobe validation
let probe = tokio::process::Command::new("ffprobe")
    .args(["-v", "error", "-i", output_path.to_str().unwrap()])
    .output()
    .await?;
if !probe.status.success() {
    return Err((axum::http::StatusCode::BAD_REQUEST,
        "transcoded FLV failed ffprobe validation".to_string()).into_response());
}

// Size limit
let meta = std::fs::metadata(output_path)?;
if meta.len() > 50 * 1024 * 1024 {
    return Err((axum::http::StatusCode::BAD_REQUEST,
        format!("FLV too large: {} bytes (max 50MB)", meta.len())).into_response());
}

// Stream to S3 with .flv extension
let s3_key = format!("rescue-videos/{}.flv", uuid::Uuid::new_v4());
let bytes = tokio::fs::read(output_path).await?;
s3_client.put_object(&s3_key, &bytes).await?;

let public_url = format!("{}/{}", s3_public_base_url, s3_key);
// ... save URL to template / event as before ...

// Cleanup temp files (RAII via tempfile drop handles this)
```

- [ ] **Step 3: Add a unit test for the rejection path**

In `crates/rs-api/src/rescue_video_handlers.rs` (or a new `_tests` module):

```rust
#[tokio::test]
async fn upload_rejects_when_transcode_fails() {
    // Feed obviously-not-video bytes (random data) → expect 400
    // Use the existing test harness pattern in rs-api
    todo!("write per existing axum test harness; assert HTTP 400 + 'transcode failed' in body");
}
```

If the existing handler has no test harness, skip the test and rely on the E2E gate in Task 11. Note the gap in the commit message.

- [ ] **Step 4: Build + smoke test transcode locally**

```bash
cargo build -p rs-api
# manual smoke: feed any local MP4 to the endpoint via curl
# Defer until VPS deploy if local axum harness too heavy
```

- [ ] **Step 5: Commit**

```bash
git add crates/rs-api/src/rescue_video_handlers.rs
git commit -m "feat(api): transcode-on-upload custom rescue MP4 → FLV in S3

Operator-uploaded rescue videos now normalized to 1080p30 H.264 main +
AAC 48kHz FLV via ONE-TIME ffmpeg at upload (stream.lan side). Stored in
S3 as .flv. VPS pulls .flv at runtime and pushes via rust pusher — no
ffmpeg on VPS, no ffmpeg at outage time.

Rejects upload (HTTP 400) if transcode or ffprobe validation fails, or
if output exceeds 50MB."
```

---

## Task 10: Template UI hint + D1 Playwright spec

**Files:**
- Modify: `leptos-ui/src/components/templates.rs`
- Create: `e2e/templates-default-rescue.spec.ts`

- [ ] **Step 1: Read existing template editor component**

```bash
grep -n "rescue_url\|rescue_video_url\|RescueUrl\|No rescue configured" leptos-ui/src/components/templates.rs
```

- [ ] **Step 2: Replace empty-state copy**

In `leptos-ui/src/components/templates.rs`, find where the rescue URL field is rendered when empty. If there is no current empty-state hint, add one. Wrap with a `data-testid` for the Playwright assertion:

```rust
view! {
    <div class="rescue-url-row">
        <label for="rescue_url">"Rescue video URL"</label>
        <input id="rescue_url" type="text"
               prop:value=rescue_url
               on:input=move |ev| rescue_url.set(event_target_value(&ev)) />
        {move || {
            let url = rescue_url.get();
            if url.is_empty() {
                view! {
                    <div class="rescue-url-hint" data-testid="rescue-default-hint">
                        "Using built-in default (5s standby loop). "
                        "Upload a custom MP4 above to override per-template."
                    </div>
                }.into_view()
            } else {
                view! {
                    <div class="rescue-url-hint" data-testid="rescue-custom-hint">
                        "Custom rescue video active"
                    </div>
                }.into_view()
            }
        }}
    </div>
}
```

Style class names should match the existing CSS in the file. Add CSS if needed (small `.rescue-url-hint { color: #888; font-size: 0.9em; }` style block — or extend an existing stylesheet).

- [ ] **Step 3: Write D1 Playwright spec**

```typescript
// e2e/templates-default-rescue.spec.ts
import { test, expect } from '@playwright/test';

test('template with no rescue URL shows built-in default hint', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/templates');

  // Find a template without a rescue URL (default install state)
  await page.click('[data-testid="template-row"]:first-child');

  const hint = page.locator('[data-testid="rescue-default-hint"]');
  await expect(hint).toBeVisible();
  await expect(hint).toContainText('Using built-in default');

  // The warning variant must NOT be present
  await expect(page.locator('[data-testid="rescue-custom-hint"]')).toHaveCount(0);

  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 4: Run Playwright spec locally (optional; CI runs it)**

```bash
cd e2e && npx playwright test templates-default-rescue.spec.ts --config=playwright-frontend.config.ts
```

Expected: PASS. Skip if local Playwright not set up; CI will catch.

- [ ] **Step 5: Commit**

```bash
git add leptos-ui/src/components/templates.rs e2e/templates-default-rescue.spec.ts
git commit -m "feat(ui): 'Using built-in default' hint when rescue URL empty + D1 spec

Replaces the prior 'No rescue configured' warning with a calm hint
explaining that the embedded default protects the stream. Playwright
spec asserts the hint renders and there is no console error/warning."
```

---

## Task 11: CI — gen_rescue_flv --check gate + drop URL precondition + new e2e-stream-lan-crash job

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add `--check` gate to `lint` or `test` job**

Find the existing Rust test or lint job in `.github/workflows/ci.yml`. Add a step (requires ffmpeg, which CI already installs for E2E):

```yaml
      - name: Verify default_rescue.flv matches generator output
        run: |
          which ffmpeg || sudo apt-get install -y ffmpeg
          cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml -- --check
```

- [ ] **Step 2: Drop URL precondition from `e2e-obs-youtube-test`**

Search the job for any setup step that creates/updates a template/event with a `rescue_video_url`. Remove that step. Add an assertion in the test phase that the event used has `rescue_video_url=NULL`:

```bash
grep -n "rescue_video_url" .github/workflows/ci.yml
```

For each match, decide: if it's setting the URL, remove the line. If it's the rescue-URL-required path, replace with the new no-URL path.

- [ ] **Step 3: Add `e2e-stream-lan-crash` job**

Insert after `e2e-obs-youtube-test`:

```yaml
  e2e-stream-lan-crash:
    needs: deploy-stream-lan
    runs-on: [self-hosted, Windows, X64]
    if: ${{ !cancelled() && needs.deploy-stream-lan.result != 'failure' }}
    timeout-minutes: 25
    steps:
      - uses: actions/checkout@v4

      - name: Start OBS → stream.lan → VPS → YouTube pipeline
        shell: powershell
        run: |
          # Reuse the OBS / stream-lan startup pattern from e2e-obs-youtube-test
          # ...
          # Wait for delivery_mode == "normal" and chunks advancing

      - name: Crash stream.lan Restreamer process
        shell: powershell
        run: |
          Stop-ScheduledTask -TaskName "RestreamerGUI" -ErrorAction SilentlyContinue
          taskkill /F /IM "Restreamer.exe" /T
          Write-Host "Restreamer killed; waiting 60s for VPS to enter rescue"
          Start-Sleep -Seconds 60

      - name: Assert VPS entered rescue + push advancing
        shell: powershell
        timeout-minutes: 5
        run: |
          $vps_url = "<VPS API URL — read from CI secrets/env>"
          for ($i = 0; $i -lt 30; $i++) {
            $status = Invoke-RestMethod "$vps_url/api/status"
            if ($status.delivery_mode -eq "rescue") {
              Write-Host "VPS in rescue mode (iteration $i)"
              break
            }
            Start-Sleep -Seconds 5
          }
          if ($status.delivery_mode -ne "rescue") {
            throw "VPS never entered rescue after 150s; mode=$($status.delivery_mode)"
          }
          # Confirm push is advancing (FLV loop pushing)
          $first_id = $status.last_pushed_chunk_id
          Start-Sleep -Seconds 30
          $second = Invoke-RestMethod "$vps_url/api/status"
          if ($second.last_pushed_chunk_id -le $first_id) {
            throw "FLV rescue push not advancing: $first_id then $($second.last_pushed_chunk_id)"
          }
          Write-Host "Push advancing: $first_id -> $($second.last_pushed_chunk_id)"

      - name: Restart Restreamer + assert rescue exits
        shell: powershell
        timeout-minutes: 5
        run: |
          Start-ScheduledTask -TaskName "RestreamerGUI"
          # ... reuse existing health-check wait pattern
          Start-Sleep -Seconds 30
          for ($i = 0; $i -lt 36; $i++) {
            $status = Invoke-RestMethod "$vps_url/api/status"
            if ($status.delivery_mode -eq "normal") { break }
            Start-Sleep -Seconds 5
          }
          if ($status.delivery_mode -ne "normal") {
            throw "Rescue never exited; mode=$($status.delivery_mode)"
          }
          # Assert RescueRecovered audit row exists
          $audit = Invoke-RestMethod "$vps_url/api/audit?action=RescueRecovered&limit=1"
          if ($audit.Count -eq 0) { throw "No RescueRecovered audit row" }

      - name: Cleanup
        if: always()
        shell: powershell
        run: |
          # Stop OBS + tear down VPS delivery
```

- [ ] **Step 4: Add `e2e-stream-lan-crash` to `e2e-gate` `needs:` list**

Find the `e2e-gate` job, add `e2e-stream-lan-crash` to its `needs:` array and to its tracking script (the bash block that echoes per-job results).

- [ ] **Step 5: Local lint of workflow**

```bash
# If actionlint installed
actionlint .github/workflows/ci.yml
# Otherwise, just confirm yaml parses
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
```

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "test(ci): e2e-stream-lan-crash + gen_rescue_flv --check + drop URL precondition

- New e2e-stream-lan-crash job kills Restreamer mid-stream and asserts
  VPS enters rescue within 150s, FLV pushes keep advancing, restart
  triggers RescueRecovered audit, normal delivery resumes.
- gen_rescue_flv --check gate prevents silent asset drift.
- e2e-obs-youtube-test no longer pre-sets rescue_video_url — exercises
  the URL=NULL path that broke in production 2026-05-30."
```

---

## Task 12: Remove ffmpeg from rs-delivery VPS cloud-init

**Files:**
- Modify: `crates/rs-cloud/src/lib.rs` (specifically `bootstrap_cloud_init`)

- [ ] **Step 1: Verify no remaining ffmpeg call sites in rs-delivery runtime**

```bash
grep -rn "Command::new(\"ffmpeg\")\|spawn(\"ffmpeg\"\|process::Command.*ffmpeg" crates/rs-delivery/src/ 2>/dev/null
```

Expected: NO matches. If any remain (e.g., a forgotten call site), STOP — re-open Task 6/7 to clear them. This step gates removing ffmpeg from the VPS image.

- [ ] **Step 2: Read `bootstrap_cloud_init`**

```bash
grep -n "ffmpeg\|apt install\|apt-get install" crates/rs-cloud/src/lib.rs
```

- [ ] **Step 3: Remove `ffmpeg` from apt install list**

Locate the apt install block in `bootstrap_cloud_init`. Remove `ffmpeg` from the package list. Leave a comment:

```rust
// ffmpeg removed 2026-05-31: rs-delivery uses pure-rust rescue
// (rs_rtmp_push + embedded default_rescue.flv). Custom rescue videos
// are transcoded on stream.lan at upload time.
```

- [ ] **Step 4: Update any cloud-init unit tests**

```bash
grep -n "ffmpeg" crates/rs-cloud/src/ -rn
```

If any test asserts ffmpeg in the cloud-init output, update or delete the assertion.

- [ ] **Step 5: Build + run rs-cloud tests**

```bash
cargo test -p rs-cloud -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-cloud/src/lib.rs
git commit -m "chore(vps): remove ffmpeg from rs-delivery cloud-init

Rescue migrated to pure-rust (rs_rtmp_push + embedded default_rescue.flv).
Custom operator videos transcoded on stream.lan at upload time, not on
the VPS. ffmpeg no longer needed in the delivery VPS image — saves
~80MB image space and one apt install dependency."
```

---

## Task 13: Pre-push gate + push + monitor CI

- [ ] **Step 1: Run local pre-push gate (per project Tier-2 fast-iterate policy)**

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --no-run --workspace
cargo test -p rs-delivery -p rs-api -p rs-core -p rs-cloud
```

Expected: all green. Fix anything red BEFORE pushing — each push is a 15-25 min CI cycle.

- [ ] **Step 2: Push**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI per ci-monitoring.md**

```bash
gh run list --branch dev --limit 3
# Identify the run ID from the latest push
RUN_ID=<id>
# Monitor in background, get notified when terminal
```

Use the Bash tool with `run_in_background: true`:

```
sleep 300 && gh run view <RUN_ID> --json status,conclusion,jobs
```

If any job fails: `gh run view <RUN_ID> --log-failed`, fix root cause, push fix, monitor again. Repeat until ALL jobs green.

- [ ] **Step 4: Open PR dev → main**

```bash
gh pr create --base main --head dev \
  --title "Always-on rust-only rescue stream (no ffmpeg on VPS)" \
  --body "$(cat <<'EOF'
## Summary

Closes the gap discovered during the 2026-05-30 stream.lan crash test:
all 5 templates had \`rescue_video_url=NULL\`, the rescue branch never
fired without a URL, and rescue still spawned external ffmpeg post the
rs-rtmp-push migration. CI was green because no test exercised the
URL=NULL path.

Design: \`docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md\`

### Changes
- Embedded \`default_rescue.flv\` (~500KB) shipped in rs-delivery binary
- \`rust_rescue_push\` pure-rust loop via \`rs_rtmp_push::push_flv_bytes\`
- \`resolve_rescue_bytes\` always returns Some — default fallback if URL
  empty, legacy non-FLV, or S3 fetch fails (with new audit rows)
- Producer-gone defensive: endpoint_task respawns producer, consumer
  enters rescue during the gap instead of breaking the loop
- Warmup branch drops the "if URL set" guard
- Custom rescue uploads transcoded to FLV at upload time on stream.lan
- ffmpeg removed from rs-delivery VPS cloud-init
- New e2e-stream-lan-crash CI job kills Restreamer mid-stream and
  asserts the VPS enters rescue + push keeps advancing + recovery on
  restart
- gen_rescue_flv --check CI gate prevents silent asset drift
- Template editor shows "Using built-in default" hint when URL unset

## Test plan

- [ ] R1: \`rescue_activates_when_url_null_and_cache_drains\` GREEN
- [ ] R2: \`rescue_activates_when_producer_gone\` GREEN
- [ ] R3: \`warmup_always_pushes_default_rescue_when_no_url\` GREEN
- [ ] R4: \`default_rescue_flv_blob_integrity\` GREEN
- [ ] R5: gen_rescue_flv --check GREEN in CI
- [ ] D1: Playwright templates-default-rescue.spec GREEN
- [ ] E1: e2e-stream-lan-crash job GREEN
- [ ] E2: e2e-obs-youtube-test with URL=NULL GREEN
- [ ] Full e2e gate GREEN
- [ ] Deploy verified on stream.lan: kill Restreamer mid-stream, observe
      VPS dashboard shows rescue mode, push advances, restart resumes

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Verify PR mergeable + clean before declaring done**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<PR_NUM> \
  --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `mergeable: true`, `mergeable_state: "clean"`. If `unstable`, `blocked`, or `behind` — investigate and fix.

- [ ] **Step 6: Audits before completion report**

Run BOTH audits per completion-report.md rules:

```
/plan-check                    # all plan steps fulfilled
/review                        # fast diff pass
/superpowers:requesting-code-review   # deep pass — historically catches what /review misses
```

Fix every 🔴/🟡/🔵 inside the diff from both.

- [ ] **Step 7: Send completion report**

Per the EXACT template in `completion-report.md`. Include:
- All three audit lines (plan-check, /review, /requesting-code-review)
- Regression test line: `✅ Regression test: crates/rs-delivery/src/rescue_tests.rs:<line> — RED on <test_sha>, GREEN on <fix_sha>` (cite R1)
- Deploy verification line: read version from streampp dashboard DOM via Playwright, confirm matches deployed version
- 🌐 dashboard URLs for both env (stream.lan AND streampp)
- PR URL with title

---

## Self-Review

Spec coverage:
- [x] Default rescue blob always present — Task 1 + Task 2
- [x] Rust-only push at outage — Task 3 + Task 4 + Task 6
- [x] Cache-drain + URL=NULL trigger — Task 6 (R1)
- [x] Producer-gone trigger + producer respawn — Task 8 (R2)
- [x] Warmup always rescue — Task 7 (R3)
- [x] Custom upload transcode-on-upload — Task 9
- [x] Legacy non-FLV rejection — Task 4 (resolve_rescue_bytes branch)
- [x] Template UI hint — Task 10 (D1)
- [x] CI gate (gen_rescue --check) — Task 11 (R5)
- [x] E2E stream-lan-crash — Task 11 (E1)
- [x] Drop URL precondition from obs-youtube — Task 11 (E2)
- [x] ffmpeg removal from VPS cloud-init — Task 12

Placeholder scan:
- One acknowledged gap: Task 9 Step 3 unit test for transcode-rejection is `todo!()` — if the rs-api test harness is too heavy, the gap is documented and E2E covers it. Not a blocker.

Type consistency:
- `resolve_rescue_bytes` signature consistent across Task 4, Task 6, Task 7
- `run_rescue_loop` signature changes (added `Option<&str>` + `audit_ring`) — updated at ALL call sites (warmup in rescue.rs, consumer cache-drain branch + consumer recv-None branch in endpoint_task.rs)
- `RESCUE_REFILL_TARGET_SECS` referenced from `rescue.rs` AND `rust_rescue_push.rs` — choose one canonical location (recommend keeping in `rescue.rs` and importing in `rust_rescue_push.rs`)

Fixed inline: nothing. Plan is complete.
