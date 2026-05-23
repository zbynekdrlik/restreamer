# Outage Survival Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Restreamer survive any-duration outage of everything downstream of OBS+stream.lan with zero chunk loss, a calm operator UX, a complete audit timeline, and a CI test that locks it — fixing the 2026-05-22 event failure where a >10-min outage permanently dropped buffered chunks and forced a recreate-from-zero.

**Architecture:** Five pillars, ONE PR. (1) Uploader never abandons network-class chunks — retry forever, capped backoff; only structural rejects (403/404) abandon after a small budget; add a disk-pressure monitor (alert-only). (2) VPS replays cleanly — verified no-give-up + no live-edge jump for continuity endpoints. (3) Audit completeness — wire the 7 defined-but-dead disk-cache events + 2 new rescue events + RtmpHandshakeFailed. (4) Operator UX — host-computed per-endpoint `EndpointLifecycle` (green/blue/red) + calm outage banner + error-string hygiene. (5) TDD/CI/E2E — RED-first unit tests for the pure cores, a mock-S3 integration test, and an extended real outage step in `e2e-obs-youtube` (block S3 5 min → zero drops + rescue audit + blue banner via Playwright → drain → back to Live).

**Tech Stack:** Rust (rs-endpoint, rs-core, rs-delivery, rs-api), Leptos CSR (WASM), sqlx/SQLite, Playwright, GitHub Actions self-hosted runner (stream.lan).

**Spec:** `docs/superpowers/specs/2026-05-23-outage-survival-hardening-design.md` (commit `d2a53d3d`).

---

## Execution constraints (apply to EVERY task)

- **TDD strict:** RED commit BEFORE GREEN commit, one commit per task, visible in `git log --oneline`. The never-drop fix is a defect-class fix → its regression test MUST be RED on the unfixed code first.
- **Tier-2 fast-iterate:** subagents do NOT run `cargo build`/`cargo test`/`cargo clippy`. The CONTROLLER runs `cargo fmt --all --check` + `cargo check --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --no-run --workspace` between task batches and before push. Where a task says "verify it fails/passes", the controller runs the named test (`cargo test -p <crate> <name>`) — subagents just write code and commit.
- **Read before Edit:** every task re-verifies exact file:line with `Read` before `Edit` (line numbers below are from commit `d2a53d3d` and may drift as earlier tasks land).
- **File-size cap** <1000 lines per `.rs`. **ASCII-only** PowerShell strings in CI YAML.
- **Commit message footer** on every commit:
  ```
  Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
  ```
- The FINAL task (T21) is ORCHESTRATOR-ONLY: file tracking issues, controller compile/lint, push, monitor CI, PR, post-deploy verify on streamsnv + streampp via `win-*` MCP, completion report.

---

## File Structure (what each task touches)

| File | Responsibility | Tasks |
|---|---|---|
| `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`, `Cargo.lock` | version | T0 |
| `crates/rs-endpoint/src/uploader.rs` | never-drop decision + disk monitor spawn | T1, T2, T4 |
| `crates/rs-endpoint/tests/upload_retention.rs` | mock-S3 integration test | T3 |
| `crates/rs-endpoint/src/disk_pressure.rs` (new) | free-space classifier + monitor | T4 |
| `crates/rs-endpoint/Cargo.toml` | add `sysinfo` direct dep | T4 |
| `crates/rs-core/src/audit.rs` | Action enum: add Rescue*, LocalDiskPressure | T5 |
| `crates/rs-delivery/src/endpoint_task.rs` | rescue enter/recover audit; verify no give-up | T6, T7, T14 |
| `crates/rs-delivery/src/disk_cache_fetcher.rs` | stall-timeout + reader-recovered audit | T8, T9 |
| `crates/rs-delivery/src/disk_cache/download_service.rs`, `download_service.rs`, `disk_cache/eviction.rs` | write-failed, throttled, evicted audit | T8, T10 |
| `crates/rs-delivery/src/disk_cache_fetcher.rs` / `disk_cache/mod.rs` | prefill started/ready audit | T11 |
| `crates/rs-delivery/src/endpoint_consumer_helpers.rs` | RtmpHandshakeFailed audit | T12 |
| `crates/rs-api/src/delivery_status.rs` | compute `EndpointLifecycle` | T15, T16 |
| `crates/rs-core/src/models.rs` | `EndpointLifecycle` enum + field on `DeliveryEndpointMetrics` | T15, T16 |
| `leptos-ui/src/ws.rs`, `src/store.rs` | mirror lifecycle field | T17 |
| `leptos-ui/src/components/operator_dashboard.rs`, `style.css` | render lifecycle + fix CSS gap + error hygiene | T17 |
| `leptos-ui/src/components/outage_banner.rs` (new) | calm outage banner | T18 |
| `e2e/frontend-*.spec.ts` | Playwright lifecycle/banner test | T19 |
| `.github/workflows/ci.yml` | extend e2e-obs-youtube outage step | T20 |

---

## Task 0: Version bump 0.19.1 → 0.20.0

**Files:** `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`, `Cargo.lock`

- [ ] **Step 1: Bump all four version files** — `Read` each, then `Edit` `version = "0.19.1"` → `version = "0.20.0"` (in `tauri.conf.json` it's `"version": "0.19.1"` → `"version": "0.20.0"`).

- [ ] **Step 2: Update Cargo.lock** — controller runs `cargo update -p restreamer -p rs-core -p rs-api -p rs-endpoint -p rs-delivery -p rs-inpoint -p rs-runtime -p rs-cloud -p rs-ffmpeg -p rs-youtube -p rs-ts-normalize --precise 0.20.0 2>/dev/null || true` then `cargo check --workspace` to regenerate lock entries. (Subagent: just edit the 4 version files; controller handles the lock.)

- [ ] **Step 3: Commit**
```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml Cargo.lock
git commit -m "chore: bump version to 0.20.0 (outage survival hardening)"
```

---

## Task 1: RED — `should_abandon_upload` decision tests

**Files:** Test: `crates/rs-endpoint/src/uploader.rs` (in its `#[cfg(test)] mod tests`)

The current permanent-drop logic (`uploader.rs:471`) abandons ANY error after 10 attempts or 10 min. We replace it with a pure decision function that NEVER abandons network-class errors. Write the failing test first.

- [ ] **Step 1: Add failing tests** — `Read` `crates/rs-endpoint/src/uploader.rs`, find the `#[cfg(test)] mod tests { ... }` block (search for `mod tests`), add inside it:

```rust
    #[test]
    fn network_class_errors_never_abandon_even_after_many_attempts() {
        // Continuity guarantee: a long outage must never drop a chunk.
        for class in ["timeout", "5xx", "conn", "other"] {
            assert!(
                !should_abandon_upload(class, 9_999),
                "network class {class} must retry forever, never abandon"
            );
        }
    }

    #[test]
    fn structural_reject_classes_abandon_only_after_budget() {
        // 403/404 mean S3 structurally rejected the object — retrying can
        // never succeed. Absorb a few transient auth/propagation hiccups,
        // then abandon.
        assert!(!should_abandon_upload("403", 4), "below budget: keep trying");
        assert!(should_abandon_upload("403", 5), "at budget: abandon");
        assert!(should_abandon_upload("404", 50), "above budget: abandon");
    }
```

- [ ] **Step 2: Verify it fails** — Controller runs `cargo test -p rs-endpoint should_abandon_upload`. Expected: FAIL to compile (`cannot find function should_abandon_upload`). That is the RED state.

- [ ] **Step 3: Commit**
```bash
git add crates/rs-endpoint/src/uploader.rs
git commit -m "test: RED should_abandon_upload — network classes never abandon (outage continuity)"
```

---

## Task 2: GREEN — never-drop network-class chunks

**Files:** Modify `crates/rs-endpoint/src/uploader.rs:84-85` (consts), `:467-528` (Err arm)

- [ ] **Step 1: Replace the retry-budget consts** — `Read` `uploader.rs:84-95`. Replace lines 84-85:

```rust
const MAX_ATTEMPTS: i64 = 10;
const MAX_WALL_CLOCK_MS: i64 = 600_000; // 10 min total retry budget
```
with:
```rust
/// Attempt budget for STRUCTURAL-reject classes (403/404) only. Network
/// classes (timeout/5xx/conn/other) are never abandoned — see
/// `should_abandon_upload`. This is the continuity guarantee: a long outage
/// must lose nothing while the laptop runs (#2026-05-22 event fix).
const ABANDON_ATTEMPT_BUDGET: i64 = 5;
```

- [ ] **Step 2: Add the decision function** — directly above `pub(crate) fn backoff_ms` (currently `:97`), insert:

```rust
/// Decide whether an upload error is terminal for the chunk.
///
/// Network-class errors (`timeout`/`5xx`/`conn`/`other`) are NEVER terminal:
/// the chunk stays on disk and is retried forever at capped backoff so an
/// outage of any duration loses nothing (continuity guarantee). Only
/// structural client rejects (`403`/`404`) — where retrying can never
/// succeed — abandon, and only after `ABANDON_ATTEMPT_BUDGET` attempts to
/// absorb transient auth/propagation hiccups.
fn should_abandon_upload(class: &str, attempt: i64) -> bool {
    matches!(class, "403" | "404") && attempt >= ABANDON_ATTEMPT_BUDGET
}
```

- [ ] **Step 3: Rewire the Err arm** — `Read` `uploader.rs:467-503`. Replace lines 467-471:
```rust
        Err(e) => {
            let err_msg = e.to_string();
            let wall_clock_ms = chrono::Utc::now().timestamp_millis()
                - chunk.upload_first_attempt_at.unwrap_or(now_ms);
            let permanent = attempt >= MAX_ATTEMPTS || wall_clock_ms >= MAX_WALL_CLOCK_MS;
```
with:
```rust
        Err(e) => {
            let err_msg = e.to_string();
            let class = classify_upload_error(&err_msg);
            // Continuity: network-class errors retry forever; only structural
            // rejects abandon after the budget.
            let permanent = should_abandon_upload(class, attempt);
```
Then in the audit block below, the line that currently re-computes the class (`:502 let class = classify_upload_error(&err_msg);`) is now redundant — `Read` `uploader.rs:501-503` and remove the duplicate `let class = classify_upload_error(&err_msg);` line (the `class` binding from Step 3 is already in scope).

- [ ] **Step 4: Verify** — Controller runs `cargo test -p rs-endpoint should_abandon_upload` (Expected: PASS) and `cargo check -p rs-endpoint` (no unused-var/`now_ms` warnings — if `now_ms` becomes unused elsewhere, leave it; it is still used at `:335`/`:470`-area picks — controller confirms clippy clean).

- [ ] **Step 5: Commit**
```bash
git add crates/rs-endpoint/src/uploader.rs
git commit -m "fix: never abandon network-class uploads — retry forever for outage continuity

Removes the 10-attempt / 10-min permanent-drop that lost every chunk
buffered during a >10-min outage (2026-05-22 event root cause). Only
structural 403/404 rejects abandon, after a 5-attempt budget."
```

---

## Task 3: Integration test — mock S3 outage longer than the old cap, zero drops

**Files:** Create `crates/rs-endpoint/tests/upload_retention.rs`

Prove the wiring end-to-end against a fake S3 that fails past the old 10-attempt cap then recovers. `axum`, `tower`, `tempfile`, `sqlx` are already dev-deps of `rs-endpoint`.

- [ ] **Step 1: Inspect the uploader's public entry + S3 config** — `Read` `crates/rs-endpoint/src/uploader.rs:1-130` and `crates/rs-endpoint/src/lib.rs` to find the public `spawn`/`run`/`UploaderContext` entry and how the S3 endpoint URL is injected (the `rust-s3` `Bucket` is built from `Config.s3.endpoint`). Confirm the chunk-store DB helpers `insert_chunk`, `count_permanently_failed_since`, `get_pending_chunk_count_for_event` (in `rs-core::db`).

- [ ] **Step 2: Write the integration test** — create `crates/rs-endpoint/tests/upload_retention.rs`:

```rust
//! Outage continuity: a network outage longer than the OLD 10-attempt cap
//! must NOT permanently drop any chunk. Mock S3 returns 503 for the first
//! 15 PUTs (well past the old budget), then 200. Every chunk must end up
//! uploaded, zero permanently-failed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::put, Router};

// Counts PUTs; first FAIL_PUTS return 503, then 200.
const FAIL_PUTS: usize = 15;

#[derive(Clone)]
struct MockS3 {
    puts: Arc<AtomicUsize>,
}

async fn handle_put(State(s): State<MockS3>) -> StatusCode {
    let n = s.puts.fetch_add(1, Ordering::SeqCst);
    if n < FAIL_PUTS {
        StatusCode::SERVICE_UNAVAILABLE // classifies as "5xx" -> retry forever
    } else {
        StatusCode::OK
    }
}

#[tokio::test]
async fn outage_longer_than_old_cap_drops_nothing() {
    // 1. Start mock S3 on an ephemeral port.
    let puts = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/*key", put(handle_put))
        .with_state(MockS3 { puts: puts.clone() });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let endpoint = format!("http://{addr}");

    // 2. Build an in-memory chunk DB + one chunk on a temp file.
    //    (Use the SAME helpers the production uploader uses.)
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    let event_id = rs_core::db::insert_streaming_event(&pool, "test-outage")
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chunk1.flv");
    std::fs::write(&path, b"FLVchunkbytes").unwrap();
    rs_core::db::insert_chunk(
        &pool,
        event_id,
        path.to_str().unwrap(),
        13,
        "deadbeef",
        2000,
    )
    .await
    .unwrap();

    // 3. Run the uploader against the mock S3 until the chunk is sent or
    //    a generous deadline (must outlast >15 retries at capped 30s? NO —
    //    use the test config that shrinks backoff; see Step 3).
    rs_endpoint::testing::run_uploader_until_idle(&pool, &endpoint, "test-bucket")
        .await
        .unwrap();

    // 4. Assertions: chunk uploaded, ZERO permanently-failed, mock saw >cap PUTs.
    let permanent = rs_core::db::count_permanently_failed_since(&pool, 0)
        .await
        .unwrap();
    assert_eq!(permanent, 0, "no chunk may be abandoned during a network outage");
    let pending = rs_core::db::get_pending_chunk_count_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(pending, 0, "the chunk must eventually upload after recovery");
    assert!(
        puts.load(Ordering::SeqCst) > FAIL_PUTS,
        "uploader must have retried past the old 10-attempt cap"
    );
}
```

- [ ] **Step 3: Add the `rs_endpoint::testing::run_uploader_until_idle` helper** — the test needs a deterministic, fast driver. `Read` `crates/rs-endpoint/src/lib.rs` and add a `#[cfg(any(test, feature = "testing"))] pub mod testing` (or a `pub mod testing` gated by a `testing` feature) exposing:

```rust
//! Test-only helpers. NOT compiled into release binaries.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use sqlx::SqlitePool;

    /// Drive the upload worker loop with a near-zero backoff against a mock
    /// S3 endpoint until no uploadable chunk remains (or a 30s safety
    /// deadline). Uses the SAME `upload_one` path as production so the
    /// never-drop decision is exercised, not bypassed.
    pub async fn run_uploader_until_idle(
        pool: &SqlitePool,
        s3_endpoint: &str,
        bucket: &str,
    ) -> anyhow::Result<()> {
        // Build a Bucket pointed at the mock; reuse production upload_one.
        // Backoff is overridden to 5ms for the test via the existing
        // backoff_ms? No — call a test-scoped worker that sleeps 5ms between
        // retries. Implementation mirrors run_worker but with test backoff.
        super::testing_support::drive_until_idle(pool, s3_endpoint, bucket).await
    }
}
```

The actual `drive_until_idle` lives in a small `#[cfg(any(test, feature = "testing"))] mod testing_support` in `uploader.rs` that mirrors `run_worker`'s loop (`uploader.rs:304-356`) but: (a) builds the `Bucket` from `s3_endpoint`+`bucket` with path-style addressing, (b) sleeps 5 ms instead of `backoff_ms` between picks, (c) returns when `pick_next_uploadable_chunk` yields `None` twice in a row OR a 30 s wall deadline. It calls the unchanged `upload_one`. **Reuse `record_upload_failure`'s `next_retry_at` but in the test path override the wait** — simplest: after each `upload_one`, set `upload_next_retry_at = now` directly via a tiny `UPDATE chunk_records SET upload_next_retry_at = 0 WHERE sent = 0 AND upload_failed_permanently = 0` so the next pick is immediate. (This keeps the production retry/backoff code intact and only accelerates the test clock.)

> Implementation note for the subagent: keep `drive_until_idle` <60 lines. If exposing `upload_one` requires `pub(crate)`, make it `pub(crate)`. Do NOT change production behavior — only add test-gated code.

- [ ] **Step 4: Verify** — Controller runs `cargo test -p rs-endpoint --features testing outage_longer_than_old_cap_drops_nothing`. Expected: PASS. (If a `testing` feature is added, declare it in `crates/rs-endpoint/Cargo.toml` `[features] testing = []` and add `required-features` is not needed for integration tests — gate via `#[cfg(feature = "testing")]` and run with `--features testing`.)

- [ ] **Step 5: Commit**
```bash
git add crates/rs-endpoint/tests/upload_retention.rs crates/rs-endpoint/src/uploader.rs crates/rs-endpoint/src/lib.rs crates/rs-endpoint/Cargo.toml
git commit -m "test: mock-S3 outage past old cap drops zero chunks (integration)"
```

---

## Task 4: Disk-pressure monitor (alert-only) + `LocalDiskPressure` audit

**Files:** Create `crates/rs-endpoint/src/disk_pressure.rs`; Modify `crates/rs-endpoint/Cargo.toml`, `crates/rs-endpoint/src/lib.rs`, `crates/rs-core/src/audit.rs`; spawn from `crates/rs-runtime/src/orchestrator.rs`

With never-drop, an arbitrarily long outage buffers chunks until the laptop disk fills. We do NOT silently drop (that breaks continuity); we monitor and alert loudly. True disk-full surfaces as RED Attention (T16) and the existing flv_chunker write-error path.

- [ ] **Step 1: RED — classifier test** — create `crates/rs-endpoint/src/disk_pressure.rs` with the test first:

```rust
//! Local chunk-store disk-pressure monitor. Alert-only: we never drop a
//! buffered chunk (continuity guarantee). At critical, the endpoint
//! lifecycle goes RED Attention (operator must act).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskPressure {
    Ok,
    Warn,
    Critical,
}

/// Classify by fraction of the volume USED (0.0..=1.0).
pub fn classify_disk_pressure(used_fraction: f64) -> DiskPressure {
    if used_fraction >= 0.90 {
        DiskPressure::Critical
    } else if used_fraction >= 0.80 {
        DiskPressure::Warn
    } else {
        DiskPressure::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_boundaries() {
        assert_eq!(classify_disk_pressure(0.0), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.799), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.80), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.899), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.90), DiskPressure::Critical);
        assert_eq!(classify_disk_pressure(1.0), DiskPressure::Critical);
    }
}
```
Add `pub mod disk_pressure;` to `crates/rs-endpoint/src/lib.rs`.

- [ ] **Step 2: Verify RED** — Controller runs `cargo test -p rs-endpoint classify_boundaries`. Expected: PASS immediately (pure fn) — so this task's RED is the boundary test guarding the thresholds; it is the regression guard. (No false-RED gymnastics: the threshold constants are the behavior under test.)

- [ ] **Step 3: Add `LocalDiskPressure` to the Action enum** — `Read` `crates/rs-core/src/audit.rs:104-112`, after `HostInternetRecovered` (`:112`) add:

```rust
    /// Local chunk-store volume crossed a disk-pressure threshold on the
    /// host (stream.lan). Warn at 80% used, Critical at 90%. Alert-only —
    /// chunks are never dropped (continuity guarantee); Critical drives the
    /// endpoint lifecycle to Attention so the operator intervenes before a
    /// true disk-full. Detail JSON: {used_fraction, used_bytes, total_bytes}.
    LocalDiskPressure,
```

- [ ] **Step 4: Add `sysinfo` direct dep** — `Read` `crates/rs-endpoint/Cargo.toml`. Under `[dependencies]` add `sysinfo = "0.37"` (already compiled transitively via rust-s3 → zero new compile cost). If the workspace pins it, prefer `sysinfo = { workspace = true }` after adding to root `[workspace.dependencies]`.

- [ ] **Step 5: Implement the monitor** — append to `disk_pressure.rs`:

```rust
use rs_core::audit::{Action, AuditRow, RateLimiter, Severity, Source};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

/// Sample the volume containing `chunk_dir` every 10s; emit LocalDiskPressure
/// (rate-limited 1/min per severity) on Warn/Critical. Returns when the
/// shutdown channel fires.
pub async fn run_disk_monitor(
    chunk_dir: PathBuf,
    audit_tx: Option<mpsc::Sender<AuditRow>>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let rl = Arc::new(RateLimiter::new());
    loop {
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
        let Some((used, total)) = volume_usage(&chunk_dir) else { continue };
        if total == 0 { continue; }
        let frac = used as f64 / total as f64;
        let pressure = classify_disk_pressure(frac);
        let (sev, class) = match pressure {
            DiskPressure::Ok => continue,
            DiskPressure::Warn => (Severity::Warn, "warn"),
            DiskPressure::Critical => (Severity::Critical, "critical"),
        };
        if let Some(tx) = &audit_tx {
            if rl.allow(Action::LocalDiskPressure, class) {
                rs_core::audit::record(
                    tx,
                    AuditRow {
                        severity: sev,
                        source: Source::Inpoint,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: Action::LocalDiskPressure,
                        detail: serde_json::json!({
                            "used_fraction": frac,
                            "used_bytes": used,
                            "total_bytes": total,
                        }),
                        ts_override: None,
                    },
                );
            }
        }
    }
}

/// (used_bytes, total_bytes) for the volume holding `path`, via sysinfo.
fn volume_usage(path: &std::path::Path) -> Option<(u64, u64)> {
    use sysinfo::Disks;
    let disks = Disks::new_with_refreshed_list();
    // Pick the disk whose mount point is the longest prefix of `path`.
    let mut best: Option<(&sysinfo::Disk, usize)> = None;
    for d in disks.list() {
        let mp = d.mount_point();
        if path.starts_with(mp) {
            let len = mp.as_os_str().len();
            if best.map(|(_, l)| len > l).unwrap_or(true) {
                best = Some((d, len));
            }
        }
    }
    let (d, _) = best?;
    let total = d.total_space();
    let avail = d.available_space();
    Some((total.saturating_sub(avail), total))
}
```

- [ ] **Step 6: Spawn the monitor** — `Read` `crates/rs-runtime/src/orchestrator.rs` where the uploader is started (search for the uploader spawn / `with_upload_blocked`, ~`:146,171,363,594`). Add a spawn of `rs_endpoint::disk_pressure::run_disk_monitor(chunk_dir, audit_tx.clone(), shutdown_rx)` using the same chunk directory the chunker writes to and the same `audit_tx`/shutdown the uploader uses. (Subagent: read the surrounding spawn block and mirror its argument sources exactly.)

- [ ] **Step 7: Verify** — Controller: `cargo test -p rs-endpoint classify_boundaries` PASS; `cargo check --workspace` clean.

- [ ] **Step 8: Commit**
```bash
git add crates/rs-endpoint/src/disk_pressure.rs crates/rs-endpoint/src/lib.rs crates/rs-endpoint/Cargo.toml crates/rs-core/src/audit.rs crates/rs-runtime/src/orchestrator.rs Cargo.toml Cargo.lock
git commit -m "feat: local disk-pressure monitor + LocalDiskPressure audit (alert-only)"
```

---

## Task 5: Add `RescueActivated` / `RescueRecovered` to the Action enum

**Files:** Modify `crates/rs-core/src/audit.rs:37-147`

No migration (the `action` column is free-text TEXT). Adding to the shared enum makes both the host `record()` path and the VPS `AuditRing`→mirror path accept the new strings.

- [ ] **Step 1: Add variants** — `Read` `crates/rs-core/src/audit.rs:66-89`. After `S3FetchFailed` (`:68`) add:

```rust
    /// Delivery VPS switched an endpoint to the rescue video because the
    /// chunk supply ran dry (upstream outage). Severity::Warn. Detail JSON:
    /// {stalled_at_chunk_id}. Pairs with RescueRecovered.
    RescueActivated,
    /// Delivery VPS exited rescue and resumed live delivery after the chunk
    /// supply recovered. Severity::Info. Detail JSON: {gap_secs} — how long
    /// the rescue video covered the outage.
    RescueRecovered,
```

- [ ] **Step 2: Verify** — Controller: `cargo check -p rs-core`. Expected: clean (serde derives the snake_case strings `rescue_activated` / `rescue_recovered`).

- [ ] **Step 3: Commit**
```bash
git add crates/rs-core/src/audit.rs
git commit -m "feat: add RescueActivated/RescueRecovered audit actions (no migration, free-text column)"
```

---

## Task 6: RED — rescue audit emission test

**Files:** Test in `crates/rs-delivery/src/rescue_audit.rs` (new) — a pure helper that builds the two rows, unit-tested; the consumer (T7) calls it.

The rescue enter/recover sites live deep in an async consumer loop (`endpoint_task.rs:678-718`). Test the row-building helper directly (pure), then wire it (T7). The E2E (T20) proves the live emission.

- [ ] **Step 1: RED test for the row builders** — create `crates/rs-delivery/src/rescue_audit.rs`:

```rust
//! Builders for the rescue-mode audit rows. Pure functions so the
//! enter/recover semantics are unit-testable; the consumer task calls these
//! and pushes the result onto the VPS AuditRing.

use rs_core::audit::{Action, RingRowParts, Severity, Source};

/// Row emitted when an endpoint enters rescue (chunk supply dried up).
pub fn rescue_activated_row(alias: &str, stalled_at_chunk_id: i64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Warn,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueActivated,
        detail: serde_json::json!({ "stalled_at_chunk_id": stalled_at_chunk_id }),
    }
}

/// Row emitted when an endpoint exits rescue back to live delivery.
pub fn rescue_recovered_row(alias: &str, gap_secs: u64) -> RingRowParts {
    RingRowParts {
        severity: Severity::Info,
        source: Source::Vps,
        endpoint: Some(alias.to_string()),
        action: Action::RescueRecovered,
        detail: serde_json::json!({ "gap_secs": gap_secs }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activated_row_is_warn_with_chunk() {
        let r = rescue_activated_row("yt-main", 4242);
        assert_eq!(r.action, Action::RescueActivated);
        assert_eq!(r.severity, Severity::Warn);
        assert_eq!(r.detail["stalled_at_chunk_id"], 4242);
    }

    #[test]
    fn recovered_row_carries_gap_secs() {
        let r = rescue_recovered_row("yt-main", 137);
        assert_eq!(r.action, Action::RescueRecovered);
        assert_eq!(r.severity, Severity::Info);
        assert_eq!(r.detail["gap_secs"], 137);
    }
}
```

This requires a small shared `RingRowParts` struct. `Read` `crates/rs-delivery/src/audit_ring.rs:14-100` to see `RingRow`/`push`. If `AuditRing::push` already takes `(severity, source, endpoint, action, detail)` separately, define `RingRowParts` as a thin struct of those fields in `audit_ring.rs` and have `push` accept it OR keep `RingRowParts` local to `rescue_audit.rs`. Add `pub mod rescue_audit;` to `crates/rs-delivery/src/lib.rs` (or `main.rs`/`api.rs` module root — check where modules are declared).

- [ ] **Step 2: Verify RED** — Controller: `cargo test -p rs-delivery rescue_audit`. Expected: FAIL to compile (`RingRowParts` / `rescue_activated_row` missing) until the struct + module are added — then PASS. (The RED is the missing symbol; once the file + `RingRowParts` exist, GREEN.)

- [ ] **Step 3: Commit**
```bash
git add crates/rs-delivery/src/rescue_audit.rs crates/rs-delivery/src/audit_ring.rs crates/rs-delivery/src/lib.rs
git commit -m "test: RED rescue audit row builders (RescueActivated/Recovered)"
```

---

## Task 7: GREEN — emit rescue audit at enter/recover in the consumer

**Files:** Modify `crates/rs-delivery/src/endpoint_task.rs:678-718`

`audit_ring: Option<Arc<AuditRing>>` is already in scope in the consumer task (param at `:493`).

- [ ] **Step 1: Emit RescueActivated on entry** — `Read` `endpoint_task.rs:678-718`. After the `tracing::warn!(... "entering rescue mode")` at `:681` and before the ffmpeg kill, capture the entry instant and push the row:

```rust
                        let rescue_started = std::time::Instant::now();
                        if let Some(ring) = &audit_ring {
                            let parts = crate::rescue_audit::rescue_activated_row(
                                &alias,
                                current_chunk_id, // the chunk id the consumer stalled at
                            );
                            ring.push(parts);
                        }
```
(Use whatever local holds the current/last chunk id in the consumer — `Read` the surrounding scope; if none, pass `-1` and note it. Adjust `ring.push(parts)` to match `AuditRing::push`'s real signature from T6/Step1.)

- [ ] **Step 2: Emit RescueRecovered on exit** — after rescue completes and before "resumed normal delivery" (`:715`), add:

```rust
                        if let Some(ring) = &audit_ring {
                            let gap = rescue_started.elapsed().as_secs();
                            ring.push(crate::rescue_audit::rescue_recovered_row(&alias, gap));
                        }
```

- [ ] **Step 3: Verify** — Controller: `cargo check -p rs-delivery` clean; `cargo test -p rs-delivery rescue_audit` PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat: emit RescueActivated/RescueRecovered with gap_secs at rescue enter/exit"
```

---

## Task 8: Thread `audit_ring` into the disk-cache subsystem (plumbing only)

**Files:** Modify `crates/rs-delivery/src/disk_cache/download_service.rs` (and/or `download_service.rs`), `disk_cache/eviction.rs`, `disk_cache_fetcher.rs`, `disk_cache/mod.rs`

Pure plumbing: add `audit_ring: Option<Arc<AuditRing>>` to the structs/constructors that need it for T9–T11. No emissions yet (so this commit is behavior-neutral and compiles).

- [ ] **Step 1: DownloadService** — `Read` `crates/rs-delivery/src/disk_cache/download_service.rs:70-120` (struct + `new`). Add field `audit_ring: Option<Arc<crate::audit_ring::AuditRing>>` and a constructor param (default the existing callers to `None` for now if the caller has no ring; otherwise pass it through). Also check the sibling `crates/rs-delivery/src/download_service.rs` (token-bucket) — same addition.

- [ ] **Step 2: EvictionTask** — `Read` `crates/rs-delivery/src/disk_cache/eviction.rs:20-40` (`spawn`/`run_once`). Add `audit_ring` param threaded from `DiskCache::new` (`mod.rs:117` spawn site).

- [ ] **Step 3: DiskCacheFetcher** — `Read` `crates/rs-delivery/src/disk_cache_fetcher.rs:34-60` (`new`). Add `audit_ring` field + param; the producer that constructs it (`endpoint_task.rs` producer) already has `audit_ring` in scope — pass it.

- [ ] **Step 4: Verify** — Controller: `cargo check -p rs-delivery` clean (all call sites updated, no emissions added).

- [ ] **Step 5: Commit**
```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs crates/rs-delivery/src/download_service.rs crates/rs-delivery/src/disk_cache/eviction.rs crates/rs-delivery/src/disk_cache_fetcher.rs crates/rs-delivery/src/disk_cache/mod.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "refactor: thread audit_ring through disk-cache subsystem (no behavior change)"
```

---

## Task 9: Emit `DiskCacheStallTimeout` + `DiskCacheReaderRecovered` on the live fetcher path

**Files:** Modify `crates/rs-delivery/src/disk_cache_fetcher.rs:82-123`

- [ ] **Step 1: Add a per-fetcher "was stalled" flag** — `Read` `disk_cache_fetcher.rs:34-123`. Add a `was_stalled: std::cell::Cell<bool>` (or `AtomicBool` if `&self`) field to the fetcher.

- [ ] **Step 2: Emit StallTimeout on the wait Err** — at the `wait_for_chunk_with_timeout(...).map_err(...)` (`:82-87`), on the Err branch, before returning the error, push (rate-limited via a per-fetcher `RateLimiter` keyed by alias):

```rust
            // outage forensics: the cache window emptied (real S3 outage longer
            // than the window). audit-only — do NOT abort; the producer keeps
            // waiting and rescue covers the gap.
            self.was_stalled.set(true);
            if let Some(ring) = &self.audit_ring {
                ring.push(crate::audit_ring::RingRowParts {
                    severity: rs_core::audit::Severity::Error,
                    source: rs_core::audit::Source::Vps,
                    endpoint: Some(self.alias.clone()),
                    action: rs_core::audit::Action::DiskCacheStallTimeout,
                    detail: serde_json::json!({ "chunk_id": chunk_id, "timeout_secs": self.stall_timeout_secs }),
                });
            }
```

- [ ] **Step 3: Emit ReaderRecovered on next success after a stall** — in the `ChunkAvailability::Available` arm (`:89-123`), at the top:

```rust
            if self.was_stalled.replace(false) {
                if let Some(ring) = &self.audit_ring {
                    ring.push(crate::audit_ring::RingRowParts {
                        severity: rs_core::audit::Severity::Info,
                        source: rs_core::audit::Source::Vps,
                        endpoint: Some(self.alias.clone()),
                        action: rs_core::audit::Action::DiskCacheReaderRecovered,
                        detail: serde_json::json!({ "chunk_id": chunk_id }),
                    });
                }
            }
```

(Adjust `RingRowParts` to the real `push` signature. Confirm the fetcher holds `alias` + `stall_timeout_secs` — both are referenced in the agent extract at `:82-87`.)

- [ ] **Step 4: Verify** — Controller: `cargo check -p rs-delivery` clean.

- [ ] **Step 5: Commit**
```bash
git add crates/rs-delivery/src/disk_cache_fetcher.rs
git commit -m "feat: emit DiskCacheStallTimeout + DiskCacheReaderRecovered (audit-only, no abort)"
```

---

## Task 10: Emit `DiskCacheWriteFailed` + `DiskCacheChunkEvicted` + `DiskCacheDownloadThrottled`

**Files:** Modify `crates/rs-delivery/src/disk_cache/download_service.rs:237-246`, `disk_cache/eviction.rs:80`, `crates/rs-delivery/src/download_service.rs:285-302`

- [ ] **Step 1: DiskCacheWriteFailed** — `Read` `disk_cache/download_service.rs:237-246`. In the `write_atomic(...)` Err branch (before `mark_not_found`), push `Severity::Error`, `Source::Vps`, `Action::DiskCacheWriteFailed`, detail `{ "chunk_id": chunk_id, "error": e.to_string() }`.

- [ ] **Step 2: DiskCacheChunkEvicted** — `Read` `disk_cache/eviction.rs:80-83`. Where `evicted > 0`, push a rate-limited (1/min) `Severity::Info`, `Action::DiskCacheChunkEvicted`, detail `{ "evicted": evicted }`. Use a `RateLimiter` owned by `EvictionTask`.

- [ ] **Step 3: DiskCacheDownloadThrottled** — `Read` `download_service.rs:285-302` (`token_bucket_consume`). When the queued wait `slot_end - now` exceeds a threshold (e.g. `>= 1s`), push a rate-limited (1/min) `Severity::Warn`, `Action::DiskCacheDownloadThrottled`, detail `{ "queued_ms": (slot_end - now).as_millis() }`.

- [ ] **Step 4: Verify** — Controller: `cargo check -p rs-delivery` clean.

- [ ] **Step 5: Commit**
```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs crates/rs-delivery/src/disk_cache/eviction.rs crates/rs-delivery/src/download_service.rs
git commit -m "feat: emit DiskCacheWriteFailed/ChunkEvicted/DownloadThrottled audit events"
```

---

## Task 11: Emit `DiskCachePrefillStarted` + `DiskCachePrefillReady`

**Files:** Modify `crates/rs-delivery/src/disk_cache_fetcher.rs` (registration = prefill start) and the warmup-complete site

- [ ] **Step 1: PrefillStarted** — `Read` `disk_cache_fetcher.rs:34-60` (`new` → `positions.register`). On first registration for an endpoint, push `Severity::Info`, `Source::Vps`, `Action::DiskCachePrefillStarted`, detail `{ "start_chunk_id": <id> }`.

- [ ] **Step 2: PrefillReady** — the "window first fully populated" signal. `Read` `crates/rs-delivery/src/rescue.rs:420-430` (warmup complete, `"Warmup complete"`) — emit `DiskCachePrefillReady` there (warmup completion == buffer first reached target), detail `{ "alias": <alias> }`. If warmup is not on the live path for a given endpoint, fall back to: first time the fetcher successfully returns `window_chunks` consecutive chunks. Pick the warmup-complete site (simplest, real).

- [ ] **Step 3: Verify** — Controller: `cargo check -p rs-delivery` clean.

- [ ] **Step 4: Commit**
```bash
git add crates/rs-delivery/src/disk_cache_fetcher.rs crates/rs-delivery/src/rescue.rs
git commit -m "feat: emit DiskCachePrefillStarted/Ready audit events"
```

---

## Task 12: Emit `RtmpHandshakeFailed`

**Files:** Modify `crates/rs-delivery/src/endpoint_consumer_helpers.rs:162-233` (the push-error audit site)

- [ ] **Step 1: Map handshake errors** — `Read` `endpoint_consumer_helpers.rs:140-235` and `crates/rs-rtmp-push/src/error.rs:41-114` (error variants). Where push errors are turned into `EndpointRtmpPushDied` audit rows, branch: if the underlying error is `HandshakeFailed` (per `rs-rtmp-push` error type), ALSO push a `RtmpHandshakeFailed` row (`Severity::Warn`, `Source::Vps`, endpoint = alias, detail `{ "error": <msg>, "backend": <service_type> }`) — keeping the existing `EndpointRtmpPushDied` row too (they serve different dashboards).

- [ ] **Step 2: Verify** — Controller: `cargo check -p rs-delivery` clean.

- [ ] **Step 3: Commit**
```bash
git add crates/rs-delivery/src/endpoint_consumer_helpers.rs
git commit -m "feat: emit RtmpHandshakeFailed audit on push handshake failure"
```

---

## Task 13: Verify replay invariants (no give-up, no live-edge jump for continuity)

**Files:** Test: `crates/rs-delivery/src/endpoint_task.rs` (or a focused test module) + assertion in `crates/rs-api/src/delivery_live_edge.rs`

The producer already retries forever (confirmed: no terminal give-up). The risk is the `is_fast` live-edge jump firing on a continuity endpoint during recovery. Lock it with a test + an explicit guard.

- [ ] **Step 1: RED — guard test** — `Read` `crates/rs-api/src/delivery_live_edge.rs:120-160` (the live-edge recompute). Identify the function deciding whether to jump (it checks `is_fast`). Add a unit test asserting a non-fast endpoint NEVER jumps:

```rust
    #[test]
    fn continuity_endpoint_never_jumps_to_live_edge() {
        // is_fast = false => replay from exact position, never skip the gap.
        assert!(!should_jump_to_live_edge(/* is_fast */ false, /* gap_chunks */ 9_999));
        // is_fast = true => jump is allowed (separate behavior, unchanged).
        assert!(should_jump_to_live_edge(true, 9_999));
    }
```

- [ ] **Step 2: GREEN — extract the guard** — if a `should_jump_to_live_edge(is_fast, gap_chunks) -> bool` predicate doesn't exist, extract the existing decision into one (`is_fast && gap_chunks > 0`) and call it at the existing site. Verify FAIL→PASS.

- [ ] **Step 3: Verify** — Controller: `cargo test -p rs-api should_jump_to_live_edge` (RED then) PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/rs-api/src/delivery_live_edge.rs
git commit -m "test: continuity endpoints never jump to live edge (replay invariant)"
```

---

## Task 14: RED — `EndpointLifecycle::compute` state machine tests

**Files:** Modify `crates/rs-core/src/models.rs` (add enum + compute), test inline

- [ ] **Step 1: RED tests** — `Read` `crates/rs-core/src/models.rs:300-332`. Add a `#[cfg(test)] mod lifecycle_tests` (near the struct) referencing not-yet-existing items:

```rust
#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    fn input(alive: bool, mode: Option<&str>, stall: Option<&str>, err: Option<&str>) -> LifecycleInput<'static> {
        LifecycleInput {
            alive,
            chunks_processed: if alive { 100 } else { 0 },
            delivery_mode: mode.map(|s| s.to_string()),
            stall_reason: stall.map(|s| s.to_string()),
            last_error: err.map(|s| s.to_string()),
            disk_critical: false,
        }
    }

    #[test]
    fn rescue_mode_is_blue_rescue() {
        assert_eq!(EndpointLifecycle::compute(&input(true, Some("rescue"), None, None)), EndpointLifecycle::Rescue);
    }

    #[test]
    fn recovering_and_warmup_are_blue_recovering() {
        assert_eq!(EndpointLifecycle::compute(&input(true, Some("recovering"), None, None)), EndpointLifecycle::Recovering);
        assert_eq!(EndpointLifecycle::compute(&input(true, Some("warmup"), None, None)), EndpointLifecycle::Recovering);
    }

    #[test]
    fn upstream_stall_is_blue_buffering_not_red() {
        // A transient network stall must NOT be red — it is survivable.
        let i = input(true, Some("normal"), Some("waiting for chunk 42 (S3)"), None);
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Buffering);
    }

    #[test]
    fn auth_reject_is_red_attention() {
        let i = input(false, None, None, Some("PublishRejected: bad stream key"));
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Attention);
    }

    #[test]
    fn disk_critical_is_red_attention() {
        let mut i = input(true, Some("normal"), None, None);
        i.disk_critical = true;
        assert_eq!(EndpointLifecycle::compute(&i), EndpointLifecycle::Attention);
    }

    #[test]
    fn healthy_is_green_live() {
        assert_eq!(EndpointLifecycle::compute(&input(true, Some("normal"), None, None)), EndpointLifecycle::Live);
    }

    #[test]
    fn not_started_is_pending() {
        assert_eq!(EndpointLifecycle::compute(&input(false, None, None, None)), EndpointLifecycle::Pending);
    }
}
```

- [ ] **Step 2: Verify RED** — Controller: `cargo test -p rs-core lifecycle_tests`. Expected: FAIL to compile (`EndpointLifecycle`, `LifecycleInput` missing).

- [ ] **Step 3: Commit**
```bash
git add crates/rs-core/src/models.rs
git commit -m "test: RED EndpointLifecycle state machine (outage = blue, never red)"
```

---

## Task 15: GREEN — `EndpointLifecycle` enum + compute + field on `DeliveryEndpointMetrics`

**Files:** Modify `crates/rs-core/src/models.rs:305-332`

- [ ] **Step 1: Add the enum + input + compute** — above `DeliveryEndpointMetrics` (`:305`):

```rust
/// Operator-facing endpoint lifecycle. Drives the dashboard semaphore:
/// Pending=gray, Live=green, Buffering/Rescue/Recovering=blue (survivable,
/// auto-recovering, NO action needed), Attention=red (operator MUST act).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointLifecycle {
    Pending,
    Live,
    Buffering,
    Rescue,
    Recovering,
    Attention,
}

/// Inputs the host has when computing lifecycle for one endpoint.
pub struct LifecycleInput<'a> {
    pub alive: bool,
    pub chunks_processed: i64,
    pub delivery_mode: Option<String>,
    pub stall_reason: Option<String>,
    pub last_error: Option<String>,
    pub disk_critical: bool,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl EndpointLifecycle {
    pub fn compute(i: &LifecycleInput<'_>) -> Self {
        // RED only for states the operator must act on.
        if i.disk_critical || last_error_is_actionable(i.last_error.as_deref()) {
            return EndpointLifecycle::Attention;
        }
        match i.delivery_mode.as_deref() {
            Some("rescue") => return EndpointLifecycle::Rescue,
            Some("recovering") | Some("warmup") => return EndpointLifecycle::Recovering,
            _ => {}
        }
        if i.alive && i.stall_reason.is_some() {
            return EndpointLifecycle::Buffering; // survivable upstream stall = blue
        }
        if !i.alive && i.chunks_processed == 0 {
            return EndpointLifecycle::Pending;
        }
        if !i.alive {
            // Dead with no actionable error => treat as recovering (the
            // pusher reconnects forever); never a bare red.
            return EndpointLifecycle::Recovering;
        }
        EndpointLifecycle::Live
    }
}

/// An error string the OPERATOR must act on (auth/key rejected). Network /
/// transient errors are NOT actionable — they auto-recover.
fn last_error_is_actionable(err: Option<&str>) -> bool {
    let Some(e) = err else { return false };
    let e = e.to_ascii_lowercase();
    e.contains("rejected")
        || e.contains("bad stream key")
        || e.contains("unauthorized")
        || e.contains("forbidden")
        || e.contains("invalid stream key")
        || e.contains("badname")
}
```

> Adjust the T14 test's `input(...)` helper to set `_marker: std::marker::PhantomData` (or drop the lifetime entirely — simpler: make `LifecycleInput` own `String`s with no lifetime and remove `<'a>`/`_marker`). **Pick the no-lifetime version** to keep the test helper clean: define `pub struct LifecycleInput { pub alive: bool, ... pub disk_critical: bool }` and `compute(i: &LifecycleInput)`. Update the T14 test accordingly in this same commit if needed.

- [ ] **Step 2: Add the field to `DeliveryEndpointMetrics`** — after `youtube_health` (`:331`):
```rust
    /// Operator-facing lifecycle (host-computed). Older payloads default to
    /// Live so the dashboard degrades gracefully.
    #[serde(default = "default_lifecycle")]
    pub lifecycle: EndpointLifecycle,
```
and add near the struct:
```rust
fn default_lifecycle() -> EndpointLifecycle { EndpointLifecycle::Live }
```

- [ ] **Step 3: Verify** — Controller: `cargo test -p rs-core lifecycle_tests` PASS; `cargo check --workspace` (the new required field on `DeliveryEndpointMetrics` will break its construction sites — those are fixed in T16).

- [ ] **Step 4: Commit**
```bash
git add crates/rs-core/src/models.rs
git commit -m "feat: EndpointLifecycle enum + compute + field on DeliveryEndpointMetrics"
```

---

## Task 16: Populate `lifecycle` on the backend

**Files:** Modify `crates/rs-api/src/delivery_status.rs:372-394`; also the placeholder construction sites `crates/rs-api/src/lib.rs:310-312`, `crates/rs-api/src/stream_handlers.rs:144-146`

- [ ] **Step 1: Compute lifecycle in the main mapping** — `Read` `delivery_status.rs:370-394`. After building `m` (before `metrics.push(m)`), set:
```rust
            m.lifecycle = rs_core::models::EndpointLifecycle::compute(
                &rs_core::models::LifecycleInput {
                    alive: m.alive,
                    chunks_processed: m.chunks_processed,
                    delivery_mode: m.delivery_mode.clone(),
                    stall_reason: m.stall_reason.clone(),
                    last_error: m.last_error.clone(),
                    disk_critical: false, // host disk-pressure wired below if available
                },
            );
```
In the struct literal at `:372-387`, add `lifecycle: rs_core::models::EndpointLifecycle::Live,` (overwritten immediately after) so the literal compiles.

> `disk_critical`: if a host-side disk-critical flag is readily available in this scope (e.g. an `AtomicBool` set by T4's monitor and shared via `AppState`), wire it. If not in scope, leave `false` and file it as a follow-up note in T21 (the LocalDiskPressure audit + UI alarm still fire from T4/T18; the per-endpoint red is a nicety). Prefer wiring it if `AppState` is reachable here.

- [ ] **Step 2: Fix placeholder construction sites** — `Read` `crates/rs-api/src/lib.rs:308-314` and `crates/rs-api/src/stream_handlers.rs:142-148`. Add `lifecycle: rs_core::models::EndpointLifecycle::Pending,` to those `DeliveryEndpointMetrics { ... }` literals (configured-but-not-live endpoints are Pending).

- [ ] **Step 3: Verify** — Controller: `cargo check --workspace` clean.

- [ ] **Step 4: Commit**
```bash
git add crates/rs-api/src/delivery_status.rs crates/rs-api/src/lib.rs crates/rs-api/src/stream_handlers.rs
git commit -m "feat: compute per-endpoint lifecycle on the backend (outage=blue, auth/disk=red)"
```

---

## Task 17: Frontend — render lifecycle, fix CSS gap, error-string hygiene

**Files:** Modify `leptos-ui/src/ws.rs:114-139,294-345`, `leptos-ui/src/store.rs:95-120`, `leptos-ui/src/components/operator_dashboard.rs:664-806`, `leptos-ui/style.css`

- [ ] **Step 1: Mirror the field** — `Read` `leptos-ui/src/ws.rs:114-139`. Add to `WsDeliveryEndpoint`:
```rust
    #[serde(default = "default_lifecycle")]
    lifecycle: crate::store::EndpointLifecycle,
```
Add a frontend `EndpointLifecycle` enum in `store.rs` (mirror of the rs-core one, `#[derive(Debug,Clone,Copy,PartialEq,Deserialize)] #[serde(rename_all="snake_case")]` with the 6 variants + a `default_lifecycle()` returning `Live`, and a `Default` impl = `Live`). `Read` `store.rs:95-120`, add `pub lifecycle: EndpointLifecycle,` to `DeliveryEndpointState`. In `ws.rs:314-333` map block add `lifecycle: ep.lifecycle,`.

- [ ] **Step 2: Drive dot + node class from lifecycle** — `Read` `operator_dashboard.rs:664-689`. Replace `status_class` and `dot_class` bodies to switch on `ep.lifecycle`:
```rust
                    let status_class = move || {
                        match ep_data.get().lifecycle {
                            EndpointLifecycle::Live => "endpoint-node live",
                            EndpointLifecycle::Pending => "endpoint-node pending",
                            EndpointLifecycle::Buffering
                            | EndpointLifecycle::Rescue
                            | EndpointLifecycle::Recovering => "endpoint-node recovering",
                            EndpointLifecycle::Attention => "endpoint-node attention",
                        }
                    };
                    let dot_class = move || {
                        match ep_data.get().lifecycle {
                            EndpointLifecycle::Live => "status-dot active",
                            EndpointLifecycle::Pending => "status-dot",
                            EndpointLifecycle::Buffering
                            | EndpointLifecycle::Rescue
                            | EndpointLifecycle::Recovering => "status-dot recovering",
                            EndpointLifecycle::Attention => "status-dot error",
                        }
                    };
```
(Import `use crate::store::EndpointLifecycle;` at top of the file.)

- [ ] **Step 3: Error-string hygiene** — `Read` `operator_dashboard.rs:777-781`. Replace the raw `last_error` render with a short, truncated, mapped label that only shows for Attention (raw stays in the audit panel):
```rust
                                {move || {
                                    let ep = ep_data.get();
                                    if ep.lifecycle == EndpointLifecycle::Attention {
                                        ep.last_error.clone().map(|e| {
                                            let short: String = e.chars().take(60).collect();
                                            let short = if e.chars().count() > 60 { format!("{short}\u{2026}") } else { short };
                                            view! { <span class="endpoint-anomaly" title=e>{short}</span> }
                                        })
                                    } else {
                                        None // survivable states: no scary raw error
                                    }
                                }}
```

- [ ] **Step 4: Fix the CSS gap + add lifecycle colors** — `Read` `leptos-ui/style.css:199-203,363-400`. Add:
```css
.status-dot.recovering { background: #3b82f6; opacity: 1; animation: pulse 2s ease-in-out infinite; }
.endpoint-node.live { border-left-color: var(--status-ok); }
.endpoint-node.pending { border-left-color: var(--border); }
.endpoint-node.recovering { border-left-color: #3b82f6; }
.endpoint-node.attention { border-left-color: var(--status-error); }
```
(These also retire the latent gap where `pending/dead/stalled/healthy` had no rules — the closures no longer emit those names.)

- [ ] **Step 5: Verify** — Controller: `cargo check -p leptos-ui --target wasm32-unknown-unknown` is heavy; instead `cargo check -p leptos-ui` (native check of the crate compiles the logic). Confirm no missing-class references remain (grep `endpoint-node ` usages vs defined classes).

- [ ] **Step 6: Commit**
```bash
git add leptos-ui/src/ws.rs leptos-ui/src/store.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css
git commit -m "feat: dashboard renders endpoint lifecycle semaphore + error-string hygiene; fix CSS gap"
```

---

## Task 18: Calm outage banner

**Files:** Create `leptos-ui/src/components/outage_banner.rs`; Modify `leptos-ui/src/components/operator_dashboard.rs:33-49`, `leptos-ui/src/components/mod.rs` (module decl), `leptos-ui/style.css`

Mirror the existing `ZeroEndpointBanner` pattern.

- [ ] **Step 1: Create the component** — `crates/.../leptos-ui/src/components/outage_banner.rs`:
```rust
//! Calm, single banner shown when any endpoint is in a survivable
//! auto-recovery state (Buffering/Rescue/Recovering). Replaces the wall of
//! red cards: the operator sees "protected, recovering, no action needed".

use crate::store::{DashboardStore, EndpointLifecycle};
use leptos::prelude::*;

#[component]
pub fn OutageBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let delivery = store.delivery;

    let recovering = Memo::new(move |_| {
        delivery.get().endpoints.iter().any(|e| matches!(
            e.lifecycle,
            EndpointLifecycle::Buffering | EndpointLifecycle::Rescue | EndpointLifecycle::Recovering
        ))
    });
    // Only show when NO endpoint needs attention (attention has its own red).
    let any_attention = Memo::new(move |_| {
        delivery.get().endpoints.iter().any(|e| e.lifecycle == EndpointLifecycle::Attention)
    });

    view! {
        <Show when=move || recovering.get() && !any_attention.get()>
            <div class="banner banner--recovering" role="status">
                {"\u{1F6E1} Upstream outage detected \u{2014} all endpoints protected, rescue video live, recovering automatically. No action needed."}
            </div>
        </Show>
    }
}
```

- [ ] **Step 2: Wire into dashboard** — `Read` `operator_dashboard.rs:33-49`. Add `<OutageBanner />` directly under `<ZeroEndpointBanner />` (`:35`), and `use super::outage_banner::OutageBanner;` near the imports (`:15`). Declare the module in `components/mod.rs` (`pub mod outage_banner;`).

- [ ] **Step 3: CSS** — `Read` `style.css:612-614`. Add:
```css
.banner--recovering { background: #11243f; color: #cfe3ff; border: 1px solid #3b82f6; }
```

- [ ] **Step 4: Verify** — Controller: `cargo check -p leptos-ui` clean.

- [ ] **Step 5: Commit**
```bash
git add leptos-ui/src/components/outage_banner.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/src/components/mod.rs leptos-ui/style.css
git commit -m "feat: calm outage banner (protected/recovering) replaces red wall"
```

---

## Task 19: Playwright frontend test — lifecycle states + banner + clean console

**Files:** `Read` an existing `e2e/*frontend*.spec.ts` for the harness; Create/extend `e2e/outage-ui.spec.ts`

- [ ] **Step 1: Inspect harness** — `Read` `e2e/playwright-frontend.config.ts` and one existing frontend spec to learn how the WASM dashboard is served + how WS data is mocked/seeded for tests.

- [ ] **Step 2: Write the test** — `e2e/outage-ui.spec.ts`. Drive the dashboard with seeded delivery states (via the same mechanism existing frontend specs use — mock WS frame or test API) and assert:
```ts
import { test, expect } from '@playwright/test';

test('survivable outage shows calm blue banner, not red wall, clean console', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // Seed a DeliveryStatus frame with one endpoint in lifecycle "rescue".
  // (Use the project's existing WS-mock helper — see harness inspection.)
  await seedDeliveryStatus(page, [{ alias: 'yt-main', lifecycle: 'rescue', alive: true, delivery_mode: 'rescue' }]);

  // The calm banner appears...
  await expect(page.locator('.banner--recovering')).toBeVisible();
  await expect(page.locator('.banner--recovering')).toContainText('No action needed');
  // ...and the endpoint node is blue (recovering), not red.
  await expect(page.locator('.endpoint-node.recovering')).toBeVisible();
  await expect(page.locator('.endpoint-node.attention')).toHaveCount(0);

  // An auth-reject endpoint IS red and suppresses the calm banner.
  await seedDeliveryStatus(page, [{ alias: 'yt-main', lifecycle: 'attention', alive: false, last_error: 'PublishRejected: bad stream key' }]);
  await expect(page.locator('.endpoint-node.attention')).toBeVisible();
  await expect(page.locator('.banner--recovering')).toHaveCount(0);

  expect(consoleMessages).toEqual([]);
});
```
(Implement `seedDeliveryStatus` using the harness's existing mock path; if none exists, add a tiny test-only WS injector mirroring how other frontend specs feed `WsEvent::DeliveryStatus`.)

- [ ] **Step 3: Verify** — Controller (or operator on stream.lan) runs `cd e2e && npx playwright test outage-ui --config playwright-frontend.config.ts`. Expected: PASS, zero console messages.

- [ ] **Step 4: Commit**
```bash
git add e2e/outage-ui.spec.ts
git commit -m "test: Playwright — outage shows calm banner not red wall, clean console"
```

---

## Task 20: Extend `e2e-obs-youtube` with a real long-outage step

**Files:** Modify `.github/workflows/ci.yml` — insert a new step AFTER the existing "Simulated network disconnect — cache drain & recovery" step (`:4072-4216`)

Rationale: the single self-hosted runner serializes e2e jobs; adding a whole new job adds ~30-40 min serial CI. Extending the already-gated `e2e-obs-youtube` job adds only the block+drain time (~6-7 min) and reuses all setup. The block trips the OLD 10-attempt cap (~3 min) so a 5-min block proves never-drop. The job is already in `e2e-gate`'s `needs:` — no gate change needed.

- [ ] **Step 1: Read the existing outage step + s3-block API** — `Read` `.github/workflows/ci.yml:4072-4216` and confirm `POST /api/v1/_test/s3-block` / `_test/s3-unblock` (`:4105`,`:4149`) and the audit query pattern (`/api/v1/audit?action=...`).

- [ ] **Step 2: Insert the long-outage step** — after line 4216, add (ASCII-only PowerShell):

```yaml
      - name: Long outage - never-drop + rescue + calm UI
        shell: powershell
        timeout-minutes: 15
        run: |
          $ErrorActionPreference = 'Stop'
          $base = "http://127.0.0.1:8910"
          $eventId = (Invoke-RestMethod -Uri "$base/api/v1/status").current_event_id
          $sinceIso = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

          Write-Host "Blocking S3 for 300s (past the old 10-attempt permanent-drop cap)..."
          Invoke-RestMethod -Method Post -Uri "$base/api/v1/_test/s3-block" | Out-Null
          Start-Sleep -Seconds 300

          # ASSERT 1: zero chunks abandoned during the outage (continuity).
          $perm = Invoke-RestMethod -Uri "$base/api/v1/audit?action=s3_upload_failed&since=$sinceIso&limit=5000"
          $abandoned = @($perm.rows | Where-Object { $_.detail -match '"permanent":true' }).Count
          if ($abandoned -ne 0) { throw "FAIL: $abandoned chunks permanently dropped during outage (continuity broken)" }
          Write-Host "OK: zero chunks abandoned during 5-min outage"

          # ASSERT 2: rescue activated (cache window 120s drained -> rescue).
          $rescue = Invoke-RestMethod -Uri "$base/api/v1/audit?action=rescue_activated&since=$sinceIso&limit=100"
          if (@($rescue.rows).Count -lt 1) { throw "FAIL: RescueActivated not recorded - rescue video did not engage" }
          Write-Host "OK: rescue activated during outage"

          # ASSERT 3: dashboard shows calm blue banner, not a red wall.
          $check = @"
          const { chromium } = require('playwright');
          (async () => {
            const b = await chromium.launch();
            const p = await b.newPage();
            await p.goto('http://127.0.0.1:8910/');
            await p.waitForTimeout(3000);
            const banner = await p.locator('.banner--recovering').count();
            const attention = await p.locator('.endpoint-node.attention').count();
            await b.close();
            if (banner < 1) { console.error('NO calm recovering banner'); process.exit(1); }
            if (attention > 0) { console.error('endpoint shown RED during survivable outage'); process.exit(1); }
            console.log('OK: calm blue banner shown, no red');
          })().catch(e => { console.error(e); process.exit(1); });
          "@
          $check | Out-File -Encoding ascii "$env:TEMP\outage_ui_check.cjs"
          node "$env:TEMP\outage_ui_check.cjs"
          if ($LASTEXITCODE -ne 0) { throw "FAIL: dashboard outage UI check failed" }

          Write-Host "Unblocking S3 - backlog must drain and replay in order..."
          Invoke-RestMethod -Method Post -Uri "$base/api/v1/_test/s3-unblock" | Out-Null

          # ASSERT 4: backlog drains (pending -> ~0) within 180s.
          $drained = $false
          for ($i = 1; $i -le 36; $i++) {
            $stats = Invoke-RestMethod -Uri "$base/api/v1/chunks/stats"
            Write-Host "drain poll $i: pending=$($stats.pending)"
            if ($stats.pending -le 2) { $drained = $true; break }
            Start-Sleep -Seconds 5
          }
          if (-not $drained) { throw "FAIL: backlog did not drain after recovery" }

          # ASSERT 5: rescue recovered (back to live).
          $rec = Invoke-RestMethod -Uri "$base/api/v1/audit?action=rescue_recovered&since=$sinceIso&limit=100"
          if (@($rec.rows).Count -lt 1) { throw "FAIL: RescueRecovered not recorded - never returned to live" }
          Write-Host "OK: outage survived end-to-end with zero chunk loss"
```

> Pre-req checks (subagent): confirm `/api/v1/chunks/stats` returns a `.pending` field (it is used at `ci.yml:2845-2875`); confirm `/api/v1/status` returns `current_event_id` (else read the event id the same way the surrounding steps do). Confirm `node`/`playwright` are available on the runner (the existing inline `.cjs` Playwright checks at `:2877-3014` prove they are). If `/api/v1/audit` rate-limits `s3_upload_failed` (it does: 1/min/class), 5 minutes yields ~5 rows — `permanent:true` rows bypass the limiter, so ASSERT 1 is exact for the property we test.

- [ ] **Step 3: Verify** — pushed to CI in T21. (No local YAML execution.)

- [ ] **Step 4: Commit**
```bash
git add .github/workflows/ci.yml
git commit -m "test(ci): e2e long-outage - zero drops + rescue audit + calm blue banner + in-order drain"
```

---

## Task 21 [ORCHESTRATOR ONLY]: tracking issues, push, CI, PR, post-deploy verify, completion report

Not a subagent task. The orchestrator performs:

- [ ] **Step 1: File tracking issues** (before push) and capture numbers for the PR body:
```bash
gh issue create --title "Outage hardening: never-drop buffered chunks during long outage" --body "..." --label bug
gh issue create --title "Outage hardening: operator lifecycle semaphore + calm banner" --body "..."
gh issue create --title "Outage hardening: complete audit timeline (wire 7 dead events + rescue + handshake)" --body "..."
gh issue create --title "Outage hardening: CI long-outage E2E gate" --body "..."
```
Reference the never-drop issue as the bug-fix this PR closes (regression-test-first applies — T1→T2 is the RED→GREEN pair).

- [ ] **Step 2: Full pre-push gate** (Tier-2): `cargo fmt --all --check` && `cargo check --workspace` && `cargo clippy --workspace --all-targets -- -D warnings` && `cargo test --no-run --workspace` && run all new unit tests (`cargo test -p rs-core lifecycle_tests`, `cargo test -p rs-endpoint should_abandon_upload classify_boundaries`, `cargo test -p rs-endpoint --features testing outage_longer_than_old_cap_drops_nothing`, `cargo test -p rs-delivery rescue_audit`, `cargo test -p rs-api should_jump_to_live_edge`). Fix any failure before pushing.

- [ ] **Step 3: Single push** `git push origin dev`; monitor CI per ci-monitoring (single `sleep 300 && gh run view <id>` background pattern) until ALL jobs terminal-green, including the extended `e2e-obs-youtube` outage step and `e2e-gate`.

- [ ] **Step 4: Run `/plan-check`, `/review`, and `superpowers:requesting-code-review`**; fix every 🔴/🟡/🔵 inside the diff before reporting.

- [ ] **Step 5: Open PR** dev→main, body lists every `Closes #N`. Verify `mergeable: true` AND `mergeable_state: "clean"`.

- [ ] **Step 6: Post-deploy verify** on streamsnv + streampp via `win-streampp` / win-* MCP (process running, `/api/v1/status` 200, dashboard version label == v0.20.0 read from DOM, lifecycle badges render). If `win-stream-snv` MCP is still down, alert the operator to restart it (do not SSH-workaround).

- [ ] **Step 7: Completion report** per `completion-report.md` (audits block + Goal/What changed/Dashboard URL/PR + the `✅ Regression test:` line citing `crates/rs-endpoint/src/uploader.rs` should_abandon_upload, RED on T1 SHA, GREEN on T2 SHA). Wait for explicit user merge instruction.

---

## Self-Review (writing-plans checklist)

**1. Spec coverage:**
- P1 never-drop → T1, T2, T3. Disk valve → T4. ✅
- P2 VPS replay (no give-up confirmed; no live-edge jump) → T13; rescue audit → T6, T7. ✅
- P3 audit completeness: Rescue* enum → T5; rescue emit → T7; stall/recovered → T9; write-failed/evicted/throttled → T10; prefill → T11; handshake → T12. All 7 dead events + 2 rescue + handshake covered. ✅
- P4 UX: lifecycle compute → T14, T15; backend populate → T16; frontend render + CSS + hygiene → T17; banner → T18. ✅
- P5 TDD/CI/E2E: unit (T1, T4, T6, T13, T14), integration (T3), Playwright (T19), CI long-outage (T20). ✅
- Acceptance criteria 1–6 map to T3/T20 (zero drops), T17/T18/T19/T20 (calm banner), T7/T9/T20 (audit timeline), T20 (every-push gate via e2e-obs-youtube), T19/T20 (clean console), T16/T21 (version label). ✅

**2. Placeholder scan:** No "TBD/TODO/handle errors". The two judgment points (T2 `now_ms` cleanup, T16 `disk_critical` wiring) carry explicit fallback instructions, not placeholders. T3 test-helper and T19 `seedDeliveryStatus` carry concrete implementation notes tied to existing patterns. ✅

**3. Type consistency:** `EndpointLifecycle` (6 variants) consistent across rs-core (T15), frontend store (T17), CSS classes (T17 `.live/.pending/.recovering/.attention`), and Playwright (T19). `should_abandon_upload(class, attempt)` signature consistent T1↔T2. `RingRowParts` introduced T6, reused T9/T10. `LifecycleInput` no-lifetime version chosen (T15 note) and used in T14/T16. `Action` variants added once (T5: Rescue*, T4: LocalDiskPressure) and referenced consistently. ✅

Fixed inline: T15 directs updating the T14 test helper to the no-lifetime `LifecycleInput`.
