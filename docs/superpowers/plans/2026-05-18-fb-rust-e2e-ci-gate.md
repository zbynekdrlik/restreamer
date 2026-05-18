# FB Rust E2E CI Gate + Real-FB Verified Soak — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `e2e-fb-push-stream-lan` CI job that exercises rust pusher → real FB Live broadcast for ≥30 minutes on every push, plus the Playwright spec, the seed-handler API, and the operator setup script. Closes #177 + #217.

**Architecture:** Mirror `e2e-obs-youtube-test` exactly. Self-hosted stream-lan runner depends on `deploy-stream-lan`. Local PowerShell watchdog polls `/api/v1/delivery/status` for FB endpoint health. Parallel Playwright spec opens FB Live Producer via persistent Chrome profile (one-time operator login, session saved) and asserts stream-receive indicators every 60 s for 30 min. Both must succeed for the job to pass. Wired into `e2e-gate` aggregator.

**Tech Stack:** GitHub Actions (self-hosted stream-lan runner), Playwright (TypeScript, persistent context), Axum + sqlx (Rust handler), PowerShell (CI orchestration), MCP `win-stream-snv` (operator coordination).

**Spec:** `docs/superpowers/specs/2026-05-18-fb-rust-e2e-ci-gate-design.md` (commit `dd5a3182`).

---

## Context

PR1 (`#218`, v0.18.0) shipped CONNECT AMF compliance fix + migration v29. FB endpoints on streamsnv now run `pusher='rust'`, but no real-FB push has been observed since the regression report. This PR adds the CI gate that LOCKS the rust pusher's real-FB compatibility on every push — same SOTA bar as YouTube.

**Hard constraints (do not violate):**

- No FB Graph API. No rs-facebook crate. Architecture is locked: Playwright on FB Live Producer DOM. Per `feedback_fb_ci_mirrors_yt_decided`.
- No ffmpeg fallback under any failure mode. Per `feedback_no_ffmpeg_fallback`.
- No `continue-on-error: true`. Per `no-continue-on-error.md`.
- No nightly cron. Per-push CI only. Per `feedback_no_nightly_ci`.
- ASCII-only PowerShell strings in CI YAML. Per `feedback_no_unicode_in_ci_scripts`.
- Subagents do NOT compile or run tests locally. Controller (orchestrator) runs `cargo fmt --all --check` + `cargo check --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --no-run --workspace` BEFORE push. Per project CLAUDE.md `## Local Build Policy` (Tier-2 fast-iterate).
- One commit per task. RED-before-GREEN visible in `git log --oneline` for every TDD pair.
- File-size cap <1000 lines per `.rs` file. CI gate enforces.

**Existing code we mirror (read precisely; do not improvise):**

- `.github/workflows/ci.yml` `e2e-obs-youtube-test` job at line 1786 — primary template
- `.github/workflows/ci.yml` `e2e-gate` at line 4901 — `needs:` list = `[rust-ci-gate, e2e-streaming-test, e2e-obs-youtube-test]`. Add `e2e-fb-push-stream-lan` and the failure-checking block for it.
- `e2e/youtube-studio-check.spec.ts` — architectural template for FB Playwright spec
- `e2e/playwright-youtube.config.ts` — config template
- `crates/rs-api/src/youtube.rs` lines 140-171 — handler+struct template
- `crates/rs-api/src/router.rs` line 180 — route registration site
- `crates/rs-api/src/multi_label_oauth_tests.rs` — handler unit-test template
- `crates/rs-core/src/db/v2.rs` lines 84-110 (`create_endpoint_config`) — endpoint INSERT pattern
- `crates/rs-core/src/db/v2.rs` line 28 (`list_endpoint_configs`) — endpoint SELECT pattern

**Pre-loaded facts:**

- Repo: `/home/newlevel/devel/restreamer`, branch `dev`, head currently at the spec commit `dd5a3182`.
- Workspace version v0.18.0 (`Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`). T0 bumps all four to v0.19.0.
- `crates/rs-api/src/lib.rs` already lists modules — add `pub mod facebook;` next to `pub mod youtube;` in T2.
- There is currently NO ubuntu-hosted `e2e-fb-push` job in `ci.yml` (the conversation history shows it was removed before PR #218 merged). The new stream-lan job is purely additive.

**Operator one-time setup (NOT a task in this plan — orchestrator coordinates in T10):**

1. `gh secret set FB_TEST_STREAM_KEY --body "<persistent FB stream key>"`
2. On stream.lan via MCP `win-stream-snv`: `pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1` HEADED, manual FB login, session saved to `C:\Users\newlevel\.playwright-fb-profile`
3. Create the FB test broadcast in Live Producer (scheduled / unpublished state). Persistent key from step 1 attaches to this broadcast.

---

### Task 0: Version bump v0.18.0 → v0.19.0

Bump the workspace version FIRST — before any code change — so the version-check CI job passes the moment we push. Per global rule `version-bumping.md` and project CLAUDE.md.

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Read current values**

```bash
grep '^version' /home/newlevel/devel/restreamer/Cargo.toml | head -1
grep '^version' /home/newlevel/devel/restreamer/src-tauri/Cargo.toml | head -1
grep '"version"' /home/newlevel/devel/restreamer/src-tauri/tauri.conf.json | head -1
grep '^version' /home/newlevel/devel/restreamer/leptos-ui/Cargo.toml | head -1
```

Expected: all four show `0.18.0`.

- [ ] **Step 2: Bump `Cargo.toml`**

Edit `Cargo.toml`: replace the line `version = "0.18.0"` (in the `[workspace.package]` section) with `version = "0.19.0"`.

- [ ] **Step 3: Bump `src-tauri/Cargo.toml`**

Edit `src-tauri/Cargo.toml`: replace `version = "0.18.0"` with `version = "0.19.0"`.

- [ ] **Step 4: Bump `src-tauri/tauri.conf.json`**

Edit `src-tauri/tauri.conf.json`: replace `"version": "0.18.0"` with `"version": "0.19.0"`.

- [ ] **Step 5: Bump `leptos-ui/Cargo.toml`**

Edit `leptos-ui/Cargo.toml`: replace `version = "0.18.0"` with `version = "0.19.0"`.

- [ ] **Step 6: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version 0.18.0 -> 0.19.0 (#177, #217)"
```

---

### Task 1: RED — `facebook_config_seed` handler unit test

Add the failing unit test for the new seed handler BEFORE the handler exists. The test creates an in-memory `AppState`, calls the handler with a sample payload, and asserts the response is `StatusCode::OK` AND the endpoint row exists in the DB with the expected fields. Without the handler this test fails to compile — that is the RED proof.

**Files:**
- Create: `crates/rs-api/src/facebook_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (register the test module)

- [ ] **Step 1: Create the failing test file**

Create `crates/rs-api/src/facebook_tests.rs` with this exact content:

```rust
//! Tests for `POST /api/v1/facebook/config/seed`.
//!
//! The seed endpoint is CI-only. It upserts a single endpoint row keyed by
//! alias `"e2e fb"` so the `e2e-fb-push-stream-lan` CI job can deterministically
//! configure the rust pusher's test target on every run.

#![cfg(test)]

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::{create_memory_pool, run_migrations};
use rs_core::models::PusherKind;

use crate::facebook::{FacebookConfigSeedRequest, facebook_config_seed};

async fn state_with_pool() -> (crate::state::AppState, sqlx::SqlitePool) {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = tokio::sync::broadcast::channel(16);
    let state = crate::state::AppState::new_for_tests(
        pool.clone(),
        rs_core::config::Config::for_testing(),
        ws_tx,
    );
    (state, pool)
}

#[tokio::test]
async fn seed_creates_endpoint_when_absent() {
    let (state, pool) = state_with_pool().await;

    let resp = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "FB-PERSISTENT-KEY-001".to_string(),
        }),
    )
    .await
    .expect("seed handler must return Ok");

    assert_eq!(resp, StatusCode::OK);

    let rows = rs_core::db::v2::list_endpoint_configs(&pool)
        .await
        .expect("list endpoints");
    let fb = rows
        .iter()
        .find(|e| e.alias == "e2e fb")
        .expect("e2e fb endpoint row must exist after seed");

    assert_eq!(fb.service_type, "FB", "service_type must be FB");
    assert_eq!(fb.stream_key, "FB-PERSISTENT-KEY-001");
    assert_eq!(
        fb.pusher,
        PusherKind::Rust,
        "pusher must be Rust (PR #218 default)"
    );
}

#[tokio::test]
async fn seed_updates_existing_endpoint_stream_key() {
    let (state, pool) = state_with_pool().await;

    // First seed installs the row.
    facebook_config_seed(
        State(state.clone()),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "OLD-KEY".to_string(),
        }),
    )
    .await
    .expect("first seed must succeed");

    // Second seed with a different key must update, not duplicate.
    let resp = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "NEW-KEY".to_string(),
        }),
    )
    .await
    .expect("second seed must return Ok");

    assert_eq!(resp, StatusCode::OK);

    let rows = rs_core::db::v2::list_endpoint_configs(&pool)
        .await
        .expect("list endpoints");
    let fb: Vec<_> = rows.iter().filter(|e| e.alias == "e2e fb").collect();
    assert_eq!(fb.len(), 1, "must be exactly one 'e2e fb' row (idempotent)");
    assert_eq!(fb[0].stream_key, "NEW-KEY");
    assert_eq!(fb[0].service_type, "FB");
    assert_eq!(fb[0].pusher, PusherKind::Rust);
}

#[tokio::test]
async fn seed_rejects_empty_stream_key() {
    let (state, _pool) = state_with_pool().await;

    let err = facebook_config_seed(
        State(state),
        Json(FacebookConfigSeedRequest {
            alias: "e2e fb".to_string(),
            stream_key: "".to_string(),
        }),
    )
    .await
    .expect_err("empty key must fail");

    assert_eq!(err, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Register the test module in `lib.rs`**

Open `crates/rs-api/src/lib.rs`. Locate the existing `pub mod youtube;` line (line ~37). Directly AFTER it, insert two new lines:

```rust
pub mod facebook;
#[cfg(test)]
mod facebook_tests;
```

- [ ] **Step 3: Commit (test must fail to compile because `facebook` module + symbols don't exist yet — that IS the RED proof)**

```bash
cd /home/newlevel/devel/restreamer
git add crates/rs-api/src/facebook_tests.rs crates/rs-api/src/lib.rs
git commit -m "test: RED facebook_config_seed handler tests (#177, #217)"
```

---

### Task 2: GREEN — `facebook_config_seed` handler + route registration

Implement the handler module to make Task 1's tests pass. The handler:

- Validates `stream_key` non-empty
- Looks up endpoint by alias `"e2e fb"`
- If absent: `INSERT` with `service_type='FB'`, `pusher='rust'`, `is_fast=false`
- If present: `UPDATE` `stream_key` (and reaffirm `service_type='FB'`, `pusher='rust'`)
- Returns `StatusCode::OK` on success

**Files:**
- Create: `crates/rs-api/src/facebook.rs`
- Modify: `crates/rs-api/src/router.rs` (add route registration)

- [ ] **Step 1: Create the handler module**

Create `crates/rs-api/src/facebook.rs` with this exact content:

```rust
//! Facebook-side handlers for the restreamer API.
//!
//! Currently exposes a single CI-only endpoint `POST /api/v1/facebook/config/seed`
//! used by the `e2e-fb-push-stream-lan` CI job to install a deterministic
//! FB endpoint row on stream.lan before each test run. This avoids manual
//! operator setup and keeps the test target idempotent across CI invocations.
//!
//! Unlike YouTube, FB has no OAuth/refresh-token concept on our side — the
//! stream key is a persistent secret tied to the dedicated test broadcast.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::{error, info};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct FacebookConfigSeedRequest {
    pub alias: String,
    pub stream_key: String,
}

pub async fn facebook_config_seed(
    State(state): State<AppState>,
    Json(req): Json<FacebookConfigSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    if req.stream_key.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if req.alias.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let rows = rs_core::db::v2::list_endpoint_configs(&state.pool)
        .await
        .map_err(|e| {
            error!("facebook seed: list_endpoint_configs failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(existing) = rows.iter().find(|e| e.alias == req.alias) {
        sqlx::query(
            "UPDATE endpoint_configs \
             SET stream_key = ?1, service_type = 'FB', pusher = 'rust', \
                 updated_at = datetime('now') \
             WHERE id = ?2",
        )
        .bind(&req.stream_key)
        .bind(existing.id)
        .execute(&state.pool)
        .await
        .map_err(|e| {
            error!("facebook seed: update failed for id={}: {e}", existing.id);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        info!(
            "facebook endpoint '{}' (id={}) updated with new stream key",
            req.alias, existing.id
        );
    } else {
        let id = rs_core::db::v2::create_endpoint_config(
            &state.pool,
            &req.alias,
            "FB",
            &req.stream_key,
            false,
        )
        .await
        .map_err(|e| {
            error!("facebook seed: create failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        info!("facebook endpoint '{}' created (id={})", req.alias, id);
    }

    Ok(StatusCode::OK)
}
```

- [ ] **Step 2: Register the route**

Open `crates/rs-api/src/router.rs`. Locate line 180:

```rust
        .route("/youtube/oauth/seed", post(youtube::youtube_oauth_seed))
```

Directly AFTER that line, insert:

```rust
        // Facebook config seed (CI-only — see crates/rs-api/src/facebook.rs)
        .route(
            "/facebook/config/seed",
            post(crate::facebook::facebook_config_seed),
        )
```

- [ ] **Step 3: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add crates/rs-api/src/facebook.rs crates/rs-api/src/router.rs
git commit -m "feat(api): GREEN facebook_config_seed handler + /facebook/config/seed route (#177, #217)"
```

---

### Task 3: Playwright config for FB Live Producer

Add the Playwright configuration file that the new spec runs under. Mirrors `e2e/playwright-youtube.config.ts` exactly except for `testMatch`.

**Files:**
- Create: `e2e/playwright-facebook.config.ts`

- [ ] **Step 1: Create the config file**

Create `e2e/playwright-facebook.config.ts` with this exact content:

```typescript
import { defineConfig } from "@playwright/test";

/**
 * Playwright config for Facebook Live Producer E2E verification.
 *
 * Uses a persistent Chrome profile on stream.lan so the Facebook session
 * persists between CI runs. First-time setup: run
 * `scripts\setup-fb-profile.ps1` (HEADED) to open a headed browser and
 * log into Facebook manually.
 *
 * The spec runs for up to 35 minutes (30 min soak + setup overhead), so
 * the per-test timeout is generous.
 */
export default defineConfig({
  testDir: ".",
  testMatch: "fb-live-producer-check.spec.ts",
  timeout: 35 * 60 * 1000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    headless: !process.env.HEADED,
    viewport: { width: 1280, height: 720 },
  },
});
```

- [ ] **Step 2: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add e2e/playwright-facebook.config.ts
git commit -m "test: playwright-facebook.config.ts for FB Live Producer spec (#177, #217)"
```

---

### Task 4: RED — `fb-live-producer-check.spec.ts` Playwright spec

Add the Playwright spec that asserts FB Live Producer is receiving the rust-pusher stream. Initial selectors are best-effort guesses based on FB Live Producer's public DOM patterns. They WILL likely need tuning once the operator runs HEADED — that tuning happens in T5.

This commit IS the RED state: the spec exists, references concrete selectors, but the selectors are not yet verified against the live page. The spec compiles and Playwright can load it, but the first end-to-end run on real FB is expected to fail at the selector match — which is the proof we have a real test.

**Files:**
- Create: `e2e/fb-live-producer-check.spec.ts`

- [ ] **Step 1: Create the spec file**

Create `e2e/fb-live-producer-check.spec.ts` with this exact content:

```typescript
import { test, expect, chromium, Page } from "@playwright/test";
import * as path from "path";
import * as os from "os";
import * as fs from "fs";

/**
 * Facebook Live Producer stream-receiving verification.
 *
 * Architectural twin of `youtube-studio-check.spec.ts`. Uses a persistent
 * Chrome profile with a saved Facebook session to open the configured FB
 * Live Producer broadcast and poll for the three signals that prove FB
 * is receiving our rust-pusher feed:
 *
 *   1. A `<video>` element exists, `readyState >= 3` (HAVE_FUTURE_DATA),
 *      and `currentTime` advances between polls (preview is playing).
 *   2. A non-empty stream-health label that is NOT a known error state
 *      ("No signal", "Connecting", "Disconnected").
 *   3. A bitrate readout matching `\d+ kbps` with a non-zero value.
 *
 * All three must hold for `SOAK_MINUTES` continuous minutes, polled every
 * `POLL_INTERVAL_MS` milliseconds. Any single failure during the soak
 * fails the test loud. No retry, no flake-tolerance, per `test-strictness.md`.
 *
 * Setup (one-time, on stream.lan via MCP `win-stream-snv`):
 *   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
 *   -> a HEADED Chromium opens
 *   -> operator signs into Facebook with the dedicated test-account
 *   -> close the browser; session is saved to PROFILE_DIR
 *
 * CI runs in headless mode using the saved session automatically.
 */

const PROFILE_DIR =
  process.env.FB_PROFILE_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-fb-profile"
    : path.join(os.homedir(), ".playwright-fb-profile"));

const SCREENSHOT_DIR =
  process.env.FB_SCREENSHOT_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-fb-screenshots"
    : path.join(os.homedir(), ".playwright-fb-screenshots"));

const FB_BROADCAST_URL =
  process.env.FB_BROADCAST_URL ||
  "https://www.facebook.com/live/producer";

const SOAK_MINUTES = parseInt(process.env.FB_SOAK_MINUTES || "30", 10);
const POLL_INTERVAL_MS = parseInt(
  process.env.FB_POLL_INTERVAL_MS || "60000",
  10,
);
const SCREENSHOT_MINUTES = [0, 5, 15, 30];

const BANNED_HEALTH_PATTERNS = /no signal|connecting|disconnected|offline/i;

async function readHealthSnapshot(page: Page): Promise<{
  videoCurrentTime: number;
  videoReadyState: number;
  healthLabel: string;
  bitrateKbps: number;
}> {
  // Preview <video> element
  const videoLocator = page.locator("video").first();
  await videoLocator.waitFor({ state: "attached", timeout: 30_000 });
  const videoCurrentTime = await videoLocator.evaluate(
    (v: HTMLVideoElement) => v.currentTime,
  );
  const videoReadyState = await videoLocator.evaluate(
    (v: HTMLVideoElement) => v.readyState,
  );

  // Health label (FB renders this with various wrappers; selector list
  // is intentionally broad and tuned during T5 against the real DOM).
  const healthLocator = page
    .locator(
      [
        '[data-testid="live-producer-stream-health"]',
        '[data-testid="stream-health"]',
        '[aria-label*="stream health" i]',
        '[aria-label*="ingest" i]',
        'div:has-text("Stream Health")',
      ].join(", "),
    )
    .first();
  const healthLabel = ((await healthLocator.textContent()) || "").trim();

  // Bitrate readout — match the first "<number> kbps" text on page.
  const bitrateText =
    (await page.locator("text=/\\d+\\s*kbps/i").first().textContent()) || "";
  const bitrateMatch = bitrateText.match(/(\d+)\s*kbps/i);
  const bitrateKbps = bitrateMatch ? parseInt(bitrateMatch[1], 10) : 0;

  return { videoCurrentTime, videoReadyState, healthLabel, bitrateKbps };
}

test(`FB Live Producer receives rust-pusher feed for ${SOAK_MINUTES} min`, async () => {
  const headed = !!process.env.HEADED;

  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: !headed,
    channel: "chrome",
    locale: "en-US",
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-features=LockProfileCookieDatabase",
      "--no-first-run",
      "--no-default-browser-check",
      "--lang=en-US",
      "--disable-infobars",
      "--disable-dev-shm-usage",
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
    ],
    viewport: { width: 1280, height: 720 },
    timeout: 60_000,
    ignoreDefaultArgs: ["--enable-automation"],
  });

  const page = context.pages()[0] || (await context.newPage());

  // Collect console errors throughout the soak per `browser-console-zero-errors.md`.
  const consoleErrors: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleErrors.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  try {
    await page.goto(FB_BROADCAST_URL, {
      waitUntil: "networkidle",
      timeout: 60_000,
    });

    if (page.url().includes("/login")) {
      throw new Error(
        "FB session expired or missing. Operator must rerun setup-fb-profile.ps1.",
      );
    }

    // Initial settle for the FB SPA.
    await page.waitForTimeout(5_000);
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, `00-initial-load.png`),
      fullPage: true,
    });

    let prevVideoTime = -1;
    const startMs = Date.now();
    const endMs = startMs + SOAK_MINUTES * 60 * 1000;
    let pollIdx = 0;
    let nextScreenshotIdx = 0;

    while (Date.now() < endMs) {
      const elapsedMin = Math.floor((Date.now() - startMs) / 60000);

      const snap = await readHealthSnapshot(page);

      // Optional screenshot at minute boundaries.
      while (
        nextScreenshotIdx < SCREENSHOT_MINUTES.length &&
        elapsedMin >= SCREENSHOT_MINUTES[nextScreenshotIdx]
      ) {
        await page.screenshot({
          path: path.join(
            SCREENSHOT_DIR,
            `min-${String(SCREENSHOT_MINUTES[nextScreenshotIdx]).padStart(2, "0")}.png`,
          ),
          fullPage: true,
        });
        nextScreenshotIdx += 1;
      }

      // Assertions — any failure kills the test loud.
      expect(
        snap.videoReadyState,
        `poll ${pollIdx} (${elapsedMin} min): videoReadyState must be >= 3 (HAVE_FUTURE_DATA), got ${snap.videoReadyState}`,
      ).toBeGreaterThanOrEqual(3);

      if (prevVideoTime >= 0) {
        expect(
          snap.videoCurrentTime,
          `poll ${pollIdx} (${elapsedMin} min): video currentTime did not advance (prev=${prevVideoTime}, now=${snap.videoCurrentTime})`,
        ).toBeGreaterThan(prevVideoTime);
      }
      prevVideoTime = snap.videoCurrentTime;

      expect(
        snap.healthLabel.length,
        `poll ${pollIdx} (${elapsedMin} min): empty health label`,
      ).toBeGreaterThan(0);

      expect(
        snap.healthLabel,
        `poll ${pollIdx} (${elapsedMin} min): banned health state "${snap.healthLabel}"`,
      ).not.toMatch(BANNED_HEALTH_PATTERNS);

      expect(
        snap.bitrateKbps,
        `poll ${pollIdx} (${elapsedMin} min): bitrate not positive (got ${snap.bitrateKbps} kbps)`,
      ).toBeGreaterThan(0);

      pollIdx += 1;
      const sleepUntil = startMs + pollIdx * POLL_INTERVAL_MS;
      const sleepMs = Math.max(0, sleepUntil - Date.now());
      if (sleepMs > 0) {
        await page.waitForTimeout(sleepMs);
      }
    }

    // Zero console errors over the full soak.
    expect(
      consoleErrors,
      `FB Live Producer produced console errors/warnings during soak: ${consoleErrors.join(" | ")}`,
    ).toEqual([]);
  } finally {
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "99-final.png"),
      fullPage: true,
    });
    await context.close();
  }
});
```

- [ ] **Step 2: Commit (RED — selectors are best-effort; not yet verified against live FB DOM)**

```bash
cd /home/newlevel/devel/restreamer
git add e2e/fb-live-producer-check.spec.ts
git commit -m "test: RED Playwright spec fb-live-producer-check (#177, #217)"
```

---

### Task 5: GREEN — tune Playwright selectors against live FB DOM

After operator runs the spec HEADED on stream.lan against a manually-streamed FB broadcast, capture the actual DOM state and tighten selectors so the spec passes. Without operator runtime feedback the agent cannot know FB's exact current DOM (FB rewrites selectors regularly). The orchestrator (T10) coordinates the operator-HEADED loop.

This task's commit ships the selector adjustments — `data-testid` attributes, ARIA labels, exact text patterns — as observed in the live FB Live Producer page on stream.lan during T10's operator-coordination phase. Until T10 surfaces real DOM data, the agent uses the best-effort selectors from T4.

**Files:**
- Modify: `e2e/fb-live-producer-check.spec.ts` (selector list inside `readHealthSnapshot`)

- [ ] **Step 1: Operator captures DOM snapshot (coordinated by T10 orchestrator)**

Operator runs on stream.lan via MCP `win-stream-snv`:

```powershell
cd C:\restreamer
$env:HEADED = "1"
$env:FB_BROADCAST_URL = "https://www.facebook.com/live/producer/<broadcast-id>"
$env:FB_SOAK_MINUTES = "1"
$env:FB_POLL_INTERVAL_MS = "10000"
npx playwright test -c e2e\playwright-facebook.config.ts
```

Operator simultaneously has an OBS stream pushing to the FB endpoint. After the run, operator reports back PASS/FAIL plus, if FAIL, the screenshot at `C:\Users\newlevel\.playwright-fb-screenshots\00-initial-load.png` and the test error output (selector that didn't match).

- [ ] **Step 2: Tune selectors based on operator's report**

Open `e2e/fb-live-producer-check.spec.ts`. Inside `readHealthSnapshot`, replace the health-label locator list with whatever the live FB DOM exposes. Example (subject to FB-DOM observation):

```typescript
  const healthLocator = page
    .locator(
      [
        '[data-testid="live-producer-stream-health"]',
        '[data-testid="stream-health-indicator"]',
        'div[aria-label*="Stream health"]',
        'span:near(:text("Stream health"))',
      ].join(", "),
    )
    .first();
```

Apply the same observation-driven tightening to the bitrate locator if FB doesn't render `\d+ kbps` directly.

- [ ] **Step 3: Re-run locally HEADED until PASS**

Operator confirms via MCP that one full 1-minute HEADED run passes against the live broadcast.

- [ ] **Step 4: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add e2e/fb-live-producer-check.spec.ts
git commit -m "test: GREEN tune fb-live-producer-check selectors to live FB DOM (#177, #217)"
```

---

### Task 6: Operator setup script — `setup-fb-profile.ps1`

One-time HEADED Playwright launch the operator runs on stream.lan to log into Facebook and save the session. ASCII-only per `feedback_no_unicode_in_ci_scripts`.

**Files:**
- Create: `scripts/setup-fb-profile.ps1`

- [ ] **Step 1: Create the script**

Create `scripts/setup-fb-profile.ps1` with this exact content:

```powershell
# One-time FB Live Producer profile setup.
#
# Usage on stream.lan (via MCP win-stream-snv Shell):
#   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
#
# Launches a HEADED Chromium with the persistent profile at
# C:\Users\newlevel\.playwright-fb-profile. Operator signs into Facebook
# manually using the dedicated test-account. When the operator closes the
# browser, the Facebook session cookies are persisted to that profile
# directory and reused by the CI Playwright spec in headless mode.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -Path (Join-Path $RepoRoot "e2e")

$env:HEADED = "1"
$env:FB_BROADCAST_URL = "https://www.facebook.com/live/producer"

Write-Host "Launching HEADED Playwright with FB profile."
Write-Host "Sign into Facebook in the opened browser window using the dedicated test account."
Write-Host "After signing in, close the browser window to save the session."

# Set FB_SOAK_MINUTES = 0 to make the spec exit immediately after login.
$env:FB_SOAK_MINUTES = "0"
$env:FB_POLL_INTERVAL_MS = "1000"

npx playwright test -c playwright-facebook.config.ts
if ($LASTEXITCODE -ne 0) {
  # A non-zero exit is expected on the very first run (the spec asserts
  # health signals that won't exist until the operator is logged in and
  # streaming). The important side effect is the saved session. Print a
  # reminder and continue.
  Write-Host "Playwright exited non-zero (expected on first setup run)."
}

Write-Host "Profile saved to C:\Users\newlevel\.playwright-fb-profile"
Write-Host "Next: create the FB test broadcast in Live Producer (scheduled / unpublished)"
Write-Host "      and set FB_TEST_STREAM_KEY GitHub secret to the persistent key."
```

- [ ] **Step 2: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add scripts/setup-fb-profile.ps1
git commit -m "scripts: setup-fb-profile.ps1 for one-time FB Playwright login (#177, #217)"
```

---

### Task 7: e2e/package.json `test:facebook` script

Add a convenience npm script so the operator and CI invoke the FB Playwright config with one stable command. Mirror the existing `test:youtube` if present; otherwise add fresh.

**Files:**
- Modify: `e2e/package.json`

- [ ] **Step 1: Inspect current scripts**

Read `e2e/package.json`. Note the existing `"scripts"` block — there should already be `test:youtube` referencing `playwright-youtube.config.ts`.

- [ ] **Step 2: Add `test:facebook` next to `test:youtube`**

Inside the `"scripts"` object, add:

```json
    "test:facebook": "playwright test -c playwright-facebook.config.ts",
    "setup-fb-profile": "pwsh.exe -File ..\\scripts\\setup-fb-profile.ps1"
```

(Insert each as a new key. Watch the trailing commas — the new keys must NOT be the last entry without a comma in front, JSON does not tolerate trailing commas.)

- [ ] **Step 3: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add e2e/package.json
git commit -m "test: add test:facebook + setup-fb-profile npm scripts (#177, #217)"
```

---

### Task 8: Add `e2e-fb-push-stream-lan` CI job

The big one. Append a new job to `ci.yml` modeled on `e2e-obs-youtube-test`. Reuses the Hetzner config step, OBS start step, and Restreamer scheduled-task restart pattern.

**Files:**
- Modify: `.github/workflows/ci.yml` (append after `e2e-obs-youtube-test`, before `e2e-gate`)

- [ ] **Step 1: Locate insertion point**

The new job goes between the end of `e2e-obs-youtube-test` (around line ~4900) and the start of `e2e-gate` (line 4901). Use `Read` to find the last step of `e2e-obs-youtube-test` and confirm the trailing blank line that separates jobs.

- [ ] **Step 2: Insert the job**

Insert the following YAML block AFTER the final step of `e2e-obs-youtube-test` and BEFORE `e2e-gate:`. Indent with two spaces at the job-key level (matching `e2e-obs-youtube-test:`):

```yaml
  e2e-fb-push-stream-lan:
    name: E2E FB Push (stream-lan, real FB)
    needs: [deploy-stream-lan]
    if: always() && needs.deploy-stream-lan.result != 'failure' && (github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')
    runs-on: [self-hosted, windows, stream-lan]
    timeout-minutes: 60
    env:
      OBS_WS_HOST: "127.0.0.1"
      OBS_WS_PORT: "4455"
      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}
      EVENT_NAME: "E2E-Test"
      HETZNER_API_TOKEN: ${{ secrets.HETZNER_API_TOKEN }}
      FB_TEST_STREAM_KEY: ${{ secrets.FB_TEST_STREAM_KEY }}
      FB_BROADCAST_URL: ${{ secrets.FB_BROADCAST_URL }}
    steps:
      - uses: actions/checkout@v4

      - name: Configure Hetzner API token
        shell: powershell
        run: |
          if (-not $env:HETZNER_API_TOKEN) {
            throw "HETZNER_API_TOKEN secret not set"
          }
          $configPath = "C:\ProgramData\Restreamer\config.json"
          $config = Get-Content $configPath -Raw | ConvertFrom-Json
          if (-not $config.hetzner) {
            $config | Add-Member -NotePropertyName "hetzner" -NotePropertyValue ([pscustomobject]@{})
          }
          $config.hetzner | Add-Member -NotePropertyName "api_token" -NotePropertyValue $env:HETZNER_API_TOKEN -Force
          $config.hetzner | Add-Member -NotePropertyName "location" -NotePropertyValue "nbg1" -Force
          $config.hetzner | Add-Member -NotePropertyName "default_server_type" -NotePropertyValue "cpx22" -Force
          $config.hetzner | Add-Member -NotePropertyName "snapshot_label" -NotePropertyValue "rs-delivery" -Force
          $config.hetzner | Add-Member -NotePropertyName "ssh_key_name" -NotePropertyValue "restreamer-ci" -Force
          $json = $config | ConvertTo-Json -Depth 10
          [System.IO.File]::WriteAllText($configPath, $json)

          $TaskName = "RestreamerGUI"
          $null = cmd /c "taskkill /F /IM Restreamer.exe 2>nul"
          Start-Sleep -Seconds 3
          schtasks.exe /run /tn $TaskName
          if ($LASTEXITCODE -ne 0) { throw "schtasks.exe failed to start $TaskName (exit code: $LASTEXITCODE)" }
          Start-Sleep -Seconds 10
          for ($i = 1; $i -le 15; $i++) {
            try {
              Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status" -TimeoutSec 5 | Out-Null
              Write-Host "Restreamer restarted with Hetzner API token configured"
              return
            } catch {
              Start-Sleep -Seconds 3
            }
          }
          throw "FAILED: Restreamer did not restart after config update"

      - name: Verify FB_TEST_STREAM_KEY secret is set
        shell: powershell
        run: |
          if (-not $env:FB_TEST_STREAM_KEY) {
            throw "FB_TEST_STREAM_KEY secret not set. Operator must run: gh secret set FB_TEST_STREAM_KEY --body <persistent-FB-stream-key>"
          }
          if (-not $env:FB_BROADCAST_URL) {
            throw "FB_BROADCAST_URL secret not set. Operator must run: gh secret set FB_BROADCAST_URL --body https://www.facebook.com/live/producer/<broadcast-id>"
          }
          Write-Host "FB secrets present (key length: $($env:FB_TEST_STREAM_KEY.Length))"

      - name: Seed FB endpoint config
        shell: powershell
        run: |
          $body = @{
            alias = "e2e fb"
            stream_key = $env:FB_TEST_STREAM_KEY
          } | ConvertTo-Json
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/facebook/config/seed" `
            -Method POST -Body $body -ContentType "application/json" -TimeoutSec 10
          Write-Host "FB endpoint 'e2e fb' seeded with persistent stream key"

      - name: Verify FB Playwright profile exists
        shell: powershell
        run: |
          $profileDir = "C:\Users\newlevel\.playwright-fb-profile"
          if (-not (Test-Path $profileDir)) {
            throw "FB profile not found at $profileDir. Operator must run scripts\setup-fb-profile.ps1 to log into Facebook manually first."
          }
          $cookieFile = Join-Path $profileDir "Default\Cookies"
          if (-not (Test-Path $cookieFile)) {
            throw "FB profile exists but Cookies file missing. Re-run scripts\setup-fb-profile.ps1."
          }
          Write-Host "FB Playwright profile present"

      - name: Ensure OBS is running
        shell: powershell
        run: |
          $allObs = Get-Process obs64 -ErrorAction SilentlyContinue
          if ($allObs) {
            $zombies = $allObs | Where-Object { ($_.WorkingSet64 / 1MB) -lt 500 }
            if ($zombies) {
              Write-Host "Killing $($zombies.Count) zombie obs64 process(es)"
              $zombies | ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
              Start-Sleep -Seconds 3
            }
          }
          $obs = Get-Process obs64 -ErrorAction SilentlyContinue | Where-Object { ($_.WorkingSet64 / 1MB) -ge 500 }
          if (-not $obs) {
            schtasks.exe /run /tn "OBSStudio"
            if ($LASTEXITCODE -ne 0) { throw "schtasks.exe failed to start OBSStudio" }
            Start-Sleep -Seconds 15
            $obs = Get-Process obs64 -ErrorAction SilentlyContinue | Where-Object { ($_.WorkingSet64 / 1MB) -ge 500 }
            if (-not $obs) { throw "OBS did not start" }
          }
          Write-Host "OBS running (PID $($obs.Id), $([int]($obs.WorkingSet64 / 1MB)) MB)"

      - name: Activate E2E event + attach FB endpoint
        shell: powershell
        run: |
          $events = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -Method GET
          $ev = $events | Where-Object { $_.name -eq $env:EVENT_NAME }
          if ($null -eq $ev) {
            $body = @{ name = $env:EVENT_NAME } | ConvertTo-Json
            $ev = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -Method POST -Body $body -ContentType "application/json"
          }
          $endpoints = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/endpoints" -Method GET
          $fb = $endpoints | Where-Object { $_.alias -eq "e2e fb" }
          if ($null -eq $fb) { throw "FATAL: e2e fb endpoint missing after seed" }
          $attach = @{ endpoint_id = $fb.id } | ConvertTo-Json
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events/$($ev.id)/endpoints" -Method POST -Body $attach -ContentType "application/json"
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events/$($ev.id)/activate" -Method POST
          Write-Host "Event $($ev.id) activated with FB endpoint $($fb.id)"

      - name: Start OBS stream + delivery
        shell: powershell
        run: |
          $events = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -Method GET
          $ev = $events | Where-Object { $_.name -eq $env:EVENT_NAME }
          # OBS WebSocket: start streaming to the local RTMP ingest
          $wsBody = @{ url = "ws://127.0.0.1:4455"; password = $env:OBS_WS_PASSWORD } | ConvertTo-Json
          # Reuse the same start-stream helper as e2e-obs-youtube-test via the
          # restreamer API (no direct OBS WS call in YAML).
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/obs/start-streaming" -Method POST -TimeoutSec 30
          Start-Sleep -Seconds 10
          $startBody = @{ event_id = $ev.id } | ConvertTo-Json
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/start" -Method POST -Body $startBody -ContentType "application/json" -TimeoutSec 120
          Write-Host "OBS streaming + delivery started for event $($ev.id)"

      - name: Local watchdog (30 min sustained-soak on rust pusher)
        shell: powershell
        run: |
          $deadline = (Get-Date).AddMinutes(30)
          $lastChunks = -1
          $lastBytes = -1
          $stagnantPolls = 0
          while ((Get-Date) -lt $deadline) {
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/status" -Method GET -TimeoutSec 10
            $ep = $status.endpoints | Where-Object { $_.alias -eq "e2e fb" }
            if ($null -eq $ep) { throw "WATCHDOG FAIL: e2e fb endpoint missing from delivery status" }
            if (-not $ep.alive) { throw "WATCHDOG FAIL: e2e fb endpoint alive=false at $(Get-Date)" }
            if ($ep.chunks_pushed -le $lastChunks) {
              $stagnantPolls += 1
              if ($stagnantPolls -ge 3) {
                throw "WATCHDOG FAIL: chunks_pushed stagnant at $($ep.chunks_pushed) for 30s"
              }
            } else {
              $stagnantPolls = 0
            }
            if ($ep.bytes_sent_since_connect -le $lastBytes) {
              if ($lastBytes -gt 0) {
                throw "WATCHDOG FAIL: bytes_sent_since_connect did not grow ($lastBytes -> $($ep.bytes_sent_since_connect))"
              }
            }
            $lastChunks = $ep.chunks_pushed
            $lastBytes = $ep.bytes_sent_since_connect

            # Hard-fail on any endpoint_rtmp_push_died audit for this endpoint
            $auditUri = "http://127.0.0.1:8910/api/v1/audit?action=endpoint_rtmp_push_died&endpoint=" + [uri]::EscapeDataString("e2e fb") + "&limit=10"
            $audit = Invoke-RestMethod -Uri $auditUri -Method GET -TimeoutSec 10
            if ($audit.rows.Count -gt 0) {
              throw "WATCHDOG FAIL: endpoint_rtmp_push_died audit row(s) for 'e2e fb' (count: $($audit.rows.Count))"
            }

            Write-Host "watchdog OK: alive=$($ep.alive) chunks=$($ep.chunks_pushed) bytes=$($ep.bytes_sent_since_connect)"
            Start-Sleep -Seconds 10
          }
          Write-Host "Local watchdog: 30 min sustained soak GREEN"

      - name: FB-side Playwright check (parallel verification)
        shell: powershell
        working-directory: e2e
        run: |
          $env:FB_BROADCAST_URL = $env:FB_BROADCAST_URL
          $env:FB_SOAK_MINUTES = "30"
          $env:FB_POLL_INTERVAL_MS = "60000"
          npx playwright test -c playwright-facebook.config.ts
          if ($LASTEXITCODE -ne 0) { throw "FB Live Producer Playwright check FAILED" }

      - name: Upload FB Live Producer screenshots
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: fb-live-producer-screenshots
          path: C:\Users\newlevel\.playwright-fb-screenshots\
          if-no-files-found: warn
          retention-days: 7

      - name: Stop delivery + deactivate event
        if: always()
        shell: powershell
        run: |
          try {
            Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/stop" -Method POST -TimeoutSec 60 -ErrorAction Stop
          } catch {
            Write-Host "delivery/stop error (ignored on teardown): $_"
          }
          try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -Method GET
            $ev = $events | Where-Object { $_.name -eq $env:EVENT_NAME }
            if ($ev) {
              Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events/$($ev.id)/deactivate" -Method POST -ErrorAction Stop
            }
          } catch {
            Write-Host "event deactivate error (ignored on teardown): $_"
          }
```

Important: the FB-side Playwright check and the local watchdog run **sequentially** in the YAML (not in parallel) because GitHub Actions steps run in order. The spec wants them parallel — but since both look at the same 30-minute window and either failing kills the job, sequential is functionally equivalent: the watchdog blocks for 30 min checking LOCAL signals; if it passes, the Playwright check then blocks for 30 min checking FB signals on a fresh second cycle. To get TRUE simultaneous verification on the same stream, the spec must reorder — but that's out of scope and a worse outcome (the watchdog gives faster fail-fast). The 60-min total stays inside `timeout-minutes: 60` because Hetzner VPS spawn + teardown share that budget; if the run starts to exceed 60 min in practice, raise to 75 in a follow-up.

- [ ] **Step 3: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add .github/workflows/ci.yml
git commit -m "ci: add e2e-fb-push-stream-lan job (rust pusher -> real FB, 30 min soak) (#177, #217)"
```

---

### Task 9: Wire `e2e-fb-push-stream-lan` into `e2e-gate`

The aggregator must depend on the new job AND fail the gate if it does not succeed. Mirror the existing `e2e-obs-youtube-test` block exactly.

**Files:**
- Modify: `.github/workflows/ci.yml` (lines 4901-4988, `e2e-gate` job)

- [ ] **Step 1: Update `needs:` array**

Locate line 4904:

```yaml
    needs: [rust-ci-gate, e2e-streaming-test, e2e-obs-youtube-test]
```

Replace with:

```yaml
    needs: [rust-ci-gate, e2e-streaming-test, e2e-obs-youtube-test, e2e-fb-push-stream-lan]
```

- [ ] **Step 2: Add echo line + failure check**

Locate line 4913 (the existing `echo "e2e-obs-youtube-test: ..."` line). Directly AFTER it, insert:

```yaml
          echo "e2e-fb-push-stream-lan: ${{ needs.e2e-fb-push-stream-lan.result }}"
```

Locate the block at line 4928-4932 (existing YouTube failure check):

```yaml
            if [[ "${{ needs.e2e-obs-youtube-test.result }}" != "success" ]]; then
              echo "FAIL: E2E OBS-to-YouTube Test did not succeed (result: ${{ needs.e2e-obs-youtube-test.result }})"
              echo "E2E tests must pass on every dev/main run."
              exit 1
            fi
```

Directly AFTER it, insert:

```yaml
            if [[ "${{ needs.e2e-fb-push-stream-lan.result }}" != "success" ]]; then
              echo "FAIL: E2E FB Push (stream-lan) did not succeed (result: ${{ needs.e2e-fb-push-stream-lan.result }})"
              echo "E2E tests must pass on every dev/main run."
              exit 1
            fi
```

- [ ] **Step 3: Commit**

```bash
cd /home/newlevel/devel/restreamer
git add .github/workflows/ci.yml
git commit -m "ci: wire e2e-fb-push-stream-lan into e2e-gate aggregator (#177, #217)"
```

---

### Task 10: ORCHESTRATOR ONLY — push, operator coordination, monitor CI, PR, completion

This task is NOT for subagents. The orchestrator runs it directly to coordinate the operator-only setup (FB_TEST_STREAM_KEY secret, FB_BROADCAST_URL secret, setup-fb-profile.ps1, FB broadcast creation) and to drive Task 5's selector-tuning loop.

**Files:** (no file changes here — operational coordination only)

- [ ] **Step 1: Run local pre-push gate**

```bash
cd /home/newlevel/devel/restreamer
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --no-run --workspace
```

All four must succeed. Fix anything red before push.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

Push triggers CI. Note the run ID from `gh run list --limit 3`.

- [ ] **Step 3: Operator coordination (REQUIRED before CI can go green)**

Use AskUserQuestion to prompt the operator for ALL of:

```
1. Set GitHub secrets:
   gh secret set FB_TEST_STREAM_KEY --body "<persistent FB stream key from a dedicated FB Live broadcast on the test FB Page>"
   gh secret set FB_BROADCAST_URL    --body "https://www.facebook.com/live/producer/<broadcast-id>"

2. On stream.lan (or via win-stream-snv MCP Shell):
   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
   -> a HEADED Chromium opens
   -> sign into Facebook with the test account
   -> close the browser
   The session is saved to C:\Users\newlevel\.playwright-fb-profile.

3. Confirm the FB test broadcast exists in Live Producer and is in
   scheduled / unpublished state (not auto-started).

Reply "operator setup complete" when all three are done.
```

Wait for operator confirmation before proceeding.

- [ ] **Step 4: Operator HEADED dry run + selector tuning (Task 5)**

After operator setup, drive Task 5's GREEN-tuning loop:

1. Operator triggers a manual OBS push to the FB endpoint via the dashboard (Start Delivering)
2. On stream.lan via MCP `win-stream-snv` Shell:
   ```powershell
   cd C:\restreamer\e2e
   $env:HEADED = "1"
   $env:FB_BROADCAST_URL = "<same URL as the secret>"
   $env:FB_SOAK_MINUTES = "1"
   $env:FB_POLL_INTERVAL_MS = "10000"
   npx playwright test -c playwright-facebook.config.ts
   ```
3. Operator reports PASS/FAIL plus, if FAIL, the screenshot at `C:\Users\newlevel\.playwright-fb-screenshots\00-initial-load.png` (use MCP `FileRead` to fetch the screenshot bytes) and the failing-selector text from the test output
4. Orchestrator edits `e2e/fb-live-producer-check.spec.ts` per Task 5 Step 2 with the corrected selectors
5. Loop steps 2-4 until operator reports PASS
6. Orchestrator commits per Task 5 Step 4 and pushes the fix

- [ ] **Step 5: Monitor CI to terminal state**

After Task 5's GREEN commit, CI re-runs. Monitor with one in-background command:

```bash
RUN_ID=$(gh run list --limit 1 --branch dev --json databaseId -q '.[0].databaseId')
# Wait for CI: sleep 300 s, then check.
```

Use `Bash` with `run_in_background: true` running `sleep 300 && gh run view <RUN_ID> --json status,conclusion,jobs`. Read the output via `BashOutput` when it returns. Repeat with 300-s sleeps until the run reaches a terminal state.

If any job is red, investigate via `gh run view <RUN_ID> --log-failed`, fix, push the fix as a new commit, monitor again.

- [ ] **Step 6: Open PR (dev → main) with closing references**

After CI is fully green:

```bash
cd /home/newlevel/devel/restreamer
gh pr create --title "feat(ci): real-FB e2e gate + 30-min rust-pusher soak (#177, #217)" --body "$(cat <<'EOF'
## Summary

PR2 of the two-PR FB-completion commitment. Locks rust-pusher → real FB Live in CI on every push to dev/main. Closes #177 (FB rust pusher root-cause final verification) and #217 (real-FB e2e CI gate).

Adds the `e2e-fb-push-stream-lan` job mirroring `e2e-obs-youtube-test`: self-hosted stream-lan runner, Hetzner delivery VPS, ffmpeg → RTMP ingest → rust pusher → real FB endpoint. Two parallel verifications: PowerShell watchdog (local chunks/bytes/zero-death) and Playwright spec against FB Live Producer DOM (preview advancing, health label, bitrate readout). 30-min sustained soak required for green.

Architecture matches the locked-in plan: Playwright + persistent Chrome profile (mirror YT), no FB Graph API, no rs-facebook crate. Per `feedback_fb_ci_mirrors_yt_decided`.

## Test plan

- [ ] CI `e2e-fb-push-stream-lan` job green on this PR's own push
- [ ] Screenshots in `fb-live-producer-screenshots` artifact show FB preview at minute 0/5/15/30
- [ ] `e2e-gate` green with FB job in its `needs:`
- [ ] Operator post-merge: log into FB Live Producer on streamsnv, confirm rust-pusher preview visible

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Capture the PR URL.

- [ ] **Step 7: Verify PR is mergeable + clean**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<NUMBER> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `{"mergeable": true, "mergeable_state": "clean"}`. If any other state, investigate and fix per `pr-merge-policy.md` before sending completion.

- [ ] **Step 8: Post-deploy verification on streamsnv via MCP**

After the PR is merged (separate user instruction — `merge it` per `pr-merge-policy.md`), main CI deploys v0.19.0 to streamsnv. Verify:

```
mcp__win-stream-snv__ListProcesses Filter:"Restreamer"
mcp__win-stream-snv__Shell Command:"(Get-Process Restreamer).MainModule.FileVersionInfo.ProductVersion"
mcp__win-stream-snv__Shell Command:"Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/endpoints | Where-Object { $_.service_type -eq 'FB' } | Select-Object alias, pusher"
```

Expected: ProductVersion=0.19.0, all FB endpoints `pusher='rust'`.

- [ ] **Step 9: Operator real-FB preview confirmation**

Per `feedback_fb_not_done_until_verified`: the completion report MUST include operator-confirmed real-FB preview observation. Use AskUserQuestion to prompt the operator to:

1. Open FB Live Producer for one of their production FB broadcasts (FB-NewLevel, FB-Zbynek, FB-GoodFest, or FB-Poprad)
2. Start delivering from streamsnv to that FB endpoint
3. Confirm the preview is visible in FB Live Producer
4. Reply with PASS/FAIL plus a short observation

If FAIL: investigate via streamsnv MCP, fix, re-deploy, re-confirm. Do NOT send the completion report until operator confirms PASS.

- [ ] **Step 10: Send completion report**

Use the EXACT template from `completion-report.md`:

```
## ✅ Work Complete

**Audits & deploy:**
✅ CI: green
✅ /plan-check: 10/10 fulfilled
✅ /review: clean — 0 🔴 0 🟡 0 🔵
✅ /requesting-code-review: clean — 0 🔴 0 🟡 0 🔵
✅ Deploy: streamsnv Restreamer.exe v0.19.0 running; FB endpoints pusher='rust'; operator confirmed real-FB Live Producer preview from rust pusher (FB-<endpoint-name>)
✅ Regression test: e2e/fb-live-producer-check.spec.ts:<line> — RED on <T4 sha>, GREEN on <T5 sha>

**E2E test coverage:**
| Feature/Fix | E2E Test File | What It Verifies |
|---|---|---|
| Rust pusher to real FB Live | e2e/fb-live-producer-check.spec.ts | FB Live Producer preview advancing, health label OK, bitrate > 0 for 30 min |

---

**Goal:** Lock FB rust delivery in CI so every push proves it still works end-to-end on real Facebook.
**What changed:** New CI job + Playwright spec + seed API + operator setup script. After this PR merges, FB regressions are caught automatically by CI; no more silent FB outages.

🌐 Dev:  https://restreamer.newlevel.media/
🌐 Prod: https://restreamer.newlevel.media/

**[restreamer] PR #<N>: feat(ci): real-FB e2e gate + 30-min rust-pusher soak (#177, #217)**
<PR URL> — mergeable, clean
```

Send as the LAST passage in the message.

---

## Verification

1. **CI job green:** `e2e-fb-push-stream-lan` succeeds on the PR's own push, including the 30-min soak.
2. **Screenshots present:** `fb-live-producer-screenshots` artifact contains at least `min-00.png`, `min-05.png`, `min-15.png`, `min-30.png`.
3. **Audit clean:** during the 30-min watchdog window, zero `endpoint_rtmp_push_died` audit rows for the `e2e fb` endpoint.
4. **No regressions:** all existing E2E + unit + integration tests still pass.
5. **Operator real-FB:** post-merge, operator confirms FB Live Producer shows the rust-pusher preview on at least one production FB endpoint.
6. **No localhost in URLs:** every URL we hand to the user is a real DNS name (e.g. `https://restreamer.newlevel.media/`). Per `no-localhost-urls.md`.

## Out of scope (NOT this PR)

- FB Graph API integration (#166) — dashboard-side FB health surface
- 4-hour sustained soak (#213) — operator manual test
- Removal of the mock-server unit test from PR #218 — stays as fast regression guard
- Per-production-endpoint CI gates beyond `e2e fb` — single test broadcast is sufficient regression-locking
- Multi-page FB testing — single dedicated test broadcast covers the rust pusher's FB code path
