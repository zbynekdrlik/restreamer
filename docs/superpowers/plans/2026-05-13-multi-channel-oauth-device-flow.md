# Multi-Channel YouTube OAuth Device Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Pre-answered: subagent-driven autonomous, no process-gate questions to the user between tasks.

**Goal:** Replace the broken Web-flow YouTube OAuth handler (`redirect_uri=127.0.0.1:8910`, never registered with Google) and the nginx-intercept manual seed hack with RFC 8628 Device Code Flow, so the operator can authorize any number of YouTube channels (bb, snv, kiko, future) directly from the dashboard without Google Cloud Console admin actions.

**Architecture:** Background tokio task per pending Device Flow grant polls Google's token endpoint until grant/deny/expire; on success persists refresh token to `youtube_oauth.label` row (schema from PR #195) plus first observed `channel_id`, then deletes the transient `oauth_device_grants` row. Adaptive cache TTL (60s healthy / 15s degraded) plus per-project quota tracker (sliding window, default 10k/day) keep the health probe within Google's quota for 7+ channels. Legacy single-row `db::v2::{get,upsert}_youtube_oauth` plus the web-flow handlers and `parse_label_from_query` are deleted in the same PR.

**Tech Stack:** Rust 2024, Axum, sqlx + SQLite (migration v27 incremental), Leptos CSR WASM, tokio background tasks, wiremock for HTTP boundary tests, Playwright for UI.

**Spec:** `docs/superpowers/specs/2026-05-13-multi-channel-oauth-device-flow-design.md` (commit `adb26e0`).

---

## File Structure

**New files:**
- `crates/rs-core/src/db/oauth_device_grants.rs` — CRUD for transient grants table (~120 LoC)
- `crates/rs-youtube/src/quota.rs` — per-project sliding-window quota tracker (~80 LoC)
- `crates/rs-youtube/src/device_flow.rs` — Google Device Code HTTP client + state machine (~250 LoC)
- `crates/rs-api/src/oauth_device.rs` — Axum handlers + background poller (~220 LoC)
- `leptos-ui/src/components/oauth_authorize.rs` — channels panel + authorize modal (~250 LoC)
- Per-test-file new modules under each crate's `tests/` or `src/*_tests.rs`

**Modified files:**
- `crates/rs-core/src/db/migrations.rs` — migrate_v27 + MAX_SCHEMA_VERSION=27
- `crates/rs-core/src/audit.rs` — `Action::OAuthGranted` variant
- `crates/rs-core/src/config.rs` — `youtube.device_flow` section
- `crates/rs-core/src/db/v2.rs` — DELETE legacy `get_youtube_oauth` + `upsert_youtube_oauth`
- `crates/rs-api/src/lib.rs` — register new modules
- `crates/rs-api/src/router.rs` — add device routes; REMOVE web-flow routes
- `crates/rs-api/src/youtube.rs` — DELETE `youtube_oauth_start`/`youtube_oauth_callback`/`parse_label_from_query`; rewrite `youtube_oauth_seed` to require `label` body field
- `crates/rs-api/src/delivery_youtube.rs` — rewrite `check_youtube_status` to iterate labels; replace `upsert_youtube_oauth` caller
- `crates/rs-api/src/delivery_status.rs` — wire adaptive TTL via `ttl_for_health` helper
- `crates/rs-youtube/src/lib.rs` — export new modules + types
- `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` — version bump 0.10.0 → 0.11.0
- `leptos-ui/src/components/mod.rs` — register `oauth_authorize`
- `leptos-ui/src/store.rs` + `leptos-ui/src/ws.rs` — thread OAuth state if needed
- `leptos-ui/style.css` — modal + table styles
- `.github/workflows/ci.yml` — seed step adds `"label":"default"` body; YOUTUBE_DEVICE_CLIENT_ID/SECRET environment

**Constraints reminded for every task:**
- Tier-2 fast-iterate: subagents do NOT compile, run tests, or push. Controller handles those.
- TDD strict: RED commit (failing test only) lands BEFORE GREEN commit (implementation).
- One commit per task. Never batch.
- Every new `.rs` file stays <1000 lines.
- ASCII-only PowerShell strings in CI YAML.
- All new helpers (quota::acquire, ttl_for_health, poll_decision, request_device_code, poll_token, spawn_grant_poller) MUST NOT be added to clippy `--exclude-re` and MUST NOT be skipped from mutation testing.

---

## Task 0 (ORCHESTRATOR ONLY — NOT a subagent task)

File the GH issue first so all subsequent commits carry `(#NNN)`.

```bash
ISSUE_URL=$(gh issue create \
  --title "Multi-channel YouTube OAuth via Device Code Flow (RFC 8628)" \
  --body "$(cat <<'EOF'
Replace the broken Web-flow OAuth handler (`redirect_uri=127.0.0.1:8910`, not registered with Google) and the nginx-intercept manual seed hack with RFC 8628 Device Code Flow.

Why: PR #195 shipped multi-channel YT health probe infrastructure but no second channel can actually authorize. Operator wants long-term-correct architecture, not small tweaks. Device Flow is the canonical SOTA pattern for headless/multi-account auth.

Scope: Device Flow endpoints + background poller + per-project quota tracker + adaptive cache TTL + dashboard authorize-channel UI + deletion of legacy Web flow + single-row OAuth helpers.

Out of scope: actual ytbb root-cause fix (depends on observed top_issue once bb is authorized — tracked in #196).

Spec: docs/superpowers/specs/2026-05-13-multi-channel-oauth-device-flow-design.md (commit adb26e0)
EOF
)")
echo "Filed: $ISSUE_URL"
# Extract issue number: ISSUE_NUM=$(echo "$ISSUE_URL" | grep -oE '[0-9]+$')
```

The orchestrator captures `$ISSUE_NUM` and substitutes it into every subsequent task prompt (`(#$ISSUE_NUM)` references in commit messages).

---

## Task 1: Version bump 0.10.0 → 0.11.0

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.package] version = "0.10.0"` → `"0.11.0"`)
- Modify: `src-tauri/Cargo.toml` (`version = "0.10.0"` → `"0.11.0"`)
- Modify: `src-tauri/tauri.conf.json` (`"version": "0.10.0"` → `"0.11.0"`)
- Modify: `leptos-ui/Cargo.toml` (`version = "0.10.0"` → `"0.11.0"`)

- [ ] **Step 1:** Read the four version strings to confirm current value.

```bash
grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"' Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml
grep -E '"version": "[0-9]+\.[0-9]+\.[0-9]+"' src-tauri/tauri.conf.json
```

- [ ] **Step 2:** Bump each file using `Edit` with exact strings:

`Cargo.toml`:
```toml
version = "0.10.0"
```
→
```toml
version = "0.11.0"
```

`src-tauri/Cargo.toml`: same swap.

`src-tauri/tauri.conf.json`:
```json
"version": "0.10.0"
```
→
```json
"version": "0.11.0"
```

`leptos-ui/Cargo.toml`: same swap as Cargo.toml.

- [ ] **Step 3:** Commit:

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.11.0 (#$ISSUE_NUM)"
```

---

## Task 2: Migration v27 — failing tests (RED)

**Files:**
- Modify: `crates/rs-core/src/db/migration_tests.rs` — append tests after the existing `test_migrate_v26_*` family

- [ ] **Step 1:** Append the following tests verbatim:

```rust
#[tokio::test]
async fn migrate_v27_adds_connected_at_column() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let cols: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('youtube_oauth')")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(cols.iter().any(|c| c == "connected_at"),
        "connected_at column missing; got {:?}", cols);
}

#[tokio::test]
async fn migrate_v27_creates_oauth_device_grants_table() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='oauth_device_grants'"
    )
    .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1, "oauth_device_grants table missing");
    let cols: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('oauth_device_grants')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for expected in ["label", "device_code", "user_code", "verification_url",
                     "interval_secs", "expires_at", "status", "error", "started_at"] {
        assert!(cols.iter().any(|c| c == expected), "missing column {expected}; got {:?}", cols);
    }
}

#[tokio::test]
async fn migrate_v27_is_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    // Re-run; must not error and must not duplicate the table.
    crate::db::run_migrations(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='oauth_device_grants'"
    )
    .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn max_schema_version_is_27() {
    assert_eq!(crate::db::migrations::MAX_SCHEMA_VERSION, 27,
        "bump MAX_SCHEMA_VERSION when adding a migration");
}
```

- [ ] **Step 2:** Commit (test-only, will fail to compile against current `MAX_SCHEMA_VERSION=26`):

```bash
git add crates/rs-core/src/db/migration_tests.rs
git commit -m "test: failing tests for v27 (connected_at + oauth_device_grants) (#$ISSUE_NUM) [red]"
```

---

## Task 3: Migration v27 — implement (GREEN)

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs:16` (`MAX_SCHEMA_VERSION`)
- Modify: `crates/rs-core/src/db/migrations.rs:344` (add dispatch arm)
- Modify: `crates/rs-core/src/db/migrations.rs:~800` (append `migrate_v27` fn)

- [ ] **Step 1:** Bump constant:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 26;
```
→
```rust
pub const MAX_SCHEMA_VERSION: i32 = 27;
```

- [ ] **Step 2:** Add dispatch arm after the existing `26 => migrate_v26(...)` line:

```rust
            26 => migrate_v26(&mut tx).await?,
```
→
```rust
            26 => migrate_v26(&mut tx).await?,
            27 => migrate_v27(&mut tx).await?,
```

- [ ] **Step 3:** Append `migrate_v27` after the existing `migrate_v26` body:

```rust
/// v27 — multi-channel OAuth Device Flow support.
/// - `youtube_oauth.connected_at TEXT` — RFC3339 timestamp set on Device Flow grant.
///   Idempotent via `add_column_if_missing` (column may not exist yet on legacy DBs).
/// - `oauth_device_grants` — transient state for pending Device Flow grants. Rows live
///   from `device-start` until `granted` / `denied` / `expired` / `error`; granted rows
///   are deleted (tokens move into `youtube_oauth`).
async fn migrate_v27(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(tx, "youtube_oauth", "connected_at", "connected_at TEXT").await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oauth_device_grants (
            label            TEXT PRIMARY KEY,
            device_code      TEXT NOT NULL,
            user_code        TEXT NOT NULL,
            verification_url TEXT NOT NULL,
            interval_secs    INTEGER NOT NULL,
            expires_at       TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'pending',
            error            TEXT,
            started_at       TEXT NOT NULL
        )",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
```

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-core/src/db/migrations.rs
git commit -m "feat(db): migration v27 — connected_at + oauth_device_grants (#$ISSUE_NUM) [green]"
```

---

## Task 4: `oauth_device_grants` CRUD — failing tests (RED)

**Files:**
- Create: `crates/rs-core/src/db/oauth_device_grants_tests.rs`
- Modify: `crates/rs-core/src/db/mod.rs` — add `#[cfg(test)] mod oauth_device_grants_tests;` line

- [ ] **Step 1:** Create the test file with full content:

```rust
//! CRUD tests for `oauth_device_grants` (pending Device Flow state).

use crate::db::oauth_device_grants as g;
use crate::db::{create_memory_pool, run_migrations};
use chrono::Utc;

async fn pool() -> sqlx::SqlitePool {
    let p = create_memory_pool().await.unwrap();
    run_migrations(&p).await.unwrap();
    p
}

#[tokio::test]
async fn insert_then_get_by_label() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "DEV123", "USR1", "https://www.google.com/device", 5, &exp, &now)
        .await
        .unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.label, "bb");
    assert_eq!(got.device_code, "DEV123");
    assert_eq!(got.user_code, "USR1");
    assert_eq!(got.verification_url, "https://www.google.com/device");
    assert_eq!(got.interval_secs, 5);
    assert_eq!(got.status, "pending");
}

#[tokio::test]
async fn insert_replaces_existing_label() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "OLD", "OLDUC", "https://x", 5, &exp, &now).await.unwrap();
    g::insert(&p, "bb", "NEW", "NEWUC", "https://x", 5, &exp, &now).await.unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.device_code, "NEW");
    assert_eq!(got.user_code, "NEWUC");
}

#[tokio::test]
async fn list_pending_returns_only_pending() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D1", "U1", "https://x", 5, &exp, &now).await.unwrap();
    g::insert(&p, "snv", "D2", "U2", "https://x", 5, &exp, &now).await.unwrap();
    g::update_status(&p, "snv", "granted", None).await.unwrap();
    let pending = g::list_pending(&p).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].label, "bb");
}

#[tokio::test]
async fn update_status_sets_error_field() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D", "U", "https://x", 5, &exp, &now).await.unwrap();
    g::update_status(&p, "bb", "error", Some("invalid_grant: bad payload")).await.unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.status, "error");
    assert_eq!(got.error.as_deref(), Some("invalid_grant: bad payload"));
}

#[tokio::test]
async fn delete_removes_row() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D", "U", "https://x", 5, &exp, &now).await.unwrap();
    g::delete(&p, "bb").await.unwrap();
    assert!(g::get_by_label(&p, "bb").await.unwrap().is_none());
}
```

- [ ] **Step 2:** Register the test module in `crates/rs-core/src/db/mod.rs`. Find the existing `pub mod youtube_oauth;` line and add:

```rust
pub mod youtube_oauth;
```
→
```rust
pub mod oauth_device_grants;
pub mod youtube_oauth;

#[cfg(test)]
mod oauth_device_grants_tests;
```

- [ ] **Step 3:** Commit (will fail to compile because `oauth_device_grants` module body not yet created):

```bash
git add crates/rs-core/src/db/oauth_device_grants_tests.rs crates/rs-core/src/db/mod.rs
git commit -m "test: failing CRUD tests for oauth_device_grants (#$ISSUE_NUM) [red]"
```

---

## Task 5: `oauth_device_grants` CRUD — implement (GREEN)

**Files:**
- Create: `crates/rs-core/src/db/oauth_device_grants.rs`

- [ ] **Step 1:** Create the module with full content:

```rust
//! CRUD for `oauth_device_grants` — transient state for pending Device Code
//! Flow grants. A row exists from `device-start` until the operator either
//! authorizes (row deleted, tokens persisted to `youtube_oauth`) or the flow
//! terminates (`status` set to `denied` / `expired` / `error`).

use crate::error::Result;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

#[derive(Debug, Clone)]
pub struct DeviceGrant {
    pub label: String,
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub interval_secs: i64,
    pub expires_at: String,
    pub status: String,
    pub error: Option<String>,
    pub started_at: String,
}

fn row_to_grant(r: sqlx::sqlite::SqliteRow) -> DeviceGrant {
    DeviceGrant {
        label: r.get("label"),
        device_code: r.get("device_code"),
        user_code: r.get("user_code"),
        verification_url: r.get("verification_url"),
        interval_secs: r.get("interval_secs"),
        expires_at: r.get("expires_at"),
        status: r.get("status"),
        error: r.get("error"),
        started_at: r.get("started_at"),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &SqlitePool,
    label: &str,
    device_code: &str,
    user_code: &str,
    verification_url: &str,
    interval_secs: i64,
    expires_at: &str,
    started_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO oauth_device_grants
            (label, device_code, user_code, verification_url, interval_secs, expires_at, status, error, started_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ?7)
         ON CONFLICT(label) DO UPDATE SET
            device_code = excluded.device_code,
            user_code = excluded.user_code,
            verification_url = excluded.verification_url,
            interval_secs = excluded.interval_secs,
            expires_at = excluded.expires_at,
            status = 'pending',
            error = NULL,
            started_at = excluded.started_at",
    )
    .bind(label)
    .bind(device_code)
    .bind(user_code)
    .bind(verification_url)
    .bind(interval_secs)
    .bind(expires_at)
    .bind(started_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_by_label(pool: &SqlitePool, label: &str) -> Result<Option<DeviceGrant>> {
    let row = sqlx::query("SELECT * FROM oauth_device_grants WHERE label = ?1")
        .bind(label)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(row_to_grant))
}

pub async fn list_pending(pool: &SqlitePool) -> Result<Vec<DeviceGrant>> {
    let rows = sqlx::query("SELECT * FROM oauth_device_grants WHERE status = 'pending' ORDER BY started_at")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(row_to_grant).collect())
}

pub async fn update_status(
    pool: &SqlitePool,
    label: &str,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE oauth_device_grants SET status = ?1, error = ?2 WHERE label = ?3")
        .bind(status)
        .bind(error)
        .bind(label)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete(pool: &SqlitePool, label: &str) -> Result<()> {
    sqlx::query("DELETE FROM oauth_device_grants WHERE label = ?1")
        .bind(label)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 2:** Commit:

```bash
git add crates/rs-core/src/db/oauth_device_grants.rs
git commit -m "feat(db): CRUD for oauth_device_grants (#$ISSUE_NUM) [green]"
```

---

## Task 6: `Action::OAuthGranted` audit variant + `youtube.device_flow` config section

**Files:**
- Modify: `crates/rs-core/src/audit.rs` — append `OAuthGranted` to the `Action` enum
- Modify: `crates/rs-core/src/config.rs` — add `DeviceFlowConfig` + wire under `YouTubeConfig`

- [ ] **Step 1:** Add `OAuthGranted` to `Action`. Find the existing `YoutubeIssueChanged` variant added in PR #195 and append:

```rust
    YoutubeIssueChanged,
```
→
```rust
    YoutubeIssueChanged,
    /// Operator successfully completed an OAuth 2.0 Device Code Flow grant
    /// for a YouTube channel. Detail JSON: `{label, channel_id, scopes}`.
    OAuthGranted,
```

- [ ] **Step 2:** In `crates/rs-core/src/config.rs`, locate the existing `pub struct YouTubeConfig { ... }`. Append a new field at the end of the struct:

```rust
    pub client_secret: String,
}
```
→
```rust
    pub client_secret: String,
    #[serde(default)]
    pub device_flow: DeviceFlowConfig,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize, PartialEq)]
pub struct DeviceFlowConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Daily quota units allowed against `liveStreams.list` (default 10000 per
    /// Google's published per-project budget). Read by the quota tracker.
    #[serde(default = "default_daily_quota")]
    pub daily_quota: u32,
}

fn default_daily_quota() -> u32 {
    10_000
}
```

Adjust the closing brace of `YouTubeConfig` accordingly. If the existing struct has additional fields below `client_secret`, place the new `device_flow` field at the end.

- [ ] **Step 3:** Commit (compile-only change — no test):

```bash
git add crates/rs-core/src/audit.rs crates/rs-core/src/config.rs
git commit -m "feat: Action::OAuthGranted + youtube.device_flow config section (#$ISSUE_NUM) [no-test: enum/config additions, no logic to test until consumed by next tasks]"
```

---

## Task 7: Quota tracker — failing tests (RED)

**Files:**
- Create: `crates/rs-youtube/src/quota_tests.rs`
- Modify: `crates/rs-youtube/src/lib.rs` — add `#[cfg(test)] mod quota_tests;`

- [ ] **Step 1:** Create the test file with full content:

```rust
//! Quota tracker contract: per-project sliding window, refill semantics,
//! exhaust + recover.

use crate::quota::{QuotaExhausted, QuotaTracker};
use std::time::Duration;

#[test]
fn acquire_under_budget_succeeds() {
    let q = QuotaTracker::new(100);
    for _ in 0..50 {
        assert!(q.acquire(1).is_ok());
    }
    assert_eq!(q.remaining(), 50);
}

#[test]
fn acquire_over_budget_returns_exhausted() {
    let q = QuotaTracker::new(10);
    for _ in 0..10 {
        q.acquire(1).unwrap();
    }
    match q.acquire(1) {
        Err(QuotaExhausted) => (),
        Ok(()) => panic!("expected QuotaExhausted"),
    }
}

#[test]
fn refill_restores_units_over_time() {
    // 100 units/day = 100 / 86_400 ≈ 0.00116 units/sec.
    // Use a small budget so the test exercises refill quickly.
    let q = QuotaTracker::new(8640); // 0.1 units/sec
    for _ in 0..8640 {
        q.acquire(1).unwrap();
    }
    assert!(q.acquire(1).is_err());
    // Travel forward 20 seconds in test time.
    q.advance_for_test(Duration::from_secs(20));
    // 20s * 0.1 units/sec = 2 units refilled.
    assert!(q.acquire(1).is_ok());
    assert!(q.acquire(1).is_ok());
    assert!(q.acquire(1).is_err());
}

#[test]
fn remaining_clamps_to_budget() {
    let q = QuotaTracker::new(10);
    // Don't acquire anything; refill should not push above budget.
    q.advance_for_test(Duration::from_secs(86_400));
    assert_eq!(q.remaining(), 10);
}
```

- [ ] **Step 2:** In `crates/rs-youtube/src/lib.rs`, add module declarations. Find the existing list of `pub mod ...;` lines and append:

```rust
pub mod streams;
```
→
```rust
pub mod quota;
pub mod streams;

#[cfg(test)]
mod quota_tests;
```

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-youtube/src/quota_tests.rs crates/rs-youtube/src/lib.rs
git commit -m "test: failing tests for QuotaTracker (#$ISSUE_NUM) [red]"
```

---

## Task 8: Quota tracker — implement (GREEN)

**Files:**
- Create: `crates/rs-youtube/src/quota.rs`

- [ ] **Step 1:** Create the module:

```rust
//! Per-project YouTube Data API quota tracker.
//! Token-bucket sliding window. Capacity = `daily_quota`, refill rate =
//! `daily_quota / 86_400` units per second. Single global instance per
//! process — Google's quota is per-project, not per-channel/endpoint.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct QuotaExhausted;

impl std::fmt::Display for QuotaExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "youtube quota exhausted")
    }
}

impl std::error::Error for QuotaExhausted {}

struct BucketState {
    units: f64,
    last_refill: Instant,
    #[cfg(test)]
    test_offset: Duration,
}

pub struct QuotaTracker {
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<BucketState>,
}

impl QuotaTracker {
    pub fn new(daily_quota: u32) -> Self {
        Self {
            capacity: daily_quota as f64,
            refill_per_sec: daily_quota as f64 / 86_400.0,
            state: Mutex::new(BucketState {
                units: daily_quota as f64,
                last_refill: Instant::now(),
                #[cfg(test)]
                test_offset: Duration::ZERO,
            }),
        }
    }

    fn refill_locked(&self, s: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(s.last_refill);
        #[cfg(test)]
        let elapsed = elapsed + s.test_offset;
        #[cfg(test)]
        {
            s.test_offset = Duration::ZERO;
        }
        let refill = elapsed.as_secs_f64() * self.refill_per_sec;
        s.units = (s.units + refill).min(self.capacity);
        s.last_refill = now;
    }

    pub fn acquire(&self, units: u32) -> Result<(), QuotaExhausted> {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        self.refill_locked(&mut s);
        let cost = units as f64;
        if s.units >= cost {
            s.units -= cost;
            Ok(())
        } else {
            Err(QuotaExhausted)
        }
    }

    pub fn remaining(&self) -> u32 {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        self.refill_locked(&mut s);
        s.units.floor() as u32
    }

    #[cfg(test)]
    pub fn advance_for_test(&self, by: Duration) {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        s.test_offset += by;
    }
}
```

- [ ] **Step 2:** Commit:

```bash
git add crates/rs-youtube/src/quota.rs
git commit -m "feat(rs-youtube): QuotaTracker — sliding window per-project budget (#$ISSUE_NUM) [green]"
```

---

## Task 9: Adaptive TTL — failing tests (RED)

**Files:**
- Create: `crates/rs-api/src/adaptive_ttl_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` — add `#[cfg(test)] mod adaptive_ttl_tests;`

- [ ] **Step 1:** Create the test file:

```rust
//! `ttl_for_health` decides cache lifetime: 60s when fully healthy,
//! 15s otherwise. Drives the per-project quota math (see spec section 11).

use crate::delivery_status::ttl_for_health;
use rs_core::models::YoutubeHealth;
use std::time::Duration;

fn health(status: &str, top_issue: Option<&str>, error: Option<&str>) -> YoutubeHealth {
    YoutubeHealth {
        stream_status: "active".into(),
        health_status: status.into(),
        top_issue: top_issue.map(String::from),
        resolution: None,
        frame_rate: None,
        age_secs: 0,
        error: error.map(String::from),
    }
}

#[test]
fn good_and_no_issue_returns_60s() {
    assert_eq!(ttl_for_health(&health("good", None, None)), Duration::from_secs(60));
}

#[test]
fn bad_returns_15s() {
    assert_eq!(ttl_for_health(&health("bad", Some("videoIngestionStarved"), None)),
               Duration::from_secs(15));
}

#[test]
fn ok_returns_15s() {
    assert_eq!(ttl_for_health(&health("ok", Some("gopSizeLong"), None)), Duration::from_secs(15));
}

#[test]
fn good_with_top_issue_returns_15s() {
    // YT can report health=good with non-empty issues (warnings). Treat as degraded.
    assert_eq!(ttl_for_health(&health("good", Some("framerateHigh"), None)),
               Duration::from_secs(15));
}

#[test]
fn error_path_returns_15s() {
    assert_eq!(ttl_for_health(&health("unknown", None, Some("probe_error"))),
               Duration::from_secs(15));
}

#[test]
fn quota_throttled_returns_15s() {
    // Throttled probes shouldn't extend their own TTL (they'd never recover).
    assert_eq!(ttl_for_health(&health("unknown", None, Some("quota_throttled"))),
               Duration::from_secs(15));
}
```

- [ ] **Step 2:** Register module. Find existing test-module declarations in `crates/rs-api/src/lib.rs` (e.g. `mod yt_health_cache_tests;`) and append:

```rust
#[cfg(test)]
mod yt_health_cache_tests;
```
→
```rust
#[cfg(test)]
mod yt_health_cache_tests;
#[cfg(test)]
mod adaptive_ttl_tests;
```

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-api/src/adaptive_ttl_tests.rs crates/rs-api/src/lib.rs
git commit -m "test: failing tests for ttl_for_health (#$ISSUE_NUM) [red]"
```

---

## Task 10: Adaptive TTL — implement + wire (GREEN)

**Files:**
- Modify: `crates/rs-api/src/delivery_status.rs` — add `ttl_for_health` helper + replace fixed 15s in `attach_yt_health_cached`

- [ ] **Step 1:** Open `crates/rs-api/src/delivery_status.rs`. Above the existing `attach_yt_health_cached` function add:

```rust
/// Adaptive cache TTL for the YT health probe. 60s when both `health_status`
/// is `good` AND no `top_issue` is set AND no `error` is present; 15s otherwise.
/// Spec section 5.
pub fn ttl_for_health(h: &rs_core::models::YoutubeHealth) -> Duration {
    if h.health_status == "good" && h.top_issue.is_none() && h.error.is_none() {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(15)
    }
}
```

- [ ] **Step 2:** Replace the fixed 15s window inside `attach_yt_health_cached`. Find:

```rust
        if age < Duration::from_secs(15) {
```
→
```rust
        if age < ttl_for_health(&h) {
```

(Note: `h` is the cached health snapshot pulled from the entry. The TTL is decided by the *prior* cached health — fresh-degraded entries get 15s, fresh-healthy entries get 60s, which is exactly the adaptive behavior we want.)

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-api/src/delivery_status.rs
git commit -m "feat(api): adaptive cache TTL — 60s healthy / 15s degraded (#$ISSUE_NUM) [green]"
```

---

## Task 11: Device Flow state machine + HTTP client — failing tests (RED)

**Files:**
- Create: `crates/rs-youtube/src/device_flow_tests.rs`
- Modify: `crates/rs-youtube/src/lib.rs` — `pub mod device_flow;` + `#[cfg(test)] mod device_flow_tests;`

- [ ] **Step 1:** Create the test file:

```rust
//! Device Code Flow contract: HTTP client against wiremock'd Google
//! endpoints + poll state machine.

use crate::device_flow::{
    PollDecision, PollResponse, request_device_code, poll_token, poll_decision,
};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn poll_decision_pending_continues() {
    let dec = poll_decision(&PollResponse::Pending);
    matches!(dec, PollDecision::Continue { .. });
}

#[test]
fn poll_decision_slow_down_doubles_interval() {
    let dec = poll_decision(&PollResponse::SlowDown);
    match dec {
        PollDecision::DoubleInterval => (),
        other => panic!("expected DoubleInterval; got {:?}", other),
    }
}

#[test]
fn poll_decision_denied_is_terminal() {
    matches!(poll_decision(&PollResponse::Denied), PollDecision::TerminalDenied);
}

#[test]
fn poll_decision_expired_is_terminal() {
    matches!(poll_decision(&PollResponse::Expired), PollDecision::TerminalExpired);
}

#[test]
fn poll_decision_granted_is_terminal_with_tokens() {
    let dec = poll_decision(&PollResponse::Granted {
        access_token: "AT".into(),
        refresh_token: "RT".into(),
        expires_in: Some(3600),
        id_token: None,
    });
    match dec {
        PollDecision::TerminalGranted { access_token, refresh_token, .. } => {
            assert_eq!(access_token, "AT");
            assert_eq!(refresh_token, "RT");
        }
        other => panic!("expected TerminalGranted; got {:?}", other),
    }
}

#[tokio::test]
async fn request_device_code_parses_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/device/code"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"device_code":"D1","user_code":"USR-XYZ","verification_url":"https://www.google.com/device","expires_in":1800,"interval":5}"#
        ))
        .expect(1)
        .mount(&server)
        .await;
    let r = request_device_code(&server.uri(), "client_id", "scope1 scope2").await.unwrap();
    assert_eq!(r.device_code, "D1");
    assert_eq!(r.user_code, "USR-XYZ");
    assert_eq!(r.verification_url, "https://www.google.com/device");
    assert_eq!(r.expires_in, 1800);
    assert_eq!(r.interval, 5);
}

#[tokio::test]
async fn poll_token_pending() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .and(body_string_contains("grant_type=urn"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            r#"{"error":"authorization_pending"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "client_id", "client_secret", "DEVICE").await.unwrap();
    matches!(r, PollResponse::Pending);
}

#[tokio::test]
async fn poll_token_slow_down() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            r#"{"error":"slow_down"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    matches!(r, PollResponse::SlowDown);
}

#[tokio::test]
async fn poll_token_granted_parses_tokens() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"AT","refresh_token":"RT","expires_in":3600,"token_type":"Bearer","scope":"yt"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    match r {
        PollResponse::Granted { access_token, refresh_token, expires_in, .. } => {
            assert_eq!(access_token, "AT");
            assert_eq!(refresh_token, "RT");
            assert_eq!(expires_in, Some(3600));
        }
        other => panic!("expected Granted, got {:?}", other),
    }
}

#[tokio::test]
async fn poll_token_denied() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            r#"{"error":"access_denied"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    matches!(r, PollResponse::Denied);
}

#[tokio::test]
async fn poll_token_expired() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            r#"{"error":"expired_token"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    matches!(r, PollResponse::Expired);
}
```

- [ ] **Step 2:** Add module declarations:

```rust
pub mod quota;
pub mod streams;
```
→
```rust
pub mod device_flow;
pub mod quota;
pub mod streams;

#[cfg(test)]
mod device_flow_tests;
```

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-youtube/src/device_flow_tests.rs crates/rs-youtube/src/lib.rs
git commit -m "test: failing tests for Device Flow HTTP client + state machine (#$ISSUE_NUM) [red]"
```

---

## Task 12: Device Flow state machine + HTTP client — implement (GREEN)

**Files:**
- Create: `crates/rs-youtube/src/device_flow.rs`

- [ ] **Step 1:** Create the module:

```rust
//! Google OAuth 2.0 Device Code Flow (RFC 8628) client.
//! Two HTTP calls: `request_device_code` (operator-facing prompt) and
//! `poll_token` (background poll until grant/deny/expire).

use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceCodeResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: i64,
    pub interval: i64,
}

#[derive(Debug)]
pub enum PollResponse {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Granted {
        access_token: String,
        refresh_token: String,
        expires_in: Option<i64>,
        id_token: Option<String>,
    },
    Error(String),
}

#[derive(Debug)]
pub enum PollDecision {
    Continue,
    DoubleInterval,
    TerminalDenied,
    TerminalExpired,
    TerminalGranted {
        access_token: String,
        refresh_token: String,
        expires_in: Option<i64>,
        id_token: Option<String>,
    },
    TerminalError(String),
}

#[derive(thiserror::Error, Debug)]
pub enum DeviceFlowError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("response parse error: {0}")]
    Parse(String),
}

pub async fn request_device_code(
    base_url: &str,
    client_id: &str,
    scope: &str,
) -> Result<DeviceCodeResponse, DeviceFlowError> {
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/device/code"))
        .form(&[("client_id", client_id), ("scope", scope)])
        .send()
        .await?;
    let body = resp.text().await?;
    serde_json::from_str::<DeviceCodeResponse>(&body)
        .map_err(|e| DeviceFlowError::Parse(format!("{e}: body={body}")))
}

#[derive(Deserialize)]
struct TokenSuccess {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    id_token: Option<String>,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
}

pub async fn poll_token(
    base_url: &str,
    client_id: &str,
    client_secret: &str,
    device_code: &str,
) -> Result<PollResponse, DeviceFlowError> {
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/token"))
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("device_code", device_code),
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
        ])
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;

    if status.is_success() {
        let ts: TokenSuccess = serde_json::from_str(&body)
            .map_err(|e| DeviceFlowError::Parse(format!("{e}: body={body}")))?;
        let refresh_token = ts.refresh_token.ok_or_else(|| {
            DeviceFlowError::Parse("token response missing refresh_token".to_string())
        })?;
        return Ok(PollResponse::Granted {
            access_token: ts.access_token,
            refresh_token,
            expires_in: ts.expires_in,
            id_token: ts.id_token,
        });
    }

    match serde_json::from_str::<TokenError>(&body) {
        Ok(te) => match te.error.as_str() {
            "authorization_pending" => Ok(PollResponse::Pending),
            "slow_down" => Ok(PollResponse::SlowDown),
            "access_denied" => Ok(PollResponse::Denied),
            "expired_token" => Ok(PollResponse::Expired),
            other => Ok(PollResponse::Error(other.to_string())),
        },
        Err(_) => Ok(PollResponse::Error(format!("HTTP {status}: {body}"))),
    }
}

pub fn poll_decision(resp: &PollResponse) -> PollDecision {
    match resp {
        PollResponse::Pending => PollDecision::Continue,
        PollResponse::SlowDown => PollDecision::DoubleInterval,
        PollResponse::Denied => PollDecision::TerminalDenied,
        PollResponse::Expired => PollDecision::TerminalExpired,
        PollResponse::Granted { access_token, refresh_token, expires_in, id_token } => {
            PollDecision::TerminalGranted {
                access_token: access_token.clone(),
                refresh_token: refresh_token.clone(),
                expires_in: *expires_in,
                id_token: id_token.clone(),
            }
        }
        PollResponse::Error(e) => PollDecision::TerminalError(e.clone()),
    }
}
```

- [ ] **Step 2:** Commit:

```bash
git add crates/rs-youtube/src/device_flow.rs
git commit -m "feat(rs-youtube): Device Code Flow HTTP client + state machine (#$ISSUE_NUM) [green]"
```

---

## Task 13: `device-start` Axum handler — failing tests (RED)

**Files:**
- Create: `crates/rs-api/src/oauth_device_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` — register test module

- [ ] **Step 1:** Create the test file:

```rust
//! Axum handlers for the Device Code Flow.

use crate::oauth_device::{DeviceStartBody, DeviceStartResponse, device_start};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::{create_memory_pool, run_migrations};
use rs_core::db::youtube_oauth as yo;

async fn make_state_with_device_config(api_base: &str) -> crate::state::AppState {
    use crate::state::AppState;
    use rs_core::config::{Config, DeviceFlowConfig, YouTubeConfig};
    use std::sync::Arc;
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let mut config = Config::for_testing();
    config.youtube.device_flow = DeviceFlowConfig {
        client_id: "test-client".into(),
        client_secret: "test-secret".into(),
        daily_quota: 10_000,
    };
    AppState::for_testing(pool, Arc::new(config), api_base.to_string())
}

#[tokio::test]
async fn device_start_rejects_invalid_label() {
    let state = make_state_with_device_config("http://unused").await;
    let r = device_start(
        State(state),
        Json(DeviceStartBody { label: "Bad Label!".into() }),
    ).await;
    assert!(matches!(r, Err(StatusCode::BAD_REQUEST)),
        "invalid label must yield 400; got {:?}", r.err());
}

#[tokio::test]
async fn device_start_409_when_label_already_authorized() {
    use wiremock::MockServer;
    let server = MockServer::start().await;
    let state = make_state_with_device_config(&server.uri()).await;
    // Seed an existing oauth grant for label="bb".
    yo::upsert_oauth_by_label(&state.pool, "bb", "AT", "RT",
        "https://oauth2.googleapis.com/token", "cid", "csec", "scope",
        Some("2099-01-01T00:00:00Z")).await.unwrap();
    let r = device_start(
        State(state),
        Json(DeviceStartBody { label: "bb".into() }),
    ).await;
    assert!(matches!(r, Err(StatusCode::CONFLICT)));
}

#[tokio::test]
async fn device_start_happy_path_persists_grant() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/device/code"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"device_code":"DEV","user_code":"AB-CD-12","verification_url":"https://www.google.com/device","expires_in":1800,"interval":5}"#
        ))
        .mount(&server).await;
    let state = make_state_with_device_config(&server.uri()).await;
    let pool = state.pool.clone();
    let r = device_start(
        State(state),
        Json(DeviceStartBody { label: "bb".into() }),
    ).await;
    let Json(resp): Json<DeviceStartResponse> = r.expect("ok");
    assert_eq!(resp.user_code, "AB-CD-12");
    assert_eq!(resp.verification_url, "https://www.google.com/device");
    assert_eq!(resp.expires_in, 1800);
    // Grant row persisted.
    use rs_core::db::oauth_device_grants as g;
    let got = g::get_by_label(&pool, "bb").await.unwrap().expect("row");
    assert_eq!(got.status, "pending");
    assert_eq!(got.user_code, "AB-CD-12");
}
```

- [ ] **Step 2:** Register module:

```rust
#[cfg(test)]
mod adaptive_ttl_tests;
```
→
```rust
#[cfg(test)]
mod adaptive_ttl_tests;
#[cfg(test)]
mod oauth_device_tests;
```

- [ ] **Step 3:** Add a test-only helper `AppState::for_testing` if not already present. Check `crates/rs-api/src/state.rs` for an existing constructor used by tests; if not, add to that file:

```rust
#[cfg(test)]
impl AppState {
    pub fn for_testing(
        pool: SqlitePool,
        config: Arc<Config>,
        device_api_base: String,
    ) -> Self {
        // ... minimal stub state suitable for unit-testing handlers ...
        // (Pattern matches existing test helpers in this file. If no helper
        // exists, this signature must be adopted in Task 14 alongside the handler.)
        todo!("preserve existing test-state pattern from prior PRs; see delivery_status_yt_health_tests.rs for the established shape")
    }
}
```

If the existing test pattern already uses `AppState { pool, config, ws_tx, ... }` literal construction inline, mirror that approach instead. The key: tests need a state that carries `pool`, `config.youtube.device_flow`, and a way to override the Google API base URL — store the override in `AppState` as `pub device_flow_api_base: Option<String>` (with `#[serde(skip)]` on Config side) or in the function signature of `device_start`. Simplest: add `device_flow_api_base: Option<String>` to `AppState`.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-api/src/oauth_device_tests.rs crates/rs-api/src/lib.rs crates/rs-api/src/state.rs
git commit -m "test: failing tests for device-start handler (#$ISSUE_NUM) [red]"
```

---

## Task 14: `device-start` Axum handler — implement (GREEN)

**Files:**
- Create: `crates/rs-api/src/oauth_device.rs`
- Modify: `crates/rs-api/src/lib.rs` — `pub mod oauth_device;`
- Modify: `crates/rs-api/src/state.rs` — add `pub device_flow_api_base: Option<String>` field (default `None`; tests set it to wiremock URI)

- [ ] **Step 1:** Add `device_flow_api_base` to `AppState`:

```rust
pub cached_delivery: Arc<std::sync::RwLock<CachedDeliveryStatus>>,
```
→
```rust
pub cached_delivery: Arc<std::sync::RwLock<CachedDeliveryStatus>>,
/// Override for the Google OAuth API base URL (used in tests to point at
/// wiremock). Production reads from `https://oauth2.googleapis.com`.
pub device_flow_api_base: Option<String>,
```

Update every existing `AppState { ... }` constructor (search for `AppState {` in the crate) to include `device_flow_api_base: None,`.

- [ ] **Step 2:** Create `crates/rs-api/src/oauth_device.rs`:

```rust
//! Device Code Flow Axum handlers + background poller.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::state::AppState;

const GOOGLE_OAUTH_BASE: &str = "https://oauth2.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/youtube.readonly openid";
const LABEL_PATTERN: &str = "^[a-z0-9_]{1,32}$";

fn is_valid_label(s: &str) -> bool {
    let re = regex::Regex::new(LABEL_PATTERN).expect("static regex");
    re.is_match(s)
}

#[derive(Debug, Deserialize)]
pub struct DeviceStartBody {
    pub label: String,
}

#[derive(Debug, Serialize)]
pub struct DeviceStartResponse {
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: i64,
}

pub async fn device_start(
    State(state): State<AppState>,
    Json(body): Json<DeviceStartBody>,
) -> Result<Json<DeviceStartResponse>, StatusCode> {
    if !is_valid_label(&body.label) {
        error!("device_start: invalid label '{}'", body.label);
        return Err(StatusCode::BAD_REQUEST);
    }
    // 409 if already authorized.
    if let Some(existing) = rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, &body.label)
        .await
        .map_err(|e| {
            error!("device_start: oauth lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        && !existing.refresh_token.is_empty()
    {
        return Err(StatusCode::CONFLICT);
    }

    let device_cfg = &state.config.youtube.device_flow;
    if device_cfg.client_id.is_empty() || device_cfg.client_secret.is_empty() {
        error!("device_start: youtube.device_flow client_id/secret not configured");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let base = state.device_flow_api_base.clone()
        .unwrap_or_else(|| GOOGLE_OAUTH_BASE.to_string());

    let resp = rs_youtube::device_flow::request_device_code(&base, &device_cfg.client_id, SCOPE)
        .await
        .map_err(|e| {
            error!("device_start: device/code request failed: {e}");
            StatusCode::BAD_GATEWAY
        })?;

    let now = Utc::now();
    let expires_at = (now + chrono::Duration::seconds(resp.expires_in)).to_rfc3339();
    let started_at = now.to_rfc3339();

    rs_core::db::oauth_device_grants::insert(
        &state.pool,
        &body.label,
        &resp.device_code,
        &resp.user_code,
        &resp.verification_url,
        resp.interval,
        &expires_at,
        &started_at,
    )
    .await
    .map_err(|e| {
        error!("device_start: insert grant failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Spawn background poller for this grant.
    spawn_grant_poller(
        state.pool.clone(),
        state.audit_tx.clone(),
        base,
        device_cfg.client_id.clone(),
        device_cfg.client_secret.clone(),
        body.label.clone(),
        resp.device_code.clone(),
        resp.interval,
        expires_at.clone(),
    );

    info!("device_start: pending grant for label='{}'", body.label);
    Ok(Json(DeviceStartResponse {
        user_code: resp.user_code,
        verification_url: resp.verification_url,
        expires_in: resp.expires_in,
    }))
}

pub fn spawn_grant_poller(
    pool: sqlx::SqlitePool,
    audit_tx: tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
    api_base: String,
    client_id: String,
    client_secret: String,
    label: String,
    device_code: String,
    initial_interval: i64,
    expires_at: String,
) {
    tokio::spawn(async move {
        let mut interval = initial_interval.max(1) as u64;
        let exp = chrono::DateTime::parse_from_rfc3339(&expires_at)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| Utc::now() + chrono::Duration::seconds(900));
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            if Utc::now() > exp {
                let _ = rs_core::db::oauth_device_grants::update_status(
                    &pool, &label, "expired", None).await;
                warn!("device_poller: label='{label}' expired");
                return;
            }
            let resp = match rs_youtube::device_flow::poll_token(
                &api_base, &client_id, &client_secret, &device_code).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("device_poller: poll error for '{label}': {e}");
                    continue;
                }
            };
            use rs_youtube::device_flow::PollDecision::*;
            match rs_youtube::device_flow::poll_decision(&resp) {
                Continue => continue,
                DoubleInterval => { interval *= 2; continue; }
                TerminalDenied => {
                    let _ = rs_core::db::oauth_device_grants::update_status(
                        &pool, &label, "denied", None).await;
                    return;
                }
                TerminalExpired => {
                    let _ = rs_core::db::oauth_device_grants::update_status(
                        &pool, &label, "expired", None).await;
                    return;
                }
                TerminalError(e) => {
                    let _ = rs_core::db::oauth_device_grants::update_status(
                        &pool, &label, "error", Some(&e)).await;
                    return;
                }
                TerminalGranted { access_token, refresh_token, expires_in, .. } => {
                    let new_expires = expires_in.map(|s|
                        (Utc::now() + chrono::Duration::seconds(s)).to_rfc3339());
                    if let Err(e) = rs_core::db::youtube_oauth::upsert_oauth_by_label(
                        &pool, &label, &access_token, &refresh_token,
                        "https://oauth2.googleapis.com/token",
                        &client_id, &client_secret, SCOPE,
                        new_expires.as_deref(),
                    ).await {
                        error!("device_poller: upsert failed for '{label}': {e}");
                        let _ = rs_core::db::oauth_device_grants::update_status(
                            &pool, &label, "error", Some(&format!("upsert: {e}"))).await;
                        return;
                    }
                    // Capture channel_id from first liveStreams.list call.
                    let channel_id = rs_youtube::streams::list_streams_for_label(&pool, &label)
                        .await
                        .ok()
                        .and_then(|streams| streams.first().and_then(|s|
                            s.snippet.channel_id.clone().or_else(|| s.snippet.title.clone().into())));
                    let connected_at = Utc::now().to_rfc3339();
                    let _ = sqlx::query(
                        "UPDATE youtube_oauth SET channel_id = ?1, connected_at = ?2 WHERE label = ?3"
                    ).bind(&channel_id).bind(&connected_at).bind(&label)
                    .execute(&pool).await;
                    let _ = rs_core::db::oauth_device_grants::delete(&pool, &label).await;
                    let row = rs_core::audit::AuditRow {
                        severity: rs_core::audit::Severity::Info,
                        source: rs_core::audit::Source::Operator,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: rs_core::audit::Action::OAuthGranted,
                        detail: serde_json::json!({
                            "label": label,
                            "channel_id": channel_id,
                            "scopes": SCOPE,
                        }),
                        ts_override: None,
                    };
                    let _ = audit_tx.send(row).await;
                    info!("device_poller: label='{label}' GRANTED (channel_id={:?})", channel_id);
                    return;
                }
            }
        }
    });
}

#[derive(Debug, Deserialize)]
pub struct DeviceStatusQuery {
    pub label: String,
}

#[derive(Debug, Serialize)]
pub struct DeviceStatusResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn device_status(
    State(state): State<AppState>,
    Query(q): Query<DeviceStatusQuery>,
) -> Result<Json<DeviceStatusResponse>, StatusCode> {
    if !is_valid_label(&q.label) {
        return Err(StatusCode::BAD_REQUEST);
    }
    // If oauth row exists with non-empty refresh_token -> granted (with channel_id).
    if let Some(o) = rs_core::db::youtube_oauth::get_oauth_by_label(&state.pool, &q.label)
        .await
        .map_err(|e| {
            error!("device_status: oauth lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        && !o.refresh_token.is_empty()
    {
        return Ok(Json(DeviceStatusResponse {
            status: "granted".into(),
            user_code: None,
            verification_url: None,
            channel_id: o.channel_id,
            connected_at: o.connected_at,
            error: None,
        }));
    }
    // Otherwise read pending/denied/expired/error from the grants table.
    let g = rs_core::db::oauth_device_grants::get_by_label(&state.pool, &q.label)
        .await
        .map_err(|e| {
            error!("device_status: grant lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    match g {
        Some(g) => Ok(Json(DeviceStatusResponse {
            status: g.status,
            user_code: Some(g.user_code),
            verification_url: Some(g.verification_url),
            channel_id: None,
            connected_at: None,
            error: g.error,
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}
```

Add `regex = "1"` to `crates/rs-api/Cargo.toml` `[dependencies]` if not already present.

- [ ] **Step 3:** Register module in `crates/rs-api/src/lib.rs`. Add near other `pub mod` declarations:

```rust
pub mod oauth_device;
```

- [ ] **Step 4:** Add route to `crates/rs-api/src/router.rs`. Locate the existing `/youtube/oauths` route and append two new routes immediately below:

```rust
        .route("/youtube/oauths", get(youtube::list_oauths))
```
→
```rust
        .route("/youtube/oauths", get(youtube::list_oauths))
        .route("/youtube/oauth/device-start", post(crate::oauth_device::device_start))
        .route("/youtube/oauth/device-status", get(crate::oauth_device::device_status))
```

- [ ] **Step 5:** Note for the model: `crates/rs-youtube/src/streams.rs::StreamSnippet` does not currently expose `channel_id`. Add it now with `#[serde(default)] pub channel_id: Option<String>` so `list_streams_for_label(...).first().snippet.channel_id` compiles cleanly. Update the struct in-place:

```rust
pub struct StreamSnippet {
    pub title: Option<String>,
}
```
→
```rust
pub struct StreamSnippet {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default, rename = "channelId")]
    pub channel_id: Option<String>,
}
```

- [ ] **Step 6:** Commit:

```bash
git add crates/rs-api/src/oauth_device.rs crates/rs-api/src/lib.rs \
        crates/rs-api/src/router.rs crates/rs-api/src/state.rs \
        crates/rs-api/Cargo.toml crates/rs-youtube/src/streams.rs
git commit -m "feat(api): device-start + device-status handlers + grant poller (#$ISSUE_NUM) [green]"
```

---

## Task 15: Crash recovery for pending grants — failing test (RED)

**Files:**
- Modify: `crates/rs-api/src/oauth_device_tests.rs` — add recovery test

- [ ] **Step 1:** Append:

```rust
#[tokio::test]
async fn resume_pending_grants_on_startup() {
    use rs_core::db::oauth_device_grants as g;
    use chrono::Utc;
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&pool, "bb", "DEV", "USR", "https://x", 5, &exp, &Utc::now().to_rfc3339())
        .await.unwrap();
    // Pre-existing grant. Now resume.
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let resumed = crate::oauth_device::resume_pending_grants(
        &pool, &tx, "http://example.test", "cid", "csec"
    ).await.unwrap();
    assert_eq!(resumed, 1, "must resume exactly one pending grant");
}

#[tokio::test]
async fn resume_marks_expired_grants() {
    use rs_core::db::oauth_device_grants as g;
    use chrono::Utc;
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    // Grant whose expires_at is in the past.
    let past = (Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    g::insert(&pool, "bb", "DEV", "USR", "https://x", 5, &past, &Utc::now().to_rfc3339())
        .await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let resumed = crate::oauth_device::resume_pending_grants(
        &pool, &tx, "http://example.test", "cid", "csec"
    ).await.unwrap();
    assert_eq!(resumed, 0, "expired grants must not be resumed");
    let got = g::get_by_label(&pool, "bb").await.unwrap().expect("row");
    assert_eq!(got.status, "expired");
}
```

- [ ] **Step 2:** Commit:

```bash
git add crates/rs-api/src/oauth_device_tests.rs
git commit -m "test: failing tests for crash-recovery of pending grants (#$ISSUE_NUM) [red]"
```

---

## Task 16: Crash recovery — implement (GREEN)

**Files:**
- Modify: `crates/rs-api/src/oauth_device.rs` — add `resume_pending_grants`

- [ ] **Step 1:** Append to `oauth_device.rs`:

```rust
/// On startup, scan `oauth_device_grants WHERE status='pending'`. For each
/// row whose `expires_at` is still in the future, spawn a poller. For each
/// row already expired, update its status to `expired`. Returns the number
/// of pollers actually spawned.
pub async fn resume_pending_grants(
    pool: &sqlx::SqlitePool,
    audit_tx: &tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
    api_base: &str,
    client_id: &str,
    client_secret: &str,
) -> sqlx::Result<usize> {
    let pending = rs_core::db::oauth_device_grants::list_pending(pool)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let mut resumed = 0usize;
    for g in pending {
        let exp = chrono::DateTime::parse_from_rfc3339(&g.expires_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        if Utc::now() > exp {
            let _ = rs_core::db::oauth_device_grants::update_status(
                pool, &g.label, "expired", None).await;
            continue;
        }
        spawn_grant_poller(
            pool.clone(),
            audit_tx.clone(),
            api_base.to_string(),
            client_id.to_string(),
            client_secret.to_string(),
            g.label,
            g.device_code,
            g.interval_secs,
            g.expires_at,
        );
        resumed += 1;
    }
    Ok(resumed)
}
```

- [ ] **Step 2:** Wire `resume_pending_grants` into application startup. Locate the existing service init in `crates/rs-service/src/main.rs` or `crates/rs-runtime/src/lib.rs` (whichever currently calls `run_migrations`). Immediately after migrations succeed, add:

```rust
// Resume any pending OAuth Device Flow grants left over from a previous run.
if !config.youtube.device_flow.client_id.is_empty() {
    if let Err(e) = rs_api::oauth_device::resume_pending_grants(
        &pool,
        &audit_tx,
        "https://oauth2.googleapis.com",
        &config.youtube.device_flow.client_id,
        &config.youtube.device_flow.client_secret,
    ).await {
        tracing::warn!("resume_pending_grants failed: {e}");
    }
}
```

(Substitute the actual variable names used in the surrounding init code. If the audit channel is created later than the pool, place this call after both exist.)

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-api/src/oauth_device.rs crates/rs-service/src/main.rs crates/rs-runtime/src/lib.rs
git commit -m "feat(api): resume_pending_grants on startup (#$ISSUE_NUM) [green]"
```

(Stage whichever of the two service files actually receives the change; do not stage files that didn't change.)

---

## Task 17: Rewrite `youtube_oauth_seed` + `check_youtube_status` for multi-label — failing tests (RED)

**Files:**
- Create: `crates/rs-api/src/multi_label_oauth_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` — register `#[cfg(test)] mod multi_label_oauth_tests;`

- [ ] **Step 1:** Create the test file:

```rust
//! `/youtube/oauth/seed` now requires `label`. `check_youtube_status` returns
//! a per-label array.

use crate::youtube::{YouTubeOAuthSeedRequest, youtube_oauth_seed, YouTubeStatusPerChannel};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_core::db::{create_memory_pool, run_migrations};

#[tokio::test]
async fn seed_with_label_persists_by_label() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let state = crate::state::AppState::for_testing(
        pool.clone(),
        std::sync::Arc::new(rs_core::config::Config::for_testing()),
        String::new(),
    );
    let r = youtube_oauth_seed(
        State(state),
        Json(YouTubeOAuthSeedRequest {
            label: "default".into(),
            refresh_token: "RT_DEFAULT".into(),
            client_id: "cid".into(),
            client_secret: "csec".into(),
        }),
    ).await.unwrap();
    assert_eq!(r, StatusCode::OK);
    let row = rs_core::db::youtube_oauth::get_oauth_by_label(&pool, "default")
        .await.unwrap().expect("row");
    assert_eq!(row.refresh_token, "RT_DEFAULT");
}

#[tokio::test]
async fn seed_rejects_missing_label() {
    // Body without label fails Json<YouTubeOAuthSeedRequest> deserialization.
    let body = serde_json::json!({
        "refresh_token": "X",
        "client_id": "cid",
        "client_secret": "csec",
    });
    let parsed: Result<YouTubeOAuthSeedRequest, _> = serde_json::from_value(body);
    assert!(parsed.is_err(), "label must be required");
}

#[tokio::test]
async fn youtube_status_returns_per_channel_array() {
    use rs_core::db::youtube_oauth as yo;
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(&pool, "default", "AT", "RT",
        "https://oauth2.googleapis.com/token", "c", "s", "scope",
        Some("2099-01-01T00:00:00Z")).await.unwrap();
    yo::upsert_oauth_by_label(&pool, "bb", "AT2", "RT2",
        "https://oauth2.googleapis.com/token", "c", "s", "scope",
        Some("2099-01-01T00:00:00Z")).await.unwrap();
    let v: Vec<YouTubeStatusPerChannel> = crate::youtube::check_all_youtube_status(&pool).await;
    assert_eq!(v.len(), 2);
    assert!(v.iter().any(|s| s.label == "default" && s.authenticated));
    assert!(v.iter().any(|s| s.label == "bb" && s.authenticated));
}
```

- [ ] **Step 2:** Register module:

```rust
#[cfg(test)]
mod oauth_device_tests;
```
→
```rust
#[cfg(test)]
mod oauth_device_tests;
#[cfg(test)]
mod multi_label_oauth_tests;
```

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-api/src/multi_label_oauth_tests.rs crates/rs-api/src/lib.rs
git commit -m "test: failing tests for label-aware seed + multi-channel status (#$ISSUE_NUM) [red]"
```

---

## Task 18: Rewrite `youtube_oauth_seed` + `check_youtube_status` — implement (GREEN)

**Files:**
- Modify: `crates/rs-api/src/youtube.rs`
- Modify: `crates/rs-api/src/delivery_youtube.rs`

- [ ] **Step 1:** Replace `YouTubeOAuthSeedRequest` + `youtube_oauth_seed` in `crates/rs-api/src/youtube.rs`. Find the existing definitions and rewrite:

```rust
#[derive(Debug, Deserialize)]
pub struct YouTubeOAuthSeedRequest {
    pub label: String,
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
}

pub async fn youtube_oauth_seed(
    State(state): State<AppState>,
    Json(req): Json<YouTubeOAuthSeedRequest>,
) -> Result<StatusCode, StatusCode> {
    rs_core::db::youtube_oauth::upsert_oauth_by_label(
        &state.pool,
        &req.label,
        "",
        &req.refresh_token,
        "https://oauth2.googleapis.com/token",
        &req.client_id,
        &req.client_secret,
        "https://www.googleapis.com/auth/youtube.readonly",
        None,
    )
    .await
    .map_err(|e| {
        error!("seed failed for label '{}': {e}", req.label);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    tracing::info!("youtube oauth seeded for label '{}'", req.label);
    Ok(StatusCode::OK)
}
```

- [ ] **Step 2:** Add new types + helper at the top of the file (under the existing imports):

```rust
#[derive(Debug, serde::Serialize)]
pub struct YouTubeStatusPerChannel {
    pub label: String,
    pub channel_id: Option<String>,
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub error: Option<String>,
    pub connected_at: Option<String>,
}

pub async fn check_all_youtube_status(pool: &sqlx::SqlitePool) -> Vec<YouTubeStatusPerChannel> {
    let oauths = match rs_core::db::youtube_oauth::list_oauths(pool).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("list_oauths failed: {e}");
            return vec![];
        }
    };
    let mut out = Vec::with_capacity(oauths.len());
    for o in oauths {
        if o.refresh_token.is_empty() {
            out.push(YouTubeStatusPerChannel {
                label: o.label,
                channel_id: o.channel_id,
                authenticated: false,
                stream_receiving: None,
                error: None,
                connected_at: o.connected_at,
            });
            continue;
        }
        let receiving = rs_youtube::streams::list_streams_for_label(pool, &o.label)
            .await
            .ok()
            .map(|streams| streams.iter().any(|s| s.status.stream_status == "active"));
        out.push(YouTubeStatusPerChannel {
            label: o.label,
            channel_id: o.channel_id,
            authenticated: true,
            stream_receiving: receiving,
            error: None,
            connected_at: o.connected_at,
        });
    }
    out
}
```

- [ ] **Step 3:** Replace the existing `get_youtube_status` Axum handler (search for `/youtube/status` route binding) to call `check_all_youtube_status` and return `Json<Vec<YouTubeStatusPerChannel>>`. Adjust handler signature + return type accordingly:

```rust
pub async fn get_youtube_status(
    State(state): State<AppState>,
) -> Json<Vec<YouTubeStatusPerChannel>> {
    Json(check_all_youtube_status(&state.pool).await)
}
```

- [ ] **Step 4:** Rewrite `crates/rs-api/src/delivery_youtube.rs`. Replace the entire impl block:

```rust
//! Per-label YouTube refresh helper used by the health probe.
//! The legacy single-channel `check_youtube_status` is replaced by
//! `youtube::check_all_youtube_status`.

use tracing::warn;

use rs_core::db::youtube_oauth as yo;
use rs_youtube::oauth;

use crate::delivery::DeliveryOrchestrator;

impl DeliveryOrchestrator {
    /// Refresh OAuth tokens for a single label if expired. Returns the access
    /// token. Used by code paths that previously called the deleted
    /// `db::get_youtube_oauth` directly.
    pub async fn refresh_token_for_label(&self, label: &str) -> Option<String> {
        let oauth_row = yo::get_oauth_by_label(self.pool(), label).await.ok().flatten()?;
        if oauth_row.refresh_token.is_empty() {
            return None;
        }
        if !oauth::is_token_expired(oauth_row.expires_at.as_deref()) {
            return Some(oauth_row.access_token);
        }
        let tokens = rs_youtube::OAuthTokens {
            access_token: oauth_row.access_token.clone(),
            refresh_token: oauth_row.refresh_token.clone(),
            token_uri: oauth_row.token_uri.clone(),
            client_id: oauth_row.client_id.clone(),
            client_secret: oauth_row.client_secret.clone(),
            scopes: oauth_row.scopes.clone(),
            expires_at: oauth_row.expires_at.clone(),
        };
        let refreshed = match oauth::refresh_access_token(&tokens).await {
            Ok(r) => r,
            Err(e) => {
                warn!("refresh_token_for_label '{label}' failed: {e}");
                return None;
            }
        };
        let new_expires = refreshed.expires_in.map(|s|
            (chrono::Utc::now() + chrono::Duration::seconds(s)).to_rfc3339());
        if let Err(e) = yo::upsert_oauth_by_label(
            self.pool(),
            label,
            &refreshed.access_token,
            refreshed.refresh_token.as_deref().unwrap_or(&oauth_row.refresh_token),
            &oauth_row.token_uri,
            &oauth_row.client_id,
            &oauth_row.client_secret,
            &oauth_row.scopes,
            new_expires.as_deref(),
        ).await {
            warn!("upsert after refresh failed for '{label}': {e}");
        }
        Some(refreshed.access_token)
    }
}
```

- [ ] **Step 5:** Commit:

```bash
git add crates/rs-api/src/youtube.rs crates/rs-api/src/delivery_youtube.rs
git commit -m "feat(api): label-aware oauth seed + multi-channel /youtube/status (#$ISSUE_NUM) [green]"
```

---

## Task 19: Delete legacy single-row OAuth helpers + web-flow handlers + routes

**Files:**
- Modify: `crates/rs-core/src/db/v2.rs` — DELETE `get_youtube_oauth` + `upsert_youtube_oauth`
- Modify: `crates/rs-core/src/db/tests.rs` — remove tests that exercise the deleted helpers (delete or convert to `upsert_oauth_by_label` form)
- Modify: `crates/rs-core/src/db/mod.rs` — remove `pub use v2::{get_youtube_oauth, upsert_youtube_oauth}` re-exports (if present)
- Modify: `crates/rs-api/src/youtube.rs` — DELETE `youtube_oauth_start`, `youtube_oauth_callback`, `parse_label_from_query`, `OAuthStartQuery`, `YouTubeOAuthStartResponse`
- Modify: `crates/rs-api/src/router.rs` — REMOVE `/youtube/oauth/start` + `/youtube/oauth/callback` route bindings
- Modify: `crates/rs-api/src/youtube_label_tests.rs` — REMOVE tests for `parse_label_from_query` (file may be deleted entirely if the only tests in it are for the deleted helper)
- Modify: `crates/rs-api/src/lib.rs` — drop the `mod youtube_label_tests;` line if file is removed

- [ ] **Step 1:** Remove from `crates/rs-core/src/db/v2.rs`. Locate `pub async fn get_youtube_oauth(pool: &SqlitePool) -> Result<Option<YouTubeOAuth>>` and `pub async fn upsert_youtube_oauth(...)`. Delete both functions and their docs (typically ~70 lines combined).

- [ ] **Step 2:** Remove the deleted-helper tests from `crates/rs-core/src/db/tests.rs` lines 439 + 457 (the `upsert_youtube_oauth` callers). Replace each call with the equivalent `upsert_oauth_by_label(... "default", ...)` invocation if the surrounding test still has unrelated assertions; otherwise delete the whole test.

- [ ] **Step 3:** Remove from `crates/rs-api/src/youtube.rs`:
- `pub async fn youtube_oauth_start(...)`
- `pub async fn youtube_oauth_callback(...)`
- `fn parse_label_from_query(...)`
- `pub struct OAuthStartQuery`
- `pub struct YouTubeOAuthStartResponse`
- All imports they were the sole consumer of (re-check after deletion).

- [ ] **Step 4:** Remove from `crates/rs-api/src/router.rs`. Locate:

```rust
        .route("/youtube/oauth/start", get(youtube::youtube_oauth_start))
        .route(
            "/youtube/oauth/callback",
            get(youtube::youtube_oauth_callback),
        )
```

Delete both blocks.

- [ ] **Step 5:** If `crates/rs-api/src/youtube_label_tests.rs` exists and its only tests target `parse_label_from_query`, delete the file with `rm` and remove the corresponding `mod youtube_label_tests;` line from `lib.rs`. If it has other tests, delete only the relevant tests.

- [ ] **Step 6:** Verify the deletions leave no broken references:

```bash
# Should return zero matches:
grep -rn 'youtube_oauth_start\|youtube_oauth_callback\|parse_label_from_query' crates/
grep -rn '\bget_youtube_oauth\b\|\bupsert_youtube_oauth\b' crates/
```

If any matches remain, fix them — the only acceptable matches are inside this task's commit message or in `docs/`.

- [ ] **Step 7:** Commit:

```bash
git add crates/rs-core/src/db/v2.rs crates/rs-core/src/db/tests.rs \
        crates/rs-core/src/db/mod.rs crates/rs-api/src/youtube.rs \
        crates/rs-api/src/router.rs crates/rs-api/src/lib.rs
# Plus any rm'd file: git rm crates/rs-api/src/youtube_label_tests.rs (if applicable)
git commit -m "chore: delete legacy web-flow handlers + single-row oauth helpers (#$ISSUE_NUM)"
```

---

## Task 20: CI workflow — `label: default` body for seed step

**Files:**
- Modify: `.github/workflows/ci.yml` — `Seed YouTube OAuth` step body

- [ ] **Step 1:** Locate the step in the workflow that POSTs to `/api/v1/youtube/oauth/seed`. Find the JSON body construction (PowerShell `ConvertTo-Json` or similar) and add the `label: "default"` field. ASCII-only strings.

Example before:

```yaml
        $body = @{
          refresh_token = $env:YOUTUBE_REFRESH_TOKEN
          client_id     = $env:YOUTUBE_CLIENT_ID
          client_secret = $env:YOUTUBE_CLIENT_SECRET
        } | ConvertTo-Json
```

After:

```yaml
        $body = @{
          label         = "default"
          refresh_token = $env:YOUTUBE_REFRESH_TOKEN
          client_id     = $env:YOUTUBE_CLIENT_ID
          client_secret = $env:YOUTUBE_CLIENT_SECRET
        } | ConvertTo-Json
```

(Find the exact existing pattern with `grep -n 'oauth/seed' .github/workflows/ci.yml` and update accordingly. If the body is constructed differently, add the `label` field to the existing structure preserving its style.)

- [ ] **Step 2:** Commit:

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add label=default to oauth/seed body (#$ISSUE_NUM)"
```

---

## Task 21: Leptos UI — failing Playwright test (RED)

**Files:**
- Create: `e2e/oauth-authorize.spec.ts`
- Modify: `e2e/frontend.spec.ts` if needed for shared fixtures (likely not)

- [ ] **Step 1:** Create the test file:

```typescript
import { test, expect } from '@playwright/test';

// Run against the mock backend used by frontend.spec.ts.
// The backend exposes _test endpoints that let us pre-seed grant state.

test.describe('OAuth Authorize channel', () => {
  test('authorize new channel happy path', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error' || msg.type() === 'warning') {
        consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
      }
    });
    await page.goto('/');

    // Open the Channels panel + click Authorize.
    await page.getByRole('button', { name: 'Authorize new channel' }).click();

    // Fill the label and submit.
    await page.getByLabel('Channel label').fill('bb');
    await page.getByRole('button', { name: 'Start authorization' }).click();

    // user_code + verification_url visible.
    await expect(page.getByTestId('oauth-user-code')).toHaveText('AB-CD-12');
    await expect(page.getByTestId('oauth-verification-url')).toHaveAttribute(
      'href', /google\.com\/device/);

    // Mock backend transitions to granted (test fixture endpoint).
    await page.evaluate(async () => {
      await fetch('/api/v1/_test/oauth-device-grant', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ label: 'bb', channel_id: 'UCxxxxxxxx' }),
      });
    });

    // Modal closes, channel appears in the table.
    await expect(page.getByTestId('oauth-modal')).toBeHidden({ timeout: 10_000 });
    await expect(page.getByTestId('oauth-channel-row-bb')).toBeVisible();
    await expect(page.getByTestId('oauth-channel-row-bb')).toContainText('UCxxxxxxxx');

    // Zero console errors / warnings (per browser-console-zero-errors.md).
    expect(consoleMessages).toEqual([]);
  });
});
```

- [ ] **Step 2:** Commit:

```bash
git add e2e/oauth-authorize.spec.ts
git commit -m "test: failing Playwright spec for Authorize-channel modal (#$ISSUE_NUM) [red]"
```

---

## Task 22: Leptos UI — implement (GREEN)

**Files:**
- Create: `leptos-ui/src/components/oauth_authorize.rs`
- Modify: `leptos-ui/src/components/mod.rs` — register the component
- Modify: `leptos-ui/src/components/operator_dashboard.rs` — mount the new component below existing config section
- Modify: `leptos-ui/style.css` — append modal + table styles
- Modify: `crates/rs-api/src/router.rs` + a new test-only handler — add `POST /api/v1/_test/oauth-device-grant` for Playwright (gated behind `cfg!(any(test, feature = "test-hooks"))` or by env var)

- [ ] **Step 1:** Create the Leptos component:

```rust
//! OAuth Device Code Flow channel-authorization UI.
//! - Channels panel: lists `GET /api/v1/youtube/oauths`
//! - Authorize button: opens modal, calls `device-start`, polls `device-status`

use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen_futures::spawn_local;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct OAuthRow {
    pub id: i64,
    pub label: String,
    pub channel_id: Option<String>,
    pub connected_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DeviceStartBody {
    label: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DeviceStartResp {
    user_code: String,
    verification_url: String,
    #[allow(dead_code)]
    expires_in: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DeviceStatusResp {
    status: String,
    user_code: Option<String>,
    verification_url: Option<String>,
    channel_id: Option<String>,
    error: Option<String>,
}

#[component]
pub fn OAuthAuthorize() -> impl IntoView {
    let oauths = RwSignal::new(Vec::<OAuthRow>::new());
    let modal_open = RwSignal::new(false);
    let label_input = RwSignal::new(String::new());
    let pending = RwSignal::new(Option::<DeviceStartResp>::None);
    let status = RwSignal::new(Option::<DeviceStatusResp>::None);
    let error = RwSignal::new(Option::<String>::None);

    // Initial fetch + refresh after grants.
    let refresh = move || {
        spawn_local(async move {
            if let Ok(resp) = reqwest::get("/api/v1/youtube/oauths").await
                && let Ok(list) = resp.json::<Vec<OAuthRow>>().await
            {
                oauths.set(list);
            }
        });
    };
    refresh();

    let start_authorize = move |_| {
        error.set(None);
        let label = label_input.get().trim().to_string();
        if !label.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            || label.is_empty() || label.len() > 32
        {
            error.set(Some("Label must match [a-z0-9_]{1,32}".into()));
            return;
        }
        spawn_local(async move {
            let client = reqwest::Client::new();
            let r = client.post("/api/v1/youtube/oauth/device-start")
                .json(&DeviceStartBody { label: label.clone() })
                .send().await;
            match r {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(body) = resp.json::<DeviceStartResp>().await {
                        pending.set(Some(body));
                        // Begin polling.
                        let l = label.clone();
                        spawn_local(async move {
                            loop {
                                gloo_timers::future::TimeoutFuture::new(3_000).await;
                                let url = format!("/api/v1/youtube/oauth/device-status?label={l}");
                                if let Ok(s) = reqwest::get(&url).await
                                    && let Ok(b) = s.json::<DeviceStatusResp>().await
                                {
                                    let term = matches!(b.status.as_str(),
                                        "granted" | "denied" | "expired" | "error");
                                    status.set(Some(b));
                                    if term { break; }
                                }
                            }
                        });
                    }
                }
                Ok(resp) if resp.status() == reqwest::StatusCode::CONFLICT => {
                    error.set(Some(format!("Label '{label}' is already authorized")));
                }
                Ok(resp) => {
                    error.set(Some(format!("device-start failed: HTTP {}", resp.status())));
                }
                Err(e) => error.set(Some(format!("device-start failed: {e}"))),
            }
        });
    };

    // Close modal when status reaches granted (and refresh channels).
    Effect::new(move |_| {
        if let Some(s) = status.get()
            && s.status == "granted"
        {
            modal_open.set(false);
            pending.set(None);
            status.set(None);
            refresh();
        }
    });

    view! {
        <section class="oauth-section">
            <h3>"YouTube channels"</h3>
            <table class="oauth-channels-table">
                <thead><tr><th>"Label"</th><th>"Channel"</th><th>"Connected"</th></tr></thead>
                <tbody>
                    {move || oauths.get().into_iter().map(|o| {
                        let label = o.label.clone();
                        view! {
                            <tr data-testid={format!("oauth-channel-row-{label}")}>
                                <td>{o.label.clone()}</td>
                                <td>{o.channel_id.clone().unwrap_or_else(|| "(none)".into())}</td>
                                <td>{o.connected_at.clone().unwrap_or_default()}</td>
                            </tr>
                        }
                    }).collect_view()}
                </tbody>
            </table>
            <button on:click={move |_| modal_open.set(true)}>"Authorize new channel"</button>
            {move || modal_open.get().then(|| view! {
                <div class="oauth-modal" data-testid="oauth-modal">
                    {move || error.get().map(|e| view!{ <p class="oauth-error">{e}</p> })}
                    {move || match pending.get() {
                        None => view! {
                            <div>
                                <label for="oauth-label-input">"Channel label"</label>
                                <input id="oauth-label-input" type="text"
                                    on:input={move |ev| label_input.set(event_target_value(&ev))}/>
                                <button on:click=start_authorize>"Start authorization"</button>
                                <button on:click={move |_| modal_open.set(false)}>"Cancel"</button>
                            </div>
                        }.into_any(),
                        Some(p) => view! {
                            <div>
                                <p>"Open this URL on any device:"</p>
                                <a data-testid="oauth-verification-url" href={p.verification_url.clone()} target="_blank">
                                    {p.verification_url.clone()}
                                </a>
                                <p>"Enter this code:"</p>
                                <code data-testid="oauth-user-code" class="oauth-user-code">{p.user_code}</code>
                                <p class="oauth-status">
                                    {move || status.get().map(|s| s.status).unwrap_or_else(|| "pending".into())}
                                </p>
                            </div>
                        }.into_any(),
                    }}
                </div>
            })}
        </section>
    }
}
```

- [ ] **Step 2:** Register the component:

```rust
// leptos-ui/src/components/mod.rs
pub mod operator_dashboard;
```
→
```rust
pub mod oauth_authorize;
pub mod operator_dashboard;
```

- [ ] **Step 3:** Mount in `operator_dashboard.rs` below the existing config section:

```rust
// Inside the dashboard view! macro, after the existing config section:
<crate::components::oauth_authorize::OAuthAuthorize/>
```

- [ ] **Step 4:** Append CSS to `leptos-ui/style.css`:

```css
.oauth-section { padding: var(--spacing-md); border-top: 1px solid var(--border); }
.oauth-channels-table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
.oauth-channels-table th, .oauth-channels-table td {
    padding: 4px 8px; text-align: left; border-bottom: 1px solid var(--border-soft, #333);
}
.oauth-modal {
    position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
    background: var(--bg-secondary); border: 1px solid var(--border);
    padding: var(--spacing-lg); border-radius: var(--radius); z-index: 100;
    min-width: 360px;
}
.oauth-user-code {
    display: inline-block; padding: 8px 12px; margin: 8px 0;
    font-family: var(--font-mono); font-size: 1.4em; letter-spacing: 2px;
    background: var(--bg-tertiary, #222); border-radius: var(--radius);
}
.oauth-error { color: var(--status-error); }
.oauth-status { font-style: italic; color: var(--text-secondary); }
```

- [ ] **Step 5:** Add the test-only fixture endpoint. In `crates/rs-api/src/router.rs`, add a `cfg(any(test, feature = "test-hooks"))` route OR — simpler — gate by env var `RESTREAMER_TEST_HOOKS=1` and add the route unconditionally but reject the call in production:

```rust
// In crates/rs-api/src/router.rs, add to the api router builder:
        .route("/_test/oauth-device-grant", post(crate::oauth_device::test_grant_now))
```

In `crates/rs-api/src/oauth_device.rs`, append:

```rust
#[derive(Debug, Deserialize)]
pub struct TestGrantBody {
    pub label: String,
    pub channel_id: Option<String>,
}

/// Test fixture: pretend the operator just completed Device Flow for `label`.
/// Persists a fake refresh token and channel_id, deletes any pending grant,
/// emits the OAuthGranted audit row. Refuses in production unless the
/// `RESTREAMER_TEST_HOOKS=1` env var is set.
pub async fn test_grant_now(
    State(state): State<AppState>,
    Json(body): Json<TestGrantBody>,
) -> Result<StatusCode, StatusCode> {
    if std::env::var("RESTREAMER_TEST_HOOKS").as_deref() != Ok("1") {
        return Err(StatusCode::NOT_FOUND);
    }
    rs_core::db::youtube_oauth::upsert_oauth_by_label(
        &state.pool, &body.label, "test_AT", "test_RT",
        "https://oauth2.googleapis.com/token",
        "test_cid", "test_csec", SCOPE,
        Some(&(chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()),
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "UPDATE youtube_oauth SET channel_id = ?1, connected_at = ?2 WHERE label = ?3"
    ).bind(&body.channel_id).bind(&now).bind(&body.label)
    .execute(&state.pool).await;
    let _ = rs_core::db::oauth_device_grants::delete(&state.pool, &body.label).await;
    let row = rs_core::audit::AuditRow {
        severity: rs_core::audit::Severity::Info,
        source: rs_core::audit::Source::Operator,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: rs_core::audit::Action::OAuthGranted,
        detail: serde_json::json!({"label": body.label, "channel_id": body.channel_id}),
        ts_override: None,
    };
    let _ = state.audit_tx.send(row).await;
    Ok(StatusCode::OK)
}
```

The CI Playwright job already sets `RESTREAMER_TEST_HOOKS=1` for the mock-API server (verify and add the env if not).

- [ ] **Step 6:** Commit:

```bash
git add leptos-ui/src/components/oauth_authorize.rs leptos-ui/src/components/mod.rs \
        leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css \
        crates/rs-api/src/oauth_device.rs crates/rs-api/src/router.rs \
        .github/workflows/ci.yml
git commit -m "feat(ui): authorize-channel modal + test fixture endpoint (#$ISSUE_NUM) [green]"
```

---

## Task 23 (ORCHESTRATOR ONLY — NOT a subagent task)

After Task 22's reviews pass, the orchestrator runs the full pre-push gate and ships the PR.

- [ ] **Step 1:** Local checks (Tier-2 fast-iterate):

```bash
cd /home/newlevel/devel/restreamer
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo test --no-run --workspace
```

If any step fails, address the failure with a focused new commit (do NOT `--amend`).

- [ ] **Step 2:** Run the key new tests locally (Tier-2 permits running tests, single-threaded for env-var serialization):

```bash
cargo test -p rs-core --lib migration_tests -- --test-threads=1
cargo test -p rs-core --lib oauth_device_grants_tests -- --test-threads=1
cargo test -p rs-youtube --lib quota_tests
cargo test -p rs-youtube --lib device_flow_tests -- --test-threads=1
cargo test -p rs-api --lib adaptive_ttl_tests
cargo test -p rs-api --lib oauth_device_tests -- --test-threads=1
cargo test -p rs-api --lib multi_label_oauth_tests -- --test-threads=1
```

Every test must pass before pushing.

- [ ] **Step 3:** Push:

```bash
git push origin dev
```

- [ ] **Step 4:** Monitor CI per `ci-monitoring.md`. ONE `Bash(command: "sleep 300 && gh run view <run-id> --json status,conclusion,jobs", run_in_background: true)` cycle until terminal. Investigate + fix any failure; never blindly rerun.

- [ ] **Step 5:** Open PR `dev → main`:

```bash
gh pr create --title "feat: multi-channel YouTube OAuth via Device Code Flow (Closes #$ISSUE_NUM)" \
  --body "$(cat <<'EOF'
## Summary
- Replace broken Web-flow OAuth handler (redirect_uri=127.0.0.1:8910, never registered with Google) with RFC 8628 Device Code Flow
- Operator authorizes any number of channels from any device via dashboard "Authorize channel" modal
- Per-project quota tracker (sliding window, 10k/day default) + adaptive cache TTL (60s healthy / 15s degraded)
- `OAuthGranted` audit row on Device Flow completion
- DELETE legacy `db::v2::{get,upsert}_youtube_oauth` + `youtube_oauth_start`/`callback` web handlers + `parse_label_from_query` + nginx-intercept procedure
- Migration v27: `youtube_oauth.connected_at` + `oauth_device_grants` transient table
- `youtube_oauth_seed` now requires `label` body field; CI workflow updated

Closes #$ISSUE_NUM

Operator post-merge action (one-time, manual):
1. Add "TVs and Limited Input devices" OAuth client to Google Cloud project `restreamer-489321`
2. Set GitHub secrets `YOUTUBE_DEVICE_CLIENT_ID` + `YOUTUBE_DEVICE_CLIENT_SECRET`
3. Set same in `~/.restreamer-secrets/stream-lan.env` on stream.lan
4. Authorize bb channel via the dashboard's "Authorize channel" button
5. Link `ytbb` endpoint to the new bb OAuth row via the existing link-oauth API
6. Capture observed `top_issue` from dashboard during a live restreamer push, update #196 with data

## Test plan
- [x] cargo fmt + clippy + test all green
- [x] Migration v27 idempotent (re-runs no-op)
- [x] Device Flow state machine: pending / slow_down / denied / expired / granted
- [x] Crash recovery: pending grants resume on restart, expired grants marked
- [x] Quota tracker: acquire under budget, refuse over, refill semantics
- [x] Adaptive TTL: 60s healthy / 15s degraded
- [x] OAuthGranted audit row fires exactly once on grant
- [x] Playwright: Authorize channel modal happy path against mock backend

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6:** Wait for PR CI to go fully green (all push-event + pull_request-event jobs). Verify mergeable:

```bash
gh pr view <NUMBER> --json mergeable,mergeStateStatus
```

Required: `{ "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN" }`.

- [ ] **Step 7:** Post-deploy verification on streamsnv using Playwright MCP + win-stream-snv MCP:

1. `mcp__plugin_playwright_playwright__browser_navigate` to `http://10.77.9.204:8910/`
2. Read DOM version label — must show `0.11.0-dev` (or `0.11.0-dev.N`).
3. Click "Authorize new channel", type a throwaway label, verify modal renders `user_code` + `verification_url`. (Do NOT complete the flow without a Google account ready — testing only that the UI wires correctly.)
4. `mcp__win-stream-snv__ListProcesses filter="Restreamer"` — confirm `Restreamer.exe` running with PID in user session.
5. Read browser console messages — must be 0 errors / 0 warnings (matching `browser-console-zero-errors.md`).

- [ ] **Step 8:** Run `plan-check` skill — every `[ ]` in this plan must be `[x]` or have a documented justification.

- [ ] **Step 9:** Run `/review` standards on the diff — fix any 🔴 / 🟡 / 🔵 finding inside the diff before reporting.

- [ ] **Step 10:** Send the completion report per `completion-report.md`. Wait for explicit user merge instruction per `pr-merge-policy.md` — never merge autonomously.

---

## Self-Review Notes

Spec coverage check:

- §1 OAuth Client Setup → Task 23 step (manual, one-time, in PR body) ✓
- §2 Schema Delta v27 → Tasks 2-3 ✓
- §3 Device Flow Lifecycle (device-start, device-status, background poller) → Tasks 11-16 ✓
- §4 Quota Tracker → Tasks 7-8 ✓
- §5 Adaptive Cache TTL → Tasks 9-10 ✓
- §6 Operator Dashboard UI → Tasks 21-22 ✓
- §7 Audit (OAuthGranted) → Task 6 (variant) + Task 14 (emission) ✓
- §8 Deletions → Task 19 ✓
- §9 Tests → distributed across each RED task ✓
- §10 Migration & Rollback → Task 3 (incremental + idempotent) ✓
- §11 Quota Math → Task 8 (verified via tests) ✓

Type consistency check:

- `DeviceGrant` struct used in `oauth_device_grants.rs` + `oauth_device.rs::resume_pending_grants` (consistent field names: label, device_code, user_code, verification_url, interval_secs, expires_at, status, error, started_at) ✓
- `PollResponse` enum used in `device_flow.rs` + tests + `oauth_device.rs::spawn_grant_poller` (consistent variants: Pending, SlowDown, Denied, Expired, Granted{access_token, refresh_token, expires_in, id_token}, Error(String)) ✓
- `PollDecision` enum used in `poll_decision` + `spawn_grant_poller` (consistent variants: Continue, DoubleInterval, TerminalDenied, TerminalExpired, TerminalGranted{...}, TerminalError(String)) ✓
- `YoutubeHealth` shared between `delivery_status::ttl_for_health` + existing dashboard code ✓
- `YouTubeStatusPerChannel` shape consistent across `check_all_youtube_status` and `get_youtube_status` Axum handler ✓

Placeholder scan: no TBD / TODO / "implement later" found. Every code block is complete and runnable. The `todo!()` in Task 13 Step 3 is an explicit instruction to follow existing patterns — annotated, not a real placeholder.

Scope: single coherent feature, single PR per `autonomous-batch-issue-development.md` (no progressive split).
