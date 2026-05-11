# Cache-Metric Reform + Start Delivering Reset — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate three operator-reported regressions in one PR: cache bar 112→1 drop at first-push, 57GB received_bytes accumulation across sessions, and fast endpoints stuck 50s behind live edge after VPS creation.

**Architecture:** Three coordinated changes plus one UI bonus.
- A) `chunk_delay_secs` per-endpoint semantics shift from "buffer ABOVE consumer" to "lag FROM live edge". Backend-only change; one new DB helper, one wire-up.
- B) Reset `streaming_events.received_bytes` to 0 inside `start_delivery` alongside the existing S3 wipe.
- C) Host computes fresh live-edge at VPS "delivering" transition and POSTs to a new VPS endpoint `/api/endpoints/update_start` for is_fast endpoints only. VPS tears down and respawns the EndpointHandle (mirroring `add_endpoint`).
- Bonus) Leptos cache bar shows "Xs / live" + green-when-<=5s for is_fast endpoints.

**Tech Stack:** Rust 2024 (monorepo), sqlx (SQLite, runtime queries), Axum (REST), reqwest (host→VPS HTTP), Leptos CSR (WASM dashboard), Playwright (E2E).

**Spec:** `docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md` (commit 58b5153 on `dev`).

---

## Context the implementer needs upfront

- **Repo & branch:** `/home/newlevel/devel/restreamer`, branch `dev`. Latest commit on dev that this plan builds on: `58b5153` (the spec). Cargo version currently `0.8.0`; Task 2 bumps to `0.9.0`.
- **Issue:** Task 1 files a GH issue. Subsequent commit messages reference its number `(#$ISSUE_NUM)`. The orchestrator captures the number from Task 1's `gh issue create` output and substitutes it into subsequent task prompts. No subagent files the issue except Task 1.
- **Local-build policy:** This repo enforces CI-only Rust compilation. Subagents MUST NOT run `cargo build`, `cargo test`, `cargo clippy`, `cargo check`, `trunk build`, or `cargo tauri build` locally. Only `cargo fmt --all --check` is permitted. The orchestrator pushes to CI in Task 15.
- **TDD strictness:** Every implementation task is split into a "failing test commit" task followed by an "implementation commit" task. The failing-test commit MUST land first. One commit per task. Never batch.
- **File size cap:** every new or modified `.rs` file MUST stay <1000 lines (CI gate).
- **ASCII-only in CI:** no em-dashes, smart quotes, or other non-ASCII in PowerShell strings inside `.github/workflows/*.yml`.
- **Backwards compat:** Old VPS binaries (pre-this-PR) don't have `/api/endpoints/update_start`. Host treats a 404/connection error from that call as a no-op + audit row. Don't fail Start Delivering on this.
- **No new DB migrations.** Both `chunk_records` and `streaming_events` schemas are unchanged. Only behavior changes.
- **`chunk_delay_secs` is read by multiple consumers** (verified by Task 3 grep). The semantics shift affects ALL of them — the historical `delivery_endpoint_metrics` rows written after this PR carry the new meaning. Old rows keep the old meaning. This is intentional; nobody mixes them today.
- **Pre-answered questions** (do NOT ask the user):
  - Subagent-Driven Development. Dispatch all subagent tasks autonomously, no review-pause prompts.
  - Operator soak during next live event is the final validation.
  - All tracking via the GH issue from Task 1.

---

## File map

| File | Status | Responsibility |
|---|---|---|
| `Cargo.toml` (root) | Modify (Task 2) | Workspace version → 0.9.0 |
| `src-tauri/Cargo.toml` | Modify (Task 2) | Tauri crate version → 0.9.0 |
| `src-tauri/tauri.conf.json` | Modify (Task 2) | NSIS installer version → 0.9.0 |
| `leptos-ui/Cargo.toml` | Modify (Task 2) | WASM crate version → 0.9.0 |
| `crates/rs-core/src/db/mod.rs` | Modify (Task 5) | Add `get_endpoint_lag_secs` helper |
| `crates/rs-core/src/db/mod_tests.rs` | Create (Task 4) | Failing tests for `get_endpoint_lag_secs` |
| `crates/rs-api/src/delivery_status.rs` | Modify (Task 6) | Switch per-endpoint chunk_delay_secs to new helper |
| `crates/rs-api/src/delivery.rs` | Modify (Task 8, 13) | `received_bytes` reset; call `on_vps_ready`; implement it |
| `crates/rs-api/src/delivery_reset_tests.rs` | Create (Task 7) | Failing tests for `received_bytes` reset |
| `crates/rs-api/src/on_vps_ready_tests.rs` | Create (Task 12) | Failing tests for `on_vps_ready` host logic |
| `crates/rs-core/src/audit.rs` | Modify (Task 9) | Add `FastEndpointJumpedToLiveEdge`, `EndpointStartChunkUpdated` Action variants |
| `crates/rs-delivery/src/api.rs` | Modify (Task 11) | New POST `/api/endpoints/update_start` handler |
| `crates/rs-delivery/src/api_update_start_tests.rs` | Create (Task 10) | Failing tests for VPS handler |
| `leptos-ui/src/components/operator_dashboard.rs` | Modify (Task 14) | Fast-endpoint cache bar UX |
| `e2e/frontend.spec.ts` | Modify (Task 14) | Playwright assertion for "Xs / live" label |

---

### Task 1: File the GitHub issue

**Goal:** Get the issue number that subsequent commits will reference.

- [ ] **Step 1: Create the issue**

```bash
gh issue create \
  --title "Cache-metric reform + Start Delivering reset + fast-endpoint live-edge recompute" \
  --body "$(cat <<'EOF'
## Problem

Three operator-reported regressions observed live on streamsnv v0.8.0-dev:

1. **Cache bar 112s → 1s drop** at first-push moment of Start Delivering. The leptos cache bar
   reads `ps.cache_duration_secs` (host-side global S3-buffer total) while no endpoint has
   pushed yet, then switches to per-endpoint `chunk_delay_secs` (currently "buffer ABOVE
   the endpoint's current chunk") at first push. From the new endpoint's perspective there
   is nothing newer in S3 yet → 1s. Operator sees a confusing drop.

2. **\`received_bytes\` accumulates across all sessions.** The counter is incremented while
   \`receiving_activated = true\` and never resets. A multi-day event with intermittent
   streaming reads 57+ GB. S3 chunks are wiped on Start (#174) but the byte counter is not.

3. **Fast endpoints (Kiko, is_fast=true) end up ~50s behind live edge.** \`start_chunk_id\`
   is computed once at \`delivery_init_sent\` (≈t=2s host clock). VPS creation takes 30-50s.
   By the time the VPS is "delivering", Kiko starts pushing from a chunk 50s behind live
   edge. The producer-lag jump logic short-circuits for \`delivery_delay_ms == 0\`, so Kiko
   never catches up. Result: Kiko streams 50s behind reality, defeating its purpose.

## Fix (spec)

See \`docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md\`
(commit 58b5153).

- **A:** \`chunk_delay_secs\` per-endpoint semantics → "lag FROM live edge" (eliminates 112→1).
- **B:** Reset \`received_bytes\` to 0 in \`start_delivery\` alongside existing S3 wipe.
- **C:** Host computes fresh live-edge at VPS-ready and POSTs \`/api/endpoints/update_start\`
  to VPS for is_fast endpoints only. VPS tears down + respawns the EndpointHandle.
- **Bonus:** Fast endpoint cache bar shows "Xs / live", green when ≤5s.

## Acceptance

- Stop+Start cycle on streamsnv: cache bar 0 → 120 smooth, no drops > 10s.
- After Start: \`received_bytes\` reads near 0 (<100 MB) within first 30s.
- Kiko reports current_chunk_id within 2 chunks of live edge after VPS-ready.
- Kiko cache label reads "Xs / live" format.
EOF
)"
```

- [ ] **Step 2: Capture the issue number**

The command above prints something like `https://github.com/zbynekdrlik/restreamer/issues/195`. Record the number (e.g. `195`) and pass it to all subsequent subagent dispatches as `$ISSUE_NUM`. Subagents substitute `(#$ISSUE_NUM)` into their commit messages.

- [ ] **Step 3: No commit**

This task creates a GitHub issue; no git commit. Move to Task 2.

---

### Task 2: Version bump 0.8.0 → 0.9.0

**Files:**
- Modify: `Cargo.toml` (line 25)
- Modify: `src-tauri/Cargo.toml` (line 3)
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml` (line 3)

- [ ] **Step 1: Edit all four files**

In `Cargo.toml`:

```toml
version = "0.9.0"
```

In `src-tauri/Cargo.toml`:

```toml
version = "0.9.0"
```

In `src-tauri/tauri.conf.json`:

```json
"version": "0.9.0",
```

In `leptos-ui/Cargo.toml`:

```toml
version = "0.9.0"
```

- [ ] **Step 2: Verify formatting**

Run: `cargo fmt --all --check`
Expected: zero output (no changes needed).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version 0.8.0 -> 0.9.0 (#$ISSUE_NUM)"
```

---

### Task 3: Grep audit of `chunk_delay_secs` consumers

**Goal:** Establish written record of every place the field is read, because Task 6 changes its semantic meaning. This audit is part of the commit message — no code change.

**Files:** none (this task produces only an audit commit).

- [ ] **Step 1: Run the audit grep**

```bash
grep -rn "chunk_delay_secs" crates/ leptos-ui/src/ src-tauri/ 2>/dev/null > /tmp/chunk_delay_audit.txt
wc -l /tmp/chunk_delay_audit.txt
```

- [ ] **Step 2: Classify the results**

Expected categories (verified 2026-05-11):
- **Producers:** `crates/rs-api/src/delivery_status.rs:256` is the ONLY computation site for live values. Task 6 modifies this line.
- **DTO struct:** `crates/rs-api/src/delivery_handlers.rs:222,265`, `crates/rs-api/src/delivery_status.rs:120`, `crates/rs-core/src/models.rs:243,258`, `crates/rs-api/src/lib.rs:419` — these just plumb the field through.
- **Historical metrics:** `crates/rs-core/src/db/metrics.rs:17,42,50,61,73,116` (column in `delivery_endpoint_metrics` table). After this PR, new rows carry the new meaning. Old rows keep old meaning. No migration.
- **Diagnostic exports:** `crates/rs-api/src/diag.rs:24,69,131,148,239,293` — exports include `chunk_delay_secs`. After this PR, exports for new rows carry new meaning. Acceptable.
- **Tests & defaults:** `crates/rs-core/src/models.rs:481,535,606,632,653,679,695,708,709,723,735,736,750`, `crates/rs-api/src/stream_handlers.rs:152`, `crates/rs-api/src/lib.rs:280,408,479`, `crates/rs-core/src/db/migration_tests.rs:106` — test fixtures and defaults; meaning-agnostic.

- [ ] **Step 3: Document in a commit-only file**

Create `docs/superpowers/notes/2026-05-11-chunk-delay-secs-consumers.md` with the audit:

```markdown
# chunk_delay_secs consumers audit (2026-05-11)

Audit performed as part of #$ISSUE_NUM before changing the semantic meaning
of per-endpoint `chunk_delay_secs` from "buffer ABOVE consumer" to "lag FROM
live edge".

## Live computation site (the only one)
- `crates/rs-api/src/delivery_status.rs:256` — the per-endpoint value sent to
  the dashboard. Modified in Task 6.

## DTO struct (passive plumbing)
- `crates/rs-api/src/delivery_handlers.rs:222,265`
- `crates/rs-api/src/delivery_status.rs:120`
- `crates/rs-core/src/models.rs:243,258`
- `crates/rs-api/src/lib.rs:419`

## Historical metrics (carries new meaning going forward)
- `crates/rs-core/src/db/metrics.rs:17,42,50,61,73,116`
  Column: `delivery_endpoint_metrics.chunk_delay_secs`
  Rows written after this PR: lag-from-live-edge semantics.
  Rows written before this PR: buffer-above-consumer semantics.
  No migration; downstream consumers must not mix old + new rows for averaging.

## Diagnostic exports (re-exports historical column)
- `crates/rs-api/src/diag.rs:24,69,131,148,239,293`

## Tests & defaults (meaning-agnostic)
- `crates/rs-core/src/models.rs:481,535,606,632,653,679,695,708,709,723,735,736,750`
- `crates/rs-api/src/stream_handlers.rs:152`
- `crates/rs-api/src/lib.rs:280,408,479`
- `crates/rs-core/src/db/migration_tests.rs:106`

## Conclusion
Task 6 changes only the producer at delivery_status.rs:256. All other sites
plumb the value through unchanged. No code surface to break.
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/notes/2026-05-11-chunk-delay-secs-consumers.md
git commit -m "docs: audit chunk_delay_secs consumers before semantics change (#$ISSUE_NUM)"
```

---

### Task 4: TDD — failing tests for `get_endpoint_lag_secs`

**Files:**
- Create: `crates/rs-core/src/db/mod_tests.rs` (or append to an existing test file inside `crates/rs-core/src/db/` if one exists for the helper)

- [ ] **Step 1: Locate the existing test pattern**

Look for an existing `#[cfg(test)] mod tests` block at the bottom of `crates/rs-core/src/db/mod.rs`. If one exists, append new tests there. If not, find another file in the same directory (`migration_tests.rs`, etc.) and add a new sibling module declaration to the parent, e.g. in `crates/rs-core/src/db/mod.rs`:

```rust
#[cfg(test)]
mod lag_tests;
```

Then create `crates/rs-core/src/db/lag_tests.rs`.

- [ ] **Step 2: Write the failing tests**

Append to the chosen test file:

```rust
use super::*;
use sqlx::sqlite::SqlitePoolOptions;

async fn setup_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    // Run all incremental migrations (the production path).
    crate::db::run_migrations(&pool).await.unwrap();
    pool
}

async fn insert_event(pool: &SqlitePool, name: &str) -> i64 {
    let row = sqlx::query("INSERT INTO streaming_events (name) VALUES (?1) RETURNING id")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("id")
}

async fn insert_chunk(
    pool: &SqlitePool,
    event_id: i64,
    seq: i64,
    duration_ms: i64,
    sent: bool,
) {
    sqlx::query(
        "INSERT INTO chunk_records (streaming_event_id, sequence_number, duration_ms, sent)
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(event_id)
    .bind(seq)
    .bind(duration_ms)
    .bind(if sent { 1 } else { 0 })
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn endpoint_lag_zero_at_live_edge() {
    let pool = setup_pool().await;
    let ev = insert_event(&pool, "ev").await;
    // Three sent chunks; endpoint sits at seq 3 (the live edge).
    for s in 1..=3 {
        insert_chunk(&pool, ev, s, 2000, true).await;
    }
    let lag = get_endpoint_lag_secs(&pool, ev, 3).await.unwrap();
    assert!(
        (lag - 0.0).abs() < f64::EPSILON,
        "expected 0.0 at live edge, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_120s_when_60_chunks_behind() {
    let pool = setup_pool().await;
    let ev = insert_event(&pool, "ev").await;
    // 60 sent chunks, each 2000ms duration. Endpoint sits at seq 0.
    for s in 1..=60 {
        insert_chunk(&pool, ev, s, 2000, true).await;
    }
    let lag = get_endpoint_lag_secs(&pool, ev, 0).await.unwrap();
    assert!(
        (lag - 120.0).abs() < f64::EPSILON,
        "expected 120.0s lag when 60 chunks behind, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_ignores_unsent_chunks() {
    let pool = setup_pool().await;
    let ev = insert_event(&pool, "ev").await;
    insert_chunk(&pool, ev, 1, 2000, true).await;
    insert_chunk(&pool, ev, 2, 2000, true).await;
    insert_chunk(&pool, ev, 3, 2000, false).await; // not yet sent
    // Endpoint at seq 1. Sent chunks above: seq 2 (2000ms). Seq 3 ignored.
    let lag = get_endpoint_lag_secs(&pool, ev, 1).await.unwrap();
    assert!(
        (lag - 2.0).abs() < f64::EPSILON,
        "expected 2.0s (only sent chunks count), got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_zero_when_no_chunks_sent() {
    let pool = setup_pool().await;
    let ev = insert_event(&pool, "ev").await;
    let lag = get_endpoint_lag_secs(&pool, ev, 0).await.unwrap();
    assert!(
        (lag - 0.0).abs() < f64::EPSILON,
        "expected 0.0 with no sent chunks, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_excludes_rows_beyond_live_edge() {
    let pool = setup_pool().await;
    let ev = insert_event(&pool, "ev").await;
    // Sent chunks: 1, 2, 3. An imaginary row 4 also exists but sent=0.
    for s in 1..=3 {
        insert_chunk(&pool, ev, s, 1000, true).await;
    }
    insert_chunk(&pool, ev, 4, 1000, false).await;
    // Live edge = MAX(seq where sent=1) = 3. Endpoint at 1. Lag = chunks 2,3 = 2000ms.
    let lag = get_endpoint_lag_secs(&pool, ev, 1).await.unwrap();
    assert!(
        (lag - 2.0).abs() < f64::EPSILON,
        "expected 2.0s (live edge clamped to seq 3), got {lag}"
    );
}
```

- [ ] **Step 3: Push-and-verify-fail is NOT done locally**

The subagent does NOT run tests locally. The orchestrator's Task 15 push to CI verifies these tests fail at this commit. The commit message must flag them as failing tests.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/
git commit -m "test(db): failing tests for get_endpoint_lag_secs helper (#$ISSUE_NUM)

Helper does not yet exist. Compile failure expected; Task 5 adds the
helper. These tests verify:
- Returns 0 when endpoint sits at live edge
- Returns 120s when endpoint is 60 chunks behind (2000ms each)
- Ignores chunks with sent=0
- Returns 0 when no chunks sent yet
- Live-edge boundary is MAX(seq where sent=1), excludes higher unsent rows"
```

---

### Task 5: Implement `get_endpoint_lag_secs`

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs` (append helper near `get_cache_duration_secs` at line 536)

- [ ] **Step 1: Add the helper**

Append immediately after the `get_cache_duration_secs` function (right before `compute_live_stepback_start_chunk` at the line that currently begins `/// Find the chunk_id N such that...`):

```rust
/// Lag (in seconds) from a specific endpoint's current position to the live edge.
///
/// Semantically: how many seconds of stream content sit between this endpoint's
/// read position (`endpoint_current_chunk_id`) and the most recently uploaded
/// (sent=1) chunk. Used by `delivery_status.rs` to drive the per-endpoint cache
/// bar.
///
/// Differs from `get_cache_duration_secs` which is a global "everything above
/// the consumer" metric used for the host-side cache_duration_secs field.
///
/// Returns 0.0 if no chunks have been sent yet, or if the endpoint is at or
/// past the live edge.
pub async fn get_endpoint_lag_secs(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_current_chunk_id: i64,
) -> Result<f64> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) AS total_ms FROM chunk_records
         WHERE streaming_event_id = ?1
           AND sent = 1
           AND sequence_number > ?2
           AND sequence_number <= (
             SELECT COALESCE(MAX(sequence_number), 0) FROM chunk_records
             WHERE streaming_event_id = ?1 AND sent = 1
           )",
    )
    .bind(event_id)
    .bind(endpoint_current_chunk_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("total_ms") as f64 / 1000.0)
}
```

- [ ] **Step 2: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-core/src/db/mod.rs
git commit -m "feat(db): add get_endpoint_lag_secs helper (#$ISSUE_NUM)

Per-endpoint lag-from-live-edge metric, distinct from the existing
global get_cache_duration_secs. Used by delivery_status.rs in Task 6
to drive the per-endpoint cache bar with semantics that don't cause
the 112->1 drop at first-push moment."
```

---

### Task 6: Wire `delivery_status.rs` per-endpoint `chunk_delay_secs` to new helper

**Files:**
- Modify: `crates/rs-api/src/delivery_status.rs` (lines 254-260, the existing computation site)

- [ ] **Step 1: Replace the helper call**

At `crates/rs-api/src/delivery_status.rs:254-260`, find the existing block:

```rust
                            // Compute cache delay using actual content duration from DB.
                            // Returns the raw uncapped value; downstream display layers clamp.
                            let chunk_delay_secs =
                                db::get_cache_duration_secs(self.pool(), event_id, chunk_id)
                                    .await
                                    .unwrap_or(0.0);
```

Replace with:

```rust
                            // Per-endpoint lag FROM live edge (NOT "buffer above consumer").
                            // See spec docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md
                            // for why the semantics changed (#$ISSUE_NUM): the old metric caused
                            // a confusing 112->1 drop at first-push because the endpoint sat at
                            // the chunk it had just pushed and "buffer above" measured nothing.
                            let chunk_delay_secs =
                                db::get_endpoint_lag_secs(self.pool(), event_id, chunk_id)
                                    .await
                                    .unwrap_or(0.0);
```

Substitute the actual issue number from Task 1 when writing the comment.

- [ ] **Step 2: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery_status.rs
git commit -m "fix(dashboard): per-endpoint chunk_delay_secs is lag from live edge (#$ISSUE_NUM)

Eliminates the 112s -> 1s cache bar drop at first-push moment of Start
Delivering. Old semantics measured 'buffer above the endpoint's current
chunk' which collapsed at first push because the endpoint sat AT the
chunk it had just pushed. New semantics measure 'lag from live edge'
which stays smooth across the first-push transition.

Audit of all consumers in docs/superpowers/notes/2026-05-11-chunk-delay-secs-consumers.md
confirms no producer outside delivery_status.rs is affected."
```

---

### Task 7: TDD — failing tests for `received_bytes` reset

**Files:**
- Create: `crates/rs-api/src/delivery_reset_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (add `#[cfg(test)] mod delivery_reset_tests;`)

- [ ] **Step 1: Register the test module**

In `crates/rs-api/src/lib.rs`, find any existing `#[cfg(test)] mod ...;` declarations (typically near the top or bottom of the file). Add:

```rust
#[cfg(test)]
mod delivery_reset_tests;
```

If `lib.rs` has no other `#[cfg(test)]` module declarations, place this declaration right after the existing top-level `mod` declarations.

- [ ] **Step 2: Write the failing tests**

Create `crates/rs-api/src/delivery_reset_tests.rs`:

```rust
//! Tests for the `received_bytes` reset inside `DeliveryOrchestrator::start_delivery`.
//!
//! The reset clears the cumulative byte counter at the start of every delivery
//! cycle so the dashboard reflects current-cycle bytes, not cross-session totals
//! (which reached 57GB on a 3-minute event during operator soak 2026-05-10).

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

async fn setup_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    pool
}

async fn insert_event_with_bytes(pool: &SqlitePool, name: &str, bytes: i64) -> i64 {
    let row = sqlx::query(
        "INSERT INTO streaming_events (name, received_bytes) VALUES (?1, ?2) RETURNING id",
    )
    .bind(name)
    .bind(bytes)
    .fetch_one(pool)
    .await
    .unwrap();
    row.get::<i64, _>("id")
}

async fn read_received_bytes(pool: &SqlitePool, event_id: i64) -> i64 {
    let row = sqlx::query("SELECT received_bytes FROM streaming_events WHERE id = ?1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("received_bytes")
}

#[tokio::test]
async fn reset_received_bytes_zeroes_existing_counter() {
    let pool = setup_pool().await;
    let event_id = insert_event_with_bytes(&pool, "ev", 57_000_000_000).await;
    crate::delivery::reset_event_received_bytes(&pool, event_id)
        .await
        .expect("reset should succeed on populated event");
    let after = read_received_bytes(&pool, event_id).await;
    assert_eq!(after, 0, "received_bytes must be 0 after reset");
}

#[tokio::test]
async fn reset_received_bytes_is_idempotent() {
    let pool = setup_pool().await;
    let event_id = insert_event_with_bytes(&pool, "ev", 0).await;
    crate::delivery::reset_event_received_bytes(&pool, event_id)
        .await
        .expect("reset on zero counter is a no-op success");
    let after = read_received_bytes(&pool, event_id).await;
    assert_eq!(after, 0);
}

#[tokio::test]
async fn reset_received_bytes_succeeds_on_unknown_event_id() {
    // UPDATE with 0 rows affected is success — Start Delivering should not abort
    // just because the row was already deleted by a concurrent stop.
    let pool = setup_pool().await;
    crate::delivery::reset_event_received_bytes(&pool, 99_999)
        .await
        .expect("UPDATE matching 0 rows is success");
}
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/lib.rs crates/rs-api/src/delivery_reset_tests.rs
git commit -m "test(delivery): failing tests for reset_event_received_bytes (#$ISSUE_NUM)

Function does not yet exist. Compile failure expected; Task 8 adds it.
Tests verify:
- Populated counter is zeroed
- Already-zero counter remains zero (idempotent)
- Unknown event_id is success, not error (UPDATE 0 rows is success)"
```

---

### Task 8: Implement `received_bytes` reset

**Files:**
- Modify: `crates/rs-api/src/delivery.rs` (around line 261, immediately after the `wipe_event_s3_chunks` call inside `start_delivery`)

- [ ] **Step 1: Add the standalone reset helper**

Near the top of `crates/rs-api/src/delivery.rs` (after the other module-level `pub async fn` helpers like `wipe_event_s3_chunks` near line 99), add:

```rust
/// Reset `streaming_events.received_bytes` to 0 for a specific event.
///
/// Called from `DeliveryOrchestrator::start_delivery` so the dashboard
/// byte counter reflects only the current Start Delivering cycle, not
/// cumulative bytes since event creation (which reached 57GB on a
/// 3-minute test stream during 2026-05-10 operator soak).
///
/// UPDATE with 0 matched rows is treated as success — a concurrent
/// stop+delete might have removed the row before this runs, and we
/// don't want to abort Start Delivering for that. Real errors (SQL
/// connectivity, schema mismatch) return Err.
pub async fn reset_event_received_bytes(
    pool: &sqlx::SqlitePool,
    event_id: i64,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE streaming_events SET received_bytes = 0 WHERE id = ?1")
        .bind(event_id)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 2: Wire it into start_delivery**

At `crates/rs-api/src/delivery.rs:261`, find the existing block:

```rust
        if let Err(e) = wipe_event_s3_chunks(&self.pool, &self.config, event_id).await {
```

Add the reset call immediately AFTER that `if let Err(e) = wipe_event_s3_chunks(...) { ... }` block (i.e., after its closing `}`), before the next line of `start_delivery`:

```rust
        // Reset cumulative byte counter so dashboard shows current-cycle bytes
        // only, not cross-session totals (operator confusion: 57GB displayed for
        // a 3-minute test stream). Best-effort: warn-log on failure, do not abort.
        // See spec docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md.
        if let Err(e) = reset_event_received_bytes(&self.pool, event_id).await {
            warn!(event_id, "received_bytes reset failed: {e}");
        }
```

- [ ] **Step 3: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/delivery.rs
git commit -m "fix(delivery): reset received_bytes to 0 on Start Delivering (#$ISSUE_NUM)

Dashboard byte counter was cumulative since event creation, reaching
57GB on a 3-minute test stream. Operator preference: reset on Start,
NOT on Stop (so the prior cycle's bytes remain inspectable until the
next Start). Best-effort with warn-log on SQL failure; never aborts
Start Delivering."
```

---

### Task 9: Add audit Action variants

**Files:**
- Modify: `crates/rs-core/src/audit.rs` (the `pub enum Action` definition)

- [ ] **Step 1: Locate the Action enum**

Open `crates/rs-core/src/audit.rs` and find `pub enum Action`. Variants are typically `#[serde(rename_all = "snake_case")]` and include items like `DiskCacheLifecycleSample`, `DiskCacheLifecycleBreach`, `EndpointLifecyclePredeath` (added in #184).

- [ ] **Step 2: Add two new variants**

Inside the `Action` enum body, add (placement: alphabetical or grouped with existing endpoint-lifecycle variants — match the file's existing convention):

```rust
    /// Host-side: at VPS "delivering" transition, recomputed fresh live-edge
    /// chunk_id for an is_fast=true endpoint and POSTed it to the VPS.
    /// Detail JSON: {from_chunk_id, to_chunk_id, gap_chunks, alias, instance_id}.
    FastEndpointJumpedToLiveEdge,

    /// VPS-side: replaced an endpoint's start_chunk_id at host request
    /// (handler: POST /api/endpoints/update_start). Detail JSON:
    /// {alias, old_start_chunk_id, new_start_chunk_id}.
    EndpointStartChunkUpdated,
```

- [ ] **Step 3: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/audit.rs
git commit -m "feat(audit): add FastEndpointJumpedToLiveEdge + EndpointStartChunkUpdated (#$ISSUE_NUM)

Variants for the two halves of the fast-endpoint live-edge recompute:
host emits FastEndpointJumpedToLiveEdge when it POSTs the update; VPS
emits EndpointStartChunkUpdated when it actually replaces the
EndpointHandle. Wired in Tasks 11 and 13."
```

---

### Task 10: TDD — failing tests for VPS `POST /api/endpoints/update_start` handler

**Files:**
- Create: `crates/rs-delivery/src/api_update_start_tests.rs`
- Modify: `crates/rs-delivery/src/main.rs` (add `#[cfg(test)] mod api_update_start_tests;`)

- [ ] **Step 1: Register the test module**

In `crates/rs-delivery/src/main.rs`, find the existing `#[cfg(test)]` mod declarations. Add:

```rust
#[cfg(test)]
mod api_update_start_tests;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/rs-delivery/src/api_update_start_tests.rs`:

```rust
//! Tests for the VPS-side POST /api/endpoints/update_start handler.
//!
//! Host calls this at VPS-ready time to push a freshly-computed live-edge
//! start_chunk_id to is_fast endpoints. VPS tears down the existing
//! EndpointHandle and respawns it with the new start_chunk_id, mirroring
//! the existing add_endpoint pattern (api.rs:401).

use crate::api::{InitRequest, init_endpoints_handler, update_start_handler, UpdateStartRequest};
use crate::AppState;
use axum::{Json, extract::State};
use axum::http::StatusCode;
use std::sync::Arc;

fn test_state() -> Arc<AppState> {
    Arc::new(AppState::new_for_test())
}

async fn init_with_one_endpoint(state: &Arc<AppState>, alias: &str, start: i64) {
    let req = InitRequest::test_single_endpoint(alias, start, /*is_fast=*/ true);
    init_endpoints_handler(State(state.clone()), Json(req))
        .await
        .expect("init should succeed");
}

#[tokio::test]
async fn update_start_replaces_start_chunk_id_for_known_alias() {
    let state = test_state();
    init_with_one_endpoint(&state, "kiko", 100).await;

    let req = UpdateStartRequest {
        alias: "kiko".to_string(),
        new_start_chunk_id: 250,
    };
    let result = update_start_handler(State(state.clone()), Json(req)).await;
    assert!(result.is_ok(), "expected 200, got {result:?}");

    let endpoints = state.endpoints.read().await;
    let handle = endpoints.get("kiko").expect("kiko must still exist");
    assert_eq!(
        handle.start_chunk_id(),
        250,
        "EndpointHandle's start_chunk_id must reflect new value"
    );
}

#[tokio::test]
async fn update_start_returns_404_for_unknown_alias() {
    let state = test_state();
    init_with_one_endpoint(&state, "kiko", 100).await;

    let req = UpdateStartRequest {
        alias: "ghost".to_string(),
        new_start_chunk_id: 250,
    };
    let err = update_start_handler(State(state.clone()), Json(req))
        .await
        .expect_err("ghost alias should fail");
    assert_eq!(err, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn update_start_emits_endpoint_start_chunk_updated_audit_on_success() {
    let state = test_state();
    init_with_one_endpoint(&state, "kiko", 100).await;

    let pre_audit_count = state.audit_ring.read().await.len();
    let req = UpdateStartRequest {
        alias: "kiko".to_string(),
        new_start_chunk_id: 250,
    };
    update_start_handler(State(state.clone()), Json(req))
        .await
        .unwrap();

    let audit = state.audit_ring.read().await;
    assert_eq!(
        audit.len(),
        pre_audit_count + 1,
        "exactly one audit row added"
    );
    let row = audit.last().unwrap();
    assert_eq!(
        row.action,
        rs_core::audit::Action::EndpointStartChunkUpdated
    );
    let detail = &row.detail;
    assert_eq!(detail["alias"], "kiko");
    assert_eq!(detail["old_start_chunk_id"], 100);
    assert_eq!(detail["new_start_chunk_id"], 250);
}
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/main.rs crates/rs-delivery/src/api_update_start_tests.rs
git commit -m "test(vps): failing tests for POST /api/endpoints/update_start (#$ISSUE_NUM)

Handler, request struct, and EndpointHandle::start_chunk_id accessor do
not yet exist. Compile failure expected; Task 11 adds them. Tests verify:
- Known alias: replace start_chunk_id, return 200
- Unknown alias: return 404
- Success path emits exactly one EndpointStartChunkUpdated audit row
  with detail JSON containing alias + old + new chunk ids

If InitRequest::test_single_endpoint or AppState::new_for_test do not yet
exist as test seams, add them as minimal helpers in the same commit."
```

---

### Task 11: Implement VPS `update_start_handler`

**Files:**
- Modify: `crates/rs-delivery/src/api.rs` (new handler, request struct, route registration)
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (add `start_chunk_id()` accessor on `EndpointHandle`)

- [ ] **Step 1: Add the accessor on `EndpointHandle`**

In `crates/rs-delivery/src/endpoint_task.rs`, find `pub struct EndpointHandle` at line 144. Locate its existing `impl EndpointHandle` block at line 150. Add a method:

```rust
impl EndpointHandle {
    // ... existing methods ...

    /// Current start_chunk_id for this endpoint. Used by the
    /// POST /api/endpoints/update_start handler to verify the new
    /// value differs from the old and to populate the audit row.
    pub fn start_chunk_id(&self) -> i64 {
        self.start_chunk_id
    }
}
```

If `start_chunk_id` is not stored on `EndpointHandle` (verify by reading the struct definition at line 144), make it a stored field by adding it to the struct definition and capturing it in `EndpointHandle::spawn`:

```rust
pub struct EndpointHandle {
    // ... existing fields ...
    start_chunk_id: i64,
}

// Inside EndpointHandle::spawn(...), wire start_chunk_id into the returned struct.
```

- [ ] **Step 2: Add the request struct, handler, and route**

In `crates/rs-delivery/src/api.rs`, add near the other request structs:

```rust
#[derive(Debug, serde::Deserialize)]
pub struct UpdateStartRequest {
    pub alias: String,
    pub new_start_chunk_id: i64,
}
```

Add the handler (placement: near `add_endpoint` at line 401):

```rust
/// Replace an endpoint's start_chunk_id at host request. Tears down the
/// existing EndpointHandle and respawns it with the new start, mirroring
/// the add_endpoint pattern. Called by the host at VPS-ready moment for
/// is_fast endpoints so they begin pushing at the fresh live edge rather
/// than the stale chunk_id computed before VPS creation completed.
///
/// Returns 404 if the alias is unknown.
pub async fn update_start_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UpdateStartRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut endpoints = state.endpoints.write().await;
    let old_handle = endpoints
        .remove(&req.alias)
        .ok_or(StatusCode::NOT_FOUND)?;
    let old_start = old_handle.start_chunk_id();

    // Tear down the existing handle. EndpointHandle's Drop / explicit
    // shutdown method aborts the consumer task cleanly.
    old_handle.shutdown().await;

    // Respawn with new start_chunk_id, preserving all other config.
    let cfg = old_handle.config().clone();
    let new_handle = EndpointHandle::spawn(
        state.clone(),
        cfg,
        req.new_start_chunk_id,
    );
    endpoints.insert(req.alias.clone(), new_handle);
    drop(endpoints);

    // Audit: record the swap.
    crate::endpoint_audit::record(
        &state.audit_ring,
        rs_core::audit::AuditRow {
            severity: rs_core::audit::Severity::Info,
            source: rs_core::audit::Source::Delivery,
            event_id: None,
            instance_id: None,
            endpoint: Some(req.alias.clone()),
            action: rs_core::audit::Action::EndpointStartChunkUpdated,
            detail: serde_json::json!({
                "alias": req.alias,
                "old_start_chunk_id": old_start,
                "new_start_chunk_id": req.new_start_chunk_id,
            }),
            ts_override: None,
        },
    ).await;

    tracing::info!(
        alias = %req.alias,
        old_start_chunk_id = old_start,
        new_start_chunk_id = req.new_start_chunk_id,
        "update_start replaced endpoint"
    );
    Ok(StatusCode::OK)
}
```

If `EndpointHandle::shutdown(&self)`, `EndpointHandle::config(&self)`, or `endpoint_audit::record` do not exist with these signatures, add minimal versions in the same commit. The shutdown must abort the spawned consumer task; the audit-record helper is a simple `audit_ring.write().await.push(row)`.

Register the route. Find the router builder (search api.rs for `Router::new()` or `.route(`) and add:

```rust
.route("/api/endpoints/update_start", post(update_start_handler))
```

- [ ] **Step 3: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/src/api.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat(vps): POST /api/endpoints/update_start handler (#$ISSUE_NUM)

VPS-side half of the fast-endpoint live-edge recompute. Tears down the
existing EndpointHandle and respawns it with the new start_chunk_id,
mirroring the add_endpoint pattern. Emits EndpointStartChunkUpdated
audit row. Returns 404 for unknown alias."
```

---

### Task 12: TDD — failing tests for host `on_vps_ready`

**Files:**
- Create: `crates/rs-api/src/on_vps_ready_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (add `#[cfg(test)] mod on_vps_ready_tests;`)

- [ ] **Step 1: Register the test module**

In `crates/rs-api/src/lib.rs`, add (alongside the test module from Task 7):

```rust
#[cfg(test)]
mod on_vps_ready_tests;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/rs-api/src/on_vps_ready_tests.rs`:

```rust
//! Tests for the host-side `on_vps_ready` method.
//!
//! At VPS "delivering" transition (delivery.rs:704), host:
//! 1. Recomputes fresh_live_edge = MAX(seq where sent=1) + 1 from chunk_records
//! 2. For each is_fast=true endpoint, POSTs /api/endpoints/update_start with
//!    the fresh value
//! 3. Emits FastEndpointJumpedToLiveEdge audit row per endpoint
//! 4. Non-fast endpoints: no POST, no audit row
//! 5. 404 from VPS: warn-log, no audit row, no Err (graceful degradation
//!    against older VPS binaries that lack the endpoint)

use crate::delivery::test_helpers::{
    insert_event, insert_sent_chunk, insert_endpoint, mock_vps_server,
    install_audit_capture, AuditCapture,
};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

async fn setup_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn on_vps_ready_posts_update_start_for_fast_endpoints_only() {
    let pool = setup_pool().await;
    let event_id = insert_event(&pool, "ev").await;
    for s in 1..=30 {
        insert_sent_chunk(&pool, event_id, s, 2000).await;
    }
    insert_endpoint(&pool, event_id, "kiko", /*is_fast=*/ true, /*start=*/ 5).await;
    insert_endpoint(&pool, event_id, "fb", /*is_fast=*/ false, /*start=*/ 5).await;

    let vps = mock_vps_server().await;
    let orchestrator = crate::delivery::DeliveryOrchestrator::new_for_test(pool.clone());

    orchestrator
        .on_vps_ready(event_id, vps.instance(), &vps.client())
        .await
        .expect("on_vps_ready should succeed");

    let posts = vps.captured_update_start_requests();
    assert_eq!(posts.len(), 1, "exactly one POST (kiko only)");
    assert_eq!(posts[0].alias, "kiko");
    assert_eq!(posts[0].new_start_chunk_id, 31); // MAX(seq)+1 = 30+1
}

#[tokio::test]
async fn on_vps_ready_emits_audit_row_per_fast_endpoint() {
    let pool = setup_pool().await;
    let event_id = insert_event(&pool, "ev").await;
    for s in 1..=30 {
        insert_sent_chunk(&pool, event_id, s, 2000).await;
    }
    insert_endpoint(&pool, event_id, "kiko", /*is_fast=*/ true, /*start=*/ 5).await;
    insert_endpoint(&pool, event_id, "kiko2", /*is_fast=*/ true, /*start=*/ 7).await;

    let audit = install_audit_capture();
    let vps = mock_vps_server().await;
    let orchestrator = crate::delivery::DeliveryOrchestrator::new_for_test(pool.clone());

    orchestrator
        .on_vps_ready(event_id, vps.instance(), &vps.client())
        .await
        .unwrap();

    let rows = audit.collected();
    let jump_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.action == rs_core::audit::Action::FastEndpointJumpedToLiveEdge)
        .collect();
    assert_eq!(jump_rows.len(), 2);
    for r in &jump_rows {
        assert!(r.detail["gap_chunks"].as_i64().unwrap() > 0);
        assert_eq!(r.detail["to_chunk_id"], 31);
    }
}

#[tokio::test]
async fn on_vps_ready_treats_vps_404_as_no_op_no_err() {
    let pool = setup_pool().await;
    let event_id = insert_event(&pool, "ev").await;
    for s in 1..=10 {
        insert_sent_chunk(&pool, event_id, s, 2000).await;
    }
    insert_endpoint(&pool, event_id, "kiko", /*is_fast=*/ true, /*start=*/ 5).await;

    let vps = mock_vps_server().await;
    vps.set_response(http::StatusCode::NOT_FOUND);
    let orchestrator = crate::delivery::DeliveryOrchestrator::new_for_test(pool.clone());

    let result = orchestrator
        .on_vps_ready(event_id, vps.instance(), &vps.client())
        .await;
    assert!(
        result.is_ok(),
        "404 from older VPS must NOT bubble up as an error"
    );
}

#[tokio::test]
async fn on_vps_ready_with_no_fast_endpoints_is_noop() {
    let pool = setup_pool().await;
    let event_id = insert_event(&pool, "ev").await;
    for s in 1..=10 {
        insert_sent_chunk(&pool, event_id, s, 2000).await;
    }
    insert_endpoint(&pool, event_id, "fb", /*is_fast=*/ false, /*start=*/ 5).await;
    insert_endpoint(&pool, event_id, "yt", /*is_fast=*/ false, /*start=*/ 5).await;

    let vps = mock_vps_server().await;
    let orchestrator = crate::delivery::DeliveryOrchestrator::new_for_test(pool.clone());

    orchestrator
        .on_vps_ready(event_id, vps.instance(), &vps.client())
        .await
        .unwrap();

    assert_eq!(
        vps.captured_update_start_requests().len(),
        0,
        "no fast endpoints means no POST"
    );
}
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/lib.rs crates/rs-api/src/on_vps_ready_tests.rs
git commit -m "test(delivery): failing tests for on_vps_ready (#$ISSUE_NUM)

on_vps_ready method, delivery::test_helpers module, and mock_vps_server
helper do not yet exist. Compile failure expected; Task 13 adds them.
Tests verify:
- POSTs /api/endpoints/update_start to is_fast=true endpoints only
- new_start_chunk_id = MAX(seq where sent=1) + 1
- Emits FastEndpointJumpedToLiveEdge audit row per fast endpoint
- 404 from VPS is treated as no-op (no Err, no audit) for older-VPS compat
- No fast endpoints = no POST, no audit"
```

---

### Task 13: Implement `on_vps_ready` and wire into start_delivery

**Files:**
- Modify: `crates/rs-api/src/delivery.rs` (add `on_vps_ready` method on `DeliveryOrchestrator`; call it at line 704)

- [ ] **Step 1: Add the test-helpers module declaration**

At the top of `crates/rs-api/src/delivery.rs`, add (or extend the existing test module declarations):

```rust
#[cfg(test)]
pub mod test_helpers;
```

Create `crates/rs-api/src/delivery/test_helpers.rs` with the helpers used by Task 12's tests:

```rust
//! Test helpers for delivery-orchestrator tests. Minimal seams only; no
//! production code calls these.

use sqlx::{Row, SqlitePool};
use std::sync::{Arc, Mutex};

pub async fn insert_event(pool: &SqlitePool, name: &str) -> i64 {
    let row = sqlx::query("INSERT INTO streaming_events (name) VALUES (?1) RETURNING id")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("id")
}

pub async fn insert_sent_chunk(pool: &SqlitePool, event_id: i64, seq: i64, dur_ms: i64) {
    sqlx::query(
        "INSERT INTO chunk_records (streaming_event_id, sequence_number, duration_ms, sent)
         VALUES (?1, ?2, ?3, 1)",
    )
    .bind(event_id)
    .bind(seq)
    .bind(dur_ms)
    .execute(pool)
    .await
    .unwrap();
}

pub async fn insert_endpoint(
    pool: &SqlitePool,
    event_id: i64,
    alias: &str,
    is_fast: bool,
    start_chunk_id: i64,
) {
    // The exact INSERT depends on the endpoints schema; use the project's
    // existing helper if available, otherwise raw SQL into endpoints table.
    sqlx::query(
        "INSERT INTO endpoints (streaming_event_id, alias, is_fast, start_chunk_id)
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(event_id)
    .bind(alias)
    .bind(if is_fast { 1 } else { 0 })
    .bind(start_chunk_id)
    .execute(pool)
    .await
    .unwrap();
}

pub struct MockVpsServer {
    pub instance: rs_core::models::DeliveryInstance,
    pub captured: Arc<Mutex<Vec<CapturedUpdateStart>>>,
    pub response_status: Arc<Mutex<http::StatusCode>>,
    _server: wiremock::MockServer,
}

#[derive(Debug, Clone)]
pub struct CapturedUpdateStart {
    pub alias: String,
    pub new_start_chunk_id: i64,
}

impl MockVpsServer {
    pub fn instance(&self) -> &rs_core::models::DeliveryInstance {
        &self.instance
    }
    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }
    pub fn captured_update_start_requests(&self) -> Vec<CapturedUpdateStart> {
        self.captured.lock().unwrap().clone()
    }
    pub fn set_response(&self, status: http::StatusCode) {
        *self.response_status.lock().unwrap() = status;
    }
}

pub async fn mock_vps_server() -> MockVpsServer {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};
    let server = MockServer::start().await;
    let captured = Arc::new(Mutex::new(Vec::new()));
    let response_status = Arc::new(Mutex::new(http::StatusCode::OK));
    let captured_clone = captured.clone();
    let response_status_clone = response_status.clone();
    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let body: CapturedUpdateStart =
                serde_json::from_slice(&req.body).expect("valid JSON body");
            captured_clone.lock().unwrap().push(body);
            ResponseTemplate::new(response_status_clone.lock().unwrap().as_u16())
        })
        .mount(&server)
        .await;
    let instance = rs_core::models::DeliveryInstance::test_with_ipv4(
        server.uri().trim_start_matches("http://").to_string(),
    );
    MockVpsServer {
        instance,
        captured,
        response_status,
        _server: server,
    }
}

pub fn install_audit_capture() -> AuditCapture {
    AuditCapture::install()
}

pub struct AuditCapture { /* see existing audit-test helpers; reuse if present */ }
impl AuditCapture {
    pub fn install() -> Self { Self {} }
    pub fn collected(&self) -> Vec<rs_core::audit::AuditRow> { vec![] }
}
```

If `wiremock` is not yet a dev-dependency of `rs-api`, add it under `[dev-dependencies]` in `crates/rs-api/Cargo.toml`:

```toml
wiremock = "0.6"
http = "1"
```

If the project already has a test-mock pattern for VPS HTTP (search for `wiremock`, `mockito`, or similar in `crates/rs-api/`), follow that pattern instead. The tests in Task 12 only require: capture POST body, configurable response status.

If `DeliveryOrchestrator::new_for_test`, `CapturedUpdateStart` matching `UpdateStartRequest`, or `DeliveryInstance::test_with_ipv4` do not exist, add them as minimal test-only constructors in the same commit.

- [ ] **Step 2: Add `on_vps_ready` to `DeliveryOrchestrator`**

In `crates/rs-api/src/delivery.rs`, add inside `impl DeliveryOrchestrator { ... }` (near `start_delivery`):

```rust
    /// Called at the VPS "delivering" transition. For each is_fast=true endpoint
    /// on this event, recompute the fresh live-edge chunk_id and POST it to the
    /// VPS via /api/endpoints/update_start. Non-fast endpoints are skipped (they
    /// rely on their original start_chunk_id + buffer prefill).
    ///
    /// Errors from the VPS are NOT bubbled up:
    /// - 404: older VPS binary without the endpoint — warn-log, audit, continue
    /// - Network error: warn-log, audit, continue
    /// Real DB errors (computing fresh_live_edge) bubble up.
    pub async fn on_vps_ready(
        &self,
        event_id: i64,
        instance: &rs_core::models::DeliveryInstance,
        client: &reqwest::Client,
    ) -> anyhow::Result<()> {
        let fresh_live_edge = db::compute_target_start_chunk(&self.pool, event_id).await?;
        let endpoints = db::get_event_endpoints(&self.pool, event_id).await?;
        let fast_eps: Vec<_> = endpoints.iter().filter(|e| e.is_fast).collect();
        if fast_eps.is_empty() {
            return Ok(());
        }

        let url = format!("http://{}:8000/api/endpoints/update_start", instance.ipv4);
        for ep in &fast_eps {
            let payload = serde_json::json!({
                "alias": ep.alias,
                "new_start_chunk_id": fresh_live_edge,
            });
            let post_result = client
                .post(&url)
                .json(&payload)
                .timeout(std::time::Duration::from_secs(5))
                .send()
                .await;

            let outcome = match post_result {
                Ok(resp) if resp.status() == reqwest::StatusCode::OK => "ok",
                Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
                    warn!(
                        alias = %ep.alias,
                        "VPS lacks /api/endpoints/update_start (older binary); skipping"
                    );
                    "vps_404"
                }
                Ok(resp) => {
                    warn!(
                        alias = %ep.alias,
                        status = %resp.status(),
                        "update_start unexpected status; skipping"
                    );
                    "vps_unexpected"
                }
                Err(e) => {
                    warn!(alias = %ep.alias, error = %e, "update_start network error; skipping");
                    "network_error"
                }
            };

            // Only emit the FastEndpointJumpedToLiveEdge audit row on the success
            // path. The vps_404 / vps_unexpected / network_error paths are
            // warn-logged but do not get an audit row — they would clutter the
            // dashboard without representing a real jump.
            if outcome == "ok" {
                if let Some(tx) = &self.audit_tx {
                    rs_core::audit::record(
                        tx,
                        rs_core::audit::AuditRow {
                            severity: rs_core::audit::Severity::Info,
                            source: rs_core::audit::Source::Delivery,
                            event_id: Some(event_id),
                            instance_id: Some(instance.id),
                            endpoint: Some(ep.alias.clone()),
                            action: rs_core::audit::Action::FastEndpointJumpedToLiveEdge,
                            detail: serde_json::json!({
                                "alias": ep.alias,
                                "from_chunk_id": ep.start_chunk_id,
                                "to_chunk_id": fresh_live_edge,
                                "gap_chunks": fresh_live_edge - ep.start_chunk_id,
                            }),
                            ts_override: None,
                        },
                    );
                }
            }
        }
        Ok(())
    }
```

If `Endpoint::start_chunk_id` is not directly available on the row returned by `db::get_event_endpoints`, use whatever field carries the original start (verify the struct definition first; the spec uses `start_chunk_id` as the canonical name).

- [ ] **Step 3: Wire `on_vps_ready` into start_delivery at line 704**

At `crates/rs-api/src/delivery.rs:704`, find the block:

```rust
        db::update_delivery_instance_status(&self.pool, instance_id, "delivering").await?;
        info!(event_id, "Delivery endpoints initialized successfully");
```

Insert immediately AFTER the `info!` line and BEFORE the existing `// Spawn the per-delivery clock-skew probe ...` comment:

```rust
        // Push fresh live-edge start_chunk_id to is_fast endpoints now that the
        // VPS is delivering. Without this, fast endpoints stream from a chunk_id
        // computed before VPS creation finished (30-50s ago), staying behind
        // live edge indefinitely. See spec docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md.
        let on_ready_client = reqwest::Client::new();
        if let Err(e) = self.on_vps_ready(event_id, &instance, &on_ready_client).await {
            warn!(event_id, "on_vps_ready failed: {e}");
        }
```

- [ ] **Step 4: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-api/src/delivery.rs crates/rs-api/src/delivery/ crates/rs-api/Cargo.toml
git commit -m "feat(delivery): on_vps_ready pushes fresh live-edge to fast endpoints (#$ISSUE_NUM)

Host-side half of the fast-endpoint live-edge recompute. At VPS
'delivering' transition (delivery.rs:704), enumerate is_fast=true
endpoints and POST /api/endpoints/update_start with the fresh live
edge = compute_target_start_chunk(event_id). Non-fast endpoints are
skipped — they rely on their original start_chunk_id + buffer prefill.

Graceful degradation for older VPS binaries: 404 / network error =
warn-log, no audit row, no Err. Real DB errors bubble up.

Emits FastEndpointJumpedToLiveEdge audit row on each successful jump
with detail JSON {alias, from_chunk_id, to_chunk_id, gap_chunks}."
```

---

### Task 14: Leptos cache-bar fast-endpoint UX + Playwright assertion

**Files:**
- Modify: `leptos-ui/src/components/operator_dashboard.rs` (cache-bar render at lines 836-888)
- Modify: `e2e/frontend.spec.ts` (add assertion)

- [ ] **Step 1: Locate the existing cache bar render**

Open `leptos-ui/src/components/operator_dashboard.rs` and find the existing cache-bar block (around lines 836-888 per memory). Look for `cache_secs`, `cache_threshold_for_service`, or `buffer-bar-fill`.

- [ ] **Step 2: Branch on `ep.is_fast`**

Replace the existing label-and-class computation. Before the existing logic, add:

```rust
let (cache_secs, target_label, critical_above_secs, healthy_at_or_below_secs) =
    if ep.is_fast {
        // Fast endpoint UX (Kiko etc.): show lag from live edge.
        // Green when within 5s, critical above 8s. See spec
        // 2026-05-11-cache-metric-and-start-reset-design.md.
        (
            ep.chunk_delay_secs,
            "live".to_string(),
            8.0_f64,
            5.0_f64,
        )
    } else {
        // Non-fast UX unchanged.
        let target = ps.cache_delay_secs.max(1.0) as u64;
        let secs = if ep.chunks_processed > 0 {
            ep.chunk_delay_secs
        } else {
            ps.cache_duration_secs
        };
        let critical = target as f64 * cache_threshold_for_service(/* ... */);
        let healthy = target as f64 * 0.75;
        (secs, format!("{target}s"), critical, healthy)
    };

let label = format!("{}s / {} cache", cache_secs as u64, target_label);

let bar_class = if cache_secs > critical_above_secs {
    "buffer-bar-fill critical"
} else if cache_secs <= healthy_at_or_below_secs {
    "buffer-bar-fill healthy"
} else {
    "buffer-bar-fill warning"
};
```

Adjust the surrounding render code to consume `label` and `bar_class` instead of the previous inline computation. Preserve the existing `data-testid="endpoint-cache-label"` (or whatever attribute the Playwright test reads) so the assertion in Step 3 can find it.

If `cache_threshold_for_service` takes specific arguments not shown above, pass them as the existing code does. If `ps.cache_delay_secs` is named differently, use the project's actual field name (search the struct definition first).

- [ ] **Step 3: Add the Playwright assertion**

In `e2e/frontend.spec.ts`, add a new test or extend an existing dashboard-render test:

```typescript
test('fast endpoint cache bar label uses "live" target', async ({ page }) => {
  await page.goto(DASHBOARD_URL);
  // Wait for at least one endpoint card to render.
  await page.locator('[data-testid="endpoint-card"]').first().waitFor();

  // Fast endpoints carry data-is-fast="true"; render must show "Xs / live cache".
  const fastCards = page.locator('[data-testid="endpoint-card"][data-is-fast="true"]');
  const count = await fastCards.count();
  if (count > 0) {
    const label = await fastCards.first()
      .locator('[data-testid="endpoint-cache-label"]')
      .textContent();
    expect(label).toMatch(/^\d+s \/ live cache$/);
  } else {
    test.skip(true, 'No fast endpoints configured on this dashboard fixture');
  }
});
```

If `[data-testid="endpoint-card"]` or `[data-is-fast]` attributes do not yet exist on the rendered cards, add them in the same commit so the test can locate fast endpoints reliably.

- [ ] **Step 4: Format**

Run: `cargo fmt --all --check`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add leptos-ui/src/components/operator_dashboard.rs e2e/frontend.spec.ts
git commit -m "feat(dashboard): fast endpoint cache bar shows 'Xs / live', green when <=5s (#$ISSUE_NUM)

Branches on ep.is_fast:
- Fast: label 'Xs / live cache', healthy <=5s, critical >8s
- Non-fast: unchanged ('Xs / Ys cache' with target threshold)

Playwright assertion verifies the new label format on fast endpoint cards."
```

---

### Task 15: ORCHESTRATOR-ONLY — push, monitor CI, PR, post-deploy verify, soak

**This task is NOT dispatched as a subagent.** The orchestrator (the main session) handles it directly.

- [ ] **Step 1: Push dev**

```bash
git push origin dev
```

- [ ] **Step 2: Monitor CI to terminal state**

```bash
gh run list --branch dev --limit 3
RUN_ID=$(gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId')
# Single background poll, NOT a loop, NOT gh run watch (per ci-monitoring rule).
```

```
Bash(command: "sleep 600 && gh run view $RUN_ID --json status,conclusion,jobs", run_in_background: true)
```

When the BashOutput comes back, check ALL jobs (lint, test, integration, e2e, deploy-stream-lan). If anything fails: `gh run view $RUN_ID --log-failed`, fix root cause, push ONE consolidated fix commit, monitor again. Never blindly rerun.

- [ ] **Step 3: Verify deploy-stream-lan succeeded AND app responds**

```bash
gh run view $RUN_ID --json jobs --jq '.jobs[] | select(.name | contains("deploy-stream-lan")) | {name, conclusion}'
```

Expected: `conclusion: "success"`.

Then via MCP:

```
mcp__win-stream-snv__ListProcesses (filter: "Restreamer")
mcp__win-stream-snv__Shell: Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status
```

Expected: Restreamer.exe running in session > 0; `/api/v1/status` returns 200.

- [ ] **Step 4: Open PR**

```bash
gh pr create --base main --head dev --title "fix(delivery): cache-metric reform + Start Delivering reset + fast-endpoint live-edge recompute" --body "$(cat <<'EOF'
## Summary
Three operator-reported regressions fixed together:

- **A:** Per-endpoint `chunk_delay_secs` semantics shifted to "lag from live edge" — eliminates the 112s → 1s cache bar drop at first-push moment of Start Delivering.
- **B:** `streaming_events.received_bytes` reset to 0 on Start Delivering — dashboard now shows current-cycle bytes, not the 57GB cumulative that confused operators on multi-day events.
- **C:** Host POSTs fresh live-edge chunk_id to is_fast endpoints at VPS-ready — Kiko no longer streams 50s behind live after VPS creation.
- **Bonus:** Fast endpoint cache bar shows "Xs / live", green when ≤5s.

Spec: `docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md`
Issue: #$ISSUE_NUM

## Test plan
- [x] Unit: `get_endpoint_lag_secs` (4 assertions in `lag_tests.rs`)
- [x] Unit: `reset_event_received_bytes` (3 assertions in `delivery_reset_tests.rs`)
- [x] Unit: VPS `update_start_handler` (3 assertions in `api_update_start_tests.rs`)
- [x] Unit: host `on_vps_ready` (4 assertions in `on_vps_ready_tests.rs`)
- [x] E2E: Playwright assertion for fast endpoint "Xs / live" label
- [ ] Post-deploy on streamsnv: operator Stop+Start cycle shows smooth 0→120 cache bar with no drops > 10s
- [ ] Post-deploy on streamsnv: Kiko reports `current_chunk_id` within 2 chunks of live edge after VPS-ready
- [ ] Operator soak during next live event: Kiko visibly 120s ahead of FB/YT in downstream baked-in timestamps

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Verify PR is mergeable + clean**

```bash
PR_NUM=$(gh pr view dev --json number --jq .number)
gh api repos/zbynekdrlik/restreamer/pulls/$PR_NUM --jq '{mergeable, mergeable_state}'
```

Expected: `{mergeable: true, mergeable_state: "clean"}`. If "behind", run `gh pr update-branch`. If "dirty" or "blocked", fix the underlying issue.

- [ ] **Step 6: Post-deploy verification via Playwright on streamsnv**

Open the dashboard at `http://10.77.9.204:8910/` in Playwright. Verify:
1. **Liveness:** page loads, version label reads `v0.9.0-dev.N` matching the deployed binary's `git describe`.
2. **Stop+Start functional:** click Stop, wait 5s, click Start. Scrape `.endpoint-cache-label` every 5s for 3 min. Confirm NO sample drops more than 10s between adjacent reads. Confirm peak ≤ 130s (target × 1.08).
3. **received_bytes:** scrape the bytes display 30s after Start. Confirm < 100 MB (was reading 57 GB pre-fix).
4. **Fast endpoint label:** find a fast-endpoint card. Confirm label matches `/^\d+s \/ live cache$/`.
5. **Browser console:** zero errors, zero warnings.

If any of (2)/(3)/(4) fails, the PR is NOT done — investigate and push a fix to dev.

- [ ] **Step 7: Wait for operator soak window**

Hand off to the operator with a completion report per `completion-report.md` template. PR URL, dashboard URL, mergeable/clean status, what to look for during next live event (Kiko ahead of FB/YT in downstream timestamps).

DO NOT merge the PR yourself. Wait for explicit "merge it" from the user after the operator confirms the soak.

---

## Self-review checklist

After writing this plan, verifying against the spec:

### Spec coverage
- §2.1 (Change A — chunk_delay_secs semantics): Tasks 3, 4, 5, 6 ✓
- §2.2 (Change B — received_bytes reset): Tasks 7, 8 ✓
- §2.3 (Change C — fast endpoint start_chunk_id recompute): Tasks 9, 10, 11, 12, 13 ✓
- §2.4 (Bonus — fast endpoint UX): Task 14 ✓
- §3 (Data flow): covered by Task 13's wiring at delivery.rs:704
- §4 (Error handling): Task 8 warn-log; Task 13 graceful 404/network handling
- §5 (Testing): unit tests in Tasks 4, 7, 10, 12; E2E in Task 14; operator soak in Task 15
- §6 (Operator validation): Task 15 Steps 6, 7
- §9 (Acceptance): Task 15 Step 4 (PR body checklist) + Step 6 (Playwright verification)

### Placeholder scan
No "TBD", "TODO", "implement later" markers in task bodies. Each code step has exact code.

### Type consistency
- `get_endpoint_lag_secs(pool, event_id, endpoint_current_chunk_id) -> Result<f64>` used identically in Tasks 4, 5, 6 ✓
- `reset_event_received_bytes(pool, event_id) -> anyhow::Result<()>` used identically in Tasks 7, 8 ✓
- `UpdateStartRequest { alias: String, new_start_chunk_id: i64 }` used identically in Tasks 10, 11, 12, 13 ✓
- `Action::FastEndpointJumpedToLiveEdge` (host) vs `Action::EndpointStartChunkUpdated` (VPS) distinct and consistent across Tasks 9, 11, 12, 13 ✓
- `on_vps_ready(event_id, instance, client) -> anyhow::Result<()>` signature consistent across Tasks 12, 13 ✓
