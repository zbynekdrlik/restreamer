# Issue #120 — add/remove_endpoint Mutation Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kill the 4 surviving mutants on `add_endpoint_to_delivery` and `remove_endpoint_from_delivery` (delete-`!`-in-guard, replace-body-with-`Ok(())`) and remove the two `--exclude-re` lines from CI mutation testing.

**Architecture:** Two integration tests in a new `crates/rs-api/tests/delivery_endpoints_tests.rs` file. Each test seeds an in-memory SqlitePool with a `delivery_instances` row whose `status = "creating"` and `ipv4 = "unreachable.invalid"`, calls the function under test, and asserts the returned Err message contains `"not in an active delivery state"`. That single assertion kills both mutant classes per function — no mock HTTP server needed.

**Tech Stack:** Rust 2024, sqlx 0.8 (in-memory SQLite), tokio, anyhow, cargo-mutants (CI-side).

**Spec:** `docs/superpowers/specs/2026-04-19-issue-120-add-remove-endpoint-mutation-tests-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `Cargo.toml` (workspace, line 24) | Workspace version | Modify: 0.3.64 → 0.3.65 |
| `src-tauri/Cargo.toml` (line 3) | Tauri crate version | Modify: 0.3.64 → 0.3.65 |
| `src-tauri/tauri.conf.json` (line 4) | Tauri runtime version | Modify: 0.3.64 → 0.3.65 |
| `leptos-ui/Cargo.toml` (line 3) | Leptos UI crate version | Modify: 0.3.64 → 0.3.65 |
| `crates/rs-api/tests/delivery_endpoints_tests.rs` | New integration tests + setup helper | Create |
| `.github/workflows/ci.yml` (lines 231-232) | Mutation-testing exclusion list | Modify: remove 2 `--exclude-re` lines |

Tests are integration-style (under `tests/`, not `#[cfg(test)] mod`) per user choice during brainstorming. Pattern matches `crates/rs-endpoint/tests/uploader_integration.rs`.

---

### Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml` (line 24)
- Modify: `src-tauri/Cargo.toml` (line 3)
- Modify: `src-tauri/tauri.conf.json` (line 4)
- Modify: `leptos-ui/Cargo.toml` (line 3)

- [ ] **Step 1: Bump version 0.3.64 → 0.3.65 in `Cargo.toml`**

In `Cargo.toml` line 24, change:
```toml
version = "0.3.64"
```
to:
```toml
version = "0.3.65"
```

- [ ] **Step 2: Bump version 0.3.64 → 0.3.65 in `src-tauri/Cargo.toml`**

In `src-tauri/Cargo.toml` line 3, change:
```toml
version = "0.3.64"
```
to:
```toml
version = "0.3.65"
```

- [ ] **Step 3: Bump version 0.3.64 → 0.3.65 in `src-tauri/tauri.conf.json`**

In `src-tauri/tauri.conf.json` line 4, change:
```json
"version": "0.3.64",
```
to:
```json
"version": "0.3.65",
```

- [ ] **Step 4: Bump version 0.3.64 → 0.3.65 in `leptos-ui/Cargo.toml`**

In `leptos-ui/Cargo.toml` line 3, change:
```toml
version = "0.3.64"
```
to:
```toml
version = "0.3.65"
```

- [ ] **Step 5: Verify rustfmt is clean**

Run: `cargo fmt --all --check`
Expected: exit 0, no output.

- [ ] **Step 6: Commit version bump**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.65"
```

---

### Task 2: Add Integration Test File With Failing Tests

**Files:**
- Create: `crates/rs-api/tests/delivery_endpoints_tests.rs`

- [ ] **Step 1: Create the new integration test file with the full test code**

Create `crates/rs-api/tests/delivery_endpoints_tests.rs` with the following exact content:

```rust
//! Integration tests for `add_endpoint_to_delivery` and
//! `remove_endpoint_from_delivery` guard clauses (issue #120).
//!
//! These tests kill two surviving-mutant classes per function:
//! - delete `!` in `if !is_delivery_active(...)` (guard inverts)
//! - replace function body with `Ok(())` (no-op)
//!
//! Both are killed by asserting the error MESSAGE contains the
//! guard's exact substring: "not in an active delivery state".
//! With either mutant applied, the message would not match.

use rs_api::delivery::DeliveryOrchestrator;
use rs_api::delivery_endpoints::{
    StartPosition, add_endpoint_to_delivery, remove_endpoint_from_delivery,
};
use rs_core::config::Config;
use rs_core::db;
use sqlx::SqlitePool;

/// Build an in-memory DB + orchestrator + config seeded with one
/// `endpoint_configs` row and one `delivery_instances` row whose
/// status is `status` and ipv4 points at an RFC 2606 .invalid host
/// (so the mutated `!`-deleted code path fails fast on DNS instead
/// of timing out).
async fn setup_with_status(
    status: &str,
) -> (DeliveryOrchestrator, SqlitePool, Config, i64, i64) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let endpoint_id: i64 = sqlx::query_scalar(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key)
         VALUES ('yt', 'YT_HLS', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let event_id: i64 = 42;

    let instance_id = db::create_delivery_instance(
        &pool,
        /* hetzner_id */ 1,
        /* name */ "test-instance",
        /* ipv4 */ "unreachable.invalid",
        /* server_type */ "cx22",
        Some(event_id),
        /* auth_token */ "test-token",
    )
    .await
    .unwrap();

    db::update_delivery_instance_status(&pool, instance_id, status)
        .await
        .unwrap();

    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    let orch = DeliveryOrchestrator::new(pool.clone(), config.clone()).unwrap();

    (orch, pool, config, event_id, endpoint_id)
}

#[tokio::test]
async fn add_endpoint_to_delivery_rejects_inactive_delivery() {
    let (orch, pool, config, event_id, endpoint_id) = setup_with_status("creating").await;
    let err = add_endpoint_to_delivery(
        &orch,
        &pool,
        &config,
        event_id,
        endpoint_id,
        StartPosition::Live,
    )
    .await
    .expect_err("creating state must be rejected by guard clause");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn remove_endpoint_from_delivery_rejects_inactive_delivery() {
    let (orch, pool, _config, event_id, _endpoint_id) = setup_with_status("creating").await;
    let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt")
        .await
        .expect_err("creating state must be rejected by guard clause");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error message: {err}"
    );
}
```

- [ ] **Step 2: Verify rustfmt is clean**

Run: `cargo fmt --all --check`
Expected: exit 0, no output.

- [ ] **Step 3: Commit the test file**

These tests already exercise the EXISTING guard-clause behavior (they pass against unmodified code — that's the whole point: they are tests that the *existing* code already satisfies, and that the *mutated* code would not).

```bash
git add crates/rs-api/tests/delivery_endpoints_tests.rs
git commit -m "test: cover add/remove_endpoint guard clauses (#120)"
```

---

### Task 3: Remove the Two Exclusion Lines From CI

**Files:**
- Modify: `.github/workflows/ci.yml` (lines 231-232)

- [ ] **Step 1: Delete the two `--exclude-re` lines**

In `.github/workflows/ci.yml`, find this exact block (around lines 230-233):

```yaml
            --exclude-re 'DeliveryOrchestrator::monitor_delivery_health' \
            --exclude-re 'add_endpoint_to_delivery' \
            --exclude-re 'remove_endpoint_from_delivery' \
            --exclude-re 'MAX_RESCUE_VIDEO_BYTES' \
```

Delete the middle two lines so it reads:

```yaml
            --exclude-re 'DeliveryOrchestrator::monitor_delivery_health' \
            --exclude-re 'MAX_RESCUE_VIDEO_BYTES' \
```

- [ ] **Step 2: Commit the CI change**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: remove add/remove_endpoint mutation exclusion (#120)"
```

---

### Task 4: Push, Monitor CI, Open PR, Verify Mergeable

**Files:** none (git/CI operations only)

- [ ] **Step 1: Final rustfmt check before push**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Identify the latest CI run**

```bash
gh run list --branch dev --limit 3
```
Note the most recent run id triggered by your push.

- [ ] **Step 4: Monitor CI to terminal state**

Use a single backgrounded `sleep && gh run view` per the airuleset CI-monitoring rule. Replace `<run-id>`:

```bash
sleep 600 && gh run view <run-id> --json status,conclusion,jobs
```
Run with `run_in_background: true`. When it completes, read with BashOutput.

Expected: every job conclusion is `success`. Pay special attention to `mutation-testing` — this is the job that previously had the two functions excluded; with the exclusion removed and the new tests in place, it must still pass.

If `mutation-testing` fails: read the failed log via `gh run view <run-id> --log-failed`, examine which mutant survived, write an additional assertion in `delivery_endpoints_tests.rs` that kills it, and re-push (single batched commit).

- [ ] **Step 5: Open PR from `dev` to `main`**

```bash
gh pr create --title "fix: cover add/remove_endpoint guard clauses for mutation testing (#120)" --body "$(cat <<'EOF'
## Summary
- Add 2 integration tests in `crates/rs-api/tests/delivery_endpoints_tests.rs` that kill the surviving mutants on `add_endpoint_to_delivery` and `remove_endpoint_from_delivery` by asserting the guard-clause error message
- Remove the two `--exclude-re 'add_endpoint_to_delivery'` / `'remove_endpoint_from_delivery'` lines from `.github/workflows/ci.yml`
- Bump version to 0.3.65

Closes #120

## Test plan
- [ ] CI mutation-testing job passes without the two exclusion lines
- [ ] New tests pass in CI test suite
- [ ] PR mergeable & clean
EOF
)"
```

- [ ] **Step 6: Monitor PR CI run**

The PR push triggers an additional `pull_request` run. Identify it and monitor:

```bash
gh run list --limit 5
sleep 600 && gh run view <pr-run-id> --json status,conclusion,jobs
```
Expected: all jobs `success`.

- [ ] **Step 7: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<pr-number> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```
Expected: `mergeable: true`, `mergeable_state: "clean"`.

- [ ] **Step 8: Report PR URL to the user and STOP**

Per `pr-merge-policy.md`: do NOT merge. Wait for explicit user "merge it".

---

## Verification

1. **Test correctness:** Both tests assert the existing guard-clause behavior — they pass on the unmodified production code. Mutation testing then proves that BOTH the `!`-deletion and `Ok(())` mutants are killed (CI's mutation-testing job passes without the exclusion lines).
2. **CI mutation-testing job:** Green, with `add_endpoint_to_delivery` and `remove_endpoint_from_delivery` removed from `--exclude-re`.
3. **Acceptance criteria from issue #120:**
   - [x] Unit tests cover the guard-clause branch for each function (Task 2)
   - [x] `cargo mutants --in-diff` passes without the exclusion (Task 4 Step 4 confirms)
   - [x] Two `--exclude-re` lines removed from ci.yml (Task 3)
4. **Version bump:** 0.3.64 → 0.3.65 in 4 files (Task 1).
5. **PR:** mergeable, clean, awaiting explicit user approval to merge.
