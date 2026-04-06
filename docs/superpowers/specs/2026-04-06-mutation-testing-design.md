# Mutation Testing CI Gate

**Date:** 2026-04-06
**Status:** Approved
**Scope:** Add `cargo-mutants` mutation testing to CI pipeline

## Problem

Line coverage (cargo-tarpaulin at 55%) proves tests execute code but not that they verify behavior. A test suite can have high coverage with weak assertions that catch no real bugs. Mutation testing fills this gap by modifying code and checking that tests detect the change.

## Design

### Tool

`cargo-mutants` — the standard Rust mutation testing tool. Modifies source code (swaps operators, removes returns, changes constants) and re-runs tests. If tests still pass after a mutation, the test is weak.

### Approach: Diff-Only, Zero Survivors

Full workspace mutation (1048 mutants) takes 50-80 hours — not feasible in CI. Instead, enforce zero surviving mutants on changed code only.

| Scope | Rule | Purpose |
|-------|------|---------|
| Changed code only (`--in-diff`) | Zero surviving mutants | Every new/modified line must be properly tested |

### CI Job: `mutation-testing`

**Trigger:** `pull_request` to `main` only (not on push to `dev`)

**Runner:** `ubuntu-latest`

**Dependencies:** `needs: [lint]` — no point mutating code that doesn't compile

**Steps:**

1. **Checkout** with `fetch-depth: 0` (full history needed for diff generation)
2. **Rust toolchain** + `Swatinem/rust-cache@v2`
3. **System dependencies** — same as test job: `libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libssl-dev ffmpeg`
4. **Install cargo-mutants:** `cargo install cargo-mutants`
5. **Run mutation testing on diff:**
   ```bash
   git diff origin/main...HEAD > pr.diff
   cargo mutants --in-diff pr.diff --timeout 300 --build-timeout 600 --output mutants-out
   ```
   Exits non-zero if any mutant survives in changed code.
6. **Upload artifact** on failure: `mutants-out/` directory for debugging which mutants survived.

**Environment:**
- `SQLX_OFFLINE: true` (same as test job)

### Gate Integration

Add `mutation-testing` to `rust-ci-gate` needs list. In the gate check, use the same pattern as `version-check`: only require success on `pull_request` events, allow `skipped` on push.

### Timeout

Per-mutant timeout: 120 seconds. If a single test run takes longer than this, the mutant is marked as a timeout (not a survivor). This prevents infinite loops from mutations.

### Exclusions

- `src-tauri/` — excluded from workspace (not in members), won't be mutated
- `leptos-ui/` — excluded from workspace, won't be mutated
- Generated code or FFI bindings — `cargo-mutants` skips `unsafe` blocks by default

### Future: Full Workspace Scoring

Full workspace mutation (1048 mutants, 50-80 hours) is not feasible in CI today.
Options for the future: sharding across parallel jobs, nightly scheduled runs, or
limiting to specific high-value crates.

## Estimated CI Impact

| Metric | Value |
|--------|-------|
| Diff-only mutation | 5-15 min (depends on PR size) |
| Trigger | PR to main only |
| Impact on dev pushes | None |

## Non-Goals

- Mutation testing on `leptos-ui` (WASM target, different compilation)
- Mutation testing on E2E/integration tests (only unit tests)
- Custom mutant operators — use cargo-mutants defaults
