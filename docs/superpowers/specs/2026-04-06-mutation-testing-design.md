# Mutation Testing CI Gate

**Date:** 2026-04-06
**Status:** Approved
**Scope:** Add `cargo-mutants` mutation testing to CI pipeline

## Problem

Line coverage (cargo-tarpaulin at 55%) proves tests execute code but not that they verify behavior. A test suite can have high coverage with weak assertions that catch no real bugs. Mutation testing fills this gap by modifying code and checking that tests detect the change.

## Design

### Tool

`cargo-mutants` — the standard Rust mutation testing tool. Modifies source code (swaps operators, removes returns, changes constants) and re-runs tests. If tests still pass after a mutation, the test is weak.

### Two-Tier Approach

| Tier | Scope | Rule | Purpose |
|------|-------|------|---------|
| **Tier 1** | Changed code only (`--in-diff`) | Zero surviving mutants | New/modified code must be properly tested |
| **Tier 2** | Full workspace | >= 60% mutation score | Existing tests must meet minimum quality bar |

Both tiers run in a single CI job. Tier 1 runs first (fast fail). Tier 2 runs second.

### CI Job: `mutation-testing`

**Trigger:** `pull_request` to `main` only (not on push to `dev`)

**Runner:** `ubuntu-latest`

**Dependencies:** `needs: [lint]` — no point mutating code that doesn't compile

**Steps:**

1. **Checkout** with `fetch-depth: 0` (full history needed for diff generation)
2. **Rust toolchain** + `Swatinem/rust-cache@v2`
3. **System dependencies** — same as test job: `libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libssl-dev ffmpeg`
4. **Install cargo-mutants:** `cargo install cargo-mutants`
5. **Tier 1 — Diff zero survivors:**
   ```bash
   git diff origin/main...HEAD > pr.diff
   cargo mutants --in-diff pr.diff --timeout 120
   ```
   Exits non-zero if any mutant survives in changed code. Fast (2-5 min).
6. **Tier 2 — Full workspace 60%:**
   ```bash
   cargo mutants --workspace --timeout 120 --output mutants-out
   ```
   Parse `mutants-out/outcomes.json` for mutation score:
   ```bash
   killed=$(jq '[.outcomes[] | select(.scenario != "Baseline") | select(.summary == "CaughtMutant")] | length' mutants-out/outcomes.json)
   total=$(jq '[.outcomes[] | select(.scenario != "Baseline")] | length' mutants-out/outcomes.json)
   score=$((killed * 100 / total))
   if [ "$score" -lt 60 ]; then
     echo "FAIL: Mutation score ${score}% is below 60% threshold"
     exit 1
   fi
   echo "OK: Mutation score ${score}%"
   ```
   Fail if below 60%.
7. **Upload artifact** on failure: `mutants-out/` directory for debugging which mutants survived.

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

### Future Ratcheting

As test quality improves, increase Tier 2 threshold:
- **Phase 1:** 60% (initial gate)
- **Phase 2:** 70% (after test hardening pass)
- **Phase 3:** 80% (target)

Update the threshold in `ci.yml` when ready to ratchet.

## Estimated CI Impact

| Metric | Value |
|--------|-------|
| Tier 1 (diff-only) | 2-5 min |
| Tier 2 (full workspace) | 30-60 min |
| Total job time | 35-65 min |
| Trigger | PR to main only |
| Impact on dev pushes | None |

## Non-Goals

- Mutation testing on `leptos-ui` (WASM target, different compilation)
- Mutation testing on E2E/integration tests (only unit tests)
- Custom mutant operators — use cargo-mutants defaults
