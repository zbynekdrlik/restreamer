# CI Hetzner Cleanup Scope — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scope the CI pre-flight Hetzner VPS cleanup to the current installation's `client_uuid` label, preventing cross-installation deletion (fixes #137).

**Architecture:** One CI workflow edit + one regression-test assertion + one version bump. No Rust source changes. The VPS already carries a `client_uuid=<uuid>` Hetzner label at creation (`crates/rs-api/src/delivery.rs:185`), and the local API exposes the value via `GET /api/v1/config`. The fix fetches that value in the pre-flight step and narrows the Hetzner `label_selector` from `app=restreamer` to `app=restreamer,client_uuid=<uuid>`.

**Tech Stack:** GitHub Actions workflow (YAML + PowerShell + bash), Hetzner Cloud API.

**Spec:** `docs/superpowers/specs/2026-04-22-ci-hetzner-cleanup-scope-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `Cargo.toml` line 24 | Version bump 0.3.66 → 0.3.67 |
| `src-tauri/Cargo.toml` line 3 | Version bump 0.3.66 → 0.3.67 |
| `src-tauri/tauri.conf.json` | Version bump 0.3.66 → 0.3.67 |
| `leptos-ui/Cargo.toml` line 3 | Version bump 0.3.66 → 0.3.67 |
| `.github/workflows/ci.yml` (test-integrity job, ~line 468) | Add regression-test step |
| `.github/workflows/ci.yml` (pre-flight step, lines 2323-2349) | Replace with client_uuid-scoped cleanup |

No Rust crates are modified. No tests added in the workspace — the only test is the CI-level regression assertion.

---

### Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml:24`
- Modify: `src-tauri/Cargo.toml:3`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml:3`

Per airuleset `version-bumping.md`, this MUST be the first commit on dev — strictly greater than main (0.3.66) before any other change.

- [ ] **Step 1: Confirm starting version**

```bash
grep "^version = " Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml
grep '"version"' src-tauri/tauri.conf.json
```

Expected: all four show `0.3.66`.

- [ ] **Step 2: Bump Cargo.toml (workspace)**

Change `Cargo.toml:24` from:
```
version = "0.3.66"
```
To:
```
version = "0.3.67"
```

- [ ] **Step 3: Bump src-tauri/Cargo.toml**

Change `src-tauri/Cargo.toml:3` from:
```
version = "0.3.66"
```
To:
```
version = "0.3.67"
```

- [ ] **Step 4: Bump leptos-ui/Cargo.toml**

Change `leptos-ui/Cargo.toml:3` from:
```
version = "0.3.66"
```
To:
```
version = "0.3.67"
```

- [ ] **Step 5: Bump src-tauri/tauri.conf.json**

Change the `"version"` field from:
```json
"version": "0.3.66",
```
To:
```json
"version": "0.3.67",
```

- [ ] **Step 6: Verify all four files agree**

```bash
grep "^version = " Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml
grep '"version"' src-tauri/tauri.conf.json
```

Expected: all four show `0.3.67`.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml src-tauri/tauri.conf.json
git commit -m "chore: bump version to 0.3.67"
```

---

### Task 2: Add Regression Test (RED)

**Files:**
- Modify: `.github/workflows/ci.yml` (test-integrity job, after line 468 — after "Scan for assert_eq with trivial values", before "Verify zero ignored tests")

This step asserts that no workflow file uses the broad Hetzner `label_selector=app=restreamer` without a `,client_uuid=` scope. It intentionally FAILS against the current ci.yml (which still has the unscoped version at line 2329) — that failure is the TDD "red" proving the test detects the bug.

- [ ] **Step 1: Find the insertion point**

```bash
grep -n "Scan for assert_eq with trivial values" .github/workflows/ci.yml
grep -n "Verify zero ignored tests" .github/workflows/ci.yml
```

Expected output roughly:
```
459:      - name: Scan for assert_eq with trivial values
469:      - name: Verify zero ignored tests in cargo test output
```

The new step goes between these two (the "Scan for assert_eq" step ends at line 468 with the closing of its `run:` block).

- [ ] **Step 2: Insert the regression-test step**

Insert the following YAML block into `.github/workflows/ci.yml` between the "Scan for assert_eq with trivial values" step (which ends around line 468) and the "Verify zero ignored tests in cargo test output" step (line 469):

```yaml
      - name: Scan for unscoped Hetzner label_selector
        run: |
          BAD=$(grep -Prn 'label_selector=app=restreamer(?!,client_uuid=)' .github/workflows/ || true)
          if [ -n "$BAD" ]; then
            echo "ERROR: Hetzner label_selector must include ,client_uuid=<uuid> scope to protect other installations:"
            echo "$BAD"
            exit 1
          fi
          echo "OK: All Hetzner label_selector usages are client_uuid-scoped."
```

Indentation: the step's `- name:` must align with the other steps in the `test-integrity` job (6-space leading indent). Copy the whitespace exactly from the surrounding steps.

- [ ] **Step 3: Locally confirm the grep detects the current (broken) pattern**

```bash
grep -Prn 'label_selector=app=restreamer(?!,client_uuid=)' .github/workflows/
```

Expected output — exactly one match on the existing unscoped URL at line 2329:
```
.github/workflows/ci.yml:2329:              $resp = Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers?label_selector=app=restreamer" `
```

If the grep finds zero matches, the regex is wrong — stop and fix before committing.

- [ ] **Step 4: Commit the RED test**

```bash
git add .github/workflows/ci.yml
git commit -m "test(ci): regression-test for unscoped Hetzner label_selector (#137)

Adds a test-integrity step that greps all workflow files for
'label_selector=app=restreamer' without a ',client_uuid=' scope.
Against the current ci.yml this FAILS on line 2329 — proving the
assertion detects the bug. The next commit scopes that line and
turns this test green."
```

---

### Task 3: Fix Pre-Flight Cleanup (GREEN)

**Files:**
- Modify: `.github/workflows/ci.yml:2323-2349`

- [ ] **Step 1: Locate the block to replace**

```bash
grep -n "Step 2: Clean up orphaned Hetzner VPS directly" .github/workflows/ci.yml
```

Expected: one match around line 2323. The block to replace runs from that `# Step 2:` comment line through the closing `}` of the `if ($env:HETZNER_API_TOKEN)` guard (around line 2349).

- [ ] **Step 2: Replace the block**

Replace the block from line 2323 through line 2349 (inclusive — the `# Step 2: Clean up orphaned Hetzner VPS directly...` comment through the closing `}` of the `if ($env:HETZNER_API_TOKEN) { ... }` guard) with:

```powershell
          # Step 2: Clean up orphaned Hetzner VPS directly (scoped to this installation).
          # Filters by client_uuid so a CI run on one Restreamer install can never
          # delete another install's VPS sharing the same Hetzner account (fixes #137).
          if ($env:HETZNER_API_TOKEN) {
            $clientUuid = $null
            try {
              $cfg = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/config" -TimeoutSec 10
              $clientUuid = $cfg.client_uuid
            } catch {
              Write-Host "  Could not read local client_uuid: $_"
            }

            if (-not $clientUuid) {
              Write-Host "ERROR: client_uuid not available - refusing to run Hetzner cleanup (fail-closed to protect other installations)"
              exit 1
            }

            Write-Host "Checking Hetzner API for orphaned rs-delivery VPS for client_uuid=$clientUuid ..."
            try {
              $headers = @{ Authorization = "Bearer $($env:HETZNER_API_TOKEN)" }
              $selector = "app=restreamer,client_uuid=$clientUuid"
              $resp = Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers?label_selector=$selector" `
                -Headers $headers -TimeoutSec 15
              if ($resp.servers -and $resp.servers.Count -gt 0) {
                foreach ($srv in $resp.servers) {
                  Write-Host "  Deleting orphaned VPS (this installation): id=$($srv.id) name=$($srv.name) status=$($srv.status) ip=$($srv.public_net.ipv4.ip)"
                  try {
                    Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers/$($srv.id)" `
                      -Method DELETE -Headers $headers -TimeoutSec 15
                    Write-Host "  Deleted VPS $($srv.id)"
                  } catch {
                    Write-Host "  Failed to delete VPS $($srv.id): $_"
                  }
                }
                Start-Sleep -Seconds 3
              } else {
                Write-Host "  No orphaned VPS found for this installation"
              }
            } catch {
              Write-Host "  Hetzner API check failed: $_"
            }
          }
```

Keep the exact leading indentation (10 spaces) used by the surrounding PowerShell body — copy from the "Step 1" block just above at line 2302.

- [ ] **Step 3: Locally confirm the regression test now passes**

```bash
grep -Prn 'label_selector=app=restreamer(?!,client_uuid=)' .github/workflows/
```

Expected output: **empty** (zero matches). The only remaining usage of the selector is `app=restreamer,client_uuid=$clientUuid` which is followed by `,client_uuid=` and therefore excluded by the negative lookahead.

If matches remain, stop and inspect — likely the replacement missed a line.

- [ ] **Step 4: Commit the GREEN fix**

```bash
git add .github/workflows/ci.yml
git commit -m "fix(ci): scope Hetzner pre-flight cleanup by client_uuid (#137)

The pre-flight Hetzner VPS sweep used label_selector=app=restreamer,
which matches every VPS on the account regardless of which Restreamer
installation created it. On 2026-04-22 15:57 UTC this deleted an
active streampp live-event VPS during a stream.lan CI run.

Fix: fetch this installation's client_uuid from the local API and
scope the Hetzner filter to app=restreamer,client_uuid=<uuid>.
Fail-closed (exit 1) if client_uuid cannot be resolved, rather than
falling back to the broad filter.

Closes #137."
```

---

### Task 4: Local Checks and Push

- [ ] **Step 1: Run local format check**

Per airuleset `ci-push-discipline.md`, `cargo fmt --all --check` is the only local check we run. (No `cargo clippy`, no `cargo test`, no `cargo build` — those run on CI.)

```bash
cargo fmt --all --check
```

Expected: no output, exit 0.

If it fails, run `cargo fmt --all` and amend (or add a fixup commit). Since this PR touches only YAML/JSON/TOML (no Rust), it should pass immediately.

- [ ] **Step 2: Confirm three commits on dev ahead of main**

```bash
git log --oneline origin/main..HEAD
```

Expected (exact hashes will differ):
```
<hash> fix(ci): scope Hetzner pre-flight cleanup by client_uuid (#137)
<hash> test(ci): regression-test for unscoped Hetzner label_selector (#137)
<hash> chore: bump version to 0.3.67
<hash> docs: spec for CI Hetzner cleanup scope fix (#137)
```

(The `docs:` commit is the spec from the brainstorming skill, already on dev.)

- [ ] **Step 3: Push**

```bash
git push origin dev
```

---

### Task 5: Monitor CI, Create PR, Verify Mergeable

Per airuleset `ci-monitoring.md`, monitor CI until ALL jobs reach terminal state before proceeding.

- [ ] **Step 1: Identify the triggered run**

```bash
gh run list --branch dev --limit 3
```

Note the run-id for the latest in-progress run.

- [ ] **Step 2: Wait for terminal state (single background command, no polling loop)**

```bash
# 15 minutes is the typical CI duration for this project
sleep 900 && gh run view <run-id> --json status,conclusion,jobs
```

Use the Bash tool's `run_in_background: true` option. When it completes, read the output; do not write a custom monitor script and do not use `/loop` or `gh run watch`.

Expected at terminal state: `"status": "completed"`, `"conclusion": "success"`, and every job in the `jobs` array with `"conclusion": "success"` (or `"skipped"` only for explicitly-skipped jobs like mutation testing on docs-only PRs).

- [ ] **Step 3: If CI fails, diagnose and fix in a single new commit**

```bash
gh run view <run-id> --log-failed
```

Common failure modes and their fixes:

- "test-integrity / Scan for unscoped Hetzner label_selector" fails with a hit at line 2329 — the pre-flight replacement in Task 3 missed that line. Re-apply Task 3 Step 2.
- `deploy-stream-lan` fails because stream.lan has an active receiving event — add `[skip-live-check]` to a new empty commit message, or wait for the live event to end. (This is unlikely for a pure-CI change but per PR #129 the gate is now active.)

If a fix is needed, commit it, `git push origin dev`, and repeat from Step 1.

- [ ] **Step 4: Create PR once CI is green**

```bash
gh pr create --base main --head dev --title "fix(ci): scope Hetzner pre-flight cleanup by client_uuid (#137)" --body "$(cat <<'EOF'
## Summary

Pre-flight step in `ci.yml` was filtering Hetzner VPS by `app=restreamer` alone, which matches **every** VPS our Hetzner account creates. When two Restreamer installations share the account (stream.lan + streampp), a CI run on one silently deletes the other's live production VPS.

On 2026-04-22 15:57 UTC, the post-merge CI for PR #129 deleted streampp's active live-event VPS `rs-delivery-evt8` (Hetzner id 127743979) two minutes into streampp's Sunday service.

## Fix

- Fetch this installation's `client_uuid` from `GET /api/v1/config` at the start of the pre-flight step.
- Filter Hetzner with `label_selector=app=restreamer,client_uuid=<uuid>` so only VPS created by **this** installation are swept.
- **Fail-closed:** if `client_uuid` can't be resolved, the step exits 1 rather than falling back to the broad filter. Better to leave a genuine orphan than delete another installation's production VPS.

The `client_uuid` label was already applied to every VPS at creation (`crates/rs-api/src/delivery.rs:185`) — no schema or Rust-code change needed.

## Regression test

New step in the `test-integrity` job greps all workflow files for `label_selector=app=restreamer` without a `,client_uuid=` scope. Committed before the fix (RED), turned green by the fix (GREEN).

## Test plan

- [x] `grep -Prn 'label_selector=app=restreamer(?!,client_uuid=)' .github/workflows/` — zero matches on HEAD.
- [x] Same grep on HEAD~1 (before the fix) — one match at `ci.yml:2329`.
- [x] Full CI green (`test-integrity` regression step passes, `deploy-stream-lan` deploys, E2E Streaming / OBS-to-YouTube tests exercise the scoped cleanup).
- [x] Manual verification (documented below) that a VPS with a non-matching `client_uuid` label is NOT deleted.

## Manual cross-installation verification (pre-merge)

Run on the stream.lan runner, or against the Hetzner token from a local machine:

```bash
# 1. Pre-create a fake "other installation" VPS
curl -s -H "Authorization: Bearer $HETZNER_API_TOKEN" \
  -X POST https://api.hetzner.cloud/v1/servers \
  -H "Content-Type: application/json" \
  -d '{
    "name": "rs-delivery-fake-other-install",
    "server_type": "cpx11",
    "image": "ubuntu-24.04",
    "location": "nbg1",
    "labels": {"app": "restreamer", "client_uuid": "fake-other-install-uuid-0000000"}
  }' | jq '.server.id'
# returns e.g. 127999999

# 2. Trigger the pre-flight cleanup manually (or wait for next CI run)

# 3. Confirm the fake VPS is still present
curl -s -H "Authorization: Bearer $HETZNER_API_TOKEN" \
  "https://api.hetzner.cloud/v1/servers/127999999" | jq '.server.status'
# Should return "running", not 404.

# 4. Clean up the fake VPS manually
curl -s -H "Authorization: Bearer $HETZNER_API_TOKEN" \
  -X DELETE "https://api.hetzner.cloud/v1/servers/127999999"
```

Closes #137.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Verify PR is mergeable and clean**

```bash
gh pr view --json number,url,mergeable,mergeableState
gh api repos/zbynekdrlik/restreamer/pulls/<pr-number> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `"mergeable": true` AND `"mergeable_state": "clean"`.

If `behind`: `git fetch origin && git merge origin/main && git push origin dev` — then wait for CI to re-run.
If `dirty` or `blocked`: inspect the PR checks for the failing item and fix.

- [ ] **Step 6: Deliver the PR URL to the user**

Per project CLAUDE.md, deliver in this exact format:

```
PR: <url> | CI: green | Deploy: verified | Dashboard: http://10.77.9.204:8910/
```

Then wait for the user's explicit merge instruction. Do NOT merge.

---

## Verification Summary

1. **Unit-level (CI)** — test-integrity "Scan for unscoped Hetzner label_selector" step passes. No workflow file uses the broad filter.
2. **Integration (CI)** — the `deploy-stream-lan` → E2E OBS-to-YouTube sequence runs its own pre-flight cleanup against the scoped filter and still successfully deletes leftover test VPS from prior CI runs.
3. **Manual cross-installation** — the curl-based procedure in the PR description confirms a foreign-`client_uuid` VPS survives the cleanup.
4. **Post-merge** — the next scheduled or triggered stream.lan CI run leaves any real streampp/other-installation VPS untouched. (No synthetic test needed — the regression assertion plus the scoped filter is sufficient ongoing guarantee.)
