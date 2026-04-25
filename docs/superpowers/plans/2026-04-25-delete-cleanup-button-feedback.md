# Delete + Cleanup Button Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issue #128 by surfacing in-progress state and API errors for the Events-tab `Delete + Cleanup` and `Clear S3 chunks` buttons in the Leptos dashboard.

**Architecture:** Pure UI fix in a single Leptos component (`leptos-ui/src/components/settings.rs`). Add two `RwSignal`s — `busy_event_id` and `action_error` — wire them into the existing per-card buttons (disabled + label flip while busy) and render an inline error banner above the events list when the API call fails. Modal still closes immediately on confirm; the visible feedback now lives on the card itself. Backend unchanged.

**Tech Stack:** Rust + Leptos (CSR/WASM frontend), Playwright + TypeScript (E2E), existing CSS classes (`.error-message`, `.btn-danger`, `.btn-secondary`).

**Spec:** `docs/superpowers/specs/2026-04-25-delete-cleanup-button-feedback-design.md`

**Issue:** #128

---

## File map

- **Modify:** `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` — version bump 0.3.69 → 0.3.70
- **Modify:** `e2e/playwright-frontend.config.ts:9` — extend testMatch regex to include the new spec
- **Create:** `e2e/delete-cleanup-button.spec.ts` — two TDD tests (success path with busy-state assertions, error path with banner assertion)
- **Modify:** `leptos-ui/src/components/settings.rs:411-700` — add busy/error signals, rewrite both confirm callbacks, make card-actions buttons reactive on busy state, render error banner

---

## Constraints (every task)

- Local checks: `cargo fmt --all --check` only. **No** `cargo build/test/clippy` locally — those run on CI per airuleset `ci-push-discipline`.
- TDD: failing test first, then implementation. The failing-test commit and the implementation commit go to dev as separate commits in the same push.
- File size gate: `settings.rs` is currently ~830 lines after the ~50-line addition; well under the 1000-line limit.
- One commit per task. Don't batch tasks.

---

### Task 1: Version bump

**Files:**
- Modify: `Cargo.toml` (workspace root, version line near top)
- Modify: `src-tauri/Cargo.toml` (top-level `version` field)
- Modify: `src-tauri/tauri.conf.json` (`"version": "0.3.69"`)
- Modify: `leptos-ui/Cargo.toml` (`version` field)

- [ ] **Step 1: Confirm current version on dev matches main**

```bash
git fetch origin
grep '^version' Cargo.toml | head -1
git show origin/main:Cargo.toml | grep '^version' | head -1
```

Expected: both lines show `version = "0.3.69"`. If they differ, the bump may already be done — check `git log` and skip this task.

- [ ] **Step 2: Bump `Cargo.toml`**

Change the workspace `version = "0.3.69"` line to `version = "0.3.70"`.

- [ ] **Step 3: Bump `src-tauri/Cargo.toml`**

Change `version = "0.3.69"` to `version = "0.3.70"`.

- [ ] **Step 4: Bump `src-tauri/tauri.conf.json`**

Change `"version": "0.3.69"` to `"version": "0.3.70"`.

- [ ] **Step 5: Bump `leptos-ui/Cargo.toml`**

Change `version = "0.3.69"` to `version = "0.3.70"`.

- [ ] **Step 6: Verify formatting**

```bash
cargo fmt --all --check
```

Expected: no output (clean).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.70"
```

---

### Task 2: Add failing Playwright spec (TDD red)

**Files:**
- Create: `e2e/delete-cleanup-button.spec.ts`
- Modify: `e2e/playwright-frontend.config.ts:9` (extend testMatch regex)

- [ ] **Step 1: Extend testMatch regex in `e2e/playwright-frontend.config.ts`**

Find this line (near line 9):

```ts
testMatch: /(frontend|audit-panel|zero-endpoint-banner|remove-last-endpoint-modal|endpoint-history-sparkline|cache-drift-panel)\.spec\.ts$/,
```

Replace with:

```ts
testMatch: /(frontend|audit-panel|zero-endpoint-banner|remove-last-endpoint-modal|endpoint-history-sparkline|cache-drift-panel|delete-cleanup-button)\.spec\.ts$/,
```

- [ ] **Step 2: Create `e2e/delete-cleanup-button.spec.ts` with both tests**

Write this exact file:

```ts
import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app runs in "Tauri mode" for invoke().
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

// The mock-api seeds two events on /__reset; we target id=1 ("Sunday Service").
const TARGET_EVENT_NAME = "Sunday Service";

test("delete + cleanup shows busy state and removes event on success", async ({
  page,
  request,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");

  // Delay the DELETE response so the busy state is observable.
  await page.route("**/api/v1/events/1", async (route) => {
    if (route.request().method() === "DELETE") {
      await new Promise((r) => setTimeout(r, 1500));
      await route.continue();
    } else {
      await route.continue();
    }
  });

  await page.goto("/settings");
  await page.locator(".settings-tabs .tab", { hasText: "Events" }).click();

  const card = page.locator(".settings-card", { hasText: TARGET_EVENT_NAME });
  const deleteBtn = card.locator("button.btn-danger");
  const clearBtn = card.locator("button.btn-secondary");

  await expect(deleteBtn).toBeEnabled();
  await deleteBtn.click();

  // Confirm in modal
  await page.locator(".confirm-modal .confirm-btn-danger").click();

  // Modal closes immediately
  await expect(page.locator(".confirm-modal")).toHaveCount(0);

  // Busy state: label flips to "Deleting…", both card buttons disabled
  await expect(deleteBtn).toHaveText(/Deleting/, { timeout: 1000 });
  await expect(deleteBtn).toBeDisabled();
  await expect(clearBtn).toBeDisabled();

  // After the delay + DELETE + list refresh, the card is gone
  await expect(card).toHaveCount(0, { timeout: 5000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("delete + cleanup shows error banner on API failure", async ({
  page,
  request,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");

  // Force a 500 response on DELETE
  await page.route("**/api/v1/events/1", async (route) => {
    if (route.request().method() === "DELETE") {
      await new Promise((r) => setTimeout(r, 200));
      await route.fulfill({ status: 500, body: "internal server error" });
    } else {
      await route.continue();
    }
  });

  await page.goto("/settings");
  await page.locator(".settings-tabs .tab", { hasText: "Events" }).click();

  const card = page.locator(".settings-card", { hasText: TARGET_EVENT_NAME });
  const deleteBtn = card.locator("button.btn-danger");

  await deleteBtn.click();
  await page.locator(".confirm-modal .confirm-btn-danger").click();

  // Error banner appears with "Delete failed"
  await expect(
    page.locator(".error-message", { hasText: /Delete failed/i }),
  ).toBeVisible({ timeout: 3000 });

  // Event card still present
  await expect(card).toBeVisible();

  // Button re-enabled and label restored
  await expect(deleteBtn).toBeEnabled();
  await expect(deleteBtn).toHaveText(/Delete \+ Cleanup/);

  // Filter out the expected 500-response noise from console.
  // The application MUST handle the error gracefully — the only allowed
  // console output is fetch's own network logging for the 500.
  const real = consoleMessages.filter(
    (m) =>
      !ALLOWED_CONSOLE.some((r) => r.test(m)) &&
      !/500/.test(m) &&
      !/Failed to load/i.test(m),
  );
  expect(real).toEqual([]);
});
```

- [ ] **Step 3: Confirm test discovery (no execution)**

The implementation runs on CI; locally we only verify the file is created and the regex is updated. No `npx playwright test` locally — CI runs the suite.

```bash
ls -la e2e/delete-cleanup-button.spec.ts
grep delete-cleanup-button e2e/playwright-frontend.config.ts
```

Expected: file exists; regex contains `delete-cleanup-button`.

- [ ] **Step 4: Commit (TDD red — test fails because busy state and error banner don't exist yet)**

```bash
git add e2e/delete-cleanup-button.spec.ts e2e/playwright-frontend.config.ts
git commit -m "test: add failing Playwright spec for delete+cleanup button feedback (#128)"
```

---

### Task 3: Implement fix in settings.rs (TDD green)

**Files:**
- Modify: `leptos-ui/src/components/settings.rs:411-700` (`EventsManagement` component)

- [ ] **Step 1: Read the current `EventsManagement` declaration block (lines 411-487) to anchor the edit**

```bash
sed -n '411,490p' leptos-ui/src/components/settings.rs
```

Confirm the existing signal declarations end around line 430 (`set_template_error`).

- [ ] **Step 2: Add the two new signals after the existing `s3_usage_error` signal**

In `leptos-ui/src/components/settings.rs`, locate this block (lines 425-426):

```rust
    let s3_usage = RwSignal::<Option<api::S3UsageResponse>>::new(None);
    let s3_usage_error = RwSignal::<Option<String>>::new(None);
```

Add these two lines immediately after:

```rust
    // Busy and error state for destructive actions on event cards.
    // `busy_event_id` is `Some(id)` while a delete or clear-S3 call is
    // in flight for that event; the buttons on that card render disabled
    // with a "Deleting…"/"Clearing…" label so the operator sees progress.
    // `action_error` holds the most recent failure message from either
    // action so it can render in a banner above the list.
    let busy_event_id = RwSignal::<Option<i64>>::new(None);
    let action_error = RwSignal::<Option<String>>::new(None);
```

- [ ] **Step 3: Replace `on_confirm_delete` (lines 451-462) with error-aware version**

Find this block:

```rust
    let on_confirm_delete = Callback::new(move |_: ()| {
        let id = delete_target_id.get();
        spawn_local(async move {
            let _ = api::delete_event(id).await;
            if let Ok(events) = api::list_events().await {
                store.events_list.set(events);
            }
            if let Ok(u) = api::get_s3_usage().await {
                s3_usage.set(Some(u));
            }
        });
    });
```

Replace with:

```rust
    let on_confirm_delete = Callback::new(move |_: ()| {
        let id = delete_target_id.get();
        busy_event_id.set(Some(id));
        action_error.set(None);
        spawn_local(async move {
            match api::delete_event(id).await {
                Ok(_) => {
                    if let Ok(events) = api::list_events().await {
                        store.events_list.set(events);
                    }
                    if let Ok(u) = api::get_s3_usage().await {
                        s3_usage.set(Some(u));
                    }
                }
                Err(e) => action_error.set(Some(format!("Delete failed: {e}"))),
            }
            busy_event_id.set(None);
        });
    });
```

- [ ] **Step 4: Replace `on_confirm_clear` (lines 464-472) with error-aware version**

Find this block:

```rust
    let on_confirm_clear = Callback::new(move |_: ()| {
        let id = clear_target_id.get();
        spawn_local(async move {
            let _ = api::clear_event_s3_chunks(id).await;
            if let Ok(u) = api::get_s3_usage().await {
                s3_usage.set(Some(u));
            }
        });
    });
```

Replace with:

```rust
    let on_confirm_clear = Callback::new(move |_: ()| {
        let id = clear_target_id.get();
        busy_event_id.set(Some(id));
        action_error.set(None);
        spawn_local(async move {
            match api::clear_event_s3_chunks(id).await {
                Ok(_) => {
                    if let Ok(u) = api::get_s3_usage().await {
                        s3_usage.set(Some(u));
                    }
                }
                Err(e) => action_error.set(Some(format!("Clear failed: {e}"))),
            }
            busy_event_id.set(None);
        });
    });
```

- [ ] **Step 5: Render the error banner above the events list**

In `EventsManagement`'s `view!` block, locate the `<div class="events-actions-bar">` (around line 493) — the "+ New from Template" button container. Immediately AFTER that closing `</div>` tag and BEFORE the S3 usage banner (the `{move || { if let Some(usage) = s3_usage.get() ...` block around line 510), insert:

```rust
            // Action error banner — surfaces the most recent failure from
            // delete/clear actions. Dismissable with the "×" button.
            {move || action_error.get().map(|err| view! {
                <div class="error-message">
                    {err}
                    <button
                        class="modal-cancel-btn"
                        style="margin-left: var(--spacing-md);"
                        on:click=move |_| action_error.set(None)
                    >
                        "×"
                    </button>
                </div>
            })}
```

- [ ] **Step 6: Make the per-card buttons reactive on busy state**

Locate the card-actions block (lines 593-621). The current `Clear S3 chunks` button is:

```rust
                                    <button
                                        class="btn-secondary"
                                        disabled=is_streaming
                                        on:click=move |_| {
                                            clear_target_id.set(id);
                                            clear_target_name.set(name_for_clear.clone());
                                            show_clear_modal.set(true);
                                        }
                                        title="Delete S3 chunks for this event but keep the event row"
                                    >
                                        "Clear S3 chunks"
                                    </button>
```

Replace with:

```rust
                                    <button
                                        class="btn-secondary"
                                        disabled=move || is_streaming || busy_event_id.get() == Some(id)
                                        on:click=move |_| {
                                            clear_target_id.set(id);
                                            clear_target_name.set(name_for_clear.clone());
                                            show_clear_modal.set(true);
                                        }
                                        title="Delete S3 chunks for this event but keep the event row"
                                    >
                                        {move || {
                                            if busy_event_id.get() == Some(id) {
                                                "Clearing…"
                                            } else {
                                                "Clear S3 chunks"
                                            }
                                        }}
                                    </button>
```

The current `Delete + Cleanup` button is:

```rust
                                    <button
                                        class="btn-danger"
                                        disabled=is_streaming
                                        on:click=move |_| {
                                            delete_target_id.set(id);
                                            delete_target_name.set(name_for_modal.clone());
                                            show_delete_modal.set(true);
                                        }
                                    >
                                        {if is_streaming {
                                            "Delete (stop stream first)"
                                        } else {
                                            "Delete + Cleanup"
                                        }}
                                    </button>
```

Replace with:

```rust
                                    <button
                                        class="btn-danger"
                                        disabled=move || is_streaming || busy_event_id.get() == Some(id)
                                        on:click=move |_| {
                                            delete_target_id.set(id);
                                            delete_target_name.set(name_for_modal.clone());
                                            show_delete_modal.set(true);
                                        }
                                    >
                                        {move || {
                                            if is_streaming {
                                                "Delete (stop stream first)"
                                            } else if busy_event_id.get() == Some(id) {
                                                "Deleting…"
                                            } else {
                                                "Delete + Cleanup"
                                            }
                                        }}
                                    </button>
```

- [ ] **Step 7: Verify formatting**

```bash
cargo fmt --all --check
```

Expected: no output. If output appears, run `cargo fmt --all` and re-check.

- [ ] **Step 8: Commit (TDD green — implementation makes the failing test pass on CI)**

```bash
git add leptos-ui/src/components/settings.rs
git commit -m "fix(settings): show busy state + surface errors for delete/clear actions (#128)"
```

---

### Task 4: Push and monitor CI to terminal state

This task is run by the orchestrator (main agent), not a subagent.

- [ ] **Step 1: Final formatting check**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Review what will be pushed**

```bash
git log --oneline origin/dev..HEAD
git diff --stat origin/dev..HEAD
```

Expected: 3 commits (`chore: bump version`, `test: add failing Playwright spec`, `fix(settings): show busy state`). Diff touches: 4 version files + Playwright config + new e2e spec + settings.rs.

- [ ] **Step 3: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 4: Identify the CI run triggered by your push**

```bash
sleep 10 && gh run list --branch dev --limit 3 --json databaseId,workflowName,status,conclusion,createdAt
```

Note the `databaseId` of the latest "Rust CI" run. Save as `RUN_ID`.

- [ ] **Step 5: Monitor that run to terminal state**

Use the airuleset-approved single-command pattern:

```bash
sleep 600 && gh run view <RUN_ID> --json status,conclusion,jobs
```

Run with `run_in_background: true`. Wait for the result; the orchestrator gets a notification when the background command finishes.

- [ ] **Step 6: Handle the result**

- If `conclusion == "success"`: proceed to Task 5.
- If failed: `gh run view <RUN_ID> --log-failed`, fix the root cause in ONE commit, push once, monitor again. Never blindly rerun.

---

### Task 5: Create PR dev → main

This task is run by the orchestrator.

- [ ] **Step 1: Verify dev is ahead of main**

```bash
git fetch origin
git log --oneline origin/main..origin/dev
```

Expected: 3 commits listed.

- [ ] **Step 2: Create the PR**

```bash
gh pr create --base main --head dev --title "fix(settings): #128 delete+cleanup button busy state and error surfacing (v0.3.70)" --body "$(cat <<'EOF'
## Summary
- Add `busy_event_id` + `action_error` signals to `EventsManagement`; both confirm callbacks now flip these signals around the async API call instead of fire-and-forget with swallowed errors.
- Per-card `Clear S3 chunks` and `Delete + Cleanup` buttons render disabled with `Clearing…` / `Deleting…` labels while their event's mutation is in flight.
- A dismissable `.error-message` banner above the events list surfaces the failure reason if the API returns non-2xx.
- Backend untouched — issue was purely UI feedback.

Closes #128

## Test plan
- [ ] CI: `e2e/delete-cleanup-button.spec.ts` passes (success + error tests)
- [ ] CI: existing E2E specs still pass (frontend, audit-panel, etc.)
- [ ] CI: `deploy-stream-lan` job succeeds
- [ ] Post-deploy: open dashboard via Playwright MCP, navigate to Settings → Events, confirm `Delete + Cleanup` shows the busy state and error banner behaviors on stream.lan v0.3.70

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Save the PR URL.

---

### Task 6: Monitor PR CI to green and verify deployment

This task is run by the orchestrator.

- [ ] **Step 1: Identify the PR CI run**

```bash
gh pr checks <PR_URL>
```

If checks haven't started yet: `sleep 20 && gh pr checks <PR_URL>`.

- [ ] **Step 2: Wait for ALL jobs (including deploy-stream-lan + e2e-gate) to reach terminal state**

```bash
sleep 900 && gh pr checks <PR_URL>
```

Run in background. When done, check the result.

- [ ] **Step 3: Verify mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<PR_NUMBER> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `mergeable: true`, `mergeable_state: "clean"`.

- [ ] **Step 4: Verify deploy on stream.lan**

After `deploy-stream-lan` is green:

```
mcp__win-stream-snv__ListProcesses with filter "Restreamer"
mcp__win-stream-snv__Shell: Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status
mcp__win-stream-snv__Shell: (Get-Item "C:\Program Files\Restreamer\Restreamer.exe").VersionInfo.FileVersion
```

Expected: process running in user session; API returns valid status; FileVersion shows `0.3.70`.

- [ ] **Step 5: Functional verification via Playwright MCP**

```
mcp__plugin_playwright_playwright__browser_navigate to http://10.77.9.204:8910/settings
mcp__plugin_playwright_playwright__browser_click on "Events" tab
mcp__plugin_playwright_playwright__browser_snapshot
```

Confirm the events tab renders. Pick an idle event (not streaming), click `Delete + Cleanup`, observe modal → confirm → verify the button label flips to `Deleting…` and the card disappears after a moment. (If no idle test event exists, just confirm the new button text/disabled-attribute wiring renders correctly via the snapshot.)

- [ ] **Step 6: Report the URL and wait for explicit merge instruction**

Per `pr-merge-policy`: provide the green PR URL and the dashboard URL (`http://10.77.9.204:8910/`) in a completion report. **Do not merge.** Wait for the user to say "merge it".

---

## Post-merge (only after user says "merge it")

- Merge the PR: `gh pr merge <PR_NUMBER> --merge` (no squash, no rebase)
- Monitor main CI run + Release workflow `restreamer-v0.3.70` to terminal state with the same `sleep N && gh run view` pattern
- Verify GitHub Release published with assets (`Restreamer_0.3.70_x64-setup.exe`, `rs-delivery-0.3.70-linux-amd64`, `.sha256`)
- Verify stream.lan now runs `0.3.70` post-deploy
- Final completion report per `completion-report` rule

---

## Verification checklist (collated for the final completion report)

1. **CI**: all jobs green (lint, test, mutation-testing, test-integrity, e2e-frontend, e2e-youtube, e2e-gate, deploy-stream-lan, security-audit, build, file-size)
2. **Playwright**: both new tests pass; existing E2E specs unaffected
3. **Mergeable**: `mergeable: true, mergeable_state: clean`
4. **Deploy**: stream.lan running `Restreamer.exe` v0.3.70 in user session with API responding
5. **Functional**: dashboard /settings → Events tab → button label flips to `Deleting…` while delete is in flight; error banner appears on API failure; event removed from list on success
6. **No console errors**: zero browser console errors/warnings on the deployed dashboard
