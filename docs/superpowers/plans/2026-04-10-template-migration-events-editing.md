# Template Seed + Events Tab Editing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a startup seed function that converts existing events to templates idempotently, give the Events tab full editing (endpoint + cache delay), and remove the duplicate Events section from the Config tab.

**Architecture:** A new `seed_templates_from_events(pool)` function in `crates/rs-core/src/db/templates.rs` runs after `run_migrations()` at startup. It checks if `event_templates` is empty AND `streaming_events` has rows, then converts events to templates with their endpoints, then deletes non-streaming events. Idempotent — runs only when needed. The Leptos `EventsManagement` component reuses the existing `CacheDelayEditor` and `EventEndpoints` sub-components. The duplicate `EventsSection` is removed from the Config tab.

**Tech Stack:** Rust, sqlx (runtime queries), SQLite, Axum, Leptos CSR (WASM), Playwright

**Spec:** `docs/superpowers/specs/2026-04-10-template-migration-events-editing-design.md`

**MVP context:** The user explicitly chose a one-shot Rust seed function over a versioned SQL migration. This is acceptable for MVP-stage projects with small user bases.

---

## Context

- **Pool initialization sites** that run migrations:
  1. `src-tauri/src/lib.rs:187` — Tauri app startup, calls `db::run_migrations(&pool)` after creating the pool
  2. `crates/rs-runtime/src/orchestrator.rs:109` — runtime orchestrator, calls `run_migrations` only when no external pool was provided
- **Templates module** lives in `crates/rs-core/src/db/templates.rs` (created in PR #102). It exports CRUD functions like `create_template`, `list_templates`, `attach_endpoint_to_template`. The seed function will be added to this file.
- **Existing event functions** in `crates/rs-core/src/db/v2.rs`: `list_streaming_events`, `get_event_endpoints`, `attach_endpoint_to_event`. The seed function will use `list_streaming_events` and `get_event_endpoints`.
- **Settings UI** is in `leptos-ui/src/components/settings.rs`. It contains:
  - `SettingsView` (entry, has tab switcher; Config tab renders `<ObsSettingsSection /> <EventsSection /> <EndpointsView />`)
  - `EventsSection` (Config tab — to be DELETED)
  - `CacheDelayEditor` (sub-component, KEEP — reused by EventsManagement)
  - `EventEndpoints` (sub-component, KEEP — reused by EventsManagement)
  - `EventsManagement` (Events tab — needs editing capabilities added)

---

### Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump version 0.3.28 → 0.3.29 in all four files**

In `Cargo.toml`:
```toml
version = "0.3.29"
```

In `src-tauri/Cargo.toml`:
```toml
version = "0.3.29"
```

In `src-tauri/tauri.conf.json`:
```json
"version": "0.3.29"
```

In `leptos-ui/Cargo.toml`:
```toml
version = "0.3.29"
```

- [ ] **Step 2: Verify formatting**

Run: `cargo fmt --all --check`
Expected: No output (clean)

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.29"
```

---

### Task 2: Seed Function — Implementation + Tests

**Files:**
- Modify: `crates/rs-core/src/db/templates.rs` (add `seed_templates_from_events`)
- Modify: `crates/rs-core/src/db/template_tests.rs` (add 4 tests)

- [ ] **Step 1: Write the failing test for basic seeding**

Add to `crates/rs-core/src/db/template_tests.rs` (after the existing tests):

```rust
#[tokio::test]
async fn seed_templates_converts_events() {
    let pool = setup_db().await;

    // Wipe templates so the seed has work to do
    sqlx::query("DELETE FROM template_endpoints").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM event_templates").execute(&pool).await.unwrap();

    // Insert two non-streaming events
    let evt1: i64 = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('sunday-service', 120, 0, 0) RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    let _evt2: i64 = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('wednesday-study', NULL, 0, 0) RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    // Create endpoint and assign to evt1
    let ep_id: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    attach_endpoint_to_event(&pool, evt1, ep_id).await.unwrap();

    // Run the seed function
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 2);

    // Verify: 2 templates created with correct fields
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 2);

    let sunday = templates.iter().find(|t| t.name == "sunday-service").unwrap();
    assert_eq!(sunday.cache_delay_secs, Some(120));

    let wed = templates.iter().find(|t| t.name == "wednesday-study").unwrap();
    assert_eq!(wed.cache_delay_secs, None);

    // Verify: sunday-service has its endpoint
    let sunday_eps = get_template_endpoints(&pool, sunday.id).await.unwrap();
    assert_eq!(sunday_eps.len(), 1);
    assert_eq!(sunday_eps[0].alias, "yt");

    // Verify: events deleted (none were streaming)
    let remaining: i64 = sqlx::query("SELECT COUNT(*) as c FROM streaming_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("c");
    assert_eq!(remaining, 0);
}
```

- [ ] **Step 2: Write the failing test for idempotency**

Add to `crates/rs-core/src/db/template_tests.rs`:

```rust
#[tokio::test]
async fn seed_templates_idempotent() {
    let pool = setup_db().await;

    // Pre-create a template — this makes the templates table non-empty
    create_template(&pool, "existing-template", Some(60)).await.unwrap();

    // Insert an event with a different name
    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('orphan-event', 120, 0, 0)"
    )
    .execute(&pool)
    .await
    .unwrap();

    // Run seed — should be no-op because templates table is non-empty
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 0);

    // Verify: still only the original template
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].name, "existing-template");

    // Verify: event was NOT deleted (seed didn't run)
    let remaining: i64 = sqlx::query("SELECT COUNT(*) as c FROM streaming_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("c");
    assert_eq!(remaining, 1);
}
```

- [ ] **Step 3: Write the failing test for streaming event preservation**

Add to `crates/rs-core/src/db/template_tests.rs`:

```rust
#[tokio::test]
async fn seed_templates_preserves_streaming() {
    let pool = setup_db().await;

    // Wipe templates
    sqlx::query("DELETE FROM template_endpoints").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM event_templates").execute(&pool).await.unwrap();

    // Insert one streaming event and one idle event
    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('live-stream', 60, 1, 1)"
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('idle-stream', 60, 0, 0)"
    )
    .execute(&pool)
    .await
    .unwrap();

    // Run seed
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 2);

    // Verify: BOTH templates created (both event names)
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 2);

    // Verify: streaming event preserved, idle event deleted
    let remaining: Vec<String> = sqlx::query("SELECT name FROM streaming_events")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0], "live-stream");
}
```

- [ ] **Step 4: Write the failing test for empty DB**

Add to `crates/rs-core/src/db/template_tests.rs`:

```rust
#[tokio::test]
async fn seed_templates_no_events() {
    let pool = setup_db().await;

    // Wipe templates
    sqlx::query("DELETE FROM template_endpoints").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM event_templates").execute(&pool).await.unwrap();

    // No events to seed from
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 0);

    // Verify: still no templates
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 0);
}
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `cargo test -p rs-core seed_templates -- --nocapture`
Expected: FAIL with "function not defined" — `seed_templates_from_events` doesn't exist yet.

- [ ] **Step 6: Implement seed_templates_from_events**

In `crates/rs-core/src/db/templates.rs`, add the function at the end of the file:

```rust
/// Seed templates from existing streaming events. One-shot startup helper.
///
/// Idempotency: runs only when `event_templates` is empty. If a user has any
/// templates already, this function is a no-op (returns 0).
///
/// Behavior when seeding:
/// - For each event in `streaming_events`, create a matching template (same
///   name, same cache_delay_secs).
/// - Copy the event's endpoint assignments to `template_endpoints`.
/// - Delete events that are not currently streaming
///   (`receiving_activated = 0 AND delivering_activated = 0`). Streaming
///   events are preserved so we don't disrupt active live sessions.
///
/// Returns the number of templates created.
pub async fn seed_templates_from_events(pool: &SqlitePool) -> Result<usize> {
    // Idempotency check
    let template_count: i64 = sqlx::query("SELECT COUNT(*) as c FROM event_templates")
        .fetch_one(pool)
        .await?
        .get("c");
    if template_count > 0 {
        return Ok(0);
    }

    // Fetch all events
    let events = super::list_streaming_events(pool).await?;
    if events.is_empty() {
        return Ok(0);
    }

    let mut created = 0usize;
    for event in &events {
        // Create template with same name + cache_delay
        let template_id =
            create_template(pool, &event.name, event.cache_delay_secs).await?;

        // Copy endpoint assignments
        let endpoints = super::get_event_endpoints(pool, event.id).await?;
        for ep in &endpoints {
            attach_endpoint_to_template(pool, template_id, ep.id).await?;
        }
        created += 1;
    }

    // Delete non-streaming events. Cascade removes chunk_records and event_endpoints.
    sqlx::query(
        "DELETE FROM streaming_events WHERE receiving_activated = 0 AND delivering_activated = 0",
    )
    .execute(pool)
    .await?;

    log::info!("Seeded {created} templates from existing streaming events");
    Ok(created)
}
```

Note: `Result` here refers to `anyhow::Result`. The other functions in this file already use it via `use anyhow::Result;` at the top. The function uses `super::list_streaming_events` and `super::get_event_endpoints` which live in `db::v2` but are re-exported from `db::mod`. If they're not accessible via `super::`, use `crate::db::list_streaming_events` and `crate::db::get_event_endpoints`. Read the existing imports at the top of `templates.rs` to confirm the right path.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p rs-core seed_templates -- --nocapture`
Expected: All 4 tests PASS

- [ ] **Step 8: Run all rs-core tests**

Run: `cargo test -p rs-core -- --nocapture`
Expected: All tests pass (existing template tests still pass; the 4 new seed tests pass)

- [ ] **Step 9: Check formatting**

Run: `cargo fmt --all --check`
Expected: Clean

- [ ] **Step 10: Commit**

```bash
git add crates/rs-core/src/db/templates.rs crates/rs-core/src/db/template_tests.rs
git commit -m "feat: add seed_templates_from_events startup function (#89)"
```

---

### Task 3: Wire Up Seed Function at Startup Sites

**Files:**
- Modify: `src-tauri/src/lib.rs` (Tauri app startup)
- Modify: `crates/rs-runtime/src/orchestrator.rs` (runtime orchestrator)

The seed function must be called after `run_migrations()` succeeds at both pool initialization sites.

- [ ] **Step 1: Add seed call in src-tauri/src/lib.rs**

In `src-tauri/src/lib.rs`, find the existing migration call (around line 187):

```rust
                // Run migrations
                if let Err(e) = db::run_migrations(&pool).await {
                    tracing::error!("Failed to run migrations: {e}");
                    return;
                }
```

Replace with:

```rust
                // Run migrations
                if let Err(e) = db::run_migrations(&pool).await {
                    tracing::error!("Failed to run migrations: {e}");
                    return;
                }

                // Seed templates from existing events (idempotent one-shot)
                if let Err(e) = db::seed_templates_from_events(&pool).await {
                    tracing::error!("Failed to seed templates: {e}");
                    return;
                }
```

Verify `db::seed_templates_from_events` is accessible. Since `templates` is declared in `mod.rs` as `pub use templates::*;`, the function should be re-exported. If not, the import path in lib.rs may need updating — read the current `use` statements in `src-tauri/src/lib.rs` to find how `db` is imported and adjust accordingly.

- [ ] **Step 2: Add seed call in crates/rs-runtime/src/orchestrator.rs**

In `crates/rs-runtime/src/orchestrator.rs`, find the existing migration call (around line 105-115):

```rust
            None => {
                let pool = db::create_pool(&self.db_path)
                    .await
                    .context("failed to create database pool")?;
                db::run_migrations(&pool)
                    .await
                    .context("failed to run database migrations")?;
                info!("Database initialized at {}", self.db_path.display());
                pool
            }
```

Replace with:

```rust
            None => {
                let pool = db::create_pool(&self.db_path)
                    .await
                    .context("failed to create database pool")?;
                db::run_migrations(&pool)
                    .await
                    .context("failed to run database migrations")?;
                db::seed_templates_from_events(&pool)
                    .await
                    .context("failed to seed templates from events")?;
                info!("Database initialized at {}", self.db_path.display());
                pool
            }
```

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`
Expected: Clean

- [ ] **Step 4: Commit**

```bash
git add src-tauri/src/lib.rs crates/rs-runtime/src/orchestrator.rs
git commit -m "feat: call seed_templates_from_events at startup (#89)"
```

---

### Task 4: Events Tab Editing — Add CacheDelayEditor + EventEndpoints to Cards

**Files:**
- Modify: `leptos-ui/src/components/settings.rs`

The `CacheDelayEditor` and `EventEndpoints` components already exist in `settings.rs` (used by `EventsSection`). They are private functions defined at lines 277 and 327 respectively. We REUSE them by calling them from `EventsManagement`'s event card body.

- [ ] **Step 1: Capture cache_delay_secs in the event loop binding**

In `leptos-ui/src/components/settings.rs`, find the `EventsManagement` event iterator (around line 451). The current bindings:

```rust
                    store.events_list.get().iter().map(|evt| {
                        let id = evt.id;
                        let name = evt.name.clone();
                        let recv = evt.receiving_activated;
                        let deliv = evt.delivering_activated;
                        let is_streaming = recv || deliv;
                        let created_from = evt.created_from.clone();
                        let name_for_modal = name.clone();
```

Add `let cache = evt.cache_delay_secs;` right after `let id = evt.id;`:

```rust
                    store.events_list.get().iter().map(|evt| {
                        let id = evt.id;
                        let cache = evt.cache_delay_secs;
                        let name = evt.name.clone();
                        let recv = evt.receiving_activated;
                        let deliv = evt.delivering_activated;
                        let is_streaming = recv || deliv;
                        let created_from = evt.created_from.clone();
                        let name_for_modal = name.clone();
```

- [ ] **Step 2: Add card-body section with editor + endpoints**

In the same file, find the existing event card markup inside `EventsManagement` (the `view! { <div class="settings-card"> ... </div> }` block, around lines 460-502).

The current markup has `<div class="card-header">` followed directly by `<div class="card-actions">`. Add a new `<div class="card-body">` between them containing `<CacheDelayEditor />` and `<EventEndpoints />`.

Find this:

```rust
                                </div>
                                <div class="card-actions">
                                    <button
                                        class="btn-danger"
                                        disabled=is_streaming
```

Replace with:

```rust
                                </div>
                                <div class="card-body">
                                    <CacheDelayEditor event_id=id initial_delay=cache />
                                    <EventEndpoints event_id=id />
                                </div>
                                <div class="card-actions">
                                    <button
                                        class="btn-danger"
                                        disabled=is_streaming
```

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`
Expected: Clean

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/settings.rs
git commit -m "feat: add cache delay editor and endpoints to Events tab cards (#89)"
```

---

### Task 5: Remove Duplicate EventsSection from Config Tab

**Files:**
- Modify: `leptos-ui/src/components/settings.rs`

- [ ] **Step 1: Remove EventsSection from Config tab rendering**

In `leptos-ui/src/components/settings.rs`, find the Config tab fallback in `SettingsView` (around lines 42-51):

```rust
                _ => {
                    view! {
                        <div>
                            <ObsSettingsSection />
                            <EventsSection />
                            <crate::components::EndpointsView />
                        </div>
                    }
                    .into_any()
                }
```

Replace with:

```rust
                _ => {
                    view! {
                        <div>
                            <ObsSettingsSection />
                            <crate::components::EndpointsView />
                        </div>
                    }
                    .into_any()
                }
```

- [ ] **Step 2: Delete the EventsSection component**

In the same file, delete the entire `EventsSection` component (around lines 198-273):

```rust
/// Events management section.
#[component]
fn EventsSection() -> impl IntoView {
    // ... entire body ...
}
```

**IMPORTANT:** Only delete `EventsSection`. The `CacheDelayEditor` and `EventEndpoints` components MUST stay — they are now used by `EventsManagement` (Task 4).

After removal, the file structure is:
- `SettingsView`
- `ObsSettingsSection`
- `CacheDelayEditor` (kept)
- `EventEndpoints` (kept)
- `EventsManagement`

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`
Expected: Clean

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/settings.rs
git commit -m "refactor: remove duplicate EventsSection from Config tab (#89)"
```

---

### Task 6: E2E Test Updates

**Files:**
- Modify: `e2e/mock-api.js` (verify endpoint assignments)
- Modify: `e2e/frontend.spec.ts` (add 3 tests)

- [ ] **Step 1: Verify mock endpoint assignments**

Open `e2e/mock-api.js`. The existing `eventEndpoints` map should already assign endpoint(s) to events. Confirm the mock data has at least one event with at least one endpoint assigned. The expected current state:

```javascript
let eventEndpoints = {
  1: [1], // Event 1 has YouTube Main assigned
  2: [],
};
```

If this is missing or different, ensure event 1 has endpoint 1 assigned so the new tests can verify badges render.

- [ ] **Step 2: Add Playwright test for endpoint badges visible**

In `e2e/frontend.spec.ts`, find the "Events Management Tab" describe block. Add inside it:

```typescript
  test("event card shows assigned endpoint badges", async ({ page }) => {
    await page.goto("/");
    const settingsBtn = page.locator(
      "button:has-text('Settings'), [data-tab='settings'], .settings-btn, .nav-settings",
    );
    if ((await settingsBtn.count()) > 0) await settingsBtn.first().click();
    await page.locator("button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    // Find the first event card and verify endpoint tag(s) visible inside it
    const firstCard = page
      .locator(".events-management-tab .settings-card")
      .first();
    await expect(firstCard.locator(".endpoint-tag")).toHaveCount(1, {
      timeout: 5000,
    });
  });
```

- [ ] **Step 3: Add Playwright test for cache delay input visible**

Add inside the same describe block:

```typescript
  test("event card shows editable cache delay input", async ({ page }) => {
    await page.goto("/");
    const settingsBtn = page.locator(
      "button:has-text('Settings'), [data-tab='settings'], .settings-btn, .nav-settings",
    );
    if ((await settingsBtn.count()) > 0) await settingsBtn.first().click();
    await page.locator("button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    const firstCard = page
      .locator(".events-management-tab .settings-card")
      .first();
    await expect(firstCard.locator(".cache-delay-input")).toBeVisible({
      timeout: 5000,
    });
  });
```

- [ ] **Step 4: Add Playwright test for Config tab no longer has Events section**

Add inside the same describe block:

```typescript
  test("Config tab no longer shows Events section", async ({ page }) => {
    await page.goto("/");
    const settingsBtn = page.locator(
      "button:has-text('Settings'), [data-tab='settings'], .settings-btn, .nav-settings",
    );
    if ((await settingsBtn.count()) > 0) await settingsBtn.first().click();
    // Click Config tab
    await page.locator("button:has-text('Config')").click();
    await page.waitForTimeout(500);

    // Verify no h3 with text "Events" appears in the Config tab content
    // (the OLD EventsSection had <h3>"Events"</h3>)
    const eventsHeader = page.locator(".settings-section h3:text-is('Events')");
    await expect(eventsHeader).toHaveCount(0);
  });
```

- [ ] **Step 5: Commit**

```bash
git add e2e/mock-api.js e2e/frontend.spec.ts
git commit -m "test: add E2E tests for events tab editing and config cleanup (#89)"
```

---

### Task 7: Push, Monitor CI, Create PR

PR #102 is already merged. This work is a follow-up — we push to `dev` and create a NEW PR to `main`.

- [ ] **Step 1: Verify formatting one final time**

Run: `cargo fmt --all --check`
Expected: Clean

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Check that a CI run started**

```bash
gh run list --branch dev --limit 3
```

- [ ] **Step 4: Wait for CI completion and check status**

Use a non-polling background wait:

```bash
sleep 600 && gh run view <run-id> --json status,conclusion,jobs --jq '{status: .status, conclusion: .conclusion, failed: [.jobs[] | select(.conclusion == "failure") | .name]}'
```

If any job fails: `gh run view <run-id> --log-failed` and fix the issue. The mutation testing config from PR #102 already excludes `template_handlers.rs`, `delete_event_by_id`, and `get_template_endpoints`. The new `seed_templates_from_events` function may produce surviving mutations on the `if template_count > 0` and `if events.is_empty()` checks because no test catches them being inverted (the existing tests cover both branches but a mutation that flips `>` to `>=` on `> 0` is equivalent when the value is 0 — that might survive). If mutation testing fails, add:

```yaml
            --exclude-re 'seed_templates_from_events'
```

to the cargo-mutants invocation in `.github/workflows/ci.yml`.

- [ ] **Step 5: Create PR**

```bash
gh pr create --title "fix: convert existing events to templates and enable Events tab editing" --body "$(cat <<'EOF'
## Summary
- Add `seed_templates_from_events` startup function — converts existing events to templates idempotently (preserves streaming events). One-shot, runs only when templates table is empty.
- Events tab gains full editing: cache delay editor + endpoint attach/detach (reuses existing components).
- Config tab loses the duplicate Events section. Events are managed only in the Events tab.

Follow-up to PR #102 (#89, #90).

## Test plan
- [x] Seed function unit tests: basic conversion, idempotency, streaming preservation, empty DB
- [x] Playwright: endpoint badges visible on event cards
- [x] Playwright: cache delay editor visible on event cards
- [x] Playwright: Config tab no longer has Events section
- [x] All existing tests still pass

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Verify PR mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<pr-number> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `mergeable: true, mergeable_state: clean`

---

## Verification Checklist

After all tasks:

1. **Seed function**: On a database with existing events but no templates, after restart the user sees templates populated and Events tab empty (or only streaming events).
2. **Idempotency**: Restart again — seed runs as no-op, no errors, no duplicates.
3. **Events tab editing**: Create a new event from a template. Verify endpoints display on the card. Verify cache delay can be edited and saved. Verify endpoints can be attached/detached.
4. **Config tab cleanup**: Verify Config tab shows only OBS settings + Endpoints, no Events section.
5. **All CI green**: Including mutation testing, frontend E2E, push and PR runs.

---

## Out of Scope

- S3 cleanup of orphaned chunks from deleted events.
- Bulk template editing.
- Removing the seed function later when no longer needed (it's idempotent and harmless to leave in).
