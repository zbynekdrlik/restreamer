# Template Seed + Events Tab Editing — Design Spec

**Issue:** Follow-up to #89 / PR #102. After deploying the template/instance model, three problems remain:

1. **Templates list is empty** — Migration V12 only created empty tables. Existing events were not converted to templates, so users see "No templates yet" when they expected their existing event configurations to be available as presets. The user explicitly stated: "i only not want lost templates and endpoints definitions to be correctly filled in db after you change."
2. **Events tab is read-only** — The new Events tab in Settings shows event names and status badges only. No endpoint information, no editing.
3. **Config tab still has duplicate Events section** — The old `EventsSection` component (with full editing) remains in the Config tab, leading to two places that manage events.

**Goal:** Convert existing events into templates without losing data, give the Events tab full editing capabilities, and remove the duplicated Events section from the Config tab.

**MVP context:** Restreamer is still MVP. The user explicitly chose to skip the formal versioned SQL migration approach in favor of a simpler one-shot startup seed function in Rust. This is acceptable because the project has a small user base and the seed function is idempotent.

---

## Startup Seed Function — Convert Existing Events to Templates

### Approach

Add a Rust function `seed_templates_from_events(pool)` in `crates/rs-core/src/db/templates.rs` (or a new file). The function runs **after** `run_migrations()` completes, called from wherever the DB pool is initialized in the runtime/service crates.

### Logic

```rust
pub async fn seed_templates_from_events(pool: &SqlitePool) -> Result<usize> {
    // Idempotency check: only run if templates is empty AND there are events to convert
    let template_count: i64 = sqlx::query("SELECT COUNT(*) as c FROM event_templates")
        .fetch_one(pool).await?
        .get("c");
    if template_count > 0 {
        return Ok(0); // Already seeded — no-op
    }

    let events = list_streaming_events(pool).await?;
    if events.is_empty() {
        return Ok(0); // Nothing to seed
    }

    let mut created = 0;
    for event in &events {
        // Create template with same name + cache_delay
        let template_id = create_template(pool, &event.name, event.cache_delay_secs).await?;

        // Copy endpoint assignments
        let endpoints = get_event_endpoints(pool, event.id).await?;
        for ep in &endpoints {
            attach_endpoint_to_template(pool, template_id, ep.id).await?;
        }
        created += 1;
    }

    // Delete non-streaming events (preserve active streams)
    sqlx::query(
        "DELETE FROM streaming_events WHERE receiving_activated = 0 AND delivering_activated = 0"
    )
    .execute(pool)
    .await?;

    log::info!("Seeded {created} templates from existing events");
    Ok(created)
}
```

### Properties

- **Idempotent.** Runs only when `event_templates` is empty. Subsequent startups skip it.
- **Lossless.** Every existing event becomes a template with its endpoints and cache delay copied over.
- **Safe for active streams.** If any event has `receiving_activated = 1` or `delivering_activated = 1` at the moment seed runs, that event row is preserved. The corresponding template still gets created, so the user can create new instances from it.
- **No S3 cleanup.** Seed touches the database only. S3 chunks for deleted events become orphaned and are left for the user to clean manually if desired.
- **Cascade deletes.** Per the V1/V2 schema, deleting a `streaming_events` row cascades to `chunk_records` and `event_endpoints`, so no dangling rows remain.
- **Not a migration.** This is a startup data fix, not part of the versioned migration framework. No `MIGRATION_V13_SQL`. The function is called after migrations complete.

### Where the seed runs

The seed function is invoked once at startup, after `run_migrations()` succeeds. Likely call sites:
- `crates/rs-runtime/src/lib.rs` or wherever the pool is created
- `crates/rs-service/src/main.rs` (Tauri service entry)
- The exact location is determined by reading the existing pool initialization code during implementation.

### Example

User has these events configured before the upgrade:

| id | name              | cache_delay_secs | receiving_activated | delivering_activated |
|----|-------------------|------------------|---------------------|----------------------|
| 1  | sunday-service    | 120              | 0                   | 0                    |
| 2  | wednesday-study   | NULL             | 0                   | 0                    |
| 3  | SNV-stream        | NULL             | 0                   | 0                    |

With endpoint assignments:

| event_id | endpoint_id |
|----------|-------------|
| 1        | 5 (YT_HLS)  |
| 1        | 7 (FB)      |
| 3        | 5 (YT_HLS)  |

After migration V13:

`event_templates`:
| id | name              | cache_delay_secs |
|----|-------------------|------------------|
| 1  | sunday-service    | 120              |
| 2  | wednesday-study   | NULL             |
| 3  | SNV-stream        | NULL             |

`template_endpoints`:
| template_id | endpoint_id |
|-------------|-------------|
| 1           | 5           |
| 1           | 7           |
| 3           | 5           |

`streaming_events`: empty.

User now sees three templates in the Templates tab and zero events in the Events tab. Creating a new stream from `sunday-service` template generates `sunday-service-2026-04-10`.

---

## Events Tab — Full Editing

The Events tab currently shows only name + status badges + delete button. Add the same editing capabilities the old `EventsSection` (in Config tab) provides:

1. **Endpoint display + management**: For each event, show assigned endpoints as removable badges plus a dropdown to attach unassigned endpoints. Reuse the existing `EventEndpoints` component logic.
2. **Cache delay editor**: For each event, show an editable input for `cache_delay_secs` with a "Save" button. Reuse the existing `CacheDelayEditor` component logic.
3. **Disable editing while streaming**: When `receiving_activated || delivering_activated`, the cache delay input and endpoint controls are disabled. Tooltip: "Stop stream first to edit." (The Delete button is already disabled in this case.)

### Card layout

```
┌─────────────────────────────────────────────────────┐
│ sunday-service-2026-04-10  [Receiving] [Delivering] │
│                            [from: sunday-service]   │
│ ─────────────────────────────────────────────────── │
│ Cache delay: [120______] s [Save]                   │
│                                                     │
│ Endpoints: [YT_HLS ×] [FB ×] [+ Assign endpoint ▼]  │
│                                                     │
│                  [ Delete + Cleanup ]               │
└─────────────────────────────────────────────────────┘
```

When streaming, cache input and `× / + Assign` controls are disabled (greyed out). Delete button shows "Delete (stop stream first)".

---

## Config Tab — Remove Duplicate Events Section

Delete the `EventsSection` component invocation from `SettingsView`'s Config tab. The component code itself (`EventsSection`, `CacheDelayEditor`, `EventEndpoints`) can be reused inside `EventsManagement` — either by extracting them as shared private functions or by copying their logic. Prefer reuse: keep the components, change only where they're rendered.

After this change, the Config tab contains:
- OBS WebSocket settings
- Endpoints list (existing `EndpointsView`)

It no longer has a per-event editor. Event management lives only in the Events tab.

---

## Components Reuse Strategy

The existing `EventsSection`, `CacheDelayEditor`, and `EventEndpoints` components in `settings.rs` are private to that file. They're only used by `EventsSection` (Config tab) today. After this change:

- `CacheDelayEditor` → reused by the new Events tab card (per-event row)
- `EventEndpoints` → reused by the new Events tab card (per-event row)
- `EventsSection` → deleted (was the wrapper for the old Config-tab list)

The new `EventsManagement` component in the Events tab gains the same per-card body (`<CacheDelayEditor /> <EventEndpoints />`) that the old `EventsSection` had, plus its existing template picker and delete-with-cleanup features.

---

## Test Coverage

### Unit tests (rs-core)

In `crates/rs-core/src/db/template_tests.rs`, add:

1. **`seed_templates_converts_events`**: Insert sample events with cache delays and endpoint assignments, call `seed_templates_from_events()`, verify templates created with correct names + cache delays + endpoint assignments, verify non-streaming events deleted, verify streaming events preserved. Verify return value is the count of templates created.
2. **`seed_templates_idempotent`**: Pre-create a template, insert events, call seed function, verify it returns 0 (no-op because templates table is non-empty) and doesn't touch existing data.
3. **`seed_templates_preserves_streaming`**: Insert an event with `receiving_activated = 1`, call seed function, verify the streaming event stays in `streaming_events` (corresponding template is still created).
4. **`seed_templates_no_events`**: Call seed function on an empty DB, verify it returns 0 and doesn't error.

### E2E (Playwright)

In `e2e/frontend.spec.ts`, update the existing "Events Management Tab" test block:

- **Verify endpoint badges visible on event cards**: After loading the Events tab, each event card shows its assigned endpoint badges.
- **Verify cache delay editor present**: Each event card has an editable cache delay input.
- **Verify Config tab no longer has Events section**: Navigate to Config tab, assert the Events section header is absent.

The mock API needs to ensure events have associated endpoints in `eventEndpoints` so badges render.

---

## Rollout

- Bump version 0.3.28 → 0.3.29 (per project version-bumping policy).
- The seed function runs automatically on first startup after deploy. Users see no action required.
- After seeding, users will find their previous events as templates in the Templates tab and an empty Events tab. Creating a new stream from a template produces a date-suffixed instance.

---

## Out of Scope

- **S3 cleanup of orphaned chunks** from deleted events. Users can delete leftover S3 objects manually or via a future cleanup endpoint.
- **Bulk template editing** (rename multiple, etc.).
- **Migration rollback** — V12/V13 are forward-only like all other migrations in this codebase.
