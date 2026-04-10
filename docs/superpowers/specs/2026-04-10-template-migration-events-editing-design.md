# Template Migration + Events Tab Editing — Design Spec

**Issue:** Follow-up to #89 / PR #102. After deploying the template/instance model, three problems remain:

1. **Templates list is empty** — Migration V12 only created empty tables. Existing events were not migrated to templates, so users see "No templates yet" when they expected their existing event configurations to be available as presets.
2. **Events tab is read-only** — The new Events tab in Settings shows event names and status badges only. No endpoint information, no editing.
3. **Config tab still has duplicate Events section** — The old `EventsSection` component (with full editing) remains in the Config tab, leading to two places that manage events.

**Goal:** Auto-migrate existing events into templates, give the Events tab full editing capabilities, and remove the duplicated Events section from the Config tab.

---

## Migration V13 — Convert Existing Events to Templates

### SQL

```sql
-- Step 1: Create templates from existing event configurations.
-- INSERT OR IGNORE skips events whose name already collides with an existing template.
INSERT OR IGNORE INTO event_templates (name, cache_delay_secs)
SELECT name, cache_delay_secs FROM streaming_events;

-- Step 2: Copy endpoint assignments from event_endpoints to template_endpoints.
-- Joined by name (the only stable identifier between the two tables).
INSERT OR IGNORE INTO template_endpoints (template_id, endpoint_id)
SELECT t.id, ee.endpoint_id
FROM event_endpoints ee
JOIN streaming_events se ON ee.event_id = se.id
JOIN event_templates t ON t.name = se.name;

-- Step 3: Delete events that are not currently streaming.
-- Streaming events stay in place to avoid disrupting active live sessions.
-- Cascade delete removes their chunk_records and event_endpoints rows.
DELETE FROM streaming_events
WHERE receiving_activated = 0 AND delivering_activated = 0;
```

### Properties

- **Idempotent.** `INSERT OR IGNORE` handles name collisions if the user already created a template with the same name. Re-running the migration is a no-op.
- **Safe for active streams.** If any event has `receiving_activated = 1` or `delivering_activated = 1` at the moment migration runs, that event row is preserved. The corresponding template still gets created, so the user can create new instances from it. Worst case is harmless duplication (an instance row and a template row sharing a name).
- **No S3 cleanup.** Migration touches the database only. S3 chunks for deleted events become orphaned and are left for the user to clean manually if desired. Adding S3 cleanup to a DB migration would require credentials and network access at startup, which is outside the migration framework's scope.
- **Cascade deletes.** Per the V1/V2 schema, deleting a `streaming_events` row cascades to `chunk_records` and `event_endpoints`, so no dangling rows remain.

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

1. **`migration_v13_converts_events_to_templates`**: Insert sample events with cache delays and endpoint assignments before migration, run all migrations, verify templates created with correct names + cache delays + endpoint assignments, verify non-streaming events deleted, verify streaming events preserved.
2. **`migration_v13_idempotent_with_existing_template`**: Insert an event whose name matches an existing template, verify migration doesn't fail and skips the duplicate template insert.
3. **`migration_v13_skips_streaming_events`**: Insert an event with `receiving_activated = 1`, verify it stays in `streaming_events` after migration.

### E2E (Playwright)

In `e2e/frontend.spec.ts`, update the existing "Events Management Tab" test block:

- **Verify endpoint badges visible on event cards**: After loading the Events tab, each event card shows its assigned endpoint badges.
- **Verify cache delay editor present**: Each event card has an editable cache delay input.
- **Verify Config tab no longer has Events section**: Navigate to Config tab, assert the Events section header is absent.

The mock API needs to ensure events have associated endpoints in `eventEndpoints` so badges render.

---

## Migration Rollout

- Bump version 0.3.28 → 0.3.29 (per project version-bumping policy).
- Migration V13 runs automatically on first startup after deploy. Users see no action required.
- After migration, users will find their previous events as templates in the Templates tab and an empty Events tab. Creating a new stream from a template produces a date-suffixed instance.

---

## Out of Scope

- **S3 cleanup of orphaned chunks** from deleted events. Users can delete leftover S3 objects manually or via a future cleanup endpoint.
- **Bulk template editing** (rename multiple, etc.).
- **Migration rollback** — V12/V13 are forward-only like all other migrations in this codebase.
