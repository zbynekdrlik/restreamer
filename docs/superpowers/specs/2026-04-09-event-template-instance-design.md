# Event Template/Instance Model — Design Spec

**Issue:** #89 (Event template/instance model), #90 (S3 chunk cleanup)

**Goal:** Separate reusable event configuration (templates) from streaming sessions (events). Enable S3 chunk cleanup when events are deleted.

**Architecture:** Templates are presets stored in the database, managed via dashboard UI. Events are fully independent after creation — no FK to templates. Creating a new stream from a template copies its config into a fresh event with a date-based name. Deleting an event deletes its S3 chunks.

---

## Core Concepts

### Template (Preset)

A saved configuration that makes it easy to start recurring streams. Contains:
- **Name**: identifier used as prefix for event names (e.g., `sunday-service`)
- **Endpoints**: which delivery endpoints to assign (M2M with `endpoint_configs`)
- **Cache delay**: optional per-template delivery delay override (seconds)

Templates live in the database and are managed through the dashboard UI (Templates tab). They have no lifecycle relationship to events — a template can be deleted or modified without affecting any event.

### Event (Streaming Session)

An independent streaming session, typically created from a template. Contains:
- **Name**: unique, date-based identifier (e.g., `sunday-service-2026-04-09`). Used as S3 key prefix.
- **Endpoints**: own M2M with `endpoint_configs` (copied from template at creation, independently modifiable)
- **Cache delay**: own value (copied from template at creation, independently modifiable)
- **created_from**: informational text recording which template was used (no constraint, survives template deletion)
- **Status fields**: `receiving_activated`, `delivering_activated`, `received_bytes`

Events own their chunk records and S3 objects. Deleting an event deletes all associated S3 objects.

### Relationship

```
Template ----creates----> Event
           (copy config)
           (no FK, no constraint)
```

After creation, the event is fully independent. Template changes do not propagate. Template deletion does not affect existing events.

---

## Database Schema

### New Table: `event_templates`

```sql
CREATE TABLE IF NOT EXISTS event_templates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    cache_delay_secs INTEGER
);
```

### New Table: `template_endpoints`

```sql
CREATE TABLE IF NOT EXISTS template_endpoints (
    template_id INTEGER NOT NULL REFERENCES event_templates(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
    PRIMARY KEY (template_id, endpoint_id)
);
```

### Modified: `streaming_events`

```sql
ALTER TABLE streaming_events ADD COLUMN created_from TEXT;
```

`created_from` stores the template name as plain text at creation time. Informational only — no foreign key, no constraint.

### Unchanged

- `event_endpoints` — stays as-is, events own their endpoint assignments
- `chunk_records` — stays as-is, chunks belong to events
- `endpoint_configs` — unchanged

---

## API

### Template Endpoints (New)

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/api/v1/templates` | List all templates |
| `POST` | `/api/v1/templates` | Create template `{name, cache_delay_secs?}` |
| `GET` | `/api/v1/templates/{id}` | Get single template |
| `PATCH` | `/api/v1/templates/{id}` | Update template (name, cache_delay_secs) |
| `DELETE` | `/api/v1/templates/{id}` | Delete template |
| `GET` | `/api/v1/templates/{id}/endpoints` | List template's endpoints |
| `POST` | `/api/v1/templates/{id}/endpoints/{eid}` | Attach endpoint to template |
| `DELETE` | `/api/v1/templates/{id}/endpoints/{eid}` | Detach endpoint from template |

### Event Endpoints (Modified)

| Method | Route | Purpose | Change |
|--------|-------|---------|--------|
| `GET` | `/api/v1/events` | List all events | No change |
| `POST` | `/api/v1/events` | Create event | Body: `{template_id}` (copies config, generates date name) OR `{name}` (standalone, for API/CI). |
| `GET` | `/api/v1/events/{id}` | Get single event | Returns `created_from` field |
| `PATCH` | `/api/v1/events/{id}` | Update event | Can change name, cache_delay_secs (independent of template) |
| `DELETE` | `/api/v1/events/{id}` | Delete event + S3 cleanup | **New behavior**: deletes S3 objects under event name prefix |
| `POST` | `/api/v1/events/{id}/start-stream` | Start streaming | No change (uses event's own config) |
| `POST` | `/api/v1/events/{id}/stop-stream` | Stop streaming | No change |
| `GET` | `/api/v1/events/{id}/endpoints` | List event's endpoints | No change |
| `POST` | `/api/v1/events/{event_id}/endpoints/{endpoint_id}` | Attach endpoint | No change |
| `DELETE` | `/api/v1/events/{event_id}/endpoints/{endpoint_id}` | Detach endpoint | No change |

### Create Event from Template Flow

`POST /api/v1/events` with `{template_id: 5}`:

1. Fetch template by ID (404 if not found)
2. Generate name: `{template.name}-{YYYY-MM-DD}` (e.g., `sunday-service-2026-04-09`)
3. If name exists: append `-2`, `-3`, etc. until unique
4. Create event: `name`, `cache_delay_secs` from template, `created_from` = template name
5. Copy template's endpoints into `event_endpoints`
6. Return `{id, name}` (201 Created)

### Delete Event with S3 Cleanup Flow

`DELETE /api/v1/events/{id}`:

1. Fetch event by ID (404 if not found)
2. If event is streaming (`receiving_activated` or `delivering_activated`): return 409 Conflict
3. List S3 objects under `{event.name}/` prefix
4. Delete all S3 objects (batch)
5. Delete DB records (cascade: chunk_records, event_endpoints)
6. Return 204 No Content

If S3 deletion fails: return 500 with error detail. Do not delete DB records if S3 cleanup fails.

---

## UI Design

### Two-Tab Layout

The events section of the dashboard splits into two tabs: **Templates** and **Events**.

#### Templates Tab

- List of all templates with: name, assigned endpoints (badges), cache delay
- "New Template" button: opens inline form (name input, endpoint selector, cache delay input)
- Edit/delete actions per template
- Delete confirmation dialog

#### Events Tab

- List of all events sorted by creation date (newest first)
- Each event shows:
  - Name (with date, e.g., `sunday-service-2026-04-09`)
  - Status badge: `STREAMING` (green), `IDLE` (gray)
  - `created_from` text (if set, shown as small label)
  - Chunk count and size (from chunk_records aggregate)
- Actions per event:
  - `Start Stream` / `Stop Stream` (for active events)
  - `Delete + Cleanup` (red, with confirmation dialog — only when not streaming)
  - Endpoint management (attach/detach, same as current)
- "New Stream" button: opens modal with template picker dropdown, shows preview of endpoints and cache delay, creates event and optionally starts streaming immediately

---

## S3 Cleanup Implementation

### S3Client Changes

Add to `crates/rs-endpoint/src/s3.rs`:

```rust
pub async fn delete_event_chunks(&self, event_name: &str) -> Result<u64, S3Error> {
    // 1. List all objects with prefix "{event_name}/"
    // 2. Delete each object
    // 3. Return count of deleted objects
}
```

Uses the `rust-s3` crate's `list` and `delete_object` methods. The list operation paginates (1000 objects per page) until all objects are found.

### Safety

- Only callable when event is not streaming (API enforces this)
- Confirmation dialog in UI with event name and chunk count
- S3 errors are reported, not swallowed — if cleanup fails, event stays in DB

---

## Delivery

No changes to the delivery pipeline. The VPS receives `event_identifier` = `event.name` via the init API. S3 key format remains `{event.name}/{sequence_number}.bin`.

---

## Data Model (Rust Structs)

### New: EventTemplate

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTemplate {
    pub id: i64,
    pub name: String,
    pub cache_delay_secs: Option<i64>,
}
```

### Modified: StreamingEvent

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEvent {
    pub id: i64,
    pub name: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
    pub cache_delay_secs: Option<i64>,
    pub created_from: Option<String>,  // NEW — template name at creation time
}
```

---

## Migration Strategy

### Migration V12

```sql
-- Create template tables
CREATE TABLE IF NOT EXISTS event_templates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL UNIQUE,
    cache_delay_secs INTEGER
);

CREATE TABLE IF NOT EXISTS template_endpoints (
    template_id INTEGER NOT NULL REFERENCES event_templates(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
    PRIMARY KEY (template_id, endpoint_id)
);

-- Add created_from to events
ALTER TABLE streaming_events ADD COLUMN created_from TEXT;
```

### Backward Compatibility

- Existing events: `created_from = NULL`. Continue working unchanged.
- Templates start empty. User creates templates through the dashboard after upgrade.
- No data migration needed — existing events are already independent.

---

## E2E Test Coverage

| Feature | What to Test |
|---------|-------------|
| Template CRUD | Create, read, update, delete template via API |
| Template endpoints | Attach/detach endpoints to template |
| Create event from template | POST with template_id, verify name generation, endpoint copy, cache delay copy |
| Duplicate name handling | Create two events from same template on same day, verify `-2` suffix |
| Delete event + S3 cleanup | Delete event, verify S3 objects removed, DB records cascaded |
| Prevent delete while streaming | Try to delete streaming event, verify 409 |
| Template deletion independence | Delete template, verify events created from it still work |
| Template CRUD (API) | Create, list, get, update, delete template via REST |
| UI: Events tab (Playwright) | Create event from template, verify in list, delete with cleanup |
| UI: Templates tab (Playwright) | Create template, assign endpoints, edit, delete via browser |

---

## Out of Scope

- **Restream**: Replaying a past event's chunks to endpoints. Future feature (#89 mentions it).
- **Auto-start from template**: Templates don't auto-trigger on RTMP connect. User must explicitly create and start.
- **Template versioning**: No history of template changes.
- **Bulk delete**: Delete multiple events at once. Future enhancement if needed.
