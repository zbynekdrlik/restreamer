# Event Template/Instance Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate reusable event configuration (templates) from streaming sessions (events), with S3 chunk cleanup on event deletion.

**Architecture:** Templates are presets stored in SQLite, managed via dashboard UI (Templates tab). Events are fully independent after creation from a template — no FK constraint. Deleting an event deletes its S3 chunks. Two-tab UI: Templates + Events.

**Tech Stack:** Rust, SQLite (sqlx runtime queries), Axum REST API, Leptos CSR (WASM), Playwright E2E, rust-s3

**Spec:** `docs/superpowers/specs/2026-04-09-event-template-instance-design.md`

---

## Context

- **DB migrations** use numbered constants (`MIGRATION_V1_SQL` through `MIGRATION_V11_SQL`) in `crates/rs-core/src/db/mod.rs`. The `run_migrations()` function iterates a `(version, sql)` array and runs each migration in a transaction.
- **DB queries** use runtime `sqlx::query()` (not compile-time macros) in `crates/rs-core/src/db/v2.rs`. Rows are mapped via `r.get("column_name")`.
- **API handlers** in `crates/rs-api/src/handlers.rs` extract `State(state): State<AppState>` and return `Result<Json<T>, StatusCode>`.
- **Router** in `crates/rs-api/src/router.rs` uses `Router::new().route("/path", get(handler))` pattern nested under `/api/v1`.
- **AppState** in `crates/rs-api/src/state.rs` has `pool: SqlitePool` and `config: Arc<Config>` (which includes `s3: S3Config`).
- **Leptos API** in `leptos-ui/src/api.rs` uses `http_get`, `http_post`, `http_post_json`, `http_delete`, `http_patch_json` helpers with `api_base()` URL prefix.
- **Leptos components** in `leptos-ui/src/components/` use `#[component]` functions with `RwSignal`, `Effect`, `spawn_local`.
- **E2E mock** in `e2e/mock-api.js` is an Express server with in-memory data arrays.
- **rs-api does NOT depend on rs-endpoint** — needs to be added for S3Client access.

---

### Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml` (line ~24)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump version 0.3.27 → 0.3.28 in all four files**

In `Cargo.toml` (workspace root, line ~24):
```toml
version = "0.3.28"
```

In `src-tauri/Cargo.toml`:
```toml
version = "0.3.28"
```

In `src-tauri/tauri.conf.json`:
```json
"version": "0.3.28"
```

In `leptos-ui/Cargo.toml`:
```toml
version = "0.3.28"
```

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.28"
```

---

### Task 2: DB Migration V12 + EventTemplate Model

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`
- Modify: `crates/rs-core/src/models.rs`

- [ ] **Step 1: Write failing test for migration V12**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn migration_v12_creates_template_tables() {
    let pool = setup_db().await;

    // event_templates table exists and is writable
    let id: i64 =
        sqlx::query("INSERT INTO event_templates (name) VALUES ('test') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("id");
    assert!(id > 0);

    // template_endpoints table exists (FK to event_templates)
    // Create a dummy endpoint first
    let ep_id: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id"
    )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("id");

    sqlx::query("INSERT INTO template_endpoints (template_id, endpoint_id) VALUES (?1, ?2)")
        .bind(id)
        .bind(ep_id)
        .execute(&pool)
        .await
        .unwrap();

    // created_from column exists on streaming_events
    let evt_id = create_streaming_event(&pool, "test-evt").await.unwrap();
    sqlx::query("UPDATE streaming_events SET created_from = 'test-template' WHERE id = ?1")
        .bind(evt_id)
        .execute(&pool)
        .await
        .unwrap();

    let val: Option<String> =
        sqlx::query("SELECT created_from FROM streaming_events WHERE id = ?1")
            .bind(evt_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("created_from");
    assert_eq!(val.as_deref(), Some("test-template"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rs-core migration_v12 -- --nocapture`
Expected: FAIL — `event_templates` table doesn't exist

- [ ] **Step 3: Add MIGRATION_V12_SQL and register it**

In `crates/rs-core/src/db/mod.rs`, add the constant (after `MIGRATION_V11_SQL`):

```rust
const MIGRATION_V12_SQL: &str = r#"
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

ALTER TABLE streaming_events ADD COLUMN created_from TEXT
"#;
```

In `run_migrations()`, add to the migrations array:

```rust
(12, MIGRATION_V12_SQL),
```

- [ ] **Step 4: Add EventTemplate struct to models.rs**

In `crates/rs-core/src/models.rs`:

```rust
/// Reusable event configuration preset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTemplate {
    pub id: i64,
    pub name: String,
    pub cache_delay_secs: Option<i64>,
}
```

Add `created_from` field to the existing `StreamingEvent` struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEvent {
    pub id: i64,
    pub name: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
    pub cache_delay_secs: Option<i64>,
    pub created_from: Option<String>,
}
```

- [ ] **Step 5: Update list_streaming_events to include created_from**

In `crates/rs-core/src/db/v2.rs`, update the `list_streaming_events` query to include the new column:

```rust
pub async fn list_streaming_events(pool: &SqlitePool) -> Result<Vec<StreamingEvent>> {
    let rows = sqlx::query(
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from
         FROM streaming_events ORDER BY id DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| StreamingEvent {
            id: r.get("id"),
            name: r.get("name"),
            received_bytes: r.get("received_bytes"),
            receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
            delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
            cache_delay_secs: r.get("cache_delay_secs"),
            created_from: r.get("created_from"),
        })
        .collect())
}
```

Also update `get_streaming_event_by_id` (same file) to include `created_from` in its SELECT and mapping.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p rs-core migration_v12 -- --nocapture`
Expected: PASS

- [ ] **Step 7: Run all rs-core tests**

Run: `cargo test -p rs-core -- --nocapture`
Expected: All pass (existing tests still work with new column defaulting to NULL)

- [ ] **Step 8: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs crates/rs-core/src/models.rs crates/rs-core/src/db/v2.rs
git commit -m "feat: add event_templates DB schema and EventTemplate model (#89)"
```

---

### Task 3: Template DB Queries + Tests

**Files:**
- Create: `crates/rs-core/src/db/templates.rs`
- Modify: `crates/rs-core/src/db/mod.rs` (add `mod templates; pub use templates::*;`)
- Modify: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write failing tests for template CRUD**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn template_crud() {
    let pool = setup_db().await;

    // Create
    let id = create_template(&pool, "sunday-service", None).await.unwrap();
    assert!(id > 0);

    // List
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].name, "sunday-service");
    assert_eq!(templates[0].cache_delay_secs, None);

    // Get by ID
    let t = get_template_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(t.name, "sunday-service");

    // Update
    update_template(&pool, id, "sunday-worship", Some(120)).await.unwrap();
    let t = get_template_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(t.name, "sunday-worship");
    assert_eq!(t.cache_delay_secs, Some(120));

    // Delete
    delete_template(&pool, id).await.unwrap();
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 0);
}

#[tokio::test]
async fn template_duplicate_name_fails() {
    let pool = setup_db().await;
    create_template(&pool, "sunday", None).await.unwrap();
    let result = create_template(&pool, "sunday", None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn template_endpoint_linking() {
    let pool = setup_db().await;

    let tid = create_template(&pool, "sunday", None).await.unwrap();
    // Create test endpoint
    let eid: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id"
    ).fetch_one(&pool).await.unwrap().get("id");

    // Attach
    attach_endpoint_to_template(&pool, tid, eid).await.unwrap();
    let eps = get_template_endpoints(&pool, tid).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].alias, "yt");

    // Duplicate attach is idempotent
    attach_endpoint_to_template(&pool, tid, eid).await.unwrap();
    let eps = get_template_endpoints(&pool, tid).await.unwrap();
    assert_eq!(eps.len(), 1);

    // Detach
    detach_endpoint_from_template(&pool, tid, eid).await.unwrap();
    let eps = get_template_endpoints(&pool, tid).await.unwrap();
    assert_eq!(eps.len(), 0);
}

#[tokio::test]
async fn template_cascade_deletes_endpoints() {
    let pool = setup_db().await;

    let tid = create_template(&pool, "sunday", None).await.unwrap();
    let eid: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id"
    ).fetch_one(&pool).await.unwrap().get("id");
    attach_endpoint_to_template(&pool, tid, eid).await.unwrap();

    // Delete template — should cascade to template_endpoints
    delete_template(&pool, tid).await.unwrap();

    let count: i64 = sqlx::query("SELECT COUNT(*) as c FROM template_endpoints")
        .fetch_one(&pool).await.unwrap().get("c");
    assert_eq!(count, 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rs-core template -- --nocapture`
Expected: FAIL — functions don't exist

- [ ] **Step 3: Implement template DB queries**

Create `crates/rs-core/src/db/templates.rs`:

```rust
use anyhow::Result;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;

use crate::models::{EndpointConfig, EventTemplate};

pub async fn create_template(
    pool: &SqlitePool,
    name: &str,
    cache_delay_secs: Option<i64>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO event_templates (name, cache_delay_secs) VALUES (?1, ?2) RETURNING id",
    )
    .bind(name)
    .bind(cache_delay_secs)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn list_templates(pool: &SqlitePool) -> Result<Vec<EventTemplate>> {
    let rows = sqlx::query("SELECT id, name, cache_delay_secs FROM event_templates ORDER BY id")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|r| EventTemplate {
            id: r.get("id"),
            name: r.get("name"),
            cache_delay_secs: r.get("cache_delay_secs"),
        })
        .collect())
}

pub async fn get_template_by_id(pool: &SqlitePool, id: i64) -> Result<Option<EventTemplate>> {
    let row = sqlx::query("SELECT id, name, cache_delay_secs FROM event_templates WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(|r| EventTemplate {
        id: r.get("id"),
        name: r.get("name"),
        cache_delay_secs: r.get("cache_delay_secs"),
    }))
}

pub async fn update_template(
    pool: &SqlitePool,
    id: i64,
    name: &str,
    cache_delay_secs: Option<i64>,
) -> Result<()> {
    sqlx::query("UPDATE event_templates SET name = ?1, cache_delay_secs = ?2 WHERE id = ?3")
        .bind(name)
        .bind(cache_delay_secs)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_template(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM event_templates WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn attach_endpoint_to_template(
    pool: &SqlitePool,
    template_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO template_endpoints (template_id, endpoint_id) VALUES (?1, ?2)",
    )
    .bind(template_id)
    .bind(endpoint_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn detach_endpoint_from_template(
    pool: &SqlitePool,
    template_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM template_endpoints WHERE template_id = ?1 AND endpoint_id = ?2")
        .bind(template_id)
        .bind(endpoint_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_template_endpoints(
    pool: &SqlitePool,
    template_id: i64,
) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT e.id, e.alias, e.service_type, e.stream_key, e.enabled, e.position_last,
         e.delivered_bytes, e.is_fast, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN template_endpoints te ON te.endpoint_id = e.id
         WHERE te.template_id = ?1
         ORDER BY e.id",
    )
    .bind(template_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EndpointConfig {
            id: r.get("id"),
            alias: r.get("alias"),
            service_type: r.get("service_type"),
            stream_key: r.get("stream_key"),
            enabled: r.get::<i32, _>("enabled") != 0,
            position_last: r.get("position_last"),
            delivered_bytes: r.get("delivered_bytes"),
            is_fast: r.get::<i32, _>("is_fast") != 0,
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}
```

- [ ] **Step 4: Register module in mod.rs**

In `crates/rs-core/src/db/mod.rs`, add:

```rust
mod templates;
pub use templates::*;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p rs-core template -- --nocapture`
Expected: All 4 template tests PASS

- [ ] **Step 6: Run all rs-core tests**

Run: `cargo test -p rs-core -- --nocapture`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add crates/rs-core/src/db/templates.rs crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat: add template CRUD DB queries with tests (#89)"
```

---

### Task 4: Create Event from Template + Date Naming

**Files:**
- Modify: `crates/rs-core/src/db/v2.rs`
- Modify: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write failing tests**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn create_event_from_template() {
    let pool = setup_db().await;

    // Create template with endpoint
    let tid = create_template(&pool, "sunday-service", Some(120)).await.unwrap();
    let eid: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id"
    ).fetch_one(&pool).await.unwrap().get("id");
    attach_endpoint_to_template(&pool, tid, eid).await.unwrap();

    // Create event from template
    let (event_id, event_name) = create_event_from_template(&pool, tid).await.unwrap();
    assert!(event_id > 0);
    // Name should be template name + today's date
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    assert_eq!(event_name, format!("sunday-service-{today}"));

    // Event should have correct fields
    let evt = get_streaming_event_by_id(&pool, event_id).await.unwrap().unwrap();
    assert_eq!(evt.cache_delay_secs, Some(120));
    assert_eq!(evt.created_from.as_deref(), Some("sunday-service"));

    // Endpoints should be copied
    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].alias, "yt");
}

#[tokio::test]
async fn create_event_from_template_duplicate_date() {
    let pool = setup_db().await;

    let tid = create_template(&pool, "sunday", None).await.unwrap();

    // First event: sunday-YYYY-MM-DD
    let (_, name1) = create_event_from_template(&pool, tid).await.unwrap();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    assert_eq!(name1, format!("sunday-{today}"));

    // Second event same day: sunday-YYYY-MM-DD-2
    let (_, name2) = create_event_from_template(&pool, tid).await.unwrap();
    assert_eq!(name2, format!("sunday-{today}-2"));

    // Third: sunday-YYYY-MM-DD-3
    let (_, name3) = create_event_from_template(&pool, tid).await.unwrap();
    assert_eq!(name3, format!("sunday-{today}-3"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rs-core create_event_from_template -- --nocapture`
Expected: FAIL — function doesn't exist

- [ ] **Step 3: Implement create_event_from_template**

Add to `crates/rs-core/src/db/v2.rs`:

```rust
/// Create a new streaming event by copying configuration from a template.
/// Generates a date-based name: `{template.name}-{YYYY-MM-DD}`.
/// If duplicate, appends `-2`, `-3`, etc.
/// Copies template endpoints into event_endpoints.
/// Returns (event_id, event_name).
pub async fn create_event_from_template(
    pool: &SqlitePool,
    template_id: i64,
) -> Result<(i64, String)> {
    // Fetch template
    let template = super::get_template_by_id(pool, template_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("template {template_id} not found"))?;

    // Generate date-based name
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let base_name = format!("{}-{today}", template.name);

    // Find unique name
    let event_name = find_unique_event_name(pool, &base_name).await?;

    // Create event
    let row = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, created_from) VALUES (?1, ?2, ?3) RETURNING id",
    )
    .bind(&event_name)
    .bind(template.cache_delay_secs)
    .bind(&template.name)
    .fetch_one(pool)
    .await?;
    let event_id: i64 = row.get("id");

    // Copy template endpoints to event
    let template_eps = super::get_template_endpoints(pool, template_id).await?;
    for ep in &template_eps {
        super::attach_endpoint_to_event(pool, event_id, ep.id).await?;
    }

    Ok((event_id, event_name))
}

/// Find a unique event name by appending -2, -3, etc. if base_name already exists.
async fn find_unique_event_name(pool: &SqlitePool, base_name: &str) -> Result<String> {
    // Check if base name is available
    let exists: bool = sqlx::query("SELECT 1 FROM streaming_events WHERE name = ?1")
        .bind(base_name)
        .fetch_optional(pool)
        .await?
        .is_some();

    if !exists {
        return Ok(base_name.to_string());
    }

    // Try suffixes -2, -3, etc.
    for suffix in 2..=100 {
        let candidate = format!("{base_name}-{suffix}");
        let exists = sqlx::query("SELECT 1 FROM streaming_events WHERE name = ?1")
            .bind(&candidate)
            .fetch_optional(pool)
            .await?
            .is_some();
        if !exists {
            return Ok(candidate);
        }
    }

    anyhow::bail!("could not find unique name for {base_name} after 100 attempts")
}
```

Add `chrono` to `crates/rs-core/Cargo.toml` dependencies if not already present:

```toml
chrono = "0.4"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p rs-core create_event_from_template -- --nocapture`
Expected: Both tests PASS

- [ ] **Step 5: Run all rs-core tests**

Run: `cargo test -p rs-core -- --nocapture`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add crates/rs-core/src/db/v2.rs crates/rs-core/src/db/tests.rs crates/rs-core/Cargo.toml
git commit -m "feat: create event from template with date naming (#89)"
```

---

### Task 5: Template API Handlers + Routes

**Files:**
- Create: `crates/rs-api/src/template_handlers.rs`
- Modify: `crates/rs-api/src/router.rs`
- Modify: `crates/rs-api/src/lib.rs` (add `mod template_handlers;`)

- [ ] **Step 1: Write failing test for template API**

Add to `crates/rs-core/src/db/tests.rs` (API integration test via DB layer — the E2E Playwright test in Task 10 covers the full HTTP path):

```rust
#[tokio::test]
async fn template_api_roundtrip() {
    let pool = setup_db().await;

    // Create template
    let id = create_template(&pool, "wednesday", Some(60)).await.unwrap();

    // Create event from template
    let (event_id, name) = create_event_from_template(&pool, id).await.unwrap();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    assert!(name.starts_with("wednesday-"));
    assert!(name.contains(&today));

    // Event is independent — update template doesn't affect event
    update_template(&pool, id, "wed-study", Some(90)).await.unwrap();
    let evt = get_streaming_event_by_id(&pool, event_id).await.unwrap().unwrap();
    assert_eq!(evt.cache_delay_secs, Some(60)); // Still 60, not 90
    assert_eq!(evt.created_from.as_deref(), Some("wednesday")); // Original name

    // Delete template — event still exists
    delete_template(&pool, id).await.unwrap();
    let evt = get_streaming_event_by_id(&pool, event_id).await.unwrap();
    assert!(evt.is_some());
}
```

- [ ] **Step 2: Run test to verify it passes** (this test validates DB-level independence; it should pass with existing code)

Run: `cargo test -p rs-core template_api_roundtrip -- --nocapture`
Expected: PASS

- [ ] **Step 3: Implement template API handlers**

Create `crates/rs-api/src/template_handlers.rs`:

```rust
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use log::error;
use serde::Deserialize;

use rs_core::db;
use rs_core::models::{EndpointConfig, EventTemplate};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateTemplateRequest {
    pub name: String,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
}

#[derive(Deserialize)]
pub struct UpdateTemplateRequest {
    pub name: Option<String>,
    pub cache_delay_secs: Option<i64>,
}

pub async fn list_templates(
    State(state): State<AppState>,
) -> Result<Json<Vec<EventTemplate>>, StatusCode> {
    let templates = db::list_templates(&state.pool).await.map_err(|e| {
        error!("Failed to list templates: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(templates))
}

pub async fn create_template(
    State(state): State<AppState>,
    Json(req): Json<CreateTemplateRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    if req.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let id = db::create_template(&state.pool, req.name.trim(), req.cache_delay_secs)
        .await
        .map_err(|e| {
            error!("Failed to create template: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
}

pub async fn get_template(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<EventTemplate>, StatusCode> {
    let template = db::get_template_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(template))
}

pub async fn update_template(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Json(req): Json<UpdateTemplateRequest>,
) -> Result<StatusCode, StatusCode> {
    let existing = db::get_template_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let new_name = req.name.as_deref().unwrap_or(&existing.name);
    if new_name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let new_delay = match req.cache_delay_secs {
        Some(d) => Some(d),
        None => existing.cache_delay_secs,
    };

    db::update_template(&state.pool, id, new_name.trim(), new_delay)
        .await
        .map_err(|e| {
            error!("Failed to update template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::OK)
}

pub async fn delete_template(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    db::delete_template(&state.pool, id).await.map_err(|e| {
        error!("Failed to delete template {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_template_endpoints(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<Vec<EndpointConfig>>, StatusCode> {
    let eps = db::get_template_endpoints(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get endpoints for template {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(eps))
}

pub async fn attach_endpoint_to_template(
    State(state): State<AppState>,
    axum::extract::Path((template_id, endpoint_id)): axum::extract::Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::attach_endpoint_to_template(&state.pool, template_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to attach endpoint {endpoint_id} to template {template_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::CREATED)
}

pub async fn detach_endpoint_from_template(
    State(state): State<AppState>,
    axum::extract::Path((template_id, endpoint_id)): axum::extract::Path<(i64, i64)>,
) -> Result<StatusCode, StatusCode> {
    db::detach_endpoint_from_template(&state.pool, template_id, endpoint_id)
        .await
        .map_err(|e| {
            error!("Failed to detach endpoint {endpoint_id} from template {template_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}
```

- [ ] **Step 4: Register module and routes**

In `crates/rs-api/src/lib.rs`, add:

```rust
mod template_handlers;
```

In `crates/rs-api/src/router.rs`, add template routes to the `api` router:

```rust
// Templates CRUD
.route("/templates", get(template_handlers::list_templates))
.route("/templates", post(template_handlers::create_template))
.route("/templates/{id}", get(template_handlers::get_template))
.route("/templates/{id}", patch(template_handlers::update_template))
.route("/templates/{id}", delete(template_handlers::delete_template))
.route("/templates/{id}/endpoints", get(template_handlers::get_template_endpoints))
.route("/templates/{template_id}/endpoints/{endpoint_id}",
    post(template_handlers::attach_endpoint_to_template))
.route("/templates/{template_id}/endpoints/{endpoint_id}",
    delete(template_handlers::detach_endpoint_from_template))
```

- [ ] **Step 5: Check formatting**

Run: `cargo fmt --all --check`
Expected: No issues (fix if needed)

- [ ] **Step 6: Commit**

```bash
git add crates/rs-api/src/template_handlers.rs crates/rs-api/src/lib.rs crates/rs-api/src/router.rs
git commit -m "feat: add template CRUD API endpoints (#89)"
```

---

### Task 6: Create Event from Template API + Modified Create

**Files:**
- Modify: `crates/rs-api/src/handlers.rs`

- [ ] **Step 1: Modify create_event handler**

In `crates/rs-api/src/handlers.rs`, update the `CreateEventRequest` and `create_event` handler:

```rust
#[derive(Deserialize)]
pub struct CreateEventRequest {
    /// Direct event name (standalone creation)
    pub name: Option<String>,
    /// Template ID (create from template with date-based name)
    pub template_id: Option<i64>,
}

pub async fn create_event(
    State(state): State<AppState>,
    Json(req): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    match (req.template_id, req.name) {
        (Some(tid), _) => {
            // Create from template
            let (id, name) = db::create_event_from_template(&state.pool, tid)
                .await
                .map_err(|e| {
                    error!("Failed to create event from template {tid}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({ "id": id, "name": name })),
            ))
        }
        (None, Some(name)) => {
            // Standalone creation (backward compat for API/CI)
            if name.trim().is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            let id = db::create_streaming_event(&state.pool, name.trim())
                .await
                .map_err(|e| {
                    error!("Failed to create event: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            Ok((StatusCode::CREATED, Json(serde_json::json!({ "id": id }))))
        }
        (None, None) => Err(StatusCode::BAD_REQUEST),
    }
}
```

- [ ] **Step 2: Check formatting**

Run: `cargo fmt --all --check`

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/handlers.rs
git commit -m "feat: create event from template via API (#89)"
```

---

### Task 7: S3 Chunk Cleanup on Event Delete

**Files:**
- Modify: `crates/rs-endpoint/src/s3.rs` (add `delete_event_chunks`)
- Modify: `crates/rs-api/Cargo.toml` (add `rs-endpoint` dependency)
- Modify: `crates/rs-api/src/handlers.rs` (modify `delete_event_by_id`)

- [ ] **Step 1: Write failing test for S3 delete method**

Add to `crates/rs-endpoint/src/s3.rs` (at the bottom, inside a `#[cfg(test)] mod tests` block):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_format() {
        assert_eq!(S3Client::chunk_key("evt-1", 42), "evt-1/42.bin");
    }
}
```

(The actual S3 delete is an integration test that needs real S3 — we test the API-level behavior via E2E in Task 10. Here we just verify the method compiles and the key format is correct.)

- [ ] **Step 2: Add delete_event_chunks to S3Client**

In `crates/rs-endpoint/src/s3.rs`, add to `impl S3Client`:

```rust
    /// Delete all S3 objects under the given event name prefix.
    /// Returns the number of objects deleted.
    pub async fn delete_event_chunks(&self, event_name: &str) -> Result<u64, EndpointError> {
        let prefix = format!("{event_name}/");
        let mut deleted = 0u64;

        loop {
            let list = self
                .bucket
                .list(prefix.clone(), None)
                .await
                .map_err(|e| EndpointError::S3(format!("list failed: {e}")))?;

            let keys: Vec<String> = list
                .iter()
                .flat_map(|page| page.contents.iter().map(|obj| obj.key.clone()))
                .collect();

            if keys.is_empty() {
                break;
            }

            for key in &keys {
                let (_, code) = self
                    .bucket
                    .delete_object(key)
                    .await
                    .map_err(|e| EndpointError::S3(format!("delete {key} failed: {e}")))?;
                if code >= 300 {
                    return Err(EndpointError::S3(format!(
                        "delete {key} returned status {code}"
                    )));
                }
                deleted += 1;
            }
        }

        info!("Deleted {deleted} S3 objects under prefix '{prefix}'");
        Ok(deleted)
    }
```

- [ ] **Step 3: Add rs-endpoint dependency to rs-api**

In `crates/rs-api/Cargo.toml`, add under `[dependencies]`:

```toml
rs-endpoint = { path = "../rs-endpoint" }
```

- [ ] **Step 4: Modify delete_event_by_id handler**

In `crates/rs-api/src/handlers.rs`, update the delete handler:

```rust
pub async fn delete_event_by_id(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<StatusCode, StatusCode> {
    // Fetch event
    let event = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to get event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Prevent delete while streaming
    if event.receiving_activated || event.delivering_activated {
        return Err(StatusCode::CONFLICT);
    }

    // S3 cleanup — delete all chunks under event name prefix
    let s3_config = &state.config.s3;
    match rs_endpoint::s3::S3Client::new(s3_config) {
        Ok(s3) => {
            if let Err(e) = s3.delete_event_chunks(&event.name).await {
                error!("S3 cleanup failed for event '{}': {e}", event.name);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
        Err(e) => {
            error!("Failed to create S3 client for cleanup: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Delete DB records (cascades to chunk_records and event_endpoints)
    db::delete_streaming_event(&state.pool, id)
        .await
        .map_err(|e| {
            error!("Failed to delete event {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::NO_CONTENT)
}
```

- [ ] **Step 5: Check formatting**

Run: `cargo fmt --all --check`

- [ ] **Step 6: Commit**

```bash
git add crates/rs-endpoint/src/s3.rs crates/rs-api/Cargo.toml crates/rs-api/src/handlers.rs
git commit -m "feat: S3 chunk cleanup on event delete (#90)"
```

---

### Task 8: Leptos Template API + Store

**Files:**
- Modify: `leptos-ui/src/api.rs`
- Modify: `leptos-ui/src/store.rs`

- [ ] **Step 1: Add EventTemplate struct and API functions to api.rs**

In `leptos-ui/src/api.rs`, add the struct and functions:

```rust
/// Event template (reusable preset).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct EventTemplate {
    pub id: i64,
    pub name: String,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
}

// Templates API
pub async fn list_templates() -> Result<Vec<EventTemplate>, String> {
    http_get("/templates").await
}

pub async fn create_template(
    name: &str,
    cache_delay_secs: Option<i64>,
) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_delay_secs: Option<i64>,
    }
    http_post_json(
        "/templates",
        &Body {
            name: name.to_string(),
            cache_delay_secs,
        },
    )
    .await
}

pub async fn update_template(
    id: i64,
    name: Option<&str>,
    cache_delay_secs: Option<i64>,
) -> Result<(), String> {
    #[derive(Serialize)]
    struct Body {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_delay_secs: Option<i64>,
    }
    http_patch_json(
        &format!("/templates/{id}"),
        &Body {
            name: name.map(|s| s.to_string()),
            cache_delay_secs,
        },
    )
    .await
}

pub async fn delete_template(id: i64) -> Result<(), String> {
    http_delete(&format!("/templates/{id}")).await
}

pub async fn get_template_endpoints(template_id: i64) -> Result<Vec<EndpointConfig>, String> {
    http_get(&format!("/templates/{template_id}/endpoints")).await
}

pub async fn attach_template_endpoint(
    template_id: i64,
    endpoint_id: i64,
) -> Result<(), String> {
    http_post(&format!("/templates/{template_id}/endpoints/{endpoint_id}")).await
}

pub async fn detach_template_endpoint(
    template_id: i64,
    endpoint_id: i64,
) -> Result<(), String> {
    http_delete(&format!("/templates/{template_id}/endpoints/{endpoint_id}")).await
}

/// Create event from template (returns {id, name}).
pub async fn create_event_from_template(
    template_id: i64,
) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        template_id: i64,
    }
    http_post_json("/events", &Body { template_id }).await
}
```

Also add `created_from` field to the existing `StreamingEvent` struct in api.rs:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StreamingEvent {
    pub id: i64,
    pub name: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub created_from: Option<String>,
}
```

- [ ] **Step 2: Add templates_list to DashboardStore**

In `leptos-ui/src/store.rs`, add to the `DashboardStore` struct:

```rust
pub templates_list: RwSignal<Vec<crate::api::EventTemplate>>,
```

And in `DashboardStore::new()`:

```rust
templates_list: RwSignal::new(Vec::new()),
```

- [ ] **Step 3: Check formatting (Leptos uses a different check)**

Run: `cargo fmt --all --check`

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/api.rs leptos-ui/src/store.rs
git commit -m "feat: add template API and store for Leptos frontend (#89)"
```

---

### Task 9: Leptos Templates Tab Component

**Files:**
- Create: `leptos-ui/src/components/templates.rs`
- Modify: `leptos-ui/src/components/mod.rs`

- [ ] **Step 1: Create TemplatesView component**

Create `leptos-ui/src/components/templates.rs`:

```rust
//! Templates management tab — CRUD for event templates with endpoint assignment.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

/// Templates management view.
#[component]
pub fn TemplatesView() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let (error, set_error) = signal::<Option<String>>(None);
    let (new_name, set_new_name) = signal(String::new());
    let (new_delay, set_new_delay) = signal(String::new());

    // Fetch templates on mount
    Effect::new(move |_| {
        spawn_local(async move {
            if let Ok(templates) = api::list_templates().await {
                store.templates_list.set(templates);
            }
        });
    });

    let on_create = move |_| {
        let name = new_name.get();
        if name.is_empty() {
            return;
        }
        let delay: Option<i64> = new_delay.get().parse().ok();
        spawn_local(async move {
            match api::create_template(&name, delay).await {
                Ok(_) => {
                    set_new_name.set(String::new());
                    set_new_delay.set(String::new());
                    if let Ok(t) = api::list_templates().await {
                        store.templates_list.set(t);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    view! {
        <div class="templates-tab">
            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}
            <div class="create-form template-create">
                <input
                    type="text"
                    placeholder="Template name..."
                    prop:value=move || new_name.get()
                    on:input=move |ev| set_new_name.set(event_target_value(&ev))
                />
                <input
                    type="number"
                    placeholder="Cache delay (s)"
                    class="cache-delay-input"
                    prop:value=move || new_delay.get()
                    on:input=move |ev| set_new_delay.set(event_target_value(&ev))
                />
                <button on:click=on_create>"+ New Template"</button>
            </div>
            <div class="template-list">
                {move || store.templates_list.get().into_iter().map(|t| {
                    let id = t.id;
                    view! { <TemplateCard template=t set_error=set_error /> }
                }).collect_view()}
            </div>
        </div>
    }
}

#[component]
fn TemplateCard(
    template: api::EventTemplate,
    set_error: WriteSignal<Option<String>>,
) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let id = template.id;
    let name = template.name.clone();
    let delay = template.cache_delay_secs;
    let (assigned, set_assigned) = signal::<Vec<api::EndpointConfig>>(Vec::new());
    let (selected_ep, set_selected_ep) = signal(String::new());

    // Fetch assigned endpoints
    Effect::new(move |_| {
        spawn_local(async move {
            match api::get_template_endpoints(id).await {
                Ok(eps) => set_assigned.set(eps),
                Err(e) => set_error.set(Some(e)),
            }
        });
    });

    let on_delete = move |_| {
        spawn_local(async move {
            match api::delete_template(id).await {
                Ok(_) => {
                    if let Ok(t) = api::list_templates().await {
                        store.templates_list.set(t);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    let on_assign = move |_| {
        let ep_id_str = selected_ep.get();
        if ep_id_str.is_empty() {
            return;
        }
        let ep_id: i64 = match ep_id_str.parse() {
            Ok(id) => id,
            Err(_) => return,
        };
        spawn_local(async move {
            match api::attach_template_endpoint(id, ep_id).await {
                Ok(_) => {
                    set_selected_ep.set(String::new());
                    if let Ok(eps) = api::get_template_endpoints(id).await {
                        set_assigned.set(eps);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    view! {
        <div class="template-card">
            <div class="template-header">
                <strong>{name}</strong>
                {delay.map(|d| view! {
                    <span class="cache-badge">{format!("{d}s cache")}</span>
                })}
                <button class="danger small" on:click=on_delete>"Delete"</button>
            </div>
            <div class="assigned-endpoints">
                <div class="assigned-label">"Endpoints:"</div>
                <div class="assigned-list">
                    {move || {
                        let eps = assigned.get();
                        if eps.is_empty() {
                            view! { <span class="empty-inline">"None"</span> }.into_any()
                        } else {
                            eps.into_iter().map(|ep| {
                                let ep_id = ep.id;
                                let alias = ep.alias.clone();
                                let stype = ep.service_type.clone();
                                view! {
                                    <span class="assigned-ep">
                                        <span class="service-badge">{stype}</span>
                                        {alias}
                                        <button class="remove-ep" on:click=move |_| {
                                            spawn_local(async move {
                                                let _ = api::detach_template_endpoint(id, ep_id).await;
                                                if let Ok(eps) = api::get_template_endpoints(id).await {
                                                    set_assigned.set(eps);
                                                }
                                            });
                                        }>"x"</button>
                                    </span>
                                }
                            }).collect_view().into_any()
                        }
                    }}
                </div>
                <div class="assign-form">
                    <select
                        prop:value=move || selected_ep.get()
                        on:change=move |ev| set_selected_ep.set(event_target_value(&ev))
                    >
                        <option value="">"-- Assign endpoint --"</option>
                        {move || {
                            let assigned_ids: Vec<i64> = assigned.get().iter().map(|e| e.id).collect();
                            store.endpoints_list.get().into_iter()
                                .filter(move |ep| !assigned_ids.contains(&ep.id))
                                .map(|ep| {
                                    let val = ep.id.to_string();
                                    let label = format!("{} ({})", ep.alias, ep.service_type);
                                    view! { <option value={val}>{label}</option> }
                                }).collect_view()
                        }}
                    </select>
                    <button on:click=on_assign>"Assign"</button>
                </div>
            </div>
        </div>
    }
}
```

- [ ] **Step 2: Register module in mod.rs**

In `leptos-ui/src/components/mod.rs`, add:

```rust
mod templates;

pub use templates::TemplatesView;
```

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/templates.rs leptos-ui/src/components/mod.rs
git commit -m "feat: add TemplatesView component for template management (#89)"
```

---

### Task 10: Leptos Events Tab + Tab Navigation in Settings

**Files:**
- Modify: `leptos-ui/src/components/settings.rs`

The settings view is the natural place for management tabs. The operator dashboard ControlBar event selector stays unchanged for quick stream start/stop. The settings view gets two new tabs: Templates and Events.

- [ ] **Step 1: Read current settings.rs**

Read `leptos-ui/src/components/settings.rs` to understand its current structure before modifying.

- [ ] **Step 2: Add tab navigation and Events management to settings**

Add to `leptos-ui/src/components/settings.rs`, a tabbed section for Templates and Events. The implementer should read the current file first, then add:

1. A `settings_tab` signal (`"config"`, `"templates"`, `"events"`) to switch between existing settings content, Templates tab, and Events tab.
2. Tab buttons at the top of the settings view.
3. For the "templates" tab: render `<TemplatesView />`.
4. For the "events" tab: render an `EventsManagement` component (inline in settings.rs or separate file) that shows:
   - List of all events with name, status badge, created_from label, chunk count
   - "New from Template" button that opens a template picker and calls `api::create_event_from_template`
   - "Delete + Cleanup" button per event (with confirmation dialog)
   - "Start Stream" / "Stop Stream" buttons per event

The events management section:

```rust
#[component]
fn EventsManagement() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let (error, set_error) = signal::<Option<String>>(None);
    let (show_template_picker, set_show_template_picker) = signal(false);
    let (confirm_delete, set_confirm_delete) = signal::<Option<(i64, String)>>(None);

    // Refresh events list
    let refresh = move || {
        spawn_local(async move {
            if let Ok(evts) = api::list_events().await {
                store.events_list.set(evts);
            }
        });
    };

    Effect::new(move |_| {
        refresh();
    });

    let on_create_from_template = move |template_id: i64| {
        spawn_local(async move {
            match api::create_event_from_template(template_id).await {
                Ok(_) => {
                    set_show_template_picker.set(false);
                    if let Ok(evts) = api::list_events().await {
                        store.events_list.set(evts);
                    }
                }
                Err(e) => set_error.set(Some(e)),
            }
        });
    };

    view! {
        <div class="events-management">
            {move || error.get().map(|e| view! {
                <div class="error-message">{e}</div>
            })}
            <button
                class="create-btn"
                on:click=move |_| set_show_template_picker.set(true)
            >"+ New from Template"</button>

            // Template picker modal
            {move || show_template_picker.get().then(|| {
                let templates = store.templates_list.get();
                view! {
                    <div class="modal-overlay">
                        <div class="modal-content">
                            <h3>"Select Template"</h3>
                            {templates.into_iter().map(|t| {
                                let tid = t.id;
                                let name = t.name.clone();
                                view! {
                                    <button
                                        class="template-option"
                                        on:click=move |_| on_create_from_template(tid)
                                    >{name}</button>
                                }
                            }).collect_view()}
                            <button on:click=move |_| set_show_template_picker.set(false)>"Cancel"</button>
                        </div>
                    </div>
                }
            })}

            // Confirm delete modal
            {move || confirm_delete.get().map(|(del_id, del_name)| {
                view! {
                    <div class="modal-overlay">
                        <div class="modal-content">
                            <h3>"Delete Event + S3 Cleanup"</h3>
                            <p>{format!("Delete '{del_name}' and all its S3 chunks?")}</p>
                            <div class="modal-actions">
                                <button class="danger" on:click=move |_| {
                                    spawn_local(async move {
                                        match api::delete_event(del_id).await {
                                            Ok(_) => {
                                                set_confirm_delete.set(None);
                                                if let Ok(evts) = api::list_events().await {
                                                    store.events_list.set(evts);
                                                }
                                            }
                                            Err(e) => set_error.set(Some(e)),
                                        }
                                    });
                                }>"Delete"</button>
                                <button on:click=move |_| set_confirm_delete.set(None)>"Cancel"</button>
                            </div>
                        </div>
                    </div>
                }
            })}

            // Events list
            <div class="event-list">
                {move || store.events_list.get().into_iter().map(|evt| {
                    let id = evt.id;
                    let name = evt.name.clone();
                    let name_for_delete = evt.name.clone();
                    let receiving = evt.receiving_activated;
                    let delivering = evt.delivering_activated;
                    let is_active = receiving || delivering;
                    let created_from = evt.created_from.clone();

                    view! {
                        <div class="event-card">
                            <div class="event-header">
                                <strong>{name}</strong>
                                {created_from.map(|cf| view! {
                                    <span class="created-from-badge">{format!("from: {cf}")}</span>
                                })}
                            </div>
                            <div class="event-status">
                                <span class=if receiving { "badge active" } else { "badge" }>
                                    {if receiving { "Receiving" } else { "Idle" }}
                                </span>
                                <span class=if delivering { "badge active" } else { "badge" }>
                                    {if delivering { "Delivering" } else { "Stopped" }}
                                </span>
                            </div>
                            <div class="event-actions">
                                {if !is_active {
                                    view! {
                                        <button class="danger small" on:click=move |_| {
                                            set_confirm_delete.set(Some((id, name_for_delete.clone())));
                                        }>"Delete + Cleanup"</button>
                                    }.into_any()
                                } else {
                                    view! { <span></span> }.into_any()
                                }}
                            </div>
                        </div>
                    }
                }).collect_view()}
            </div>
        </div>
    }
}
```

The tab navigation in settings should look like:

```rust
let (settings_tab, set_settings_tab) = signal("config".to_string());

view! {
    <div class="settings-tabs">
        <button
            class=move || if settings_tab.get() == "config" { "tab active" } else { "tab" }
            on:click=move |_| set_settings_tab.set("config".to_string())
        >"Config"</button>
        <button
            class=move || if settings_tab.get() == "templates" { "tab active" } else { "tab" }
            on:click=move |_| set_settings_tab.set("templates".to_string())
        >"Templates"</button>
        <button
            class=move || if settings_tab.get() == "events" { "tab active" } else { "tab" }
            on:click=move |_| set_settings_tab.set("events".to_string())
        >"Events"</button>
    </div>
    {move || match settings_tab.get().as_str() {
        "templates" => view! { <super::templates::TemplatesView /> }.into_any(),
        "events" => view! { <EventsManagement /> }.into_any(),
        _ => view! { /* existing settings content */ }.into_any(),
    }}
}
```

- [ ] **Step 3: Check formatting**

Run: `cargo fmt --all --check`

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/settings.rs
git commit -m "feat: add Templates/Events tabs to settings view (#89)"
```

---

### Task 11: E2E Mock API + Playwright Tests

**Files:**
- Modify: `e2e/mock-api.js`
- Modify: `e2e/frontend.spec.ts`

- [ ] **Step 1: Add template mock data and endpoints to mock-api.js**

In `e2e/mock-api.js`, add template mock data alongside existing events/endpoints:

```javascript
let templates = [
  {
    id: 1,
    name: "sunday-service",
    cache_delay_secs: 120,
  },
  {
    id: 2,
    name: "wednesday-study",
    cache_delay_secs: null,
  },
];

let templateEndpoints = {
  1: [1], // sunday-service has YouTube Main
  2: [],
};
```

Add template API endpoints:

```javascript
// Templates CRUD
app.get("/api/v1/templates", (_req, res) => {
  res.json(templates);
});

app.post("/api/v1/templates", (req, res) => {
  const newTemplate = {
    id: templates.length + 1,
    name: req.body.name || "New Template",
    cache_delay_secs: req.body.cache_delay_secs || null,
  };
  templates.push(newTemplate);
  templateEndpoints[newTemplate.id] = [];
  res.status(201).json({ id: newTemplate.id });
});

app.get("/api/v1/templates/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const t = templates.find((t) => t.id === id);
  if (!t) return res.status(404).json({ error: "not found" });
  res.json(t);
});

app.patch("/api/v1/templates/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const t = templates.find((t) => t.id === id);
  if (!t) return res.status(404).json({ error: "not found" });
  if (req.body.name) t.name = req.body.name;
  if (req.body.cache_delay_secs !== undefined)
    t.cache_delay_secs = req.body.cache_delay_secs;
  res.json(t);
});

app.delete("/api/v1/templates/:id", (req, res) => {
  const id = parseInt(req.params.id);
  templates = templates.filter((t) => t.id !== id);
  delete templateEndpoints[id];
  res.status(204).send();
});

app.get("/api/v1/templates/:id/endpoints", (req, res) => {
  const id = parseInt(req.params.id);
  const epIds = templateEndpoints[id] || [];
  const eps = endpoints.filter((e) => epIds.includes(e.id));
  res.json(eps);
});

app.post("/api/v1/templates/:tid/endpoints/:eid", (req, res) => {
  const tid = parseInt(req.params.tid);
  const eid = parseInt(req.params.eid);
  if (!templateEndpoints[tid]) templateEndpoints[tid] = [];
  if (!templateEndpoints[tid].includes(eid)) {
    templateEndpoints[tid].push(eid);
  }
  res.status(201).send();
});

app.delete("/api/v1/templates/:tid/endpoints/:eid", (req, res) => {
  const tid = parseInt(req.params.tid);
  const eid = parseInt(req.params.eid);
  if (templateEndpoints[tid]) {
    templateEndpoints[tid] = templateEndpoints[tid].filter((e) => e !== eid);
  }
  res.status(204).send();
});
```

Also modify the `POST /api/v1/events` handler to support `template_id`:

```javascript
app.post("/api/v1/events", (req, res) => {
  let name;
  let created_from = null;
  if (req.body.template_id) {
    const t = templates.find((t) => t.id === req.body.template_id);
    if (!t) return res.status(404).json({ error: "template not found" });
    const today = new Date().toISOString().split("T")[0];
    name = `${t.name}-${today}`;
    created_from = t.name;
    // Deduplicate
    let suffix = 1;
    let candidate = name;
    while (events.find((e) => e.name === candidate)) {
      suffix++;
      candidate = `${name}-${suffix}`;
    }
    name = candidate;
  } else {
    name = req.body.name || "New Event";
  }

  const newEvent = {
    id: events.length + 1,
    name,
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
    created_from,
  };
  events.push(newEvent);
  eventEndpoints[newEvent.id] = [];
  res.status(201).json({ id: newEvent.id, name: newEvent.name });
});
```

Add `created_from` to existing mock event data:

```javascript
let events = [
  {
    id: 1,
    name: "Sunday Service",
    received_bytes: 52428800,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
    created_from: null,
  },
  // ...
];
```

- [ ] **Step 2: Add Playwright tests for templates**

In `e2e/frontend.spec.ts`, add a new test describe block:

```typescript
test.describe("Templates Management", () => {
  test("templates tab shows template list", async ({ page }) => {
    await page.goto("/");
    // Navigate to settings
    await page.locator(".settings-btn, [data-tab='settings']").click();
    // Click Templates tab
    await page.locator("button:text('Templates')").click();
    await page.waitForTimeout(500);

    // Should show mock templates
    await expect(page.locator(".template-card")).toHaveCount(2);
    await expect(page.locator(".template-card").first()).toContainText(
      "sunday-service",
    );
  });

  test("create template via UI", async ({ page }) => {
    await page.goto("/");
    await page.locator(".settings-btn, [data-tab='settings']").click();
    await page.locator("button:text('Templates')").click();
    await page.waitForTimeout(500);

    // Fill in template name
    await page
      .locator(".template-create input[type='text']")
      .fill("special-event");
    await page.locator(".template-create button").click();
    await page.waitForTimeout(500);

    // Should now have 3 templates
    await expect(page.locator(".template-card")).toHaveCount(3);
  });
});

test.describe("Events Management", () => {
  test("events tab shows event list with created_from", async ({ page }) => {
    await page.goto("/");
    await page.locator(".settings-btn, [data-tab='settings']").click();
    await page.locator("button:text('Events')").click();
    await page.waitForTimeout(500);

    await expect(page.locator(".event-card")).toHaveCount(2);
  });

  test("create event from template via UI", async ({ page }) => {
    await page.goto("/");
    await page.locator(".settings-btn, [data-tab='settings']").click();
    await page.locator("button:text('Events')").click();
    await page.waitForTimeout(500);

    // Click "New from Template"
    await page.locator("button:text('New from Template')").click();
    await page.waitForTimeout(300);

    // Select sunday-service template
    await page.locator(".template-option:text('sunday-service')").click();
    await page.waitForTimeout(500);

    // Should have 3 events now
    await expect(page.locator(".event-card")).toHaveCount(3);
  });

  test("delete event shows confirmation dialog", async ({ page }) => {
    await page.goto("/");
    await page.locator(".settings-btn, [data-tab='settings']").click();
    await page.locator("button:text('Events')").click();
    await page.waitForTimeout(500);

    // Click delete on first event
    await page.locator(".event-card").first().locator("button:text('Delete + Cleanup')").click();
    await page.waitForTimeout(300);

    // Confirmation modal should appear
    await expect(page.locator(".modal-content h3")).toContainText(
      "Delete Event",
    );

    // Confirm deletion
    await page.locator(".modal-content button.danger").click();
    await page.waitForTimeout(500);

    // Should have 1 event now
    await expect(page.locator(".event-card")).toHaveCount(1);
  });
});
```

- [ ] **Step 3: Run E2E tests locally**

Run: `cd e2e && npx playwright test frontend.spec.ts --reporter=line`
Expected: New tests pass (along with existing tests)

- [ ] **Step 4: Commit**

```bash
git add e2e/mock-api.js e2e/frontend.spec.ts
git commit -m "feat: add E2E tests for templates and events management (#89)"
```

---

### Task 12: Push, Monitor CI, Create PR

- [ ] **Step 1: Run local checks**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI**

```bash
gh run list --limit 3
# Wait for completion
gh run view <run-id>
```

All jobs must pass: lint, test, build, E2E frontend, E2E streaming, mutation testing.

- [ ] **Step 4: Create PR**

```bash
gh pr create --title "feat: event template/instance model with S3 cleanup (#89, #90)" --body "$(cat <<'EOF'
## Summary
- Add event templates (presets) stored in DB with dashboard UI management
- Events created from templates get date-based names (e.g., `sunday-service-2026-04-09`)
- Templates and events are fully independent — no FK constraint
- Deleting an event deletes its S3 chunks (bucket cleanup)
- Two-tab UI in settings: Templates + Events management
- Backward compatible: existing events work unchanged with `created_from = NULL`

Closes #89
Closes #90

## Test plan
- [ ] Template CRUD: create, list, update, delete via API and UI
- [ ] Template endpoints: attach/detach endpoints to templates
- [ ] Create event from template: copies name+date, endpoints, cache delay
- [ ] Duplicate name handling: same-day events get `-2`, `-3` suffix
- [ ] Delete event with S3 cleanup: confirmation dialog, S3 objects removed
- [ ] Prevent delete while streaming: returns 409
- [ ] Template independence: delete/modify template doesn't affect events
- [ ] E2E Playwright: templates tab, events tab, create from template, delete
- [ ] All existing E2E tests still pass

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Monitor PR CI run and verify mergeable**

```bash
gh run list --limit 3
gh run view <run-id>
gh api repos/zbynekdrlik/restreamer/pulls/NUMBER --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

---

## Verification

1. **Template CRUD**: API returns correct data, DB persists across restarts
2. **Create from template**: Event gets date name, copied endpoints and delay, `created_from` set
3. **Duplicate naming**: Second event same day gets `-2` suffix
4. **S3 cleanup**: Deleting event removes all `{event.name}/*.bin` objects from S3
5. **Delete protection**: Cannot delete active (streaming) event — returns 409
6. **Independence**: Deleting template doesn't affect events. Modifying template doesn't propagate.
7. **UI**: Templates tab shows CRUD. Events tab shows create-from-template + delete-with-cleanup.
8. **E2E**: All Playwright tests pass including new template/events tests
9. **Backward compat**: Existing events have `created_from = NULL`, work unchanged
