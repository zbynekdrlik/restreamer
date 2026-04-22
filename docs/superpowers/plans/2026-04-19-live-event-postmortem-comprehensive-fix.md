# 2026-04-19 Live-Event Post-Mortem — Comprehensive Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single PR (v0.3.66) that delivers the full spec at `docs/superpowers/specs/2026-04-19-live-event-postmortem-comprehensive-fix-design.md` — persistent `audit_log` + per-endpoint metrics time-series + eight targeted reliability fixes exposed by today's live-event failure.

**Architecture:** Two new DB-backed subsystems (audit_log, delivery_endpoint_metrics) act as the observability backbone. Eight targeted fixes either emit into the audit log or enforce guardrails (RTMP-stable gate, remove-last-endpoint guard, CI deploy gate, SQLite BUSY mitigation, reconnect backoff). All code follows existing repo conventions — sqlx runtime queries (no compile-time macros), idempotent migrations with `IF NOT EXISTS` / guarded `ALTER`, `mpsc` channels for backpressure, axum handlers, Leptos CSR components. Current schema is at V17; new migrations are V18 (audit_log) and V19 (metrics + cursor column).

**Tech Stack:** Rust 2024, axum + sqlx 0.8 SQLite, Leptos 0.7 CSR WASM, Tauri 2, Tokio `broadcast` WS. Existing files the plan touches: `crates/rs-core/src/db/{mod,migrations,upload}.rs`, `crates/rs-api/src/{delivery,delivery_endpoints,delivery_handlers,stream_handlers,lib,state,router,s3_handlers}.rs`, `crates/rs-delivery/src/{endpoint_task,api_handlers}.rs`, `crates/rs-endpoint/src/uploader.rs`, `leptos-ui/src/{ws,store}.rs`, `leptos-ui/src/components/operator_dashboard.rs`, `.github/workflows/ci.yml`.

**Spec:** `docs/superpowers/specs/2026-04-19-live-event-postmortem-comprehensive-fix-design.md`

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `Cargo.toml` line 24, `src-tauri/Cargo.toml` line 3, `src-tauri/tauri.conf.json` line 4, `leptos-ui/Cargo.toml` line 3 | Workspace version | Modify: 0.3.65 → 0.3.66 |
| `crates/rs-core/src/db/migrations.rs` | Schema migrations | Modify: bump `MAX_SCHEMA_VERSION` to 19; add V18 (audit_log) + V19 (metrics + cursor) |
| `crates/rs-core/src/db/mod.rs` | Pool creation | Modify: add `busy_timeout` + `synchronous=NORMAL` pragmas to both `create_pool` and `create_memory_pool` |
| `crates/rs-core/src/db/audit.rs` | DB access for `audit_log` | Create |
| `crates/rs-core/src/db/metrics.rs` | DB access for `delivery_endpoint_metrics` | Create |
| `crates/rs-core/src/audit.rs` | Enums (Severity/Source/Action), `AuditRow`, `record()`, `audit_writer_task` | Create |
| `crates/rs-core/src/models.rs` | WsEvent variants | Modify: add `AuditAppended` |
| `crates/rs-core/src/lib.rs` | Crate module exports | Modify: `pub mod audit` |
| `crates/rs-delivery/src/ffmpeg_reason.rs` | stderr classify + reconnect_floor + pick_last_error_line | Create |
| `crates/rs-delivery/tests/ffmpeg_reason_fixtures/*.txt` | Real stderr captures (16) | Create — extracted from prod DB |
| `crates/rs-delivery/src/audit_ring.rs` | In-memory audit ring + JSONL writer | Create |
| `crates/rs-delivery/src/lib.rs` | Module exports | Modify: add `ffmpeg_reason`, `audit_ring` |
| `crates/rs-delivery/src/endpoint_task.rs` | Use `ffmpeg_reason::classify` + `reconnect_floor`; push audit rows | Modify |
| `crates/rs-delivery/src/api_handlers.rs` | `/api/status` includes `recent_audit` + `next_audit_cursor` | Modify |
| `crates/rs-api/src/audit_handlers.rs` | `GET /api/v1/audit`, `/api/v1/audit/{id}` | Create |
| `crates/rs-api/src/metrics_handlers.rs` | `GET /api/v1/delivery/metrics` | Create |
| `crates/rs-api/src/router.rs` | Mount new handlers | Modify |
| `crates/rs-api/src/state.rs` | Add `rtmp_stable_since` + audit `mpsc::Sender` | Modify |
| `crates/rs-api/src/lib.rs` | Spawn `audit_writer_task`; metrics write every 6 s in broadcast loop; rotation task | Modify |
| `crates/rs-api/src/delivery.rs` | Audit call sites; populate `restart_history`; mirror VPS audit cursor | Modify |
| `crates/rs-api/src/delivery_handlers.rs` | Audit emissions for start/stop | Modify |
| `crates/rs-api/src/delivery_endpoints.rs` | Fix `StartPosition::Live`; remove-last-endpoint guard; audit emissions | Modify |
| `crates/rs-api/src/stream_handlers.rs` | Audit emissions | Modify |
| `crates/rs-api/src/s3_handlers.rs` | Audit emissions; rate-limited error audit | Modify |
| `crates/rs-api/src/handlers.rs` | Audit on config PATCH | Modify |
| `crates/rs-inpoint/src/lib.rs` (or publish event sink) | Audit on RTMP connect/disconnect; signal `rtmp_stable_since` | Modify |
| `crates/rs-endpoint/src/uploader.rs` | Claim-coordinator pattern; audit emissions | Modify |
| `crates/rs-core/src/db/upload.rs` | New helper `pick_next_uploadable_chunks(pool, limit)` | Modify |
| `leptos-ui/src/ws.rs` | Restore ActivityFeed handling + `AuditAppended` + `MetricsSample` | Modify |
| `leptos-ui/src/store.rs` | Restore `activity_feed`; add `audit_feed` + `endpoint_metrics_history` | Modify |
| `leptos-ui/src/components/audit_panel.rs` | Right-side live audit feed | Create |
| `leptos-ui/src/components/endpoint_history.rs` | Sparkline tab | Create |
| `leptos-ui/src/components/endpoint_remove_confirm_modal.rs` | Remove-last-endpoint modal | Create |
| `leptos-ui/src/components/zero_endpoint_banner.rs` | Zero-endpoint warning banner | Create |
| `leptos-ui/src/components/operator_dashboard.rs` | Mount new components; gate start-delivery button | Modify |
| `leptos-ui/src/components/mod.rs` | Export new components | Modify |
| `e2e/audit-panel.spec.ts` | Audit panel shows rows | Create |
| `e2e/remove-last-endpoint-modal.spec.ts` | Modal blocks zero-endpoint state | Create |
| `e2e/start-delivery-rtmp-gate.spec.ts` | Button disabled until RTMP stable ≥15s | Create |
| `e2e/endpoint-history-sparkline.spec.ts` | Sparkline renders | Create |
| `e2e/zero-endpoint-banner.spec.ts` | Banner visible when 0 endpoints active | Create |
| `.github/workflows/ci.yml` | Pre-deploy live-event gate | Modify |

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml` line 24
- Modify: `src-tauri/Cargo.toml` line 3
- Modify: `src-tauri/tauri.conf.json` line 4
- Modify: `leptos-ui/Cargo.toml` line 3

- [ ] **Step 1: Bump `Cargo.toml`**

In `Cargo.toml` line 24 change:
```toml
version = "0.3.65"
```
to:
```toml
version = "0.3.66"
```

- [ ] **Step 2: Bump `src-tauri/Cargo.toml`**

In `src-tauri/Cargo.toml` line 3 change:
```toml
version = "0.3.65"
```
to:
```toml
version = "0.3.66"
```

- [ ] **Step 3: Bump `src-tauri/tauri.conf.json`**

In `src-tauri/tauri.conf.json` line 4 change:
```json
"version": "0.3.65",
```
to:
```json
"version": "0.3.66",
```

- [ ] **Step 4: Bump `leptos-ui/Cargo.toml`**

In `leptos-ui/Cargo.toml` line 3 change:
```toml
version = "0.3.65"
```
to:
```toml
version = "0.3.66"
```

- [ ] **Step 5: Verify format**

Run: `cargo fmt --all --check`
Expected: exit 0, no output.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.66"
```

---

### Task 1: Migrations V18 (audit_log) + V19 (metrics + cursor)

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs` (bump `MAX_SCHEMA_VERSION` from 17 to 19; add two SQL constants + dispatch arms)

- [ ] **Step 1: Write failing migration idempotency test**

In `crates/rs-core/src/db/migration_tests.rs`, append:
```rust
#[tokio::test]
async fn migration_v18_creates_audit_log_idempotent() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    // Rerunning is idempotent.
    crate::db::run_migrations(&pool).await.unwrap();

    // audit_log exists and has expected columns.
    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('audit_log')")
            .fetch_all(&pool)
            .await
            .unwrap();
    for expected in ["id","ts","severity","source","event_id","instance_id","endpoint","action","detail"] {
        assert!(cols.iter().any(|c| c == expected),
            "audit_log missing column {expected}; have {cols:?}");
    }

    // Indexes exist.
    let indexes: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='audit_log'")
            .fetch_all(&pool).await.unwrap();
    for expected in ["idx_audit_ts","idx_audit_event","idx_audit_sev"] {
        assert!(indexes.iter().any(|i| i == expected),
            "audit_log missing index {expected}; have {indexes:?}");
    }
}

#[tokio::test]
async fn migration_v19_creates_metrics_and_cursor_column() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    let cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('delivery_endpoint_metrics')")
            .fetch_all(&pool).await.unwrap();
    for expected in ["id","ts_ms","instance_id","event_id","alias","alive","current_chunk_id","chunks_processed","chunk_delay_secs","bytes_processed_total","ffmpeg_restart_count","delivery_mode"] {
        assert!(cols.iter().any(|c| c == expected),
            "delivery_endpoint_metrics missing column {expected}; have {cols:?}");
    }

    let inst_cols: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info('delivery_instances')")
            .fetch_all(&pool).await.unwrap();
    assert!(inst_cols.iter().any(|c| c == "last_audit_cursor"),
        "delivery_instances missing last_audit_cursor; have {inst_cols:?}");
}

#[tokio::test]
async fn run_migrations_reaches_v19() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    let v: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version),0) FROM schema_version")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(v, 19);
}
```

- [ ] **Step 2: Run the test to verify it fails (no V18/V19 yet)**

Note: per `airuleset`/CLAUDE.md, no local `cargo test`. These tests run on CI. The failure is: `MAX_SCHEMA_VERSION=17`, so tests will fail because no V18 runs and `audit_log` table isn't created. That is the expected RED state.

- [ ] **Step 3: Add migration SQL constants**

Append to `crates/rs-core/src/db/migrations.rs` near the other `MIGRATION_V*_SQL` constants (around line 540):

```rust
const MIGRATION_V18_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    severity    TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    event_id    INTEGER,
    instance_id INTEGER,
    endpoint    TEXT,
    action      TEXT    NOT NULL,
    detail      TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_ts    ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_sev   ON audit_log(severity, ts DESC);
"#;

const MIGRATION_V19_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_endpoint_metrics (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms                 INTEGER NOT NULL,
    instance_id           INTEGER NOT NULL,
    event_id              INTEGER NOT NULL,
    alias                 TEXT    NOT NULL,
    alive                 INTEGER NOT NULL,
    current_chunk_id      INTEGER NOT NULL,
    chunks_processed      INTEGER NOT NULL,
    chunk_delay_secs      REAL    NOT NULL,
    bytes_processed_total INTEGER NOT NULL,
    ffmpeg_restart_count  INTEGER NOT NULL,
    delivery_mode         TEXT
);
CREATE INDEX IF NOT EXISTS idx_dem_event_alias
    ON delivery_endpoint_metrics(event_id, alias, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_dem_ts
    ON delivery_endpoint_metrics(ts_ms DESC);
"#;

/// V19 adds `last_audit_cursor` column to `delivery_instances` idempotently.
async fn migrate_v19(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    execute_sql_statements(tx, MIGRATION_V19_SQL).await?;
    add_column_if_missing(
        tx,
        "delivery_instances",
        "last_audit_cursor",
        "last_audit_cursor INTEGER NOT NULL DEFAULT 0",
    ).await?;
    Ok(())
}
```

- [ ] **Step 4: Bump MAX_SCHEMA_VERSION and add dispatch arms**

In `crates/rs-core/src/db/migrations.rs`:

Change line 16:
```rust
pub const MAX_SCHEMA_VERSION: i32 = 17;
```
to:
```rust
pub const MAX_SCHEMA_VERSION: i32 = 19;
```

In the dispatch match (around line 320), add after the `17 =>` arm:
```rust
            18 => execute_sql_statements(&mut tx, MIGRATION_V18_SQL).await?,
            19 => migrate_v19(&mut tx).await?,
```

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/db/migrations.rs crates/rs-core/src/db/migration_tests.rs
git commit -m "feat(db): V18 audit_log + V19 endpoint metrics + cursor (#120 post-mortem)"
```

---

### Task 2: SQLite pragmas — busy_timeout + synchronous=NORMAL

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`

- [ ] **Step 1: Write failing pragma-applied test**

Append to `crates/rs-core/src/db/tests.rs`:
```rust
#[tokio::test]
async fn create_pool_sets_busy_timeout_and_synchronous() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = crate::db::create_pool(tmp.path()).await.unwrap();

    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(busy_timeout, 5000, "busy_timeout must be 5000 ms");

    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&pool).await.unwrap();
    // NORMAL = 1. FULL = 2. OFF = 0.
    assert_eq!(sync, 1, "synchronous must be NORMAL (1), got {sync}");
}

#[tokio::test]
async fn create_memory_pool_sets_busy_timeout() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(busy_timeout, 5000);
}
```

Add `tempfile` to `[dev-dependencies]` of `crates/rs-core/Cargo.toml` if not already present:
```toml
tempfile = "3"
```

- [ ] **Step 2: Patch `create_pool` and `create_memory_pool`**

In `crates/rs-core/src/db/mod.rs`, replace the two pool-creation functions (lines ~42-70):

```rust
/// Create a SQLite connection pool.
pub async fn create_pool(db_path: &Path) -> Result<SqlitePool> {
    let url = format!("sqlite:{}?mode=rwc", db_path.display());
    let options = SqliteConnectOptions::from_str(&url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_millis(5000))
        .create_if_missing(true)
        .pragma("foreign_keys", "1");

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Create an in-memory SQLite pool for testing.
pub async fn create_memory_pool() -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")?
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_millis(5000))
        .pragma("foreign_keys", "1");

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    Ok(pool)
}
```

- [ ] **Step 3: Verify rustfmt**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs crates/rs-core/Cargo.toml
git commit -m "fix(db): add busy_timeout=5s + synchronous=NORMAL pragmas (#120 post-mortem)"
```

---

### Task 3: Audit module — enums, AuditRow, record, writer task

**Files:**
- Create: `crates/rs-core/src/audit.rs`
- Modify: `crates/rs-core/src/lib.rs` (add `pub mod audit;`)
- Modify: `crates/rs-core/Cargo.toml` (add `dashmap = "6"` if not present)

- [ ] **Step 1: Write failing test for enum serde**

Append to `crates/rs-core/src/audit.rs` (creating file; test first, below production code convention):
```rust
//! Typed audit log with fire-and-forget write API.
//!
//! Callers invoke `record()` which pushes into a bounded `mpsc` channel.
//! A dedicated `audit_writer_task` drains the channel, batches INSERTs
//! into `audit_log`, and broadcasts `WsEvent::AuditAppended` to clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity { Info, Warn, Error, Critical }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Operator, Inpoint, Uploader, Delivery, Vps, Ffmpeg, S3, System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    EventStarted, EventStopped,
    DeliveryStarted, DeliveryStopped,
    EndpointAdded, EndpointRemoved,
    S3Cleared, ConfigChanged,
    RtmpConnected, RtmpDisconnected, RtmpHandshakeFailed,
    VpsCreating, VpsReady, VpsDeleted, VpsUnreachable,
    DeliveryInitSent, DeliveryInitResponse,
    EndpointStarted, EndpointAliveTransition,
    EndpointFfmpegDied, EndpointFfmpegRestartFailed,
    S3UploadFailed, S3FetchFailed,
    RestreamerStarted, MigrationsApplied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub severity: Severity,
    pub source: Source,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: Action,
    pub detail: Value,
    /// Optional pre-set timestamp (used when mirroring VPS rows to preserve their ts).
    /// `None` means "use current wall clock at INSERT".
    pub ts_override: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_serde_snake_case() {
        assert_eq!(serde_json::to_string(&Severity::Info).unwrap(), r#""info""#);
        assert_eq!(serde_json::to_string(&Severity::Critical).unwrap(), r#""critical""#);
        let back: Severity = serde_json::from_str(r#""warn""#).unwrap();
        assert_eq!(back, Severity::Warn);
    }

    #[test]
    fn source_serde_snake_case() {
        assert_eq!(serde_json::to_string(&Source::Vps).unwrap(), r#""vps""#);
        assert_eq!(serde_json::to_string(&Source::Operator).unwrap(), r#""operator""#);
    }

    #[test]
    fn action_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Action::EndpointFfmpegDied).unwrap(),
            r#""endpoint_ffmpeg_died""#
        );
        assert_eq!(
            serde_json::to_string(&Action::RtmpConnected).unwrap(),
            r#""rtmp_connected""#
        );
    }
}
```

- [ ] **Step 2: Add `record()` + rate limiter**

Append to `crates/rs-core/src/audit.rs`:
```rust
/// Rate limiter for noisy audit categories. Keyed by (Action, class-string).
/// Emits at most 1 row per minute per key.
pub struct RateLimiter {
    last: dashmap::DashMap<(Action, String), Instant>,
}

impl RateLimiter {
    pub fn new() -> Self { Self { last: dashmap::DashMap::new() } }

    pub fn allow(&self, action: Action, class: &str) -> bool {
        let key = (action, class.to_string());
        let now = Instant::now();
        let mut allow = true;
        self.last.entry(key).and_modify(|t| {
            if now.duration_since(*t) < Duration::from_secs(60) {
                allow = false;
            } else {
                *t = now;
            }
        }).or_insert(now);
        allow
    }
}

impl Default for RateLimiter {
    fn default() -> Self { Self::new() }
}

/// Push an audit row into the writer channel. Non-blocking.
/// On channel-full, oldest Info rows are dropped; higher severities always kept.
pub fn record(tx: &mpsc::Sender<AuditRow>, row: AuditRow) {
    // try_send drops oldest-on-full; we treat it as best-effort for Info/Warn.
    match tx.try_send(row.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(r)) if matches!(r.severity, Severity::Error | Severity::Critical) => {
            // Error/Critical: spawn a tiny task to send blocking so we don't lose them.
            let tx2 = tx.clone();
            tokio::spawn(async move {
                let _ = tx2.send(r).await;
            });
        }
        Err(_) => { /* drop Info/Warn under pressure */ }
    }
}

#[cfg(test)]
mod record_tests {
    use super::*;

    #[test]
    fn rate_limiter_allows_first_and_blocks_within_minute() {
        let rl = RateLimiter::new();
        assert!(rl.allow(Action::S3UploadFailed, "timeout"));
        assert!(!rl.allow(Action::S3UploadFailed, "timeout"));
        // Different class is independent.
        assert!(rl.allow(Action::S3UploadFailed, "404"));
    }

    #[tokio::test]
    async fn record_try_send_drops_info_on_full_channel() {
        let (tx, mut rx) = mpsc::channel::<AuditRow>(1);
        // Fill channel.
        let mut row = AuditRow {
            severity: Severity::Info, source: Source::System,
            event_id: None, instance_id: None, endpoint: None,
            action: Action::RestreamerStarted, detail: serde_json::json!({}),
            ts_override: None,
        };
        record(&tx, row.clone());
        // Channel full. Info row dropped silently.
        record(&tx, row.clone());
        drop(tx);
        let mut count = 0;
        while rx.recv().await.is_some() { count += 1; }
        assert_eq!(count, 1, "second Info row should have been dropped");
    }
}
```

- [ ] **Step 3: Add `audit_writer_task`**

Append to `crates/rs-core/src/audit.rs`:
```rust
use crate::models::WsEvent;

/// Drains the audit channel, INSERTs rows (batched), broadcasts WS events.
pub async fn audit_writer_task(
    pool: SqlitePool,
    ws_tx: broadcast::Sender<WsEvent>,
    mut rx: mpsc::Receiver<AuditRow>,
) {
    const BATCH_MAX: usize = 32;
    const FLUSH_AFTER: Duration = Duration::from_millis(100);

    let mut buf: Vec<AuditRow> = Vec::with_capacity(BATCH_MAX);
    loop {
        // Wait for at least one row.
        let Some(first) = rx.recv().await else { return };
        buf.push(first);

        // Gather more up to BATCH_MAX or FLUSH_AFTER.
        let deadline = Instant::now() + FLUSH_AFTER;
        while buf.len() < BATCH_MAX {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { break; }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(r)) => buf.push(r),
                _ => break,
            }
        }

        // Insert batch in a single transaction.
        if let Err(e) = crate::db::audit::insert_batch(&pool, &buf, &ws_tx).await {
            tracing::error!("audit batch insert failed: {e}");
        }
        buf.clear();
    }
}
```

- [ ] **Step 4: Export audit module**

In `crates/rs-core/src/lib.rs`, add (alphabetically placed among existing `pub mod …`):
```rust
pub mod audit;
```

- [ ] **Step 5: Add `dashmap` dependency**

In `crates/rs-core/Cargo.toml` `[dependencies]`, ensure:
```toml
dashmap = "6"
```
If already present at a different version, keep existing — compatibility is not a concern (API used is stable since 5.x).

- [ ] **Step 6: Commit**

```bash
git add crates/rs-core/src/audit.rs crates/rs-core/src/lib.rs crates/rs-core/Cargo.toml
git commit -m "feat(audit): typed enums + record + writer task skeleton (#120 post-mortem)"
```

---

### Task 4: `db::audit` module — insert_batch + query

**Files:**
- Create: `crates/rs-core/src/db/audit.rs`
- Modify: `crates/rs-core/src/db/mod.rs` (re-export)

- [ ] **Step 1: Write failing test**

Create `crates/rs-core/src/db/audit_tests.rs`:
```rust
//! Tests for audit_log DB access.

use super::*;
use crate::audit::{Action, AuditRow, Severity, Source};
use tokio::sync::broadcast;

#[tokio::test]
async fn insert_batch_persists_rows_and_broadcasts() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, mut ws_rx) = broadcast::channel(16);

    let rows = vec![
        AuditRow {
            severity: Severity::Info, source: Source::Operator,
            event_id: Some(1), instance_id: None, endpoint: None,
            action: Action::EventStarted,
            detail: serde_json::json!({"name":"test"}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Error, source: Source::Ffmpeg,
            event_id: Some(1), instance_id: Some(42),
            endpoint: Some("YT NLW 4k".to_string()),
            action: Action::EndpointFfmpegDied,
            detail: serde_json::json!({"chunk_id":1436,"reason_class":"youtube_rtmp_closed"}),
            ts_override: None,
        },
    ];
    audit::insert_batch(&pool, &rows, &ws_tx).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2);

    // Broadcast fired twice.
    let ev1 = ws_rx.recv().await.unwrap();
    let ev2 = ws_rx.recv().await.unwrap();
    for ev in [ev1, ev2] {
        matches!(ev, crate::models::WsEvent::AuditAppended { .. });
    }
}

#[tokio::test]
async fn query_filters_event_and_severity() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let (ws_tx, _rx) = broadcast::channel(16);

    let rows = vec![
        AuditRow {
            severity: Severity::Info, source: Source::Operator,
            event_id: Some(1), instance_id: None, endpoint: None,
            action: Action::EventStarted, detail: serde_json::json!({}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Error, source: Source::Ffmpeg,
            event_id: Some(1), instance_id: None, endpoint: None,
            action: Action::EndpointFfmpegDied, detail: serde_json::json!({}),
            ts_override: None,
        },
        AuditRow {
            severity: Severity::Info, source: Source::Operator,
            event_id: Some(2), instance_id: None, endpoint: None,
            action: Action::EventStarted, detail: serde_json::json!({}),
            ts_override: None,
        },
    ];
    audit::insert_batch(&pool, &rows, &ws_tx).await.unwrap();

    // Filter event_id=1 + severity=error.
    let filtered = audit::query(&pool, audit::Filter {
        event_id: Some(1),
        severities: vec!["error".to_string()],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].severity, "error");
    assert_eq!(filtered[0].event_id, Some(1));
}
```

Register this module in `crates/rs-core/src/db/mod.rs`:
```rust
#[cfg(test)]
mod audit_tests;
```

- [ ] **Step 2: Implement `insert_batch` + `query`**

Create `crates/rs-core/src/db/audit.rs`:
```rust
//! `audit_log` DB access.

use crate::audit::AuditRow;
use crate::error::Result;
use crate::models::WsEvent;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use tokio::sync::broadcast;

/// Row as returned from the DB. `detail` is the parsed JSON, not raw text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogRow {
    pub id: i64,
    pub ts: String,
    pub severity: String,
    pub source: String,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: String,
    pub detail: serde_json::Value,
}

/// Filter for `query`. All fields optional.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub severities: Vec<String>,
    pub sources: Vec<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn insert_batch(
    pool: &SqlitePool,
    rows: &[AuditRow],
    ws_tx: &broadcast::Sender<WsEvent>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut inserted_ids: Vec<(i64, &AuditRow)> = Vec::with_capacity(rows.len());

    for row in rows {
        let severity = serde_json::to_string(&row.severity)
            .unwrap_or_else(|_| "\"info\"".into())
            .trim_matches('"').to_string();
        let source = serde_json::to_string(&row.source)
            .unwrap_or_else(|_| "\"system\"".into())
            .trim_matches('"').to_string();
        let action = serde_json::to_string(&row.action)
            .unwrap_or_else(|_| "\"unknown\"".into())
            .trim_matches('"').to_string();
        let detail = row.detail.to_string();

        let id: i64 = if let Some(ts) = &row.ts_override {
            sqlx::query_scalar(
                "INSERT INTO audit_log (ts, severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id"
            )
            .bind(ts)
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        } else {
            sqlx::query_scalar(
                "INSERT INTO audit_log (severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING id"
            )
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        };
        inserted_ids.push((id, row));
    }
    tx.commit().await?;

    // Broadcast post-commit so subscribers see durable state.
    for (id, row) in inserted_ids {
        // Re-read ts for consistency.
        if let Ok(ts) = sqlx::query_scalar::<_, String>(
            "SELECT ts FROM audit_log WHERE id = ?1"
        ).bind(id).fetch_one(pool).await {
            let severity = serde_json::to_string(&row.severity).unwrap_or_default().trim_matches('"').to_string();
            let source = serde_json::to_string(&row.source).unwrap_or_default().trim_matches('"').to_string();
            let action = serde_json::to_string(&row.action).unwrap_or_default().trim_matches('"').to_string();
            let _ = ws_tx.send(WsEvent::AuditAppended {
                id, ts, severity, source,
                event_id: row.event_id, instance_id: row.instance_id,
                endpoint: row.endpoint.clone(), action,
                detail: row.detail.clone(),
            });
        }
    }
    Ok(())
}

pub async fn query(pool: &SqlitePool, f: Filter) -> Result<Vec<AuditLogRow>> {
    let mut sql = String::from("SELECT id, ts, severity, source, event_id, instance_id, endpoint, action, detail FROM audit_log WHERE 1=1");
    let mut binds: Vec<String> = Vec::new();

    if let Some(ev) = f.event_id { sql.push_str(&format!(" AND event_id = ?{}", binds.len()+1)); binds.push(ev.to_string()); }
    if let Some(inst) = f.instance_id { sql.push_str(&format!(" AND instance_id = ?{}", binds.len()+1)); binds.push(inst.to_string()); }
    if let Some(ep) = &f.endpoint { sql.push_str(&format!(" AND endpoint = ?{}", binds.len()+1)); binds.push(ep.clone()); }
    if !f.severities.is_empty() {
        let placeholders: Vec<String> = f.severities.iter().enumerate().map(|(i, _)| format!("?{}", binds.len()+i+1)).collect();
        sql.push_str(&format!(" AND severity IN ({})", placeholders.join(",")));
        binds.extend(f.severities.iter().cloned());
    }
    if !f.sources.is_empty() {
        let placeholders: Vec<String> = f.sources.iter().enumerate().map(|(i, _)| format!("?{}", binds.len()+i+1)).collect();
        sql.push_str(&format!(" AND source IN ({})", placeholders.join(",")));
        binds.extend(f.sources.iter().cloned());
    }
    if let Some(s) = &f.since { sql.push_str(&format!(" AND ts >= ?{}", binds.len()+1)); binds.push(s.clone()); }
    if let Some(u) = &f.until { sql.push_str(&format!(" AND ts <= ?{}", binds.len()+1)); binds.push(u.clone()); }

    sql.push_str(" ORDER BY id DESC");
    sql.push_str(&format!(" LIMIT {}", f.limit.unwrap_or(200).clamp(1, 5000)));
    if let Some(off) = f.offset { sql.push_str(&format!(" OFFSET {}", off.max(0))); }

    let mut q = sqlx::query(&sql);
    for b in &binds { q = q.bind(b); }
    let rows = q.fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| AuditLogRow {
        id: r.get("id"),
        ts: r.get("ts"),
        severity: r.get("severity"),
        source: r.get("source"),
        event_id: r.get("event_id"),
        instance_id: r.get("instance_id"),
        endpoint: r.get("endpoint"),
        action: r.get("action"),
        detail: serde_json::from_str(&r.get::<String, _>("detail")).unwrap_or(serde_json::json!({})),
    }).collect())
}

pub async fn get_by_id(pool: &SqlitePool, id: i64) -> Result<Option<AuditLogRow>> {
    let row = sqlx::query(
        "SELECT id, ts, severity, source, event_id, instance_id, endpoint, action, detail FROM audit_log WHERE id = ?1"
    ).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| AuditLogRow {
        id: r.get("id"), ts: r.get("ts"),
        severity: r.get("severity"), source: r.get("source"),
        event_id: r.get("event_id"), instance_id: r.get("instance_id"),
        endpoint: r.get("endpoint"), action: r.get("action"),
        detail: serde_json::from_str(&r.get::<String, _>("detail")).unwrap_or(serde_json::json!({})),
    }))
}

pub async fn rotate(pool: &SqlitePool, keep_days: i64) -> Result<i64> {
    let res = sqlx::query(
        "DELETE FROM audit_log WHERE ts < datetime('now', ?1)"
    ).bind(format!("-{keep_days} days")).execute(pool).await?;
    Ok(res.rows_affected() as i64)
}
```

- [ ] **Step 2.5: Re-export module**

In `crates/rs-core/src/db/mod.rs` add:
```rust
pub mod audit;
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-core/src/db/audit.rs crates/rs-core/src/db/audit_tests.rs crates/rs-core/src/db/mod.rs
git commit -m "feat(audit): db::audit insert_batch + query + rotate (#120 post-mortem)"
```

---

### Task 5: WsEvent::AuditAppended + MetricsSample variants

**Files:**
- Modify: `crates/rs-core/src/models.rs`
- Modify: `leptos-ui/src/ws.rs`

- [ ] **Step 1: Locate existing WsEvent enum and add variants**

In `crates/rs-core/src/models.rs`, find `enum WsEvent` (around line 175). After the existing `ActivityFeed { … }` variant, add:
```rust
    AuditAppended {
        id: i64,
        ts: String,
        severity: String,
        source: String,
        event_id: Option<i64>,
        instance_id: Option<i64>,
        endpoint: Option<String>,
        action: String,
        detail: serde_json::Value,
    },
    MetricsSample {
        ts_ms: i64,
        event_id: i64,
        instance_id: i64,
        alias: String,
        chunk_delay_secs: f64,
        current_chunk_id: i64,
        chunks_processed: i64,
        alive: bool,
    },
```

Update any serde `#[serde(tag = "...")]` if present and any match-all test by adding these variants to the test fixture. If `WsEvent` has a `#[cfg(test)] fn example_of_each()` (check around line 377), add sample instances.

- [ ] **Step 2: Mirror into `leptos-ui/src/ws.rs`**

In `leptos-ui/src/ws.rs`, find the Leptos-side `enum WsEvent` (around line 60). Add the same variants:
```rust
    AuditAppended {
        id: i64,
        ts: String,
        severity: String,
        source: String,
        event_id: Option<i64>,
        instance_id: Option<i64>,
        endpoint: Option<String>,
        action: String,
        detail: serde_json::Value,
    },
    MetricsSample {
        ts_ms: i64,
        event_id: i64,
        instance_id: i64,
        alias: String,
        chunk_delay_secs: f64,
        current_chunk_id: i64,
        chunks_processed: i64,
        alive: bool,
    },
```

- [ ] **Step 3: Add stub handlers in match expression**

In `leptos-ui/src/ws.rs` line ~318 (the match), replace the hardcoded-ignore for `ActivityFeed` (the regression) AND add arms for the new variants:

BEFORE:
```rust
        WsEvent::ActivityFeed { .. } => {
            // Backend still sends these events; we just ignore them now.
        }
```

AFTER:
```rust
        WsEvent::ActivityFeed { timestamp, severity, message, source } => {
            store.activity_feed.update(|feed| {
                feed.push(crate::store::ActivityEntry {
                    timestamp, severity, message, source,
                });
                if feed.len() > 200 { feed.remove(0); }
            });
        }
        WsEvent::AuditAppended {
            id, ts, severity, source, event_id, instance_id, endpoint, action, detail,
        } => {
            store.audit_feed.update(|feed| {
                feed.push(crate::store::AuditEntry {
                    id, ts, severity, source, event_id, instance_id, endpoint, action, detail,
                });
                if feed.len() > 500 { feed.remove(0); }
            });
        }
        WsEvent::MetricsSample {
            ts_ms, event_id, instance_id, alias, chunk_delay_secs,
            current_chunk_id, chunks_processed, alive,
        } => {
            store.endpoint_metrics_history.update(|hist| {
                let entry = crate::store::MetricsSample {
                    ts_ms, event_id, instance_id, alias: alias.clone(),
                    chunk_delay_secs, current_chunk_id, chunks_processed, alive,
                };
                hist.entry(alias).or_default().push(entry);
            });
        }
```

(The `store.audit_feed`, `AuditEntry`, `store.endpoint_metrics_history`, and `MetricsSample` types will be added in Task 19 — this code compiles only after that task lands.)

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/models.rs leptos-ui/src/ws.rs
git commit -m "feat(ws): WsEvent::AuditAppended + MetricsSample; restore ActivityFeed (#120 post-mortem)"
```

---

### Task 6: Audit API handlers — GET /api/v1/audit + /{id}

**Files:**
- Create: `crates/rs-api/src/audit_handlers.rs`
- Modify: `crates/rs-api/src/router.rs` (mount)
- Modify: `crates/rs-api/src/lib.rs` (`pub mod audit_handlers;`)

- [ ] **Step 1: Write failing integration test**

Append to `crates/rs-api/src/router_tests.rs` (or create a new file `crates/rs-api/tests/audit_api.rs` following the existing integration-test pattern of `crates/rs-api/tests/api_integration.rs`). For consistency with existing pattern use `router_tests`:

```rust
#[tokio::test]
async fn audit_list_returns_empty_on_fresh_db() {
    let (app, _state) = build_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/audit")
        .body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["rows"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn audit_list_returns_inserted_rows() {
    let (app, state) = build_test_app().await;
    // Insert a row directly.
    sqlx::query("INSERT INTO audit_log (severity, source, action, detail) VALUES ('info','operator','event_started','{\"n\":1}')")
        .execute(&state.pool).await.unwrap();

    let resp = app.clone().oneshot(Request::builder().uri("/api/v1/audit").body(Body::empty()).unwrap()).await.unwrap();
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["rows"].as_array().unwrap().len(), 1);
    assert_eq!(json["rows"][0]["action"], "event_started");
}

#[tokio::test]
async fn audit_get_by_id_returns_row() {
    let (app, state) = build_test_app().await;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO audit_log (severity, source, action, detail) VALUES ('error','ffmpeg','endpoint_ffmpeg_died','{\"reason_class\":\"youtube_rtmp_closed\"}') RETURNING id"
    ).fetch_one(&state.pool).await.unwrap();

    let resp = app.clone().oneshot(Request::builder().uri(&format!("/api/v1/audit/{id}")).body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["id"], id);
    assert_eq!(json["detail"]["reason_class"], "youtube_rtmp_closed");
}

#[tokio::test]
async fn audit_get_by_id_returns_404_on_missing() {
    let (app, _) = build_test_app().await;
    let resp = app.oneshot(Request::builder().uri("/api/v1/audit/9999999").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 2: Implement handlers**

Create `crates/rs-api/src/audit_handlers.rs`:
```rust
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rs_core::db::audit::{self, Filter};
use serde::Deserialize;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub event_id: Option<i64>,
    #[serde(default)]
    pub instance_id: Option<i64>,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Comma-separated: "info,warn,error,critical"
    #[serde(default)]
    pub severity: Option<String>,
    /// Comma-separated sources
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub until: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
}

pub async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> impl IntoResponse {
    let severities = q.severity
        .as_deref().unwrap_or("")
        .split(',').filter(|s| !s.is_empty())
        .map(|s| s.to_string()).collect();
    let sources = q.source
        .as_deref().unwrap_or("")
        .split(',').filter(|s| !s.is_empty())
        .map(|s| s.to_string()).collect();

    let filter = Filter {
        event_id: q.event_id,
        instance_id: q.instance_id,
        endpoint: q.endpoint,
        severities, sources,
        since: q.since, until: q.until,
        limit: q.limit, offset: q.offset,
    };

    match audit::query(&state.pool, filter).await {
        Ok(rows) => {
            let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
                .fetch_one(&state.pool).await.unwrap_or(0);
            Json(serde_json::json!({ "rows": rows, "total": total })).into_response()
        }
        Err(e) => {
            tracing::error!("audit list failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "audit list failed").into_response()
        }
    }
}

pub async fn get_one(Path(id): Path<i64>, State(state): State<AppState>) -> impl IntoResponse {
    match audit::get_by_id(&state.pool, id).await {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!("audit get_by_id failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "audit get failed").into_response()
        }
    }
}
```

- [ ] **Step 3: Mount in router**

In `crates/rs-api/src/router.rs`, add to the router builder near the other `/api/v1/...` routes:
```rust
        .route("/api/v1/audit", axum::routing::get(crate::audit_handlers::list))
        .route("/api/v1/audit/{id}", axum::routing::get(crate::audit_handlers::get_one))
```

In `crates/rs-api/src/lib.rs`, alongside the other module declarations:
```rust
pub mod audit_handlers;
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/audit_handlers.rs crates/rs-api/src/router.rs crates/rs-api/src/lib.rs crates/rs-api/src/router_tests.rs
git commit -m "feat(api): GET /api/v1/audit list+get_one (#120 post-mortem)"
```

---

### Task 7: ffmpeg_reason module with real stderr fixtures

**Files:**
- Create: `crates/rs-delivery/src/ffmpeg_reason.rs`
- Create: `crates/rs-delivery/tests/ffmpeg_reason_fixtures/*.txt` (16 files extracted from prod DB)
- Modify: `crates/rs-delivery/src/lib.rs`

- [ ] **Step 1: Extract real stderr fixtures from prod DB**

On stream.lan (via MCP `win-stream-snv__Shell`), run:
```powershell
cd "C:\ProgramData\Restreamer"
$out = "C:\Users\newlevel\Downloads\ffmpeg_fixtures"
New-Item -ItemType Directory -Force -Path $out | Out-Null

$ids = .\sqlite3.exe restreamer.db "SELECT id FROM delivery_restart_log WHERE instance_id=654 ORDER BY id;"
foreach ($id in $ids.Trim().Split("`n")) {
    if ($id -match '^\d+$') {
        $stderr = .\sqlite3.exe restreamer.db "SELECT stderr_tail FROM delivery_restart_log WHERE id=$id;"
        # Name pattern: {id}_{alias_sanitised}_{reason_hint}.txt
        $alias_raw = .\sqlite3.exe restreamer.db "SELECT alias FROM delivery_restart_log WHERE id=$id;"
        $alias = $alias_raw -replace '[^a-zA-Z0-9]', '_'
        # Extract the last 4 KB (fixtures must be small enough to grep)
        $tail = $stderr.Substring([Math]::Max(0, $stderr.Length - 4000))
        Set-Content -Path "$out\${id}_${alias}.txt" -Value $tail -NoNewline
    }
}
Get-ChildItem $out | Select-Object Name,Length
```

Then `FileDownload` each file from `$out` to local path `crates/rs-delivery/tests/ffmpeg_reason_fixtures/`.

Alternative if MCP `FileDownload` isn't available: base64-encode via Shell and decode locally.

- [ ] **Step 2: Write failing classification test**

Create `crates/rs-delivery/tests/ffmpeg_reason_integration.rs`:
```rust
use rs_delivery::ffmpeg_reason::{classify, ReasonClass};
use std::fs;

fn read_fixture(name: &str) -> String {
    let path = format!("tests/ffmpeg_reason_fixtures/{name}");
    fs::read_to_string(&path).unwrap_or_else(|_| panic!("fixture not found: {path}"))
}

/// From today's event, first-wave restart of Control Stream SNV (YT_RTMP):
/// stderr tail contains "Error submitting a packet to the muxer: Broken pipe".
#[test]
fn classify_youtube_rtmp_broken_pipe() {
    let stderr = read_fixture("14_Control_Stream_SNV.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_yt_nlch_4k() {
    let stderr = read_fixture("15_YT_NLCH_4K.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_yt_nlw_4k() {
    let stderr = read_fixture("16_YT_NLW_4k.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_facebook_tls_fatal_alert() {
    // Wave 3: FB-Zbynek id=19 stderr contains "TLS fatal alert" / "session has been invalidated"
    let stderr = read_fixture("19_FB_Zbynek.txt");
    assert_eq!(classify("FB", &stderr), ReasonClass::FacebookTlsInvalidated);
}

#[test]
fn classify_facebook_tls_fatal_alert_newlevel() {
    let stderr = read_fixture("23_FB_NewLevel.txt");
    assert_eq!(classify("FB", &stderr), ReasonClass::FacebookTlsInvalidated);
}

#[test]
fn classify_unknown_empty_stderr() {
    assert_eq!(classify("YT_RTMP", ""), ReasonClass::Unknown);
}

#[test]
fn classify_process_killed_marker() {
    assert_eq!(
        classify("YT_RTMP", "some random lines\nrs-delivery: killed\nlast line"),
        ReasonClass::ProcessKilled
    );
}

#[test]
fn classify_invalid_input() {
    assert_eq!(
        classify("YT_RTMP", "Invalid data found when processing input"),
        ReasonClass::InvalidInput
    );
}
```

- [ ] **Step 3: Implement classify + reconnect_floor + pick_last_error_line**

Create `crates/rs-delivery/src/ffmpeg_reason.rs`:
```rust
//! Parse ffmpeg stderr tail into a ReasonClass and decide reconnect backoff.

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonClass {
    YoutubeRtmpClosed,
    FacebookTlsInvalidated,
    RemoteBrokenPipe,
    NetworkTimeout,
    InvalidInput,
    S3FetchError,
    ProcessKilled,
    Unknown,
}

/// Classify the last portion of ffmpeg stderr into a reason class.
///
/// `service_type` is one of "YT_RTMP", "YT_HLS", "FB", "CUSTOM_RTMP", etc.
pub fn classify(service_type: &str, stderr_tail: &str) -> ReasonClass {
    // Cheap: look at the last 4 KB only.
    let start = stderr_tail.len().saturating_sub(4096);
    let tail = &stderr_tail[start..];

    if tail.contains("rs-delivery: killed") { return ReasonClass::ProcessKilled; }

    if tail.contains("TLS fatal alert")
        || tail.contains("session has been invalidated") {
        return ReasonClass::FacebookTlsInvalidated;
    }

    if tail.contains("Error submitting a packet to the muxer: Broken pipe")
        || tail.contains("IO error: Broken pipe")
        || tail.contains("Error writing trailer: Broken pipe") {
        return if service_type.starts_with("YT_") {
            ReasonClass::YoutubeRtmpClosed
        } else {
            ReasonClass::RemoteBrokenPipe
        };
    }

    if tail.contains("Connection timed out") { return ReasonClass::NetworkTimeout; }
    if tail.contains("Invalid data found") || tail.contains("No start code") {
        return ReasonClass::InvalidInput;
    }
    if tail.contains("rs-delivery: S3 fetch failed") { return ReasonClass::S3FetchError; }

    ReasonClass::Unknown
}

/// Minimum wait before next restart, given reason + consecutive count in this class.
pub fn reconnect_floor(class: ReasonClass, consecutive: u32) -> Duration {
    use ReasonClass::*;
    match class {
        // Never restart — caller suppresses.
        ProcessKilled => Duration::from_secs(u64::MAX),
        YoutubeRtmpClosed | FacebookTlsInvalidated | RemoteBrokenPipe => {
            // 30s * 2^consecutive, capped at 5 min.
            let base: u64 = 30;
            let mul = 2u64.saturating_pow(consecutive.min(10));
            Duration::from_secs(base.saturating_mul(mul).min(300))
        }
        NetworkTimeout => Duration::from_secs(10),
        InvalidInput => Duration::from_secs(1),
        S3FetchError => Duration::from_secs(5),
        Unknown => Duration::from_secs(15),
    }
}

/// Pick a single display-worthy line from stderr tail.
/// Skips progress lines (size=...time=...) and banner lines (ffmpeg version...).
pub fn pick_last_error_line(stderr_tail: &str) -> Option<String> {
    stderr_tail.lines().rev()
        .filter(|l| {
            let l = l.trim();
            !l.is_empty()
                && !l.starts_with("size=")
                && !l.starts_with("frame=")
                && !l.starts_with("ffmpeg version ")
                && !l.starts_with("  built with ")
                && !l.starts_with("  configuration: ")
                && !l.starts_with("  lib")
        })
        .find(|l| {
            let l = l.to_ascii_lowercase();
            l.contains("error") || l.contains("broken pipe") || l.contains("fatal")
                || l.contains("invalid") || l.contains("failed") || l.contains("timeout")
        })
        .map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_floor_remote_close_starts_at_30s() {
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 0), Duration::from_secs(30));
    }

    #[test]
    fn reconnect_floor_remote_close_doubles_and_caps() {
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 1), Duration::from_secs(60));
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 2), Duration::from_secs(120));
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 3), Duration::from_secs(240));
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 10), Duration::from_secs(300));
        assert_eq!(reconnect_floor(ReasonClass::YoutubeRtmpClosed, 100), Duration::from_secs(300));
    }

    #[test]
    fn reconnect_floor_network_timeout_fixed_10s() {
        assert_eq!(reconnect_floor(ReasonClass::NetworkTimeout, 0), Duration::from_secs(10));
        assert_eq!(reconnect_floor(ReasonClass::NetworkTimeout, 5), Duration::from_secs(10));
    }

    #[test]
    fn reconnect_floor_process_killed_infinite() {
        assert_eq!(reconnect_floor(ReasonClass::ProcessKilled, 0), Duration::from_secs(u64::MAX));
    }

    #[test]
    fn pick_last_error_line_skips_progress() {
        let s = "size= 1234kB time=00:00:10 bitrate=1000kbits/s\n\
                 [aost#0:1/copy] Error submitting a packet to the muxer: Broken pipe\n\
                 size= 1235kB time=00:00:11 bitrate=999kbits/s";
        assert_eq!(
            pick_last_error_line(s).unwrap(),
            "[aost#0:1/copy] Error submitting a packet to the muxer: Broken pipe"
        );
    }

    #[test]
    fn pick_last_error_line_none_when_no_error() {
        let s = "size= 1kB time=00:00:01 bitrate=0";
        assert_eq!(pick_last_error_line(s), None);
    }
}
```

- [ ] **Step 4: Export from lib.rs**

In `crates/rs-delivery/src/lib.rs`, add:
```rust
pub mod ffmpeg_reason;
```

- [ ] **Step 5: Commit (including fixture .txt files)**

```bash
git add crates/rs-delivery/src/ffmpeg_reason.rs \
        crates/rs-delivery/tests/ffmpeg_reason_integration.rs \
        crates/rs-delivery/tests/ffmpeg_reason_fixtures/*.txt \
        crates/rs-delivery/src/lib.rs
git commit -m "feat(delivery): ffmpeg_reason classify + backoff with real stderr fixtures (#120 post-mortem)"
```

---

### Task 8: Integrate reconnect_floor into endpoint_task

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task.rs`

- [ ] **Step 1: Write failing test in endpoint_task_backoff_tests.rs**

Append to `crates/rs-delivery/src/endpoint_task_backoff_tests.rs`:
```rust
use crate::ffmpeg_reason::{ReasonClass, reconnect_floor};
use std::time::Duration;

#[test]
fn backoff_uses_reconnect_floor_for_youtube_broken_pipe() {
    // First failure of YT RTMP: must wait 30s before retry.
    let floor = reconnect_floor(ReasonClass::YoutubeRtmpClosed, 0);
    assert_eq!(floor, Duration::from_secs(30));
}

#[test]
fn backoff_resets_consecutive_on_class_change() {
    // Simulated: YT broken-pipe × 2, then NetworkTimeout should reset.
    // The endpoint_task itself is tested via state-machine helpers below.
    // (Integration verified by inspecting EndpointRestartState struct.)
    let state = crate::endpoint_task::EndpointRestartState::new();
    let s = state.advance(ReasonClass::YoutubeRtmpClosed);
    assert_eq!(s.consecutive_same_class, 1);
    let s = s.advance(ReasonClass::YoutubeRtmpClosed);
    assert_eq!(s.consecutive_same_class, 2);
    let s = s.advance(ReasonClass::NetworkTimeout);
    assert_eq!(s.consecutive_same_class, 1);
}
```

- [ ] **Step 2: Add `EndpointRestartState` + integrate classify() in spawn-loop**

In `crates/rs-delivery/src/endpoint_task.rs`, add near the top-level types:

```rust
use crate::ffmpeg_reason::{self, ReasonClass};

#[derive(Debug, Clone, Copy)]
pub struct EndpointRestartState {
    pub consecutive_same_class: u32,
    pub last_class: Option<ReasonClass>,
}

impl EndpointRestartState {
    pub fn new() -> Self { Self { consecutive_same_class: 0, last_class: None } }

    /// Called after each ffmpeg death. Returns new state.
    pub fn advance(self, class: ReasonClass) -> Self {
        if self.last_class == Some(class) {
            Self { consecutive_same_class: self.consecutive_same_class + 1, last_class: Some(class) }
        } else {
            Self { consecutive_same_class: 1, last_class: Some(class) }
        }
    }
}

impl Default for EndpointRestartState {
    fn default() -> Self { Self::new() }
}
```

In the existing ffmpeg-restart loop (wherever `tokio::time::sleep(backoff)` is called — search for that pattern), replace the naive backoff calculation with:
```rust
// OLD (naive exponential from 1s):
// let backoff = Duration::from_secs(2u64.saturating_pow(consecutive).min(60));

// NEW:
let class = ffmpeg_reason::classify(&service_type, &stderr_tail);
restart_state = restart_state.advance(class);
let backoff = ffmpeg_reason::reconnect_floor(class, restart_state.consecutive_same_class.saturating_sub(1));
if backoff == Duration::from_secs(u64::MAX) {
    // ProcessKilled: do not restart.
    tracing::info!(alias = %alias, "endpoint intentionally killed; not restarting");
    break;
}
tracing::info!(alias = %alias, ?class, consecutive = restart_state.consecutive_same_class, ?backoff, "ffmpeg restart scheduled");
tokio::time::sleep(backoff).await;
```

Also capture the row for `delivery_restart_log` with the new `reason` string — use the serde serialization of `ReasonClass`:
```rust
let reason_str = serde_json::to_string(&class).unwrap_or_default().trim_matches('"').to_string();
// Pass reason_str into the existing insert_delivery_restart_log(... reason=reason_str ...) call.
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/endpoint_task_backoff_tests.rs
git commit -m "feat(delivery): endpoint_task uses ffmpeg_reason classify + reconnect_floor (#120 post-mortem)"
```

---

### Task 9: audit_ring in rs-delivery + /api/status extension

**Files:**
- Create: `crates/rs-delivery/src/audit_ring.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (exports + spawn JSONL writer in init)
- Modify: `crates/rs-delivery/src/api_handlers.rs` (`/api/status` returns `recent_audit` + `next_audit_cursor`)

- [ ] **Step 1: Write failing test for audit_ring**

Create `crates/rs-delivery/src/audit_ring_tests.rs` and register with `#[cfg(test)] mod audit_ring_tests;` in `lib.rs`:
```rust
use super::audit_ring::{AuditRing, RingRow};
use rs_core::audit::{Action, Severity, Source};

#[test]
fn ring_since_returns_rows_after_cursor() {
    let ring = AuditRing::new(500);
    let a = ring.push_ts("t1".into(), Severity::Info, Source::Vps, Some("yt".into()), Action::EndpointStarted, serde_json::json!({}));
    let b = ring.push_ts("t2".into(), Severity::Warn, Source::Ffmpeg, Some("yt".into()), Action::EndpointFfmpegDied, serde_json::json!({}));

    let (rows, cursor) = ring.since(0);
    assert_eq!(rows.len(), 2);
    assert_eq!(cursor, b.id);

    let (rows, _) = ring.since(a.id);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, b.id);

    let (rows, _) = ring.since(b.id);
    assert_eq!(rows.len(), 0);
}

#[test]
fn ring_drops_oldest_when_cap_reached() {
    let ring = AuditRing::new(3);
    let r1 = ring.push_ts("t1".into(), Severity::Info, Source::Vps, None, Action::EndpointStarted, serde_json::json!({}));
    let r2 = ring.push_ts("t2".into(), Severity::Info, Source::Vps, None, Action::EndpointStarted, serde_json::json!({}));
    let r3 = ring.push_ts("t3".into(), Severity::Info, Source::Vps, None, Action::EndpointStarted, serde_json::json!({}));
    let r4 = ring.push_ts("t4".into(), Severity::Info, Source::Vps, None, Action::EndpointStarted, serde_json::json!({}));

    let (rows, _) = ring.since(0);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id, r2.id, "oldest dropped");
    assert_eq!(rows[2].id, r4.id);
    let _ = r1;
}
```

- [ ] **Step 2: Implement `audit_ring`**

Create `crates/rs-delivery/src/audit_ring.rs`:
```rust
//! In-memory audit ring for rs-delivery. Last N rows kept + optional JSONL append.

use parking_lot::Mutex;
use rs_core::audit::{Action, Severity, Source};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingRow {
    pub id: i64,
    pub ts: String,
    pub severity: Severity,
    pub source: Source,
    pub endpoint: Option<String>,
    pub action: Action,
    pub detail: serde_json::Value,
}

pub struct AuditRing {
    cap: usize,
    rows: Mutex<VecDeque<RingRow>>,
    next_id: AtomicI64,
    jsonl_path: Mutex<Option<std::path::PathBuf>>,
}

impl AuditRing {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap, rows: Mutex::new(VecDeque::with_capacity(cap)),
            next_id: AtomicI64::new(1),
            jsonl_path: Mutex::new(None),
        })
    }

    pub fn set_jsonl_path<P: Into<std::path::PathBuf>>(&self, p: P) {
        *self.jsonl_path.lock() = Some(p.into());
    }

    pub fn push(
        &self,
        severity: Severity, source: Source,
        endpoint: Option<String>, action: Action,
        detail: serde_json::Value,
    ) -> RingRow {
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        self.push_ts(ts, severity, source, endpoint, action, detail)
    }

    pub fn push_ts(
        &self, ts: String,
        severity: Severity, source: Source,
        endpoint: Option<String>, action: Action,
        detail: serde_json::Value,
    ) -> RingRow {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let row = RingRow { id, ts, severity, source, endpoint, action, detail };
        let mut rows = self.rows.lock();
        if rows.len() >= self.cap { rows.pop_front(); }
        rows.push_back(row.clone());

        // Best-effort JSONL append.
        if let Some(p) = self.jsonl_path.lock().clone() {
            if let Ok(line) = serde_json::to_string(&row) {
                use std::io::Write;
                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&p) {
                    let _ = writeln!(f, "{line}");
                }
            }
        }
        row
    }

    /// Return rows with id > cursor and the new cursor (= largest id returned, or input cursor if none).
    pub fn since(&self, cursor: i64) -> (Vec<RingRow>, i64) {
        let rows = self.rows.lock();
        let filtered: Vec<RingRow> = rows.iter().filter(|r| r.id > cursor).cloned().collect();
        let new_cursor = filtered.last().map(|r| r.id).unwrap_or(cursor);
        (filtered, new_cursor)
    }
}
```

Add `parking_lot = "0.12"` and `chrono = { version = "0.4", features = ["serde","clock"] }` to `crates/rs-delivery/Cargo.toml` if not present.

- [ ] **Step 3: Extend /api/status response**

In `crates/rs-delivery/src/api_handlers.rs`, find the struct that serializes to the `/api/status` response. Add:
```rust
#[derive(Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
    pub endpoint_count: usize,
    pub endpoints: Vec<EndpointStatus>,
    #[serde(default)]
    pub recent_audit: Vec<rs_delivery::audit_ring::RingRow>,
    #[serde(default)]
    pub next_audit_cursor: i64,
}
```

Modify the handler to accept `?since=<i64>` and to consult the app's `Arc<AuditRing>`:
```rust
use axum::extract::Query;
#[derive(serde::Deserialize)]
pub struct StatusQuery {
    #[serde(default)]
    pub since: Option<i64>,
}

pub async fn status(
    State(state): State<AppState>,
    Query(q): Query<StatusQuery>,
) -> impl IntoResponse {
    let (recent_audit, next_audit_cursor) = state.audit_ring.since(q.since.unwrap_or(0));
    // … existing code that builds endpoints etc. …
    Json(StatusResponse {
        status: "ok",
        endpoint_count, endpoints,
        recent_audit, next_audit_cursor,
    })
}
```

(Precise struct and state shape will vary — match the existing file's conventions. If the file is large, prefer minimal-delta edits.)

- [ ] **Step 4: Wire ring into rs-delivery app state**

In the rs-delivery crate's app-state struct (search for `AppState` in `crates/rs-delivery/src/*`), add:
```rust
pub audit_ring: Arc<rs_delivery::audit_ring::AuditRing>,
```

Initialize it in main (or wherever `AppState::new` is called):
```rust
let audit_ring = rs_delivery::audit_ring::AuditRing::new(500);
audit_ring.set_jsonl_path("/var/log/rs-delivery/audit.jsonl");
```

- [ ] **Step 5: Emit audit rows from endpoint_task at death + restart-failed points**

In `crates/rs-delivery/src/endpoint_task.rs`, wherever ffmpeg dies (near the `delivery_restart_log` write site in Task 8):
```rust
audit_ring.push(
    Severity::Error, Source::Ffmpeg,
    Some(alias.clone()),
    Action::EndpointFfmpegDied,
    serde_json::json!({
        "alias": alias,
        "chunk_id": last_chunk_id,
        "lifetime_secs": lifetime.as_secs(),
        "reason_class": class,
        "stderr_last_error_line": ffmpeg_reason::pick_last_error_line(&stderr_tail),
        "backoff_secs": backoff.as_secs(),
    }),
);
```

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/audit_ring.rs \
        crates/rs-delivery/src/audit_ring_tests.rs \
        crates/rs-delivery/src/api_handlers.rs \
        crates/rs-delivery/src/endpoint_task.rs \
        crates/rs-delivery/src/lib.rs \
        crates/rs-delivery/Cargo.toml
git commit -m "feat(delivery): audit_ring + /api/status recent_audit + ffmpeg death audit (#120 post-mortem)"
```

---

### Task 10: Host-side VPS audit cursor mirroring

**Files:**
- Modify: `crates/rs-api/src/delivery.rs`

- [ ] **Step 1: Write failing test** — best verified in E2E; no unit test (controls an HTTP call to a real rs-delivery). Instead add an integration test using a wiremock-style axum server. Create `crates/rs-api/tests/vps_audit_mirror.rs`:
```rust
// Spin a fake rs-delivery axum server returning a canned status with recent_audit,
// call poll_delivery_metrics-equivalent, assert rows inserted in host audit_log
// with source='vps' and instance_id matches, and cursor advanced.
//
// Skeleton — full test code body included directly (no "similar to"):

use axum::{routing::get, Router, Json, extract::Query};
use rs_core::db;
use serde::Deserialize;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct Q { #[serde(default)] since: Option<i64> }

#[tokio::test]
async fn vps_audit_mirror_inserts_rows_and_advances_cursor() {
    // Fake rs-delivery server returning two audit rows, ids 1 and 2.
    let served = Arc::new(AtomicI64::new(0));
    let served2 = Arc::clone(&served);
    let app = Router::new().route("/api/status", get(move |Query(q): Query<Q>| {
        let served = Arc::clone(&served2);
        async move {
            let since = q.since.unwrap_or(0);
            served.fetch_add(1, Ordering::Relaxed);
            let rows = if since < 2 {
                serde_json::json!([
                    {"id":1,"ts":"2026-04-19T07:10:00.000Z","severity":"info","source":"vps","endpoint":"yt1","action":"endpoint_started","detail":{}},
                    {"id":2,"ts":"2026-04-19T07:10:05.000Z","severity":"error","source":"ffmpeg","endpoint":"yt1","action":"endpoint_ffmpeg_died","detail":{"reason_class":"youtube_rtmp_closed"}}
                ])
            } else { serde_json::json!([]) };
            Json(serde_json::json!({
                "status":"ok","endpoint_count":1,
                "endpoints":[{"alias":"yt1","alive":true,"current_chunk_id":100,"bytes_processed_total":1000,"chunks_processed":100,"ffmpeg_restart_count":0,"consecutive_chunk_misses":0,"consecutive_ffmpeg_failures":0,"restart_history":[],"delivery_mode":"normal"}],
                "recent_audit": rows,
                "next_audit_cursor": 2
            }))
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    // Set up host DB with a delivery_instance pointing at the fake server.
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "t").await.unwrap();
    let inst_id = db::create_delivery_instance(&pool, 1, "fake", &addr.ip().to_string(), "cx22", Some(event_id), "tok").await.unwrap();
    // Patch ipv4 to include port (helper only accepts IP; use raw SQL):
    sqlx::query("UPDATE delivery_instances SET ipv4 = ?1 WHERE id = ?2")
        .bind(format!("{}:{}", addr.ip(), addr.port())).bind(inst_id)
        .execute(&pool).await.unwrap();

    // Function under test: rs_api::delivery::mirror_vps_audit (new helper added by this task).
    rs_api::delivery::mirror_vps_audit(&pool, inst_id, None).await.unwrap();

    let rows: Vec<(String,String)> = sqlx::query_as(
        "SELECT source, action FROM audit_log WHERE instance_id = ?1 ORDER BY id"
    ).bind(inst_id).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("vps".into(), "endpoint_started".into()));
    assert_eq!(rows[1], ("ffmpeg".into(), "endpoint_ffmpeg_died".into()));

    let cursor: i64 = sqlx::query_scalar("SELECT last_audit_cursor FROM delivery_instances WHERE id = ?1")
        .bind(inst_id).fetch_one(&pool).await.unwrap();
    assert_eq!(cursor, 2);
}
```

- [ ] **Step 2: Implement `mirror_vps_audit` in `crates/rs-api/src/delivery.rs`**

Add a new public async function near `poll_delivery_metrics`:
```rust
/// Pull audit rows from the VPS (`/api/status?since=<cursor>`) and mirror them
/// into the host `audit_log` preserving source/action/endpoint/ts.
/// Advances `delivery_instances.last_audit_cursor` on success.
pub async fn mirror_vps_audit(
    pool: &sqlx::SqlitePool,
    instance_id: i64,
    audit_tx: Option<&tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>>,
) -> anyhow::Result<()> {
    use rs_core::audit::{Action, AuditRow, Severity, Source};

    let instance = rs_core::db::get_delivery_instance(pool, instance_id).await?
        .ok_or_else(|| anyhow::anyhow!("instance {instance_id} not found"))?;
    let cursor: i64 = sqlx::query_scalar("SELECT last_audit_cursor FROM delivery_instances WHERE id = ?1")
        .bind(instance_id).fetch_one(pool).await?;

    let url = format!("http://{}:8000/api/status?since={cursor}", instance.ipv4);
    let client = reqwest::Client::new();
    let resp = client.get(&url).bearer_auth(&instance.auth_token)
        .timeout(std::time::Duration::from_secs(5)).send().await?;
    if !resp.status().is_success() { return Ok(()); }

    #[derive(serde::Deserialize)]
    struct StatusBody {
        #[serde(default)] recent_audit: Vec<serde_json::Value>,
        #[serde(default)] next_audit_cursor: i64,
    }
    let body: StatusBody = resp.json().await?;
    if body.recent_audit.is_empty() { return Ok(()); }

    for r in &body.recent_audit {
        let ts = r["ts"].as_str().unwrap_or("").to_string();
        let severity: Severity = serde_json::from_value(r["severity"].clone()).unwrap_or(Severity::Info);
        let source: Source = serde_json::from_value(r["source"].clone()).unwrap_or(Source::Vps);
        let action: Action = serde_json::from_value(r["action"].clone()).unwrap_or(Action::EndpointStarted);
        let endpoint = r["endpoint"].as_str().map(|s| s.to_string());
        let detail = r["detail"].clone();

        if let Some(tx) = audit_tx {
            rs_core::audit::record(tx, AuditRow {
                severity, source, event_id: instance.event_id,
                instance_id: Some(instance_id), endpoint,
                action, detail,
                ts_override: if ts.is_empty() { None } else { Some(ts) },
            });
        } else {
            // Synchronous insert path used by integration tests.
            let (ws_tx, _rx) = tokio::sync::broadcast::channel::<rs_core::models::WsEvent>(16);
            let rows = vec![AuditRow {
                severity, source, event_id: instance.event_id,
                instance_id: Some(instance_id), endpoint,
                action, detail, ts_override: Some(ts),
            }];
            rs_core::db::audit::insert_batch(pool, &rows, &ws_tx).await?;
        }
    }

    sqlx::query("UPDATE delivery_instances SET last_audit_cursor = ?1 WHERE id = ?2")
        .bind(body.next_audit_cursor).bind(instance_id).execute(pool).await?;
    Ok(())
}
```

Call `mirror_vps_audit` from `delivery_broadcast_loop` every tick where we already call `poll_delivery_metrics`.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery.rs crates/rs-api/tests/vps_audit_mirror.rs
git commit -m "feat(api): mirror_vps_audit pulls /api/status rows into host audit_log (#120 post-mortem)"
```

---

### Task 11: Fix C — StartPosition::Live returns latest sequence

**Files:**
- Modify: `crates/rs-api/src/delivery_endpoints.rs:45-50`

- [ ] **Step 1: Write failing test**

Append to `crates/rs-api/tests/delivery_endpoints_tests.rs`:
```rust
#[tokio::test]
async fn start_position_live_returns_latest_sequence_not_first() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "evt-live-test").await.unwrap();

    // Insert 10 chunks with sequence_number 1..=10 manually.
    for seq in 1i64..=10 {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO chunk_records (streaming_event_id, chunk_file_path, data_size, md5, sequence_number)
             VALUES (?1, ?2, ?3, '', ?4) RETURNING id"
        ).bind(event_id).bind(format!("c{seq}.bin")).bind(1024_i64).bind(seq)
         .fetch_one(&pool).await.unwrap();
        let _ = id;
    }

    let live = rs_api::delivery_endpoints::resolve_start_chunk_id(
        &pool, event_id, &rs_api::delivery_endpoints::StartPosition::Live
    ).await.unwrap();
    assert_eq!(live, 10, "Live must resolve to latest sequence (10), got {live}");

    let beg = rs_api::delivery_endpoints::resolve_start_chunk_id(
        &pool, event_id, &rs_api::delivery_endpoints::StartPosition::Beginning
    ).await.unwrap();
    assert_eq!(beg, 1, "Beginning must resolve to first sequence (1)");

    assert_ne!(live, beg, "Live and Beginning must differ");
}
```

- [ ] **Step 2: Fix the implementation**

In `crates/rs-api/src/delivery_endpoints.rs`, replace the body of the `StartPosition::Live` arm (lines 45-50):

FROM:
```rust
        StartPosition::Live => {
            let first_seq = db::get_first_sequence_number_for_event(pool, event_id)
                .await?
                .unwrap_or(1);
            Ok(first_seq)
        }
```
TO:
```rust
        StartPosition::Live => {
            // "Live" means the current live edge — latest chunk. Starting from
            // here makes the endpoint track real-time ingest. (Historically this
            // was identical to Beginning; see 2026-04-19 post-mortem.)
            let last_seq = db::get_latest_sequence_number_for_event(pool, event_id)
                .await?
                .unwrap_or(1);
            Ok(last_seq)
        }
```

Also update the doc-comment on `resolve_start_chunk_id` (line 28-31):
```rust
/// Resolve a StartPosition into a concrete start_chunk_id for an event.
///
/// - `Live`      → latest sequence number (track the current live edge)
/// - `Beginning` → first sequence number (replay from event start)
/// - `Resume`    → passes through the chunk_id directly
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery_endpoints.rs crates/rs-api/tests/delivery_endpoints_tests.rs
git commit -m "fix(delivery): StartPosition::Live resolves to latest seq not first (#120 post-mortem)"
```

---

### Task 12: Fix F — Remove-last-endpoint guard (server side)

**Files:**
- Modify: `crates/rs-api/src/delivery_endpoints.rs`
- Modify: `crates/rs-api/src/delivery_handlers.rs` (HTTP handler layer — map anyhow → 409)

- [ ] **Step 1: Write failing test**

Append to `crates/rs-api/tests/delivery_endpoints_tests.rs`:
```rust
#[tokio::test]
async fn remove_endpoint_rejects_when_would_leave_zero_and_delivery_active() {
    // Setup event + delivery instance with single endpoint in delivery_endpoint_status.
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "t").await.unwrap();
    // Mark event as delivering.
    sqlx::query("UPDATE streaming_events SET delivering_activated = 1 WHERE id = ?1")
        .bind(event_id).execute(&pool).await.unwrap();

    let instance_id = db::create_delivery_instance(&pool, 1, "x", "192.0.2.1", "cx22", Some(event_id), "tok").await.unwrap();
    db::update_delivery_instance_status(&pool, instance_id, "delivering").await.unwrap();

    // Seed delivery_endpoint_status with exactly 1 endpoint.
    sqlx::query(
        "INSERT INTO delivery_endpoint_status (instance_id, alias, alive, chunks_processed, current_chunk_id, bytes_processed_total)
         VALUES (?1, 'yt1', 1, 0, 0, 0)"
    ).bind(instance_id).execute(&pool).await.unwrap();

    let mut cfg = rs_core::config::Config::for_testing();
    cfg.hetzner.api_token = "tok".into();
    let orch = rs_api::delivery::DeliveryOrchestrator::new(pool.clone(), cfg).unwrap();

    // Call without force → expect error with substring "would_leave_zero_endpoints".
    let err = rs_api::delivery_endpoints::remove_endpoint_from_delivery(
        &orch, &pool, event_id, "yt1", /*force*/ false
    ).await.expect_err("must reject");
    assert!(err.to_string().contains("would_leave_zero_endpoints"), "got {err}");

    // With force=true, guard passes — though underlying HTTP call will fail
    // (unreachable.invalid); that's fine for this test — we only care that
    // the guard does NOT fire.
    let err2 = rs_api::delivery_endpoints::remove_endpoint_from_delivery(
        &orch, &pool, event_id, "yt1", /*force*/ true
    ).await.expect_err("HTTP will fail on bogus ip");
    assert!(!err2.to_string().contains("would_leave_zero_endpoints"));
}
```

- [ ] **Step 2: Add `force` param to `remove_endpoint_from_delivery`**

In `crates/rs-api/src/delivery_endpoints.rs`, change the function signature:
```rust
pub async fn remove_endpoint_from_delivery(
    orch: &DeliveryOrchestrator,
    pool: &SqlitePool,
    event_id: i64,
    alias: &str,
    force: bool,
) -> anyhow::Result<()> {
    let instance = db::get_delivery_instance_by_event(pool, event_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No active delivery instance for event {event_id}"))?;

    if !is_delivery_active(&instance.status) {
        return Err(anyhow::anyhow!(
            "Delivery instance is in state '{}', not in an active delivery state",
            instance.status
        ));
    }

    // Remove-last-endpoint guard.
    if !force {
        let endpoint_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_endpoint_status WHERE instance_id = ?1"
        ).bind(instance.id).fetch_one(pool).await?;
        let event = db::get_streaming_event_by_id(pool, event_id).await?
            .ok_or_else(|| anyhow::anyhow!("event {event_id} not found"))?;
        if event.delivering_activated && endpoint_count <= 1 {
            return Err(anyhow::anyhow!(
                "would_leave_zero_endpoints: delivery active and removing '{alias}' leaves 0 endpoints; \
                 pass x-force-remove:true header to override"
            ));
        }
    }

    // (existing HTTP call + fast cache removal unchanged below)
    let delivery_url = format!("http://{}:8000", instance.ipv4);
    // … rest of function body unchanged …
}
```

- [ ] **Step 3: Update all callers of `remove_endpoint_from_delivery` to pass `force`**

In `crates/rs-api/src/delivery_handlers.rs` (or wherever the HTTP handler for `DELETE /api/v1/delivery/events/{event_id}/endpoints/{alias}` lives), read `x-force-remove` header:

```rust
use axum::http::HeaderMap;

pub async fn delete_endpoint_handler(
    State(state): State<AppState>,
    Path((event_id, alias)): Path<(i64, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let force = headers.get("x-force-remove")
        .and_then(|v| v.to_str().ok())
        == Some("true");

    let Some(orch) = state.delivery_orchestrator.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "no orchestrator").into_response();
    };

    match delivery_endpoints::remove_endpoint_from_delivery(orch, &state.pool, event_id, &alias, force).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.to_string().contains("would_leave_zero_endpoints") => {
            (StatusCode::CONFLICT, Json(serde_json::json!({
                "error": "would_leave_zero_endpoints",
                "message": e.to_string(),
            }))).into_response()
        }
        Err(e) => {
            tracing::error!("remove_endpoint failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
```

If this handler already exists, modify it minimally; if not, add it and route it.

Also update `delivery_endpoints_tests.rs` existing test (`remove_endpoint_from_delivery_rejects_inactive_delivery`) to pass the new `force: false` argument:
```rust
let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt", false)
    .await.expect_err(…);
```
Same for any other call site.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/delivery_endpoints.rs crates/rs-api/src/delivery_handlers.rs crates/rs-api/tests/delivery_endpoints_tests.rs
git commit -m "feat(delivery): remove-last-endpoint guard returns 409 without x-force-remove (#120 post-mortem)"
```

---

### Task 13: Fix I — RTMP-stable gate on start_delivery

**Files:**
- Modify: `crates/rs-api/src/state.rs`
- Modify: `crates/rs-api/src/delivery_handlers.rs`
- Modify: `crates/rs-inpoint/src/lib.rs` (or wherever the RTMP state transitions are signalled)

- [ ] **Step 1: Write failing test**

Append to `crates/rs-api/src/router_tests.rs`:
```rust
#[tokio::test]
async fn start_delivery_rejects_when_rtmp_unstable() {
    let (app, state) = build_test_app().await;
    // Simulate: rtmp_stable_since was set 5 seconds ago (< 15s).
    *state.rtmp_stable_since.lock().await = Some(std::time::Instant::now() - std::time::Duration::from_secs(5));

    let body = serde_json::json!({"event_id": 1}).to_string();
    let req = Request::builder().method("POST").uri("/api/v1/delivery/start")
        .header("content-type","application/json").body(Body::from(body)).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], "rtmp_not_stable");
    assert_eq!(json["need_secs"], 15);
}

#[tokio::test]
async fn start_delivery_proceeds_when_rtmp_stable_15s() {
    let (app, state) = build_test_app().await;
    *state.rtmp_stable_since.lock().await = Some(std::time::Instant::now() - std::time::Duration::from_secs(20));
    // Ensure the rest of the stack fails for a different reason — either 503 (no orchestrator)
    // or 200 if event exists. What matters: no 400 "rtmp_not_stable".
    let body = serde_json::json!({"event_id": 1}).to_string();
    let req = Request::builder().method("POST").uri("/api/v1/delivery/start")
        .header("content-type","application/json").body(Body::from(body)).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(resp.status(), StatusCode::BAD_REQUEST,
        "must not be rtmp_not_stable when stable >= 15s");
}
```

- [ ] **Step 2: Add `rtmp_stable_since` to AppState**

In `crates/rs-api/src/state.rs`, extend `AppState`:
```rust
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

pub struct AppState {
    // … existing fields …
    pub rtmp_stable_since: Arc<Mutex<Option<Instant>>>,
    pub audit_tx: tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
}
```
and plumb defaults through any `AppState::new` / `with_*` helpers. For `audit_tx`, create a throwaway channel for tests:
```rust
impl Default for AppState {
    fn default() -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(1024);
        Self {
            // … existing defaults …
            rtmp_stable_since: Arc::new(Mutex::new(None)),
            audit_tx: tx,
        }
    }
}
```

- [ ] **Step 3: Gate in start_delivery handler**

In `crates/rs-api/src/delivery_handlers.rs`, at the top of `start_delivery`:
```rust
const RTMP_STABLE_REQUIRED_SECS: u64 = 15;

let stable_since = *state.rtmp_stable_since.lock().await;
let current_secs = stable_since.map(|t| t.elapsed().as_secs()).unwrap_or(0);
if current_secs < RTMP_STABLE_REQUIRED_SECS {
    return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
        "error": "rtmp_not_stable",
        "current_secs": current_secs,
        "need_secs": RTMP_STABLE_REQUIRED_SECS,
    }))).into_response();
}
```

- [ ] **Step 4: Wire inpoint state transitions to set/clear `rtmp_stable_since`**

Locate where inpoint signals RTMP connected/disconnected (likely via a callback or broadcast channel into `AppState`). Add:
```rust
// on RtmpConnected transition:
*state.rtmp_stable_since.lock().await = Some(Instant::now());
// on RtmpDisconnected:
*state.rtmp_stable_since.lock().await = None;
```

If no such hook exists, add one in `crates/rs-inpoint/src/lib.rs` (the xiu subscriber) — emit events to a broadcast channel already plumbed into AppState (search for existing WS broadcast of inpoint state — there is one that feeds `/api/v1/status`).

- [ ] **Step 5: Commit**

```bash
git add crates/rs-api/src/state.rs crates/rs-api/src/delivery_handlers.rs crates/rs-api/src/router_tests.rs crates/rs-inpoint/src/lib.rs
git commit -m "feat(delivery): RTMP-stable gate (15s) on POST /delivery/start (#120 post-mortem)"
```

---

### Task 14: Metrics time-series writer in delivery_broadcast_loop

**Files:**
- Modify: `crates/rs-api/src/lib.rs` (delivery_broadcast_loop)
- Create: `crates/rs-core/src/db/metrics.rs`

- [ ] **Step 1: Write failing test**

Create `crates/rs-core/src/db/metrics_tests.rs` (register `#[cfg(test)] mod metrics_tests;` in `db/mod.rs`):
```rust
use super::*;

#[tokio::test]
async fn insert_metrics_row_round_trips() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    metrics::insert(
        &pool, 1234567890_i64, /*instance*/ 1, /*event*/ 1, "yt1", true,
        100, 99, 10.5, 1_000_000, 0, Some("normal"),
    ).await.unwrap();

    let rows = metrics::query(&pool, metrics::Filter {
        event_id: Some(1), alias: Some("yt1".into()),
        since_ms: None, until_ms: None, limit: None,
    }).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].alias, "yt1");
    assert!((rows[0].chunk_delay_secs - 10.5).abs() < 1e-9);
}
```

- [ ] **Step 2: Implement `db::metrics`**

Create `crates/rs-core/src/db/metrics.rs`:
```rust
//! delivery_endpoint_metrics DB access.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRow {
    pub id: i64,
    pub ts_ms: i64,
    pub instance_id: i64,
    pub event_id: i64,
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub bytes_processed_total: i64,
    pub ffmpeg_restart_count: i64,
    pub delivery_mode: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub event_id: Option<i64>,
    pub alias: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub limit: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &SqlitePool,
    ts_ms: i64, instance_id: i64, event_id: i64,
    alias: &str, alive: bool, current_chunk_id: i64,
    chunks_processed: i64, chunk_delay_secs: f64,
    bytes_processed_total: i64, ffmpeg_restart_count: i64,
    delivery_mode: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_endpoint_metrics
         (ts_ms, instance_id, event_id, alias, alive, current_chunk_id,
          chunks_processed, chunk_delay_secs, bytes_processed_total,
          ffmpeg_restart_count, delivery_mode)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
    ).bind(ts_ms).bind(instance_id).bind(event_id).bind(alias)
     .bind(alive as i64).bind(current_chunk_id).bind(chunks_processed)
     .bind(chunk_delay_secs).bind(bytes_processed_total)
     .bind(ffmpeg_restart_count).bind(delivery_mode)
     .execute(pool).await?;
    Ok(())
}

pub async fn query(pool: &SqlitePool, f: Filter) -> Result<Vec<MetricsRow>> {
    let mut sql = String::from(
        "SELECT id, ts_ms, instance_id, event_id, alias, alive, current_chunk_id,
         chunks_processed, chunk_delay_secs, bytes_processed_total,
         ffmpeg_restart_count, delivery_mode
         FROM delivery_endpoint_metrics WHERE 1=1"
    );
    let mut binds: Vec<String> = Vec::new();
    if let Some(ev) = f.event_id { sql.push_str(&format!(" AND event_id = ?{}", binds.len()+1)); binds.push(ev.to_string()); }
    if let Some(a) = &f.alias { sql.push_str(&format!(" AND alias = ?{}", binds.len()+1)); binds.push(a.clone()); }
    if let Some(s) = f.since_ms { sql.push_str(&format!(" AND ts_ms >= ?{}", binds.len()+1)); binds.push(s.to_string()); }
    if let Some(u) = f.until_ms { sql.push_str(&format!(" AND ts_ms <= ?{}", binds.len()+1)); binds.push(u.to_string()); }
    sql.push_str(" ORDER BY ts_ms ASC");
    sql.push_str(&format!(" LIMIT {}", f.limit.unwrap_or(2000).clamp(1, 20000)));

    let mut q = sqlx::query(&sql);
    for b in &binds { q = q.bind(b); }
    let rows = q.fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| MetricsRow {
        id: r.get("id"), ts_ms: r.get("ts_ms"),
        instance_id: r.get("instance_id"), event_id: r.get("event_id"),
        alias: r.get("alias"),
        alive: r.get::<i64,_>("alive") != 0,
        current_chunk_id: r.get("current_chunk_id"),
        chunks_processed: r.get("chunks_processed"),
        chunk_delay_secs: r.get("chunk_delay_secs"),
        bytes_processed_total: r.get("bytes_processed_total"),
        ffmpeg_restart_count: r.get("ffmpeg_restart_count"),
        delivery_mode: r.get("delivery_mode"),
    }).collect())
}

pub async fn rotate(pool: &SqlitePool, keep_days: i64) -> Result<i64> {
    let cutoff_ms = chrono::Utc::now().timestamp_millis() - keep_days * 86_400_000;
    let res = sqlx::query("DELETE FROM delivery_endpoint_metrics WHERE ts_ms < ?1")
        .bind(cutoff_ms).execute(pool).await?;
    Ok(res.rows_affected() as i64)
}
```

Register in `crates/rs-core/src/db/mod.rs`:
```rust
pub mod metrics;
```

- [ ] **Step 3: Write every-6s in `delivery_broadcast_loop`**

In `crates/rs-api/src/lib.rs` `delivery_broadcast_loop`, add a `tick_counter: u64` local. On each iteration where `final_endpoints` are computed, insert rows every 3rd tick (6 s):

```rust
tick_counter = tick_counter.wrapping_add(1);
if tick_counter % 3 == 0 {
    let ts_ms = chrono::Utc::now().timestamp_millis();
    // Need instance_id + event_id; instance_id available via orch.get_active_instance_id(event_id) or by querying DB.
    if let Ok(Some(inst)) = rs_core::db::get_delivery_instance_by_event(&pool, event.id).await {
        for m in &final_endpoints {
            let _ = rs_core::db::metrics::insert(
                &pool, ts_ms, inst.id, event.id, &m.alias,
                m.alive, m.current_chunk_id, m.chunks_processed as i64,
                m.chunk_delay_secs, m.bytes_processed_total as i64,
                m.ffmpeg_restart_count as i64, m.delivery_mode.as_deref(),
            ).await;
            let _ = ws_tx.send(rs_core::models::WsEvent::MetricsSample {
                ts_ms, event_id: event.id, instance_id: inst.id,
                alias: m.alias.clone(), chunk_delay_secs: m.chunk_delay_secs,
                current_chunk_id: m.current_chunk_id,
                chunks_processed: m.chunks_processed as i64, alive: m.alive,
            });
        }
    }
}
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/metrics.rs crates/rs-core/src/db/metrics_tests.rs crates/rs-core/src/db/mod.rs crates/rs-api/src/lib.rs
git commit -m "feat(metrics): persist per-endpoint metrics every 6s + MetricsSample WS (#120 post-mortem)"
```

---

### Task 15: Metrics API handler

**Files:**
- Create: `crates/rs-api/src/metrics_handlers.rs`
- Modify: `crates/rs-api/src/router.rs`
- Modify: `crates/rs-api/src/lib.rs` (module export)

- [ ] **Step 1: Write failing test**

Append to `crates/rs-api/src/router_tests.rs`:
```rust
#[tokio::test]
async fn metrics_endpoint_returns_inserted_rows() {
    let (app, state) = build_test_app().await;
    let ts_ms = chrono::Utc::now().timestamp_millis();
    rs_core::db::metrics::insert(
        &state.pool, ts_ms, 1, 1, "yt1", true, 10, 10, 5.5, 1000, 0, Some("normal")
    ).await.unwrap();

    let resp = app.oneshot(Request::builder()
        .uri("/api/v1/delivery/metrics?event_id=1")
        .body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["rows"].as_array().unwrap().len() >= 1);
}
```

- [ ] **Step 2: Implement `metrics_handlers::list`**

Create `crates/rs-api/src/metrics_handlers.rs`:
```rust
use axum::{extract::{Query, State}, http::StatusCode, response::IntoResponse, Json};
use rs_core::db::metrics::{self, Filter};
use serde::Deserialize;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub event_id: i64,
    #[serde(default)] pub alias: Option<String>,
    #[serde(default)] pub since_ms: Option<i64>,
    #[serde(default)] pub until_ms: Option<i64>,
    #[serde(default)] pub limit: Option<i64>,
}

pub async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> impl IntoResponse {
    let f = Filter {
        event_id: Some(q.event_id),
        alias: q.alias,
        since_ms: q.since_ms,
        until_ms: q.until_ms,
        limit: q.limit,
    };
    match metrics::query(&state.pool, f).await {
        Ok(rows) => Json(serde_json::json!({ "rows": rows })).into_response(),
        Err(e) => {
            tracing::error!("metrics query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics query failed").into_response()
        }
    }
}
```

- [ ] **Step 3: Mount + export**

In `crates/rs-api/src/router.rs`:
```rust
        .route("/api/v1/delivery/metrics", axum::routing::get(crate::metrics_handlers::list))
```
In `crates/rs-api/src/lib.rs`:
```rust
pub mod metrics_handlers;
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/metrics_handlers.rs crates/rs-api/src/router.rs crates/rs-api/src/lib.rs crates/rs-api/src/router_tests.rs
git commit -m "feat(api): GET /api/v1/delivery/metrics (#120 post-mortem)"
```

---

### Task 16: Populate `restart_history` in /api/v1/delivery/status

**Files:**
- Modify: `crates/rs-api/src/delivery.rs` (wherever `poll_delivery_metrics` builds the status response)

- [ ] **Step 1: Write failing test**

Append to `crates/rs-api/src/router_tests.rs`:
```rust
#[tokio::test]
async fn delivery_status_includes_restart_history_from_db() {
    let (app, state) = build_test_app().await;
    let event_id = rs_core::db::create_streaming_event(&state.pool, "t").await.unwrap();
    let inst_id = rs_core::db::create_delivery_instance(&state.pool, 1, "x", "192.0.2.1", "cx22", Some(event_id), "tok").await.unwrap();
    rs_core::db::update_delivery_instance_status(&state.pool, inst_id, "delivering").await.unwrap();
    // Seed two restart rows.
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs)
         VALUES (?1, ?2, 'yt1', ?3, 100, 300, 'youtube_rtmp_closed', 30)"
    ).bind(inst_id).bind(event_id).bind(now).execute(&state.pool).await.unwrap();
    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs)
         VALUES (?1, ?2, 'yt1', ?3, 200, 200, 'youtube_rtmp_closed', 60)"
    ).bind(inst_id).bind(event_id).bind(now + 1000).execute(&state.pool).await.unwrap();

    let resp = app.oneshot(Request::builder()
        .uri(&format!("/api/v1/delivery/status?event_id={event_id}"))
        .body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_bytes(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // restart_history inside first endpoint_details entry
    let restarts = &json["endpoint_details"][0]["restart_history"];
    assert!(restarts.is_array(), "restart_history must be an array");
    assert_eq!(restarts.as_array().unwrap().len(), 2);
}
```

- [ ] **Step 2: Query the table and populate in status builder**

In `crates/rs-api/src/delivery.rs` where the status response assembles per-endpoint details, replace the placeholder empty `restart_history: []` with:
```rust
// Fetch last 10 restart rows for this alias on this instance.
let restart_history: Vec<serde_json::Value> = sqlx::query(
    "SELECT timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs, stderr_tail
     FROM delivery_restart_log
     WHERE instance_id = ?1 AND alias = ?2
     ORDER BY timestamp_ms DESC
     LIMIT 10"
).bind(instance.id).bind(&m.alias)
 .fetch_all(pool).await.unwrap_or_default()
 .into_iter().map(|row| {
    let stderr_tail: Option<String> = row.try_get("stderr_tail").ok();
    let last_line = stderr_tail.as_deref()
        .and_then(rs_delivery::ffmpeg_reason::pick_last_error_line);
    serde_json::json!({
        "timestamp_ms": row.get::<i64,_>("timestamp_ms"),
        "chunk_id": row.get::<i64,_>("chunk_id"),
        "lifetime_secs": row.get::<i64,_>("lifetime_secs"),
        "reason": row.get::<String,_>("reason"),
        "backoff_secs": row.get::<i64,_>("backoff_secs"),
        "stderr_last_error_line": last_line,
    })
 }).collect();
```

Use `restart_history` in the serialised struct instead of empty.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery.rs crates/rs-api/src/router_tests.rs crates/rs-api/Cargo.toml
git commit -m "feat(api): populate restart_history from delivery_restart_log (#120 post-mortem)"
```

(Ensure `rs-delivery` is a dep of `rs-api` — if it isn't, add to `crates/rs-api/Cargo.toml [dependencies]`: `rs-delivery = { path = "../rs-delivery" }`. If that creates a cycle, move `pick_last_error_line` into a neutral crate, e.g. `rs-core/src/ffmpeg_parse.rs`, and have both `rs-api` and `rs-delivery` depend on it.)

---

### Task 17: Uploader worker dedup (claim-coordinator)

**Files:**
- Modify: `crates/rs-endpoint/src/uploader.rs`
- Modify: `crates/rs-core/src/db/upload.rs`

- [ ] **Step 1: Write failing stress test**

Append to `crates/rs-endpoint/tests/uploader_integration.rs`:
```rust
#[tokio::test]
async fn claim_coordinator_handles_100_chunks_without_busy_errors() {
    // Seed DB with 100 pending chunks.
    // Spin ChunkUploader with N workers (default N ≥ 4).
    // Set up mock S3 that accepts all uploads quickly.
    // After uploads complete, assert:
    //   - all 100 chunks marked sent
    //   - zero log messages containing "database is locked"
    //     (captured via tracing-test or by wrapping the log subscriber)

    use tracing_test::traced_test;

    #[traced_test]
    async fn inner() {
        // … reuse the existing helper that spawns mock S3 + builds uploader …
        // Same pattern as existing uploader_full_flow_success test.
        let (pool, s3_addr, client_uuid) = setup_uploader_env().await;
        let event_id = db::create_streaming_event(&pool, "stress").await.unwrap();
        for i in 1..=100i64 {
            db::insert_chunk(&pool, event_id, format!("c{i}.bin"), 1024).await.unwrap();
        }

        let (ws_tx, _rx) = tokio::sync::broadcast::channel(16);
        let s3 = rs_endpoint::s3::S3Client::new_local(s3_addr);
        let uploader = rs_endpoint::uploader::ChunkUploader::new(
            pool.clone(), s3, ws_tx, client_uuid
        );
        let handle = tokio::spawn(async move { uploader.run().await });

        // Wait up to 30s for all chunks to be uploaded.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let sent: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunk_records WHERE sent = 1")
                .fetch_one(&pool).await.unwrap();
            if sent == 100 { break; }
            if tokio::time::Instant::now() > deadline { panic!("only {sent}/100 uploaded in 30s"); }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        handle.abort();
        assert!(!logs_contain("database is locked"), "claim coordinator must not BUSY-thrash");
    }
    inner().await;
}
```

Add `tracing-test = "0.2"` to `crates/rs-endpoint/[dev-dependencies]` if not present.

- [ ] **Step 2: Add `pick_next_uploadable_chunks` (plural)**

In `crates/rs-core/src/db/upload.rs`, add alongside `pick_next_uploadable_chunk`:
```rust
/// Claim-batch version: returns up to `limit` chunks that are unsent,
/// not-in-process, not permanently-failed, past their retry-time. Does
/// NOT mark them in_process — caller does that per chunk via
/// `mark_chunk_in_process(pool, id)`.
pub async fn pick_next_uploadable_chunks(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<ChunkRecord>> {
    let now = chrono::Utc::now().timestamp();
    let rows = sqlx::query_as::<_, ChunkRecord>(
        "SELECT * FROM chunk_records
         WHERE sent = 0 AND in_process = 0 AND upload_failed_permanently = 0
           AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= ?1)
         ORDER BY upload_next_retry_at ASC, id ASC
         LIMIT ?2"
    ).bind(now).bind(limit).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn mark_chunk_in_process(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("UPDATE chunk_records SET in_process = 1 WHERE id = ?1 AND in_process = 0")
        .bind(id).execute(pool).await?;
    Ok(())
}
```

- [ ] **Step 3: Refactor uploader to claim-coordinator + workers**

In `crates/rs-endpoint/src/uploader.rs`, replace the worker-per-pick pattern. The exact code differs from today's; here is the new shape (rewrite the `run` method):

```rust
pub async fn run(self) -> anyhow::Result<()> {
    const BATCH: i64 = 16;
    const WORKERS: usize = 4;

    let (job_tx, _): (tokio::sync::mpsc::Sender<ChunkJob>, _) = tokio::sync::mpsc::channel(32);

    // Workers.
    let mut workers = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let mut rx = job_tx.subscribe_like(); // OR fan-out via separate per-worker mpsc
        let ctx = self.shared_ctx.clone();
        workers.push(tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                Self::upload_one(&ctx, job).await;
            }
        }));
    }

    // Coordinator.
    let pool = self.pool.clone();
    let coord = tokio::spawn(async move {
        loop {
            match rs_core::db::upload::pick_next_uploadable_chunks(&pool, BATCH).await {
                Ok(batch) if !batch.is_empty() => {
                    for chunk in batch {
                        if rs_core::db::upload::mark_chunk_in_process(&pool, chunk.id).await.is_ok() {
                            let _ = job_tx.send(ChunkJob { chunk }).await;
                        }
                    }
                }
                Ok(_) => { tokio::time::sleep(Duration::from_millis(200)).await; }
                Err(e) => {
                    tracing::error!("claim-coordinator pick failed: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
    // Join (in practice these run until the task is dropped).
    coord.await?;
    for w in workers { w.abort(); }
    Ok(())
}
```

Note on fan-out: Tokio `broadcast::Sender` loses messages if a subscriber falls behind; not what we want here. Use **N separate** `mpsc::channel` and round-robin the coordinator's send, OR use a single `async-channel`-style multi-consumer `mpsc` (not stdlib). Simplest is a single `mpsc::Sender<ChunkJob>` whose receiver is `Arc<Mutex<mpsc::Receiver>>` — workers compete via `rx.lock().await.recv().await`. Implement that pattern.

Concretely:
```rust
use std::sync::Arc;
use tokio::sync::Mutex;

let (job_tx, job_rx) = tokio::sync::mpsc::channel::<ChunkJob>(32);
let shared_rx = Arc::new(Mutex::new(job_rx));

for _ in 0..WORKERS {
    let rx = Arc::clone(&shared_rx);
    let ctx = self.shared_ctx.clone();
    tokio::spawn(async move {
        loop {
            let job = rx.lock().await.recv().await;
            match job {
                Some(j) => Self::upload_one(&ctx, j).await,
                None => break,
            }
        }
    });
}
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-endpoint/src/uploader.rs crates/rs-core/src/db/upload.rs crates/rs-endpoint/tests/uploader_integration.rs crates/rs-endpoint/Cargo.toml
git commit -m "fix(uploader): claim-coordinator pattern — eliminate SQLite BUSY thrash (#120 post-mortem)"
```

---

### Task 18: Audit call sites across rs-api and rs-endpoint

**Files:**
- Modify: `crates/rs-api/src/stream_handlers.rs`
- Modify: `crates/rs-api/src/delivery_handlers.rs`
- Modify: `crates/rs-api/src/delivery.rs`
- Modify: `crates/rs-api/src/delivery_endpoints.rs`
- Modify: `crates/rs-api/src/s3_handlers.rs`
- Modify: `crates/rs-api/src/handlers.rs` (config patch)
- Modify: `crates/rs-endpoint/src/uploader.rs`
- Modify: `crates/rs-inpoint/src/lib.rs`

This task wires `audit::record(&state.audit_tx, AuditRow { … })` at every listed call site in the spec §"Call sites". To keep this bite-sized, do one pass per source group.

- [ ] **Step 1: operator — stream_handlers + delivery_handlers + s3_handlers + handlers**

Add at the top of each affected function (after permission/validation checks, before side effects):
```rust
rs_core::audit::record(&state.audit_tx, rs_core::audit::AuditRow {
    severity: rs_core::audit::Severity::Info,
    source: rs_core::audit::Source::Operator,
    event_id: Some(event_id),
    instance_id: None,
    endpoint: None,
    action: rs_core::audit::Action::EventStarted, // or EventStopped / DeliveryStarted / …
    detail: serde_json::json!({ "event_name": name }),
    ts_override: None,
});
```

Locations (fill exact action per handler):
- `stream_handlers::start_stream` → `EventStarted`
- `stream_handlers::stop_stream` → `EventStopped`
- `delivery_handlers::start_delivery` → `DeliveryStarted` (after gate passes)
- `delivery_handlers::stop_delivery` → `DeliveryStopped`
- `delivery_endpoints::add_endpoint_to_delivery` → `EndpointAdded`
- `delivery_endpoints::remove_endpoint_from_delivery` → `EndpointRemoved` (include `was_last_endpoint` flag)
- `s3_handlers::clear_event_s3_chunks` → `S3Cleared`
- `handlers::patch_config` → `ConfigChanged`

- [ ] **Step 2: delivery source (host side)**

In `crates/rs-api/src/delivery.rs`:
- Before `hetzner::create_server()` → `VpsCreating`
- After `rs-delivery health check passed` log → `VpsReady`
- Before `Delivery instance stopped and deleted` → `VpsDeleted`
- Failed health-check path → `VpsUnreachable` (rate-limited via `RateLimiter`)
- Before POST `/api/init` → `DeliveryInitSent`
- After init response → `DeliveryInitResponse`

- [ ] **Step 3: uploader source (rate-limited)**

In `crates/rs-endpoint/src/uploader.rs::upload_one` failure branch:
```rust
static RL: once_cell::sync::Lazy<rs_core::audit::RateLimiter> =
    once_cell::sync::Lazy::new(rs_core::audit::RateLimiter::new);

let class = /* classify error: timeout / 4xx / 5xx / other */;
if RL.allow(rs_core::audit::Action::S3UploadFailed, class) {
    rs_core::audit::record(&ctx.audit_tx, rs_core::audit::AuditRow {
        severity: rs_core::audit::Severity::Warn,
        source: rs_core::audit::Source::Uploader,
        event_id: Some(ev_id), instance_id: None,
        endpoint: None,
        action: rs_core::audit::Action::S3UploadFailed,
        detail: serde_json::json!({
            "chunk_id": chunk.id,
            "error_class": class,
            "error_msg": err.to_string(),
        }),
        ts_override: None,
    });
}
```

Add `audit_tx: mpsc::Sender<AuditRow>` to `WorkerCtx` / `SharedCtx` and thread it from the constructor.

- [ ] **Step 4: inpoint source**

In `crates/rs-inpoint/src/lib.rs` — wherever the publish/unpublish events are observed (xiu hook), emit audit + set rtmp_stable_since (already covered in Task 13). The audit emissions need an `audit_tx`; plumb it in through `InpointServer::new(…)`.

- [ ] **Step 5: system source**

In `crates/rs-service` startup (find `ServiceCore::run_with_signal` or equivalent), emit `RestreamerStarted` once and `MigrationsApplied` after the migration runner returns.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-api crates/rs-endpoint crates/rs-inpoint crates/rs-service
git commit -m "feat(audit): wire record() at all call sites (#120 post-mortem)"
```

---

### Task 19: Leptos store + ws.rs — restore feeds and add types

**Files:**
- Modify: `leptos-ui/src/store.rs`
- Modify: `leptos-ui/src/ws.rs`

- [ ] **Step 1: Restore `activity_feed` + add `audit_feed` + `endpoint_metrics_history` in store**

In `leptos-ui/src/store.rs`, inside `pub struct DashboardStore`:
```rust
    // Pipeline state and feeds
    pub pipeline_state: RwSignal<PipelineState>,
    pub activity_feed: RwSignal<Vec<ActivityEntry>>,
    pub audit_feed: RwSignal<Vec<AuditEntry>>,
    pub endpoint_metrics_history: RwSignal<std::collections::HashMap<String, Vec<MetricsSample>>>,
    pub selected_event_id: RwSignal<Option<i64>>,
```

And the corresponding entries in the `Default`/`new` impl. Define the new types in the same file:
```rust
#[derive(Debug, Clone, PartialEq)]
pub struct AuditEntry {
    pub id: i64,
    pub ts: String,
    pub severity: String,
    pub source: String,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSample {
    pub ts_ms: i64,
    pub event_id: i64,
    pub instance_id: i64,
    pub alias: String,
    pub chunk_delay_secs: f64,
    pub current_chunk_id: i64,
    pub chunks_processed: i64,
    pub alive: bool,
}
```

The existing `ActivityEntry` type should still be in the file (check git); if not, restore from commit 60289c1 parent.

- [ ] **Step 2: `ws.rs` match arms** — already drafted in Task 5 Step 3. Nothing to do here unless Task 5 hasn't been merged.

- [ ] **Step 3: Commit**

```bash
git add leptos-ui/src/store.rs
git commit -m "feat(ui): restore activity_feed + add audit_feed + endpoint_metrics_history (#120 post-mortem)"
```

---

### Task 20: Leptos `AuditPanel` component

**Files:**
- Create: `leptos-ui/src/components/audit_panel.rs`
- Modify: `leptos-ui/src/components/mod.rs` (export)
- Modify: `leptos-ui/src/components/operator_dashboard.rs` (mount)

- [ ] **Step 1: Implement component**

Create `leptos-ui/src/components/audit_panel.rs`:
```rust
use leptos::prelude::*;
use crate::store::{AuditEntry, DashboardStore};

#[component]
pub fn AuditPanel() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let feed = store.audit_feed;
    let (filter_source, set_filter_source) = signal::<Option<String>>(None);

    let visible = Memo::new(move |_| {
        let src = filter_source.get();
        feed.get().into_iter().rev().take(50)
            .filter(|e| src.as_deref().map_or(true, |s| e.source == s))
            .collect::<Vec<_>>()
    });

    view! {
        <div class="audit-panel">
            <header class="audit-panel__header">
                <h3>"Activity"</h3>
                <select on:change=move |ev| {
                    let v = event_target_value(&ev);
                    set_filter_source.set(if v == "all" { None } else { Some(v) });
                }>
                    <option value="all">"all sources"</option>
                    <option value="operator">"operator"</option>
                    <option value="inpoint">"inpoint"</option>
                    <option value="uploader">"uploader"</option>
                    <option value="delivery">"delivery"</option>
                    <option value="vps">"vps"</option>
                    <option value="ffmpeg">"ffmpeg"</option>
                    <option value="s3">"s3"</option>
                    <option value="system">"system"</option>
                </select>
            </header>
            <ul class="audit-panel__list">
                <For
                    each=move || visible.get()
                    key=|e| e.id
                    children=move |e: AuditEntry| {
                        let sev_class = format!("audit-row audit-row--{}", e.severity);
                        let time = e.ts.split('T').nth(1).unwrap_or(&e.ts).split('.').next().unwrap_or("").to_string();
                        let endpoint = e.endpoint.clone().unwrap_or_default();
                        view! {
                            <li class=sev_class>
                                <span class="audit-row__time">{time}</span>
                                <span class="audit-row__source">{e.source.clone()}</span>
                                <span class="audit-row__action">{e.action.clone()}</span>
                                <Show when=move || !endpoint.is_empty()>
                                    <span class="audit-row__endpoint">{endpoint.clone()}</span>
                                </Show>
                                <details class="audit-row__detail">
                                    <summary>"detail"</summary>
                                    <pre>{serde_json::to_string_pretty(&e.detail).unwrap_or_default()}</pre>
                                </details>
                            </li>
                        }
                    }
                />
            </ul>
        </div>
    }
}
```

Add matching CSS classes in `leptos-ui/style.css` (or wherever styles live):
```css
.audit-panel { width: 360px; max-height: 600px; overflow-y: auto; border-left: 1px solid #333; padding: 8px; }
.audit-row { display: flex; gap: 6px; font-size: 11px; padding: 2px 0; }
.audit-row--info { color: #bbb; }
.audit-row--warn { color: #e8c547; }
.audit-row--error, .audit-row--critical { color: #e85c47; }
.audit-row--critical { animation: pulse 1.2s infinite; }
@keyframes pulse { 50% { background: rgba(232,92,71,.2); } }
```

- [ ] **Step 2: Export + mount**

In `leptos-ui/src/components/mod.rs`:
```rust
pub mod audit_panel;
```
In `operator_dashboard.rs` mount the panel in the right column:
```rust
use crate::components::audit_panel::AuditPanel;
// … inside layout view!:
<aside class="dashboard__sidebar"><AuditPanel/></aside>
```

- [ ] **Step 3: Commit**

```bash
git add leptos-ui/src/components/audit_panel.rs leptos-ui/src/components/mod.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css
git commit -m "feat(ui): AuditPanel live feed (#120 post-mortem)"
```

---

### Task 21: Leptos `ZeroEndpointBanner`

**Files:**
- Create: `leptos-ui/src/components/zero_endpoint_banner.rs`
- Modify: `leptos-ui/src/components/mod.rs`, `operator_dashboard.rs`

- [ ] **Step 1: Component**

Create `leptos-ui/src/components/zero_endpoint_banner.rs`:
```rust
use leptos::prelude::*;
use crate::store::DashboardStore;

#[component]
pub fn ZeroEndpointBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let pipeline = store.pipeline_state;
    let delivery = store.delivery;

    let show = Memo::new(move |_| {
        let ps = pipeline.get();
        let d = delivery.get();
        ps.state != "idle" && d.endpoints.is_empty()
    });

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--critical" role="alert">
                "⚠ Delivery is active but 0 endpoints are running. Audience sees nothing."
            </div>
        </Show>
    }
}
```

CSS:
```css
.banner { padding: 12px; font-weight: 600; text-align: center; }
.banner--critical { background: #4a1416; color: #ffdede; border: 1px solid #e85c47; animation: pulse 1.5s infinite; }
```

- [ ] **Step 2: Mount at top of dashboard**

In `operator_dashboard.rs`:
```rust
use crate::components::zero_endpoint_banner::ZeroEndpointBanner;
// … top of main panel:
<ZeroEndpointBanner/>
```

- [ ] **Step 3: Commit**

```bash
git add leptos-ui/src/components/zero_endpoint_banner.rs leptos-ui/src/components/mod.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css
git commit -m "feat(ui): ZeroEndpointBanner when delivery active with 0 endpoints (#120 post-mortem)"
```

---

### Task 22: Leptos `EndpointRemoveConfirmModal`

**Files:**
- Create: `leptos-ui/src/components/endpoint_remove_confirm_modal.rs`
- Modify: `leptos-ui/src/components/mod.rs`, `operator_dashboard.rs` (wire click handler)

- [ ] **Step 1: Component**

Create `leptos-ui/src/components/endpoint_remove_confirm_modal.rs`:
```rust
use leptos::prelude::*;

#[component]
pub fn EndpointRemoveConfirmModal(
    /// Endpoint alias being removed.
    #[prop(into)] alias: Signal<String>,
    /// Event name, for confirm-typing verification.
    #[prop(into)] event_name: Signal<String>,
    /// True when the modal should be shown.
    #[prop(into)] visible: Signal<bool>,
    /// Called on cancel.
    on_cancel: impl Fn() + 'static + Send + Sync + Clone,
    /// Called on confirm (only fires when typed name matches event_name).
    on_confirm: impl Fn() + 'static + Send + Sync + Clone,
) -> impl IntoView {
    let (typed, set_typed) = signal(String::new());

    let match_ok = {
        let event_name = event_name;
        let typed = typed;
        Memo::new(move |_| typed.get() == event_name.get())
    };

    view! {
        <Show when=move || visible.get()>
            <div class="modal__backdrop">
                <div class="modal">
                    <h3>"Remove last endpoint"</h3>
                    <p>"Removing " <strong>{move || alias.get()}</strong>
                       " is the last endpoint on this delivery. Audience will see NOTHING."</p>
                    <p>"Type the event name (" <code>{move || event_name.get()}</code>
                       ") to confirm:"</p>
                    <input
                        type="text"
                        prop:value=move || typed.get()
                        on:input=move |ev| set_typed.set(event_target_value(&ev))
                    />
                    <div class="modal__actions">
                        <button on:click={
                            let on_cancel = on_cancel.clone();
                            move |_| on_cancel()
                        }>"Cancel"</button>
                        <button
                            prop:disabled=move || !match_ok.get()
                            on:click={
                                let on_confirm = on_confirm.clone();
                                move |_| on_confirm()
                            }
                        >"Remove anyway"</button>
                    </div>
                </div>
            </div>
        </Show>
    }
}
```

- [ ] **Step 2: Wire in operator_dashboard.rs**

At the click site for "Remove endpoint", check endpoint count + delivery active; if last, show modal. On modal confirm, call `DELETE /api/v1/delivery/events/{event_id}/endpoints/{alias}` with header `x-force-remove: true`.

- [ ] **Step 3: Commit**

```bash
git add leptos-ui/src/components/endpoint_remove_confirm_modal.rs leptos-ui/src/components/mod.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css
git commit -m "feat(ui): EndpointRemoveConfirmModal for last-endpoint removal (#120 post-mortem)"
```

---

### Task 23: Leptos `EndpointHistory` sparkline

**Files:**
- Create: `leptos-ui/src/components/endpoint_history.rs`
- Modify: `leptos-ui/src/components/mod.rs`
- Modify: `operator_dashboard.rs` (add history tab to endpoint card)

- [ ] **Step 1: Component (SVG inline sparkline — no chart lib needed)**

Create `leptos-ui/src/components/endpoint_history.rs`:
```rust
use leptos::prelude::*;
use crate::store::DashboardStore;

#[component]
pub fn EndpointHistory(#[prop(into)] alias: Signal<String>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let history = store.endpoint_metrics_history;

    let series = Memo::new(move |_| {
        let alias = alias.get();
        let h = history.get();
        h.get(&alias).cloned().unwrap_or_default()
    });

    view! {
        <div class="endpoint-history">
            <h4>"chunk_delay over last 200 samples"</h4>
            {move || {
                let pts = series.get();
                if pts.is_empty() { return view! { <p>"no data yet"</p> }.into_any(); }
                let w = 300.0; let h = 60.0;
                let n = pts.len().max(2) as f64;
                let max = pts.iter().map(|p| p.chunk_delay_secs).fold(1.0_f64, f64::max);
                let path: String = pts.iter().enumerate().map(|(i, p)| {
                    let x = (i as f64) * (w / (n - 1.0));
                    let y = h - (p.chunk_delay_secs / max * h);
                    format!("{} {x:.1},{y:.1}", if i == 0 { "M" } else { "L" })
                }).collect::<Vec<_>>().join(" ");
                view! {
                    <svg width=w.to_string() height=h.to_string() style="background:#111">
                        <path d=path stroke="#4caf50" stroke-width="1.2" fill="none"/>
                    </svg>
                }.into_any()
            }}
        </div>
    }
}
```

- [ ] **Step 2: Mount inside endpoint card in `operator_dashboard.rs`**

Find the endpoint card rendering; add a toggleable "History" pane that renders `<EndpointHistory alias=ep.alias.clone()/>`.

- [ ] **Step 3: Commit**

```bash
git add leptos-ui/src/components/endpoint_history.rs leptos-ui/src/components/mod.rs leptos-ui/src/components/operator_dashboard.rs
git commit -m "feat(ui): EndpointHistory sparkline for chunk_delay (#120 post-mortem)"
```

---

### Task 24: Start-delivery button gating in dashboard

**Files:**
- Modify: `leptos-ui/src/components/operator_dashboard.rs`

- [ ] **Step 1: Add `rtmp_stable_since` derivation**

Consume `/api/v1/status` polling result; compute `rtmp_stable_secs` client-side by tracking the transition time locally OR by having the backend expose `rtmp_stable_secs` in the status JSON. Preferred: backend exposes it.

Extend `crates/rs-api/src/handlers.rs` `get_status` to include `rtmp_stable_secs`:
```rust
let stable_secs = state.rtmp_stable_since.lock().await
    .map(|t| t.elapsed().as_secs()).unwrap_or(0);
// Attach to response json: "rtmp_stable_secs": stable_secs
```

In `operator_dashboard.rs`, disable the start-delivery button when `!receiving_activated || rtmp_stable_secs < 15`:
```rust
let start_disabled = Memo::new(move |_| {
    let s = store.status.get();
    !s.streaming_event.receiving_activated || s.rtmp_stable_secs.unwrap_or(0) < 15
});

view! {
    <button
        prop:disabled=move || start_disabled.get()
        title=move || if start_disabled.get() {
            format!("Waiting for OBS stream to stabilize ({}/15s)",
                store.status.get().rtmp_stable_secs.unwrap_or(0))
        } else { "Start delivery".into() }
        on:click=start_delivery_click
    >"Start delivery"</button>
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/rs-api/src/handlers.rs leptos-ui/src/components/operator_dashboard.rs
git commit -m "feat(ui): gate Start Delivery button on RTMP stable ≥15s (#120 post-mortem)"
```

---

### Task 25: CI deploy gate on active live event

**Files:**
- Modify: `.github/workflows/ci.yml` (deploy-stream-lan job)

- [ ] **Step 1: Add pre-deploy step**

In `.github/workflows/ci.yml`, under the `deploy-stream-lan:` job's `steps:`, BEFORE the actual deploy step, add:

```yaml
      - name: Refuse deploy during active live event
        if: "!contains(github.event.head_commit.message, '[skip-live-check]')"
        shell: powershell
        run: |
          try {
            $s = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status" -TimeoutSec 10
          } catch {
            Write-Error "FAIL: stream.lan API unreachable — refusing to deploy (conservative)."
            exit 1
          }
          if ($s.streaming_event.receiving_activated -eq $true) {
            $name = $s.streaming_event.name
            Write-Error "FAIL: stream.lan has active live event '$name'. Refusing to deploy. Use [skip-live-check] in commit message to override."
            exit 1
          }
          Write-Host "OK: no active live event; deploy may proceed."
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: refuse deploy during active live event (#120 post-mortem)"
```

---

### Task 26: Playwright E2E specs

**Files:**
- Create: `e2e/audit-panel.spec.ts`
- Create: `e2e/remove-last-endpoint-modal.spec.ts`
- Create: `e2e/start-delivery-rtmp-gate.spec.ts`
- Create: `e2e/endpoint-history-sparkline.spec.ts`
- Create: `e2e/zero-endpoint-banner.spec.ts`

Each new spec follows the existing `e2e/frontend.spec.ts` pattern (mock API via `mock-api.js`; navigate; interact; assert; assert-zero-console-errors).

- [ ] **Step 1: Create all five spec files**

`e2e/audit-panel.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

test('audit panel shows rows from WebSocket', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // Wait for audit panel to exist.
  await expect(page.locator('.audit-panel')).toBeVisible();

  // Trigger an operator action that produces an audit row — e.g. create an event.
  // The mock-api.js must emit an AuditAppended WS event in response.
  await page.getByRole('button', { name: /new event/i }).click();
  await page.getByLabel(/name/i).fill('e2e-audit-test');
  await page.getByRole('button', { name: /create/i }).click();

  // Audit row appears.
  await expect(page.locator('.audit-panel .audit-row', { hasText: 'event_started' })).toBeVisible();

  expect(consoleMessages).toEqual([]);
});
```

`e2e/remove-last-endpoint-modal.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

test('removing the last endpoint during active delivery shows confirm modal', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // mock-api.js seeds: one active event "test-event", delivering, one endpoint "yt1".
  await page.getByRole('button', { name: /remove.*yt1/i }).click();

  await expect(page.locator('.modal')).toBeVisible();
  await expect(page.locator('.modal')).toContainText('Remove last endpoint');

  // Disabled until event name typed.
  const confirm = page.getByRole('button', { name: /remove anyway/i });
  await expect(confirm).toBeDisabled();
  await page.getByRole('textbox').fill('test-event');
  await expect(confirm).toBeEnabled();
  await confirm.click();

  // After confirm, modal closes. Zero-endpoint banner appears.
  await expect(page.locator('.modal')).toBeHidden();
  await expect(page.locator('.banner--critical')).toBeVisible();
  await expect(page.locator('.banner--critical')).toContainText('0 endpoints');

  expect(consoleMessages).toEqual([]);
});
```

`e2e/start-delivery-rtmp-gate.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

test('start delivery button disabled until RTMP stable 15s', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  await page.goto('/');
  const btn = page.getByRole('button', { name: /start delivery/i });
  await expect(btn).toBeDisabled();
  await expect(btn).toHaveAttribute('title', /Waiting for OBS.*[0-9]+\/15s/);
  // mock-api.js ticks rtmp_stable_secs from 0 → 20 over ~1s.
  await expect(btn).toBeEnabled({ timeout: 5000 });
  expect(consoleMessages).toEqual([]);
});
```

`e2e/endpoint-history-sparkline.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

test('endpoint history sparkline renders after metrics samples arrive', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  await page.goto('/');
  // Click the History tab on the first endpoint card.
  await page.getByRole('button', { name: /history/i }).first().click();
  // SVG sparkline present.
  await expect(page.locator('.endpoint-history svg path')).toBeVisible({ timeout: 10000 });
  expect(consoleMessages).toEqual([]);
});
```

`e2e/zero-endpoint-banner.spec.ts`:
```typescript
import { test, expect } from '@playwright/test';

test('banner visible when delivering active with zero endpoints', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  // mock-api.js seeds: delivering_activated=true, endpoints=[].
  await page.goto('/');
  await expect(page.locator('.banner--critical')).toBeVisible();
  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 2: Update `e2e/mock-api.js` to support new scenarios**

Extend the mock to:
- Broadcast `AuditAppended` WS events in response to operator actions.
- Tick `rtmp_stable_secs` from 0 upward over time in `/api/v1/status` responses.
- Expose scenario toggles via query params (`?scenario=zero-endpoints`, `?scenario=last-endpoint`, etc.) so each spec can select its seed state.

- [ ] **Step 3: Commit**

```bash
git add e2e/audit-panel.spec.ts e2e/remove-last-endpoint-modal.spec.ts e2e/start-delivery-rtmp-gate.spec.ts e2e/endpoint-history-sparkline.spec.ts e2e/zero-endpoint-banner.spec.ts e2e/mock-api.js
git commit -m "test(e2e): Playwright specs for audit panel, modals, banners, sparkline, RTMP gate (#120 post-mortem)"
```

---

### Task 27: Spawn audit writer task + rotation

**Files:**
- Modify: `crates/rs-api/src/lib.rs` (spawn on `serve`) or `crates/rs-service/src/*` (startup)

- [ ] **Step 1: Spawn `audit_writer_task` on service start**

In `crates/rs-api/src/lib.rs::serve` (or earlier if the `mpsc` is constructed there), after building `state`:
```rust
let (audit_tx, audit_rx) = tokio::sync::mpsc::channel::<rs_core::audit::AuditRow>(1024);
let mut state = state; state.audit_tx = audit_tx.clone();

{
    let pool = state.pool.clone();
    let ws_tx = state.ws_tx.clone();
    tokio::spawn(async move {
        rs_core::audit::audit_writer_task(pool, ws_tx, audit_rx).await;
    });
}

// Nightly rotation.
{
    let pool = state.pool.clone();
    tokio::spawn(async move {
        loop {
            let now = chrono::Utc::now();
            let next = (now + chrono::Duration::hours(24))
                .date_naive().and_hms_opt(2, 0, 0).unwrap()
                .and_utc();
            let sleep_secs = (next - now).num_seconds().max(60) as u64;
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            let _ = rs_core::db::audit::rotate(&pool, 90).await;
            let _ = rs_core::db::metrics::rotate(&pool, 7).await;
        }
    });
}
```

Plumb `audit_tx` into any sub-components that need it (inpoint server, uploader, orchestrator).

- [ ] **Step 2: Commit**

```bash
git add crates/rs-api/src/lib.rs
git commit -m "feat(audit): spawn writer + nightly rotation on service start (#120 post-mortem)"
```

---

### Task 28: Final push, CI, PR

**Files:** none (git + CI)

- [ ] **Step 1: Local format check**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Identify latest CI run**

```bash
gh run list --branch dev --limit 3
```
Note the run id.

- [ ] **Step 4: Monitor CI to terminal state**

Per airuleset, single background poll:
```bash
sleep 900 && gh run view <run-id> --json status,conclusion,jobs
```
Run with `run_in_background: true`. When it completes, read with BashOutput.

Expected: every job conclusion `success`. The mutation testing job must pass without new exclusions.

On failure: `gh run view <run-id> --log-failed`, fix ALL issues in ONE commit, push ONCE, monitor again.

- [ ] **Step 5: Open PR**

```bash
gh pr create --title "feat: live-event post-mortem — audit log, metrics, ffmpeg reason + reconnect, RTMP/endpoint guards, SQLite WAL (v0.3.66, post-mortem 2026-04-19)" --body "$(cat <<'EOF'
## Summary

Comprehensive fix for the 2026-04-19 Sunday-service live-event failure.

**Audit log + metrics time-series (new observability backbone):**
- `audit_log` DB table with typed `Severity/Source/Action` enums
- Non-blocking `mpsc` write path + batched INSERTs + WebSocket broadcast
- `/api/v1/audit` and `/api/v1/audit/{id}` REST
- Dashboard `AuditPanel` (right-side live feed, severity-coded, source-filterable)
- `delivery_endpoint_metrics` time-series, 6 s sampling, 7 d retention
- `/api/v1/delivery/metrics` REST
- Dashboard `EndpointHistory` sparkline per endpoint

**Targeted fixes (all with TDD):**
- `StartPosition::Live` now returns latest sequence, not first
- `ffmpeg_reason::classify` + `reconnect_floor` — 30 s floor for YouTube/FB remote-close; 16 real stderr captures from today checked in as test fixtures
- Endpoint restart state machine uses reason-class backoff
- `remove_endpoint_from_delivery` rejects with 409 when would leave zero endpoints + delivery active (overridable via `x-force-remove: true`)
- `EndpointRemoveConfirmModal` — dashboard blocks silent zero-endpoint state
- `ZeroEndpointBanner` — warns when `delivering_activated` with 0 endpoints
- `start_delivery` returns 400 `rtmp_not_stable` for first 15 s after OBS connects
- Start-delivery button disabled in dashboard during those 15 s
- rs-delivery `audit_ring` + JSONL + `/api/status` piggyback → host mirrors VPS rows into `audit_log` with `source='vps'`
- `/api/v1/delivery/status.restart_history` populated from `delivery_restart_log` (was `[]`)
- SQLite `busy_timeout=5000` + `synchronous=NORMAL` pragmas (WAL already on)
- Uploader claim-coordinator — single batched SELECT, round-robin mpsc dispatch — eliminates BUSY thrash
- CI `deploy-stream-lan` refuses deploy when stream.lan has active live event; `[skip-live-check]` overrides

Spec: `docs/superpowers/specs/2026-04-19-live-event-postmortem-comprehensive-fix-design.md`
Plan: `docs/superpowers/plans/2026-04-19-live-event-postmortem-comprehensive-fix.md`
Closes #120.

## Test plan
- [ ] `StartPosition::Live` unit test: latest ≠ first
- [ ] ffmpeg_reason classifies all 16 real stderr fixtures correctly
- [ ] reconnect_floor returns 30/60/120/240/300 s for consecutive YT closes
- [ ] audit round-trip: POST action → DB row → WS broadcast → GET /audit returns it
- [ ] VPS mirror: fake rs-delivery returns audit rows → host inserts with source='vps' + cursor advances
- [ ] remove_endpoint returns 409 when would-leave-zero + active; ok with `x-force-remove: true`
- [ ] start_delivery returns 400 when RTMP stable <15 s
- [ ] metrics INSERT every 6 s; GET /metrics returns rows
- [ ] SQLite `PRAGMA busy_timeout` = 5000 at pool init
- [ ] Claim-coordinator: 100 chunks / 4 workers produce zero BUSY log lines
- [ ] CI deploy step refuses when `receiving_activated=true`; `[skip-live-check]` overrides
- [ ] Playwright specs pass (audit panel, modal, banner, sparkline, RTMP gate)
- [ ] Mutation testing green without new exclusions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Monitor PR CI run**

Same single-poll pattern. All jobs green including mutation testing.

- [ ] **Step 7: Verify PR mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<pr-number> --jq '{mergeable, mergeable_state}'
```
Expected: `mergeable: true`, `mergeable_state: "clean"`.

- [ ] **Step 8: Report URL to user and STOP**

Per `pr-merge-policy`, do NOT merge. Wait for explicit user instruction.

---

## Verification Matrix

| Spec requirement | Task | Evidence |
|---|---|---|
| V18 audit_log migration | 1 | `migration_v18_creates_audit_log_idempotent` test |
| V19 metrics + cursor migration | 1 | `migration_v19_creates_metrics_and_cursor_column` test |
| busy_timeout + synchronous=NORMAL pragmas | 2 | `create_pool_sets_busy_timeout_and_synchronous` test |
| Typed Severity/Source/Action | 3 | `severity_serde_snake_case` + siblings |
| record() with rate limiting | 3 | `rate_limiter_allows_first_and_blocks_within_minute` |
| audit_writer_task batched inserts + broadcast | 4, 27 | `insert_batch_persists_rows_and_broadcasts` |
| `WsEvent::AuditAppended` + `MetricsSample` | 5 | WS match arms in ws.rs |
| `GET /api/v1/audit` + `/{id}` | 6 | `audit_list_*` + `audit_get_by_id_*` tests |
| ffmpeg_reason classify (incl. fixtures) | 7 | 16 fixture-driven tests |
| reconnect_floor policy | 7, 8 | `reconnect_floor_*` table tests |
| endpoint_task integration | 8 | `backoff_uses_reconnect_floor_for_youtube_broken_pipe` |
| audit_ring + /api/status piggyback | 9 | `ring_since_returns_rows_after_cursor` |
| Host-side VPS audit mirror | 10 | `vps_audit_mirror_inserts_rows_and_advances_cursor` |
| StartPosition::Live → latest | 11 | `start_position_live_returns_latest_sequence_not_first` |
| Remove-last-endpoint 409 | 12 | `remove_endpoint_rejects_when_would_leave_zero_and_delivery_active` |
| RTMP-stable gate | 13 | `start_delivery_rejects_when_rtmp_unstable` |
| Metrics writer every 6 s | 14 | broadcast_loop tick logic + unit |
| `GET /api/v1/delivery/metrics` | 15 | `metrics_endpoint_returns_inserted_rows` |
| restart_history populated | 16 | `delivery_status_includes_restart_history_from_db` |
| Uploader claim-coordinator | 17 | `claim_coordinator_handles_100_chunks_without_busy_errors` |
| Audit call sites everywhere | 18 | Integration end-to-end test emits and reads back |
| Leptos AuditPanel | 20 | `e2e/audit-panel.spec.ts` |
| ZeroEndpointBanner | 21 | `e2e/zero-endpoint-banner.spec.ts` |
| EndpointRemoveConfirmModal | 22 | `e2e/remove-last-endpoint-modal.spec.ts` |
| EndpointHistory sparkline | 23 | `e2e/endpoint-history-sparkline.spec.ts` |
| Start-delivery button gated | 24 | `e2e/start-delivery-rtmp-gate.spec.ts` |
| CI deploy gate | 25 | CI step `Refuse deploy during active live event` |
| Version 0.3.66 | 0 | 4 files |
| Single PR, mergeable, green | 28 | `gh pr view` output |

No uncovered spec requirements.
