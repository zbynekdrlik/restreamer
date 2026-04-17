# Migration Idempotency — Design Spec

**Issue:** #112 — `v0.3.35` fresh-install bug: migrations halt at V4 with `duplicate column name: auth_token`. `event_templates` never gets created, Templates tab permanently empty.

**Goal:** Fresh install reaches max `schema_version`, migration failure halts startup with a clear error, and CI asserts fresh-DB convergence.

---

## Problem

On a fresh install of v0.3.35, the migration runner reaches `schema_version = 4` and fails at V5:

```
ERROR Failed to run migrations: database error: error returned from database: (code: 1)
duplicate column name: auth_token
```

Result:
- `schema_version` stops at 4.
- V5–V17 never execute.
- `event_templates` and `template_endpoints` (created in V12) never exist.
- Dashboard Templates tab loads but is permanently empty — operator cannot create templates from a fresh install.

Current error handling in `src-tauri/src/lib.rs:187`:

```rust
if let Err(e) = db::run_migrations(&pool).await {
    tracing::error!("Failed to run migrations: {e}");
    return;  // async task returns; tray app keeps running with broken DB
}
```

The task returns, Tauri stays up, the service never starts, but there is no visible error — operators see the tray icon and assume everything is fine. The issue calls this a "silent broken state."

## Root Cause

V2 creates `delivery_instances` with an `auth_token` column. V4 rebuilds the table via `CREATE TABLE ... _new; INSERT ... SELECT; DROP; ALTER RENAME` — dropping `auth_token` along the way. V5 then re-adds `auth_token` via `ALTER TABLE ... ADD COLUMN`.

On a clean, successful run this sequence is fine. But if a prior run was interrupted after V4 committed but before V5 completed (e.g., process kill, Windows antivirus lock, power loss), the `auth_token` column may already exist on startup when V5 runs — and SQLite rejects `ADD COLUMN` with `duplicate column name`.

The exact interruption path is hard to reproduce in a controlled test — the user saw it on a real install, manually cherry-picked endpoints + templates from `streamsnv`, and moved on. Regardless of the specific trigger, the fix is the same: make each `ALTER TABLE ADD COLUMN` idempotent so resumption from partial state always succeeds.

## Approach

Three independent changes, one each for the three acceptance criteria.

### 1. Idempotent `ALTER TABLE ADD COLUMN`

Add a helper in `crates/rs-core/src/db/mod.rs`:

```rust
async fn add_column_if_missing(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
    col_def: &str,
) -> sqlx::Result<()> {
    let exists: bool = sqlx::query_scalar(
        "SELECT COUNT(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2",
    )
    .bind(table)
    .bind(column)
    .fetch_one(&mut **tx)
    .await?;
    if !exists {
        sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {col_def}"))
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
```

Migrations that use `ALTER TABLE ADD COLUMN`: V5, V6, V9, V10, V11, V12, V14, V15, V17. Replace each raw `ADD COLUMN` statement with a call to `add_column_if_missing`.

V7 is a `RENAME COLUMN` (not `ADD COLUMN`) and uses a different idempotency check: no-op if the new column name already exists. Add a second helper, `rename_column_if_old_exists(tx, table, old, new)`.

Statements that create tables or indexes already use `IF NOT EXISTS` — no change needed. V3, V4, and V16 are destructive table rebuilds and are left as-is; the idempotency fix targets only `ADD COLUMN` / `RENAME COLUMN` statements, which are the documented failure mode.

**Migration runner refactor.** The current runner treats each migration as a raw SQL string split on `;`. To mix raw SQL with programmatic helpers, change the migration list signature from `&[(i32, &'static str)]` to `&[(i32, MigrationFn)]`, where `MigrationFn` takes `&mut Transaction` and returns `BoxFuture<'_, sqlx::Result<()>>`. Pure-SQL migrations (V1, V2, V3, V4, V8, V13, V16) wrap the existing constant in a helper that executes the split statements. Migrations needing idempotent helpers (V5, V6, V7, V9, V10, V11, V12, V14, V15, V17) become small async functions that call `add_column_if_missing` or `rename_column_if_old_exists` directly.

### 2. Halt on migration failure

Change the error branch in `src-tauri/src/lib.rs:187` to exit the process with a non-zero code and surface a Windows notification via `tauri_plugin_notification`:

```rust
if let Err(e) = db::run_migrations(&pool).await {
    tracing::error!("Failed to run migrations: {e}");
    let _ = notification::Notification::new(&handle.config().identifier)
        .title("Restreamer: database migration failed")
        .body(format!("See log at C:\\ProgramData\\Restreamer\\logs\\ — error: {e}"))
        .show();
    std::process::exit(1);
}
```

Rationale: a tray app showing a running icon with no service is worse than a process that crashes — the operator immediately notices, and Task Scheduler records the failure. The log file already has the full error chain.

The `run_headless()` path (used in tests / CI deploy) already exits on migration failure; no change needed there.

### 3. CI integration test

Add two tests in `crates/rs-core/src/db/tests.rs`:

**`fresh_database_reaches_max_schema_version`** — creates an empty `SqlitePool` via `create_pool` in a temp dir, calls `run_migrations`, asserts `schema_version = MAX_VERSION` where `MAX_VERSION = 17`. Fails CI if any migration misses.

**`migrations_idempotent_when_altered_columns_preexist`** — simulates the bug:
1. Create a fresh pool.
2. Apply migrations V1–V4 manually.
3. Manually `ALTER TABLE delivery_instances ADD COLUMN auth_token` to put the DB in the broken state the user observed.
4. Set `schema_version = 4`.
5. Call `run_migrations`.
6. Assert it reaches `schema_version = 17` without error.

This test directly reproduces the reported failure path and proves the idempotency fix works.

## Components

| Change | File | Scope |
|--------|------|-------|
| `add_column_if_missing` helper | `crates/rs-core/src/db/mod.rs` | ~15 lines |
| `rename_column_if_old_exists` helper | `crates/rs-core/src/db/mod.rs` | ~20 lines |
| Migration runner refactor (`MigrationFn` list) | `crates/rs-core/src/db/mod.rs` | ~80 lines (10 migrations become programmatic; 7 stay raw) |
| Halt-on-failure | `src-tauri/src/lib.rs` | 5 lines |
| Fresh-DB convergence test | `crates/rs-core/src/db/tests.rs` | ~15 lines |
| Idempotency test (bug repro) | `crates/rs-core/src/db/tests.rs` | ~30 lines |
| Version bump (0.3.62 → 0.3.63) | `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` | 4 lines |

Total: roughly 170 lines across 7 files.

## Testing

- **Unit**: the two new tests above in `db/tests.rs` — both run in every CI Rust test job.
- **Existing coverage**: all existing `run_migrations` tests (`tests.rs`, `upload_tests.rs`, `template_tests.rs`, `delivery_log_tests.rs`, `delivery_status_tests.rs`) continue to exercise the full migration stack — the refactor must not break them.
- **Deployment**: after CI deploys to stream.lan, verify Restreamer service starts and dashboard Templates tab works. The stream.lan DB is already at max schema, so this is a regression check on the refactor.

## Error Handling

- Migration failure inside `run_migrations` already wraps each migration in a transaction; a failure rolls back the current migration and returns `Err`.
- At the call site, the new behavior is a hard process exit with notification, not a silent async-task return.
- The idempotent helpers degrade gracefully: if `pragma_table_info` returns no rows (table doesn't exist), `COUNT(*) > 0` is `false`, and we proceed to `ADD COLUMN`, which will then fail loudly if the table is genuinely missing — which is the correct behavior.

## Out of Scope

- Rewriting V3/V4 destructive rebuilds to be idempotent. Those run exactly once on the initial schema and are not part of the observed failure.
- Preventing startup when config is missing. The issue theory includes "don't open DB before config validates" as a preventive, but making migrations idempotent makes the preventive unnecessary — partial prior state is now safe.
- Error recovery UI in the dashboard. The tray-crash + Windows-notification flow is sufficient for the MVP. A dashboard-side banner can be added later if operator feedback warrants it.
