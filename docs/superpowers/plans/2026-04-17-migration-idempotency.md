# Migration Idempotency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ALTER TABLE ADD COLUMN` migrations idempotent, halt startup on migration failure, and add CI tests that prove fresh-install reaches max schema version. Closes #112.

**Architecture:** Refactor the migration runner in `crates/rs-core/src/db/mod.rs` to dispatch each version to either a raw-SQL executor (for pure CREATE TABLE/INDEX migrations) or a small async fn (for migrations containing ADD COLUMN / RENAME COLUMN). Add two helper fns — `add_column_if_missing` and `rename_column_if_old_exists` — that check `pragma_table_info` before executing DDL. Change the Tauri startup path to hard-exit on migration failure instead of silently returning from the async task.

**Tech Stack:** Rust 1.85 (edition 2024), sqlx 0.7 (SQLite, runtime queries only, no compile-time macros), tauri, tokio. Tests use `#[tokio::test]` with in-memory SQLite pools.

**Spec:** `docs/superpowers/specs/2026-04-17-migration-idempotency-design.md`

---

## Context

Fresh installs of v0.3.35 reached `schema_version = 4` and failed at V5 with `duplicate column name: auth_token`. `event_templates` never got created so the Templates tab was broken. Root cause: a prior interrupted run left the `auth_token` column present when V5's `ALTER TABLE ADD COLUMN` ran. Current code silently returns from the migrations task and leaves the tray app running with no backing service.

Total schema versions: **17**. The ALTER-containing migrations are V5, V6, V7 (rename), V9, V10, V11, V12, V14, V15, V17. V3, V4, V16 are destructive rebuilds (out of scope). V1, V2, V8, V13 are pure CREATE statements already using `IF NOT EXISTS`.

---

### Task 0: Version bump

Must be the first commit on `dev` per airuleset `version-bumping.md`.

**Files:**
- Modify: `Cargo.toml` (workspace version)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump version 0.3.62 → 0.3.63 in all four files**

```bash
# Sanity check current values
grep -m 1 '^version' Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml
grep '"version"' src-tauri/tauri.conf.json
```

All four must read `0.3.62`. Change to `0.3.63` using Edit tool (not sed).

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.63"
```

---

### Task 1: Add `MAX_SCHEMA_VERSION` constant and fresh-DB convergence test

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`
- Modify: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Add `MAX_SCHEMA_VERSION` constant**

In `crates/rs-core/src/db/mod.rs`, below the `create_memory_pool` function (around line 60, before `run_migrations`), add:

```rust
/// Maximum schema version. Must equal the highest version in the migration list.
/// Tests assert that `run_migrations` reaches this exact value.
pub const MAX_SCHEMA_VERSION: i32 = 17;
```

- [ ] **Step 2: Add fresh-DB convergence test**

Append to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn fresh_database_reaches_max_schema_version() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    let current: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(current, MAX_SCHEMA_VERSION);
}
```

- [ ] **Step 3: Local format check (CI will run the test itself — no local `cargo test`)**

```bash
cargo fmt --all --check
```

Per this project's rule (CLAUDE.md + auto-memory): NO local Rust builds. `cargo test` / `cargo check` run only on CI. Format is the one local gate.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "test: assert fresh DB reaches MAX_SCHEMA_VERSION (#112)"
```

---

### Task 2: Add failing idempotency test (RED)

**Files:**
- Modify: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn migrations_idempotent_when_schema_version_rewound() {
    // Simulates the #112 failure mode: a prior interrupted run left the
    // schema fully advanced but schema_version was rolled back to an older
    // value. Re-running migrations must succeed — ALTER TABLE ADD COLUMN
    // statements must no-op when the column already exists.
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    // Simulate rolled-back schema_version while schema is intact.
    sqlx::query("DELETE FROM schema_version WHERE version > 4")
        .execute(&pool)
        .await
        .unwrap();
    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 4, "precondition: schema_version rewound to 4");

    // Without the fix, V5 fails with 'duplicate column name: auth_token'.
    run_migrations(&pool).await.unwrap();

    let v: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, MAX_SCHEMA_VERSION);
}
```

- [ ] **Step 2: Commit the failing test as its own commit**

The commit history should show the test added before the fix, so git-blame makes the RED → GREEN transition visible. CI on this commit alone would fail — but per project rules we push the complete PR-ready set (test + fix together) as one push cycle. The separate commit preserves the TDD narrative in git-log.

```bash
git add crates/rs-core/src/db/tests.rs
git commit -m "test: add failing idempotency test reproducing #112"
```

---

### Task 3: Add idempotent DDL helpers

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`

- [ ] **Step 1: Add `add_column_if_missing` and `rename_column_if_old_exists`**

Insert directly above `run_migrations` in `crates/rs-core/src/db/mod.rs`:

```rust
/// Returns true if the column exists on the table, false otherwise.
///
/// Uses `pragma_table_info` as a table-valued function so the table name
/// can be interpolated safely (sqlx cannot bind PRAGMA arguments).
/// Table names must come from trusted code constants — never user input.
async fn column_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
) -> sqlx::Result<bool> {
    let query = format!("SELECT name FROM pragma_table_info('{table}') WHERE name = ?1");
    let row: Option<String> = sqlx::query_scalar(&query)
        .bind(column)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.is_some())
}

/// Idempotent `ALTER TABLE ... ADD COLUMN`. No-ops if the column already
/// exists. `col_def` is the full column definition including the column
/// name and type (e.g. `"auth_token TEXT NOT NULL DEFAULT ''"`).
async fn add_column_if_missing(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    column: &str,
    col_def: &str,
) -> sqlx::Result<()> {
    if column_exists(tx, table, column).await? {
        return Ok(());
    }
    sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {col_def}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Idempotent `ALTER TABLE ... RENAME COLUMN`. No-ops if `new_name`
/// already exists on the table.
async fn rename_column_if_old_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> sqlx::Result<()> {
    if column_exists(tx, table, new_name).await? {
        return Ok(());
    }
    sqlx::query(&format!(
        "ALTER TABLE {table} RENAME COLUMN {old_name} TO {new_name}"
    ))
    .execute(&mut **tx)
    .await?;
    Ok(())
}
```

- [ ] **Step 2: Format check**

```bash
cargo fmt --all --check
```

No local `cargo check` — CI compiles. Helpers will flag as unused until Task 4 wires them in; that's expected and resolved by Task 4 in the same push.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-core/src/db/mod.rs
git commit -m "feat(db): add idempotent DDL helpers (#112)"
```

---

### Task 4: Convert ALTER-containing migrations to programmatic form

This task is the core of the fix. We replace the `for (version, sql) in migrations` loop with a `match`-based dispatcher, and convert V5, V6, V7, V9, V10, V11, V12, V14, V15, V17 to small async functions that use the idempotent helpers.

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`

- [ ] **Step 1: Add `execute_sql_statements` helper**

Insert directly below the two helpers added in Task 3:

```rust
/// Execute a multi-statement SQL script inside a transaction.
/// Splits on `;` and skips empty statements.
/// Used for pure-DDL migrations that only contain CREATE TABLE / CREATE INDEX
/// (already idempotent via IF NOT EXISTS).
async fn execute_sql_statements(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sql: &str,
) -> sqlx::Result<()> {
    for statement in sql.split(';') {
        let trimmed = statement.trim();
        if !trimmed.is_empty() {
            sqlx::query(trimmed).execute(&mut **tx).await?;
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Replace V5–V7, V9–V12, V14, V15, V17 SQL constants with async fns**

Delete the following constants and replace them with async fns. Work through them in order. After all replacements are in place, the `MIGRATION_V5_SQL` through `MIGRATION_V15_SQL` constants no longer exist (keep V1, V2, V3, V4, V8, V13, V16; delete V5, V6, V7, V9, V10, V11, V12, V14, V15, V17).

Insert the following async fns between `execute_sql_statements` and the remaining migration SQL constants:

```rust
async fn migrate_v5(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "delivery_instances",
        "auth_token",
        "auth_token TEXT NOT NULL DEFAULT ''",
    )
    .await
}

async fn migrate_v6(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(tx, "chunk_records", "sent_at", "sent_at TEXT").await?;
    add_column_if_missing(
        tx,
        "delivery_endpoint_status",
        "bytes_processed_total",
        "bytes_processed_total INTEGER NOT NULL DEFAULT 0",
    )
    .await
}

async fn migrate_v7(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    rename_column_if_old_exists(
        tx,
        "delivery_endpoint_status",
        "buff_size_bytes",
        "chunks_processed",
    )
    .await
}

async fn migrate_v9(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "streaming_events",
        "cache_delay_secs",
        "cache_delay_secs INTEGER",
    )
    .await
}

async fn migrate_v10(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "chunk_format",
        "chunk_format TEXT NOT NULL DEFAULT 'ts'",
    )
    .await
}

async fn migrate_v11(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "duration_ms",
        "duration_ms INTEGER NOT NULL DEFAULT 0",
    )
    .await
}

async fn migrate_v12(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // CREATE TABLE statements use IF NOT EXISTS so they are already idempotent.
    // The ALTER TABLE ADD COLUMN at the end is the only non-idempotent part.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS event_templates (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL UNIQUE,
            cache_delay_secs INTEGER
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS template_endpoints (
            template_id INTEGER NOT NULL REFERENCES event_templates(id) ON DELETE CASCADE,
            endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
            PRIMARY KEY (template_id, endpoint_id)
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    add_column_if_missing(tx, "streaming_events", "created_from", "created_from TEXT").await
}

async fn migrate_v14(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "streaming_events",
        "rescue_video_url",
        "rescue_video_url TEXT",
    )
    .await
}

async fn migrate_v15(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "event_templates",
        "rescue_video_url",
        "rescue_video_url TEXT",
    )
    .await
}

async fn migrate_v17(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_attempts",
        "upload_attempts INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_first_attempt_at",
        "upload_first_attempt_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_completed_at",
        "upload_completed_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_duration_ms",
        "upload_duration_ms INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_last_error",
        "upload_last_error TEXT",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_next_retry_at",
        "upload_next_retry_at INTEGER",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "upload_failed_permanently",
        "upload_failed_permanently INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_chunks_upload_queue
          ON chunk_records(upload_failed_permanently, sent, in_process, upload_next_retry_at, id)
          WHERE sent = 0 AND in_process = 0 AND upload_failed_permanently = 0
        "#,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
```

- [ ] **Step 3: Rewrite the `run_migrations` loop to dispatch by version**

Replace the current `run_migrations` body (lines ~66–127 in the original file) with:

```rust
/// Run database migrations.
///
/// Each migration is wrapped in its own transaction so a failure rolls
/// back that one migration and halts startup with an error. ALTER TABLE
/// ADD COLUMN / RENAME COLUMN statements go through idempotent helpers
/// so partial prior state does not break resumption.
pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        .execute(pool)
        .await?;

    let current: i32 = sqlx::query("SELECT COALESCE(MAX(version), 0) as v FROM schema_version")
        .fetch_one(pool)
        .await
        .map(|r| r.get("v"))?;

    for version in (current + 1)..=MAX_SCHEMA_VERSION {
        let mut tx = pool.begin().await?;
        match version {
            1 => execute_sql_statements(&mut tx, MIGRATION_V1_SQL).await?,
            2 => execute_sql_statements(&mut tx, MIGRATION_V2_SQL).await?,
            3 => execute_sql_statements(&mut tx, MIGRATION_V3_SQL).await?,
            4 => execute_sql_statements(&mut tx, MIGRATION_V4_SQL).await?,
            5 => migrate_v5(&mut tx).await?,
            6 => migrate_v6(&mut tx).await?,
            7 => migrate_v7(&mut tx).await?,
            8 => execute_sql_statements(&mut tx, MIGRATION_V8_SQL).await?,
            9 => migrate_v9(&mut tx).await?,
            10 => migrate_v10(&mut tx).await?,
            11 => migrate_v11(&mut tx).await?,
            12 => migrate_v12(&mut tx).await?,
            13 => execute_sql_statements(&mut tx, MIGRATION_V13_SQL).await?,
            14 => migrate_v14(&mut tx).await?,
            15 => migrate_v15(&mut tx).await?,
            16 => execute_sql_statements(&mut tx, MIGRATION_V16_SQL).await?,
            17 => migrate_v17(&mut tx).await?,
            _ => unreachable!("unhandled migration version {version}"),
        }
        sqlx::query("INSERT OR REPLACE INTO schema_version (version) VALUES (?1)")
            .bind(version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }

    // Startup cleanup: delete old sent chunk records to keep the DB fast.
    // Without this, CI runs accumulate 100K+ rows making startup take >30s.
    let deleted: i64 = sqlx::query(
        "DELETE FROM chunk_records WHERE sent = 1 AND created_at < datetime('now', '-1 hour')",
    )
    .execute(pool)
    .await
    .map(|r| r.rows_affected() as i64)
    .unwrap_or(0);
    if deleted > 0 {
        tracing::info!("Cleaned {deleted} old chunk records from database");
    }

    Ok(())
}
```

- [ ] **Step 4: Delete obsolete constants**

Remove these constants from `crates/rs-core/src/db/mod.rs` (they're replaced by the async fns above):

- `MIGRATION_V5_SQL`
- `MIGRATION_V6_SQL`
- `MIGRATION_V7_SQL`
- `MIGRATION_V9_SQL`
- `MIGRATION_V10_SQL`
- `MIGRATION_V11_SQL`
- `MIGRATION_V12_SQL`
- `MIGRATION_V14_SQL`
- `MIGRATION_V15_SQL`
- `MIGRATION_V17_SQL`

Keep: `MIGRATION_V1_SQL`, `MIGRATION_V2_SQL`, `MIGRATION_V3_SQL`, `MIGRATION_V4_SQL`, `MIGRATION_V8_SQL`, `MIGRATION_V13_SQL`, `MIGRATION_V16_SQL`.

- [ ] **Step 5: Format check**

```bash
cargo fmt --all --check
```

CI will run both new tests (`fresh_database_reaches_max_schema_version`, `migrations_idempotent_when_schema_version_rewound`) plus the full existing `rs-core` test suite. Any regression in the refactor shows up there, not locally.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-core/src/db/mod.rs
git commit -m "fix(db): idempotent ALTER TABLE ADD COLUMN migrations (#112)"
```

---

### Task 5: Halt Tauri startup on migration failure

Currently `src-tauri/src/lib.rs:187` logs the error and returns from the async task, leaving the tray app alive with no backing service. Change this to a hard exit so the operator sees Restreamer.exe crash immediately, and Task Scheduler records the failure.

**Files:**
- Modify: `src-tauri/src/lib.rs`

- [ ] **Step 1: Change migration-failure branch to hard-exit**

In `src-tauri/src/lib.rs`, find:

```rust
                // Run migrations
                if let Err(e) = db::run_migrations(&pool).await {
                    tracing::error!("Failed to run migrations: {e}");
                    return;
                }
```

Replace with:

```rust
                // Run migrations — hard-exit on failure so the operator sees
                // the crash instead of a silently-broken tray app. Issue #112.
                if let Err(e) = db::run_migrations(&pool).await {
                    tracing::error!("Failed to run migrations: {e}");
                    eprintln!("FATAL: database migration failed: {e}");
                    eprintln!(
                        "See log file at C:\\ProgramData\\Restreamer\\logs\\ for details."
                    );
                    std::process::exit(1);
                }
```

Rationale: `eprintln!` goes to stderr which Task Scheduler captures; `tracing::error!` goes to the log file for forensics; `std::process::exit(1)` terminates the process so the tray icon goes away instead of lingering.

- [ ] **Step 2: Format check**

```bash
cargo fmt --all --check
```

CI's `Build Tauri app` job verifies this compiles. No local `cargo check`.

- [ ] **Step 3: Commit**

```bash
git add src-tauri/src/lib.rs
git commit -m "fix(tauri): exit on migration failure instead of silent broken state (#112)"
```

---

### Task 6: Push, monitor CI, create PR

- [ ] **Step 1: Final local check — format**

```bash
cargo fmt --all --check
```

Expected: no output (clean). If it reports differences, run `cargo fmt --all` and commit as a new commit (this project forbids `git commit --amend`).

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI**

```bash
gh run list --branch dev --limit 3
```

Identify the latest run ID, then wait:

```bash
sleep 300 && gh run view <run-id> --json status,conclusion,jobs
```

Per `airuleset/ci-monitoring.md`: one `sleep N && gh run view` background command, not `/loop`, not custom bash monitors.

Key jobs to verify green:
- `Lint (fmt + clippy)`
- `Test (ubuntu-latest)` — runs both new tests
- `Test (windows-latest)`
- `Coverage`
- `Mutation Testing` (incremental, may take 10+ min)
- `Build Tauri app` — verifies the `src-tauri/src/lib.rs` change compiles
- `Test integrity check`
- `File size check` — `crates/rs-core/src/db/mod.rs` grows by ~150 lines; check it stays under 1000

If the db/mod.rs line count approaches 1000, **stop and split the file** — typically move migrations to `crates/rs-core/src/db/migrations.rs`. Do not rush past the size gate.

- [ ] **Step 4: If file-size gate fires — split migrations into a module**

Only if Step 3 reports file-size violation. Otherwise skip.

Create `crates/rs-core/src/db/migrations.rs` and move all migration-related code into it:
- All `MIGRATION_V*_SQL` constants
- `MAX_SCHEMA_VERSION`
- `column_exists`, `add_column_if_missing`, `rename_column_if_old_exists`
- `execute_sql_statements`
- `migrate_v5` .. `migrate_v17`
- `run_migrations`

In `crates/rs-core/src/db/mod.rs`:

```rust
mod migrations;
pub use migrations::{MAX_SCHEMA_VERSION, run_migrations};
```

Re-run tests and push again.

- [ ] **Step 5: If CI fails on anything — investigate, fix, re-push ONCE**

Per `airuleset/ci-push-discipline.md`: one push, one CI cycle. Batch all fixes. Do not push streams of one-line fixes.

- [ ] **Step 6: Create PR from dev to main**

```bash
gh pr create --title "fix: idempotent migrations + halt on failure (#112)" --body "$(cat <<'EOF'
## Summary
- Idempotent `ALTER TABLE ADD COLUMN` helpers (`add_column_if_missing`, `rename_column_if_old_exists`) so partial prior state doesn't break resumption.
- Migration runner refactored to dispatch per-version (pure-SQL migrations use `execute_sql_statements`, ALTER-containing migrations use idempotent helpers).
- Tauri app now hard-exits on migration failure instead of silently returning from the async task.
- Two new tests: `fresh_database_reaches_max_schema_version` (regression guard) and `migrations_idempotent_when_schema_version_rewound` (reproduces the #112 failure).

Closes #112

## Test plan
- [ ] CI `Test (ubuntu-latest)` + `Test (windows-latest)` green — both new tests pass on both platforms
- [ ] `Mutation Testing` green — new helpers and `match` dispatcher don't survive mutation
- [ ] Manual: on stream.lan (already at max schema), confirm dashboard Templates tab still loads

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/$(gh pr view --json number --jq .number) --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `{mergeable: true, mergeable_state: "clean"}`. If "behind", `git fetch origin && git merge origin/main && git push`. If "blocked", fix the blocker.

- [ ] **Step 8: Monitor PR CI run to full green**

```bash
gh run list --branch dev --limit 3
sleep 300 && gh run view <pr-run-id>
```

Must reach terminal state with all jobs success before reporting complete. The `Deploy to stream.lan` step verifies the upgrade-path works on a real Windows machine (stream.lan's DB is already at `schema_version = 17`, so this exercises the "no-op path" of the new code).

---

### Verification

1. **Fresh-DB test green**: `fresh_database_reaches_max_schema_version` passes on Linux and Windows.
2. **Repro test green**: `migrations_idempotent_when_schema_version_rewound` passes — the `auth_token` duplicate-column error no longer occurs when schema is pre-populated.
3. **No regressions**: all existing `rs-core` tests (the ones that call `run_migrations` in setup) still pass.
4. **stream.lan deploy green**: `Deploy to stream.lan` job succeeds and Restreamer service starts.
5. **Halt-on-failure**: manual inspection of `src-tauri/src/lib.rs` shows `std::process::exit(1)` on migration failure instead of `return;`.
6. **Line count**: `crates/rs-core/src/db/mod.rs` stays under the 1000-line gate (split into `migrations.rs` only if the gate fires).
