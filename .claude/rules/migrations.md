---
paths:
  - "crates/rs-core/src/db/migrations.rs"
  - "crates/rs-core/src/db/migration_sql.rs"
  - "crates/rs-core/src/db/migration_tests.rs"
---

# DB migrations (rs-core `db::migrations`)

## Destructive table rebuilds MUST be idempotent against a schema_version rewind (#112, #125)

Most migrations are ALTER ADD/RENAME COLUMN (idempotent via `add_column_if_missing`
/ `rename_column_if_old_exists`) or `CREATE ... IF NOT EXISTS` DDL. The DANGEROUS
ones are **destructive table rebuilds** (`CREATE _new; INSERT SELECT; DROP old;
RENAME` — currently V3 `streaming_events`, V4 + V16 `delivery_instances`).

`run_migrations` loops `(current+1)..=MAX`, so if `schema_version` is ever rewound
below a rebuild's version while the schema is already advanced (corruption, manual
edit, the #112 interrupted-run class), that rebuild RE-RUNS against a
newer-than-its-era table and, unguarded, either crashes (its INSERT references a
dropped column / its narrow CHECK rejects a later status value) or DROPs columns
added by later migrations → data loss.

**Rule: every destructive rebuild guards on its POST-migration shape and no-ops if
already present.** Signals used (all cheap, `sqlite_master` / `pragma_table_info`):
- V3 → `column_exists("streaming_events", "name")` (V1 shape has `identifier`, V3+ has `name`).
- V4 → `delivery_status_check_allows("failed")` — CHECK already lists `'failed'`.
- V16 → `delivery_status_check_allows("booting")` — CHECK already lists `'booting'`.

The CHECK-token signal is sound because the runner is strictly sequential: any
migration that could rewrite a table's DDL runs AFTER the one that first added the
token, so `sql contains '<token>' ⟹ shape ≥ that migration`. Also `DROP TABLE IF
EXISTS <temp>` before the CREATE, so a crashed prior run's leftover temp can't wedge
the re-run. **Any NEW destructive rebuild you add must follow this same guard
pattern** (there is no way to `ALTER ... DROP CONSTRAINT` in SQLite, so CHECK
widening always means a rebuild).

## Historical quirks that look like bugs but aren't
- **V4 drops `auth_token`, V5 re-adds it `DEFAULT ''`.** A genuine forward V2→V5
  migration therefore RESETS `auth_token` to `''` (harmless: it's written at runtime
  after migrations). This is why the rewind tests (guard fires → skip → value
  preserved) and the legacy-forward test (guard doesn't fire → historical reset)
  assert OPPOSITE things about `auth_token`.
- **`PRAGMA foreign_keys=OFF` inside a migration is a NO-OP** — SQLite ignores it
  mid-transaction and `run_migrations` wraps each migration in `pool.begin()`. FK
  enforcement stays ON, so a rebuild's `DROP TABLE` cascades child rows on the
  forward path (harmless only because rebuilds run before any child rows exist).

## File-size: pure-DDL consts live in `migration_sql.rs`
`migrations.rs` is close to the 1000-line CI cap (`rust-crate-hygiene.md`). The six
pure-DDL `MIGRATION_V*_SQL` consts were moved to a sibling `migration_sql.rs`
(`#[path]` child module, `pub(crate)`). Put a new pure-DDL migration's SQL there;
keep guarded rebuild logic in `migrations.rs`.

## Testing rewind + forward paths (dev1 is Tier-0 — verify on dev2)
- Rewind (guard-fire) coverage: seed a fully-migrated DB, `DELETE FROM
  schema_version WHERE version > <keep>`, re-run, assert data preserved + version
  reaches MAX. keep=2/3/11/15 forces re-running V3/V4/V12/V16.
- Forward (guard-not-fire) coverage: build the real V1+V2 schema from the
  `migration_sql` consts, pin `schema_version=2`, seed pre-V3 rows, migrate to MAX.
- Prove RED before GREEN on dev2 with `git stash push -- <fix-file>` → rsync → run
  (fails) → `git stash pop` → rsync → run (passes). `MAX_SCHEMA_VERSION` and the
  `max_schema_version_constant` test must be bumped together when adding a migration.
