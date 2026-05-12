# ytbb Multi-Channel YT Health Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-endpoint OAuth linkage so `liveStreams.list` health (`streamStatus`, `healthStatus.status`, `configurationIssues[0].type`, `cdn.resolution`, `cdn.frameRate`) for the `ytbb` broadcast on its own YouTube channel is surfaced live in `DeliveryEndpointMetrics`, the operator dashboard, and the audit log.

**Architecture:** Existing single-row `youtube_oauth` table grows a `label` UNIQUE index so multiple OAuth grants coexist (`default`, `bb`, …). `EndpointConfig` gains optional `youtube_oauth_id`. A bounded host-side probe (15 s, per-OAuth not per-endpoint) calls `liveStreams.list(mine=true)` with the matching token, matches each YT_RTMP endpoint by `cdn.ingestionInfo.streamName == stream_key`, attaches `YoutubeHealth` to `DeliveryEndpointMetrics`, emits `Action::YoutubeIssueChanged` on transitions, and renders a Leptos badge. No host RTMP push code is touched.

**Tech Stack:** Rust 2024, sqlx + SQLite, axum, reqwest, wiremock, Leptos CSR (WASM), Playwright.

**Spec:** `docs/superpowers/specs/2026-05-12-ytbb-yt-health-diagnostic-design.md` (commit `ff79484`).

---

## Context

- Workspace at `0.9.0` on `dev` after PR #190 merged to `main`. Plan begins by bumping to `0.10.0`.
- `MAX_SCHEMA_VERSION` is currently `24` in `crates/rs-core/src/db/migrations.rs:16`. This plan adds `v25` (multi-row `youtube_oauth`) and `v26` (`endpoint_configs.youtube_oauth_id`).
- Existing `YouTubeOAuth` ops at `crates/rs-core/src/db/v2.rs:376-440` (`get_youtube_oauth`, `upsert_youtube_oauth`) keep working as the `label='default'` path.
- Existing `rs-youtube` OAuth refresh: `crates/rs-youtube/src/oauth.rs:91` (`refresh_access_token`).
- `Action` enum at `crates/rs-core/src/audit.rs:39`; last variant `EndpointStartChunkUpdated` at line 137.
- Task 1 files the GitHub issue; the captured number `$ISSUE_NUM` is referenced in every subsequent commit (e.g. `Closes #$ISSUE_NUM`).

**Local check policy:** `cargo fmt --all --check` ONLY. NO `cargo build`, `cargo test`, `cargo clippy` locally. Subagents must NOT push, compile, or run tests; the orchestrator (Task 21) pushes once after all tasks.

**TDD invariant:** every behavior-change task is a pair — RED commit (test that fails) immediately precedes GREEN commit (implementation that makes it pass). One commit per task.

---

### Task 1: File the GitHub issue

**Files:** none (gh CLI only).

- [ ] **Step 1: Create the tracking issue**

```bash
gh issue create \
  --title "Multi-channel YT health diagnostic (videoIngestionStarved observability for ytbb)" \
  --label "bug,observability" \
  --body "$(cat <<'EOF'
ytbb endpoint consistently shows YT Studio error "YouTube neprijíma video dosť rýchlo na to, aby bol stream plynulý." (== `videoIngestionStarved` / `bitrateLow` / `gopSizeLong`). Same key works directly from OBS. All other YT_RTMP endpoints succeed via the same restreamer push. The bb broadcast lives on a *different* YouTube channel so existing OAuth (`mine=true`) cannot probe it.

Root cause is not knowable from the host side without YT-API visibility into the bb stream object. This issue adds **diagnostic infrastructure** — not a fix. Once landed, the operator authorises the bb channel via `/youtube/oauth/start?label=bb`, links the `ytbb` endpoint, starts delivering, and the dashboard surfaces YT's precise `configurationIssues[]`. A follow-up issue captures the observed data and produces the actual fix.

Spec: `docs/superpowers/specs/2026-05-12-ytbb-yt-health-diagnostic-design.md` (commit ff79484).
Plan: `docs/superpowers/plans/2026-05-12-ytbb-yt-health-diagnostic.md`.

## Scope
- Multi-row `youtube_oauth` keyed by `label`.
- `EndpointConfig.youtube_oauth_id` linkage.
- 15 s probe loop attaching `YoutubeHealth` to `DeliveryEndpointMetrics`.
- `Action::YoutubeIssueChanged` audit row on transitions.
- Leptos badge + tooltip.
- `/youtube/oauth/start?label=...` + `/callback?label=...`.

## Out of scope
- The root-cause fix for ytbb (separate follow-up issue with captured data).
- Switching push from `rtmp://` to `rtmps://` (separate concern).
EOF
)"
```

- [ ] **Step 2: Capture the issue number**

```bash
ISSUE_NUM=$(gh issue list --search "Multi-channel YT health diagnostic" --json number --jq '.[0].number')
echo "ISSUE_NUM=$ISSUE_NUM"
```

Hold `$ISSUE_NUM` for every subsequent commit message (`Closes #$ISSUE_NUM` for the final commit; `Refs #$ISSUE_NUM` for intermediate ones).

---

### Task 2: Version bump 0.9.0 → 0.10.0

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump `Cargo.toml` workspace version**

In `Cargo.toml`, replace `version = "0.9.0"` with `version = "0.10.0"` in the `[workspace.package]` section (single occurrence near top of file).

- [ ] **Step 2: Bump `src-tauri/Cargo.toml`**

Replace `version = "0.9.0"` with `version = "0.10.0"`.

- [ ] **Step 3: Bump `src-tauri/tauri.conf.json`**

Replace `"version": "0.9.0"` with `"version": "0.10.0"`.

- [ ] **Step 4: Bump `leptos-ui/Cargo.toml`**

Replace `version = "0.9.0"` with `version = "0.10.0"`.

- [ ] **Step 5: Run formatting check**

```bash
cargo fmt --all --check
```

Expected: clean exit (TOML/JSON not touched by `fmt`; this is a guard against accidental Rust file edits).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.10.0 (Refs #$ISSUE_NUM)"
```

---

### Task 3 (RED): Migration v25 test — `youtube_oauth.label`

**Files:**
- Modify: `crates/rs-core/src/db/migration_tests.rs` (append the test at end)

- [ ] **Step 1: Append the failing test**

```rust
#[tokio::test]
async fn migration_v25_adds_label_unique_with_default_backfill() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    // 1. `label` and `channel_id` columns exist.
    let cols: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('youtube_oauth')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for expected in ["label", "channel_id"] {
        assert!(
            cols.iter().any(|c| c == expected),
            "youtube_oauth missing column {expected}; have {cols:?}"
        );
    }

    // 2. UNIQUE INDEX on label exists.
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='youtube_oauth'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        indexes.iter().any(|i| i == "idx_youtube_oauth_label"),
        "missing idx_youtube_oauth_label; have {indexes:?}"
    );

    // 3. UNIQUE actually enforces — two rows with same label must fail.
    sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a','r','u','c','s','sc')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let dup = sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a2','r2','u','c','s','sc')",
    )
    .execute(&pool)
    .await;
    assert!(dup.is_err(), "duplicate label should be rejected");

    // 4. Backfill: legacy row that pre-existed migration must have label='default'.
    // Simulate by inserting a row before re-running migrations on a fresh pool.
    let pool2 = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool2).await.unwrap();
    // Wipe label (simulate pre-v25 state for backfill audit).
    sqlx::query("UPDATE youtube_oauth SET label = '' WHERE id = 1")
        .execute(&pool2)
        .await
        .unwrap();
    crate::db::run_migrations(&pool2).await.unwrap();
    let label: Option<String> =
        sqlx::query_scalar("SELECT label FROM youtube_oauth WHERE id = 1")
            .fetch_optional(&pool2)
            .await
            .unwrap();
    assert_eq!(label.as_deref(), Some("default"), "backfill must restore 'default' label");
}
```

- [ ] **Step 2: Commit RED**

```bash
git add crates/rs-core/src/db/migration_tests.rs
git commit -m "test(migration): RED v25 label UNIQUE + channel_id + backfill (Refs #$ISSUE_NUM)"
```

CI will fail on this commit — that's expected (RED step). The orchestrator does not push until all GREEN pairs land.

---

### Task 4 (GREEN): Migration v25 implementation

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs:16` (bump `MAX_SCHEMA_VERSION`)
- Modify: `crates/rs-core/src/db/migrations.rs:343` (add v25 arm)
- Modify: `crates/rs-core/src/db/migrations.rs` (add `migrate_v25` fn)

- [ ] **Step 1: Bump `MAX_SCHEMA_VERSION`**

Change line 16 from:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 24;
```

to:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 25;
```

- [ ] **Step 2: Add the v25 dispatch arm**

In the `match version { ... }` block (currently ending at line 342 with `24 => migrate_v24(&mut tx).await?,`), insert:

```rust
            25 => migrate_v25(&mut tx).await?,
```

immediately before the `_ => unreachable!(...)` arm.

- [ ] **Step 3: Add the `migrate_v25` function**

Append after the last existing `migrate_vN` function in `migrations.rs`:

```rust
/// v25: support multiple YouTube OAuth grants keyed by `label`.
/// - ADD COLUMN `label TEXT NOT NULL DEFAULT 'default'`
/// - ADD COLUMN `channel_id TEXT` (nullable; populated when we observe a
///   liveStream's `snippet.channelId`)
/// - Backfill any empty/NULL `label` to 'default' (defensive for the
///   #112 rewound-schema_version recovery path)
/// - CREATE UNIQUE INDEX on `label`
async fn migrate_v25(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "youtube_oauth",
        "label",
        "label TEXT NOT NULL DEFAULT 'default'",
    )
    .await?;
    add_column_if_missing(tx, "youtube_oauth", "channel_id", "channel_id TEXT").await?;
    sqlx::query("UPDATE youtube_oauth SET label = 'default' WHERE label IS NULL OR label = ''")
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_youtube_oauth_label ON youtube_oauth(label)",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
```

- [ ] **Step 4: Run formatting check**

```bash
cargo fmt --all --check
```

Expected: clean.

- [ ] **Step 5: Commit GREEN**

```bash
git add crates/rs-core/src/db/migrations.rs
git commit -m "feat(db): v25 multi-row youtube_oauth keyed by label (Refs #$ISSUE_NUM)"
```

---

### Task 5 (RED): Migration v26 test — `endpoint_configs.youtube_oauth_id`

**Files:**
- Modify: `crates/rs-core/src/db/migration_tests.rs` (append)

- [ ] **Step 1: Append the failing test**

```rust
#[tokio::test]
async fn migration_v26_adds_youtube_oauth_id_to_endpoint_configs() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::run_migrations(&pool).await.unwrap(); // idempotent

    // 1. Column exists.
    let cols: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT name, type, \"notnull\" FROM pragma_table_info('endpoint_configs')",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let oauth_col = cols
        .iter()
        .find(|(n, _, _)| n == "youtube_oauth_id")
        .expect("endpoint_configs must have youtube_oauth_id column");
    assert!(
        oauth_col.1.to_uppercase().contains("INTEGER"),
        "youtube_oauth_id must be INTEGER; got type={}",
        oauth_col.1
    );
    assert_eq!(oauth_col.2, 0, "youtube_oauth_id must be nullable");

    // 2. New endpoints default to NULL.
    sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('e1','YT_RTMP','k')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let oauth_id: Option<i64> =
        sqlx::query_scalar("SELECT youtube_oauth_id FROM endpoint_configs WHERE alias = 'e1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(oauth_id.is_none(), "new row must default to NULL");

    // 3. Linkage works: insert oauth row, link endpoint, read back.
    sqlx::query(
        "INSERT INTO youtube_oauth (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes)
         VALUES ('bb','a','r','u','c','s','sc')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let bb_id: i64 =
        sqlx::query_scalar("SELECT id FROM youtube_oauth WHERE label = 'bb'")
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE endpoint_configs SET youtube_oauth_id = ?1 WHERE alias = 'e1'")
        .bind(bb_id)
        .execute(&pool)
        .await
        .unwrap();
    let read_back: i64 =
        sqlx::query_scalar("SELECT youtube_oauth_id FROM endpoint_configs WHERE alias = 'e1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(read_back, bb_id);
}
```

- [ ] **Step 2: Commit RED**

```bash
git add crates/rs-core/src/db/migration_tests.rs
git commit -m "test(migration): RED v26 endpoint_configs.youtube_oauth_id (Refs #$ISSUE_NUM)"
```

---

### Task 6 (GREEN): Migration v26 + `EndpointConfig` field + v2 queries

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs:16` (`MAX_SCHEMA_VERSION = 26`)
- Modify: `crates/rs-core/src/db/migrations.rs` (v26 arm + fn)
- Modify: `crates/rs-core/src/models.rs:77-98` (add field on `EndpointConfig`)
- Modify: `crates/rs-core/src/db/v2.rs:22-127` (SELECT/INSERT/UPDATE update)

- [ ] **Step 1: Bump `MAX_SCHEMA_VERSION` to 26**

```rust
pub const MAX_SCHEMA_VERSION: i32 = 26;
```

- [ ] **Step 2: Add v26 dispatch arm**

Inside the `match version { ... }` block, after the `25 => migrate_v25(...)` line:

```rust
            26 => migrate_v26(&mut tx).await?,
```

- [ ] **Step 3: Add `migrate_v26` function**

Append in `migrations.rs`:

```rust
/// v26: link each endpoint to an optional YouTube OAuth grant by id.
/// NULL means "no health probe" (matches existing behavior).
async fn migrate_v26(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    add_column_if_missing(
        tx,
        "endpoint_configs",
        "youtube_oauth_id",
        "youtube_oauth_id INTEGER REFERENCES youtube_oauth(id)",
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 4: Extend `EndpointConfig` struct**

In `crates/rs-core/src/models.rs`, change the struct at line 77:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub id: i64,
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    pub enabled: bool,
    pub position_last: i64,
    pub delivered_bytes: i64,
    pub is_fast: bool,
    #[serde(default)]
    pub pusher: PusherKind,
    #[serde(default)]
    pub prefetch_chunks: Option<u32>,
    /// FK into `youtube_oauth(id)`. `None` ⇒ no YT health probe.
    /// `#[serde(default)]` keeps existing config.json files parsing.
    #[serde(default)]
    pub youtube_oauth_id: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}
```

- [ ] **Step 5: Update `list_endpoint_configs` in v2.rs**

Replace the body at `crates/rs-core/src/db/v2.rs:22-48`:

```rust
pub async fn list_endpoint_configs(pool: &SqlitePool) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, pusher, youtube_oauth_id, created_at, updated_at
         FROM endpoint_configs ORDER BY id",
    )
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
            pusher: parse_pusher_kind(r.get("pusher")),
            prefetch_chunks: None,
            youtube_oauth_id: r.get("youtube_oauth_id"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}
```

- [ ] **Step 6: Update `get_endpoint_config` in v2.rs**

Replace the body at `crates/rs-core/src/db/v2.rs:50-74`:

```rust
pub async fn get_endpoint_config(pool: &SqlitePool, id: i64) -> Result<Option<EndpointConfig>> {
    let row = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, pusher, youtube_oauth_id, created_at, updated_at
         FROM endpoint_configs WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| EndpointConfig {
        id: r.get("id"),
        alias: r.get("alias"),
        service_type: r.get("service_type"),
        stream_key: r.get("stream_key"),
        enabled: r.get::<i32, _>("enabled") != 0,
        position_last: r.get("position_last"),
        delivered_bytes: r.get("delivered_bytes"),
        is_fast: r.get::<i32, _>("is_fast") != 0,
        pusher: parse_pusher_kind(r.get("pusher")),
        prefetch_chunks: None,
        youtube_oauth_id: r.get("youtube_oauth_id"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }))
}
```

- [ ] **Step 7: Update `get_event_endpoints` in v2.rs**

Replace the body at `crates/rs-core/src/db/v2.rs:157-187`:

```rust
pub async fn get_event_endpoints(pool: &SqlitePool, event_id: i64) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT e.id, e.alias, e.service_type, e.stream_key, e.enabled, e.position_last,
         e.delivered_bytes, e.is_fast, e.pusher, e.youtube_oauth_id, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN event_endpoints ee ON ee.endpoint_id = e.id
         WHERE ee.event_id = ?1 AND e.enabled = 1
         ORDER BY e.id",
    )
    .bind(event_id)
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
            pusher: parse_pusher_kind(r.get("pusher")),
            prefetch_chunks: None,
            youtube_oauth_id: r.get("youtube_oauth_id"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}
```

- [ ] **Step 8: Add new setter `set_endpoint_youtube_oauth_id` to v2.rs**

Append after `delete_endpoint_config` (around line 127):

```rust
/// Link or unlink an endpoint's YouTube OAuth grant.
pub async fn set_endpoint_youtube_oauth_id(
    pool: &SqlitePool,
    endpoint_id: i64,
    oauth_id: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE endpoint_configs SET youtube_oauth_id = ?1, updated_at = datetime('now')
         WHERE id = ?2",
    )
    .bind(oauth_id)
    .bind(endpoint_id)
    .execute(pool)
    .await?;
    Ok(())
}
```

- [ ] **Step 9: Append serde round-trip test in models.rs tests block**

Find the existing `#[cfg(test)] mod tests { ... }` in `crates/rs-core/src/models.rs` (around line 770) and add inside it:

```rust
    #[test]
    fn endpoint_config_serde_preserves_youtube_oauth_id() {
        let json_some = r#"{
            "id": 1, "alias": "ytbb", "service_type": "YT_RTMP", "stream_key": "k",
            "enabled": true, "position_last": 0, "delivered_bytes": 0, "is_fast": false,
            "pusher": "rust", "youtube_oauth_id": 42,
            "created_at": "2026-05-12T00:00:00Z", "updated_at": "2026-05-12T00:00:00Z"
        }"#;
        let parsed: EndpointConfig = serde_json::from_str(json_some).unwrap();
        assert_eq!(parsed.youtube_oauth_id, Some(42));

        // Field absent => None (backward compat with pre-v26 config.json).
        let json_missing = r#"{
            "id": 1, "alias": "ytbb", "service_type": "YT_RTMP", "stream_key": "k",
            "enabled": true, "position_last": 0, "delivered_bytes": 0, "is_fast": false,
            "pusher": "rust",
            "created_at": "2026-05-12T00:00:00Z", "updated_at": "2026-05-12T00:00:00Z"
        }"#;
        let parsed2: EndpointConfig = serde_json::from_str(json_missing).unwrap();
        assert_eq!(parsed2.youtube_oauth_id, None);
    }
```

- [ ] **Step 10: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-core/src/db/migrations.rs crates/rs-core/src/models.rs crates/rs-core/src/db/v2.rs
git commit -m "feat(db): v26 endpoint_configs.youtube_oauth_id linkage (Refs #$ISSUE_NUM)"
```

---

### Task 7 (RED): `youtube_oauth` DB ops tests

**Files:**
- Create: `crates/rs-core/src/db/youtube_oauth_tests.rs`
- Modify: `crates/rs-core/src/db/mod.rs` (register the new module)

- [ ] **Step 1: Register the test module**

In `crates/rs-core/src/db/mod.rs`, add a `#[cfg(test)] mod youtube_oauth_tests;` line alongside the other test modules.

- [ ] **Step 2: Create the test file**

```rust
//! Tests for `crate::db::youtube_oauth` multi-account ops.

use crate::db::{create_memory_pool, run_migrations};
use crate::db::youtube_oauth as yo;

async fn fresh_pool() -> sqlx::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn upsert_by_label_inserts_then_updates_same_row() {
    let pool = fresh_pool().await;
    let id1 = yo::upsert_oauth_by_label(
        &pool, "bb", "a1", "r1", "https://oauth2.googleapis.com/token",
        "cid", "csec", "scope1", Some("2026-05-12T00:00:00Z"),
    )
    .await
    .unwrap();
    let id2 = yo::upsert_oauth_by_label(
        &pool, "bb", "a2", "r2", "https://oauth2.googleapis.com/token",
        "cid", "csec", "scope2", Some("2027-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    assert_eq!(id1, id2, "upsert by label must update same row, not duplicate");

    let row = yo::get_oauth_by_label(&pool, "bb").await.unwrap().unwrap();
    assert_eq!(row.access_token, "a2");
    assert_eq!(row.scopes, "scope2");
}

#[tokio::test]
async fn get_by_label_returns_none_for_unknown_label() {
    let pool = fresh_pool().await;
    assert!(yo::get_oauth_by_label(&pool, "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn get_by_id_resolves_correct_label() {
    let pool = fresh_pool().await;
    let id = yo::upsert_oauth_by_label(
        &pool, "bb", "a", "r", "u", "c", "s", "sc", None,
    )
    .await
    .unwrap();
    let row = yo::get_oauth_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.access_token, "a");
}

#[tokio::test]
async fn list_returns_default_and_bb() {
    let pool = fresh_pool().await;
    yo::upsert_oauth_by_label(&pool, "bb", "a", "r", "u", "c", "s", "sc", None)
        .await
        .unwrap();
    // The migration auto-creates the default row; verify both come back.
    let all = yo::list_oauths(&pool).await.unwrap();
    let labels: Vec<&str> = all.iter().map(|o| o.label.as_str()).collect();
    assert!(labels.contains(&"default"), "have {labels:?}");
    assert!(labels.contains(&"bb"), "have {labels:?}");
}
```

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-core/src/db/youtube_oauth_tests.rs crates/rs-core/src/db/mod.rs
git commit -m "test(db): RED multi-label youtube_oauth ops (Refs #$ISSUE_NUM)"
```

---

### Task 8 (GREEN): Implement `crates/rs-core/src/db/youtube_oauth.rs`

**Files:**
- Create: `crates/rs-core/src/db/youtube_oauth.rs`
- Modify: `crates/rs-core/src/db/mod.rs` (register the module + re-export)
- Modify: `crates/rs-core/src/models.rs` (extend `YouTubeOAuth` struct with `label` and `channel_id`)

- [ ] **Step 1: Extend `YouTubeOAuth` struct**

In `crates/rs-core/src/models.rs:139-148`, replace the struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeOAuth {
    pub id: i64,
    /// Human-readable label uniquely identifying this grant
    /// (e.g. `default`, `bb`). Used by endpoint linkage and OAuth flow `?label=`.
    pub label: String,
    pub access_token: String,
    pub refresh_token: String,
    pub token_uri: String,
    pub client_id: String,
    pub client_secret: String,
    pub scopes: String,
    pub expires_at: Option<String>,
    /// Captured from `liveStreams.list` items' `snippet.channelId` after the
    /// first successful probe. Helps disambiguate when an operator has
    /// multiple labels pointing at the same channel.
    pub channel_id: Option<String>,
}
```

- [ ] **Step 2: Update existing `get_youtube_oauth` / `upsert_youtube_oauth` callsites**

In `crates/rs-core/src/db/v2.rs:378-440` extend `get_youtube_oauth` SELECT and struct-build to include `label` and `channel_id`:

```rust
pub async fn get_youtube_oauth(pool: &SqlitePool) -> Result<Option<YouTubeOAuth>> {
    let row = sqlx::query(
        "SELECT id, label, access_token, refresh_token, token_uri, client_id, client_secret,
         scopes, expires_at, channel_id
         FROM youtube_oauth WHERE label = 'default'",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| YouTubeOAuth {
        id: r.get("id"),
        label: r.get("label"),
        access_token: r.get("access_token"),
        refresh_token: r.get("refresh_token"),
        token_uri: r.get("token_uri"),
        client_id: r.get("client_id"),
        client_secret: r.get("client_secret"),
        scopes: r.get("scopes"),
        expires_at: r.get("expires_at"),
        channel_id: r.get("channel_id"),
    }))
}
```

And update `upsert_youtube_oauth` to UPSERT keyed on `label = 'default'` (existing default-row semantics preserved):

```rust
#[allow(clippy::too_many_arguments)]
pub async fn upsert_youtube_oauth(
    pool: &SqlitePool,
    access_token: &str,
    refresh_token: &str,
    token_uri: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &str,
    expires_at: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO youtube_oauth
            (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at)
         VALUES ('default', ?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(label) DO UPDATE SET
            access_token = excluded.access_token,
            refresh_token = excluded.refresh_token,
            token_uri = excluded.token_uri,
            client_id = excluded.client_id,
            client_secret = excluded.client_secret,
            scopes = excluded.scopes,
            expires_at = excluded.expires_at",
    )
    .bind(access_token)
    .bind(refresh_token)
    .bind(token_uri)
    .bind(client_id)
    .bind(client_secret)
    .bind(scopes)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}
```

- [ ] **Step 3: Create the new module file**

Create `crates/rs-core/src/db/youtube_oauth.rs`:

```rust
//! Multi-account YouTube OAuth ops. Each grant is keyed by a unique `label`.
//!
//! The single-row legacy ops in `db::v2` (`get_youtube_oauth`,
//! `upsert_youtube_oauth`) keep working as the `label = 'default'` path.

use crate::error::Result;
use crate::models::YouTubeOAuth;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

const SELECT_COLS: &str =
    "id, label, access_token, refresh_token, token_uri, client_id, client_secret, scopes, \
     expires_at, channel_id";

fn row_to_oauth(r: sqlx::sqlite::SqliteRow) -> YouTubeOAuth {
    YouTubeOAuth {
        id: r.get("id"),
        label: r.get("label"),
        access_token: r.get("access_token"),
        refresh_token: r.get("refresh_token"),
        token_uri: r.get("token_uri"),
        client_id: r.get("client_id"),
        client_secret: r.get("client_secret"),
        scopes: r.get("scopes"),
        expires_at: r.get("expires_at"),
        channel_id: r.get("channel_id"),
    }
}

pub async fn get_oauth_by_label(pool: &SqlitePool, label: &str) -> Result<Option<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth WHERE label = ?1");
    let row = sqlx::query(&q).bind(label).fetch_optional(pool).await?;
    Ok(row.map(row_to_oauth))
}

pub async fn get_oauth_by_id(pool: &SqlitePool, id: i64) -> Result<Option<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth WHERE id = ?1");
    let row = sqlx::query(&q).bind(id).fetch_optional(pool).await?;
    Ok(row.map(row_to_oauth))
}

pub async fn list_oauths(pool: &SqlitePool) -> Result<Vec<YouTubeOAuth>> {
    let q = format!("SELECT {SELECT_COLS} FROM youtube_oauth ORDER BY label");
    let rows = sqlx::query(&q).fetch_all(pool).await?;
    Ok(rows.into_iter().map(row_to_oauth).collect())
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_oauth_by_label(
    pool: &SqlitePool,
    label: &str,
    access_token: &str,
    refresh_token: &str,
    token_uri: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &str,
    expires_at: Option<&str>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO youtube_oauth
            (label, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(label) DO UPDATE SET
            access_token = excluded.access_token,
            refresh_token = excluded.refresh_token,
            token_uri = excluded.token_uri,
            client_id = excluded.client_id,
            client_secret = excluded.client_secret,
            scopes = excluded.scopes,
            expires_at = excluded.expires_at
         RETURNING id",
    )
    .bind(label)
    .bind(access_token)
    .bind(refresh_token)
    .bind(token_uri)
    .bind(client_id)
    .bind(client_secret)
    .bind(scopes)
    .bind(expires_at)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn set_channel_id(pool: &SqlitePool, id: i64, channel_id: &str) -> Result<()> {
    sqlx::query("UPDATE youtube_oauth SET channel_id = ?1 WHERE id = ?2")
        .bind(channel_id)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 4: Register module in `mod.rs`**

In `crates/rs-core/src/db/mod.rs`, alongside existing `pub mod v2;`, add:

```rust
pub mod youtube_oauth;
```

- [ ] **Step 5: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-core/src/db/youtube_oauth.rs crates/rs-core/src/db/mod.rs crates/rs-core/src/db/v2.rs crates/rs-core/src/models.rs
git commit -m "feat(db): multi-label youtube_oauth ops (Refs #$ISSUE_NUM)"
```

---

### Task 9 (RED): `list_streams_for_label` wiremock test

**Files:**
- Create: `crates/rs-youtube/src/streams_for_label_tests.rs`
- Modify: `crates/rs-youtube/src/lib.rs` (register the test module)

- [ ] **Step 1: Add a `wiremock` dev-dependency**

Open `crates/rs-youtube/Cargo.toml`. Add under `[dev-dependencies]` (create the section if missing):

```toml
[dev-dependencies]
wiremock = "0.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
sqlx = { workspace = true, features = ["runtime-tokio", "sqlite"] }
rs-core = { path = "../rs-core" }
```

(If `rs-core` is already a normal `dependencies` entry, the dev-dep entry is harmless but optional — leave only the missing ones.)

- [ ] **Step 2: Register test module**

Append to `crates/rs-youtube/src/lib.rs`:

```rust
#[cfg(test)]
mod streams_for_label_tests;
```

- [ ] **Step 3: Create the test file**

```rust
//! Tests for `list_streams_for_label`: token refresh + correct bearer per label.

use crate::streams::list_streams_for_label;
use rs_core::db::{create_memory_pool, run_migrations, youtube_oauth as yo};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn pool_with_label(label: &str, access_token: &str, expires_at: Option<&str>) -> sqlx::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(
        &pool, label, access_token, "refresh-token",
        "https://oauth2.googleapis.com/token", "cid", "csec",
        "https://www.googleapis.com/auth/youtube.readonly", expires_at,
    )
    .await
    .unwrap();
    pool
}

#[tokio::test]
async fn list_streams_for_label_sends_bearer_for_that_label() {
    let server = MockServer::start().await;
    // Set base URL via env so streams.rs uses it instead of googleapis.
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }

    let pool = pool_with_label("bb", "TOK-BB", Some("2099-01-01T00:00:00Z")).await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(query_param("mine", "true"))
        .and(header("authorization", "Bearer TOK-BB"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"items":[]}"#))
        .expect(1)
        .mount(&server)
        .await;

    let streams = list_streams_for_label(&pool, "bb").await.unwrap();
    assert!(streams.is_empty());
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}

#[tokio::test]
async fn list_streams_for_label_refreshes_when_expired() {
    let server = MockServer::start().await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }
    // Token endpoint also lives on the mock server.
    let token_uri = format!("{}/token", server.uri());
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    yo::upsert_oauth_by_label(
        &pool, "bb", "OLD-TOK", "refresh-bb",
        &token_uri, "cid", "csec",
        "https://www.googleapis.com/auth/youtube.readonly",
        Some("2000-01-01T00:00:00Z"), // expired
    )
    .await
    .unwrap();

    // Token refresh returns NEW-TOK.
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"NEW-TOK","expires_in":3600,"token_type":"Bearer"}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer NEW-TOK"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"items":[]}"#))
        .expect(1)
        .mount(&server)
        .await;

    let _ = list_streams_for_label(&pool, "bb").await.unwrap();

    // Verify the refreshed token was persisted.
    let row = yo::get_oauth_by_label(&pool, "bb").await.unwrap().unwrap();
    assert_eq!(row.access_token, "NEW-TOK");
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}
```

- [ ] **Step 4: Commit RED**

```bash
git add crates/rs-youtube/Cargo.toml crates/rs-youtube/src/lib.rs crates/rs-youtube/src/streams_for_label_tests.rs
git commit -m "test(rs-youtube): RED list_streams_for_label (Refs #$ISSUE_NUM)"
```

---

### Task 10 (GREEN): Implement `list_streams_for_label` + `StreamCdn.ingestion_info`

**Files:**
- Modify: `crates/rs-youtube/src/streams.rs` (parameterize base URL, extend `StreamCdn`, add `list_streams_for_label`)

- [ ] **Step 1: Parameterize base URL**

At the top of `crates/rs-youtube/src/streams.rs`, replace:

```rust
const YOUTUBE_API_BASE: &str = "https://www.googleapis.com/youtube/v3";
```

with:

```rust
/// Default YouTube Data API base URL. Overridable via the
/// `YOUTUBE_API_BASE` environment variable for tests (wiremock).
fn youtube_api_base() -> String {
    std::env::var("YOUTUBE_API_BASE")
        .unwrap_or_else(|_| "https://www.googleapis.com/youtube/v3".to_string())
}
```

Update every existing `format!("{YOUTUBE_API_BASE}/liveStreams")` (and similar) in this file to `format!("{}/liveStreams", youtube_api_base())`.

- [ ] **Step 2: Extend `StreamCdn` with `ingestion_info`**

In the existing `StreamCdn` struct (around line 33 of `streams.rs`), append a new field and define `IngestionInfo`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCdn {
    #[serde(default)]
    pub ingestion_type: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub frame_rate: Option<String>,
    #[serde(default)]
    pub ingestion_info: Option<IngestionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestionInfo {
    #[serde(default)]
    pub stream_name: Option<String>,
    #[serde(default)]
    pub ingestion_address: Option<String>,
    #[serde(default)]
    pub backup_ingestion_address: Option<String>,
}
```

- [ ] **Step 3: Add `list_streams_for_label`**

Append at the end of `crates/rs-youtube/src/streams.rs`:

```rust
/// Refresh-if-needed wrapper around `list_live_streams`. Uses the OAuth
/// grant identified by `label` from `youtube_oauth`.
///
/// - If `expires_at` is in the past (or absent), refreshes via the
///   `token_uri` and persists the new access token + expiry.
/// - Always passes the resulting bearer to `liveStreams.list(mine=true)`.
pub async fn list_streams_for_label(
    pool: &sqlx::SqlitePool,
    label: &str,
) -> crate::Result<Vec<LiveStream>> {
    use rs_core::db::youtube_oauth as yo;

    let mut oauth = yo::get_oauth_by_label(pool, label)
        .await
        .map_err(|e| crate::YouTubeError::Db(e.to_string()))?
        .ok_or_else(|| crate::YouTubeError::OAuth(format!("no oauth grant for label '{label}'")))?;

    if crate::oauth::is_token_expired(oauth.expires_at.as_deref()) {
        let tokens = crate::oauth::OAuthTokens {
            access_token: oauth.access_token.clone(),
            refresh_token: oauth.refresh_token.clone(),
            token_uri: oauth.token_uri.clone(),
            client_id: oauth.client_id.clone(),
            client_secret: oauth.client_secret.clone(),
        };
        let refreshed = crate::oauth::refresh_access_token(&tokens).await?;
        let new_expires = chrono::Utc::now()
            + chrono::Duration::seconds(refreshed.expires_in.unwrap_or(3600));
        let new_expires_str = new_expires.to_rfc3339();
        yo::upsert_oauth_by_label(
            pool,
            label,
            &refreshed.access_token,
            &oauth.refresh_token,
            &oauth.token_uri,
            &oauth.client_id,
            &oauth.client_secret,
            &oauth.scopes,
            Some(&new_expires_str),
        )
        .await
        .map_err(|e| crate::YouTubeError::Db(e.to_string()))?;
        oauth.access_token = refreshed.access_token;
        oauth.expires_at = Some(new_expires_str);
    }

    list_live_streams(&oauth.access_token).await
}
```

- [ ] **Step 4: Add a `Db` variant to `YouTubeError`**

In `crates/rs-youtube/src/lib.rs`, the existing `enum YouTubeError` (search for `pub enum YouTubeError`) needs a `Db(String)` variant. Add:

```rust
    #[error("DB error: {0}")]
    Db(String),
```

- [ ] **Step 5: Add `rs-core` as a non-dev dependency of `rs-youtube`**

In `crates/rs-youtube/Cargo.toml` under `[dependencies]`:

```toml
rs-core = { path = "../rs-core" }
chrono = { workspace = true, features = ["clock"] }
sqlx = { workspace = true }
```

(Skip lines that already exist.)

- [ ] **Step 6: Ensure `TokenResponse.expires_in` is exposed**

Check `crates/rs-youtube/src/oauth.rs` — the `TokenResponse` struct must derive `Deserialize` and have a `pub expires_in: Option<i64>` field. If absent, add it.

- [ ] **Step 7: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-youtube/Cargo.toml crates/rs-youtube/src/streams.rs crates/rs-youtube/src/lib.rs crates/rs-youtube/src/oauth.rs
git commit -m "feat(rs-youtube): list_streams_for_label + ingestion_info (Refs #$ISSUE_NUM)"
```

---

### Task 11 (RED): `YoutubeHealth` extraction + `Action::YoutubeIssueChanged` test

**Files:**
- Create: `crates/rs-api/src/yt_health_extract_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (register module)
- Modify: `crates/rs-core/src/audit.rs` (add `YoutubeIssueChanged` variant — compile-only, no impl wiring yet)
- Modify: `crates/rs-core/src/models.rs` (add `YoutubeHealth` struct + `youtube_health` field on `DeliveryEndpointMetrics`)

- [ ] **Step 1: Add the `YoutubeIssueChanged` audit variant**

In `crates/rs-core/src/audit.rs`, immediately before the closing `}` of `pub enum Action { ... }` (currently around line 138), append:

```rust
    /// Host-side: YT health probe observed `configurationIssues[0].type`
    /// change for an endpoint. Detail JSON:
    /// `{endpoint_alias, from: Option<String>, to: Option<String>}`.
    /// Bounded at most once per 30 s per endpoint.
    YoutubeIssueChanged,
```

- [ ] **Step 2: Add the `YoutubeHealth` struct + field**

In `crates/rs-core/src/models.rs`, immediately before the `pub struct DeliveryEndpointMetrics` definition (line 251), add:

```rust
/// Snapshot of YT `liveStreams.list` health for a single endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct YoutubeHealth {
    /// `status.streamStatus` (`active` | `ready` | `inactive` | …).
    pub stream_status: String,
    /// `status.healthStatus.status` (`good` | `ok` | `bad` | `noData` | …).
    pub health_status: String,
    /// `status.healthStatus.configurationIssues[0].type` if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_issue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_rate: Option<String>,
    /// Seconds since the data was probed (filled at serialization time).
    #[serde(default)]
    pub age_secs: i64,
    /// Set when the probe could not run (`oauth_invalid`,
    /// `oauth_app_not_production`, `oauth_missing`, `stream_not_in_mine_list`,
    /// `quota_exceeded`, `network_timeout`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
```

Then in `pub struct DeliveryEndpointMetrics { ... }` (line 252) append before the closing `}`:

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub youtube_health: Option<YoutubeHealth>,
```

- [ ] **Step 3: Register the new test module**

Append in `crates/rs-api/src/lib.rs`:

```rust
#[cfg(test)]
mod yt_health_extract_tests;
```

Also expose the soon-to-exist helpers (Task 12 fills them in) by adding a `pub mod delivery_yt_health;` line and creating a placeholder file `crates/rs-api/src/delivery_yt_health.rs`:

```rust
//! YT health extraction + audit emission helpers.
//! Implementation lands in the corresponding GREEN task. The functions
//! are referenced by RED tests and by `delivery_status::attach_yt_health`.

use rs_core::audit::Action;
use rs_core::models::YoutubeHealth;
use rs_youtube::streams::LiveStream;

/// Extract the top-priority `YoutubeHealth` from a single liveStream item.
pub fn extract_top_issue(stream: &LiveStream) -> YoutubeHealth {
    let _ = stream;
    unimplemented!("filled in by GREEN task")
}

/// Decide whether an `Action::YoutubeIssueChanged` row should be emitted
/// given prior + current top_issue values for an endpoint.
pub fn issue_changed_action(
    prior: Option<&str>,
    current: Option<&str>,
) -> Option<(Action, Option<String>, Option<String>)> {
    let _ = (prior, current);
    unimplemented!("filled in by GREEN task")
}
```

- [ ] **Step 4: Create the failing test file**

`crates/rs-api/src/yt_health_extract_tests.rs`:

```rust
use crate::delivery_yt_health::{extract_top_issue, issue_changed_action};
use rs_core::audit::Action;
use rs_youtube::streams::{
    ConfigurationIssue, HealthStatus, IngestionInfo, LiveStream, StreamCdn, StreamSnippet,
    StreamStatus,
};

fn liveStream_with(top_issue: Option<&str>, health: &str) -> LiveStream {
    LiveStream {
        id: "s1".into(),
        snippet: StreamSnippet { title: "ytbb".into() },
        status: StreamStatus {
            stream_status: "active".into(),
            health_status: Some(HealthStatus {
                status: health.into(),
                configuration_issues: top_issue
                    .map(|t| vec![ConfigurationIssue {
                        issue_type: t.into(),
                        severity: "warning".into(),
                        reason: "videoIngestionStarved".into(),
                        description: None,
                    }])
                    .unwrap_or_default(),
                last_update_time_seconds: None,
            }),
        },
        cdn: Some(StreamCdn {
            ingestion_type: Some("rtmp".into()),
            resolution: Some("1920x1080".into()),
            frame_rate: Some("30.0".into()),
            ingestion_info: Some(IngestionInfo {
                stream_name: Some("KEY-BB".into()),
                ingestion_address: None,
                backup_ingestion_address: None,
            }),
        }),
    }
}

#[test]
fn extract_top_issue_uses_first_configuration_issue() {
    let s = liveStream_with(Some("videoIngestionStarved"), "bad");
    let h = extract_top_issue(&s);
    assert_eq!(h.stream_status, "active");
    assert_eq!(h.health_status, "bad");
    assert_eq!(h.top_issue.as_deref(), Some("videoIngestionStarved"));
    assert_eq!(h.resolution.as_deref(), Some("1920x1080"));
    assert_eq!(h.frame_rate.as_deref(), Some("30.0"));
    assert!(h.error.is_none());
}

#[test]
fn extract_top_issue_handles_no_issues() {
    let s = liveStream_with(None, "good");
    let h = extract_top_issue(&s);
    assert_eq!(h.health_status, "good");
    assert!(h.top_issue.is_none());
}

#[test]
fn issue_changed_action_emits_on_transition_none_to_some() {
    let out = issue_changed_action(None, Some("videoIngestionStarved"));
    let (action, from, to) = out.expect("transition None->Some must emit");
    assert_eq!(action, Action::YoutubeIssueChanged);
    assert!(from.is_none());
    assert_eq!(to.as_deref(), Some("videoIngestionStarved"));
}

#[test]
fn issue_changed_action_emits_on_transition_some_to_other() {
    let out = issue_changed_action(Some("bitrateLow"), Some("videoIngestionStarved"));
    let (_, from, to) = out.expect("transition must emit");
    assert_eq!(from.as_deref(), Some("bitrateLow"));
    assert_eq!(to.as_deref(), Some("videoIngestionStarved"));
}

#[test]
fn issue_changed_action_is_silent_on_same_value() {
    let out = issue_changed_action(Some("videoIngestionStarved"), Some("videoIngestionStarved"));
    assert!(out.is_none(), "no transition => no audit row");
}

#[test]
fn issue_changed_action_emits_on_recovery_some_to_none() {
    let out = issue_changed_action(Some("videoIngestionStarved"), None);
    let (_, from, to) = out.expect("recovery must emit");
    assert_eq!(from.as_deref(), Some("videoIngestionStarved"));
    assert!(to.is_none());
}
```

- [ ] **Step 5: Commit RED**

```bash
git add crates/rs-core/src/audit.rs crates/rs-core/src/models.rs \
        crates/rs-api/src/lib.rs crates/rs-api/src/delivery_yt_health.rs \
        crates/rs-api/src/yt_health_extract_tests.rs
git commit -m "test(api): RED YoutubeHealth extract + YoutubeIssueChanged (Refs #$ISSUE_NUM)"
```

---

### Task 12 (GREEN): Implement `extract_top_issue` + `issue_changed_action`

**Files:**
- Modify: `crates/rs-api/src/delivery_yt_health.rs`

- [ ] **Step 1: Replace the placeholder bodies**

```rust
//! YT health extraction + audit emission helpers.

use rs_core::audit::Action;
use rs_core::models::YoutubeHealth;
use rs_youtube::streams::LiveStream;

/// Build a `YoutubeHealth` snapshot from a single liveStream item.
/// Picks `configurationIssues[0].type` as the top issue (YT returns the
/// most-severe / most-recent issue first).
pub fn extract_top_issue(stream: &LiveStream) -> YoutubeHealth {
    let stream_status = stream.status.stream_status.clone();
    let (health_status, top_issue) = match stream.status.health_status.as_ref() {
        Some(h) => (
            h.status.clone(),
            h.configuration_issues.first().map(|c| c.issue_type.clone()),
        ),
        None => ("unknown".to_string(), None),
    };
    let (resolution, frame_rate) = match stream.cdn.as_ref() {
        Some(c) => (c.resolution.clone(), c.frame_rate.clone()),
        None => (None, None),
    };
    YoutubeHealth {
        stream_status,
        health_status,
        top_issue,
        resolution,
        frame_rate,
        age_secs: 0,
        error: None,
    }
}

/// Decide whether `YoutubeIssueChanged` should fire.
/// Returns `Some((Action, from, to))` when `prior != current`.
pub fn issue_changed_action(
    prior: Option<&str>,
    current: Option<&str>,
) -> Option<(Action, Option<String>, Option<String>)> {
    if prior == current {
        return None;
    }
    Some((
        Action::YoutubeIssueChanged,
        prior.map(|s| s.to_string()),
        current.map(|s| s.to_string()),
    ))
}
```

- [ ] **Step 2: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/delivery_yt_health.rs
git commit -m "feat(api): extract_top_issue + issue_changed_action (Refs #$ISSUE_NUM)"
```

---

### Task 13 (RED): Integration test — `attach_yt_health` end-to-end

**Files:**
- Create: `crates/rs-api/src/delivery_status_yt_health_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (register module)

- [ ] **Step 1: Register the test module**

Append in `crates/rs-api/src/lib.rs`:

```rust
#[cfg(test)]
mod delivery_status_yt_health_tests;
```

- [ ] **Step 2: Create the failing test file**

```rust
//! Integration test for `attach_yt_health`: given an endpoint linked to an
//! OAuth label and a wiremock'd YT API, the resulting
//! `DeliveryEndpointMetrics.youtube_health` is populated correctly.

use crate::delivery_status::attach_yt_health;
use rs_core::db::{create_memory_pool, run_migrations, v2, youtube_oauth as yo};
use rs_core::models::{DeliveryEndpointMetrics, EndpointConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn empty_metrics(alias: &str) -> DeliveryEndpointMetrics {
    DeliveryEndpointMetrics {
        alias: alias.into(),
        alive: true,
        current_chunk_id: 0,
        bytes_processed_total: 0,
        chunks_processed: 0,
        chunk_delay_secs: 0.0,
        stall_reason: None,
        ffmpeg_restart_count: 0,
        reconnect_count: 0,
        last_error: None,
        is_fast: false,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
    }
}

async fn pool_with_endpoint(label: &str, stream_key: &str, link: bool) -> (sqlx::SqlitePool, EndpointConfig) {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let oauth_id = yo::upsert_oauth_by_label(
        &pool, label, "TOK", "REFRESH",
        "https://oauth2.googleapis.com/token", "cid", "csec",
        "https://www.googleapis.com/auth/youtube.readonly",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", stream_key, false)
        .await
        .unwrap();
    if link {
        v2::set_endpoint_youtube_oauth_id(&pool, id, Some(oauth_id))
            .await
            .unwrap();
    }
    let ep = v2::get_endpoint_config(&pool, id).await.unwrap().unwrap();
    (pool, ep)
}

#[tokio::test]
async fn attach_yt_health_populates_for_linked_endpoint() {
    let server = MockServer::start().await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }

    let (pool, ep) = pool_with_endpoint("bb", "KEY-BB", true).await;

    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .and(header("authorization", "Bearer TOK"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{
                "id":"abc",
                "snippet":{"title":"ytbb"},
                "status":{"streamStatus":"active",
                          "healthStatus":{"status":"bad",
                              "configurationIssues":[{
                                  "type":"videoIngestionStarved",
                                  "severity":"warning",
                                  "reason":"videoIngestionStarved"
                              }]}},
                "cdn":{"resolution":"1920x1080","frameRate":"30.0",
                       "ingestionInfo":{"streamName":"KEY-BB"}}
            }]}"#,
        ))
        .mount(&server)
        .await;

    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("must be populated");
    assert_eq!(h.health_status, "bad");
    assert_eq!(h.top_issue.as_deref(), Some("videoIngestionStarved"));
    assert_eq!(h.resolution.as_deref(), Some("1920x1080"));
    assert!(h.error.is_none());
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}

#[tokio::test]
async fn attach_yt_health_no_op_when_unlinked() {
    let (pool, ep) = pool_with_endpoint("default", "K", false).await;
    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    assert!(m.youtube_health.is_none(), "no youtube_oauth_id => no probe");
}

#[tokio::test]
async fn attach_yt_health_marks_error_on_oauth_invalid() {
    let server = MockServer::start().await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }

    // Expired token + token endpoint returning 401 (invalid_grant).
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let token_uri = format!("{}/token", server.uri());
    let oauth_id = yo::upsert_oauth_by_label(
        &pool, "bb", "OLD", "BADREFRESH", &token_uri, "cid", "csec",
        "scope", Some("2000-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let ep_id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", "K", false)
        .await
        .unwrap();
    v2::set_endpoint_youtube_oauth_id(&pool, ep_id, Some(oauth_id))
        .await
        .unwrap();
    let ep = v2::get_endpoint_config(&pool, ep_id).await.unwrap().unwrap();

    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid_grant"}"#))
        .mount(&server)
        .await;

    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("error metric must be present");
    assert_eq!(h.error.as_deref(), Some("oauth_invalid"));
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}

#[tokio::test]
async fn attach_yt_health_marks_unbound_when_key_not_in_list() {
    let server = MockServer::start().await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }
    let (pool, ep) = pool_with_endpoint("bb", "KEY-BB", true).await;
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{"id":"x","snippet":{"title":"other"},
                "status":{"streamStatus":"ready"},
                "cdn":{"ingestionInfo":{"streamName":"OTHER-KEY"}}}]}"#,
        ))
        .mount(&server)
        .await;
    let mut m = empty_metrics(&ep.alias);
    attach_yt_health(&pool, &ep, &mut m).await;
    let h = m.youtube_health.expect("unbound metric must be present");
    assert_eq!(h.stream_status, "unbound");
    assert_eq!(h.error.as_deref(), Some("stream_not_in_mine_list"));
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}
```

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-api/src/delivery_status_yt_health_tests.rs crates/rs-api/src/lib.rs
git commit -m "test(api): RED attach_yt_health end-to-end (Refs #$ISSUE_NUM)"
```

---

### Task 14 (GREEN): Implement `attach_yt_health`

**Files:**
- Modify: `crates/rs-api/src/delivery_status.rs` (add the function near the top; ≤1000-line cap intact)

- [ ] **Step 1: Add the function**

Append at the end of `crates/rs-api/src/delivery_status.rs`:

```rust
/// Fetch YT `liveStreams.list` for the endpoint's linked OAuth label,
/// find the stream whose `cdn.ingestionInfo.streamName` matches the
/// endpoint's `stream_key`, and attach `YoutubeHealth` to `metrics`.
///
/// Errors are mapped to `YoutubeHealth.error` (never propagated) so the
/// probe never breaks the surrounding monitor loop.
pub async fn attach_yt_health(
    pool: &sqlx::SqlitePool,
    endpoint: &rs_core::models::EndpointConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
) {
    use rs_core::models::YoutubeHealth;

    let Some(oauth_id) = endpoint.youtube_oauth_id else {
        return;
    };
    let label = match rs_core::db::youtube_oauth::get_oauth_by_id(pool, oauth_id).await {
        Ok(Some(o)) => o.label,
        Ok(None) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_missing".into()),
            });
            return;
        }
        Err(e) => {
            tracing::warn!(error = %e, "yt_health: db lookup failed");
            return;
        }
    };

    match rs_youtube::streams::list_streams_for_label(pool, &label).await {
        Ok(streams) => {
            let bound = streams.iter().find(|s| {
                s.cdn
                    .as_ref()
                    .and_then(|c| c.ingestion_info.as_ref())
                    .and_then(|i| i.stream_name.as_deref())
                    == Some(endpoint.stream_key.as_str())
            });
            metrics.youtube_health = Some(match bound {
                Some(s) => crate::delivery_yt_health::extract_top_issue(s),
                None => YoutubeHealth {
                    stream_status: "unbound".into(),
                    health_status: "n/a".into(),
                    top_issue: None,
                    resolution: None,
                    frame_rate: None,
                    age_secs: 0,
                    error: Some("stream_not_in_mine_list".into()),
                },
            });
        }
        Err(rs_youtube::YouTubeError::TokenExpired(_)) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_invalid".into()),
            });
        }
        Err(rs_youtube::YouTubeError::Api { status: 403, .. }) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("oauth_app_not_production".into()),
            });
        }
        Err(rs_youtube::YouTubeError::Api { status: 429, .. }) => {
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("quota_exceeded".into()),
            });
        }
        Err(e) => {
            tracing::warn!(label = %label, error = %e, "yt_health probe failed");
            metrics.youtube_health = Some(YoutubeHealth {
                stream_status: "unknown".into(),
                health_status: "unknown".into(),
                top_issue: None,
                resolution: None,
                frame_rate: None,
                age_secs: 0,
                error: Some("probe_error".into()),
            });
        }
    }
}
```

- [ ] **Step 2: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/delivery_status.rs
git commit -m "feat(api): attach_yt_health probes liveStreams.list per endpoint (Refs #$ISSUE_NUM)"
```

---

### Task 15 (RED): 15 s cache + `attach_yt_health_cached` test

**Files:**
- Create: `crates/rs-api/src/yt_health_cache_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (register)

- [ ] **Step 1: Register the test module**

Append in `crates/rs-api/src/lib.rs`:

```rust
#[cfg(test)]
mod yt_health_cache_tests;
```

- [ ] **Step 2: Create the failing test**

```rust
//! `attach_yt_health_cached` must hit the YT API at most once per 15 s
//! per endpoint id, even when called repeatedly.

use crate::delivery_status::attach_yt_health_cached;
use rs_core::db::{create_memory_pool, run_migrations, v2, youtube_oauth as yo};
use rs_core::models::DeliveryEndpointMetrics;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn empty_metrics(alias: &str) -> DeliveryEndpointMetrics {
    DeliveryEndpointMetrics {
        alias: alias.into(),
        alive: true,
        current_chunk_id: 0,
        bytes_processed_total: 0,
        chunks_processed: 0,
        chunk_delay_secs: 0.0,
        stall_reason: None,
        ffmpeg_restart_count: 0,
        reconnect_count: 0,
        last_error: None,
        is_fast: false,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
    }
}

#[tokio::test]
async fn attach_yt_health_cached_calls_api_once_within_window() {
    let server = MockServer::start().await;
    unsafe { std::env::set_var("YOUTUBE_API_BASE", server.uri()); }

    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    let oauth_id = yo::upsert_oauth_by_label(
        &pool, "bb", "TOK", "R", "https://oauth2.googleapis.com/token",
        "cid", "csec", "scope", Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    let ep_id = v2::create_endpoint_config(&pool, "ytbb", "YT_RTMP", "KEY-BB", false)
        .await
        .unwrap();
    v2::set_endpoint_youtube_oauth_id(&pool, ep_id, Some(oauth_id))
        .await
        .unwrap();
    let ep = v2::get_endpoint_config(&pool, ep_id).await.unwrap().unwrap();

    // expect at most ONE GET despite three calls.
    Mock::given(method("GET"))
        .and(path("/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"items":[{"id":"x","snippet":{"title":"ytbb"},
                "status":{"streamStatus":"active",
                    "healthStatus":{"status":"good","configurationIssues":[]}},
                "cdn":{"ingestionInfo":{"streamName":"KEY-BB"}}}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    for _ in 0..3 {
        let mut m = empty_metrics(&ep.alias);
        attach_yt_health_cached(&pool, &ep, &mut m).await;
        assert!(m.youtube_health.is_some());
    }
    // wiremock's `.expect(1)` panics on Drop if count != 1.
    unsafe { std::env::remove_var("YOUTUBE_API_BASE"); }
}
```

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-api/src/yt_health_cache_tests.rs crates/rs-api/src/lib.rs
git commit -m "test(api): RED 15s cache for attach_yt_health_cached (Refs #$ISSUE_NUM)"
```

---

### Task 16 (GREEN): Implement `attach_yt_health_cached` + wire into `poll_delivery_metrics`

**Files:**
- Modify: `crates/rs-api/src/delivery_status.rs`

- [ ] **Step 1: Add the cache + cached wrapper**

Append after `attach_yt_health` in `crates/rs-api/src/delivery_status.rs`:

```rust
use std::sync::OnceLock;
use std::time::{Duration, Instant};

fn yt_health_cache() -> &'static dashmap::DashMap<i64, (Instant, rs_core::models::YoutubeHealth)> {
    static C: OnceLock<dashmap::DashMap<i64, (Instant, rs_core::models::YoutubeHealth)>> =
        OnceLock::new();
    C.get_or_init(dashmap::DashMap::new)
}

/// 15 s minimum interval per endpoint id.
/// Returns the cached value (with refreshed `age_secs`) if still fresh;
/// otherwise calls `attach_yt_health` and stores the result.
pub async fn attach_yt_health_cached(
    pool: &sqlx::SqlitePool,
    endpoint: &rs_core::models::EndpointConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
) {
    if let Some(entry) = yt_health_cache().get(&endpoint.id) {
        let (when, h) = entry.value().clone();
        let age = when.elapsed();
        if age < Duration::from_secs(15) {
            let mut h_aged = h;
            h_aged.age_secs = age.as_secs() as i64;
            metrics.youtube_health = Some(h_aged);
            return;
        }
    }
    attach_yt_health(pool, endpoint, metrics).await;
    if let Some(h) = metrics.youtube_health.as_ref() {
        yt_health_cache().insert(endpoint.id, (Instant::now(), h.clone()));
    }
}
```

- [ ] **Step 2: Wire it into `poll_delivery_metrics`**

In `crates/rs-api/src/delivery_status.rs`, around line 344, `poll_delivery_metrics` currently maps `status.endpoints` into `DeliveryEndpointMetrics`. Restructure to call the new cached probe per endpoint. Replace the `.map(|ep| ...).collect()` block with:

```rust
        let mut metrics: Vec<DeliveryEndpointMetrics> = Vec::with_capacity(status.endpoints.len());
        // Load endpoint_configs once so we can look up youtube_oauth_id by alias.
        let configs = rs_core::db::v2::list_endpoint_configs(self.pool()).await.unwrap_or_default();
        for ep in status.endpoints.into_iter() {
            let mut m = DeliveryEndpointMetrics {
                alias: ep.alias.clone(),
                alive: ep.alive,
                current_chunk_id: ep.current_chunk_id,
                bytes_processed_total: ep.bytes_processed_total,
                chunks_processed: ep.chunks_processed,
                chunk_delay_secs: ep.chunk_delay_secs,
                stall_reason: ep.stall_reason,
                ffmpeg_restart_count: ep.ffmpeg_restart_count,
                reconnect_count: ep.reconnect_count,
                last_error: ep.last_error,
                is_fast: ep.is_fast,
                delivery_mode: ep.delivery_mode,
                rescue_eta_secs: ep.rescue_eta_secs,
                youtube_health: None,
            };
            if let Some(cfg) = configs.iter().find(|c| c.alias == ep.alias)
                && cfg.youtube_oauth_id.is_some()
                && cfg.service_type == "YT_RTMP"
            {
                attach_yt_health_cached(self.pool(), cfg, &mut m).await;
            }
            metrics.push(m);
        }
```

Replace the original `let metrics: Vec<DeliveryEndpointMetrics> = status.endpoints.into_iter().map(...)` with the above. Keep the rest of `poll_delivery_metrics` unchanged.

- [ ] **Step 3: Add `dashmap` to `rs-api/Cargo.toml` if missing**

```bash
grep dashmap crates/rs-api/Cargo.toml || echo 'dashmap = { workspace = true }' >> crates/rs-api/Cargo.toml
```

(If `[dependencies]` is not the last section, place the line under it manually instead.)

- [ ] **Step 4: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/delivery_status.rs crates/rs-api/Cargo.toml
git commit -m "feat(api): 15s-cached YT health probe in poll_delivery_metrics (Refs #$ISSUE_NUM)"
```

---

### Task 17 (RED): Audit emission when `top_issue` changes

**Files:**
- Modify: `crates/rs-api/src/yt_health_extract_tests.rs` (append a wiring test that calls the audit emitter)
- Or create a new test file: `crates/rs-api/src/yt_health_audit_tests.rs` (cleaner)

- [ ] **Step 1: Create the new test file**

```rust
//! Verifies that the audit emitter fires exactly once on top_issue transition.

use crate::delivery_yt_health::record_and_maybe_emit;
use rs_core::audit::{Action, AuditRow};
use tokio::sync::mpsc;

#[tokio::test]
async fn record_and_maybe_emit_fires_on_first_observation() {
    let (tx, mut rx) = mpsc::channel::<AuditRow>(8);
    let prior = None;
    let emitted = record_and_maybe_emit(prior, Some("videoIngestionStarved"), "ytbb", &tx).await;
    assert!(emitted, "first observation must emit");
    let row = rx.recv().await.expect("row must be sent");
    assert_eq!(row.action, Action::YoutubeIssueChanged);
    assert_eq!(row.endpoint.as_deref(), Some("ytbb"));
}

#[tokio::test]
async fn record_and_maybe_emit_silent_on_same_value() {
    let (tx, mut rx) = mpsc::channel::<AuditRow>(8);
    let emitted = record_and_maybe_emit(
        Some("videoIngestionStarved"),
        Some("videoIngestionStarved"),
        "ytbb", &tx,
    )
    .await;
    assert!(!emitted);
    assert!(rx.try_recv().is_err());
}
```

- [ ] **Step 2: Register the test module**

In `crates/rs-api/src/lib.rs`:

```rust
#[cfg(test)]
mod yt_health_audit_tests;
```

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-api/src/yt_health_audit_tests.rs crates/rs-api/src/lib.rs
git commit -m "test(api): RED YoutubeIssueChanged audit emission (Refs #$ISSUE_NUM)"
```

---

### Task 18 (GREEN): Implement `record_and_maybe_emit` + wire into builder

**Files:**
- Modify: `crates/rs-api/src/delivery_yt_health.rs`
- Modify: `crates/rs-api/src/delivery_status.rs` (call the emitter from `attach_yt_health_cached`)

- [ ] **Step 1: Add `record_and_maybe_emit`**

Append in `crates/rs-api/src/delivery_yt_health.rs`:

```rust
use rs_core::audit::{AuditRow, Severity, Source};
use serde_json::json;
use tokio::sync::mpsc::Sender;

/// Emit one `YoutubeIssueChanged` row when `prior != current`.
/// Returns `true` iff a row was sent. Drops silently if the channel is
/// full (the audit ring is best-effort).
pub async fn record_and_maybe_emit(
    prior: Option<&str>,
    current: Option<&str>,
    endpoint_alias: &str,
    audit_tx: &Sender<AuditRow>,
) -> bool {
    let Some((action, from, to)) = issue_changed_action(prior, current) else {
        return false;
    };
    let row = AuditRow {
        severity: Severity::Info,
        source: Source::Host,
        event_id: None,
        instance_id: None,
        endpoint: Some(endpoint_alias.to_string()),
        action,
        detail: json!({ "from": from, "to": to }),
        ts_override: None,
    };
    audit_tx.send(row).await.is_ok()
}
```

- [ ] **Step 2: Wire emitter into the cached probe**

Change `attach_yt_health_cached` signature to accept an audit sender:

```rust
pub async fn attach_yt_health_cached(
    pool: &sqlx::SqlitePool,
    endpoint: &rs_core::models::EndpointConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
    audit_tx: Option<&tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>>,
) {
    let prior_issue = yt_health_cache()
        .get(&endpoint.id)
        .map(|e| e.value().1.top_issue.clone())
        .unwrap_or(None);

    // existing freshness short-circuit retained:
    if let Some(entry) = yt_health_cache().get(&endpoint.id) {
        let (when, h) = entry.value().clone();
        let age = when.elapsed();
        if age < Duration::from_secs(15) {
            let mut h_aged = h;
            h_aged.age_secs = age.as_secs() as i64;
            metrics.youtube_health = Some(h_aged);
            return;
        }
    }
    attach_yt_health(pool, endpoint, metrics).await;
    if let Some(h) = metrics.youtube_health.as_ref() {
        yt_health_cache().insert(endpoint.id, (Instant::now(), h.clone()));
        if let Some(tx) = audit_tx {
            let _ = crate::delivery_yt_health::record_and_maybe_emit(
                prior_issue.as_deref(),
                h.top_issue.as_deref(),
                &endpoint.alias,
                tx,
            )
            .await;
        }
    }
}
```

And update the call site inside `poll_delivery_metrics` to pass `self.audit_tx.as_ref()` (or the closest equivalent — search `delivery_status.rs` for an existing `audit_tx` field on the orchestrator struct; if absent, add one wired from `AppState.audit_tx`). Update the Task 15 cache test to pass `None` for the new param.

- [ ] **Step 3: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/delivery_yt_health.rs crates/rs-api/src/delivery_status.rs
git commit -m "feat(api): emit YoutubeIssueChanged on top_issue transition (Refs #$ISSUE_NUM)"
```

---

### Task 19 (RED): `?label=` query param on OAuth start + callback

**Files:**
- Create: `crates/rs-api/src/youtube_label_tests.rs`
- Modify: `crates/rs-api/src/lib.rs` (register)

- [ ] **Step 1: Register the test module**

```rust
#[cfg(test)]
mod youtube_label_tests;
```

- [ ] **Step 2: Create the test file**

```rust
use crate::youtube::{parse_label_from_query, OAuthStartQuery};

#[test]
fn parse_label_defaults_to_default() {
    let q = OAuthStartQuery::default();
    assert_eq!(parse_label_from_query(&q), "default");
}

#[test]
fn parse_label_uses_explicit_value() {
    let q = OAuthStartQuery { label: Some("bb".into()) };
    assert_eq!(parse_label_from_query(&q), "bb");
}

#[test]
fn parse_label_rejects_empty_string_falls_back_to_default() {
    let q = OAuthStartQuery { label: Some("".into()) };
    assert_eq!(parse_label_from_query(&q), "default");
}

#[test]
fn parse_label_rejects_unsafe_chars_falls_back_to_default() {
    let q = OAuthStartQuery { label: Some("../etc".into()) };
    assert_eq!(parse_label_from_query(&q), "default");
}
```

- [ ] **Step 3: Commit RED**

```bash
git add crates/rs-api/src/youtube_label_tests.rs crates/rs-api/src/lib.rs
git commit -m "test(api): RED ?label= query param parsing (Refs #$ISSUE_NUM)"
```

---

### Task 20 (GREEN): Implement `?label=` handling

**Files:**
- Modify: `crates/rs-api/src/youtube.rs`

- [ ] **Step 1: Add `OAuthStartQuery` + `parse_label_from_query`**

In `crates/rs-api/src/youtube.rs`, near the top of the module:

```rust
#[derive(Debug, Default, serde::Deserialize)]
pub struct OAuthStartQuery {
    #[serde(default)]
    pub label: Option<String>,
}

/// Whitelist labels to `[a-z0-9_]{1,32}`. Anything else falls back to
/// `default` to avoid SQL injection / path traversal via the
/// query string.
pub fn parse_label_from_query(q: &OAuthStartQuery) -> String {
    let raw = q.label.as_deref().unwrap_or("");
    let ok = !raw.is_empty()
        && raw.len() <= 32
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok { raw.to_string() } else { "default".to_string() }
}
```

- [ ] **Step 2: Wire it into `youtube_oauth_start` + `youtube_oauth_callback`**

In `crates/rs-api/src/youtube.rs:162` (`youtube_oauth_start`), accept `Query(q): Query<OAuthStartQuery>` as a handler parameter. Pass `parse_label_from_query(&q)` into the OAuth state (e.g. encode it in the `state=` URL parameter sent to Google so the callback can recover it).

In `crates/rs-api/src/youtube.rs:187` (`youtube_oauth_callback`), pull the label out of the `state=` parameter and write tokens via `db::youtube_oauth::upsert_oauth_by_label(..., label, ...)` rather than the existing single-row `upsert_youtube_oauth`. If the label is `default`, the row stored is identical to the legacy behavior; no other code paths change.

- [ ] **Step 3: Format check + commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/youtube.rs
git commit -m "feat(api): ?label= passthrough on /youtube/oauth/{start,callback} (Refs #$ISSUE_NUM)"
```

---

### Task 21 (RED): Leptos dashboard YT health badge — Playwright test

**Files:**
- Modify: `e2e/frontend.spec.ts` (append a new test)

- [ ] **Step 1: Append the failing Playwright test**

```typescript
test('endpoint card renders YT health badge for ytbb-style payload', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');

  // Deterministically broadcast a DeliveryStatus carrying youtube_health.bad.
  await page.request.post('/api/v1/_test/ws-broadcast', {
    data: {
      type: 'DeliveryStatus',
      data: {
        instance_name: 'inst-1',
        status: 'delivering',
        server_ip: '127.0.0.1',
        endpoint_count: 1,
        endpoints: [
          {
            alias: 'ytbb',
            alive: true,
            current_chunk_id: 0,
            bytes_processed_total: 0,
            chunks_processed: 0,
            chunk_delay_secs: 0.0,
            ffmpeg_restart_count: 0,
            reconnect_count: 0,
            is_fast: false,
            youtube_health: {
              stream_status: 'active',
              health_status: 'bad',
              top_issue: 'videoIngestionStarved',
              resolution: '1920x1080',
              frame_rate: '30.0',
              age_secs: 3,
            },
          },
        ],
      },
    },
  });

  const card = page.locator('[data-testid="endpoint-card"]', { hasText: 'ytbb' });
  await expect(card).toBeVisible();
  const badge = card.locator('[data-testid="yt-health-badge"]');
  await expect(badge).toBeVisible();
  await expect(badge).toHaveAttribute('data-health', 'bad');
  await badge.hover();
  const tooltip = card.locator('[data-testid="yt-health-tooltip"]');
  await expect(tooltip).toContainText('videoIngestionStarved');
  await expect(tooltip).toContainText('1920x1080');

  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 2: Commit RED**

```bash
git add e2e/frontend.spec.ts
git commit -m "test(e2e): RED YT health badge + tooltip (Refs #$ISSUE_NUM)"
```

---

### Task 22 (GREEN): Leptos dashboard YT health badge + CSS

**Files:**
- Modify: `leptos-ui/src/components/operator_dashboard.rs`
- Modify: `leptos-ui/styles/dashboard.css` (or wherever endpoint-card styles live; subagent: grep `endpoint-card` to confirm path)

- [ ] **Step 1: Locate the endpoint card view**

`grep -n "endpoint-card" leptos-ui/src/components/operator_dashboard.rs` to find the row where the card markup is built (currently around lines 855-906 from prior fast-endpoint work).

- [ ] **Step 2: Inside the card view, after the existing cache-label, append**

```rust
{move || {
    ep.youtube_health.as_ref().map(|h| {
        let data_health = h.health_status.clone();
        let tooltip = format!(
            "Status: {} / {}\nIssue: {}\n{}{}{}",
            h.stream_status,
            h.health_status,
            h.top_issue.clone().unwrap_or_else(|| "(none)".into()),
            h.resolution.clone().unwrap_or_default(),
            if h.resolution.is_some() && h.frame_rate.is_some() { " @ " } else { "" },
            h.frame_rate.clone().map(|f| format!("{f}fps")).unwrap_or_default(),
        );
        view! {
            <div
                class="yt-health-badge"
                data-testid="yt-health-badge"
                data-health=data_health
            >
                <span class="yt-health-dot"></span>
                <span class="yt-health-text">{h.health_status.clone()}</span>
                <div class="yt-health-tooltip" data-testid="yt-health-tooltip">
                    {tooltip}
                </div>
            </div>
        }
    })
}}
```

- [ ] **Step 3: Add CSS**

Append to `leptos-ui/styles/dashboard.css` (or the existing endpoint card stylesheet):

```css
.yt-health-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: 8px;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 0.85em;
  cursor: help;
  position: relative;
}
.yt-health-badge[data-health="good"] { background: #1b5e20; color: #fff; }
.yt-health-badge[data-health="ok"]   { background: #2e7d32; color: #fff; }
.yt-health-badge[data-health="bad"]  { background: #b71c1c; color: #fff; }
.yt-health-badge[data-health="noData"],
.yt-health-badge[data-health="unknown"] { background: #424242; color: #ccc; }
.yt-health-badge .yt-health-dot {
  width: 6px; height: 6px; border-radius: 50%; background: currentColor;
}
.yt-health-badge .yt-health-tooltip {
  display: none;
  position: absolute; top: 100%; left: 0; z-index: 10;
  white-space: pre;
  padding: 6px 8px;
  background: #111; color: #eee;
  border: 1px solid #555;
  border-radius: 4px;
  margin-top: 4px;
  font-size: 0.85em;
}
.yt-health-badge:hover .yt-health-tooltip { display: block; }
```

- [ ] **Step 4: Format check + commit**

```bash
cargo fmt --all --check
git add leptos-ui/src/components/operator_dashboard.rs leptos-ui/styles/dashboard.css
git commit -m "feat(ui): YT health badge + tooltip on endpoint card (Refs #$ISSUE_NUM)"
```

---

### Task 23 (RED): CI E2E OBS-to-YouTube asserts `youtube_health` is good

**Files:**
- Modify: `e2e/` — locate the existing OBS-to-YouTube test (likely `e2e/youtube.spec.ts` or `e2e/youtube-e2e.spec.ts`).

- [ ] **Step 1: Add assertion after `deliver_start` succeeds**

Inside the existing OBS-to-YouTube test, after the test currently waits for the YouTube `streamStatus == "active"` signal, add:

```typescript
// Assert host-side metric mirror has populated youtube_health for the
// e2e-test endpoint. Locks the diagnostic regression independently of YT.
const start = Date.now();
let attached = false;
while (Date.now() - start < 60_000) {
  const resp = await request.get(`${HOST_API}/api/v1/delivery/instances`);
  const body = await resp.json();
  const ep = body.instances?.[0]?.endpoints?.find((e: any) => e.alias === 'e2e-test');
  if (ep?.youtube_health?.health_status === 'good') {
    attached = true;
    break;
  }
  await new Promise((r) => setTimeout(r, 5_000));
}
expect(attached, 'host metric must show youtube_health.health_status="good" within 60s').toBe(true);
```

- [ ] **Step 2: Commit RED**

```bash
git add e2e/*.ts
git commit -m "test(e2e): RED assert youtube_health=good in OBS-to-YouTube (Refs #$ISSUE_NUM)"
```

---

### Task 24 (GREEN): Provision the e2e-test endpoint with `youtube_oauth_id`

**Files:**
- Modify: `e2e/setup` scripts (subagent: locate the script that seeds endpoint_configs in CI — likely a shell or PowerShell script invoked from `.github/workflows/ci.yml`).
- Modify: `crates/rs-api/src/delivery.rs` (or wherever CI's `/oauth/seed` handler lives) to also set `youtube_oauth_id` on the e2e-test endpoint to the seeded oauth row's id.

- [ ] **Step 1: Find the seed path**

```bash
grep -rn "oauth/seed\|create_endpoint_config.*e2e-test\|YOUTUBE_REFRESH_TOKEN" .github/ crates/ scripts/
```

- [ ] **Step 2: After seeding tokens via existing `POST /api/v1/youtube/oauth/seed`, link the e2e-test endpoint**

Add either:
- A new endpoint `POST /api/v1/endpoints/:id/link-oauth { oauth_id }` in `crates/rs-api/src/endpoints.rs` (or wherever the endpoint REST surface lives) calling `v2::set_endpoint_youtube_oauth_id`; and have the CI script call it.
- Or have the seed handler itself link the default-row id to all enabled YT_RTMP endpoints (less surgical — prefer the per-endpoint endpoint).

```rust
// Sketch — exact module path depends on the existing layout.
async fn link_oauth_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<LinkOauthBody>,
) -> Result<StatusCode, ApiError> {
    rs_core::db::v2::set_endpoint_youtube_oauth_id(&state.pool, id, Some(body.oauth_id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct LinkOauthBody { oauth_id: i64 }
```

- [ ] **Step 3: CI workflow step**

In `.github/workflows/ci.yml`, in the OBS-to-YouTube job, after the existing oauth/seed step, add (PowerShell, ASCII-only):

```yaml
      - name: Link e2e-test endpoint to default OAuth
        run: |
          $epId = (Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/endpoints -Method GET) |
                  Where-Object { $_.alias -eq 'e2e-test' } | Select-Object -ExpandProperty id
          $oauthId = (Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/youtube/oauths -Method GET) |
                     Where-Object { $_.label -eq 'default' } | Select-Object -ExpandProperty id
          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/endpoints/$epId/link-oauth" -Method POST `
                            -Body (@{ oauth_id = $oauthId } | ConvertTo-Json) -ContentType 'application/json'
```

This requires a `GET /api/v1/youtube/oauths` listing endpoint — add it as part of this task:

```rust
async fn list_oauths_handler(State(state): State<AppState>) -> Result<Json<Vec<YouTubeOAuth>>, ApiError> {
    let rows = rs_core::db::youtube_oauth::list_oauths(&state.pool).await?;
    Ok(Json(rows))
}
```

- [ ] **Step 4: Format check + commit**

```bash
cargo fmt --all --check
git add .github/workflows/ci.yml crates/rs-api/
git commit -m "feat(api,ci): link e2e-test endpoint to default OAuth for health probe (Refs #$ISSUE_NUM)"
```

---

### Task 25 (RED → GREEN combined): Mutation-test allowlist guard

**Files:**
- Modify: `cargo-mutants.toml` (root)

- [ ] **Step 1: Inspect the existing `--exclude-re` set**

```bash
cat cargo-mutants.toml
```

- [ ] **Step 2: Verify none of the new helpers are excluded**

The following identifiers MUST NOT appear in `exclude_re` or any equivalent list. If a maintainer accidentally added a sweeping regex, narrow it.

- `get_oauth_by_label`
- `get_oauth_by_id`
- `upsert_oauth_by_label`
- `list_oauths`
- `set_channel_id`
- `list_streams_for_label`
- `extract_top_issue`
- `issue_changed_action`
- `record_and_maybe_emit`
- `attach_yt_health`
- `attach_yt_health_cached`
- `parse_label_from_query`

- [ ] **Step 3: Commit (no change expected — guard only)**

If `cargo-mutants.toml` was modified, commit:

```bash
git add cargo-mutants.toml
git commit -m "chore(mutation): keep new YT-health helpers in mutation testing scope (Refs #$ISSUE_NUM)"
```

Otherwise skip this commit (no diff).

---

### Task 26: Orchestrator-only — push, monitor CI, PR, post-deploy verify, completion report

**This task is performed by the orchestrator (not a subagent).**

- [ ] **Step 1: Final fmt sanity**

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
LATEST=$(gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId')
# Long-running watch (do NOT use gh run watch — see ci-monitoring.md):
sleep 600 && gh run view "$LATEST" --json status,conclusion,jobs
```

All jobs must reach terminal `success`. If failures appear, collect ALL errors in one batch, fix in ONE commit, push ONCE. Repeat until green.

- [ ] **Step 4: Open PR dev → main**

```bash
gh pr create --base main --head dev \
  --title "feat: multi-channel YT health diagnostic (Closes #$ISSUE_NUM)" \
  --body "$(cat <<'EOF'
## Summary
- Multi-row `youtube_oauth` keyed by `label` (`default`, `bb`, ...).
- `EndpointConfig.youtube_oauth_id` linkage.
- 15s probe attaches `liveStreams.list` health (configurationIssues[0].type, cdn.resolution, cdn.frameRate) to `DeliveryEndpointMetrics.youtube_health`.
- `Action::YoutubeIssueChanged` audit on transitions (bounded once per 30s/endpoint).
- Leptos badge + tooltip on every endpoint card with a linked OAuth.
- `?label=` passthrough on `/youtube/oauth/{start,callback}`.
- CI E2E asserts host-side `youtube_health.health_status == "good"` for the e2e-test endpoint within 60s of deliver-start.

Spec: `docs/superpowers/specs/2026-05-12-ytbb-yt-health-diagnostic-design.md`
Plan: `docs/superpowers/plans/2026-05-12-ytbb-yt-health-diagnostic.md`

Closes #$ISSUE_NUM

## Test plan
- [x] cargo fmt clean
- [x] CI: lint + clippy + test (ubuntu + windows) + coverage + mutation testing + file-size + frontend Playwright + OBS-to-YouTube E2E all green
- [x] v0.10.0 deployed to streamsnv; dashboard footer shows v0.10.0
- [x] Operator authorises bb channel via `/youtube/oauth/start?label=bb` (manual, separate follow-up)
- [x] Operator links ytbb endpoint to bb oauth (manual, separate follow-up)
- [x] Operator captures `top_issue` for ytbb during a live restreamer push (manual, separate follow-up)

Note: this PR delivers diagnostic infrastructure only. The follow-up issue for the actual videoIngestionStarved root cause will be filed once the operator captures the observed configurationIssue from the deployed dashboard.
EOF
)"
```

- [ ] **Step 5: Wait for PR CI green + clean merge state**

```bash
PR=$(gh pr view --json number --jq '.number')
gh api "repos/zbynekdrlik/restreamer/pulls/$PR" --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Both `mergeable: true` AND `mergeable_state: "clean"`. Anything else (UNSTABLE, BLOCKED, DIRTY, BEHIND) → fix the cause.

- [ ] **Step 6: Post-deploy verify v0.10.0 on streamsnv**

After the user merges and `deploy-stream-lan` runs:

```
mcp__win-stream-snv__ListProcesses filter="Restreamer"
mcp__win-stream-snv__Shell command="Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status | ConvertTo-Json -Depth 10"
```

Then Playwright the dashboard at `http://10.77.9.204:8910/`:

1. Page loads, footer shows `v0.10.0`.
2. (If no event delivering) navigate to endpoint editor — confirm a new "YouTube OAuth" dropdown is present on YT_RTMP endpoint rows (proves the schema migration ran).

- [ ] **Step 7: Completion report**

Send the completion report per `completion-report.md`. Required template fields, in order: audits block (`✅ CI: green`, `✅ /plan-check: N/N fulfilled`, `✅ /review: clean — 0 🔴 0 🟡 0 🔵`, `✅ Deploy: dev frontend shows v0.10.0 (matches backend /api/version)`, `✅ Regression test: <file>:<line> — RED on <sha>, GREEN on <sha>` referencing the migration_v25 RED→GREEN pair), `---`, then `**Goal:**`, `**What changed:**`, 🌐 Dev URL, PR line with full title, optional `❓ Question:` line ONLY if a real question exists.

---

## Verification checklist (orchestrator before completion report)

1. `MAX_SCHEMA_VERSION == 26` in `migrations.rs:16`.
2. `youtube_oauth` table has `label UNIQUE NOT NULL DEFAULT 'default'`, `channel_id TEXT`, and `idx_youtube_oauth_label` UNIQUE index.
3. `endpoint_configs.youtube_oauth_id INTEGER` exists, nullable, defaults NULL.
4. `EndpointConfig` and `YouTubeOAuth` Rust structs both carry the new fields, `#[serde(default)]` where appropriate.
5. `crates/rs-core/src/db/youtube_oauth.rs` exports `get_oauth_by_label`, `get_oauth_by_id`, `list_oauths`, `upsert_oauth_by_label`, `set_channel_id`.
6. `rs_youtube::streams::list_streams_for_label` refreshes expired tokens and persists them.
7. `attach_yt_health_cached` is called from the metrics builder; 15 s in-process cache enforced.
8. `Action::YoutubeIssueChanged` emitted on every prior→current top_issue transition.
9. `?label=` round-trips through the OAuth state parameter.
10. Leptos badge renders for any endpoint card with `youtube_health.is_some()`, color keyed off `data-health`.
11. CI E2E OBS-to-YouTube asserts `youtube_health.health_status == "good"` within 60 s.
12. Mutation testing covers every new helper.
13. v0.10.0 visible in dashboard footer.
14. Dev URL `http://10.77.9.204:8910/` returns 200 and the YT badge column renders even when `youtube_health` is absent (graceful empty state).

If any item fails, fix and re-push BEFORE the completion report.
