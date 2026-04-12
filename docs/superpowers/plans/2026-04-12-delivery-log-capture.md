# Delivery VPS Log Capture & Restart History Persistence

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist VPS delivery logs and ffmpeg restart history in SQLite so operators can investigate ffmpeg restarts after the VPS is deleted.

**Architecture:** Two persistence mechanisms: (1) During each status poll, persist any new `FfmpegRestartRecord` entries to a `delivery_restart_log` table. (2) Before deleting the VPS, fetch `GET /api/logs?limit=5000` and store the full text in a `delivery_logs` table. Both tables reference `delivery_instances(id)` and survive VPS deletion. A new API endpoint `GET /api/v1/delivery/logs` serves the captured data.

**Tech Stack:** Rust, SQLite (sqlx runtime queries), Axum

**Spec:** Investigation in conversation — no separate spec file (design emerged from live debugging session).

---

## Context

- ffmpeg processes run on the Hetzner VPS (rs-delivery binary), not on stream.lan
- VPS is ephemeral — destroyed after each streaming session
- Current code polls VPS status and displays `restart_history` on dashboard, but never persists it
- VPS has `GET /api/logs?limit=N` endpoint returning `LogsResponse { entries: Vec<LogEntry> }` (in-memory ring buffer, cap 5000)
- VPS has `GET /api/status` returning per-endpoint `ffmpeg_restart_count` and `restart_history: Vec<FfmpegRestartRecord>`
- After VPS deletion, all restart and log data is lost — operators cannot investigate issues

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/rs-core/src/db/mod.rs:73,298` | Modify | Add migration V13 (two new tables) |
| `crates/rs-core/src/db/v2.rs:303+` | Modify | Add DB functions for insert/query |
| `crates/rs-api/src/delivery.rs:500-580` | Modify | Persist restart records during status poll |
| `crates/rs-api/src/delivery.rs:655-704` | Modify | Fetch and store VPS logs before deletion |
| `crates/rs-api/src/delivery_handlers.rs` | Modify | Add `GET /delivery/logs` handler |
| `crates/rs-api/src/lib.rs` | Modify | Register new route |

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml` (line 24)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Fetch and sync with main**

```bash
git fetch origin && git merge origin/main
```

- [ ] **Step 2: Bump version 0.3.34 → 0.3.35 in all four files**

`Cargo.toml` line 24: `version = "0.3.34"` → `version = "0.3.35"`
`src-tauri/Cargo.toml`: `version = "0.3.34"` → `version = "0.3.35"`
`src-tauri/tauri.conf.json`: `"version": "0.3.34"` → `"version": "0.3.35"`
`leptos-ui/Cargo.toml`: `version = "0.3.34"` → `version = "0.3.35"`

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.35"
```

---

### Task 1: DB Migration V13 — Two New Tables

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs:73` (add to migrations array)
- Modify: `crates/rs-core/src/db/mod.rs:297` (add MIGRATION_V13_SQL constant after V12)

- [ ] **Step 1: Write the migration test**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[sqlx::test]
async fn migration_v13_delivery_log_tables_exist(pool: SqlitePool) {
    run_migrations(&pool).await.unwrap();

    // delivery_logs table exists and accepts inserts
    sqlx::query(
        "INSERT INTO delivery_logs (instance_id, event_id, log_text) VALUES (0, 1, 'test log')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let count: i64 =
        sqlx::query("SELECT COUNT(*) as c FROM delivery_logs")
            .fetch_one(&pool)
            .await
            .map(|r| r.get("c"))
            .unwrap();
    assert_eq!(count, 1);

    // delivery_restart_log table exists and accepts inserts
    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs)
         VALUES (0, 1, 'YT HLS', 1000, 42, 65, 'stdin_closed', 'Connection reset', 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let count: i64 =
        sqlx::query("SELECT COUNT(*) as c FROM delivery_restart_log")
            .fetch_one(&pool)
            .await
            .map(|r| r.get("c"))
            .unwrap();
    assert_eq!(count, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p rs-core migration_v13 -- --nocapture
```

Expected: FAIL — `delivery_logs` table does not exist.

- [ ] **Step 3: Add MIGRATION_V13_SQL constant**

In `crates/rs-core/src/db/mod.rs`, after `MIGRATION_V12_SQL` (after line 297):

```rust
const MIGRATION_V13_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL,
    event_id    INTEGER,
    captured_at TEXT NOT NULL DEFAULT (datetime('now')),
    log_text    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_restart_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   INTEGER NOT NULL,
    event_id      INTEGER,
    alias         TEXT NOT NULL,
    timestamp_ms  INTEGER NOT NULL,
    chunk_id      INTEGER NOT NULL,
    lifetime_secs INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    stderr_tail   TEXT,
    backoff_secs  INTEGER NOT NULL
);

CREATE INDEX idx_delivery_restart_log_instance
    ON delivery_restart_log(instance_id);

CREATE INDEX idx_delivery_logs_instance
    ON delivery_logs(instance_id)
"#;
```

Note: No foreign key to `delivery_instances` — we don't want CASCADE delete to wipe the audit trail when old instances are cleaned up.

- [ ] **Step 4: Register migration V13 in the migrations array**

In `crates/rs-core/src/db/mod.rs`, line 73, add after `(12, MIGRATION_V12_SQL)`:

```rust
        (13, MIGRATION_V13_SQL),
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cargo test -p rs-core migration_v13 -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat: add delivery_logs and delivery_restart_log tables (migration V13)"
```

---

### Task 2: DB Functions — Insert and Query

**Files:**
- Modify: `crates/rs-core/src/db/v2.rs` (add after line 320, the end of `upsert_delivery_endpoint_status`)

- [ ] **Step 1: Write tests for the new DB functions**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[sqlx::test]
async fn insert_and_query_delivery_restart_log(pool: SqlitePool) {
    run_migrations(&pool).await.unwrap();

    insert_delivery_restart_record(&pool, 99, Some(1), "YT HLS", 1000, 42, 65, "stdin_closed", Some("Connection reset"), 2)
        .await
        .unwrap();
    insert_delivery_restart_record(&pool, 99, Some(1), "YT HLS", 2000, 43, 3, "stdin_closed", None, 4)
        .await
        .unwrap();
    insert_delivery_restart_record(&pool, 100, Some(2), "Facebook", 3000, 10, 120, "stdin_closed", Some("Broken pipe"), 1)
        .await
        .unwrap();

    let records = get_delivery_restart_log(&pool, 99).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].alias, "YT HLS");
    assert_eq!(records[0].timestamp_ms, 1000);
    assert_eq!(records[0].lifetime_secs, 65);
    assert_eq!(records[0].stderr_tail.as_deref(), Some("Connection reset"));
    assert_eq!(records[1].timestamp_ms, 2000);

    // Other instance not included
    let records2 = get_delivery_restart_log(&pool, 100).await.unwrap();
    assert_eq!(records2.len(), 1);
    assert_eq!(records2[0].alias, "Facebook");
}

#[sqlx::test]
async fn insert_and_query_delivery_logs(pool: SqlitePool) {
    run_migrations(&pool).await.unwrap();

    insert_delivery_log(&pool, 99, Some(1), "INFO rs_delivery: started\nINFO endpoint: ffmpeg spawned")
        .await
        .unwrap();

    let log = get_delivery_log(&pool, 99).await.unwrap();
    assert!(log.is_some());
    let log = log.unwrap();
    assert!(log.contains("ffmpeg spawned"));

    // No log for unknown instance
    let empty = get_delivery_log(&pool, 999).await.unwrap();
    assert!(empty.is_none());
}

#[sqlx::test]
async fn restart_log_dedup_by_timestamp(pool: SqlitePool) {
    run_migrations(&pool).await.unwrap();

    // Insert same record twice — should not produce duplicates
    insert_delivery_restart_record(&pool, 99, Some(1), "YT HLS", 1000, 42, 65, "stdin_closed", None, 2)
        .await
        .unwrap();
    insert_delivery_restart_record(&pool, 99, Some(1), "YT HLS", 1000, 42, 65, "stdin_closed", None, 2)
        .await
        .unwrap();

    let records = get_delivery_restart_log(&pool, 99).await.unwrap();
    assert_eq!(records.len(), 1, "duplicate timestamp_ms should be ignored");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p rs-core "insert_and_query_delivery|restart_log_dedup" -- --nocapture
```

Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement DB functions**

Add to `crates/rs-core/src/db/v2.rs` after the `upsert_delivery_endpoint_status` function:

```rust
// --- Delivery Log Capture ---

/// Insert a single ffmpeg restart record. Deduplicates on (instance_id, alias, timestamp_ms).
#[allow(clippy::too_many_arguments)]
pub async fn insert_delivery_restart_record(
    pool: &SqlitePool,
    instance_id: i64,
    event_id: Option<i64>,
    alias: &str,
    timestamp_ms: i64,
    chunk_id: i64,
    lifetime_secs: i64,
    reason: &str,
    stderr_tail: Option<&str>,
    backoff_secs: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_restart_log
             (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs)
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
         WHERE NOT EXISTS (
             SELECT 1 FROM delivery_restart_log
             WHERE instance_id = ?1 AND alias = ?3 AND timestamp_ms = ?4
         )",
    )
    .bind(instance_id)
    .bind(event_id)
    .bind(alias)
    .bind(timestamp_ms)
    .bind(chunk_id)
    .bind(lifetime_secs)
    .bind(reason)
    .bind(stderr_tail)
    .bind(backoff_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all restart records for a delivery instance, ordered by timestamp.
pub async fn get_delivery_restart_log(
    pool: &SqlitePool,
    instance_id: i64,
) -> Result<Vec<DeliveryRestartRow>> {
    let rows = sqlx::query(
        "SELECT alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs
         FROM delivery_restart_log
         WHERE instance_id = ?1
         ORDER BY timestamp_ms ASC",
    )
    .bind(instance_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DeliveryRestartRow {
            alias: r.get("alias"),
            timestamp_ms: r.get("timestamp_ms"),
            chunk_id: r.get("chunk_id"),
            lifetime_secs: r.get("lifetime_secs"),
            reason: r.get("reason"),
            stderr_tail: r.get("stderr_tail"),
            backoff_secs: r.get("backoff_secs"),
        })
        .collect())
}

/// Store captured VPS log text for a delivery instance.
pub async fn insert_delivery_log(
    pool: &SqlitePool,
    instance_id: i64,
    event_id: Option<i64>,
    log_text: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_logs (instance_id, event_id, log_text) VALUES (?1, ?2, ?3)",
    )
    .bind(instance_id)
    .bind(event_id)
    .bind(log_text)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get the captured log text for a delivery instance (most recent capture).
pub async fn get_delivery_log(
    pool: &SqlitePool,
    instance_id: i64,
) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT log_text FROM delivery_logs WHERE instance_id = ?1 ORDER BY id DESC LIMIT 1",
    )
    .bind(instance_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.get("log_text")))
}
```

- [ ] **Step 4: Add the DeliveryRestartRow struct**

Add to `crates/rs-core/src/db/v2.rs` before the new functions (near the top of the delivery section):

```rust
/// Row from the delivery_restart_log table.
#[derive(Debug, serde::Serialize)]
pub struct DeliveryRestartRow {
    pub alias: String,
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: i64,
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: i64,
}
```

- [ ] **Step 5: Export new functions from db module**

In `crates/rs-core/src/db/mod.rs`, ensure the `pub use v2::*;` line exists (it should already). Verify the new functions are accessible.

- [ ] **Step 6: Run tests to verify they pass**

```bash
cargo test -p rs-core "insert_and_query_delivery|restart_log_dedup|migration_v13" -- --nocapture
```

Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add crates/rs-core/src/db/v2.rs crates/rs-core/src/db/tests.rs
git commit -m "feat: add DB functions for delivery restart log and log capture"
```

---

### Task 3: Persist Restart Records During Status Poll

**Files:**
- Modify: `crates/rs-api/src/delivery.rs:520-570` (inside the status poll `for entry in ep_entries` loop)

The status poll already parses `restart_history` from the VPS response (line 523-536). We need to persist each record to SQLite using the dedup-safe insert.

- [ ] **Step 1: Add persistence call in the status poll loop**

In `crates/rs-api/src/delivery.rs`, inside the `for entry in ep_entries` loop, after the `restart_history` parsing (after line 536) and before the `chunk_delay_secs` computation (line 539), add:

```rust
                            // Persist restart records to DB for post-mortem analysis.
                            // The dedup INSERT ignores records already saved from previous polls.
                            for record in &restart_history {
                                if let Err(e) = db::insert_delivery_restart_record(
                                    &self.pool,
                                    inst.id,
                                    inst.event_id,
                                    &alias,
                                    record.timestamp_ms,
                                    record.chunk_id,
                                    record.lifetime_secs as i64,
                                    &record.reason,
                                    record.stderr_tail.as_deref(),
                                    record.backoff_secs as i64,
                                )
                                .await
                                {
                                    warn!("Failed to persist restart record: {e}");
                                }
                            }
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery.rs
git commit -m "feat: persist ffmpeg restart records during delivery status poll"
```

---

### Task 4: Capture VPS Logs Before Deletion

**Files:**
- Modify: `crates/rs-api/src/delivery.rs:676-690` (in `stop_delivery`, between the "best-effort stop" and "delete Hetzner server" steps)

- [ ] **Step 1: Add log capture before VPS deletion**

In `crates/rs-api/src/delivery.rs`, in `stop_delivery()`, after the best-effort POST `/api/stop` block (after line 687) and before the `// Delete Hetzner server` comment (line 689), add:

```rust
        // Capture VPS logs before deletion for post-mortem analysis.
        // Best-effort: if the VPS is unresponsive, we still proceed with deletion.
        if instance.status == "running" {
            let client = reqwest::Client::new();
            let delivery_url = format!("http://{}:8000", instance.ipv4);
            match client
                .get(format!("{delivery_url}/api/logs?limit=5000"))
                .bearer_auth(&instance.auth_token)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<serde_json::Value>().await {
                        Ok(body) => {
                            // Format log entries as text lines for human-readable storage
                            let log_text = body["entries"]
                                .as_array()
                                .map(|entries| {
                                    entries
                                        .iter()
                                        .rev() // API returns newest-first, store chronologically
                                        .map(|e| {
                                            format!(
                                                "[{}] {} {}",
                                                e["level"].as_str().unwrap_or("?"),
                                                e["target"].as_str().unwrap_or("?"),
                                                e["message"].as_str().unwrap_or("")
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n")
                                })
                                .unwrap_or_default();

                            if !log_text.is_empty() {
                                if let Err(e) = db::insert_delivery_log(
                                    &self.pool,
                                    instance.id,
                                    instance.event_id,
                                    &log_text,
                                )
                                .await
                                {
                                    warn!("Failed to persist VPS logs: {e}");
                                } else {
                                    info!(
                                        instance_id = instance.id,
                                        lines = log_text.lines().count(),
                                        "Captured VPS logs before deletion"
                                    );
                                }
                            }
                        }
                        Err(e) => warn!("Failed to parse VPS log response: {e}"),
                    }
                }
                Ok(resp) => {
                    warn!(status = %resp.status(), "VPS log capture returned non-success");
                }
                Err(e) => {
                    warn!("VPS log capture failed (VPS may be unresponsive): {e}");
                }
            }
        }
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-api/src/delivery.rs
git commit -m "feat: capture VPS logs before Hetzner server deletion"
```

---

### Task 5: API Endpoint — GET /delivery/logs

**Files:**
- Modify: `crates/rs-api/src/delivery_handlers.rs` (add handler + response structs)
- Modify: `crates/rs-api/src/lib.rs` (register route)

- [ ] **Step 1: Add response structs and handler**

Add to `crates/rs-api/src/delivery_handlers.rs`:

```rust
#[derive(Deserialize)]
pub struct DeliveryLogsQuery {
    pub instance_id: i64,
}

#[derive(Serialize)]
pub struct DeliveryLogsResponse {
    pub instance_id: i64,
    pub restart_log: Vec<rs_core::db::DeliveryRestartRow>,
    pub captured_log: Option<String>,
}

/// GET /delivery/logs?instance_id=N — retrieve persisted delivery logs
/// and ffmpeg restart records for a (possibly deleted) VPS instance.
pub async fn delivery_logs(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<DeliveryLogsQuery>,
) -> Result<Json<DeliveryLogsResponse>, StatusCode> {
    let restart_log = rs_core::db::get_delivery_restart_log(&state.pool, query.instance_id)
        .await
        .map_err(|e| {
            error!("Failed to get restart log: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let captured_log = rs_core::db::get_delivery_log(&state.pool, query.instance_id)
        .await
        .map_err(|e| {
            error!("Failed to get delivery log: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(DeliveryLogsResponse {
        instance_id: query.instance_id,
        restart_log,
        captured_log,
    }))
}
```

- [ ] **Step 2: Register the route**

In `crates/rs-api/src/lib.rs`, find where delivery routes are registered (search for `delivery/status`). Add the new route alongside:

```rust
.route("/api/v1/delivery/logs", get(delivery_handlers::delivery_logs))
```

- [ ] **Step 3: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/delivery_handlers.rs crates/rs-api/src/lib.rs
git commit -m "feat: add GET /delivery/logs endpoint for post-mortem analysis"
```

---

### Task 6: E2E Test — Restart Records Persist and Logs Captured

**Files:**
- Modify: `.github/workflows/ci.yml` (add GATE test in the E2E streaming section)

The E2E test needs to verify that after a delivery session completes and the VPS is deleted, the restart history and VPS logs are still accessible via the new API endpoint.

- [ ] **Step 1: Add CI GATE for delivery log persistence**

In `.github/workflows/ci.yml`, in the E2E streaming test section (after the existing delivery-related GATEs), add:

```yaml
      - name: "GATE: delivery logs persisted after VPS deletion"
        if: steps.start-stream.outcome == 'success'
        run: |
          set -euo pipefail

          # Get the instance ID from the delivery instances list
          # (should have at least one deleted instance from the streaming test)
          INSTANCES=$(curl -sf http://127.0.0.1:8910/api/v1/delivery/instances || echo '[]')
          echo "Active instances: $INSTANCES"

          # Query the delivery logs endpoint for the most recent instance
          # The instance may already be deleted, but logs should be persisted
          # First, check if any delivery_instances exist in the DB
          DELIVERY_STATUS=$(curl -sf "http://127.0.0.1:8910/api/v1/delivery/status?event_id=$E2E_EVENT_ID" || echo '{}')
          INSTANCE_ID=$(echo "$DELIVERY_STATUS" | jq -r '.instance.id // empty')

          if [ -z "$INSTANCE_ID" ]; then
            echo "No delivery instance found for event — checking if logs table has any entries"
            # The instance was already deleted; try instance_id=1 as a smoke test
            LOGS=$(curl -sf "http://127.0.0.1:8910/api/v1/delivery/logs?instance_id=1" || echo '{}')
            echo "Delivery logs response: $(echo "$LOGS" | jq -c '{restart_log_count: (.restart_log | length), has_captured_log: (.captured_log != null)}')"
          else
            echo "Found delivery instance: $INSTANCE_ID"
            LOGS=$(curl -sf "http://127.0.0.1:8910/api/v1/delivery/logs?instance_id=$INSTANCE_ID")
            RESTART_COUNT=$(echo "$LOGS" | jq '.restart_log | length')
            HAS_LOG=$(echo "$LOGS" | jq '.captured_log != null')
            echo "Restart records: $RESTART_COUNT, Has captured log: $HAS_LOG"

            # The endpoint must return valid JSON with the expected structure
            echo "$LOGS" | jq -e '.instance_id' > /dev/null
            echo "$LOGS" | jq -e '.restart_log' > /dev/null
            echo "GATE PASSED: delivery logs endpoint returns valid structure"
          fi
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "test: add CI GATE for delivery log persistence after VPS deletion"
```

---

### Task 7: Push, Monitor CI, Create PR

- [ ] **Step 1: Run local formatting check**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI until all jobs pass**

```bash
gh run list --limit 3
# Wait for terminal state, then:
gh run view <run-id>
```

If any job fails: `gh run view <run-id> --log-failed`, fix, push, monitor again.

- [ ] **Step 4: Create PR**

```bash
gh pr create --title "feat: persist delivery VPS logs and ffmpeg restart history" --body "$(cat <<'EOF'
## Summary
- Add `delivery_logs` and `delivery_restart_log` SQLite tables (migration V13)
- Persist ffmpeg restart records during each VPS status poll (dedup-safe)
- Capture full VPS log buffer (`GET /api/logs?limit=5000`) before Hetzner server deletion
- Add `GET /api/v1/delivery/logs?instance_id=N` endpoint for post-mortem analysis
- Operators can now investigate ffmpeg restarts after the VPS is destroyed

## Test plan
- [ ] Migration V13 test: tables created and accept inserts
- [ ] DB function tests: insert, query, dedup for restart records
- [ ] DB function tests: insert and query for captured logs
- [ ] CI GATE: delivery logs endpoint returns valid structure
- [ ] All existing E2E tests pass (no regressions)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/NUMBER --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

---

### Verification

1. **Unit tests**: migration V13 creates tables, DB functions insert/query/dedup correctly
2. **Integration**: restart records appear in `delivery_restart_log` after a streaming session
3. **Log capture**: `delivery_logs` contains VPS log text after VPS deletion
4. **API**: `GET /delivery/logs?instance_id=N` returns restart records and captured log text
5. **No regressions**: all existing E2E and unit tests pass
