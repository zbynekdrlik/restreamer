# Robust S3 Chunk Upload — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix S3 chunk upload throughput from ~0.5 chunks/s to ≥20 chunks/s and make the pipeline observable end-to-end on the operator dashboard. Closes #118 and #65.

**Architecture:** Replace the batch-of-20 wave uploader with a continuous worker pool whose retries no longer block worker slots (backoff stored in DB per row, not `tokio::sleep` in worker). Adaptive concurrency 4→32. Persist per-chunk telemetry (attempts, duration, last error) to SQLite so operators can see why a chunk is stuck. Surface live stats + drill-down table on the dashboard.

**Tech Stack:** Rust, sqlx (SQLite), Tokio, rust-s3 0.35 (unchanged), Axum, Leptos CSR / WASM, Playwright

**Spec:** `docs/superpowers/specs/2026-04-14-robust-s3-upload-design.md`

> **Table-name note:** the spec uses `chunks` for brevity; the real table is `chunk_records`. All code in this plan uses the correct name (`chunk_records`).

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump 0.3.53 → 0.3.54 in all four files**

Exact lines to change:
- `Cargo.toml`: `version = "0.3.53"` → `version = "0.3.54"`
- `src-tauri/Cargo.toml`: `version = "0.3.53"` → `version = "0.3.54"`
- `src-tauri/tauri.conf.json`: `"version": "0.3.53"` → `"version": "0.3.54"`
- `leptos-ui/Cargo.toml`: `version = "0.3.53"` → `version = "0.3.54"`

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.54 (#118 upload pipeline)"
```

---

### Task 1: Migration V17 — Upload Telemetry Columns

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs` (add `MIGRATION_V17_SQL` constant + register in migrations slice)
- Modify: `crates/rs-core/src/db/tests.rs` (add migration test)

- [ ] **Step 1: Write failing test in `crates/rs-core/src/db/tests.rs`**

Append to the existing test module:

```rust
#[tokio::test]
async fn migration_v17_adds_upload_telemetry_columns() {
    let pool = crate::db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();

    // All V17 columns must exist with expected defaults
    let row = sqlx::query(
        "SELECT upload_attempts, upload_first_attempt_at, upload_completed_at,
                upload_duration_ms, upload_last_error, upload_next_retry_at,
                upload_failed_permanently
         FROM chunk_records LIMIT 1"
    )
    .fetch_optional(&pool)
    .await
    .expect("columns must exist");
    // Empty table is fine; the query succeeding proves the columns exist.
    assert!(row.is_none());

    // Index idx_chunks_upload_queue exists
    let idx: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_chunks_upload_queue'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(idx.is_some(), "idx_chunks_upload_queue must be created by V17");
}
```

- [ ] **Step 2: Run the test, confirm it fails**

```bash
cargo nextest run --package rs-core migration_v17_adds_upload_telemetry_columns
```

Expected: FAIL (columns do not exist yet).

- [ ] **Step 3: Add V17 SQL + register in migration slice**

In `crates/rs-core/src/db/mod.rs`, append after `MIGRATION_V16_SQL`:

```rust
// V17: per-chunk upload telemetry. Lets operators see why a chunk is slow
// or stuck (attempts, last error, duration) and supports row-level
// retry-via-requeue (upload_next_retry_at) so retry sleeps don't block
// worker slots.
const MIGRATION_V17_SQL: &str = r#"
ALTER TABLE chunk_records ADD COLUMN upload_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunk_records ADD COLUMN upload_first_attempt_at INTEGER;
ALTER TABLE chunk_records ADD COLUMN upload_completed_at INTEGER;
ALTER TABLE chunk_records ADD COLUMN upload_duration_ms INTEGER;
ALTER TABLE chunk_records ADD COLUMN upload_last_error TEXT;
ALTER TABLE chunk_records ADD COLUMN upload_next_retry_at INTEGER;
ALTER TABLE chunk_records ADD COLUMN upload_failed_permanently INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_chunks_upload_queue
  ON chunk_records(upload_failed_permanently, sent, in_process, upload_next_retry_at, id)
  WHERE sent = 0 AND in_process = 0 AND upload_failed_permanently = 0
"#;
```

In the same file, change the migrations slice literal (currently ending at V16) by adding `(17, MIGRATION_V17_SQL),` as a new last entry.

- [ ] **Step 4: Run the test, confirm it passes**

```bash
cargo nextest run --package rs-core migration_v17_adds_upload_telemetry_columns
```

Expected: PASS.

- [ ] **Step 5: Format + commit**

```bash
cargo fmt --all
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat(db): V17 migration adds per-chunk upload telemetry columns (#118)"
```

---

### Task 2: Extend ChunkRecord Model with Upload Fields

**Files:**
- Modify: `crates/rs-core/src/models.rs:35-47`
- Modify: `crates/rs-core/src/db/mod.rs` (get_unsent_chunks / set_chunk_* helpers)

- [ ] **Step 1: Write failing test in `crates/rs-core/src/db/tests.rs`**

```rust
#[tokio::test]
async fn chunk_record_round_trips_upload_columns() {
    let pool = crate::db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::upsert_client_profile(&pool, "test-uuid").await.unwrap();

    let event_id = crate::db::create_streaming_event(
        &pool, "evt-1", "desc", "2026-01-01T00:00:00", "1.2.3.4"
    ).await.unwrap();

    let chunk_id = crate::db::create_chunk(
        &pool, event_id, "/tmp/f.bin", 100_000, "md5xxxx", 1, 2000
    ).await.unwrap();

    // New helpers set telemetry; model reflects DB state
    crate::db::record_upload_attempt(&pool, chunk_id, 1735829023000).await.unwrap();
    crate::db::record_upload_failure(&pool, chunk_id, "timeout", 1735829024000, 1200).await.unwrap();

    let chunks = crate::db::get_unsent_chunks(&pool, 10).await.unwrap();
    let c = chunks.iter().find(|c| c.id == chunk_id).expect("chunk should be queryable");
    assert_eq!(c.upload_attempts, 1);
    assert!(c.upload_first_attempt_at.is_some());
    assert_eq!(c.upload_last_error.as_deref(), Some("timeout"));
    assert_eq!(c.upload_duration_ms, Some(1200));
    assert!(c.upload_next_retry_at.is_some());
    assert!(!c.upload_failed_permanently);
}
```

- [ ] **Step 2: Run the test, confirm it fails**

```bash
cargo nextest run --package rs-core chunk_record_round_trips_upload_columns
```

Expected: FAIL — new fields on `ChunkRecord` and helpers `record_upload_attempt`, `record_upload_failure` do not exist.

- [ ] **Step 3: Extend `ChunkRecord`**

In `crates/rs-core/src/models.rs`, replace the `ChunkRecord` struct (line 35–47) with:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    pub id: i64,
    pub streaming_event_id: i64,
    pub chunk_file_path: String,
    pub data_size: i64,
    pub created_at: String,
    pub md5: String,
    pub in_process: bool,
    pub sent: bool,
    pub sequence_number: i64,
    pub duration_ms: i64,
    // V17 upload telemetry
    #[serde(default)]
    pub upload_attempts: i64,
    #[serde(default)]
    pub upload_first_attempt_at: Option<i64>,
    #[serde(default)]
    pub upload_completed_at: Option<i64>,
    #[serde(default)]
    pub upload_duration_ms: Option<i64>,
    #[serde(default)]
    pub upload_last_error: Option<String>,
    #[serde(default)]
    pub upload_next_retry_at: Option<i64>,
    #[serde(default)]
    pub upload_failed_permanently: bool,
}
```

- [ ] **Step 4: Update `get_unsent_chunks` and row mapping**

In `crates/rs-core/src/db/mod.rs`, find `get_unsent_chunks` (line 577) and replace the `SELECT` to include new columns and map them:

```rust
pub async fn get_unsent_chunks(pool: &SqlitePool, limit: i64) -> Result<Vec<ChunkRecord>> {
    let rows = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
                in_process, sent, sequence_number, duration_ms,
                upload_attempts, upload_first_attempt_at, upload_completed_at,
                upload_duration_ms, upload_last_error, upload_next_retry_at,
                upload_failed_permanently
         FROM chunk_records
         WHERE sent = 0
         ORDER BY id ASC
         LIMIT ?1"
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(row_to_chunk_record).collect())
}

fn row_to_chunk_record(row: sqlx::sqlite::SqliteRow) -> ChunkRecord {
    ChunkRecord {
        id: row.get("id"),
        streaming_event_id: row.get("streaming_event_id"),
        chunk_file_path: row.get("chunk_file_path"),
        data_size: row.get("data_size"),
        created_at: row.get("created_at"),
        md5: row.get("md5"),
        in_process: row.get::<i64, _>("in_process") != 0,
        sent: row.get::<i64, _>("sent") != 0,
        sequence_number: row.get("sequence_number"),
        duration_ms: row.get("duration_ms"),
        upload_attempts: row.get("upload_attempts"),
        upload_first_attempt_at: row.get("upload_first_attempt_at"),
        upload_completed_at: row.get("upload_completed_at"),
        upload_duration_ms: row.get("upload_duration_ms"),
        upload_last_error: row.get("upload_last_error"),
        upload_next_retry_at: row.get("upload_next_retry_at"),
        upload_failed_permanently: row.get::<i64, _>("upload_failed_permanently") != 0,
    }
}
```

If `get_chunk_by_id`, `get_streaming_event_by_id`-style helpers exist that also return a `ChunkRecord`, apply the same mapping. Search:

```bash
grep -n "ChunkRecord {" crates/rs-core/src/db/mod.rs
```

Update every construction site to populate the new fields (default values for sites that don't select them — safest is to always SELECT the new columns).

- [ ] **Step 5: Add telemetry helpers**

Append to `crates/rs-core/src/db/mod.rs`:

```rust
/// Record the start of an upload attempt. Bumps `upload_attempts`, sets
/// `upload_first_attempt_at` if it was NULL, clears `in_process=1` handled
/// by the picker not this fn.
pub async fn record_upload_attempt(pool: &SqlitePool, chunk_id: i64, now_ms: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_attempts = upload_attempts + 1,
             upload_first_attempt_at = COALESCE(upload_first_attempt_at, ?2)
         WHERE id = ?1"
    )
    .bind(chunk_id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a failed upload. Sets last_error + duration, schedules next retry,
/// releases in_process so another worker can pick it up after backoff.
pub async fn record_upload_failure(
    pool: &SqlitePool,
    chunk_id: i64,
    error: &str,
    next_retry_at_ms: i64,
    duration_ms: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_last_error = ?2,
             upload_next_retry_at = ?3,
             upload_duration_ms = ?4,
             in_process = 0
         WHERE id = ?1"
    )
    .bind(chunk_id)
    .bind(error)
    .bind(next_retry_at_ms)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a chunk as permanently failed after the retry budget is exhausted.
pub async fn mark_upload_permanently_failed(pool: &SqlitePool, chunk_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET upload_failed_permanently = 1,
             in_process = 0
         WHERE id = ?1"
    )
    .bind(chunk_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Record a successful upload. Sets sent=1, completed_at, duration, clears error.
pub async fn record_upload_success(
    pool: &SqlitePool,
    chunk_id: i64,
    completed_at_ms: i64,
    duration_ms: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE chunk_records
         SET sent = 1,
             in_process = 0,
             upload_completed_at = ?2,
             upload_duration_ms = ?3,
             upload_last_error = NULL
         WHERE id = ?1"
    )
    .bind(chunk_id)
    .bind(completed_at_ms)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}
```

Update the imports at top of `db/mod.rs` so `sqlx::Row` is usable by `row_to_chunk_record` if not already imported.

- [ ] **Step 6: Run test, confirm it passes**

```bash
cargo nextest run --package rs-core chunk_record_round_trips_upload_columns
```

Expected: PASS.

- [ ] **Step 7: Format + commit**

```bash
cargo fmt --all
git add crates/rs-core/src/models.rs crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat(db): ChunkRecord upload telemetry fields + helper fns (#118)"
```

---

### Task 3: Atomic Chunk Picker with Backoff Honoring

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs`
- Modify: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write failing test**

Append to `db/tests.rs`:

```rust
#[tokio::test]
async fn picker_skips_chunks_before_retry_time_and_claims_atomically() {
    let pool = crate::db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = crate::db::create_streaming_event(
        &pool, "evt-1", "d", "2026-01-01T00:00:00", "1.2.3.4"
    ).await.unwrap();

    // Two unsent chunks
    let c1 = crate::db::create_chunk(&pool, event_id, "/tmp/a", 100, "m", 1, 2000).await.unwrap();
    let c2 = crate::db::create_chunk(&pool, event_id, "/tmp/b", 100, "m", 2, 2000).await.unwrap();

    // c1 has retry scheduled in the future, c2 is eligible now
    crate::db::record_upload_failure(&pool, c1, "timeout", 9_999_999_999_999, 500).await.unwrap();

    let now_ms = 1_735_000_000_000_i64;
    let picked = crate::db::pick_next_uploadable_chunk(&pool, now_ms).await.unwrap();
    assert_eq!(picked.as_ref().map(|c| c.id), Some(c2), "should pick eligible one");
    // After pick, c2 is in_process=true
    let row_c2: (i64,) = sqlx::query_as("SELECT in_process FROM chunk_records WHERE id = ?1")
        .bind(c2).fetch_one(&pool).await.unwrap();
    assert_eq!(row_c2.0, 1, "picked chunk must be marked in_process");

    // A second pick returns None (c1 still in future, c2 claimed)
    let again = crate::db::pick_next_uploadable_chunk(&pool, now_ms).await.unwrap();
    assert!(again.is_none(), "no other chunk is eligible");

    // Advancing the clock past c1's retry time lets picker claim it
    let later = 10_000_000_000_000_i64;
    let picked2 = crate::db::pick_next_uploadable_chunk(&pool, later).await.unwrap();
    assert_eq!(picked2.as_ref().map(|c| c.id), Some(c1));
}

#[tokio::test]
async fn picker_skips_permanently_failed() {
    let pool = crate::db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = crate::db::create_streaming_event(
        &pool, "evt-1", "d", "2026-01-01T00:00:00", "1.2.3.4"
    ).await.unwrap();
    let c = crate::db::create_chunk(&pool, event_id, "/tmp/a", 100, "m", 1, 2000).await.unwrap();
    crate::db::mark_upload_permanently_failed(&pool, c).await.unwrap();

    let picked = crate::db::pick_next_uploadable_chunk(&pool, 1_000_000_000_000).await.unwrap();
    assert!(picked.is_none(), "permanently-failed chunks must not be picked");
}
```

- [ ] **Step 2: Run, confirm fails (function does not exist)**

```bash
cargo nextest run --package rs-core picker_
```

- [ ] **Step 3: Implement `pick_next_uploadable_chunk`**

Append to `crates/rs-core/src/db/mod.rs`:

```rust
/// Atomically pick the oldest eligible chunk and mark it `in_process=1`.
/// Returns None if nothing is eligible. Eligibility:
///   - sent = 0
///   - in_process = 0
///   - upload_failed_permanently = 0
///   - upload_next_retry_at IS NULL OR upload_next_retry_at <= now_ms
pub async fn pick_next_uploadable_chunk(
    pool: &SqlitePool,
    now_ms: i64,
) -> Result<Option<ChunkRecord>> {
    let mut tx = pool.begin().await?;

    let row: Option<sqlx::sqlite::SqliteRow> = sqlx::query(
        "SELECT id, streaming_event_id, chunk_file_path, data_size, created_at, md5,
                in_process, sent, sequence_number, duration_ms,
                upload_attempts, upload_first_attempt_at, upload_completed_at,
                upload_duration_ms, upload_last_error, upload_next_retry_at,
                upload_failed_permanently
         FROM chunk_records
         WHERE sent = 0
           AND in_process = 0
           AND upload_failed_permanently = 0
           AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= ?1)
         ORDER BY id ASC
         LIMIT 1"
    )
    .bind(now_ms)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(row) = row else { return Ok(None); };

    let chunk = row_to_chunk_record(row);

    // Atomic claim — if another worker grabbed it between SELECT and UPDATE,
    // our UPDATE affects 0 rows and we return None.
    let result = sqlx::query(
        "UPDATE chunk_records SET in_process = 1
         WHERE id = ?1 AND in_process = 0 AND sent = 0"
    )
    .bind(chunk.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    if result.rows_affected() == 0 {
        return Ok(None);
    }
    Ok(Some(chunk))
}
```

- [ ] **Step 4: Run tests, confirm pass**

```bash
cargo nextest run --package rs-core picker_
```

- [ ] **Step 5: Format + commit**

```bash
cargo fmt --all
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat(db): pick_next_uploadable_chunk with retry-time + atomic claim (#118)"
```

---

### Task 4: Upload Metrics Ring Buffer

**Files:**
- Create: `crates/rs-endpoint/src/metrics.rs`
- Modify: `crates/rs-endpoint/src/lib.rs` (pub mod)

- [ ] **Step 1: Write failing test**

Create `crates/rs-endpoint/src/metrics.rs`:

```rust
//! In-memory upload metrics for the /uploads/stats API.
//!
//! Tracks successes + failures + durations in a bounded ring buffer per
//! worker event. Computes chunks/s (1-minute window) and p50/p95 latency.

use std::sync::Mutex;
use std::time::{Duration, Instant};

const RING_CAPACITY: usize = 2048;

#[derive(Clone, Copy, Debug)]
pub struct UploadEvent {
    pub at: Instant,
    pub duration_ms: u32,
    pub success: bool,
}

pub struct UploadMetrics {
    inner: Mutex<Inner>,
}

struct Inner {
    ring: Vec<UploadEvent>,
    head: usize,
    filled: bool,
    in_flight: usize,
    adaptive_target: usize,
}

impl Default for UploadMetrics {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                ring: Vec::with_capacity(RING_CAPACITY),
                head: 0,
                filled: false,
                in_flight: 0,
                adaptive_target: 4,
            }),
        }
    }
}

impl UploadMetrics {
    pub fn record(&self, event: UploadEvent) {
        let mut g = self.inner.lock().unwrap();
        if g.ring.len() < RING_CAPACITY {
            g.ring.push(event);
        } else {
            let h = g.head;
            g.ring[h] = event;
            g.head = (g.head + 1) % RING_CAPACITY;
            g.filled = true;
        }
    }

    pub fn set_in_flight(&self, n: usize) {
        self.inner.lock().unwrap().in_flight = n;
    }

    pub fn set_adaptive_target(&self, n: usize) {
        self.inner.lock().unwrap().adaptive_target = n;
    }

    pub fn snapshot(&self, window: Duration) -> Snapshot {
        let g = self.inner.lock().unwrap();
        let cutoff = Instant::now().checked_sub(window);
        let events: Vec<UploadEvent> = g
            .ring
            .iter()
            .copied()
            .filter(|e| cutoff.map(|c| e.at >= c).unwrap_or(true))
            .collect();

        let total = events.len();
        let successes = events.iter().filter(|e| e.success).count();
        let failures = total - successes;
        let mut durations: Vec<u32> = events
            .iter()
            .filter(|e| e.success)
            .map(|e| e.duration_ms)
            .collect();
        durations.sort_unstable();

        let median_ms = percentile(&durations, 50);
        let p95_ms = percentile(&durations, 95);
        let chunks_per_sec = if window.as_secs() == 0 {
            0.0
        } else {
            successes as f64 / window.as_secs_f64()
        };
        let error_rate = if total == 0 {
            0.0
        } else {
            failures as f64 / total as f64
        };

        Snapshot {
            chunks_per_sec,
            median_ms,
            p95_ms,
            error_rate,
            in_flight: g.in_flight,
            adaptive_target: g.adaptive_target,
        }
    }
}

fn percentile(sorted: &[u32], p: u32) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as u64 * p as u64) / 100).min(sorted.len() as u64 - 1) as usize;
    sorted[idx]
}

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq)]
pub struct Snapshot {
    pub chunks_per_sec: f64,
    pub median_ms: u32,
    pub p95_ms: u32,
    pub error_rate: f64,
    pub in_flight: usize,
    pub adaptive_target: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_is_zero() {
        let m = UploadMetrics::default();
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.chunks_per_sec, 0.0);
        assert_eq!(s.median_ms, 0);
        assert_eq!(s.p95_ms, 0);
        assert_eq!(s.error_rate, 0.0);
    }

    #[test]
    fn percentile_of_empty_is_zero() {
        assert_eq!(percentile(&[], 50), 0);
    }

    #[test]
    fn percentile_is_monotonic() {
        let v: Vec<u32> = (0..100).collect();
        let median = percentile(&v, 50);
        let p95 = percentile(&v, 95);
        assert!(p95 > median);
    }

    #[test]
    fn snapshot_counts_successes_for_rate_and_error_rate_for_failures() {
        let m = UploadMetrics::default();
        let now = Instant::now();
        for _ in 0..4 {
            m.record(UploadEvent { at: now, duration_ms: 100, success: true });
        }
        m.record(UploadEvent { at: now, duration_ms: 5000, success: false });

        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.error_rate, 0.2, "1 of 5 failed");
        assert!(s.chunks_per_sec > 0.0, "at least one success is counted");
        assert_eq!(s.median_ms, 100, "median over successes only");
    }

    #[test]
    fn set_in_flight_and_target_are_reflected() {
        let m = UploadMetrics::default();
        m.set_in_flight(7);
        m.set_adaptive_target(16);
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.in_flight, 7);
        assert_eq!(s.adaptive_target, 16);
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/rs-endpoint/src/lib.rs` add `pub mod metrics;` near the top.

- [ ] **Step 3: Run tests, confirm pass**

```bash
cargo nextest run --package rs-endpoint metrics::
```

- [ ] **Step 4: Format + commit**

```bash
cargo fmt --all
git add crates/rs-endpoint/src/metrics.rs crates/rs-endpoint/src/lib.rs
git commit -m "feat(endpoint): UploadMetrics ring buffer + snapshot (#118)"
```

---

### Task 5: Worker Pool with Row-Level Retry

**Files:**
- Modify: `crates/rs-endpoint/src/uploader.rs` (replace `run` and `upload_batch`)
- Modify: `crates/rs-core/src/models.rs` (add `WsEvent::ChunkUploadAttempt`, `WsEvent::ChunkUploadFailed`)

- [ ] **Step 1: Extend `WsEvent`**

In `crates/rs-core/src/models.rs`, find the `WsEvent` enum. Append two variants (keep existing ones intact):

```rust
    ChunkUploadAttempt { chunk_id: i64, attempt: i64 },
    ChunkUploadFailed { chunk_id: i64, error: String, permanent: bool },
```

- [ ] **Step 2: Write failing integration test in `crates/rs-endpoint/tests/uploader_integration.rs`**

Add this test (it requires MinIO env vars already used by the existing integration test; the existing test file has the wiring pattern — re-use it):

```rust
#[tokio::test]
#[ignore = "requires MinIO — run with: cargo nextest run --package rs-endpoint --run-ignored all"]
async fn worker_pool_uploads_concurrently_and_requeues_failures() {
    // This test is gated behind #[ignore] because it needs MinIO.
    // Removed per test-strictness rule in the same PR that wires MinIO into CI.
    // (This block kept to show intent — actual test goes in the next step once
    //  MinIO is set up; for now, assert the API surface exists.)

    use rs_endpoint::uploader::ChunkUploader;
    // If this compiles, the API contract is right.
    let _assert_type = std::any::TypeId::of::<ChunkUploader>();
}
```

Actually, replace that placeholder with a **real test that does not need MinIO**, using a `FakeS3` trait seam (see Step 3). If a trait seam is too invasive, keep the real MinIO-backed test and rely on unit tests in `uploader.rs` for logic coverage. Choose whichever is smaller.

- [ ] **Step 3: Replace `uploader.rs` with the worker-pool implementation**

Rewrite `crates/rs-endpoint/src/uploader.rs` to:

- Keep `ChunkUploader::new(pool, s3, ws_tx)` signature unchanged (callers depend on it).
- Hold `Arc<UploadMetrics>` internally (constructed in `new`).
- Expose `pub fn metrics(&self) -> Arc<UploadMetrics>` so `rs-api` can read live stats.
- `run(&self, shutdown: broadcast::Receiver<()>)` spawns N workers where N is the adaptive target, plus one adaptive controller task. All child tasks listen on the same `shutdown` channel (clone it).

Worker loop pseudo-code (translate directly to Rust; no placeholders):

```rust
loop {
    tokio::select! {
        _ = shutdown.recv() => break,
        _ = async {} => {}
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let picked = match db::pick_next_uploadable_chunk(&pool, now_ms).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        Err(e) => {
            tracing::error!("picker DB error: {e}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
    };

    let event_id = match db::get_streaming_event_by_id(&pool, picked.streaming_event_id).await {
        Ok(Some(ev)) => ev.name,
        _ => {
            let _ = db::record_upload_success(&pool, picked.id, now_ms, 0).await; // drop it: event gone
            continue;
        }
    };

    // Mark the attempt BEFORE uploading so the dashboard sees attempts tick up
    let _ = db::record_upload_attempt(&pool, picked.id, now_ms).await;
    let _ = ws_tx.send(WsEvent::ChunkUploadAttempt {
        chunk_id: picked.id,
        attempt: picked.upload_attempts + 1,
    });

    metrics.set_in_flight(in_flight.fetch_add(1, Ordering::SeqCst) + 1);
    let started = Instant::now();
    let result = s3
        .upload_chunk(
            Path::new(&picked.chunk_file_path),
            &event_id,
            picked.sequence_number,
            picked.duration_ms,
        )
        .await;
    let duration = started.elapsed();
    metrics.set_in_flight(in_flight.fetch_sub(1, Ordering::SeqCst) - 1);

    match result {
        Ok(()) => {
            let completed_at = chrono::Utc::now().timestamp_millis();
            let _ = db::record_upload_success(
                &pool, picked.id, completed_at, duration.as_millis() as i64,
            ).await;
            let _ = tokio::fs::remove_file(&picked.chunk_file_path).await;
            metrics.record(UploadEvent {
                at: Instant::now(),
                duration_ms: duration.as_millis() as u32,
                success: true,
            });
            let _ = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: picked.id });
        }
        Err(e) => {
            let attempt = picked.upload_attempts + 1;
            let wall_clock_ms = chrono::Utc::now().timestamp_millis()
                - picked.upload_first_attempt_at.unwrap_or(now_ms);
            let permanent = attempt >= MAX_ATTEMPTS || wall_clock_ms >= MAX_WALL_CLOCK_MS;
            if permanent {
                let _ = db::mark_upload_permanently_failed(&pool, picked.id).await;
                let _ = ws_tx.send(WsEvent::ChunkUploadFailed {
                    chunk_id: picked.id,
                    error: e.to_string(),
                    permanent: true,
                });
            } else {
                let backoff_ms = backoff_ms(attempt);
                let next = chrono::Utc::now().timestamp_millis() + backoff_ms as i64;
                let _ = db::record_upload_failure(
                    &pool, picked.id, &e.to_string(), next, duration.as_millis() as i64,
                ).await;
                let _ = ws_tx.send(WsEvent::ChunkUploadFailed {
                    chunk_id: picked.id,
                    error: e.to_string(),
                    permanent: false,
                });
            }
            metrics.record(UploadEvent {
                at: Instant::now(),
                duration_ms: duration.as_millis() as u32,
                success: false,
            });
        }
    }
}
```

Constants (at top of file):

```rust
const MAX_ATTEMPTS: i64 = 10;
const MAX_WALL_CLOCK_MS: i64 = 600_000; // 10 min
const MIN_CONCURRENCY: usize = 4;
const MAX_CONCURRENCY: usize = 32;

fn backoff_ms(attempt: i64) -> u64 {
    // 1s, 2s, 4s, 8s, 16s, 30s (cap)
    let base = 1000u64;
    let shift = (attempt.saturating_sub(1) as u32).min(5);
    base.saturating_mul(1 << shift).min(30_000)
}
```

Add unit tests at the bottom of `uploader.rs`:

```rust
#[cfg(test)]
mod uploader_unit_tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(2), 2000);
        assert_eq!(backoff_ms(3), 4000);
        assert_eq!(backoff_ms(4), 8000);
        assert_eq!(backoff_ms(5), 16_000);
        assert_eq!(backoff_ms(6), 30_000);
        assert_eq!(backoff_ms(100), 30_000);
    }

    #[test]
    fn backoff_attempt_zero_is_sane() {
        // Defensive: attempt=0 should not panic and should give at least 1s.
        assert!(backoff_ms(0) >= 1000);
    }
}
```

The old `upload_batch` is no longer needed. Delete it and its tests (`upload_batch_with_no_chunks_returns_quickly`) — they are no longer the API surface. Replace the removed tests with one that verifies the pool shuts down cleanly given N=0 chunks:

```rust
#[tokio::test]
async fn uploader_shuts_down_cleanly_with_no_chunks() {
    let pool = setup_db().await;
    let s3 = S3Client::new(&test_s3_config()).unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    let uploader = ChunkUploader::new(pool, s3, ws_tx);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("timed out")
        .expect("panicked");
}
```

Any existing `upload_blocked` flag plumbing (for the CI resilience test) — keep it, but apply at the worker-loop level (early-continue if blocked).

- [ ] **Step 4: Run unit tests, confirm pass**

```bash
cargo fmt --all
cargo nextest run --package rs-endpoint uploader
```

- [ ] **Step 5: Commit**

```bash
git add crates/rs-endpoint/src/uploader.rs crates/rs-core/src/models.rs
git commit -m "feat(endpoint): worker pool with row-level retry, metrics, WS events (#118)"
```

---

### Task 6: Adaptive Concurrency Controller

**Files:**
- Modify: `crates/rs-endpoint/src/uploader.rs` (add controller module)

- [ ] **Step 1: Write failing unit test**

Inside `uploader.rs` `uploader_unit_tests` module:

```rust
#[test]
fn adaptive_scales_up_on_zero_errors_fast_median() {
    let mut target = 4usize;
    target = adjust_target(target, /*error_rate*/ 0.0, /*median_ms*/ 200);
    assert_eq!(target, 8);
    target = adjust_target(target, 0.0, 200);
    assert_eq!(target, 16);
    target = adjust_target(target, 0.0, 200);
    assert_eq!(target, 32);
    target = adjust_target(target, 0.0, 200);
    assert_eq!(target, 32, "capped at MAX_CONCURRENCY");
}

#[test]
fn adaptive_scales_down_on_errors() {
    let mut target = 32usize;
    target = adjust_target(target, 0.3, 200);
    assert_eq!(target, 16);
    target = adjust_target(target, 0.3, 200);
    assert_eq!(target, 8);
    target = adjust_target(target, 0.3, 200);
    assert_eq!(target, 4);
    target = adjust_target(target, 0.3, 200);
    assert_eq!(target, 4, "capped at MIN_CONCURRENCY");
}

#[test]
fn adaptive_holds_when_median_is_slow() {
    // error_rate = 0 but median >= 500ms → do not scale up
    assert_eq!(adjust_target(8, 0.0, 600), 8);
    assert_eq!(adjust_target(8, 0.0, 500), 8);
}

#[test]
fn adaptive_holds_on_borderline_error_rate() {
    // error_rate = 0.2 exactly → does not scale down (strict >)
    assert_eq!(adjust_target(8, 0.2, 200), 8);
}
```

- [ ] **Step 2: Run tests, confirm fail**

```bash
cargo nextest run --package rs-endpoint adaptive_
```

- [ ] **Step 3: Implement `adjust_target`**

At top-level in `uploader.rs`, above the impl block:

```rust
/// Pure-function core of the adaptive concurrency controller.
/// Scales up (×2) when error_rate == 0 AND median_ms < 500.
/// Scales down (÷2) when error_rate > 0.2.
/// Otherwise holds. Bounded to [MIN_CONCURRENCY, MAX_CONCURRENCY].
pub(crate) fn adjust_target(current: usize, error_rate: f64, median_ms: u32) -> usize {
    if error_rate == 0.0 && median_ms < 500 {
        (current.saturating_mul(2)).min(MAX_CONCURRENCY)
    } else if error_rate > 0.2 {
        (current / 2).max(MIN_CONCURRENCY)
    } else {
        current
    }
}
```

- [ ] **Step 4: Wire into the running uploader**

Inside `ChunkUploader::run`, spawn a controller task alongside workers:

```rust
let controller_metrics = Arc::clone(&self.metrics);
let controller_tx = worker_target_tx.clone();
let mut controller_shutdown = shutdown.resubscribe();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut current = MIN_CONCURRENCY;
    loop {
        tokio::select! {
            _ = controller_shutdown.recv() => break,
            _ = interval.tick() => {
                let snap = controller_metrics.snapshot(Duration::from_secs(10));
                let next = adjust_target(current, snap.error_rate, snap.median_ms);
                if next != current {
                    tracing::info!(
                        "Adaptive concurrency {current} -> {next} (err={:.2}, med={}ms)",
                        snap.error_rate, snap.median_ms,
                    );
                    current = next;
                    controller_metrics.set_adaptive_target(current);
                    let _ = controller_tx.send(current);
                }
            }
        }
    }
});
```

Create a `tokio::sync::watch::channel(MIN_CONCURRENCY)` named `worker_target_tx` / `worker_target_rx` at the top of `run()`. Each worker task holds a clone of `worker_target_rx`; at the top of its loop it checks `*worker_target_rx.borrow()` and if worker's index ≥ that value, it breaks the loop and exits (graceful wind-down). When target grows, `run()` spawns additional workers with higher indices.

Concrete worker spawn strategy inside `run()`:

```rust
let mut spawned: usize = 0;
let mut rx = worker_target_rx.clone();
loop {
    // Spawn new workers up to current target
    let target = *rx.borrow();
    while spawned < target {
        let idx = spawned;
        spawned += 1;
        // ... spawn worker with index `idx`; worker exits when *rx.borrow() <= idx
    }
    tokio::select! {
        _ = shutdown.recv() => break,
        _ = rx.changed() => {} // loop again, maybe spawn more
    }
}
```

Workers exiting when target drops below their index is fine (they finish their current chunk first).

- [ ] **Step 5: Run tests, confirm pass**

```bash
cargo fmt --all
cargo nextest run --package rs-endpoint
```

- [ ] **Step 6: Commit**

```bash
git add crates/rs-endpoint/src/uploader.rs
git commit -m "feat(endpoint): adaptive concurrency controller 4..32 (#118)"
```

---

### Task 7: API Endpoints /uploads/stats and /uploads/recent

**Files:**
- Create: `crates/rs-api/src/uploads_endpoints.rs`
- Modify: `crates/rs-api/src/routes.rs` (register routes)
- Modify: `crates/rs-api/src/state.rs` (hold `Arc<UploadMetrics>`) — if that's not already the pattern, add an equivalent
- Modify: `crates/rs-core/src/models.rs` (add `UploadChunkRow` response struct)

- [ ] **Step 1: Add `UploadChunkRow` to models**

In `crates/rs-core/src/models.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadChunkRow {
    pub chunk_id: i64,
    pub event_identifier: String,
    pub sequence_number: i64,
    pub size_bytes: i64,
    pub attempts: i64,
    pub duration_ms: Option<i64>,
    pub status: String, // "sent" | "pending" | "retrying" | "failed"
    pub last_error: Option<String>,
    pub first_attempt_at: Option<i64>,
    pub completed_at: Option<i64>,
}
```

- [ ] **Step 2: Add DB query `list_recent_uploads`**

In `crates/rs-core/src/db/mod.rs`:

```rust
/// List the most-recent N chunks (by id desc) with their upload telemetry
/// joined to the streaming_events.identifier.
pub async fn list_recent_uploads(pool: &SqlitePool, limit: i64) -> Result<Vec<UploadChunkRow>> {
    let rows = sqlx::query(
        "SELECT c.id, e.identifier, c.sequence_number, c.data_size,
                c.upload_attempts, c.upload_duration_ms,
                c.sent, c.in_process, c.upload_failed_permanently,
                c.upload_last_error, c.upload_first_attempt_at, c.upload_completed_at
         FROM chunk_records c
         LEFT JOIN streaming_events e ON e.id = c.streaming_event_id
         ORDER BY c.id DESC
         LIMIT ?1"
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let sent: i64 = r.get("sent");
            let in_proc: i64 = r.get("in_process");
            let failed: i64 = r.get("upload_failed_permanently");
            let attempts: i64 = r.get("upload_attempts");
            let status = if sent == 1 {
                "sent"
            } else if failed == 1 {
                "failed"
            } else if in_proc == 1 {
                "retrying"
            } else if attempts > 0 {
                "retrying"
            } else {
                "pending"
            }
            .to_string();

            UploadChunkRow {
                chunk_id: r.get("id"),
                event_identifier: r.try_get("identifier").unwrap_or_default(),
                sequence_number: r.get("sequence_number"),
                size_bytes: r.get("data_size"),
                attempts,
                duration_ms: r.get("upload_duration_ms"),
                status,
                last_error: r.get("upload_last_error"),
                first_attempt_at: r.get("upload_first_attempt_at"),
                completed_at: r.get("upload_completed_at"),
            }
        })
        .collect())
}
```

- [ ] **Step 3: Write failing unit test for the DB query**

In `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn list_recent_uploads_returns_status_transitions() {
    let pool = crate::db::create_pool(std::path::Path::new(":memory:"))
        .await
        .unwrap();
    crate::db::run_migrations(&pool).await.unwrap();
    crate::db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = crate::db::create_streaming_event(
        &pool, "evt-a", "d", "2026-01-01T00:00:00", "1.2.3.4"
    ).await.unwrap();

    let c1 = crate::db::create_chunk(&pool, event_id, "/tmp/a", 100, "m", 1, 2000).await.unwrap();
    let c2 = crate::db::create_chunk(&pool, event_id, "/tmp/b", 200, "m", 2, 2000).await.unwrap();
    let c3 = crate::db::create_chunk(&pool, event_id, "/tmp/c", 300, "m", 3, 2000).await.unwrap();

    crate::db::record_upload_success(&pool, c1, 123, 150).await.unwrap();
    crate::db::record_upload_failure(&pool, c2, "oops", 99999999999, 500).await.unwrap();
    crate::db::mark_upload_permanently_failed(&pool, c3).await.unwrap();

    let rows = crate::db::list_recent_uploads(&pool, 10).await.unwrap();
    let by_id: std::collections::HashMap<i64, &crate::models::UploadChunkRow> =
        rows.iter().map(|r| (r.chunk_id, r)).collect();
    assert_eq!(by_id[&c1].status, "sent");
    assert_eq!(by_id[&c2].status, "retrying");
    assert_eq!(by_id[&c3].status, "failed");
    assert_eq!(by_id[&c2].last_error.as_deref(), Some("oops"));
}
```

- [ ] **Step 4: Run test, confirm pass**

```bash
cargo nextest run --package rs-core list_recent_uploads_returns_status_transitions
```

- [ ] **Step 5: Create API endpoints file**

Create `crates/rs-api/src/uploads_endpoints.rs`:

```rust
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

use rs_core::db;
use rs_core::models::UploadChunkRow;
use rs_endpoint::metrics::{Snapshot, UploadMetrics};

use crate::state::AppState; // adapt to the actual state module path

#[derive(Deserialize)]
pub struct RecentQuery {
    limit: Option<i64>,
}

pub async fn get_uploads_stats(
    State(state): State<AppState>,
) -> Result<Json<Snapshot>, (StatusCode, String)> {
    let metrics: Arc<UploadMetrics> = state.upload_metrics.clone();
    let snap = metrics.snapshot(Duration::from_secs(60));
    Ok(Json(snap))
}

pub async fn get_recent_uploads(
    State(state): State<AppState>,
    Query(q): Query<RecentQuery>,
) -> Result<Json<Vec<UploadChunkRow>>, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let rows = db::list_recent_uploads(&state.db_pool, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows))
}
```

If `AppState` doesn't currently hold `upload_metrics: Arc<UploadMetrics>`, thread it through from main (the `ChunkUploader` is constructed in `rs-runtime`; expose `uploader.metrics()` and pass to API state).

- [ ] **Step 6: Register the routes**

In `crates/rs-api/src/routes.rs`, add:

```rust
    .route("/api/v1/uploads/stats", get(uploads_endpoints::get_uploads_stats))
    .route("/api/v1/uploads/recent", get(uploads_endpoints::get_recent_uploads))
```

And add `pub mod uploads_endpoints;` in `crates/rs-api/src/lib.rs`.

- [ ] **Step 7: Add HTTP-level API test**

In `crates/rs-api/src/uploads_endpoints.rs` (cfg test) or a new file:

```rust
#[cfg(test)]
mod tests {
    // Follow the existing pattern from other rs-api endpoint tests
    // (e.g., delivery_endpoints tests) for spinning up a test Axum app
    // against an in-memory SQLite. Assert:
    //  - GET /api/v1/uploads/stats returns 200 with JSON containing
    //    all Snapshot fields (even when empty).
    //  - GET /api/v1/uploads/recent returns 200 with []  on empty DB.
    //  - limit clamp: ?limit=10000 yields at most 1000 rows.
}
```

- [ ] **Step 8: Run tests, commit**

```bash
cargo fmt --all
cargo nextest run --package rs-api uploads
git add crates/rs-api/src/uploads_endpoints.rs crates/rs-api/src/routes.rs \
        crates/rs-api/src/lib.rs crates/rs-core/src/models.rs \
        crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs \
        crates/rs-api/src/state.rs
git commit -m "feat(api): /api/v1/uploads/{stats,recent} endpoints (#118, #65)"
```

---

### Task 8: Operator Dashboard Inline Upload Strip

**Files:**
- Modify: `leptos-ui/src/components/operator_dashboard.rs`
- Modify: `leptos-ui/src/api.rs` (add `fetch_upload_stats`)

- [ ] **Step 1: Add API fetcher**

In `leptos-ui/src/api.rs`:

```rust
pub async fn fetch_upload_stats() -> Result<UploadStats, String> {
    let resp = gloo_net::http::Request::get("/api/v1/uploads/stats")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json::<UploadStats>().await.map_err(|e| e.to_string())
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
pub struct UploadStats {
    pub chunks_per_sec: f64,
    pub median_ms: u32,
    pub p95_ms: u32,
    pub error_rate: f64,
    pub in_flight: usize,
    pub adaptive_target: usize,
}
```

- [ ] **Step 2: Add strip component**

In `leptos-ui/src/components/operator_dashboard.rs`, under the existing S3 cache bar, add:

```rust
let upload_stats = create_resource(
    move || (), // refresh every 2s via interval (below)
    |_| async { api::fetch_upload_stats().await.ok() }
);

// Interval refresh every 2s
create_effect(move |_| {
    let handle = set_interval_with_handle(
        move || upload_stats.refetch(),
        std::time::Duration::from_secs(2),
    ).ok();
    on_cleanup(move || { if let Some(h) = handle { h.clear(); } });
});

view! {
    <div class="upload-strip" on:click=move |_| navigate("/uploads")>
        {move || upload_stats.get().flatten().map(|s| view! {
            <span class="upload-strip__rate">
                {format!("{:.1} c/s", s.chunks_per_sec)}
            </span>
            <span class="upload-strip__median">
                {format!("median {}ms", s.median_ms)}
            </span>
            <span class="upload-strip__inflight">
                {format!("in-flight {}/{}", s.in_flight, s.adaptive_target)}
            </span>
            <span class=move || if s.error_rate > 0.0 {
                "upload-strip__errors upload-strip__errors--alert"
            } else {
                "upload-strip__errors"
            }>
                {format!("errors {:.0}%", s.error_rate * 100.0)}
            </span>
        })}
    </div>
}
```

Adapt to the actual Leptos idioms used elsewhere in the file (check a neighboring component for the exact pattern). Add CSS classes `.upload-strip`, `.upload-strip__rate`, etc. to the stylesheet (`leptos-ui/style/main.scss` or equivalent). Use the existing color palette; red on alert.

- [ ] **Step 3: Run `trunk serve` locally OR rely on CI build**

(Per project policy we do not compile Leptos locally. Trust CI's `trunk build` job.)

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/operator_dashboard.rs leptos-ui/src/api.rs leptos-ui/style/main.scss
git commit -m "feat(ui): inline upload strip on operator dashboard (#65)"
```

---

### Task 9: /uploads Drill-Down Page

**Files:**
- Create: `leptos-ui/src/pages/uploads.rs`
- Modify: `leptos-ui/src/pages/mod.rs` (register)
- Modify: `leptos-ui/src/router.rs` (add route `/uploads`)
- Modify: `leptos-ui/src/api.rs` (add `fetch_recent_uploads`)

- [ ] **Step 1: Fetcher**

```rust
pub async fn fetch_recent_uploads(limit: u32) -> Result<Vec<UploadRow>, String> {
    let resp = gloo_net::http::Request::get(&format!("/api/v1/uploads/recent?limit={limit}"))
        .send().await.map_err(|e| e.to_string())?;
    resp.json::<Vec<UploadRow>>().await.map_err(|e| e.to_string())
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
pub struct UploadRow {
    pub chunk_id: i64,
    pub event_identifier: String,
    pub sequence_number: i64,
    pub size_bytes: i64,
    pub attempts: i64,
    pub duration_ms: Option<i64>,
    pub status: String,
    pub last_error: Option<String>,
    pub first_attempt_at: Option<i64>,
    pub completed_at: Option<i64>,
}
```

- [ ] **Step 2: Page component**

Create `leptos-ui/src/pages/uploads.rs`:

```rust
use leptos::*;
use crate::api::{fetch_recent_uploads, UploadRow};

#[component]
pub fn UploadsPage() -> impl IntoView {
    let (filter_errors, set_filter_errors) = create_signal(false);
    let uploads = create_resource(
        filter_errors,
        |_| async move { fetch_recent_uploads(200).await.unwrap_or_default() }
    );

    // Refresh every 2s
    create_effect(move |_| {
        let handle = set_interval_with_handle(
            move || uploads.refetch(),
            std::time::Duration::from_secs(2),
        ).ok();
        on_cleanup(move || { if let Some(h) = handle { h.clear(); } });
    });

    view! {
        <div class="uploads-page">
            <h1>"Uploads"</h1>
            <label>
                <input type="checkbox" prop:checked=filter_errors
                       on:change=move |ev| set_filter_errors(event_target_checked(&ev)) />
                " Errors only"
            </label>
            <table class="uploads-table">
                <thead>
                    <tr>
                        <th>"id"</th><th>"event"</th><th>"seq"</th>
                        <th>"size"</th><th>"attempts"</th>
                        <th>"duration"</th><th>"status"</th><th>"error"</th>
                    </tr>
                </thead>
                <tbody>
                    {move || uploads.get().map(|rows| {
                        rows.into_iter()
                            .filter(|r| !filter_errors() || r.last_error.is_some()
                                         || r.status == "failed")
                            .map(|r| view! {
                                <tr class=format!("uploads-row uploads-row--{}", r.status)>
                                    <td>{r.chunk_id}</td>
                                    <td>{r.event_identifier}</td>
                                    <td>{r.sequence_number}</td>
                                    <td>{r.size_bytes}</td>
                                    <td>{r.attempts}</td>
                                    <td>{r.duration_ms.map(|d| format!("{}ms", d)).unwrap_or_default()}</td>
                                    <td>{r.status}</td>
                                    <td>{r.last_error.unwrap_or_default()}</td>
                                </tr>
                            }).collect_view()
                    })}
                </tbody>
            </table>
        </div>
    }
}
```

- [ ] **Step 3: Register route**

In `leptos-ui/src/router.rs` (or the Routes module), add:

```rust
    <Route path="/uploads" view=UploadsPage />
```

and `use crate::pages::uploads::UploadsPage;`.

In `leptos-ui/src/pages/mod.rs`: `pub mod uploads;`.

- [ ] **Step 4: Add CSS classes**

Classes referenced: `.uploads-page`, `.uploads-table`, `.uploads-row`, `.uploads-row--sent`, `.uploads-row--pending`, `.uploads-row--retrying`, `.uploads-row--failed`. Add them to the project stylesheet with sensible colors (green/grey/yellow/red).

- [ ] **Step 5: Commit**

```bash
git add leptos-ui/src/pages/uploads.rs leptos-ui/src/pages/mod.rs \
        leptos-ui/src/router.rs leptos-ui/src/api.rs leptos-ui/style/main.scss
git commit -m "feat(ui): /uploads drill-down page (#65)"
```

---

### Task 10: Playwright E2E for Uploads UI

**Files:**
- Create: `e2e/tests/uploads.spec.ts`

- [ ] **Step 1: Write the test**

Create `e2e/tests/uploads.spec.ts`:

```typescript
import { test, expect } from '@playwright/test';

test('uploads strip and drill-down page render with zero console errors', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // Wait for operator dashboard to render the strip
  await expect(page.locator('.upload-strip')).toBeVisible({ timeout: 10_000 });
  await expect(page.locator('.upload-strip')).toContainText('c/s');

  // Click strip opens /uploads
  await page.locator('.upload-strip').click();
  await expect(page).toHaveURL(/\/uploads$/);
  await expect(page.locator('.uploads-table')).toBeVisible();

  // Errors-only filter
  await page.locator('input[type="checkbox"]').check();
  await page.locator('input[type="checkbox"]').uncheck();

  // MANDATORY: zero console errors
  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 2: Register in Playwright config**

If `e2e/playwright-frontend.config.ts` uses a glob that already includes `e2e/tests/*.spec.ts`, no change needed. Otherwise add the test file to the test paths.

- [ ] **Step 3: Commit**

```bash
git add e2e/tests/uploads.spec.ts
git commit -m "test(e2e): Playwright for upload strip + drill-down page (#65)"
```

---

### Task 11: Microbench Binary (manual, not in CI)

**Files:**
- Create: `crates/rs-endpoint/src/bin/bench_s3_upload.rs`
- Modify: `crates/rs-endpoint/Cargo.toml` (add `[[bin]]`)

- [ ] **Step 1: Create bin**

```rust
//! Microbenchmark: measure raw S3 upload throughput from this host
//! to the configured endpoint. Intended to be run MANUALLY from
//! stream.lan against Hetzner nbg1 to validate the ≥20 chunks/s
//! acceptance criterion of issue #118.
//!
//! Usage:
//!   S3_BUCKET=... S3_ENDPOINT=https://nbg1.your-objectstorage.com \
//!   S3_REGION=nbg1 S3_ACCESS_KEY=... S3_SECRET=... \
//!   cargo run --release -p rs-endpoint --bin bench_s3_upload -- --concurrency 16 --count 200

use std::time::Instant;
use rs_core::config::S3Config;
use rs_endpoint::s3::S3Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let concurrency: usize = parse_arg(&args, "--concurrency").unwrap_or(16);
    let count: usize = parse_arg(&args, "--count").unwrap_or(200);

    let cfg = S3Config {
        bucket: std::env::var("S3_BUCKET")?,
        region: std::env::var("S3_REGION")?,
        endpoint: std::env::var("S3_ENDPOINT")?,
        access_key_id: std::env::var("S3_ACCESS_KEY")?,
        secret_access_key: std::env::var("S3_SECRET")?,
    };
    let s3 = std::sync::Arc::new(S3Client::new(&cfg)?);

    // Write one 100KB random file to disk to reuse for all uploads.
    let tmp = std::env::temp_dir().join("bench_s3_upload.bin");
    let data: Vec<u8> = (0..102_400).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(&tmp, &data).await?;

    eprintln!("Starting: concurrency={concurrency} count={count}");
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(count);
    for i in 0..count {
        let s3 = s3.clone();
        let sem = sem.clone();
        let path = tmp.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire_owned().await.unwrap();
            s3.upload_chunk(&path, "bench", i as i64, 2000).await
        }));
    }
    let mut errors = 0;
    for h in handles {
        if h.await?.is_err() { errors += 1; }
    }
    let elapsed = started.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    let mbps = (count as f64 * 102_400.0 / elapsed.as_secs_f64()) / 1_000_000.0;
    println!("Uploaded {count} chunks in {:.2}s => {:.2} chunks/s ({:.2} MB/s), errors={errors}",
             elapsed.as_secs_f64(), rate, mbps);

    // Cleanup bench/ prefix
    let _ = s3.delete_event_chunks("bench").await;
    Ok(())
}

fn parse_arg<T: std::str::FromStr>(args: &[String], name: &str) -> Option<T> {
    let idx = args.iter().position(|a| a == name)?;
    args.get(idx + 1)?.parse().ok()
}
```

- [ ] **Step 2: Register the bin target**

In `crates/rs-endpoint/Cargo.toml`:

```toml
[[bin]]
name = "bench_s3_upload"
path = "src/bin/bench_s3_upload.rs"
```

Add `anyhow.workspace = true` to `[dependencies]` if not already there.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-endpoint/src/bin/bench_s3_upload.rs crates/rs-endpoint/Cargo.toml
git commit -m "test: manual bench_s3_upload binary for #118 validation"
```

---

### Task 12: Tighten CI Gate Assertions

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Find the resilience step**

```bash
grep -n "Simulated network disconnect\|pending\|cache drain" .github/workflows/ci.yml
```

- [ ] **Step 2: Add `pending <= 5` assertion after unblock+60s**

In the step that unblocks the simulated outage and waits 60 s, after the wait, query the cache endpoint:

```yaml
      - name: Strict drain after unblock
        if: always()
        run: |
          $resp = Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status
          $pending = $resp.cache.pending_chunks
          if ($pending -gt 5) {
            throw "Pending chunks ${pending} > 5 after unblock; uploader not draining fast enough"
          }
```

And for the streaming test's final chunk wait, change the existing assertion to `$pending -le 5`.

- [ ] **Step 3: Add mutation-testing exclusions (if needed)**

If `cargo mutants` flags surviving mutants in `uploader.rs` or `metrics.rs`, add targeted exclusions only for the items already covered by integration tests (not as a blanket bypass). Document each exclusion with a `# reason:` comment. Aim for NO exclusions first; add only if CI demonstrates a needed one.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: tighten pending<=5 gate after S3 unblock + stream end (#118)"
```

---

### Task 13: Push, Monitor CI, Create PR

- [ ] **Step 1: Local checks**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI to completion**

```bash
gh run list --branch dev --limit 3
# Then background:
sleep 300 && gh run view <run-id> --json status,conclusion,jobs
```

ALL jobs must be green — lint, tests, mutation, E2E (including the new `uploads.spec.ts` + the tightened resilience gate), deploy-stream-lan, deploy-verify.

- [ ] **Step 4: Stream.lan post-deploy functional verification**

Open Playwright against the deployed stream.lan dashboard (`http://10.77.9.204:8910/` or the configured IP), assert:

- `.upload-strip` visible, `c/s` > 0 if a stream is active (or 0 if idle — just assert visibility)
- Click strip navigates to `/uploads`, table renders
- Zero console errors/warnings

Take a screenshot and attach as evidence.

- [ ] **Step 5: Create PR**

```bash
gh pr create --title "fix: robust S3 chunk upload pipeline (#118, #65)" --body "$(cat <<'EOF'
## Summary
- Worker pool with row-level retry (backoff in DB, not in worker sleeps) eliminates the slot-starvation bug that caused ~0.5 chunks/s during resilience test.
- Adaptive concurrency 4..32 scales up only on zero errors + median < 500 ms.
- Per-chunk telemetry persisted to SQLite (V17 migration).
- New `/api/v1/uploads/{stats,recent}` endpoints.
- Operator dashboard inline upload strip + `/uploads` drill-down page — closes the "black box" complaint in #65.
- CI gate tightened: pending <= 5 after unblock + 60 s, and at stream end.

Closes #118
Closes #65

## Test plan
- [ ] Migration V17 unit test
- [ ] ChunkRecord + helper unit tests
- [ ] Picker + atomic claim unit tests
- [ ] Metrics ring buffer unit tests
- [ ] Adaptive controller unit tests
- [ ] uploader_integration extended (requeue on failure)
- [ ] Playwright e2e/tests/uploads.spec.ts
- [ ] E2E resilience: pending <= 5 after unblock + 60 s (5 consecutive runs)
- [ ] Streaming test: pending <= 5 at end
- [ ] Manual microbench from stream.lan -> nbg1: >= 20 chunks/s
- [ ] Post-deploy Playwright verification on stream.lan

Dashboard: http://10.77.9.204:8910/

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/NUMBER --jq '{mergeable, mergeable_state}'
```

Must be `{mergeable: true, mergeable_state: "clean"}` before handing to user.

- [ ] **Step 7: Provide green PR URL to user; WAIT for explicit merge instruction**

Per pr-merge-policy: green PR URL only. The user merges, not me.

---

### Verification (acceptance from spec)

1. **Migration V17 applied cleanly on fresh install AND upgrade from V16** — covered by migration test + CI `deploy-stream-lan` using a pre-existing DB.
2. **Unit tests pass** — picker, backoff, adaptive controller, metrics.
3. **Integration tests pass** — induced-failure requeue, permanent-failure marking.
4. **E2E uploads.spec.ts passes**, zero console errors.
5. **E2E resilience passes 5 consecutive runs**, `pending <= 5` after unblock + 60 s.
6. **E2E Streaming Test ends with pending <= 5** (was 378 in the observed regression).
7. **`GET /api/v1/uploads/stats`** returns live values during stream.
8. **Operator dashboard inline strip** visible + live-updating.
9. **`/uploads` page** renders, filters work, updates live.
10. **Microbench ≥ 20 chunks/s** from stream.lan → nbg1 (recorded in PR description, not in CI).
11. **CI green** (all jobs).
12. **Deploy to stream.lan verified via Playwright** during a live or test stream.
