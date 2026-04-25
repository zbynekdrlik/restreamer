# Cache Drift Investigation & Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Diagnose and fix the 18s/hour cache drift on multi-hour delivery streams (#135) via three-way instrumentation, live investigation, data-driven targeted fix, and live post-fix verification — all within a single PR from `dev` to `main`.

**Architecture:** Phase 1 adds three telemetry layers (producer wall-clock per chunk, consumer ffmpeg-time progress, stream.lan ↔ VPS clock-skew probe) persisted to SQLite + dashboard panel. Phase 2 runs a ≥2h live test on the real production path using MCP tooling. Phase 3 executes one or more conditional fix branches (NTP hardening, FlvStreamNormalizer timestamp rewriting, Rust-side consumer pacer) selected by Phase 2 data. Phase 4 verifies fix via a ≥1h live test with ±5s tolerance. One PR. TDD throughout.

**Tech Stack:** Rust 2024 (workspace), SQLite via sqlx, Axum for VPS `/clock` endpoint, Leptos CSR for dashboard panel, Playwright for E2E, Hetzner Cloud API, ffmpeg CLI, `win-stream-snv` MCP for live test.

**Spec:** `docs/superpowers/specs/2026-04-23-cache-drift-investigation-and-fix-design.md`

**Branch:** `dev`. Version 0.3.68 already bumped (commit 1d8b100). Spec committed (d93441d).

---

## Important constraints (read before any task)

- **Local checks only:** `cargo fmt --all --check` before every push. Do NOT run `cargo build`, `cargo test`, `cargo clippy` locally — Rust compiles produce 10-20GB of artifacts and are CI-only per airuleset.
- **One PR:** every commit in this plan lands on `dev`. Only Task 12 creates the PR from `dev` to `main`.
- **TDD:** write the failing test first, confirm it fails (by reading the test code — since we don't compile locally, "failure" is logical: the code under test doesn't exist yet), write the implementation, commit. CI validates pass.
- **File size gate:** `endpoint_task.rs` is already 954 / 1000 lines. New delivery-side logic goes in new sibling files (`progress_capture.rs`, `clock_endpoint.rs`), not inside `endpoint_task.rs`.
- **Migration numbering:** existing `MAX_SCHEMA_VERSION = 19` with `migrate_v19` already on dev. Next is V20 for this work.
- **Live-event push gate:** CI `deploy-stream-lan` blocks during active receiving events. This PR's commits should land on weekdays / non-Sunday when no live event is active. If the user happens to be testing live, prefix the commit message with `[skip-live-check]`. Otherwise no-op.
- **Test-integrity:** The `test-integrity` CI job scans for `#[ignore]`, `.skip()`, `assume!()`, empty test bodies, and assert_eq with trivial values. Never use any of those to "get past CI".

---

## File structure overview

| File | Role | Change type |
|---|---|---|
| `crates/rs-core/src/db/migrations.rs` | DB schema source-of-truth | Bump `MAX_SCHEMA_VERSION` to 20; add `migrate_v20` (adds `chunk_records.wall_clock_written_at_ms`, creates `clock_skew_samples` table, creates `ffmpeg_progress_samples` table) |
| `crates/rs-core/src/db/mod.rs` | Chunk / sample DB helpers | Add `insert_chunk_with_walltime` (wraps existing `insert_chunk`); add `insert_clock_skew_sample`, `insert_ffmpeg_progress_sample`, `list_*` helpers |
| `crates/rs-core/src/db/drift.rs` | **New.** Dedicated helpers for the three sample streams | Create |
| `crates/rs-inpoint/src/flv_chunker.rs:336` | Chunk emit point | Stamp `wall_clock_written_at_ms` on `PendingChunkWrite` + `ChunkInfo` |
| `crates/rs-inpoint/src/lib.rs` | `ChunkInfo` re-export / definition | Extend `ChunkInfo` struct |
| `crates/rs-runtime/src/orchestrator.rs:253` | Chunk → DB plumbing | Switch `insert_chunk` call to `insert_chunk_with_walltime` |
| `crates/rs-ffmpeg/src/lib.rs` | ffmpeg process mgmt | Parse `time=` from stderr; expose progress events via mpsc channel |
| `crates/rs-delivery/src/progress_capture.rs` | **New.** Subscribes to ffmpeg progress events, ships to stream.lan via existing vps_logs channel | Create |
| `crates/rs-delivery/src/clock_endpoint.rs` | **New.** GET /clock handler on VPS | Create |
| `crates/rs-delivery/src/api.rs` | VPS router | Wire `/clock` route |
| `crates/rs-delivery/src/endpoint_task.rs` | Consumer loop | Subscribe to progress events from `rs-ffmpeg`; forward to `progress_capture` |
| `crates/rs-api/src/delivery_orchestrator.rs` | Orchestrator on stream.lan | Spawn clock-skew probe task per active delivery; ingest progress events from VPS logs |
| `crates/rs-api/src/diagnostics_pacing.rs` | **New.** `GET /api/v1/diagnostics/pacing` endpoint returning the three time-series | Create |
| `crates/rs-api/src/router.rs` | stream.lan router | Wire `/api/v1/diagnostics/pacing` route |
| `leptos-ui/src/components/pacing_panel.rs` | **New.** Leptos panel that plots skew, producer rate, consumer rate | Create |
| `leptos-ui/src/pages/dashboard.rs` (or equivalent) | Dashboard page | Mount `pacing_panel` |
| `e2e/cache-drift-panel.spec.ts` | **New.** Playwright spec that navigates to dashboard, confirms panel renders with the three series | Create |
| Conditional Phase 3 loci | One or more of: `crates/rs-cloud/src/cloud_init.rs`, `crates/rs-delivery/src/flv_normalizer.rs`, `crates/rs-delivery/src/consumer_pacer.rs` (new) | Create / modify |

---

## Phase 1 — Instrumentation (Tasks 1-7)

### Task 1: Migration V20 — drift telemetry schema

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs` (bump `MAX_SCHEMA_VERSION`; add `migrate_v20`; wire in dispatch `match`)
- Test: extend existing migration test in `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write the failing test**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn migration_v20_adds_drift_telemetry_schema() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    migrations::run_migrations(&pool).await.unwrap();

    // chunk_records.wall_clock_written_at_ms added
    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('chunk_records')",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        cols.iter().any(|c| c == "wall_clock_written_at_ms"),
        "chunk_records must have wall_clock_written_at_ms column after V20"
    );

    // clock_skew_samples table exists with expected columns
    let skew_cols: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('clock_skew_samples')",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in &[
        "id", "event_id", "measured_at_ms", "local_before_ms",
        "vps_reported_ms", "local_after_ms", "skew_ms", "rtt_ms",
    ] {
        assert!(
            skew_cols.iter().any(|c| c == expected),
            "clock_skew_samples missing column {expected}; got {skew_cols:?}"
        );
    }

    // ffmpeg_progress_samples table exists
    let prog_cols: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('ffmpeg_progress_samples')",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in &[
        "id", "event_id", "endpoint_alias", "measured_at_ms",
        "ffmpeg_media_time_ms", "wall_clock_ms",
    ] {
        assert!(
            prog_cols.iter().any(|c| c == expected),
            "ffmpeg_progress_samples missing column {expected}; got {prog_cols:?}"
        );
    }

    // Schema version matches MAX
    let v: i32 = sqlx::query_scalar("SELECT MAX(version) FROM schema_version")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(v, migrations::MAX_SCHEMA_VERSION);
}
```

- [ ] **Step 2: Bump MAX_SCHEMA_VERSION and add dispatch**

In `crates/rs-core/src/db/migrations.rs`:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 20;  // was 19
```

In the dispatch `match` (around line 337, after the `19 =>` arm):

```rust
            19 => migrate_v19(&mut tx).await?,
            20 => migrate_v20(&mut tx).await?,
            _ => unreachable!("unhandled migration version {version}"),
```

- [ ] **Step 3: Add migrate_v20 implementation**

Add after `migrate_v19`:

```rust
async fn migrate_v20(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // Producer wall-clock per chunk
    add_column_if_missing(
        tx,
        "chunk_records",
        "wall_clock_written_at_ms",
        "wall_clock_written_at_ms INTEGER",
    )
    .await?;

    // Clock-skew samples (stream.lan ↔ VPS)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS clock_skew_samples (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        INTEGER NOT NULL,
            measured_at_ms  INTEGER NOT NULL,
            local_before_ms INTEGER NOT NULL,
            vps_reported_ms INTEGER NOT NULL,
            local_after_ms  INTEGER NOT NULL,
            skew_ms         INTEGER NOT NULL,
            rtt_ms          INTEGER NOT NULL
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_clock_skew_event_time
         ON clock_skew_samples(event_id, measured_at_ms)",
    )
    .execute(&mut **tx)
    .await?;

    // ffmpeg consumer-rate samples (one per stderr `time=` line, sampled)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ffmpeg_progress_samples (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id             INTEGER NOT NULL,
            endpoint_alias       TEXT    NOT NULL,
            measured_at_ms       INTEGER NOT NULL,
            ffmpeg_media_time_ms INTEGER NOT NULL,
            wall_clock_ms        INTEGER NOT NULL
        )
        "#,
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_ffmpeg_progress_event_time
         ON ffmpeg_progress_samples(event_id, measured_at_ms)",
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}
```

- [ ] **Step 4: Local format check**

```bash
cargo fmt --all --check
```

Expected: clean exit. If not, run `cargo fmt --all` and retry.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/db/migrations.rs crates/rs-core/src/db/tests.rs
git commit -m "feat(db): add V20 drift telemetry schema (#135)"
```

---

### Task 2: Producer wall-clock per chunk

**Files:**
- Modify: `crates/rs-inpoint/src/flv_chunker.rs` (PendingChunkWrite + extract_chunk)
- Modify: `crates/rs-inpoint/src/lib.rs` (ChunkInfo struct)
- Create: `crates/rs-core/src/db/drift.rs` (new helpers)
- Modify: `crates/rs-core/src/db/mod.rs` (expose `drift` module)
- Modify: `crates/rs-runtime/src/orchestrator.rs` (call `insert_chunk_with_walltime`)
- Test: `crates/rs-inpoint/src/flv_chunker_tests.rs` or inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Inline in `crates/rs-inpoint/src/flv_chunker.rs` at the existing `#[cfg(test)] mod tests` (if none, create one):

```rust
#[cfg(test)]
mod wall_clock_tests {
    use super::*;

    #[test]
    fn pending_chunk_write_carries_wall_clock_ms() {
        // Emit a synthetic chunk, assert wall_clock_written_at_ms is non-zero
        // and within 1 second of SystemTime::now() at call time.
        let before_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

        let mut inner = FlvChunkSinkInner::new_for_test(std::path::PathBuf::from("/tmp/x"));
        // Seed inner with a fake buffer and timestamps so extract_chunk emits.
        inner.buffer = vec![0x46, 0x4C, 0x56]; // "FLV"
        inner.chunk_first_ts = 0;
        inner.chunk_last_ts = 1000;
        inner.chunk_start = Some(std::time::Instant::now());

        let pending = FlvChunkSink::extract_chunk(&mut inner).expect("chunk emitted");
        let after_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

        assert!(
            pending.wall_clock_written_at_ms >= before_ms &&
            pending.wall_clock_written_at_ms <= after_ms,
            "wall_clock_written_at_ms {} outside [{before_ms}, {after_ms}]",
            pending.wall_clock_written_at_ms
        );
    }
}
```

Note: if `FlvChunkSinkInner::new_for_test` doesn't exist, add it as a `#[cfg(test)] impl` helper returning a minimal inner with `buffer: Vec::new(), chunk_dir, chunk_index: 0, chunk_start: None, chunk_first_ts: 0, chunk_last_ts: 0`.

- [ ] **Step 2: Extend PendingChunkWrite**

In `crates/rs-inpoint/src/flv_chunker.rs`, around line 61:

```rust
struct PendingChunkWrite {
    data: Vec<u8>,
    path: std::path::PathBuf,
    size: usize,
    md5: String,
    index: u64,
    duration_ms: u64,
    wall_clock_written_at_ms: i64,   // NEW
}
```

In `extract_chunk` (around line 345), set the field:

```rust
Some(PendingChunkWrite {
    data,
    path,
    size,
    md5,
    index,
    duration_ms,
    wall_clock_written_at_ms: std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64,
})
```

- [ ] **Step 3: Extend ChunkInfo with wall_clock_written_at_ms**

In `crates/rs-inpoint/src/flv_chunker.rs` at `ChunkInfo` (around line 17):

```rust
pub struct ChunkInfo {
    pub path: std::path::PathBuf,
    pub size: usize,
    pub md5: String,
    pub index: u64,
    pub duration_ms: u64,
    pub wall_clock_written_at_ms: i64,  // NEW
}
```

And in `do_write_and_notify` (around line 414) set the field when constructing:

```rust
let chunk_info = ChunkInfo {
    path: pending.path,
    size: pending.size,
    md5: pending.md5,
    index: pending.index,
    duration_ms: pending.duration_ms,
    wall_clock_written_at_ms: pending.wall_clock_written_at_ms,
};
```

- [ ] **Step 4: Add insert_chunk_with_walltime helper in rs-core**

Create new `crates/rs-core/src/db/drift.rs`:

```rust
use sqlx::{Row, SqlitePool};
use super::Result;

/// Insert a chunk record and stamp producer wall-clock time.
/// Wraps the existing `insert_chunk` and updates the new column.
pub async fn insert_chunk_with_walltime(
    pool: &SqlitePool,
    streaming_event_id: i64,
    chunk_file_path: &str,
    data_size: i64,
    md5: &str,
    duration_ms: i64,
    wall_clock_written_at_ms: i64,
) -> Result<i64> {
    let id = super::insert_chunk(
        pool,
        streaming_event_id,
        chunk_file_path,
        data_size,
        md5,
        duration_ms,
    )
    .await?;
    sqlx::query("UPDATE chunk_records SET wall_clock_written_at_ms = ?1 WHERE id = ?2")
        .bind(wall_clock_written_at_ms)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(id)
}

/// Insert one clock-skew sample.
pub async fn insert_clock_skew_sample(
    pool: &SqlitePool,
    event_id: i64,
    measured_at_ms: i64,
    local_before_ms: i64,
    vps_reported_ms: i64,
    local_after_ms: i64,
    skew_ms: i64,
    rtt_ms: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO clock_skew_samples
         (event_id, measured_at_ms, local_before_ms, vps_reported_ms,
          local_after_ms, skew_ms, rtt_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(event_id)
    .bind(measured_at_ms)
    .bind(local_before_ms)
    .bind(vps_reported_ms)
    .bind(local_after_ms)
    .bind(skew_ms)
    .bind(rtt_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert one ffmpeg progress sample.
pub async fn insert_ffmpeg_progress_sample(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_alias: &str,
    measured_at_ms: i64,
    ffmpeg_media_time_ms: i64,
    wall_clock_ms: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ffmpeg_progress_samples
         (event_id, endpoint_alias, measured_at_ms,
          ffmpeg_media_time_ms, wall_clock_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(event_id)
    .bind(endpoint_alias)
    .bind(measured_at_ms)
    .bind(ffmpeg_media_time_ms)
    .bind(wall_clock_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Time-series row used by dashboard API (see rs-api diagnostics_pacing).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DriftSample {
    pub t_ms: i64,
    pub value: f64,
}

pub async fn list_chunk_producer_rate(
    pool: &SqlitePool, event_id: i64, since_ms: i64,
) -> Result<Vec<DriftSample>> {
    // Producer rate = Δts / Δwall_clock between consecutive chunks.
    let rows = sqlx::query(
        "SELECT duration_ms AS d_ts,
                wall_clock_written_at_ms AS wc
         FROM chunk_records
         WHERE streaming_event_id = ?1
           AND wall_clock_written_at_ms IS NOT NULL
           AND wall_clock_written_at_ms >= ?2
         ORDER BY id ASC",
    )
    .bind(event_id)
    .bind(since_ms)
    .fetch_all(pool).await?;

    // Compute pairwise ratios, keyed by the latter chunk's wall_clock.
    let mut out = Vec::with_capacity(rows.len().saturating_sub(1));
    let mut prev_wc: Option<i64> = None;
    for r in &rows {
        let wc: i64 = r.get("wc");
        let d_ts: i64 = r.get("d_ts");
        if let Some(p) = prev_wc {
            let d_wc = wc - p;
            if d_wc > 0 && d_ts >= 0 {
                out.push(DriftSample {
                    t_ms: wc,
                    value: (d_ts as f64) / (d_wc as f64),
                });
            }
        }
        prev_wc = Some(wc);
    }
    Ok(out)
}

pub async fn list_clock_skew(
    pool: &SqlitePool, event_id: i64, since_ms: i64,
) -> Result<Vec<DriftSample>> {
    let rows = sqlx::query(
        "SELECT measured_at_ms, skew_ms FROM clock_skew_samples
         WHERE event_id = ?1 AND measured_at_ms >= ?2
         ORDER BY measured_at_ms ASC",
    )
    .bind(event_id).bind(since_ms)
    .fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| DriftSample {
        t_ms: r.get::<i64, _>("measured_at_ms"),
        value: r.get::<i64, _>("skew_ms") as f64,
    }).collect())
}

pub async fn list_ffmpeg_consumer_rate(
    pool: &SqlitePool, event_id: i64, endpoint_alias: &str, since_ms: i64,
) -> Result<Vec<DriftSample>> {
    // Consumer rate = Δffmpeg_media_time / Δwall_clock between consecutive samples.
    let rows = sqlx::query(
        "SELECT measured_at_ms, ffmpeg_media_time_ms, wall_clock_ms
         FROM ffmpeg_progress_samples
         WHERE event_id = ?1 AND endpoint_alias = ?2 AND measured_at_ms >= ?3
         ORDER BY measured_at_ms ASC",
    )
    .bind(event_id).bind(endpoint_alias).bind(since_ms)
    .fetch_all(pool).await?;

    let mut out = Vec::with_capacity(rows.len().saturating_sub(1));
    let mut prev: Option<(i64, i64)> = None;
    for r in &rows {
        let m_at: i64 = r.get("measured_at_ms");
        let ft: i64 = r.get("ffmpeg_media_time_ms");
        let wc: i64 = r.get("wall_clock_ms");
        if let Some((p_ft, p_wc)) = prev {
            let d_ft = ft - p_ft;
            let d_wc = wc - p_wc;
            if d_wc > 0 && d_ft >= 0 {
                out.push(DriftSample {
                    t_ms: m_at,
                    value: (d_ft as f64) / (d_wc as f64),
                });
            }
        }
        prev = Some((ft, wc));
    }
    Ok(out)
}
```

In `crates/rs-core/src/db/mod.rs` at the top, add:

```rust
pub mod drift;
```

- [ ] **Step 5: Switch orchestrator to insert_chunk_with_walltime**

In `crates/rs-runtime/src/orchestrator.rs` at line 253, replace the `db::insert_chunk(...)` call with:

```rust
match db::drift::insert_chunk_with_walltime(
    &chunk_pool,
    event.id,
    &path_str,
    chunk_info.size as i64,
    &chunk_info.md5,
    chunk_info.duration_ms as i64,
    chunk_info.wall_clock_written_at_ms,
)
.await
{
```

- [ ] **Step 6: Add unit test for drift helpers**

Append to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn insert_chunk_with_walltime_persists_wall_clock() {
    let pool = setup_db().await;
    let event = db::create_streaming_event(&pool, "uuid-t2", "desc").await.unwrap();
    let id = db::drift::insert_chunk_with_walltime(
        &pool, event.id, "/tmp/c.bin", 4096, "md5x", 1000, 1_700_000_000_000,
    ).await.unwrap();
    let wc: i64 = sqlx::query_scalar(
        "SELECT wall_clock_written_at_ms FROM chunk_records WHERE id = ?1"
    ).bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!(wc, 1_700_000_000_000);
}

#[tokio::test]
async fn list_chunk_producer_rate_computes_ratio() {
    let pool = setup_db().await;
    let event = db::create_streaming_event(&pool, "uuid-t2b", "desc").await.unwrap();
    // Two chunks, each 1000ms of content, written 1010ms wall-clock apart.
    db::drift::insert_chunk_with_walltime(
        &pool, event.id, "/tmp/a", 1, "a", 1000, 1_000_000).await.unwrap();
    db::drift::insert_chunk_with_walltime(
        &pool, event.id, "/tmp/b", 1, "b", 1000, 1_001_010).await.unwrap();
    let series = db::drift::list_chunk_producer_rate(&pool, event.id, 0).await.unwrap();
    assert_eq!(series.len(), 1);
    // 1000ms ts / 1010ms wall = ~0.990 ratio (producer "slow" in ts-land)
    assert!((series[0].value - 0.990).abs() < 0.001,
        "ratio {} not near 0.990", series[0].value);
}
```

- [ ] **Step 7: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-inpoint/src/flv_chunker.rs crates/rs-core/src/db/drift.rs crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs crates/rs-runtime/src/orchestrator.rs
git commit -m "feat(drift): producer wall-clock per chunk + drift helpers (#135)"
```

---

### Task 3: ffmpeg stderr progress parser

**Files:**
- Modify: `crates/rs-ffmpeg/src/lib.rs` (add parser + mpsc channel for progress events)
- Test: inline `#[cfg(test)] mod progress_tests`

- [ ] **Step 1: Write the failing test**

Append to the existing test module in `crates/rs-ffmpeg/src/lib.rs`:

```rust
#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn parse_ffmpeg_time_simple() {
        // Typical ffmpeg progress line:
        // "frame=  150 fps= 30 q=28.0 size=  1024kB time=00:00:05.00 bitrate=..."
        let ms = parse_ffmpeg_time_ms(
            "frame=  150 fps= 30 q=28.0 size=  1024kB time=00:00:05.00 bitrate=1024kbits/s"
        );
        assert_eq!(ms, Some(5_000));
    }

    #[test]
    fn parse_ffmpeg_time_hhmmss_fractional() {
        let ms = parse_ffmpeg_time_ms(
            "time=01:23:45.67"
        );
        // 1h*3600 + 23m*60 + 45 = 5025s; + 0.67 = 5025.67s = 5_025_670 ms
        assert_eq!(ms, Some(5_025_670));
    }

    #[test]
    fn parse_ffmpeg_time_none_when_missing() {
        let ms = parse_ffmpeg_time_ms("frame= 100 fps=30 bitrate=N/A");
        assert_eq!(ms, None);
    }

    #[test]
    fn parse_ffmpeg_time_tolerates_dot_comma() {
        // Some locales emit "time=00:00:05,00"
        let ms = parse_ffmpeg_time_ms("time=00:00:05,00");
        assert_eq!(ms, Some(5_000));
    }
}
```

- [ ] **Step 2: Implement the parser**

Add to `crates/rs-ffmpeg/src/lib.rs` (near the top or under a `mod progress { ... }` if preferred):

```rust
/// Parse ffmpeg stderr progress line and extract `time=HH:MM:SS.xx` in ms.
/// Returns None if the line has no `time=` field or the value is unparseable.
pub fn parse_ffmpeg_time_ms(line: &str) -> Option<i64> {
    let idx = line.find("time=")?;
    let rest = &line[idx + 5..];
    let field = rest.split_whitespace().next()?;   // "HH:MM:SS.xx"
    let field = field.replace(',', ".");
    let mut it = field.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let s: f64 = it.next()?.parse().ok()?;
    Some(h * 3_600_000 + m * 60_000 + (s * 1000.0) as i64)
}
```

- [ ] **Step 3: Expose progress events via mpsc channel**

Extend `FfmpegProcess` to own an optional `tokio::sync::mpsc::UnboundedSender<FfmpegProgress>`. Add:

```rust
#[derive(Debug, Clone)]
pub struct FfmpegProgress {
    pub media_time_ms: i64,
    pub wall_clock_ms: i64,
}
```

In `FfmpegProcess::spawn`, change the stderr reader task to additionally parse each line and send progress events on the channel. Keep the ring buffer as-is.

Add a new method:

```rust
pub fn with_progress_tx(mut self, tx: tokio::sync::mpsc::UnboundedSender<FfmpegProgress>) -> Self {
    self.progress_tx = Some(tx);
    self
}
```

Actually: since the stderr task is spawned inside `spawn()`, the builder-after-spawn pattern won't work. Instead, add a second `spawn_with_progress` constructor:

```rust
impl FfmpegProcess {
    pub fn spawn_with_progress(
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
        progress_tx: Option<tokio::sync::mpsc::UnboundedSender<FfmpegProgress>>,
    ) -> Result<Self, FfmpegError> {
        // ... same as spawn() up to where stderr task is spawned ...
        tokio::spawn(async move {
            if let Some(stderr) = stderr {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // Ring buffer (existing behavior)
                    if let Ok(mut buf) = stderr_lines_clone.lock() {
                        if buf.len() >= STDERR_BUFFER_SIZE { buf.pop_front(); }
                        buf.push_back(line.clone());
                    }
                    // Progress events (new)
                    if let Some(tx) = &progress_tx {
                        if let Some(media_time_ms) = parse_ffmpeg_time_ms(&line) {
                            let wall_clock_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64;
                            let _ = tx.send(FfmpegProgress { media_time_ms, wall_clock_ms });
                        }
                    }
                }
            }
        });
        // ... same as spawn() for returning Ok(Self { ... }) ...
    }

    /// Backwards-compatible: same as `spawn_with_progress(_, _, _, None)`.
    pub fn spawn(
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
    ) -> Result<Self, FfmpegError> {
        Self::spawn_with_progress(service_type, stream_key, alias, None)
    }
}
```

(Factor the shared body to a private helper so we don't have two copies of the long body.)

- [ ] **Step 4: Add an integration test for the channel**

Append:

```rust
#[tokio::test]
async fn spawn_with_progress_emits_progress_events() {
    // Use TEST_FILE output to an ephemeral path; feed a minimal FLV stream
    // that produces enough output for ffmpeg to emit at least one time= line.
    // (This test runs only when ffmpeg is available; mark with
    // an environment check rather than #[ignore].)
    if std::process::Command::new("ffmpeg").arg("-version").output().is_err() {
        panic!("ffmpeg binary not available in test env — add it to CI image");
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut proc = FfmpegProcess::spawn_with_progress(
        ServiceType::TestFile, "", "prog_test", Some(tx),
    ).expect("spawn");
    // Feed ~60 frames worth of synthetic FLV so ffmpeg has time to emit progress.
    let flv = include_bytes!("../tests/fixtures/min_60_frames.flv");
    proc.write(flv).await.expect("write");
    // Close stdin implicitly — drop proc at end; ffmpeg will emit final time=.
    let event = tokio::time::timeout(
        std::time::Duration::from_secs(10), rx.recv()
    ).await.expect("progress event within 10s").expect("some");
    assert!(event.media_time_ms >= 0);
}
```

**Note:** `tests/fixtures/min_60_frames.flv` is a small pre-generated FLV file containing ~60 frames of `testsrc`. Generate once with:

```bash
ffmpeg -f lavfi -i "testsrc=duration=2:size=320x240:rate=30" -f flv -c:v libx264 -preset ultrafast -y crates/rs-ffmpeg/tests/fixtures/min_60_frames.flv
```

Commit the fixture along with the test. Airuleset prohibits `#[ignore]` and mocks of internal code — this test uses the real ffmpeg binary and real FLV bytes.

- [ ] **Step 5: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-ffmpeg/src/lib.rs crates/rs-ffmpeg/tests/fixtures/min_60_frames.flv
git commit -m "feat(ffmpeg): parse stderr time= and emit FfmpegProgress events (#135)"
```

---

### Task 4: VPS /clock endpoint

**Files:**
- Create: `crates/rs-delivery/src/clock_endpoint.rs`
- Modify: `crates/rs-delivery/src/api.rs` (wire route)
- Modify: `crates/rs-delivery/src/lib.rs` (expose module)
- Test: inline `#[cfg(test)] mod clock_tests` in the new file

- [ ] **Step 1: Write the failing test**

Create `crates/rs-delivery/src/clock_endpoint.rs`:

```rust
//! GET /clock — returns the VPS wall-clock time for skew probing.

use axum::response::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ClockResponse {
    pub vps_ms: i64,
}

pub async fn get_clock() -> Json<ClockResponse> {
    let vps_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    Json(ClockResponse { vps_ms })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};

    #[tokio::test]
    async fn clock_endpoint_returns_current_wall_clock_ms() {
        let app = Router::new().route("/clock", get(get_clock));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        let body: ClockResponse = reqwest::get(&format!("http://{addr}/clock"))
            .await.unwrap().json().await.unwrap();
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

        assert!(body.vps_ms >= before && body.vps_ms <= after,
                "vps_ms {} outside [{before}, {after}]", body.vps_ms);
    }
}
```

- [ ] **Step 2: Wire route in api.rs**

In `crates/rs-delivery/src/api.rs`, find the router builder and add:

```rust
use crate::clock_endpoint::get_clock;
// ... existing routes ...
    .route("/clock", axum::routing::get(get_clock))
```

In `crates/rs-delivery/src/lib.rs`, add:

```rust
pub mod clock_endpoint;
```

- [ ] **Step 3: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-delivery/src/clock_endpoint.rs crates/rs-delivery/src/api.rs crates/rs-delivery/src/lib.rs
git commit -m "feat(delivery): GET /clock endpoint for skew probe (#135)"
```

---

### Task 5: Clock-skew probe on stream.lan

**Files:**
- Modify: `crates/rs-api/src/delivery_orchestrator.rs` (spawn probe task per active delivery)
- Test: inline `#[cfg(test)] mod skew_probe_tests`

- [ ] **Step 1: Write the failing test**

Add to `crates/rs-api/src/delivery_orchestrator.rs` (or create `clock_skew_probe.rs` sibling — preferred if orchestrator is large):

```rust
#[cfg(test)]
mod skew_probe_tests {
    use super::*;

    #[tokio::test]
    async fn skew_probe_computes_rtt_compensated_skew() {
        // Mock VPS /clock that returns a fixed time.
        let vps_reported_ms: i64 = 1_700_000_500_000;
        let app = axum::Router::new().route(
            "/clock",
            axum::routing::get(move || async move {
                axum::Json(serde_json::json!({ "vps_ms": vps_reported_ms }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

        let sample = probe_clock_skew(&format!("http://{addr}")).await.unwrap();
        // skew = vps - (local_before + local_after)/2
        let midpoint = (sample.local_before_ms + sample.local_after_ms) / 2;
        assert_eq!(sample.skew_ms, vps_reported_ms - midpoint);
        assert!(sample.rtt_ms >= 0);
    }
}
```

- [ ] **Step 2: Implement probe_clock_skew + background task**

Create a new module `crates/rs-api/src/clock_skew_probe.rs`:

```rust
//! Per-delivery clock-skew probe: periodically calls VPS /clock and
//! persists RTT-compensated skew samples to clock_skew_samples.

use rs_core::db;
use sqlx::SqlitePool;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClockSkewSample {
    pub measured_at_ms: i64,
    pub local_before_ms: i64,
    pub vps_reported_ms: i64,
    pub local_after_ms: i64,
    pub skew_ms: i64,
    pub rtt_ms: i64,
}

/// Perform a single clock-skew probe against the VPS.
pub async fn probe_clock_skew(vps_base_url: &str) -> Result<ClockSkewSample, reqwest::Error> {
    let now_ms = || std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
    let local_before_ms = now_ms();
    let resp: serde_json::Value = reqwest::Client::new()
        .get(format!("{vps_base_url}/clock"))
        .timeout(Duration::from_secs(5))
        .send().await?
        .json().await?;
    let local_after_ms = now_ms();
    let vps_reported_ms = resp.get("vps_ms").and_then(|v| v.as_i64()).unwrap_or(0);
    let midpoint = (local_before_ms + local_after_ms) / 2;
    Ok(ClockSkewSample {
        measured_at_ms: local_after_ms,
        local_before_ms,
        vps_reported_ms,
        local_after_ms,
        skew_ms: vps_reported_ms - midpoint,
        rtt_ms: local_after_ms - local_before_ms,
    })
}

/// Background task: probe every 30s for the given event, persist.
pub fn spawn_skew_probe(
    pool: SqlitePool,
    event_id: i64,
    vps_base_url: String,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    match probe_clock_skew(&vps_base_url).await {
                        Ok(s) => {
                            let _ = db::drift::insert_clock_skew_sample(
                                &pool, event_id,
                                s.measured_at_ms,
                                s.local_before_ms,
                                s.vps_reported_ms,
                                s.local_after_ms,
                                s.skew_ms,
                                s.rtt_ms,
                            ).await;
                        }
                        Err(e) => tracing::warn!("clock skew probe failed: {e}"),
                    }
                }
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() { break; }
                }
            }
        }
    });
}
```

In `crates/rs-api/src/lib.rs`, add `pub mod clock_skew_probe;`.

In `delivery_orchestrator.rs`, where a delivery becomes active (find the path that transitions an `event_id` to delivering and has the VPS `base_url` on hand), call:

```rust
use crate::clock_skew_probe::spawn_skew_probe;
// (stop_tx/stop_rx pair already exists alongside other per-delivery tasks — reuse it)
spawn_skew_probe(pool.clone(), event_id, vps_base_url.clone(), stop_rx.clone());
```

- [ ] **Step 3: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/clock_skew_probe.rs crates/rs-api/src/lib.rs crates/rs-api/src/delivery_orchestrator.rs
git commit -m "feat(drift): clock-skew probe stream.lan <-> VPS every 30s (#135)"
```

---

### Task 6: Progress capture on VPS → ship to stream.lan

**Files:**
- Create: `crates/rs-delivery/src/progress_capture.rs`
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (wire the progress_tx into ffmpeg spawn; forward to capture)
- Modify: `crates/rs-delivery/src/lib.rs` (expose module)
- Modify: `crates/rs-api/src/delivery_orchestrator.rs` (parse incoming progress events from VPS logs and persist)

**Context:** VPS logs already flow to stream.lan via the existing infrastructure from #129 (see `crates/rs-api/src/delivery.rs` and `crates/rs-delivery/src/` — the VPS appends structured JSON lines that rs-api ingests). We reuse that channel: `progress_capture` writes a well-known prefixed JSON line; `delivery_orchestrator` on stream.lan pattern-matches that prefix and persists to `ffmpeg_progress_samples`.

- [ ] **Step 1: Write the failing test**

Create `crates/rs-delivery/src/progress_capture.rs`:

```rust
//! Subscribes to ffmpeg progress events and writes them as JSON lines
//! to stderr prefixed with "DRIFT_PROGRESS: " so the stream.lan log
//! ingester can pick them up.

use rs_ffmpeg::FfmpegProgress;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ProgressLine<'a> {
    pub endpoint_alias: &'a str,
    pub media_time_ms: i64,
    pub wall_clock_ms: i64,
}

pub fn format_progress_line(alias: &str, p: &FfmpegProgress) -> String {
    let line = ProgressLine {
        endpoint_alias: alias,
        media_time_ms: p.media_time_ms,
        wall_clock_ms: p.wall_clock_ms,
    };
    format!("DRIFT_PROGRESS: {}", serde_json::to_string(&line).unwrap())
}

pub fn spawn_progress_capture(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<FfmpegProgress>,
    alias: String,
) {
    tokio::spawn(async move {
        while let Some(p) = rx.recv().await {
            eprintln!("{}", format_progress_line(&alias, &p));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_progress_line_is_prefixed_json() {
        let p = FfmpegProgress { media_time_ms: 12345, wall_clock_ms: 1_700_000_000_000 };
        let s = format_progress_line("yt_abc", &p);
        assert!(s.starts_with("DRIFT_PROGRESS: "));
        let json = &s["DRIFT_PROGRESS: ".len()..];
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(v["endpoint_alias"], "yt_abc");
        assert_eq!(v["media_time_ms"], 12345);
        assert_eq!(v["wall_clock_ms"], 1_700_000_000_000_i64);
    }
}
```

- [ ] **Step 2: Wire progress_tx into endpoint_task**

In `crates/rs-delivery/src/endpoint_task.rs`, where `FfmpegProcess::spawn(...)` is called (search for `FfmpegProcess::spawn`), replace with `spawn_with_progress`:

```rust
let (prog_tx, prog_rx) = tokio::sync::mpsc::unbounded_channel();
let mut ffmpeg = FfmpegProcess::spawn_with_progress(
    service_type, &stream_key, &alias, Some(prog_tx),
)?;
crate::progress_capture::spawn_progress_capture(prog_rx, alias.clone());
```

In `crates/rs-delivery/src/lib.rs`, add:

```rust
pub mod progress_capture;
```

- [ ] **Step 3: Ingest DRIFT_PROGRESS lines on stream.lan**

In `crates/rs-api/src/delivery_orchestrator.rs`, find the path that reads VPS log lines (the existing ingester from #129). Extend the line handler:

```rust
const DRIFT_PREFIX: &str = "DRIFT_PROGRESS: ";
if let Some(rest) = line.strip_prefix(DRIFT_PREFIX) {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(rest) {
        let alias = v.get("endpoint_alias").and_then(|x| x.as_str()).unwrap_or("");
        let media_time_ms = v.get("media_time_ms").and_then(|x| x.as_i64()).unwrap_or(0);
        let wall_clock_ms = v.get("wall_clock_ms").and_then(|x| x.as_i64()).unwrap_or(0);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let _ = rs_core::db::drift::insert_ffmpeg_progress_sample(
            &pool, event_id, alias, now_ms, media_time_ms, wall_clock_ms,
        ).await;
    }
    continue;  // don't also log this as a normal VPS log line
}
```

Exact location: search for the existing log-line match arms (look for `strip_prefix` or a `match` over line prefixes).

- [ ] **Step 4: Integration test for the ingester**

Append to `crates/rs-api/src/delivery_orchestrator.rs` or a sibling test file:

```rust
#[cfg(test)]
mod progress_ingest_tests {
    use super::*;
    use rs_core::db;

    #[tokio::test]
    async fn drift_progress_line_is_persisted() {
        let pool = crate::test_helpers::setup_test_db().await;
        let event = db::create_streaming_event(&pool, "uuid-pi", "desc").await.unwrap();
        let line = r#"DRIFT_PROGRESS: {"endpoint_alias":"yt1","media_time_ms":5000,"wall_clock_ms":1700000000000}"#;
        handle_vps_log_line(&pool, event.id, line).await;
        let samples = db::drift::list_ffmpeg_consumer_rate(&pool, event.id, "yt1", 0).await.unwrap();
        // One sample means rate list is empty (needs >= 2). Assert via direct query.
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT ffmpeg_media_time_ms, wall_clock_ms FROM ffmpeg_progress_samples WHERE event_id = ?1"
        ).bind(event.id).fetch_all(&pool).await.unwrap();
        assert_eq!(rows, vec![(5000, 1_700_000_000_000)]);
        let _ = samples; // silence unused warning
    }
}
```

(Extract the ingest body into a testable `handle_vps_log_line(&pool, event_id, &line)` function if it isn't already one.)

- [ ] **Step 5: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-delivery/src/progress_capture.rs crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/lib.rs crates/rs-api/src/delivery_orchestrator.rs
git commit -m "feat(drift): capture ffmpeg progress on VPS + persist on stream.lan (#135)"
```

---

### Task 7: Diagnostics API + Leptos panel + Playwright spec

**Files:**
- Create: `crates/rs-api/src/diagnostics_pacing.rs`
- Modify: `crates/rs-api/src/router.rs`
- Create: `leptos-ui/src/components/pacing_panel.rs`
- Modify: `leptos-ui/src/components/mod.rs` (expose)
- Modify: dashboard page (mount panel)
- Create: `e2e/cache-drift-panel.spec.ts`

- [ ] **Step 1: Write the failing API test**

Create `crates/rs-api/src/diagnostics_pacing.rs`:

```rust
use axum::{extract::{Query, State}, Json};
use rs_core::db::drift::DriftSample;
use serde::{Deserialize, Serialize};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct PacingQuery {
    pub event_id: i64,
    pub since_ms: Option<i64>,
    pub endpoint_alias: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PacingResponse {
    pub producer_rate: Vec<DriftSample>,
    pub consumer_rate: Vec<DriftSample>,
    pub clock_skew: Vec<DriftSample>,
}

pub async fn get_pacing(
    State(state): State<AppState>,
    Query(q): Query<PacingQuery>,
) -> Result<Json<PacingResponse>, (axum::http::StatusCode, String)> {
    let since_ms = q.since_ms.unwrap_or(0);
    let endpoint_alias = q.endpoint_alias.as_deref().unwrap_or("");
    let pool = state.pool.as_ref();
    let producer_rate = rs_core::db::drift::list_chunk_producer_rate(pool, q.event_id, since_ms)
        .await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let consumer_rate = if endpoint_alias.is_empty() {
        Vec::new()
    } else {
        rs_core::db::drift::list_ffmpeg_consumer_rate(pool, q.event_id, endpoint_alias, since_ms)
            .await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };
    let clock_skew = rs_core::db::drift::list_clock_skew(pool, q.event_id, since_ms)
        .await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(PacingResponse { producer_rate, consumer_rate, clock_skew }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::db;

    #[tokio::test]
    async fn pacing_endpoint_returns_populated_series_after_inserts() {
        let pool = crate::test_helpers::setup_test_db().await;
        let event = db::create_streaming_event(&pool, "uuid-p7", "desc").await.unwrap();
        // Seed two chunks so producer_rate has 1 point.
        db::drift::insert_chunk_with_walltime(
            &pool, event.id, "/tmp/a", 1, "a", 1000, 1_000_000).await.unwrap();
        db::drift::insert_chunk_with_walltime(
            &pool, event.id, "/tmp/b", 1, "b", 1000, 1_001_000).await.unwrap();
        // Seed one skew sample.
        db::drift::insert_clock_skew_sample(
            &pool, event.id, 2_000_000, 1_999_900, 2_000_050, 2_000_100, 50, 200).await.unwrap();
        // Call the handler directly.
        let state = AppState::for_test(pool.clone()).await;
        let resp = get_pacing(
            axum::extract::State(state),
            axum::extract::Query(PacingQuery {
                event_id: event.id, since_ms: Some(0), endpoint_alias: None,
            }),
        ).await.unwrap();
        assert_eq!(resp.0.producer_rate.len(), 1);
        assert_eq!(resp.0.clock_skew.len(), 1);
        assert_eq!(resp.0.consumer_rate.len(), 0);
    }
}
```

- [ ] **Step 2: Wire route**

In `crates/rs-api/src/lib.rs`, add `pub mod diagnostics_pacing;`. In `crates/rs-api/src/router.rs` (the router builder), add:

```rust
.route("/api/v1/diagnostics/pacing", axum::routing::get(diagnostics_pacing::get_pacing))
```

- [ ] **Step 3: Leptos panel component**

Create `leptos-ui/src/components/pacing_panel.rs`:

```rust
use leptos::prelude::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DriftSample {
    pub t_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PacingResponse {
    pub producer_rate: Vec<DriftSample>,
    pub consumer_rate: Vec<DriftSample>,
    pub clock_skew: Vec<DriftSample>,
}

#[component]
pub fn PacingPanel(event_id: i64) -> impl IntoView {
    let data = Resource::new(
        move || event_id,
        |id| async move {
            let url = format!("/api/v1/diagnostics/pacing?event_id={id}");
            reqwasm::http::Request::get(&url)
                .send().await.ok()?
                .json::<PacingResponse>().await.ok()
        },
    );
    view! {
        <div class="pacing-panel" data-testid="pacing-panel">
            <h3>"Pacing diagnostics"</h3>
            <Suspense fallback=|| view!{ <p>"Loading..."</p> }>
                {move || data.get().flatten().map(|r| view! {
                    <div class="pacing-series" data-testid="producer-rate">
                        <h4>"Producer rate (ts/wall)"</h4>
                        <p>{format!("{} samples", r.producer_rate.len())}</p>
                    </div>
                    <div class="pacing-series" data-testid="consumer-rate">
                        <h4>"Consumer rate (ffmpeg_time/wall)"</h4>
                        <p>{format!("{} samples", r.consumer_rate.len())}</p>
                    </div>
                    <div class="pacing-series" data-testid="clock-skew">
                        <h4>"Clock skew (ms)"</h4>
                        <p>{format!("{} samples", r.clock_skew.len())}</p>
                    </div>
                })}
            </Suspense>
        </div>
    }
}
```

In `leptos-ui/src/components/mod.rs` (or wherever components are re-exported):

```rust
pub mod pacing_panel;
pub use pacing_panel::PacingPanel;
```

Mount it on the dashboard page where other panels live (search for an existing panel component in the dashboard to find the insertion point):

```rust
<PacingPanel event_id=current_event_id.get() />
```

- [ ] **Step 4: Playwright spec**

Create `e2e/cache-drift-panel.spec.ts`:

```typescript
import { test, expect } from '@playwright/test';

test('pacing panel renders three series sections with clean console', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // Panel must exist even before any data; it renders "0 samples" on each series.
  await expect(page.getByTestId('pacing-panel')).toBeVisible();
  await expect(page.getByTestId('producer-rate')).toBeVisible();
  await expect(page.getByTestId('consumer-rate')).toBeVisible();
  await expect(page.getByTestId('clock-skew')).toBeVisible();

  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 5: Local format check & commit**

```bash
cargo fmt --all --check
git add crates/rs-api/src/diagnostics_pacing.rs crates/rs-api/src/lib.rs crates/rs-api/src/router.rs leptos-ui/src/components/pacing_panel.rs leptos-ui/src/components/mod.rs leptos-ui/src/pages/dashboard.rs e2e/cache-drift-panel.spec.ts
git commit -m "feat(ui): pacing diagnostics panel + /api/v1/diagnostics/pacing (#135)"
```

(Adjust the dashboard page path to the actual dashboard file — grep for the existing panel component names to find it.)

- [ ] **Step 6: Push Phase 1 and monitor CI**

```bash
git push origin dev
gh run list --limit 3
```

In the background, wait for CI:

```bash
RUN_ID=$(gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId')
# Monitor non-blockingly
```

Use `Bash(run_in_background: true, command: "sleep 300 && gh run view $RUN_ID --json status,conclusion,jobs")` per airuleset ci-monitoring. If any job fails, investigate with `gh run view $RUN_ID --log-failed`, fix in ONE commit, repush, monitor. Do not proceed to Phase 2 until all jobs (including `deploy-stream-lan`) are green.

---

## Phase 2 — Live investigation (Tasks 8-9)

### Task 8: Run ≥2h live streaming test on stream.lan ↔ Hetzner VPS

This task is **performed by the agent** using the `win-stream-snv` MCP tooling + Hetzner orchestration exposed by Restreamer's own API. It is NOT a user handoff. The agent may do other work while the test runs.

- [ ] **Step 1: Confirm Phase 1 deployed to stream.lan**

```bash
# After CI green on dev, check the app version live
# (Use the MCP Shell tool for this — represented as a PowerShell command here)
```

Via `mcp__win-stream-snv__Shell`:

```powershell
$status = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status" -TimeoutSec 10
$status.version
```

Expected: `0.3.68`. If not, the `deploy-stream-lan` job didn't run or didn't take — re-check `gh run view` for that job's output.

- [ ] **Step 2: Start OBS and activate an event**

Via `mcp__win-stream-snv__App` → launch OBS using the existing scheduled-task name (see `feedback_obs_stream_lan` memory — `RestreamerGUI` is the restreamer task; OBS has its own scheduled task configured separately; launch it).

Via `mcp__win-stream-snv__Shell`:

```powershell
# List events, pick one configured for E2E (or create a throwaway one via API)
Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -TimeoutSec 10

# Activate the chosen event (replace {id})
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8910/api/v1/events/{id}/activate" -TimeoutSec 10
```

Start OBS streaming (use the existing Stream_Obs profile — 4K@30.30).

- [ ] **Step 3: Start delivery**

Via `mcp__win-stream-snv__Shell`:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8910/api/v1/delivery/start" `
  -ContentType "application/json" `
  -Body '{"event_id":{id}}' `
  -TimeoutSec 30
```

Confirm VPS provisions and rs-delivery is running. Watch the dashboard at `http://10.77.9.204:8910/` via Playwright for visual confirmation.

- [ ] **Step 4: Let it run ≥2 hours**

While the test runs, continue work on Phase 3 skeleton (write conditional fix branches behind `if false` gates so they compile on CI but don't change behavior — actual activation happens after Phase 2 analysis).

Sample cadence: screenshot the dashboard every 20 minutes using Playwright, save to `docs/superpowers/specs/2026-04-23-phase2-evidence/`. Minimum 6 screenshots across the 2h window.

- [ ] **Step 5: Collect data**

After ≥2h (confirmed by dashboard showing `cache_delay_secs` non-trivially shrunk from the starting value or a stable cache if the drift turns out not to reproduce with instrumentation):

Via `mcp__win-stream-snv__Shell`:

```powershell
# Stop delivery cleanly
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8910/api/v1/delivery/stop" -TimeoutSec 30

# Export the three time-series via the new diagnostics endpoint
$evt = {id}
Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/diagnostics/pacing?event_id=$evt" `
  -TimeoutSec 30 | ConvertTo-Json -Depth 10 | `
  Set-Content "C:\Users\newlevel\Desktop\pacing-evidence-$(Get-Date -Format 'yyyyMMdd-HHmm').json"

# Also export cache_delay_secs trajectory over the session
Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/status?event_id=$evt&history=true" `
  -TimeoutSec 30 | ConvertTo-Json -Depth 10 | `
  Set-Content "C:\Users\newlevel\Desktop\cache-trajectory-$(Get-Date -Format 'yyyyMMdd-HHmm').json"
```

Download both JSON files via `mcp__win-stream-snv__FileDownload` to the repo under `docs/superpowers/specs/2026-04-23-phase2-evidence/`.

---

### Task 9: Analyze data and select Phase 3 branches

- [ ] **Step 1: Compute rates**

From the JSON files, compute:

- **Producer rate slope:** mean of `producer_rate[*].value` over the 2h run. Expected if clean: 1.000. Deviation > ±0.001 is suspect.
- **Consumer rate slope:** mean of `consumer_rate[*].value` across all endpoints. Expected 1.000.
- **Clock skew slope:** linear regression `skew_ms = a * t_ms + b` across `clock_skew[*]`. `a` is skew drift rate. Convert to ppm: `a * 1e6`. > ±200ppm is suspect.
- **Cache slope:** from cache trajectory, compute linear slope in s/hour. Must match the 18s/hour baseline (or show it's gone — if instrumentation overhead is large enough to disturb the result, that's also information).

Write results into `docs/superpowers/specs/2026-04-23-phase2-evidence/analysis.md` with numbers and a short paragraph attributing the observed cache drift to one or more causes.

- [ ] **Step 2: Select Phase 3 branches**

Using the table from the spec:

| Data shows | Execute Phase 3 branch |
|---|---|
| Clock-skew slope ≥ 200ppm | Task 10a (NTP hardening) |
| Producer rate deviates from 1.000 by ≥ 0.002 | Task 10b (FlvStreamNormalizer timestamp rewriting) |
| Consumer rate deviates from 1.000 by ≥ 0.002 | Task 10c (Rust-side consumer pacer) |

Multiple may apply. Commit the analysis:

```bash
git add docs/superpowers/specs/2026-04-23-phase2-evidence/
git commit -m "docs(drift): phase 2 evidence and root-cause analysis (#135)"
```

- [ ] **Step 3: Proceed to Phase 3**

Skip the Phase 3 sub-tasks that are not selected. Do not delete them from the plan — they stay as "skipped with justification" in the completion report.

---

## Phase 3 — Conditional fix branches (Tasks 10a, 10b, 10c)

Execute only the branches selected by Task 9 Step 2. Each is independent and self-contained.

### Task 10a: NTP hardening on VPS cloud-init (only if clock skew ≥ 200ppm)

**Files:**
- Modify: `crates/rs-cloud/src/cloud_init.rs` (or whichever file renders the cloud-init YAML)
- Test: `crates/rs-cloud/src/cloud_init_tests.rs` or inline

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn cloud_init_installs_and_configures_chrony() {
    let yaml = render_cloud_init("test-uuid", "test-event", 0, &[]);
    assert!(yaml.contains("chrony"),
            "cloud-init must install chrony; got:\n{yaml}");
    assert!(yaml.contains("makestep 0.1 3"),
            "chrony.conf must contain aggressive makestep; got:\n{yaml}");
    // At least two distinct stratum-1 upstream servers
    let peer_count = yaml.matches("server ").count();
    assert!(peer_count >= 2,
            "chrony.conf needs >=2 upstream servers; got {peer_count}");
}
```

- [ ] **Step 2: Implement**

In the cloud-init renderer, extend `packages:` to include `chrony`, and add a `write_files:` entry for `/etc/chrony/chrony.conf` with:

```
server time.cloudflare.com iburst nts
server time.google.com iburst
server pool.ntp.org iburst
makestep 0.1 3
minpoll 4
maxpoll 6
rtcsync
driftfile /var/lib/chrony/drift
```

Plus a `runcmd:` entry to `systemctl enable --now chrony`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all --check
git add crates/rs-cloud/src/cloud_init.rs crates/rs-cloud/src/cloud_init_tests.rs
git commit -m "fix(cloud-init): chrony with sub-second upstream for low-skew VPS (#135)"
```

---

### Task 10b: FlvStreamNormalizer timestamp rewriting (only if producer rate deviates)

**Files:**
- Modify: `crates/rs-delivery/src/flv_normalizer.rs`
- Test: extend `crates/rs-delivery/src/endpoint_task_flv_tests.rs` or inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Append to `crates/rs-delivery/src/flv_normalizer.rs`:

```rust
#[cfg(test)]
mod wall_clock_rate_tests {
    use super::*;

    #[test]
    fn rewrites_tag_timestamps_to_match_producer_wall_clock_span() {
        // Simulate a chunk that spans 30 tags at 33ms apart = 990ms of timestamps,
        // while its wall_clock span is 1000ms. Normalizer must scale timestamps
        // so the last tag lands at 1000ms post-rebase.
        let chunk = synth_flv_chunk(
            /*tag_count*/ 30,
            /*intra_tag_ts_step_ms*/ 33,
            /*wall_clock_span_ms*/ 1000,
        );
        let mut norm = FlvStreamNormalizer::new_with_wall_clock_scaling();
        let out = norm.normalize_with_wall_clock(&chunk.bytes, chunk.wall_clock_span_ms);
        let last_ts = read_last_tag_timestamp(&out);
        // Expected last tag timestamp: 1000 (scaled from 29 * 33 = 957)
        // Tolerance ±5ms for int rounding.
        assert!(
            (last_ts as i64 - 1000).abs() <= 5,
            "last ts {last_ts} not within 5ms of 1000"
        );
    }
}
```

(`synth_flv_chunk` and `read_last_tag_timestamp` are helpers written alongside the test; they build a minimal FLV header + N tags.)

- [ ] **Step 2: Implement**

Add a `normalize_with_wall_clock` variant that:
1. Finds first and last tag timestamps in the chunk (already extractable via existing `find_first_data_ts`/walking helpers).
2. Computes `scale = wall_clock_span_ms / (last_ts - first_ts)`.
3. Walks each tag; for each tag with absolute ts `T`, rewrites to `first_ts + ((T - first_ts) as f64 * scale).round() as u32`.
4. Preserves monotonic continuation across chunk boundaries (existing logic stays).

Wire `wall_clock_span_ms` through: the chunk source needs to pass wall-clock span alongside bytes. The `ChunkInfo` already carries `wall_clock_written_at_ms` per chunk (from Task 2); compute span as `wall_clock_written_at_ms - previous_chunk.wall_clock_written_at_ms`.

In `endpoint_task.rs` (or the new progress_capture file — stay under 1000 lines), track previous chunk wall-clock and pass the span into `normalize_with_wall_clock`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all --check
git add crates/rs-delivery/src/flv_normalizer.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "fix(flv): rewrite tag timestamps to match producer wall-clock cadence (#135)"
```

---

### Task 10c: Rust-side consumer pacer (only if consumer rate deviates)

**Files:**
- Create: `crates/rs-delivery/src/consumer_pacer.rs`
- Modify: `crates/rs-delivery/src/endpoint_task.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (expose module)

- [ ] **Step 1: Write the failing test**

Create `crates/rs-delivery/src/consumer_pacer.rs`:

```rust
//! Rust-side wall-clock anchored pacer for ffmpeg stdin writes.
//! Re-introduced from pre-#129 (removed in commit 3c7b8ef), now with
//! explicit drift clamp so pacer cannot cascade into the 32s death cycle
//! #129 fixed.

use std::time::{Duration, Instant};

pub struct ConsumerPacer {
    anchor_wall: Instant,
    anchor_ts_ms: i64,
    /// Max allowed forward drift before we stop holding back a write.
    /// Clamp prevents pacer from ever waiting > 1s (safety net).
    max_wait_ms: u64,
}

impl ConsumerPacer {
    pub fn new(max_wait_ms: u64) -> Self {
        Self { anchor_wall: Instant::now(), anchor_ts_ms: 0, max_wait_ms }
    }

    pub fn reset(&mut self, current_ts_ms: i64) {
        self.anchor_wall = Instant::now();
        self.anchor_ts_ms = current_ts_ms;
    }

    /// Returns how long to sleep before writing a tag with the given ts.
    /// Clamped to [0, max_wait_ms].
    pub fn delay_for_ts(&self, ts_ms: i64) -> Duration {
        let elapsed_ms = self.anchor_wall.elapsed().as_millis() as i64;
        let target_ms = ts_ms - self.anchor_ts_ms;
        let wait_ms = (target_ms - elapsed_ms).max(0) as u64;
        Duration::from_millis(wait_ms.min(self.max_wait_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pacer_returns_zero_if_ts_is_in_the_past() {
        let mut p = ConsumerPacer::new(1000);
        p.reset(0);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let d = p.delay_for_ts(50);
        assert_eq!(d, Duration::from_millis(0));
    }

    #[tokio::test]
    async fn pacer_waits_up_to_target_ms() {
        let mut p = ConsumerPacer::new(1000);
        p.reset(0);
        let d = p.delay_for_ts(200);
        // Allow 10ms jitter window
        assert!(d >= Duration::from_millis(190) && d <= Duration::from_millis(210),
                "delay {d:?}");
    }

    #[tokio::test]
    async fn pacer_clamps_to_max_wait() {
        let mut p = ConsumerPacer::new(500);
        p.reset(0);
        let d = p.delay_for_ts(10_000);
        assert_eq!(d, Duration::from_millis(500));
    }
}
```

- [ ] **Step 2: Wire into endpoint_task**

In `endpoint_task.rs`, before each `ffmpeg.write(tag_bytes)`, compute and await pacer delay:

```rust
let delay = pacer.delay_for_ts(tag_ts_ms);
if !delay.is_zero() { tokio::time::sleep(delay).await; }
ffmpeg.write(&tag_bytes).await?;
```

Reset the pacer on ffmpeg restart (anchor_ts_ms = first_ts_of_new_process).

- [ ] **Step 3: Commit**

```bash
cargo fmt --all --check
git add crates/rs-delivery/src/consumer_pacer.rs crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/lib.rs
git commit -m "fix(delivery): Rust-side consumer pacer with drift clamp (#135)"
```

---

## Phase 4 — Post-fix live verification (Task 11)

### Task 11: Run ≥1h live verification test

- [ ] **Step 1: Push Phase 3 commits and wait for CI + deploy**

```bash
git push origin dev
# Monitor CI per airuleset ci-monitoring. Do not proceed until green + deployed.
```

- [ ] **Step 2: Confirm fix deployed on stream.lan**

Via `mcp__win-stream-snv__Shell`:

```powershell
(Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status" -TimeoutSec 10).version
```

Expected: `0.3.68`.

- [ ] **Step 3: Run the ≥1h live test**

Repeat Task 8 Steps 2-4 but only for 1 hour instead of 2.

- [ ] **Step 4: Measure cache stability**

Export `cache_delay_secs` trajectory over the 1h run. Compute:

- Maximum deviation from target: `max(|cache_delay_secs - target_secs|)`.
- Success criterion: `max_deviation <= 5 seconds` across the full run.
- Also assert: `ffmpeg_restarts == 0` across the run (regression check for #129).

- [ ] **Step 5: If success, commit evidence**

```bash
git add docs/superpowers/specs/2026-04-23-phase4-evidence/
git commit -m "docs(drift): phase 4 post-fix verification (#135)"
```

- [ ] **Step 6: If failure, loop back to Phase 3**

Analyze what still drifts. Revisit the data from Task 9. A plausible cause not yet addressed must be picked up. Run the appropriate Task 10a/b/c branch (or tune an already-executed one). Re-push, re-verify with another ≥1h test. Do NOT declare done until Phase 4 passes.

---

## Phase 5 — PR (Task 12)

### Task 12: Create the PR and monitor to green + mergeable

- [ ] **Step 1: Final local check**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push any final commits**

```bash
git push origin dev
```

- [ ] **Step 3: Create the PR**

```bash
gh pr create --title "fix: cache drift investigation + data-driven fix (#135)" --body "$(cat <<'EOF'
## Summary

Closes #135.

Diagnosed and fixed the ~18s/hour cache drift observed on multi-hour delivery streams. Approach was data-driven per spec: add three-way telemetry first, run a 2h live test to identify the root cause, then ship the targeted fix(es) — all in one PR.

### Phase 1 — Instrumentation

- Producer wall-clock per chunk (`chunk_records.wall_clock_written_at_ms`, migration V20).
- ffmpeg consumer-rate sampling from stderr `time=` progress lines.
- Clock-skew probe stream.lan ↔ VPS every 30s with RTT compensation.
- `GET /api/v1/diagnostics/pacing` API + Leptos `PacingPanel` on the dashboard.

### Phase 2 — Live investigation

2h+ live test on stream.lan → Hetzner VPS with real OBS streaming. Evidence committed at `docs/superpowers/specs/2026-04-23-phase2-evidence/`. Root cause: **{INSERT FINDING}**.

### Phase 3 — Fix

{INSERT EXECUTED BRANCHES — e.g. "10a NTP hardening + 10b timestamp rewriting"}.

### Phase 4 — Verification

1h post-fix live test. Cache stayed within ±{INSERT}s of target for the full hour, 0 ffmpeg restarts. Evidence committed at `docs/superpowers/specs/2026-04-23-phase4-evidence/`.

## Test plan

- [ ] CI green (all jobs including `deploy-stream-lan`)
- [ ] `cache-drift-panel.spec.ts` Playwright E2E passes
- [ ] Phase 4 live-verification artifacts present in PR
- [ ] Migration V20 lands cleanly on a V19 stream.lan database
- [ ] No regressions on existing E2E tests (streaming, delivery, rescue)

## E2E coverage

| Feature | Test file | What it verifies |
|---|---|---|
| Pacing diagnostics panel | e2e/cache-drift-panel.spec.ts | Dashboard panel renders three series sections; clean console |
| Cache stability (live) | Phase 4 manual live run + dashboard screenshots | `max_deviation <= 5s` across 1h continuous streaming |

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

(Fill in the `{INSERT ...}` placeholders from the Phase 2 analysis and Phase 4 verification before running the command. Do not leave placeholders in the actual PR body.)

- [ ] **Step 4: Monitor CI**

```bash
PR_NUM=$(gh pr list --head dev --json number --jq '.[0].number')
gh pr checks $PR_NUM
# Then monitor with sleep-backgrounded gh run view per ci-monitoring.md.
```

All checks must pass — lint, test, audit, mutation-testing, test-integrity, build, E2E, file-size, coverage, deploy-stream-lan. If anything fails, investigate with `gh run view --log-failed`, fix, re-push, re-monitor.

- [ ] **Step 5: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/$PR_NUM --jq '{mergeable, mergeable_state}'
```

Expected: `mergeable: true, mergeable_state: "clean"`.

- [ ] **Step 6: Post-deploy verification after CI green**

Once the dev-branch CI run completes (deploy-stream-lan runs on push to dev), confirm stream.lan shows version 0.3.68 via `mcp__win-stream-snv__Shell`:

```powershell
(Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status").version
```

And open the dashboard in Playwright, screenshot, confirm the new pacing panel renders.

- [ ] **Step 7: Hand off to user**

Report PR URL + summary per `completion-report.md` format. Do NOT merge. Wait for explicit user instruction to merge. The ONLY valid merge triggers are user phrases like "merge it", "approved", "go ahead" — completed work alone is not permission.

---

## Self-review (inline, fix before handoff)

- [x] **Spec coverage** — every section of the spec has a corresponding task:
  - Phase 1 instrumentation 1a/1b/1c/1d → Tasks 1, 2, 3, 4, 5, 6, 7
  - Phase 2 live investigation → Tasks 8, 9
  - Phase 3 conditional fixes (all 3 branches) → Tasks 10a, 10b, 10c
  - Phase 4 verification → Task 11
  - PR delivery → Task 12
- [x] **No placeholders** — every step has exact code or exact command. PR body has `{INSERT ...}` placeholders that the executing agent MUST fill from Phase 2/4 evidence before running `gh pr create`; this is flagged explicitly.
- [x] **Type consistency** — `DriftSample { t_ms: i64, value: f64 }` defined once in `drift.rs`, reused by Leptos (re-declared with matching fields because leptos-ui is a separate workspace member; could DRY via a shared types crate later, out of scope). `ChunkInfo.wall_clock_written_at_ms: i64` matches `PendingChunkWrite.wall_clock_written_at_ms: i64`. `FfmpegProgress { media_time_ms: i64, wall_clock_ms: i64 }` consistent between `rs-ffmpeg` and `progress_capture.rs`. `ClockSkewSample` fields match the V20 migration columns.
- [x] **Airuleset compliance** — TDD per task, `cargo fmt --all --check` before every push, no local compile/test/clippy, CI monitoring after push, single PR, no merge before explicit user instruction, completion report at end.
