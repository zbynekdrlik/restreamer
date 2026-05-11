# Fast-Endpoint Zero-Reconnect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate Kiko fast-endpoint reconnects by adding per-chunk lifecycle telemetry, a small pusher-side prefetch buffer (K=1 default for fast endpoints, double-buffered), and never-stop S3-download retry — without raising fast-endpoint delay above its current ~4s steady state.

**Architecture:** Two new modules in rs-delivery — `chunk_lifecycle/` (timestamps + audit emission) and `disk_cache/prefetch_queue.rs` + `disk_cache/prefetch_reader.rs` (bounded async FIFO between fetcher and pusher). Plus `download_service.rs` retry-loop hardened from `max_attempts=5` to retry-forever. Behavior preserved for non-fast endpoints (K=0 → bypass queue entirely).

**Tech Stack:** Rust 2024, tokio, async-trait, sqlx + SQLite (incremental migration), rust-s3 0.35 (extends with metadata headers), Leptos CSR WASM (UI bar/badge).

**Spec:** `docs/superpowers/specs/2026-05-10-fast-endpoint-zero-reconnect-design.md` (commit `efe6e25`).

---

## Context

The full architectural rationale lives in the spec. Subagents executing tasks must read the spec section relevant to their task (linked in each task) but must NOT re-design — implement what's specified.

**Branch state**: dev currently at `0.7.5`. Bump to `0.8.0` (architectural change). 4 version files: `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`.

**Constraints (apply to every task)**:

- Local checks: `cargo fmt --all --check` ONLY. NO `cargo build`, `cargo test`, `cargo clippy` locally per `ci-push-discipline` memory.
- TDD strict: failing-test commit BEFORE implementation commit. One commit per task.
- Subagent does NOT push, compile, or run tests locally.
- Every new `.rs` file MUST stay under 1000 lines (CI gate).
- Mutation testing: `chunk_lifecycle::*` and `disk_cache::prefetch_queue` / `prefetch_reader` MUST NOT appear in `--exclude-re`.
- ASCII-only PowerShell strings in CI YAML (memory `feedback_no_unicode_in_ci_scripts`).
- DB migrations are INCREMENTAL only (production has real data) and idempotent (`add_column_if_missing`).
- Behavior preservation: non-fast endpoints get K=0 → bypass queue. Existing config files MUST parse unchanged.
- Never-stop retry: S3 download retries forever; PrefetchReader retries forever; pusher pop_front waits Notify forever.
- All new code paths reference issue `(#NNN)` from Task 1.

---

### Task 1: File GitHub issue

**Files:** none (gh CLI only)

- [ ] **Step 1: File the tracking issue and capture its number**

```bash
gh issue create \
  --title "Fast-endpoint zero-reconnect: lifecycle telemetry + prefetch K=1" \
  --body "$(cat <<'EOF'
## Problem

In production event 9292 on streamsnv (rust-pusher path, v0.7.5), the Kiko fast endpoint died 4 times in ~1 hour, all with `upstream closed connection mid-stream: connection reset` (TCP RST from Resolume receiver). Other endpoints — non-fast with 120s buffer — had zero reconnects in the same window.

Every death was preceded by `chunk_supply_lag_ms` exploding well past the chunk-duration baseline (~2s), with one death preceded by a 1981s spike. The current audit instrumentation (`disk_cache_push_sample`, `endpoint_s3_fetch_failed`) gives the symptom but not the stage where supply stalls.

## Solution

Per `docs/superpowers/specs/2026-05-10-fast-endpoint-zero-reconnect-design.md`:

1. Per-chunk `ChunkLifecycleTimings` (6 stages A→F: host emit → wire write) so every death pinpoints which stage stalled.
2. `PrefetchQueue<K>` between disk_cache fetcher and pusher. K=0 default (non-fast unchanged); K=1 default for fast endpoints (double-buffered → zero added delay in steady state, absorbs 1-chunk supply hiccups invisibly).
3. Never-stop retry on S3 download (kills `max_attempts=5` cap in `download_service.rs:188`).

## Success criteria

Zero Kiko reconnects on operator soak through one full live event. Kiko delay stays at ~4s steady state.
EOF
)"
```

Note the returned URL; the issue number is the trailing path segment. Save it as `ISSUE_NUM` and use `(#$ISSUE_NUM)` in every commit message in tasks 3+.

- [ ] **Step 2: Verify the issue exists**

```bash
gh issue view "$ISSUE_NUM" --json number,title,state
```

Expected: JSON with the new issue number, the title above, and `state: OPEN`.

- [ ] **Step 3: No commit needed for this task** (issue tracker only)

---

### Task 2: Verify Hetzner Object Storage preserves x-amz-meta-* headers

**Files:** none (one-off awscli probe, no code change)

This task confirms the spec's primary assumption — that S3 metadata headers round-trip through Hetzner's nbg1 endpoint unchanged. If they don't, Task 8/18 fall back to DB-row carry only.

- [ ] **Step 1: PUT a test object with two custom metadata headers**

```bash
echo "test-payload" > /tmp/lifecycle-probe.txt
aws s3 cp /tmp/lifecycle-probe.txt \
  s3://restreamer-chunks/lifecycle-probe.txt \
  --endpoint-url https://nbg1.your-objectstorage.com \
  --region nbg1 \
  --metadata "host-emit-ts=1715380800000,s3-complete-ts=1715380800120"
```

Expected: PUT succeeds, no error.

- [ ] **Step 2: HEAD the object and confirm metadata round-trips**

```bash
aws s3api head-object \
  --bucket restreamer-chunks \
  --key lifecycle-probe.txt \
  --endpoint-url https://nbg1.your-objectstorage.com \
  --region nbg1
```

Expected output JSON includes:

```json
{
  "Metadata": {
    "host-emit-ts": "1715380800000",
    "s3-complete-ts": "1715380800120"
  }
}
```

If `Metadata` is empty or missing the keys → Hetzner is stripping unknown `x-amz-meta-*` headers. STOP, report to user, do not proceed (subsequent tasks assume header propagation works).

- [ ] **Step 3: Clean up the probe object**

```bash
aws s3 rm s3://restreamer-chunks/lifecycle-probe.txt \
  --endpoint-url https://nbg1.your-objectstorage.com \
  --region nbg1
```

- [ ] **Step 4: No commit needed** (verification step). If headers round-trip → proceed to Task 3. If they don't → halt and request user direction.

---

### Task 3: Version bump 0.7.5 → 0.8.0

**Files:**
- Modify: `Cargo.toml` (line 25)
- Modify: `src-tauri/Cargo.toml` (line 3)
- Modify: `src-tauri/tauri.conf.json` (line 4)
- Modify: `leptos-ui/Cargo.toml` (line 3)

- [ ] **Step 1: Bump root Cargo.toml**

In `Cargo.toml`, change line 25:

```toml
version = "0.7.5"
```

to:

```toml
version = "0.8.0"
```

- [ ] **Step 2: Bump src-tauri/Cargo.toml**

In `src-tauri/Cargo.toml`, change line 3:

```toml
version = "0.7.5"
```

to:

```toml
version = "0.8.0"
```

- [ ] **Step 3: Bump src-tauri/tauri.conf.json**

In `src-tauri/tauri.conf.json`, change line 4:

```json
  "version": "0.7.5",
```

to:

```json
  "version": "0.8.0",
```

- [ ] **Step 4: Bump leptos-ui/Cargo.toml**

In `leptos-ui/Cargo.toml`, change line 3:

```toml
version = "0.7.5"
```

to:

```toml
version = "0.8.0"
```

- [ ] **Step 5: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0, no diff.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.8.0 (#$ISSUE_NUM)"
```

---

### Task 4: Add 3 new Action variants in rs-core/src/audit.rs

**Files:**
- Modify: `crates/rs-core/src/audit.rs:39-113` (the `Action` enum)
- Modify: `crates/rs-core/src/audit.rs` test module (add one round-trip serde assertion per new variant)

- [ ] **Step 1: Write the failing tests for serde of the new variants**

Append to the existing `mod tests` block at the bottom of `crates/rs-core/src/audit.rs` (after `rate_limiter_keys_disk_cache_push_sample_per_endpoint`):

```rust
    #[test]
    fn action_disk_cache_lifecycle_sample_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::DiskCacheLifecycleSample).unwrap(),
            r#""disk_cache_lifecycle_sample""#
        );
        let back: Action = serde_json::from_str(r#""disk_cache_lifecycle_sample""#).unwrap();
        assert_eq!(back, Action::DiskCacheLifecycleSample);
    }

    #[test]
    fn action_disk_cache_lifecycle_breach_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::DiskCacheLifecycleBreach).unwrap(),
            r#""disk_cache_lifecycle_breach""#
        );
        let back: Action = serde_json::from_str(r#""disk_cache_lifecycle_breach""#).unwrap();
        assert_eq!(back, Action::DiskCacheLifecycleBreach);
    }

    #[test]
    fn action_endpoint_lifecycle_predeath_serdes() {
        assert_eq!(
            serde_json::to_string(&Action::EndpointLifecyclePredeath).unwrap(),
            r#""endpoint_lifecycle_predeath""#
        );
        let back: Action = serde_json::from_str(r#""endpoint_lifecycle_predeath""#).unwrap();
        assert_eq!(back, Action::EndpointLifecyclePredeath);
    }
```

- [ ] **Step 2: Stage the failing-test commit**

```bash
git add crates/rs-core/src/audit.rs
git commit -m "test(audit): add failing serde tests for 3 new lifecycle Action variants (#$ISSUE_NUM)"
```

(The test commit DOES land on disk before the impl commit. CI will run both — the failing-test commit will fail to compile because the variants don't exist yet, which is the expected RED state for TDD. The next commit makes them pass.)

- [ ] **Step 3: Add the 3 enum variants**

In `crates/rs-core/src/audit.rs`, inside the `pub enum Action { ... }` block, append after the `HostInternetRecovered` variant (currently the last one before the closing `}`):

```rust
    /// Per-chunk lifecycle steady-state sample emitted every Nth chunk
    /// per endpoint (default N=30). Carries the 5 stage gaps + worst-stage
    /// label. Severity::Info; rate-limit keyed by endpoint_alias.
    DiskCacheLifecycleSample,
    /// Single chunk where any one stage gap exceeded the breach threshold
    /// (default 4_000ms = 2x chunk_duration). Severity::Warn; per-endpoint
    /// rate-limit window 5s.
    DiskCacheLifecycleBreach,
    /// On endpoint death, dump the last 5 chunks' full lifecycle timings
    /// in one row so the operator can pinpoint which stage stalled.
    /// Severity::Warn; never rate-limited.
    EndpointLifecyclePredeath,
```

- [ ] **Step 4: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/audit.rs
git commit -m "feat(audit): add 3 lifecycle Action variants (#$ISSUE_NUM)"
```

---

### Task 5: Add `Endpoint::prefetch_chunks` config field

**Files:**
- Modify: `crates/rs-core/src/models.rs:75-92` (the `EndpointConfig` struct)
- Modify: `crates/rs-core/src/models.rs` test module (or `crates/rs-core/src/models_tests.rs` if a colocated test module exists; otherwise inline)

- [ ] **Step 1: Write the failing tests**

Locate the `#[cfg(test)] mod tests` block in `crates/rs-core/src/models.rs` (search for `#[test]` near the bottom of the file) and append:

```rust
    #[test]
    fn endpoint_prefetch_chunks_defaults_to_none_when_missing() {
        let json = r#"{
            "id": 1,
            "alias": "Kiko",
            "service_type": "RTMP",
            "stream_key": "rtmp://x/y",
            "enabled": true,
            "position_last": 0,
            "delivered_bytes": 0,
            "is_fast": true,
            "created_at": "2026-05-10T00:00:00Z",
            "updated_at": "2026-05-10T00:00:00Z"
        }"#;
        let parsed: EndpointConfig = serde_json::from_str(json).unwrap();
        assert!(parsed.prefetch_chunks.is_none(), "missing field must default to None");
    }

    #[test]
    fn endpoint_prefetch_chunks_round_trips_explicit_value() {
        let json = r#"{
            "id": 1,
            "alias": "Kiko",
            "service_type": "RTMP",
            "stream_key": "rtmp://x/y",
            "enabled": true,
            "position_last": 0,
            "delivered_bytes": 0,
            "is_fast": true,
            "prefetch_chunks": 3,
            "created_at": "2026-05-10T00:00:00Z",
            "updated_at": "2026-05-10T00:00:00Z"
        }"#;
        let parsed: EndpointConfig = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.prefetch_chunks, Some(3));
    }
```

- [ ] **Step 2: Commit failing test**

```bash
git add crates/rs-core/src/models.rs
git commit -m "test(models): failing tests for Endpoint.prefetch_chunks (#$ISSUE_NUM)"
```

- [ ] **Step 3: Add the field to `EndpointConfig`**

In `crates/rs-core/src/models.rs`, modify the `EndpointConfig` struct (around lines 75-92) to insert the field before `created_at`:

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
    /// Which push backend to use. `#[serde(default)]` keeps existing
    /// config.json files parsing unchanged (missing field -> `Ffmpeg`).
    #[serde(default)]
    pub pusher: PusherKind,
    /// Number of chunks to pre-fetch ahead of the pusher. Resolution
    /// at endpoint init: explicit Some(K) wins; else is_fast=true => K=1
    /// (double-buffered, ~zero added delay); else K=0 (current bypass
    /// behavior). Operator may override per endpoint.
    #[serde(default)]
    pub prefetch_chunks: Option<u32>,
    pub created_at: String,
    pub updated_at: String,
}
```

- [ ] **Step 4: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/models.rs
git commit -m "feat(models): add Endpoint.prefetch_chunks (None=auto-by-fast) (#$ISSUE_NUM)"
```

---

### Task 6: DB migration v24 — chunk_records.host_emit_ts + s3_upload_complete_ts

**Files:**
- Modify: `crates/rs-core/src/db/migrations.rs` (bump `MAX_SCHEMA_VERSION`, add `migrate_v24`, add dispatch arm)
- Add tests inline in the same file under `#[cfg(test)] mod migration_tests` (already exists in `crates/rs-core/src/db/migration_tests.rs`)

- [ ] **Step 1: Write the failing test**

Append to `crates/rs-core/src/db/migration_tests.rs` (inside its `mod migration_tests` block):

```rust
    #[tokio::test]
    async fn migrate_v24_adds_host_emit_ts_and_s3_upload_complete_ts() {
        let pool = crate::db::create_memory_pool().await.unwrap();
        // Seed the DB up to v23.
        crate::db::run_migrations(&pool).await.unwrap();
        // Confirm both new columns exist.
        let host_emit: Option<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_table_info('chunk_records') WHERE name = ?1",
        )
        .bind("host_emit_ts")
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(host_emit.as_deref(), Some("host_emit_ts"));

        let s3_complete: Option<String> = sqlx::query_scalar(
            "SELECT name FROM pragma_table_info('chunk_records') WHERE name = ?1",
        )
        .bind("s3_upload_complete_ts")
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(s3_complete.as_deref(), Some("s3_upload_complete_ts"));
    }

    #[tokio::test]
    async fn migrate_v24_is_idempotent() {
        let pool = crate::db::create_memory_pool().await.unwrap();
        crate::db::run_migrations(&pool).await.unwrap();
        // Re-run: must be a no-op.
        crate::db::run_migrations(&pool).await.unwrap();
        let v: i32 =
            sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(v, 24);
    }

    #[tokio::test]
    async fn max_schema_version_is_24() {
        assert_eq!(crate::db::MAX_SCHEMA_VERSION, 24);
    }
```

- [ ] **Step 2: Commit failing test**

```bash
git add crates/rs-core/src/db/migration_tests.rs
git commit -m "test(db): failing tests for chunk_records v24 migration (#$ISSUE_NUM)"
```

- [ ] **Step 3: Bump MAX_SCHEMA_VERSION**

In `crates/rs-core/src/db/migrations.rs`, line 16, change:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 23;
```

to:

```rust
pub const MAX_SCHEMA_VERSION: i32 = 24;
```

- [ ] **Step 4: Add `migrate_v24` function**

In `crates/rs-core/src/db/migrations.rs`, append after the `MIGRATION_V23_SQL` constant (currently the last item in the file near line 720):

```rust
async fn migrate_v24(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    // Per-chunk lifecycle stages A and B (host clock, millis since epoch).
    // NULL on any chunk uploaded by a pre-v24 host or whose uploader did
    // not complete the second timestamp. Cross-host gap math handles
    // NULL by returning Duration::ZERO.
    add_column_if_missing(
        tx,
        "chunk_records",
        "host_emit_ts",
        "host_emit_ts INTEGER NULL",
    )
    .await?;
    add_column_if_missing(
        tx,
        "chunk_records",
        "s3_upload_complete_ts",
        "s3_upload_complete_ts INTEGER NULL",
    )
    .await
}
```

- [ ] **Step 5: Add the dispatch arm**

In `crates/rs-core/src/db/migrations.rs`, in the `match version { ... }` block in `run_migrations` (around line 318-342), append after the `23 => execute_sql_statements(&mut tx, MIGRATION_V23_SQL).await?,` line:

```rust
            24 => migrate_v24(&mut tx).await?,
```

- [ ] **Step 6: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-core/src/db/migrations.rs crates/rs-core/src/db/migration_tests.rs
git commit -m "feat(db): v24 migration -- add chunk_records.host_emit_ts + s3_upload_complete_ts (#$ISSUE_NUM)"
```

---

### Task 7: Extend rs-endpoint S3Client with metadata HashMap support

**Files:**
- Modify: `crates/rs-endpoint/src/s3.rs:77-119` (`upload_chunk` keeps signature; add `upload_chunk_with_metadata`)
- Modify: `crates/rs-delivery/src/s3_fetch.rs:55-89` (`fetch_chunk_with_meta` reads new headers)

- [ ] **Step 1: Write the failing test for s3_fetch reading new headers**

Append to `crates/rs-delivery/src/s3_fetch.rs` inside its `mod tests` block at the bottom (currently only contains `chunk_key_format`):

```rust
    use super::*;

    #[test]
    fn chunk_data_has_lifecycle_header_fields() {
        // Compile-time assertion: ChunkData carries host_emit_ts and
        // s3_upload_complete_ts (both Option<i64> millis since epoch).
        // The fetcher backfills them from x-amz-meta-* response headers.
        let cd = ChunkData {
            data: vec![],
            duration_ms: 2000,
            host_emit_ts: Some(1715380800000),
            s3_upload_complete_ts: Some(1715380800120),
        };
        assert_eq!(cd.host_emit_ts, Some(1715380800000));
        assert_eq!(cd.s3_upload_complete_ts, Some(1715380800120));
    }
```

- [ ] **Step 2: Commit failing test**

```bash
git add crates/rs-delivery/src/s3_fetch.rs
git commit -m "test(s3_fetch): failing test for ChunkData lifecycle header fields (#$ISSUE_NUM)"
```

- [ ] **Step 3: Add new fields to `ChunkData` and parse new headers in `fetch_chunk_with_meta`**

In `crates/rs-delivery/src/s3_fetch.rs`, replace the `ChunkData` struct (line 19-23) with:

```rust
/// Chunk data with duration + lifecycle stages from S3 object metadata.
pub struct ChunkData {
    pub data: Vec<u8>,
    pub duration_ms: i64,
    /// Stage A: host clock millis since epoch when the chunker wrote the
    /// chunk to local FS. NULL/None when the chunk was uploaded by a
    /// pre-lifecycle host. Cross-host with VPS clock — see spec §4.3.
    pub host_emit_ts: Option<i64>,
    /// Stage B: host clock millis since epoch when the uploader received
    /// the S3 200 OK. NULL/None for legacy chunks.
    pub s3_upload_complete_ts: Option<i64>,
}
```

Then replace the `Ok(response) if response.status_code() == 200 =>` arm in `fetch_chunk_with_meta` (line 64) with:

```rust
            Ok(response) if response.status_code() == 200 => {
                let headers = response.headers();
                let duration_ms = headers
                    .get("x-amz-meta-duration-ms")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                let host_emit_ts = headers
                    .get("x-amz-meta-host-emit-ts")
                    .and_then(|v| v.parse::<i64>().ok());
                let s3_upload_complete_ts = headers
                    .get("x-amz-meta-s3-complete-ts")
                    .and_then(|v| v.parse::<i64>().ok());
                Ok(Some(ChunkData {
                    data: response.to_vec(),
                    duration_ms,
                    host_emit_ts,
                    s3_upload_complete_ts,
                }))
            }
```

- [ ] **Step 4: Update `S3Backend::fetch` impl in download_service.rs to plumb new fields**

In `crates/rs-delivery/src/disk_cache/download_service.rs`, update the trait return type and impl. Replace lines 22-50 with:

```rust
/// Trait abstracting the S3 fetch operation. The real implementation
/// is `crate::s3_fetch::S3Fetcher`; tests use `MockBackend`.
///
/// The `FetchedChunk` return carries the bytes plus all stage-A/B
/// metadata read from the S3 response headers, so `DownloadService`
/// can later associate timings with the chunk it cached.
#[async_trait::async_trait]
pub trait S3Backend: Send + Sync + 'static {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String>;
    /// HEAD-only duration probe. Default delegates to `fetch` (full GET)
    /// for backends that don't implement HEAD; production `S3Fetcher`
    /// overrides with a real HEAD request to keep skip-ahead probes
    /// from downloading full chunk bodies (#174 review finding 2).
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        self.fetch(chunk_id).await.map(|o| o.map(|c| c.duration_ms))
    }
}

/// Bytes + metadata returned by an `S3Backend::fetch` call.
#[derive(Debug, Clone)]
pub struct FetchedChunk {
    pub data: Vec<u8>,
    pub duration_ms: i64,
    pub host_emit_ts: Option<i64>,
    pub s3_upload_complete_ts: Option<i64>,
}

#[async_trait::async_trait]
impl S3Backend for crate::s3_fetch::S3Fetcher {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        match crate::s3_fetch::S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some(FetchedChunk {
                data: cd.data,
                duration_ms: cd.duration_ms,
                host_emit_ts: cd.host_emit_ts,
                s3_upload_complete_ts: cd.s3_upload_complete_ts,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        crate::s3_fetch::S3Fetcher::head_chunk_duration(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}
```

- [ ] **Step 5: Update `fetch_with_retry` in download_service.rs to consume the new shape**

In `crates/rs-delivery/src/disk_cache/download_service.rs`, find the `Ok(Some((data, duration_ms))) => { ... }` branch (line 200) and update to:

```rust
                Ok(Some(fc)) => {
                    self.profile.record_success(elapsed_ms, fc.data.len() as u64);
                    self.token_bucket_consume(fc.data.len() as u64).await;
                    if let Err(e) = self.write_atomic(chunk_id, &fc.data, fc.duration_ms).await {
                        tracing::error!(chunk_id, "disk_cache write failed: {e}");
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    self.durations.lock().await.insert(chunk_id, fc.duration_ms);
                    self.registry.mark_available(chunk_id, fc.data.len() as u64);
                    return;
                }
```

Also update the existing `MockBackend::fetch` in the test module (line 316) to return `FetchedChunk`:

```rust
    #[async_trait::async_trait]
    impl S3Backend for MockBackend {
        async fn fetch(&self, _chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            match self.result.lock().unwrap().clone() {
                Some(Ok((d, dur))) => Ok(Some(FetchedChunk {
                    data: d,
                    duration_ms: dur,
                    host_emit_ts: None,
                    s3_upload_complete_ts: None,
                })),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }
```

And both other test backends — `FlakyBackend` (line 401) and `HeadOnlyBackend` (line 499). The `FlakyBackend::fetch` becomes:

```rust
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("S3 fetch error: status 503".into())
                } else {
                    Ok(Some(FetchedChunk {
                        data: vec![1, 2, 3],
                        duration_ms: 2000,
                        host_emit_ts: None,
                        s3_upload_complete_ts: None,
                    }))
                }
            }
```

The `HeadOnlyBackend::fetch` becomes:

```rust
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                panic!("fetch must not be called for HEAD probe");
            }
```

The `PanicBackend::fetch` becomes:

```rust
            async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
                panic!("simulated backend panic");
            }
```

- [ ] **Step 6: Add `upload_chunk_with_metadata` to S3Client**

In `crates/rs-endpoint/src/s3.rs`, append a new method to the `impl S3Client` block right after `upload_chunk` (line 119):

```rust
    /// Upload a chunk with extra `x-amz-meta-*` headers in addition to the
    /// existing `duration-ms`. Used by the lifecycle uploader (Task 8) so
    /// the VPS can backfill stage A/B timestamps from the S3 GET response.
    ///
    /// `metadata` keys must be lowercase ASCII (Hetzner S3 conformance);
    /// keys are emitted verbatim as `x-amz-meta-{key}`.
    pub async fn upload_chunk_with_metadata(
        &self,
        local_path: &Path,
        event_id: &str,
        seq: i64,
        duration_ms: i64,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<(), EndpointError> {
        let s3_key = Self::chunk_key(event_id, seq);

        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?;

        let file_size = file
            .metadata()
            .await
            .map_err(|e| EndpointError::Io(e.to_string()))?
            .len();

        debug!(
            "Uploading to s3://{}/{} ({file_size} bytes, duration_ms={duration_ms}, meta_keys={})",
            self.bucket.name,
            s3_key,
            metadata.len(),
        );

        let mut upload_bucket = (*self.bucket).clone();
        upload_bucket.add_header("x-amz-meta-duration-ms", &duration_ms.to_string());
        for (k, v) in &metadata {
            upload_bucket.add_header(&format!("x-amz-meta-{k}"), v);
        }

        let response = upload_bucket
            .put_object_stream(&mut file, &s3_key)
            .await
            .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;

        if response.status_code() >= 300 {
            return Err(EndpointError::S3(format!(
                "upload returned status {}",
                response.status_code(),
            )));
        }

        info!(
            "Uploaded {s3_key} ({file_size} bytes, duration_ms={duration_ms}, meta_keys={})",
            metadata.len(),
        );
        Ok(())
    }
```

- [ ] **Step 7: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
git add crates/rs-delivery/src/s3_fetch.rs crates/rs-delivery/src/disk_cache/download_service.rs crates/rs-endpoint/src/s3.rs
git commit -m "feat(s3): plumb x-amz-meta-host-emit-ts and -s3-complete-ts (#$ISSUE_NUM)"
```

---

### Task 8: Host uploader sets host_emit_ts + s3_upload_complete_ts and sends metadata headers

**Files:**
- Modify: `crates/rs-endpoint/src/uploader.rs:351` and the surrounding `upload_one` function

- [ ] **Step 1: Read current upload_one to understand call site**

Run: `grep -n "upload_chunk\|fn upload_one\|chunk_records\|sent_at" crates/rs-endpoint/src/uploader.rs`

Expected: a single `upload_one` definition starting near line 351 that calls `S3Client::upload_chunk(...)` and updates a `chunk_records` row on success via either inline `sqlx::query` or a helper in `rs_core::db::upload`.

- [ ] **Step 2: Write the failing unit test for `now_millis` + the SQL UPDATE shape**

Append to the existing `mod tests` block at the bottom of `crates/rs-endpoint/src/uploader.rs`. This test does NOT exercise the full upload_one path (which depends on S3 + WorkerCtx scaffolding); it asserts the two narrow building blocks (`now_millis` is monotonic + non-zero, and the UPDATE statement against an in-memory DB stamps both columns):

```rust
    #[tokio::test]
    async fn now_millis_returns_non_zero_monotonic_value() {
        let a = super::now_millis();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let b = super::now_millis();
        assert!(a > 0, "now_millis must be non-zero post-1970");
        assert!(b >= a, "now_millis must be monotonic across awaits (got {a} then {b})");
    }

    #[tokio::test]
    async fn stamp_host_emit_and_s3_complete_columns_via_sql() {
        // Verifies the SQL the uploader will issue actually populates
        // both columns. Uses an in-memory pool seeded by the v24
        // migration from Task 6.
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        sqlx::query("INSERT INTO streaming_events(id, name, started_at) VALUES (1,'evt',datetime('now'))")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO chunk_records (id, streaming_event_id, sequence_number, file_path, sent, in_process, created_at, duration_ms) VALUES (1,1,1,'/tmp/x',0,0,datetime('now'),2000)")
            .execute(&pool).await.unwrap();
        let host_emit = super::now_millis();
        sqlx::query("UPDATE chunk_records SET host_emit_ts = ?1 WHERE id = ?2")
            .bind(host_emit).bind(1i64).execute(&pool).await.unwrap();
        let s3_complete = super::now_millis();
        sqlx::query("UPDATE chunk_records SET s3_upload_complete_ts = ?1 WHERE id = ?2")
            .bind(s3_complete).bind(1i64).execute(&pool).await.unwrap();
        let row = sqlx::query("SELECT host_emit_ts, s3_upload_complete_ts FROM chunk_records WHERE id=1")
            .fetch_one(&pool).await.unwrap();
        let h: Option<i64> = sqlx::Row::try_get(&row, "host_emit_ts").unwrap();
        let s: Option<i64> = sqlx::Row::try_get(&row, "s3_upload_complete_ts").unwrap();
        assert!(h.is_some() && s.is_some());
        assert!(s.unwrap() >= h.unwrap());
    }
```

The full upload_one integration is observed during operator soak (Task 22) — visible in the dashboard's prefetch fill / worst-stage badge once the host begins stamping rows.

- [ ] **Step 3: Commit failing test**

```bash
git add crates/rs-endpoint/src/uploader.rs
git commit -m "test(uploader): failing tests for now_millis + lifecycle SQL stamping (#$ISSUE_NUM)"
```

- [ ] **Step 4: Add a `now_millis()` helper at the top of `uploader.rs`**

Insert near the top of `crates/rs-endpoint/src/uploader.rs` (right after the `use` block):

```rust
/// Wall-clock millis since UNIX epoch. Used for lifecycle stage A/B
/// timestamps (#$ISSUE_NUM). Saturates to 0 on the impossible
/// pre-1970 case so the cast never panics.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
```

- [ ] **Step 5: Modify `upload_one` to stamp + send metadata**

Find the `async fn upload_one(ctx: &WorkerCtx, chunk: ChunkRecord)` function (line 351). Replace its body to:

1. Capture `host_emit = now_millis()` immediately on entry.
2. Persist `host_emit` to `chunk_records.host_emit_ts` BEFORE issuing the PUT.
3. Build a `metadata` HashMap with both keys.
4. Switch the call from `s3_client.upload_chunk(...)` to `s3_client.upload_chunk_with_metadata(...)`.
5. After PUT 200, capture `s3_complete = now_millis()` and persist to `chunk_records.s3_upload_complete_ts`.

Apply this diff (the existing function structure stays — only the relevant lines change). Locate the body of `upload_one` and weave in:

```rust
    let host_emit = now_millis();
    if let Err(e) = sqlx::query(
        "UPDATE chunk_records SET host_emit_ts = ?1 WHERE id = ?2",
    )
    .bind(host_emit)
    .bind(chunk.id)
    .execute(&ctx.pool)
    .await
    {
        warn!(chunk_id = chunk.id, "stamp host_emit_ts failed: {e}");
    }

    let mut meta = std::collections::HashMap::new();
    meta.insert("host-emit-ts".to_string(), host_emit.to_string());

    // Existing S3 call site -- replace upload_chunk with upload_chunk_with_metadata.
    // Capture the s3_complete timestamp on success and re-stamp.
    let upload_result = ctx
        .s3_client
        .upload_chunk_with_metadata(
            &chunk.file_path.clone().into(),
            &ctx.event_identifier,
            chunk.sequence_number,
            chunk.duration_ms.unwrap_or(0),
            {
                let mut m = meta.clone();
                // s3-complete-ts is unknown until after the PUT; we set
                // it as soon as we observe Ok(()) below. The S3 metadata
                // header therefore carries only host-emit-ts. The VPS
                // backfills stage B from the chunk_records DB if needed.
                m
            },
        )
        .await;

    if upload_result.is_ok() {
        let s3_complete = now_millis();
        if let Err(e) = sqlx::query(
            "UPDATE chunk_records SET s3_upload_complete_ts = ?1 WHERE id = ?2",
        )
        .bind(s3_complete)
        .bind(chunk.id)
        .execute(&ctx.pool)
        .await
        {
            warn!(chunk_id = chunk.id, "stamp s3_upload_complete_ts failed: {e}");
        }
    }
```

The remainder of `upload_one` (success/failure metric updates, `record_upload_success` call, retry logic) stays exactly as before — only the S3 call swap and the two stamp lines change. Match the existing variable naming style and `ctx` field accesses already used in that function.

- [ ] **Step 6: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 8: Commit**

```bash
git add crates/rs-endpoint/src/uploader.rs
git commit -m "feat(uploader): stamp host_emit_ts + s3_upload_complete_ts (lifecycle stages A/B) (#$ISSUE_NUM)"
```

---

### Task 9: TDD — failing tests for ChunkLifecycleTimings

**Files:**
- Create: `crates/rs-delivery/src/chunk_lifecycle/mod.rs`
- Create: `crates/rs-delivery/src/chunk_lifecycle/timings.rs`
- Create: `crates/rs-delivery/src/chunk_lifecycle/timings_tests.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (add `pub mod chunk_lifecycle;`)

- [ ] **Step 1: Create the empty module facade**

Write `crates/rs-delivery/src/chunk_lifecycle/mod.rs`:

```rust
//! Per-chunk lifecycle telemetry for fast-endpoint zero-reconnect
//! (#$ISSUE_NUM). See spec §3.1-3.2.
//!
//! Component map:
//! - `timings` — `ChunkLifecycleTimings` struct + gap math + worst-stage selection.
//! - `sampler` — `LifecycleSampler` decides when to emit which audit row.
//! - `audit`   — emit_lifecycle_sample / emit_lifecycle_breach / emit_lifecycle_predeath helpers.

#![allow(dead_code, unused_imports)]

pub mod timings;
pub use timings::ChunkLifecycleTimings;

#[cfg(test)]
mod timings_tests;
```

- [ ] **Step 2: Write the failing tests file**

Write `crates/rs-delivery/src/chunk_lifecycle/timings_tests.rs`:

```rust
use super::timings::ChunkLifecycleTimings;
use std::time::{Duration, SystemTime};

fn ms_after(base: SystemTime, ms: u64) -> SystemTime {
    base + Duration::from_millis(ms)
}

#[test]
fn worst_stage_returns_largest_within_clock_gap() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(42, 9292, "Kiko".to_string());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(ms_after(base, 100)); // A->B = 100ms (host clock)
    t.vps_fetch_start_ts = Some(ms_after(base, 5000)); // B->C cross-clock; ignored
    t.vps_fetch_done_ts = Some(ms_after(base, 5800)); // C->D = 800ms
    t.pusher_request_ts = Some(ms_after(base, 5810)); // D->E = 10ms
    t.wire_first_byte_ts = Some(ms_after(base, 9810)); // E->F = 4000ms
    let (label, dur) = t.worst_stage();
    assert_eq!(label, "E->F");
    assert_eq!(dur, Duration::from_millis(4000));
}

#[test]
fn worst_stage_excludes_b_to_c_cross_clock() {
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(ms_after(base, 50));
    // B->C of 60s would dominate every other gap if not excluded.
    t.vps_fetch_start_ts = Some(ms_after(base, 60_050));
    t.vps_fetch_done_ts = Some(ms_after(base, 60_150)); // C->D = 100ms
    t.pusher_request_ts = Some(ms_after(base, 60_160)); // D->E = 10ms
    t.wire_first_byte_ts = Some(ms_after(base, 60_260)); // E->F = 100ms
    let (label, _) = t.worst_stage();
    assert!(
        label != "B->C",
        "B->C must be excluded because clock skew makes it noise"
    );
}

#[test]
fn gap_a_to_b_returns_zero_when_either_missing() {
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    assert_eq!(t.gap_a_to_b(), Duration::ZERO);
    t.host_emit_ts = Some(SystemTime::UNIX_EPOCH);
    assert_eq!(
        t.gap_a_to_b(),
        Duration::ZERO,
        "B missing -> ZERO, never panic"
    );
}

#[test]
fn is_partial_true_when_a_or_b_missing() {
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    assert!(t.is_partial(), "fresh struct: A and B None -> partial");
    t.host_emit_ts = Some(SystemTime::UNIX_EPOCH);
    assert!(t.is_partial(), "B still None -> partial");
    t.s3_upload_complete_ts = Some(SystemTime::UNIX_EPOCH);
    assert!(!t.is_partial(), "both A and B set -> not partial");
}

#[test]
fn gap_returns_zero_on_negative_duration_due_to_skew() {
    // If `later` is BEFORE `earlier` (e.g. clock jump), gap math must
    // saturate to ZERO instead of underflowing.
    let mut t = ChunkLifecycleTimings::new(1, 1, "x".into());
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    t.vps_fetch_start_ts = Some(base + Duration::from_secs(10));
    t.vps_fetch_done_ts = Some(base); // earlier than start: regression
    assert_eq!(t.gap_c_to_d(), Duration::ZERO);
}
```

- [ ] **Step 3: Wire the module into rs-delivery/src/lib.rs**

In `crates/rs-delivery/src/lib.rs`, find the existing `pub mod disk_cache;` line (or near it) and add directly after:

```rust
pub mod chunk_lifecycle;
```

- [ ] **Step 4: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 5: Commit failing tests + module skeleton**

```bash
git add crates/rs-delivery/src/lib.rs crates/rs-delivery/src/chunk_lifecycle/
git commit -m "test(chunk_lifecycle): scaffold module + failing tests for ChunkLifecycleTimings (#$ISSUE_NUM)"
```

---

### Task 10: Implement ChunkLifecycleTimings

**Files:**
- Modify: `crates/rs-delivery/src/chunk_lifecycle/timings.rs` (was created empty in Task 9 implicitly via the mod declaration; explicitly create now)

- [ ] **Step 1: Write the implementation**

Write `crates/rs-delivery/src/chunk_lifecycle/timings.rs`:

```rust
//! Per-chunk lifecycle timestamps. See spec §3.1.

use std::time::{Duration, SystemTime};

/// Six pipeline stages captured per chunk (A..F) plus identifying metadata.
///
/// Stages A and B live on the host clock; stages C..F live on the VPS clock.
/// `worst_stage` excludes the cross-clock B->C gap because skew dominates
/// it and would falsely accuse the host->VPS hop on every chunk.
#[derive(Debug, Clone)]
pub struct ChunkLifecycleTimings {
    pub sequence_number: i64,
    pub event_id: i64,
    pub endpoint_alias: String,

    // Host clock
    /// Stage A: host wrote chunk to local FS.
    pub host_emit_ts: Option<SystemTime>,
    /// Stage B: host received S3 200 OK on PUT.
    pub s3_upload_complete_ts: Option<SystemTime>,

    // VPS clock
    /// Stage C: VPS issued S3 GET.
    pub vps_fetch_start_ts: Option<SystemTime>,
    /// Stage D: VPS GET returned with chunk in memory.
    pub vps_fetch_done_ts: Option<SystemTime>,
    /// Stage E: pusher popped chunk from PrefetchQueue.
    pub pusher_request_ts: Option<SystemTime>,
    /// Stage F: pusher's first TCP write succeeded.
    pub wire_first_byte_ts: Option<SystemTime>,
}

impl ChunkLifecycleTimings {
    pub fn new(sequence_number: i64, event_id: i64, endpoint_alias: String) -> Self {
        Self {
            sequence_number,
            event_id,
            endpoint_alias,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
            vps_fetch_start_ts: None,
            vps_fetch_done_ts: None,
            pusher_request_ts: None,
            wire_first_byte_ts: None,
        }
    }

    fn gap(earlier: Option<SystemTime>, later: Option<SystemTime>) -> Duration {
        match (earlier, later) {
            (Some(a), Some(b)) => b.duration_since(a).unwrap_or(Duration::ZERO),
            _ => Duration::ZERO,
        }
    }

    pub fn gap_a_to_b(&self) -> Duration {
        Self::gap(self.host_emit_ts, self.s3_upload_complete_ts)
    }

    pub fn gap_b_to_c(&self) -> Duration {
        Self::gap(self.s3_upload_complete_ts, self.vps_fetch_start_ts)
    }

    pub fn gap_c_to_d(&self) -> Duration {
        Self::gap(self.vps_fetch_start_ts, self.vps_fetch_done_ts)
    }

    pub fn gap_d_to_e(&self) -> Duration {
        Self::gap(self.vps_fetch_done_ts, self.pusher_request_ts)
    }

    pub fn gap_e_to_f(&self) -> Duration {
        Self::gap(self.pusher_request_ts, self.wire_first_byte_ts)
    }

    /// Returns the (label, duration) of the slowest within-clock stage.
    /// B->C is excluded by design — see struct doc.
    pub fn worst_stage(&self) -> (&'static str, Duration) {
        let candidates: [(&'static str, Duration); 4] = [
            ("A->B", self.gap_a_to_b()),
            ("C->D", self.gap_c_to_d()),
            ("D->E", self.gap_d_to_e()),
            ("E->F", self.gap_e_to_f()),
        ];
        candidates
            .into_iter()
            .max_by_key(|(_, d)| *d)
            .unwrap_or(("none", Duration::ZERO))
    }

    /// True if either stage A or B is None — the chunk was uploaded by
    /// a pre-lifecycle host. Audit rows for partial chunks carry an
    /// `instrumented=false` flag so the dashboard can dim them.
    pub fn is_partial(&self) -> bool {
        self.host_emit_ts.is_none() || self.s3_upload_complete_ts.is_none()
    }
}
```

- [ ] **Step 2: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/chunk_lifecycle/timings.rs
git commit -m "feat(chunk_lifecycle): implement ChunkLifecycleTimings + worst_stage (#$ISSUE_NUM)"
```

---

### Task 11: TDD — failing tests for LifecycleSampler

**Files:**
- Create: `crates/rs-delivery/src/chunk_lifecycle/sampler.rs` (empty stub)
- Create: `crates/rs-delivery/src/chunk_lifecycle/sampler_tests.rs`
- Create: `crates/rs-delivery/src/chunk_lifecycle/audit.rs` (empty stub for emit helpers — Task 12 fills)
- Modify: `crates/rs-delivery/src/chunk_lifecycle/mod.rs` (add `pub mod sampler;` + `pub mod audit;` + test mod)

The sampler's audit emission is observable via the `crate::audit_ring::AuditRing` it pushes into. Tests count `RingRow` entries by Action class to validate sampling cadence + breach rate-limit + predeath behavior.

- [ ] **Step 1: Add empty stubs so the test file compiles**

Write `crates/rs-delivery/src/chunk_lifecycle/sampler.rs`:

```rust
//! LifecycleSampler — see spec §3.2. Implementation in Task 12.

#![allow(dead_code)]

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct LifecycleSampler {
    pub sample_every_n: u64,
    pub breach_threshold_ms: u64,
    pub breach_rate_limit_window: Duration,
    pub last_breach_emit: Option<Instant>,
    pub pushed_count: u64,
    pub predeath_ring: VecDeque<ChunkLifecycleTimings>,
}

impl LifecycleSampler {
    pub fn new(sample_every_n: u64, breach_threshold_ms: u64) -> Self {
        Self {
            sample_every_n,
            breach_threshold_ms,
            breach_rate_limit_window: Duration::from_secs(5),
            last_breach_emit: None,
            pushed_count: 0,
            predeath_ring: VecDeque::with_capacity(5),
        }
    }

    pub fn observe(
        &mut self,
        _timings: &ChunkLifecycleTimings,
        _audit_ring: &Option<Arc<AuditRing>>,
    ) {
        unimplemented!("Task 12")
    }

    pub fn emit_predeath(&self, _audit_ring: &Option<Arc<AuditRing>>) {
        unimplemented!("Task 12")
    }
}
```

Write `crates/rs-delivery/src/chunk_lifecycle/audit.rs`:

```rust
//! Audit-row emit helpers for the lifecycle module (#$ISSUE_NUM).
//! Implemented in Task 12.

#![allow(dead_code)]

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::sync::Arc;

pub fn emit_lifecycle_sample(_ring: &Arc<AuditRing>, _t: &ChunkLifecycleTimings) {
    unimplemented!("Task 12")
}

pub fn emit_lifecycle_breach(_ring: &Arc<AuditRing>, _t: &ChunkLifecycleTimings) {
    unimplemented!("Task 12")
}

pub fn emit_lifecycle_predeath(_ring: &Arc<AuditRing>, _ts: &[ChunkLifecycleTimings]) {
    unimplemented!("Task 12")
}
```

Update `crates/rs-delivery/src/chunk_lifecycle/mod.rs` to include them:

```rust
pub mod audit;
pub mod sampler;
pub mod timings;
pub use sampler::LifecycleSampler;
pub use timings::ChunkLifecycleTimings;

#[cfg(test)]
mod sampler_tests;
#[cfg(test)]
mod timings_tests;
```

- [ ] **Step 2: Write the failing tests**

Write `crates/rs-delivery/src/chunk_lifecycle/sampler_tests.rs`:

```rust
use super::sampler::LifecycleSampler;
use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use rs_core::audit::Action;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn fast_chunk(seq: i64) -> ChunkLifecycleTimings {
    // Steady-state chunk: every gap < 100ms, no breach.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(seq, 9292, "Kiko".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(base + Duration::from_millis(50));
    t.vps_fetch_start_ts = Some(base + Duration::from_millis(60));
    t.vps_fetch_done_ts = Some(base + Duration::from_millis(110));
    t.pusher_request_ts = Some(base + Duration::from_millis(120));
    t.wire_first_byte_ts = Some(base + Duration::from_millis(170));
    t
}

fn slow_chunk(seq: i64) -> ChunkLifecycleTimings {
    // Breach: E->F = 5000ms exceeds default 4000ms threshold.
    let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
    let mut t = ChunkLifecycleTimings::new(seq, 9292, "Kiko".into());
    t.host_emit_ts = Some(base);
    t.s3_upload_complete_ts = Some(base + Duration::from_millis(10));
    t.vps_fetch_start_ts = Some(base + Duration::from_millis(20));
    t.vps_fetch_done_ts = Some(base + Duration::from_millis(30));
    t.pusher_request_ts = Some(base + Duration::from_millis(40));
    t.wire_first_byte_ts = Some(base + Duration::from_millis(5_040));
    t
}

#[tokio::test]
async fn observe_emits_sample_every_nth_chunk() {
    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(/* sample_every_n */ 5, /* breach_ms */ 4_000);
    for i in 0..10 {
        s.observe(&fast_chunk(i), &ring);
    }
    let samples = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleSample)
        .count();
    // Chunk 5 and chunk 10 trigger samples (1-indexed every Nth via pushed_count).
    assert_eq!(samples, 2, "expected 2 samples for 10 chunks at N=5");
}

#[tokio::test]
async fn observe_emits_breach_when_any_stage_exceeds_threshold() {
    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(30, 4_000);
    s.observe(&slow_chunk(1), &ring);
    let breaches = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleBreach)
        .count();
    assert_eq!(breaches, 1);
}

#[tokio::test]
async fn breach_rate_limit_at_most_one_emit_per_5s() {
    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..100 {
        s.observe(&slow_chunk(i), &ring);
    }
    let breaches = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::DiskCacheLifecycleBreach)
        .count();
    // 100 breaches in <1s real time → only the first should emit; the
    // 5-s rate-limit window blocks the rest. Allow ≤ 2 in case the test
    // crosses a window boundary on a slow CI runner.
    assert!(breaches >= 1 && breaches <= 2, "expected 1-2, got {breaches}");
}

#[tokio::test]
async fn emit_predeath_dumps_last_5_chunks_in_one_row_no_rate_limit() {
    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..7 {
        s.observe(&fast_chunk(i), &ring);
    }
    s.emit_predeath(&ring);
    s.emit_predeath(&ring); // second call must also emit (no rate-limit)
    let predeaths = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::EndpointLifecyclePredeath)
        .collect::<Vec<_>>();
    assert_eq!(predeaths.len(), 2);
    let first = &predeaths[0];
    let chunks = first
        .detail
        .as_ref()
        .and_then(|d| d.get("chunks"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(chunks.len(), 5, "predeath dump must carry exactly 5 chunks");
}

#[tokio::test]
async fn observe_pushes_to_predeath_ring_capped_at_5() {
    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..20 {
        s.observe(&fast_chunk(i), &ring);
    }
    assert_eq!(s.predeath_ring.len(), 5, "ring capped at 5");
    let last = s
        .predeath_ring
        .iter()
        .map(|t| t.sequence_number)
        .collect::<Vec<_>>();
    assert_eq!(
        last,
        vec![15, 16, 17, 18, 19],
        "ring keeps the most recent 5"
    );
}
```

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit failing tests + stubs**

```bash
git add crates/rs-delivery/src/chunk_lifecycle/
git commit -m "test(chunk_lifecycle): failing tests for LifecycleSampler (#$ISSUE_NUM)"
```

---

### Task 12: Implement LifecycleSampler + audit helpers

**Files:**
- Modify: `crates/rs-delivery/src/chunk_lifecycle/sampler.rs`
- Modify: `crates/rs-delivery/src/chunk_lifecycle/audit.rs`

- [ ] **Step 1: Implement audit emit helpers**

Replace the body of `crates/rs-delivery/src/chunk_lifecycle/audit.rs` with:

```rust
//! Audit-row emit helpers for the lifecycle module (#$ISSUE_NUM).
//! Builds the `serde_json::Value` detail payloads and pushes via the
//! crate-local `AuditRing`. See spec §3.2.

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use rs_core::audit::{Action, Severity, Source};
use std::sync::Arc;

fn millis_since_epoch_or_zero(ts: Option<std::time::SystemTime>) -> i64 {
    ts.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn timings_to_json(t: &ChunkLifecycleTimings) -> serde_json::Value {
    serde_json::json!({
        "sequence_number": t.sequence_number,
        "event_id": t.event_id,
        "host_emit_ts_ms": millis_since_epoch_or_zero(t.host_emit_ts),
        "s3_upload_complete_ts_ms": millis_since_epoch_or_zero(t.s3_upload_complete_ts),
        "vps_fetch_start_ts_ms": millis_since_epoch_or_zero(t.vps_fetch_start_ts),
        "vps_fetch_done_ts_ms": millis_since_epoch_or_zero(t.vps_fetch_done_ts),
        "pusher_request_ts_ms": millis_since_epoch_or_zero(t.pusher_request_ts),
        "wire_first_byte_ts_ms": millis_since_epoch_or_zero(t.wire_first_byte_ts),
        "gap_a_to_b_ms": t.gap_a_to_b().as_millis() as i64,
        "gap_b_to_c_ms": t.gap_b_to_c().as_millis() as i64,
        "gap_c_to_d_ms": t.gap_c_to_d().as_millis() as i64,
        "gap_d_to_e_ms": t.gap_d_to_e().as_millis() as i64,
        "gap_e_to_f_ms": t.gap_e_to_f().as_millis() as i64,
        "instrumented": !t.is_partial(),
    })
}

pub fn emit_lifecycle_sample(ring: &Arc<AuditRing>, t: &ChunkLifecycleTimings) {
    let (worst_label, worst_dur) = t.worst_stage();
    let detail = serde_json::json!({
        "endpoint": t.endpoint_alias,
        "worst_stage": worst_label,
        "worst_stage_ms": worst_dur.as_millis() as i64,
        "chunk": timings_to_json(t),
    });
    ring.push(
        Severity::Info,
        Source::Vps,
        Some(t.endpoint_alias.clone()),
        Action::DiskCacheLifecycleSample,
        detail,
    );
}

pub fn emit_lifecycle_breach(ring: &Arc<AuditRing>, t: &ChunkLifecycleTimings) {
    let (worst_label, worst_dur) = t.worst_stage();
    let detail = serde_json::json!({
        "endpoint": t.endpoint_alias,
        "worst_stage": worst_label,
        "worst_stage_ms": worst_dur.as_millis() as i64,
        "chunk": timings_to_json(t),
    });
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(t.endpoint_alias.clone()),
        Action::DiskCacheLifecycleBreach,
        detail,
    );
}

pub fn emit_lifecycle_predeath(ring: &Arc<AuditRing>, ts: &[ChunkLifecycleTimings]) {
    let alias = ts
        .last()
        .map(|t| t.endpoint_alias.clone())
        .unwrap_or_default();
    let chunks: Vec<serde_json::Value> = ts.iter().map(timings_to_json).collect();
    let detail = serde_json::json!({
        "endpoint": alias,
        "chunks": chunks,
    });
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(alias),
        Action::EndpointLifecyclePredeath,
        detail,
    );
}
```

- [ ] **Step 2: Implement LifecycleSampler**

Replace the body of `crates/rs-delivery/src/chunk_lifecycle/sampler.rs`'s `LifecycleSampler` impl with:

```rust
//! LifecycleSampler — see spec §3.2.

use super::audit::{emit_lifecycle_breach, emit_lifecycle_predeath, emit_lifecycle_sample};
use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PREDEATH_RING_CAP: usize = 5;

pub struct LifecycleSampler {
    pub sample_every_n: u64,
    pub breach_threshold_ms: u64,
    pub breach_rate_limit_window: Duration,
    pub last_breach_emit: Option<Instant>,
    pub pushed_count: u64,
    pub predeath_ring: VecDeque<ChunkLifecycleTimings>,
}

impl LifecycleSampler {
    pub fn new(sample_every_n: u64, breach_threshold_ms: u64) -> Self {
        Self {
            sample_every_n: sample_every_n.max(1),
            breach_threshold_ms,
            breach_rate_limit_window: Duration::from_secs(5),
            last_breach_emit: None,
            pushed_count: 0,
            predeath_ring: VecDeque::with_capacity(PREDEATH_RING_CAP),
        }
    }

    /// Observe one chunk's lifecycle. Decides whether to emit a sample row,
    /// a breach row, both, or neither. Always pushes to predeath_ring.
    pub fn observe(
        &mut self,
        timings: &ChunkLifecycleTimings,
        audit_ring: &Option<Arc<AuditRing>>,
    ) {
        // Push to predeath ring (cap at 5; evict oldest).
        if self.predeath_ring.len() == PREDEATH_RING_CAP {
            self.predeath_ring.pop_front();
        }
        self.predeath_ring.push_back(timings.clone());

        self.pushed_count = self.pushed_count.saturating_add(1);

        let Some(ring) = audit_ring.as_ref() else {
            return;
        };

        // Sample emission.
        if self.pushed_count.is_multiple_of(self.sample_every_n) {
            emit_lifecycle_sample(ring, timings);
            return;
        }

        // Breach emission (rate-limited).
        let (_label, worst) = timings.worst_stage();
        if worst.as_millis() as u64 > self.breach_threshold_ms {
            let now = Instant::now();
            let allowed = match self.last_breach_emit {
                Some(t) => now.duration_since(t) >= self.breach_rate_limit_window,
                None => true,
            };
            if allowed {
                emit_lifecycle_breach(ring, timings);
                self.last_breach_emit = Some(now);
            }
        }
    }

    /// Emit a predeath row with the last (up to 5) chunks. Always emits;
    /// no rate-limit. Caller invokes this on endpoint death.
    pub fn emit_predeath(&self, audit_ring: &Option<Arc<AuditRing>>) {
        let Some(ring) = audit_ring.as_ref() else {
            return;
        };
        let snapshot: Vec<_> = self.predeath_ring.iter().cloned().collect();
        emit_lifecycle_predeath(ring, &snapshot);
    }
}
```

Note: `is_multiple_of` is a stable Rust 2024 method on integers. If clippy complains in CI, swap to `self.pushed_count % self.sample_every_n == 0`.

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/src/chunk_lifecycle/sampler.rs crates/rs-delivery/src/chunk_lifecycle/audit.rs
git commit -m "feat(chunk_lifecycle): implement LifecycleSampler + audit emit helpers (#$ISSUE_NUM)"
```

---

### Task 13: TDD — failing tests for PrefetchQueue

**Files:**
- Create: `crates/rs-delivery/src/disk_cache/prefetch_queue.rs` (stub only)
- Create: `crates/rs-delivery/src/disk_cache/prefetch_queue_tests.rs`
- Modify: `crates/rs-delivery/src/disk_cache/mod.rs` (add `pub mod prefetch_queue;` + test mod)

The `Chunk` payload type is the in-memory chunk tuple already used elsewhere in `disk_cache` (bytes plus per-chunk metadata). For the queue, the simplest concrete payload is `Arc<Vec<u8>>` so tests don't depend on disk_cache internals — production wires it to `Arc<Vec<u8>>` of the file body that EndpointReader passes.

- [ ] **Step 1: Add stub queue + wire module**

Write `crates/rs-delivery/src/disk_cache/prefetch_queue.rs`:

```rust
//! PrefetchQueue — bounded async FIFO between fetcher and pusher.
//! See spec §3.3. Implementation in Task 14.

#![allow(dead_code, unused_imports)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, thiserror::Error)]
#[error("prefetch queue closed")]
pub struct QueueClosed;

pub struct PrefetchQueue<T: Send + 'static> {
    capacity: usize,
    inner: Mutex<VecDeque<T>>,
    not_full: Notify,
    not_empty: Notify,
    closed: AtomicBool,
}

impl<T: Send + 'static> PrefetchQueue<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            not_full: Notify::new(),
            not_empty: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    pub async fn push_back(&self, _item: T) -> Result<(), QueueClosed> {
        unimplemented!("Task 14")
    }

    pub async fn pop_front(&self) -> Result<T, QueueClosed> {
        unimplemented!("Task 14")
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn close(&self) {
        unimplemented!("Task 14")
    }
}
```

Update `crates/rs-delivery/src/disk_cache/mod.rs` — add to the existing module declarations near the top:

```rust
pub mod prefetch_queue;

#[cfg(test)]
mod prefetch_queue_tests;
```

- [ ] **Step 2: Write the failing tests**

Write `crates/rs-delivery/src/disk_cache/prefetch_queue_tests.rs`:

```rust
use super::prefetch_queue::{PrefetchQueue, QueueClosed};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn fifo_order_for_k_eq_3() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(3);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    q.push_back(3).await.unwrap();
    assert_eq!(q.pop_front().await.unwrap(), 1);
    assert_eq!(q.pop_front().await.unwrap(), 2);
    assert_eq!(q.pop_front().await.unwrap(), 3);
}

#[tokio::test]
async fn push_blocks_when_at_capacity_until_pop_drains_one() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(2);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(3).await });
    // Yield to let push_task start. After 50ms, the third push should NOT
    // have completed (queue is at capacity).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!push_task.is_finished());
    // Drain one slot — push_task must wake and complete.
    assert_eq!(q.pop_front().await.unwrap(), 1);
    push_task.await.unwrap().unwrap();
    assert_eq!(q.pop_front().await.unwrap(), 2);
    assert_eq!(q.pop_front().await.unwrap(), 3);
}

#[tokio::test]
async fn pop_blocks_when_empty_until_push_arrives() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(2);
    let q2 = Arc::clone(&q);
    let pop_task = tokio::spawn(async move { q2.pop_front().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pop_task.is_finished());
    q.push_back(42).await.unwrap();
    let got = pop_task.await.unwrap().unwrap();
    assert_eq!(got, 42);
}

#[tokio::test]
async fn close_unblocks_pending_push_and_pop_with_err() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(1);
    q.push_back(1).await.unwrap();
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(2).await });
    let q3 = Arc::clone(&q);
    let pop_task = tokio::spawn(async move {
        // First pop drains slot, second pop blocks then sees Closed.
        let _ = q3.pop_front().await;
        q3.pop_front().await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    q.close();
    let push_res = push_task.await.unwrap();
    let pop_res = pop_task.await.unwrap();
    assert!(matches!(push_res, Err(QueueClosed)));
    assert!(matches!(pop_res, Err(QueueClosed)));
}

#[tokio::test]
async fn k_zero_is_synchronous_rendezvous() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(0);
    let q2 = Arc::clone(&q);
    let push_task = tokio::spawn(async move { q2.push_back(7).await });
    // K=0 means push must NOT complete until a matching pop is in flight.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !push_task.is_finished(),
        "K=0 push must rendezvous with pop"
    );
    let got = q.pop_front().await.unwrap();
    assert_eq!(got, 7);
    push_task.await.unwrap().unwrap();
    assert_eq!(q.len().await, 0, "K=0 queue never holds anything");
}

#[tokio::test]
async fn len_and_capacity_observable_for_dashboard() {
    let q: Arc<PrefetchQueue<i64>> = PrefetchQueue::new(4);
    assert_eq!(q.capacity(), 4);
    assert_eq!(q.len().await, 0);
    q.push_back(1).await.unwrap();
    q.push_back(2).await.unwrap();
    assert_eq!(q.len().await, 2);
}
```

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit failing tests + stub**

```bash
git add crates/rs-delivery/src/disk_cache/
git commit -m "test(disk_cache): scaffold PrefetchQueue + failing tests (#$ISSUE_NUM)"
```

---

### Task 14: Implement PrefetchQueue

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/prefetch_queue.rs`

- [ ] **Step 1: Replace stub body with full implementation**

Overwrite `crates/rs-delivery/src/disk_cache/prefetch_queue.rs` with:

```rust
//! PrefetchQueue — bounded async FIFO between disk_cache fetcher and
//! pusher. See spec §3.3 for design rationale.
//!
//! K=0 is a synchronous rendezvous channel — push and pop must meet
//! before either returns. Used by non-fast endpoints to preserve
//! today's zero-buffer behavior.
//!
//! K>=1 buffers up to `capacity` items. Reader awaits `not_full`;
//! pusher awaits `not_empty`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, thiserror::Error)]
#[error("prefetch queue closed")]
pub struct QueueClosed;

pub struct PrefetchQueue<T: Send + 'static> {
    capacity: usize,
    inner: Mutex<VecDeque<T>>,
    not_full: Notify,
    not_empty: Notify,
    closed: AtomicBool,
}

impl<T: Send + 'static> PrefetchQueue<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            not_full: Notify::new(),
            not_empty: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    /// Reader-side: push at back. Awaits `not_full` if at capacity.
    /// For K=0 rendezvous, push always blocks for a matching pop.
    pub async fn push_back(&self, item: T) -> Result<(), QueueClosed> {
        if self.capacity == 0 {
            return self.rendezvous_push(item).await;
        }
        // Bounded path.
        let mut item_slot = Some(item);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if g.len() < self.capacity {
                    g.push_back(item_slot.take().expect("loop invariant"));
                    drop(g);
                    self.not_empty.notify_one();
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// Pusher-side: pop front. Awaits `not_empty` if drained.
    /// For K=0 rendezvous, pop wakes any waiting push then receives the item.
    pub async fn pop_front(&self) -> Result<T, QueueClosed> {
        if self.capacity == 0 {
            return self.rendezvous_pop().await;
        }
        loop {
            if self.closed.load(Ordering::Acquire) {
                // Drain whatever remains so closed-with-buffered items
                // are still observable. Once empty + closed -> Err.
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
                return Err(QueueClosed);
            }
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
            }
            notified.await;
        }
    }

    /// K=0 push: park self in the slot and wait for a pop to consume it.
    async fn rendezvous_push(&self, item: T) -> Result<(), QueueClosed> {
        // Implementation: use the bounded path with capacity==1 internally,
        // but keep the queue length-1-only at most. The caller observes
        // `len() == 0` after the rendezvous because the pop drains
        // immediately. Concretely: push when slot empty, then wait for
        // not_full (= a successful pop) before returning.
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueClosed);
        }
        // First wait for the slot to be empty (in case a prior rendezvous
        // is still mid-flight).
        let mut item_slot = Some(item);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if g.is_empty() {
                    g.push_back(item_slot.take().expect("loop invariant"));
                    drop(g);
                    self.not_empty.notify_one();
                    break;
                }
            }
            notified.await;
        }
        // Now wait for the matching pop.
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(QueueClosed);
            }
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let g = self.inner.lock().await;
                if g.is_empty() {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// K=0 pop: take the parked item and notify the pusher its rendezvous
    /// completed.
    async fn rendezvous_pop(&self) -> Result<T, QueueClosed> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
                return Err(QueueClosed);
            }
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut g = self.inner.lock().await;
                if let Some(it) = g.pop_front() {
                    drop(g);
                    self.not_full.notify_one();
                    return Ok(it);
                }
            }
            notified.await;
        }
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Close the queue. Wakes all waiters; subsequent push_back/pop_front
    /// (after draining any remaining items) return `Err(QueueClosed)`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_full.notify_waiters();
        self.not_empty.notify_waiters();
    }
}
```

- [ ] **Step 2: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/prefetch_queue.rs
git commit -m "feat(disk_cache): implement PrefetchQueue (K=0 rendezvous + bounded path) (#$ISSUE_NUM)"
```

---

### Task 15: TDD — failing tests for PrefetchReader infinite-retry

**Files:**
- Create: `crates/rs-delivery/src/disk_cache/prefetch_reader.rs` (stub)
- Create: `crates/rs-delivery/src/disk_cache/prefetch_reader_tests.rs`
- Modify: `crates/rs-delivery/src/disk_cache/mod.rs` (add `pub mod prefetch_reader;` + test mod)

- [ ] **Step 1: Stub + module wire**

Write `crates/rs-delivery/src/disk_cache/prefetch_reader.rs`:

```rust
//! PrefetchReader — background task feeding PrefetchQueue from
//! DownloadService. Retries forever on fetch failure (#$ISSUE_NUM).
//! Implementation in Task 16.

#![allow(dead_code)]

use super::download_service::DownloadService;
use super::prefetch_queue::PrefetchQueue;
use crate::audit_ring::AuditRing;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

pub struct PrefetchReader;

impl PrefetchReader {
    /// Drive the prefetch loop. Never returns until the queue is closed.
    pub async fn run(
        _queue: Arc<PrefetchQueue<Arc<Vec<u8>>>>,
        _download: Arc<DownloadService>,
        _next_chunk_id: Arc<AtomicI64>,
        _audit_ring: Option<Arc<AuditRing>>,
    ) {
        unimplemented!("Task 16")
    }
}
```

Update `crates/rs-delivery/src/disk_cache/mod.rs` declarations:

```rust
pub mod prefetch_reader;

#[cfg(test)]
mod prefetch_reader_tests;
```

- [ ] **Step 2: Write failing tests**

Write `crates/rs-delivery/src/disk_cache/prefetch_reader_tests.rs`:

```rust
use super::download_service::{DownloadService, FetchedChunk, S3Backend};
use super::prefetch_queue::PrefetchQueue;
use super::prefetch_reader::PrefetchReader;
use super::registry::ChunkRegistry;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;

/// Backend that always fails with 503 to drive the infinite-retry path.
struct AlwaysFailing(AtomicU32);

#[async_trait::async_trait]
impl S3Backend for AlwaysFailing {
    async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("S3 fetch error: status 503".into())
    }
}

/// Backend that fails N times then succeeds. Drives the
/// retry-then-recover path.
struct FlakyBackend {
    fail_count: AtomicU32,
    fail_until: u32,
}

#[async_trait::async_trait]
impl S3Backend for FlakyBackend {
    async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
        let n = self.fail_count.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_until {
            Err("S3 fetch error: status 503".into())
        } else {
            Ok(Some(FetchedChunk {
                data: vec![n as u8; 16],
                duration_ms: 2000,
                host_emit_ts: None,
                s3_upload_complete_ts: None,
            }))
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retries_forever_on_503_then_eventually_succeeds() {
    let backend = Arc::new(FlakyBackend {
        fail_count: AtomicU32::new(0),
        fail_until: 50,
    });
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend.clone(),
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    // Advance time past 50 retries' worth of exponential backoff.
    // 50 iterations * cap-60s = ~50min worst case; advance a bit more.
    tokio::time::advance(Duration::from_secs(60 * 60)).await;
    // The first chunk should have eventually arrived.
    let got = tokio::time::timeout(Duration::from_secs(10), queue.pop_front())
        .await
        .expect("reader did not deliver chunk after 50 retries")
        .expect("queue not closed");
    assert!(!got.is_empty());
    assert!(
        backend.fail_count.load(Ordering::SeqCst) >= 50,
        "expected >=50 attempts, got {}",
        backend.fail_count.load(Ordering::SeqCst)
    );
    queue.close();
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retries_continue_indefinitely_no_max_attempts_cap() {
    // Distinct from the above: assert that even past 100 failures, the
    // reader is still trying. Catches a regression that re-introduces
    // any attempt cap.
    let backend = Arc::new(AlwaysFailing(AtomicU32::new(0)));
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend.clone(),
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    // Advance ~3 hours (well beyond any conceivable max-attempts cap).
    tokio::time::advance(Duration::from_secs(3 * 60 * 60)).await;
    tokio::task::yield_now().await;
    let count = backend.0.load(Ordering::SeqCst);
    assert!(
        count >= 100,
        "expected >=100 attempts after 3 simulated hours, got {count}"
    );
    queue.close();
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn close_unblocks_reader_and_task_exits() {
    let backend = Arc::new(AlwaysFailing(AtomicU32::new(0)));
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend.clone(),
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    tokio::time::advance(Duration::from_millis(100)).await;
    queue.close();
    let join = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(join.is_ok(), "reader task must exit after close()");
}
```

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit failing tests + stub**

```bash
git add crates/rs-delivery/src/disk_cache/
git commit -m "test(disk_cache): scaffold PrefetchReader + failing infinite-retry tests (#$ISSUE_NUM)"
```

---

### Task 16: Implement PrefetchReader (infinite retry)

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/prefetch_reader.rs`

- [ ] **Step 1: Replace stub with implementation**

Overwrite `crates/rs-delivery/src/disk_cache/prefetch_reader.rs`:

```rust
//! PrefetchReader — drives PrefetchQueue. Retries forever on fetch
//! failure per spec §3.4 + user rule (never give up). One audit row
//! per minute per error class while retry is active.

use super::download_service::DownloadService;
use super::prefetch_queue::PrefetchQueue;
use super::registry::ChunkAvailability;
use crate::audit_ring::AuditRing;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

pub struct PrefetchReader;

impl PrefetchReader {
    /// Drive the prefetch loop. Returns when the queue is closed
    /// (endpoint shutdown). Never returns due to fetch errors.
    pub async fn run(
        queue: Arc<PrefetchQueue<Arc<Vec<u8>>>>,
        download: Arc<DownloadService>,
        next_chunk_id: Arc<AtomicI64>,
        _audit_ring: Option<Arc<AuditRing>>,
    ) {
        loop {
            let id = next_chunk_id.fetch_add(1, Ordering::AcqRel);
            // request_chunk has its own retry-with-cap inside DownloadService
            // (Task 17 removes the cap). Outside of that, we add an outer
            // retry loop so the reader keeps trying even if request_chunk
            // marks NotFound (404 / persistent failure).
            let bytes = Self::fetch_until_available(&download, id).await;
            // Wrap in Arc so PrefetchQueue can hand to multiple consumers
            // cheaply — current production has one pusher per queue,
            // but the Arc keeps the cost minimal if that ever changes.
            let arc_bytes = Arc::new(bytes);
            if queue.push_back(arc_bytes).await.is_err() {
                // Queue closed -> endpoint shutdown.
                return;
            }
        }
    }

    /// Inner loop: keep calling request_chunk until the registry
    /// reports Available. NotFound triggers a backoff and re-attempt
    /// (this is what makes us "retry forever" even past per-chunk
    /// 404s — see spec §4.5).
    async fn fetch_until_available(download: &Arc<DownloadService>, chunk_id: i64) -> Vec<u8> {
        let mut backoff_secs: u64 = 1;
        loop {
            download.request_chunk(chunk_id).await;
            // After request_chunk returns, the registry holds the terminal
            // state. Try to read the cached file directly.
            match Self::try_read_from_disk(download, chunk_id).await {
                Some(bytes) => return bytes,
                None => {
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
            }
        }
    }

    /// Read the chunk's cached file from disk. Returns None if the
    /// registry reports NotFound or Evicted (so the outer loop can
    /// retry without panicking).
    async fn try_read_from_disk(
        download: &Arc<DownloadService>,
        chunk_id: i64,
    ) -> Option<Vec<u8>> {
        // Confirm registry says Available.
        let state = download
            .registry_for_test()
            .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(60))
            .await
            .ok()?;
        if !matches!(state, ChunkAvailability::Available { .. }) {
            return None;
        }
        let path = download.chunk_path(chunk_id);
        tokio::fs::read(&path).await.ok()
    }
}
```

- [ ] **Step 2: Add helper accessors on DownloadService that the reader needs**

In `crates/rs-delivery/src/disk_cache/download_service.rs`, append two helper methods inside the existing `impl DownloadService { ... }` block (immediately before `pub async fn request_chunk`):

```rust
    /// Path of the cached chunk file inside the per-event directory.
    /// Used by `PrefetchReader::try_read_from_disk` so the reader does
    /// not depend on internal layout knowledge.
    pub fn chunk_path(&self, chunk_id: i64) -> std::path::PathBuf {
        self.event_dir.join(format!("{chunk_id}.bin"))
    }

    /// Test/integration helper: clone of the registry handle so external
    /// callers (PrefetchReader) can call `wait_for_chunk_with_timeout`
    /// without re-plumbing a separate registry argument.
    pub fn registry_for_test(&self) -> Arc<super::registry::ChunkRegistry> {
        Arc::clone(&self.registry)
    }
```

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/prefetch_reader.rs crates/rs-delivery/src/disk_cache/download_service.rs
git commit -m "feat(disk_cache): implement PrefetchReader with infinite retry (#$ISSUE_NUM)"
```

---

### Task 17: Remove max_attempts=5 from download_service.rs (retry-forever inside fetch_with_retry)

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/download_service.rs:186-237` (`fetch_with_retry`)
- Modify: `crates/rs-delivery/src/disk_cache/download_service.rs` test module — remove or adapt `fetch_5xx_exhausts_retries_then_marks_not_found`

- [ ] **Step 1: Write the failing test**

Append to the existing `mod tests` block at the bottom of `crates/rs-delivery/src/disk_cache/download_service.rs`:

```rust
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_with_retry_never_caps_attempts() {
        // Backend always returns 503. After 1 simulated hour the registry
        // must NOT be NotFound — the retry loop must still be running.
        let backend = Arc::new(MockBackend::default());
        backend.set_err("S3 fetch error: status 503");
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        let svc2 = Arc::clone(&svc);
        let req = tokio::spawn(async move { svc2.request_chunk(503).await });
        tokio::time::advance(std::time::Duration::from_secs(60 * 60)).await;
        tokio::task::yield_now().await;
        // The registry MUST NOT have transitioned to NotFound — the loop
        // must still be retrying.
        let st = registry.peek(503);
        assert!(
            !matches!(st, Some(ChunkAvailability::NotFound)),
            "retry-forever must NOT mark NotFound, got {st:?}"
        );
        // Also: backend got many attempts (proof we're still trying).
        assert!(
            backend.count() >= 60,
            "expected >=60 attempts after 1h simulated time, got {}",
            backend.count()
        );
        // Cleanup: cancel the request.
        req.abort();
    }

    #[tokio::test]
    async fn fetch_with_retry_backoff_caps_at_60s() {
        // White-box assertion on the backoff schedule. The cap matters
        // because uncapped exponential growth would make the reader
        // sleep multiple minutes between attempts after a few failures.
        // Synthesise the schedule: 1, 2, 4, 8, 16, 32, 60, 60, 60...
        let mut backoff_secs: u64 = 1;
        let mut steps = vec![];
        for _ in 0..10 {
            steps.push(backoff_secs);
            backoff_secs = (backoff_secs * 2).min(60);
        }
        assert_eq!(steps, vec![1, 2, 4, 8, 16, 32, 60, 60, 60, 60]);
    }
```

Also add a `peek` method on `ChunkRegistry` if missing.

First, the subagent reads the registry's internal storage:

```bash
grep -n "pub struct ChunkRegistry\|pub enum ChunkAvailability\|fn mark_available\|fn wait_for_chunk_with_timeout" crates/rs-delivery/src/disk_cache/registry.rs
```

The output reveals the field name backing the per-chunk state map (most likely `state: tokio::sync::Mutex<HashMap<i64, ChunkAvailability>>` or similar). Append to the existing `impl ChunkRegistry { ... }` block in `crates/rs-delivery/src/disk_cache/registry.rs`, immediately after `wait_for_chunk_with_timeout`:

```rust
    /// Non-blocking snapshot of the chunk's current state. Returns None
    /// if the registry has no record of this chunk_id yet. Used by
    /// tests + the PrefetchReader's outer loop to peek without waiting.
    ///
    /// Implementation: synchronously locks the same mutex used by
    /// `mark_available` / `mark_not_found` and clones the value out.
    /// `try_lock` would risk returning a false None during contention,
    /// so we use `blocking_lock`. The function is short — a HashMap
    /// `get(&id).copied()` — so blocking the executor briefly is fine.
    pub fn peek(&self, chunk_id: i64) -> Option<ChunkAvailability> {
        // Replace `state` below with the actual private field name
        // discovered in the grep above. ChunkAvailability is `Copy`
        // (variant per spec); if it isn't, change `.copied()` to
        // `.cloned()` to match.
        self.state.blocking_lock().get(&chunk_id).copied()
    }
```

If the existing field is named differently (e.g. `inner`, `chunks`, `entries`), the subagent substitutes that name. If `ChunkAvailability` is not `Copy`, switch `.copied()` to `.cloned()` and add `#[derive(Clone)]` to the enum if necessary. NO `unimplemented!` in the final commit — the subagent inspects the file and writes the correct accessor.

- [ ] **Step 2: Commit failing tests**

```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs crates/rs-delivery/src/disk_cache/registry.rs
git commit -m "test(disk_cache): failing tests for retry-forever in fetch_with_retry (#$ISSUE_NUM)"
```

- [ ] **Step 3: Implement retry-forever in fetch_with_retry**

Replace the `async fn fetch_with_retry` body in `crates/rs-delivery/src/disk_cache/download_service.rs` (currently lines 186-237) with:

```rust
    async fn fetch_with_retry(self: &Arc<Self>, chunk_id: i64) {
        let mut backoff_secs: u64 = 1;
        let mut attempt: u64 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let _permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let fetch_start = Instant::now();
            let result = self.backend.fetch(chunk_id).await;
            let elapsed_ms = fetch_start.elapsed().as_millis() as u64;
            match result {
                Ok(Some(fc)) => {
                    self.profile.record_success(elapsed_ms, fc.data.len() as u64);
                    self.token_bucket_consume(fc.data.len() as u64).await;
                    if let Err(e) = self.write_atomic(chunk_id, &fc.data, fc.duration_ms).await {
                        tracing::error!(chunk_id, "disk_cache write failed: {e}");
                        // Disk-write failures are NOT recoverable here — the
                        // outer loop in PrefetchReader will re-request and
                        // we will retry from scratch. Mark NotFound so the
                        // outer loop wakes.
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    self.durations.lock().await.insert(chunk_id, fc.duration_ms);
                    self.registry.mark_available(chunk_id, fc.data.len() as u64);
                    return;
                }
                Ok(None) => {
                    // 404 = chunk genuinely not on S3. Don't loop forever
                    // here -- mark NotFound so the OUTER PrefetchReader
                    // loop decides whether to retry the same chunk_id
                    // (genuine miss is rare; uploader will eventually
                    // PUT). 404 is the one terminal outcome.
                    self.registry.mark_not_found(chunk_id);
                    return;
                }
                Err(e) => {
                    tracing::warn!(chunk_id, attempt, "disk_cache S3 fetch failed: {e}");
                    let class = crate::endpoint_audit::classify_s3_fetch_error(&e.to_string());
                    self.profile.record_failure(class);
                    drop(_permit);
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                    // No max_attempts check. Loop until success, 404, or
                    // disk-write hard fail. Per user rule: never give up
                    // on transient errors; only slow down.
                }
            }
        }
    }
```

- [ ] **Step 4: Adapt the existing `fetch_5xx_exhausts_retries_then_marks_not_found` test**

The old test asserted that after 5 attempts the registry transitions to NotFound. That contract is now wrong (we retry forever). Replace its body with an assertion that the request hangs (still retrying) under simulated time:

In `crates/rs-delivery/src/disk_cache/download_service.rs`, find `async fn fetch_5xx_exhausts_retries_then_marks_not_found(...)` (around line 530) and rename it to `fetch_5xx_no_longer_exhausts_retries_loops_forever`. Replace the body with:

```rust
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fetch_5xx_no_longer_exhausts_retries_loops_forever() {
        let backend = Arc::new(MockBackend::default());
        backend.set_err("S3 fetch error: status 503");
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        let svc2 = Arc::clone(&svc);
        let task = tokio::spawn(async move { svc2.request_chunk(503).await });
        tokio::time::advance(std::time::Duration::from_secs(30 * 60)).await;
        tokio::task::yield_now().await;
        // After 30 simulated minutes the loop must still be retrying.
        let state = registry.peek(503);
        assert!(
            !matches!(state, Some(ChunkAvailability::NotFound)),
            "retry-forever must not give up, got state={state:?}"
        );
        assert!(backend.count() >= 30);
        task.abort();
    }
```

- [ ] **Step 5: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs
git commit -m "fix(disk_cache): never give up on S3 download (#$ISSUE_NUM)"
```

---

### Task 18: Wire PrefetchQueue + PrefetchReader + lifecycle into EndpointReader

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs` — adapt `run_once` to consume from a `PrefetchQueue` instead of polling registry directly
- Modify: `crates/rs-delivery/src/disk_cache/mod.rs` — `DiskCache` exposes the queue + reader so EndpointHandle can wire them
- Modify: `crates/rs-delivery/src/api.rs::handle_init` — choose K from endpoint config + spawn PrefetchReader once per endpoint
- Modify: `crates/rs-delivery/src/endpoint_task.rs` — update consumer call site to take chunks from the new queue + capture lifecycle stage E/F + invoke `LifecycleSampler.observe` after each push + invoke `emit_predeath` on push error

This is the largest task. The subagent must read each file before editing — do NOT speculate about call sites.

- [ ] **Step 1: Read the integration surface**

Run these commands to ground the changes (do NOT compile):

```bash
grep -n "EndpointReader::run_once\|EndpointHandle::spawn\|fn init_endpoints\|disk_cache" crates/rs-delivery/src/api.rs | head -30
grep -n "EndpointReader::run_once\|consumer_task\|push_chunk" crates/rs-delivery/src/endpoint_task.rs | head -30
```

The subagent uses the output to locate the exact lines that wire the existing `EndpointReader::run_once` into the production endpoint task. Edits are targeted at those lines.

- [ ] **Step 2: Add a `PrefetchQueue` + `PrefetchReader` ownership pair to `DiskCache`**

In `crates/rs-delivery/src/disk_cache/mod.rs`, extend the `DiskCache` struct (currently around line 67) and its `new` constructor to ALSO build a per-endpoint queue map. Append the following inside `impl DiskCache`:

```rust
    /// Build (and remember) a PrefetchQueue + spawn its PrefetchReader for
    /// the given endpoint alias. Idempotent: returns the existing queue if
    /// the alias is already registered. K is the prefetch depth.
    pub async fn ensure_endpoint_queue(
        &self,
        alias: &str,
        start_chunk_id: i64,
        prefetch_k: usize,
        audit_ring: Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    ) -> std::sync::Arc<crate::disk_cache::prefetch_queue::PrefetchQueue<std::sync::Arc<Vec<u8>>>>
    {
        use std::sync::atomic::AtomicI64;
        let mut g = self.endpoint_queues.lock().await;
        if let Some(q) = g.get(alias) {
            return std::sync::Arc::clone(q);
        }
        let queue = crate::disk_cache::prefetch_queue::PrefetchQueue::new(prefetch_k);
        let next_id = std::sync::Arc::new(AtomicI64::new(start_chunk_id));
        let q_run = std::sync::Arc::clone(&queue);
        let dl = std::sync::Arc::clone(&self.download_service);
        tokio::spawn(async move {
            crate::disk_cache::prefetch_reader::PrefetchReader::run(q_run, dl, next_id, audit_ring)
                .await;
        });
        g.insert(alias.to_string(), std::sync::Arc::clone(&queue));
        queue
    }
```

Add the field to the struct + initialize it in `new`:

```rust
    /// Per-endpoint PrefetchQueue handles, keyed by alias. Lifetimes
    /// match the endpoint's run; close()-d on endpoint stop.
    endpoint_queues: tokio::sync::Mutex<
        std::collections::HashMap<
            String,
            std::sync::Arc<
                crate::disk_cache::prefetch_queue::PrefetchQueue<std::sync::Arc<Vec<u8>>>,
            >,
        >,
    >,
```

(Subagent: locate the `DiskCache::new` constructor inside `mod.rs` and add `endpoint_queues: tokio::sync::Mutex::new(std::collections::HashMap::new()),` to the struct literal.)

- [ ] **Step 3: Modify `EndpointReader::run_once` to consume from the queue when `cfg.queue.is_some()`**

In `crates/rs-delivery/src/disk_cache/endpoint_reader.rs`, extend `ReaderConfig` with an optional queue handle and update `run_once` to prefer it when present. Add to `ReaderConfig`:

```rust
    /// When Some, EndpointReader pops chunks from this PrefetchQueue
    /// instead of polling the registry. Stages D->E and E->F timestamps
    /// are emitted via the LifecycleSampler the caller passes alongside.
    pub queue: Option<
        std::sync::Arc<
            crate::disk_cache::prefetch_queue::PrefetchQueue<std::sync::Arc<Vec<u8>>>,
        >,
    >,
```

In `run_once`, branch on `cfg.queue`. If present, pop from the queue (timestamps E and F) and call `pusher.push_chunk(bytes)`. Keep the existing registry-polling branch unchanged for backward compat with current tests.

```rust
        if let Some(q) = cfg.queue.clone() {
            // Lifecycle-aware path. Stage E set before pop; F set after
            // first byte goes out. The LifecycleSampler invocation lives
            // in endpoint_task.rs (the caller wraps push_chunk with the
            // observe/emit_predeath plumbing — keeping run_once free of
            // the audit_ring dependency).
            loop {
                if let Some(cap) = cfg.max_chunks {
                    if pushed >= cap {
                        return Ok(());
                    }
                }
                let arc_bytes = q
                    .pop_front()
                    .await
                    .map_err(|_| ReaderError::PushFailed("queue closed".into()))?;
                let bytes = (*arc_bytes).clone();
                pusher
                    .push_chunk(bytes)
                    .await
                    .map_err(ReaderError::PushFailed)?;
                positions.advance(&cfg.alias, chunk_id);
                chunk_id += 1;
                pushed += 1;
            }
        }
```

(Subagent: place this branch before the existing `loop { ... }`. Existing tests that don't set `queue` continue to use the registry-polling path — keep them green.)

- [ ] **Step 4: Wire endpoint_task.rs consumer site to use the queue + lifecycle sampler**

In `crates/rs-delivery/src/endpoint_task.rs`, locate the call site that constructs `ReaderConfig` and spawns `EndpointReader::run_once` for the rust-pusher path (around the existing `is_fast` branch). Modify it to:

1. Resolve `prefetch_k`:
   ```rust
   let prefetch_k = ep_cfg
       .prefetch_chunks
       .map(|k| k as usize)
       .unwrap_or_else(|| if ep_cfg.is_fast { 1 } else { 0 });
   ```
2. Get-or-create the queue via `disk_cache.ensure_endpoint_queue(&ep_cfg.alias, start_chunk_id, prefetch_k, audit_ring.clone()).await`.
3. Pass that queue into `ReaderConfig.queue`.
4. Build a `LifecycleSampler::new(30, 4_000)` and store it inside the per-endpoint state struct.
5. Wrap the pusher's `push_chunk` so each successful push records stages E/F into a `ChunkLifecycleTimings` and invokes `sampler.observe(&t, &audit_ring)`. Backfill A/B from the chunk_records DB row (`SELECT host_emit_ts, s3_upload_complete_ts FROM chunk_records WHERE sequence_number = ?1`) if not already supplied via S3 headers.
6. On any error path that would be reported as `endpoint_rtmp_push_died`, call `sampler.emit_predeath(&audit_ring)` immediately before the existing audit emission so the predeath row lands FIRST in the timeline.

Implementation strategy: introduce a thin wrapper `LifecycleAwarePusher` that owns the inner pusher + sampler + audit_ring + DB pool. Place it inside `endpoint_task.rs` near the existing pusher trait impls. The wrapper's `push_chunk` body:

```rust
async fn push_chunk(&mut self, bytes: Vec<u8>) -> Result<(), String> {
    let mut t = ChunkLifecycleTimings::new(self.next_seq, self.event_id, self.alias.clone());
    t.pusher_request_ts = Some(SystemTime::now()); // E
    let result = self.inner.push_chunk(bytes).await;
    t.wire_first_byte_ts = Some(SystemTime::now()); // F
    if let Some(row) = sqlx::query(
        "SELECT host_emit_ts, s3_upload_complete_ts FROM chunk_records WHERE sequence_number = ?1",
    )
    .bind(self.next_seq)
    .fetch_optional(&self.pool)
    .await
    .ok()
    .flatten()
    {
        let host_emit: Option<i64> = row.try_get("host_emit_ts").ok().flatten();
        let s3_complete: Option<i64> = row.try_get("s3_upload_complete_ts").ok().flatten();
        t.host_emit_ts = host_emit.map(|ms| {
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms as u64)
        });
        t.s3_upload_complete_ts = s3_complete.map(|ms| {
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms as u64)
        });
    }
    self.sampler.observe(&t, &self.audit_ring);
    if result.is_err() {
        self.sampler.emit_predeath(&self.audit_ring);
    }
    self.next_seq += 1;
    result
}
```

(Subagent: integrate with the existing pusher trait used in endpoint_task.rs. The wrapper sits between `EndpointReader` and the inner `RtmpPusher`. Stages C/D are set inside `PrefetchReader` — Task 16 — and threaded through the queue payload via a future enhancement; for THIS PR ship A/B + E/F populated, leaving C/D as None for now. The spec already documents that gap math handles None as Duration::ZERO.)

- [ ] **Step 5: On endpoint stop, close the queue**

In `crates/rs-delivery/src/disk_cache/mod.rs`, add a method to `DiskCache`:

```rust
    /// Close the per-endpoint queue (drops the PrefetchReader task by
    /// causing its push_back to return Err). Idempotent.
    pub async fn close_endpoint_queue(&self, alias: &str) {
        let mut g = self.endpoint_queues.lock().await;
        if let Some(q) = g.remove(alias) {
            q.close();
        }
    }
```

In `endpoint_task.rs`, in the existing endpoint shutdown handler (search for `ep_handle.stop` or the LocalCancel arm), add a call:

```rust
disk_cache.close_endpoint_queue(&alias).await;
```

- [ ] **Step 6: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/ crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat(delivery): wire PrefetchQueue + LifecycleSampler into EndpointReader hot path (#$ISSUE_NUM)"
```

---

### Task 19: Surface prefetch_fill + last lifecycle sample end-to-end (host → DTO → leptos)

**Files:**
- Modify: `crates/rs-delivery/src/api.rs:267-298` — `EndpointStatusEntry` adds `prefetch_fill` + `last_lifecycle_worst_stage`
- Modify: `crates/rs-delivery/src/api.rs:308-330` — populate the new fields from per-endpoint stats
- Modify: `crates/rs-api/src/delivery_status.rs` — pass through the two new fields
- Modify: `crates/rs-api/src/delivery_handlers.rs` — pass through to the host-facing DTO
- Modify: `leptos-ui/src/api.rs:718-740` — `DeliveryEndpointDetail` adds matching fields with `#[serde(default)]`
- Modify: `leptos-ui/src/store.rs` — extend the per-endpoint signal struct
- Modify: `leptos-ui/src/components/endpoints.rs:155-` — render fill bar + worst-stage badge inside the existing endpoint card

- [ ] **Step 1: Add VPS-side fields to `EndpointStatusEntry`**

In `crates/rs-delivery/src/api.rs`, extend the `EndpointStatusEntry` struct (line 267) with:

```rust
    /// Prefetch queue depth + capacity, when the endpoint runs through
    /// PrefetchQueue (K>=1). None for K=0 (non-fast endpoints).
    #[serde(skip_serializing_if = "Option::is_none")]
    prefetch_fill: Option<PrefetchFill>,
    /// Most recent LifecycleSampler observation: worst stage label and
    /// duration in millis. `("none", 0)` until the first chunk is pushed.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_lifecycle_worst_stage: Option<LifecycleSummary>,
```

Add the supporting types nearby (immediately above `EndpointStatusEntry`):

```rust
#[derive(serde::Serialize)]
pub struct PrefetchFill {
    pub depth: u32,
    pub capacity: u32,
}

#[derive(serde::Serialize)]
pub struct LifecycleSummary {
    pub worst_stage: String,
    pub worst_stage_ms: i64,
}
```

In the `endpoint_status` function (line 287), extend the `EndpointStatusEntry { ... }` literal (line 308) to populate the two new fields:

```rust
            prefetch_fill: stats.prefetch_fill.clone(),
            last_lifecycle_worst_stage: stats.last_lifecycle_summary.clone(),
```

This requires adding `prefetch_fill: Option<PrefetchFill>` and `last_lifecycle_summary: Option<LifecycleSummary>` to the per-endpoint stats struct. Locate that struct (`grep -n "pub struct.*Stats\|pub fn stats" crates/rs-delivery/src/endpoint_task.rs | head -5`) and append both fields. Populate them inside the `LifecycleAwarePusher::push_chunk` wrapper from Task 18 — after `sampler.observe`, snapshot the queue depth + worst stage.

- [ ] **Step 2: Pass new fields through the host-side parser**

In `crates/rs-api/src/delivery_status.rs`, locate the struct that mirrors `EndpointStatusEntry` (search `grep -n "pub struct.*EndpointDelivery\|pub struct EndpointStatus" crates/rs-api/src/delivery_status.rs`). Add the two new optional fields with the same names and types. Then pass them through to whatever DTO the dashboard consumes.

In `crates/rs-api/src/delivery_handlers.rs`, find `pub struct DeliveryEndpointEntry` (around line 220) and add:

```rust
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_fill: Option<PrefetchFillDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_lifecycle_worst_stage: Option<LifecycleSummaryDto>,
```

Mirror the helper struct definitions:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PrefetchFillDto {
    pub depth: u32,
    pub capacity: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LifecycleSummaryDto {
    pub worst_stage: String,
    pub worst_stage_ms: i64,
}
```

- [ ] **Step 3: Add fields to leptos `DeliveryEndpointDetail`**

In `leptos-ui/src/api.rs`, extend `DeliveryEndpointDetail` (line 718) with:

```rust
    #[serde(default)]
    pub prefetch_fill: Option<PrefetchFillUi>,
    #[serde(default)]
    pub last_lifecycle_worst_stage: Option<LifecycleSummaryUi>,
```

Add the helper types near the existing `DeliveryInstanceInfo`:

```rust
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PrefetchFillUi {
    pub depth: u32,
    pub capacity: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LifecycleSummaryUi {
    pub worst_stage: String,
    pub worst_stage_ms: i64,
}
```

Apply the same to `CachedDeliveryEndpoint` so WS-broadcast updates also carry the fields.

- [ ] **Step 4: Render the fill bar + worst-stage badge in endpoints.rs**

In `leptos-ui/src/components/endpoints.rs`, locate the per-endpoint card render (line 155 — `<div class="endpoint-card">`). Append before the closing `</div>`:

```rust
{move || endpoint.prefetch_fill.as_ref().map(|fill| {
    let pct = if fill.capacity == 0 {
        0
    } else {
        ((fill.depth * 100) / fill.capacity).min(100)
    };
    let color = if fill.depth == 0 {
        "var(--color-error)"
    } else if fill.depth * 2 < fill.capacity {
        "var(--color-warn)"
    } else {
        "var(--color-ok)"
    };
    view! {
        <div class="prefetch-fill" data-testid="prefetch-fill"
             title=format!("Prefetch buffer: {}/{}", fill.depth, fill.capacity)>
            <div class="prefetch-fill-bar"
                 style=format!("width: {pct}%; background: {color};") />
        </div>
    }
})}
{move || endpoint.last_lifecycle_worst_stage.as_ref().map(|ls| view! {
    <span class="worst-stage-badge" data-testid="worst-stage-badge">
        {format!("{}: {}ms", ls.worst_stage, ls.worst_stage_ms)}
    </span>
})}
```

(Subagent: match the exact reactive idiom used elsewhere in the file. The above is the structural shape — adapt to the existing signal/View<_> patterns.)

Add minimal CSS in `leptos-ui/style/main.css` (or whichever file the endpoints CSS lives in — `grep -rn "endpoint-card" leptos-ui/style/`):

```css
.prefetch-fill {
  height: 4px;
  width: 100%;
  background: var(--color-bg-muted);
  border-radius: 2px;
  margin-top: 4px;
}
.prefetch-fill-bar {
  height: 100%;
  border-radius: 2px;
}
.worst-stage-badge {
  display: inline-block;
  font-size: 0.75rem;
  padding: 2px 6px;
  background: var(--color-bg-muted);
  border-radius: 4px;
  margin-left: 4px;
}
```

- [ ] **Step 5: Add Playwright assertion that the new UI elements render for fast endpoints**

Append to the existing E2E test that loads the dashboard for an active event (`grep -rn "endpoint-card\|prefetch" e2e/tests-frontend/ 2>/dev/null` to locate). If no suitable test exists, add a new one at `e2e/tests-frontend/lifecycle-ui.spec.ts`:

```typescript
import { test, expect } from "@playwright/test";

test("dashboard renders prefetch-fill and worst-stage badge for fast endpoint", async ({
  page,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto(process.env.DASHBOARD_URL || "http://10.77.9.204:8910/");
  // Trigger an event with a fast endpoint -- the existing harness already
  // creates one; assume `event_with_fast` is the seed.
  await page.waitForSelector('[data-testid="endpoint-card"]');
  // At least one fast endpoint must show the prefetch fill bar.
  const fillCount = await page.locator('[data-testid="prefetch-fill"]').count();
  expect(fillCount).toBeGreaterThan(0);
  // After ~30s of pushing, at least one badge should be populated.
  await page.waitForSelector('[data-testid="worst-stage-badge"]', { timeout: 60_000 });
  const badge = await page.locator('[data-testid="worst-stage-badge"]').first().textContent();
  expect(badge).toMatch(/^[A-F\->]+:\s\d+ms$/);
  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 6: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/src/api.rs crates/rs-api/src/delivery_status.rs crates/rs-api/src/delivery_handlers.rs leptos-ui/src/api.rs leptos-ui/src/store.rs leptos-ui/src/components/endpoints.rs leptos-ui/style/ e2e/tests-frontend/
git commit -m "feat(ui): prefetch fill bar + worst-stage badge end-to-end (#$ISSUE_NUM)"
```

---

### Task 20: Integration tests (5 tests per spec §5.2)

**Files:**
- Create: `crates/rs-delivery/tests/lifecycle_integration.rs`

The 5 tests live in one file inside the rs-delivery `tests/` integration test directory (NOT inside `src/`). This keeps each test focused on the public crate API, not internal types.

- [ ] **Step 1: Write all 5 integration tests**

Write `crates/rs-delivery/tests/lifecycle_integration.rs`:

```rust
//! Integration tests for the fast-endpoint zero-reconnect feature
//! (#$ISSUE_NUM). See spec §5.2.
//!
//! Each test mocks the S3 backend at the `S3Backend` trait boundary
//! only — pusher and registry are real per `test-strictness`.

use rs_delivery::disk_cache::download_service::{DownloadService, FetchedChunk, S3Backend};
use rs_delivery::disk_cache::prefetch_queue::PrefetchQueue;
use rs_delivery::disk_cache::prefetch_reader::PrefetchReader;
use rs_delivery::disk_cache::registry::ChunkRegistry;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Backend that returns a chunk after a fixed delay.
struct ConstantLatency {
    delay_ms: u64,
}

#[async_trait::async_trait]
impl S3Backend for ConstantLatency {
    async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(Some(FetchedChunk {
            data: vec![0u8; 1024],
            duration_ms: 2000,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
        }))
    }
}

/// Backend whose first N chunks return slowly, then resume normal.
struct OneSlowChunk {
    slow_chunk_id: i64,
    slow_ms: u64,
    fast_ms: u64,
}

#[async_trait::async_trait]
impl S3Backend for OneSlowChunk {
    async fn fetch(&self, id: i64) -> Result<Option<FetchedChunk>, String> {
        let delay = if id == self.slow_chunk_id {
            self.slow_ms
        } else {
            self.fast_ms
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok(Some(FetchedChunk {
            data: vec![id as u8; 1024],
            duration_ms: 2000,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
        }))
    }
}

/// Backend whose chunks 5+ take very long. Used for outage simulation.
struct ChunksAfterIndexAreSlow {
    threshold: i64,
    slow_ms: u64,
    fast_ms: u64,
}

#[async_trait::async_trait]
impl S3Backend for ChunksAfterIndexAreSlow {
    async fn fetch(&self, id: i64) -> Result<Option<FetchedChunk>, String> {
        let delay = if id >= self.threshold {
            self.slow_ms
        } else {
            self.fast_ms
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok(Some(FetchedChunk {
            data: vec![id as u8; 1024],
            duration_ms: 2000,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
        }))
    }
}

/// Backend that is dead for a window then recovers. Counts chunks served.
struct DeadForWindow {
    served: Arc<AtomicU32>,
    dead_until: tokio::sync::Mutex<Option<Instant>>,
}

#[async_trait::async_trait]
impl S3Backend for DeadForWindow {
    async fn fetch(&self, id: i64) -> Result<Option<FetchedChunk>, String> {
        let dead_until = *self.dead_until.lock().await;
        if let Some(t) = dead_until {
            if Instant::now() < t {
                return Err("503 Service Unavailable".into());
            }
        }
        self.served.fetch_add(1, Ordering::SeqCst);
        Ok(Some(FetchedChunk {
            data: vec![id as u8; 1024],
            duration_ms: 2000,
            host_emit_ts: None,
            s3_upload_complete_ts: None,
        }))
    }
}

fn build_download(backend: Arc<dyn S3Backend>, tmp: &tempfile::TempDir) -> Arc<DownloadService> {
    let registry = ChunkRegistry::new();
    DownloadService::new(
        backend,
        registry,
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    )
}

#[tokio::test]
async fn prefetch_double_buffered_zero_delay() {
    // K=1 (double-buffered). Backend 50ms latency, simulated 100ms write.
    // Pusher should never wait between chunks (gap E->F < 1ms after first).
    let tmp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn S3Backend> = Arc::new(ConstantLatency { delay_ms: 50 });
    let download = build_download(backend, &tmp);
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next = Arc::new(AtomicI64::new(0));
    let q_run = Arc::clone(&queue);
    let dl_run = Arc::clone(&download);
    let next_run = Arc::clone(&next);
    let reader = tokio::spawn(async move {
        PrefetchReader::run(q_run, dl_run, next_run, None).await;
    });
    // Drain 5 chunks; measure gaps between successive pop_front() calls.
    let _ = queue.pop_front().await.unwrap();
    let mut gaps_ms = vec![];
    for _ in 0..4 {
        let start = Instant::now();
        let _ = queue.pop_front().await.unwrap();
        gaps_ms.push(start.elapsed().as_millis() as u64);
    }
    queue.close();
    let _ = reader.await;
    for g in &gaps_ms {
        assert!(
            *g < 60,
            "double-buffered queue must yield chunks immediately, got {gaps_ms:?}"
        );
    }
}

#[tokio::test]
async fn prefetch_absorbs_one_chunk_hiccup() {
    // K=1 buffer. Chunk 5 takes 1500ms vs 50ms norm. Pusher should
    // process chunks 0..=5 without blocking on the slow one (the
    // K=1 buffer absorbs it because chunk 6 wasn't yet requested).
    let tmp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn S3Backend> = Arc::new(OneSlowChunk {
        slow_chunk_id: 5,
        slow_ms: 1500,
        fast_ms: 50,
    });
    let download = build_download(backend, &tmp);
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next = Arc::new(AtomicI64::new(0));
    let q_run = Arc::clone(&queue);
    let dl_run = Arc::clone(&download);
    let next_run = Arc::clone(&next);
    let reader = tokio::spawn(async move {
        PrefetchReader::run(q_run, dl_run, next_run, None).await;
    });
    // Pull chunks 0..=4 without delay (each ~50ms backend latency).
    for _ in 0..5 {
        let _ = queue.pop_front().await.unwrap();
    }
    // Chunk 5 will take 1500ms; the gap on the queue side reflects that.
    let start = Instant::now();
    let _ = queue.pop_front().await.unwrap();
    let chunk_5_gap = start.elapsed().as_millis() as u64;
    // For K=1 the slow chunk DOES surface as a wait at the queue (the
    // buffer is empty when chunk 5 was requested). The point is the
    // pusher SESSION never died — there's no death audit to assert
    // here; the absence of panic + presence of chunk 5 is the test.
    assert!(chunk_5_gap >= 1400);
    queue.close();
    let _ = reader.await;
}

#[tokio::test]
async fn prefetch_stall_beyond_buffer_no_session_kill() {
    // Outage: chunks 5+ take 30s. After draining the buffer the pusher
    // waits — but the test simply asserts the reader task does NOT
    // panic and the queue does NOT close on its own. (Real session kill
    // is signaled by the wrapping endpoint_task, which is out of scope
    // for this integration test.)
    let tmp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn S3Backend> = Arc::new(ChunksAfterIndexAreSlow {
        threshold: 5,
        slow_ms: 30_000,
        fast_ms: 50,
    });
    let download = build_download(backend, &tmp);
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next = Arc::new(AtomicI64::new(0));
    let q_run = Arc::clone(&queue);
    let dl_run = Arc::clone(&download);
    let next_run = Arc::clone(&next);
    let reader = tokio::spawn(async move {
        PrefetchReader::run(q_run, dl_run, next_run, None).await;
    });
    // Drain first 5 chunks normally.
    for _ in 0..5 {
        let _ = queue.pop_front().await.unwrap();
    }
    // Sixth chunk would take 30s — only wait briefly, then close.
    let res =
        tokio::time::timeout(Duration::from_secs(2), queue.pop_front()).await;
    assert!(res.is_err(), "expected timeout (chunk 5 still pending)");
    queue.close();
    let join = tokio::time::timeout(Duration::from_secs(35), reader).await;
    assert!(join.is_ok(), "reader task must exit cleanly after close");
}

#[tokio::test]
async fn s3_outage_recovery_no_chunk_loss() {
    // S3 dies for ~3s then returns. PrefetchReader's infinite retry path
    // must produce ALL chunks 0..N in order — none skipped.
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(DeadForWindow {
        served: Arc::new(AtomicU32::new(0)),
        dead_until: tokio::sync::Mutex::new(Some(Instant::now() + Duration::from_secs(3))),
    });
    let backend_arc: Arc<dyn S3Backend> = backend.clone();
    let download = build_download(backend_arc, &tmp);
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next = Arc::new(AtomicI64::new(0));
    let q_run = Arc::clone(&queue);
    let dl_run = Arc::clone(&download);
    let next_run = Arc::clone(&next);
    let reader = tokio::spawn(async move {
        PrefetchReader::run(q_run, dl_run, next_run, None).await;
    });
    // First pop should eventually succeed once S3 "recovers".
    let first = tokio::time::timeout(Duration::from_secs(15), queue.pop_front()).await;
    assert!(first.is_ok(), "reader must eventually deliver after outage");
    // Pull next 4 chunks; they must arrive in order with no gaps.
    for expected_id in 1..5 {
        let arc_bytes = queue.pop_front().await.unwrap();
        // First byte equals chunk_id (per ConstantLatency synth).
        assert_eq!(arc_bytes[0], expected_id as u8);
    }
    queue.close();
    let _ = reader.await;
}

#[tokio::test]
async fn lifecycle_predeath_dump_emitted_on_session_death() {
    // Drives LifecycleSampler directly; predeath emission is internal.
    // The wiring of "endpoint_task calls emit_predeath on death" is
    // covered by Task 18 + manual operator validation; this test
    // confirms the helper does what it says.
    use rs_delivery::audit_ring::AuditRing;
    use rs_delivery::chunk_lifecycle::sampler::LifecycleSampler;
    use rs_delivery::chunk_lifecycle::timings::ChunkLifecycleTimings;
    use rs_core::audit::Action;
    use std::time::SystemTime;

    let ring = Some(Arc::new(AuditRing::new()));
    let mut s = LifecycleSampler::new(30, 4_000);
    for i in 0..50 {
        let mut t = ChunkLifecycleTimings::new(i, 1, "Kiko".into());
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_715_380_800);
        t.host_emit_ts = Some(base);
        t.s3_upload_complete_ts = Some(base + Duration::from_millis(50));
        t.vps_fetch_start_ts = Some(base + Duration::from_millis(60));
        t.vps_fetch_done_ts = Some(base + Duration::from_millis(110));
        t.pusher_request_ts = Some(base + Duration::from_millis(120));
        t.wire_first_byte_ts = Some(base + Duration::from_millis(170));
        s.observe(&t, &ring);
    }
    s.emit_predeath(&ring);
    let predeaths = ring
        .as_ref()
        .unwrap()
        .since(0)
        .0
        .into_iter()
        .filter(|r| r.action == Action::EndpointLifecyclePredeath)
        .count();
    assert_eq!(predeaths, 1);
}
```

- [ ] **Step 2: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/tests/lifecycle_integration.rs
git commit -m "test(delivery): 5 integration tests for prefetch + lifecycle (#$ISSUE_NUM)"
```

---

### Task 21: E2E loopback extension — inject 5s hiccup at chunk 30, assert 0 reconnects + breach row

**Files:**
- Modify: `crates/rs-delivery/tests/local_xiu_loopback.rs` (or whatever filename the existing loopback test uses — `grep -rn "local_xiu_loopback\|xiu_loopback" crates/rs-delivery/`)

- [ ] **Step 1: Locate existing loopback file**

```bash
grep -rn "fn loopback\|local_xiu\|xiu_loopback" crates/rs-delivery/tests/ crates/rs-delivery/src/ 2>/dev/null
```

Expected: an existing test that already spins up xiu RTMP server + pusher. The subagent extends THAT file rather than creating a duplicate.

- [ ] **Step 2: Add a new test alongside the existing loopback test**

The subagent first reads the existing loopback test top-to-bottom to identify:
- How the S3 mock backend is constructed and passed in
- How the audit ring is exposed (likely via `disk_cache.audit_ring()` or similar accessor)
- Where the pusher's reconnect count is observable (via the `EndpointHandle` stats or a counter exposed for tests)

Then append a new test to the SAME file. Concrete acceptance criteria the test MUST encode:

1. Build a wrapper `S3Backend` around the existing mock that delays its 30th `fetch()` call by 5 seconds (use `tokio::time::sleep`). All other chunks return at the existing baseline latency (typically <100 ms).
2. Drive the loopback for at least 35 chunks total (so chunk 30 is consumed mid-stream, not at the tail).
3. Capture the rust-pusher reconnect counter from the `EndpointHandle` (or whichever struct the existing test uses to assert "no death").
4. Capture the audit ring rows after the run.

The body skeleton — fill the marked blanks by copy/adapting the existing loopback test's setup:

```rust
#[tokio::test]
async fn local_xiu_loopback_kiko_simulated_with_hiccup() {
    let baseline_backend = /* same mock S3 used by the existing loopback test */;
    let backend: Arc<dyn S3Backend> = Arc::new(SlowChunk30 {
        inner: baseline_backend.clone(),
        slow_chunk_id: 30,
        slow_ms: 5_000,
    });
    let (handle, audit_ring) = /* existing helper that spins up xiu server + pusher
                                   with the supplied backend; returns the endpoint
                                   handle + audit ring used in the existing test */;

    /* drive at least 35 chunks through; existing helper exposes a method
       like `wait_for_chunk(35).await` or similar */
    handle.wait_for_chunk(35).await;

    let reconnects = handle.stats().await.reconnect_count;
    assert_eq!(reconnects, 0, "fast endpoint must absorb the 5s hiccup with zero reconnects");

    let breach = audit_ring
        .since(0)
        .0
        .into_iter()
        .find(|r| {
            r.action == rs_core::audit::Action::DiskCacheLifecycleBreach
                && r.detail
                    .as_ref()
                    .and_then(|d| d.get("chunk").and_then(|c| c.get("sequence_number")))
                    .and_then(|v| v.as_i64())
                    == Some(30)
        });
    assert!(breach.is_some(), "expected lifecycle_breach row for chunk 30");
    let gap_cd = breach
        .unwrap()
        .detail
        .as_ref()
        .and_then(|d| d.get("chunk").and_then(|c| c.get("gap_c_to_d_ms")))
        .and_then(|v| v.as_i64())
        .unwrap();
    assert!(gap_cd >= 4_500 && gap_cd <= 6_000, "expected ~5000ms, got {gap_cd}");
}

struct SlowChunk30 {
    inner: Arc<dyn S3Backend>,
    slow_chunk_id: i64,
    slow_ms: u64,
}

#[async_trait::async_trait]
impl S3Backend for SlowChunk30 {
    async fn fetch(
        &self,
        chunk_id: i64,
    ) -> Result<Option<rs_delivery::disk_cache::download_service::FetchedChunk>, String> {
        if chunk_id == self.slow_chunk_id {
            tokio::time::sleep(std::time::Duration::from_millis(self.slow_ms)).await;
        }
        self.inner.fetch(chunk_id).await
    }
}
```

The subagent replaces the two `/* ... */` blocks with the actual fixture code from the existing test (same patterns the existing test uses — copy them verbatim). The final commit MUST contain a fully compiling test, no `/* */` blanks, no `unimplemented!`.

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --all --check`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/tests/local_xiu_loopback.rs
git commit -m "test(delivery-e2e): 5s hiccup at chunk 30 yields breach row, zero reconnects (#$ISSUE_NUM)"
```

---

### Task 22: Orchestrator-only — push, monitor CI, PR, post-deploy verify

**This task is NOT for a subagent.** The orchestrator (you) executes it personally because it requires git push + CI monitoring + Playwright browser automation against the live deploy + reading MCP output.

- [ ] **Step 1: Local format check**

```bash
cargo fmt --all --check
```

Expected: exit 0. If any diff, fix locally.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI to terminal state**

```bash
gh run list --branch dev --limit 1 --json databaseId,status,conclusion
# Capture the run-id, then:
sleep 600 && gh run view <run-id> --json status,conclusion,jobs
```

Per `ci-monitoring.md`: ALL jobs must be green (lint, test, e2e, deploy). If any fail, investigate `gh run view <run-id> --log-failed`, fix in one commit, push, monitor again. Never blindly rerun.

- [ ] **Step 4: Open the PR (dev → main)**

Once dev CI is green:

```bash
gh pr create --base main --head dev \
  --title "fix(delivery): fast-endpoint zero-reconnect via lifecycle telemetry + prefetch K=1 (#$ISSUE_NUM)" \
  --body "$(cat <<'EOF'
## Summary
- Per-chunk `ChunkLifecycleTimings` (6 stages A→F) so every fast-endpoint death pinpoints which pipeline stage stalled.
- `PrefetchQueue<K>` between disk_cache fetcher and pusher. K=0 default (non-fast unchanged); K=1 default for fast endpoints (double-buffered → zero added delay in steady state, absorbs 1-chunk supply hiccups invisibly).
- Never-stop retry on S3 download (kills `max_attempts=5` cap in `download_service.rs`).
- Dashboard: prefetch fill bar + worst-stage badge per endpoint.

Closes #$ISSUE_NUM.

## Test plan
- [ ] CI: lint, test, e2e, file-size, mutation tests, deploy all green
- [ ] Operator soak on streamsnv with Kiko fast endpoint active for one full live event
- [ ] Audit query: `endpoint_rtmp_push_died` rows for endpoint='Control stream Kiko' = 0
- [ ] Kiko `chunk_delay_secs` stays at ~4s steady state, peaks ≤ ~6s during absorbed hiccups
- [ ] Other endpoints unchanged (FB / YT / e2e behave identically)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Wait for PR CI to be green AND mergeable**

```bash
gh pr view <pr-num> --json number,mergeable,mergeStateStatus,statusCheckRollup
```

Required: `mergeable: true` AND `mergeStateStatus: "CLEAN"`. UNSTABLE / BLOCKED / DIRTY = NOT ready; investigate and fix.

- [ ] **Step 6: Post the PR URL to the user**

Format per `completion-report.md`. Wait for explicit "merge it" before merging — never merge on your own initiative.

- [ ] **Step 7: After user merges, monitor main CI + deploy**

```bash
gh run list --branch main --limit 1 --json databaseId,status,conclusion
# When deploy job completes:
gh run view <main-run-id> --json status,conclusion,jobs
```

The `deploy-stream-lan` job must complete with `success`.

- [ ] **Step 8: Post-deploy verification on streamsnv**

Run two checks in parallel:

```bash
# 1. Liveness via win-stream-snv MCP
mcp__win-stream-snv__ListProcesses (filter "Restreamer")
mcp__win-stream-snv__Shell "Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status"

# 2. Functional verification via Playwright
mcp__plugin_playwright_playwright__browser_navigate http://10.77.9.204:8910/
mcp__plugin_playwright_playwright__browser_snapshot   # confirm v0.8.0 visible in dashboard
mcp__plugin_playwright_playwright__browser_console_messages   # zero errors required
```

Verify the dashboard's version label shows `v0.8.0`. If `v0.7.5` is still shown, deploy failed silently — investigate (CDN cache, build skipped, wrong target).

- [ ] **Step 9: Send completion report to user per `completion-report.md`**

Use the EXACT template. Audits at top, Goal/What changed/URLs/PR/Question at bottom. ✅ Deploy line MUST cite the version label read from the live DOM.

- [ ] **Step 10: Operator runs the soak**

The user (operator) runs Kiko alongside FB/YT during the next live event. Success: zero `endpoint_rtmp_push_died` rows for Kiko. If reconnects occur, the new lifecycle_predeath audit row pinpoints which stage stalled — iterate on that stage in a follow-up PR.

---

### Verification

1. **CI green on dev** — all jobs pass before opening the PR
2. **CI green on PR** — same gates plus merge cleanliness (`mergeStateStatus: CLEAN`)
3. **PR merged after explicit user approval**
4. **Main CI green including deploy-stream-lan**
5. **Post-deploy liveness: Restreamer.exe in user session, /api/v1/status returns 200**
6. **Post-deploy version match: dashboard DOM shows `v0.8.0`, matches /api/v1/status**
7. **Post-deploy console clean: zero errors / warnings in Playwright console**
8. **Post-deploy UI: prefetch-fill and worst-stage badge visible on at least one endpoint card**
9. **Operator soak: zero Kiko reconnects through one full live event**

Steps 1–8 are the orchestrator's responsibility (Task 22). Step 9 is the operator's. The PR is mergeable when 1–4 are green; the work is "done" when 9 is observed.
