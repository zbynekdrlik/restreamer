# Robust S3 Chunk Upload — Design

**Date:** 2026-04-14
**Closes:** #118 (throughput ~0.5 chunks/s), #65 (robust + observable uploads)

## Problem

Observed in PR #105 CI and on stream.lan live stream:
- E2E "Simulated network disconnect" drain phase: 0.47 chunks/s effective upload
- E2E Streaming Test end-of-stream: 378 chunks still pending
- Operator observation (live): per-endpoint cache bar climbing, no visibility into why

Verified corrections to initial hypothesis:
- `rust-s3 0.35` uses **simple PUT** for files < 8 MiB (`bucket.rs:67`, `CHUNK_SIZE = 8_388_608`).
  Our 100 KB chunks are NOT going through multipart. Multipart is NOT the bottleneck.

## Root cause

`crates/rs-endpoint/src/uploader.rs` current behavior:

1. **Retry sleeps occupy worker slots.** Lines 140–171 run `tokio::time::sleep(backoff)` *inside* the semaphore-held task. Exponential backoff up to 30 s × 10 attempts means one slow chunk can hold a permit for ~1 min. With `max_concurrent = 4`, four simultaneous failures starve the pool. This alone explains 0.47 chunks/s after the resilience test's "unblock" phase — all four workers were asleep in backoff inherited from the blocked phase.

2. **Batch-of-20 wave synchronization.** `upload_batch` calls `get_unsent_chunks(&pool, 20)`, spawns up to 4 concurrent tasks, then `handle.await`s every handle before returning and sleeping 500 ms. The slowest of 20 chunks gates the next DB poll.

3. **`max_concurrent = 4` is too low** for the 20 chunks/s target.

4. **No telemetry.** Operators see `pending_chunks` grow, nothing about why. Debugging requires log-diving on stream.lan.

## Goals

From #118:
- Steady-state ≥ 20 chunks/s sustained on healthy network (stream.lan → nbg1, ~22 ms RTT)
- E2E "Simulated network disconnect" passes reliably (no pending > 5 after unblock + 60 s) across 5 consecutive CI runs
- E2E Streaming Test "Wait for final chunk upload" ends with pending ≤ 5

From #65:
- Multiple chunks uploaded in parallel (saturate bandwidth)
- First-try success on healthy internet
- Dashboard shows live upload rate + per-chunk debug info (no black box)

## Architecture

Five cooperating pieces. Each is independently testable.

### 1. DB schema — Migration V17

Add to the `chunks` table:

```sql
ALTER TABLE chunks ADD COLUMN upload_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chunks ADD COLUMN upload_first_attempt_at INTEGER;  -- ms epoch
ALTER TABLE chunks ADD COLUMN upload_completed_at INTEGER;      -- ms epoch
ALTER TABLE chunks ADD COLUMN upload_duration_ms INTEGER;       -- last attempt wall-clock
ALTER TABLE chunks ADD COLUMN upload_last_error TEXT;
ALTER TABLE chunks ADD COLUMN upload_next_retry_at INTEGER;     -- ms epoch, NULL = eligible now
ALTER TABLE chunks ADD COLUMN upload_failed_permanently INTEGER NOT NULL DEFAULT 0;
```

Reason: persisting to the existing `chunks` row (instead of a separate event log) gives one-row-per-chunk state, survives ffmpeg restarts, is cheap to index on `(sent, upload_next_retry_at)`, and matches the user preference captured during brainstorming.

Index for fast picker queries:

```sql
CREATE INDEX idx_chunks_upload_queue
  ON chunks(sent, upload_failed_permanently, upload_next_retry_at, id)
  WHERE sent = 0 AND upload_failed_permanently = 0;
```

### 2. Continuous worker pool (rs-endpoint/src/uploader.rs)

Replace batch-of-20 wave with a **continuous worker pool**:

- N worker tasks spawned at init, each loops forever:
  1. `pick_next_uploadable_chunk(pool)` — SQL: oldest `chunks` where `sent=0 AND in_process=0 AND upload_failed_permanently=0 AND (upload_next_retry_at IS NULL OR upload_next_retry_at <= now_ms)`
  2. Claim row (`UPDATE ... SET in_process=1 WHERE id=? AND in_process=0`), skip if lost race.
  3. Record `upload_first_attempt_at` if NULL, increment `upload_attempts`.
  4. Do the PUT. Measure wall-clock. On success: `set_chunk_sent`, set `upload_completed_at`, `upload_duration_ms`, clear error.
  5. On failure: write `upload_last_error`, compute `upload_next_retry_at = now_ms + backoff(attempts)`, clear `in_process`. **Do NOT sleep in worker.** Worker loops back and picks next eligible chunk.
  6. If `attempts >= MAX_ATTEMPTS (10)` OR `now_ms - upload_first_attempt_at >= MAX_WALL_CLOCK (600_000 ms)`: set `upload_failed_permanently=1`, emit WsEvent::ChunkUploadFailed for dashboard alert.

Between picker attempts when queue is empty: `tokio::time::sleep(100ms)`.

Backoff formula: `min(1s * 2^(attempt-1), 30s)`. Unchanged from current — but it no longer blocks other chunks.

### 3. Adaptive concurrency controller

Separate tokio task, 10 s tick:

```text
every 10s:
  compute over last 10s:
    successes, failures, sum_duration_ms
  error_rate = failures / (successes + failures)
  median_ms = (best-effort quantile over in-memory ring buffer)
  if error_rate == 0 AND median_ms < 500 AND target < MAX_CONCURRENCY (32):
    target = min(target * 2, 32)
  else if error_rate > 0.2 AND target > MIN_CONCURRENCY (4):
    target = max(target / 2, 4)
  spawn/signal-shutdown workers to match target
```

Worker shutdown: each worker checks a shared `should_stop` flag (`AtomicUsize` count of workers to wind down) at the top of its loop.

Starting target: 4 (same as today). Scale only upward when error-free, so we never degrade a live stream.

### 4. API endpoints (rs-api)

- `GET /api/v1/uploads/stats` — aggregate snapshot:
  ```json
  {
    "chunks_per_sec_1m": 12.4,
    "median_ms": 180,
    "p95_ms": 540,
    "in_flight": 7,
    "adaptive_target": 16,
    "error_rate_1m": 0.0,
    "backlog_pending": 3,
    "backlog_failed": 0
  }
  ```
  Values computed from a shared `UploadMetrics` struct updated by workers.

- `GET /api/v1/uploads/recent?limit=200` — last-N chunks, newest first:
  ```json
  [
    {
      "chunk_id": 1234,
      "event_id": "sunday-service-2026",
      "sequence_number": 42,
      "size_bytes": 102400,
      "attempts": 1,
      "duration_ms": 180,
      "status": "sent" | "pending" | "retrying" | "failed",
      "last_error": null | "timeout",
      "first_attempt_at": 1735829023000,
      "completed_at": 1735829023180
    }
  ]
  ```

### 5. Frontend (leptos-ui)

**Inline strip** on `operator_dashboard.rs`, under the S3 cache bar:

```text
Upload: 12.4 c/s · median 180ms · in-flight 7/16 · errors 0% · failed 0  [click for detail]
```

Colors: green when chunks_per_sec ≥ production rate (~0.5 c/s) AND error_rate = 0; yellow when error_rate > 0; red when backlog growing or any `failed_permanently`.

**Drill-down page** `leptos-ui/src/pages/uploads.rs` (new route `/uploads`):

- Table: chunk_id | event | seq | size | attempts | duration | status | last_error
- Live-updates via WS subscription to `ChunkUploaded`, `ChunkUploadAttempt`, `ChunkUploadFailed`
- Filter: "errors only", "pending only"
- Sort: newest first (default), slowest first

## Data flow

```text
┌────────────┐  ┌──────────────────┐  ┌───────────┐
│ Chunkerizer│─▶│ chunks table DB  │◀─│ Worker N  │
└────────────┘  │ (sent=0 queue)   │  │ (picker)  │
                └─────────▲────────┘  └─────┬─────┘
                          │                 │ PUT
                          │                 ▼
                          │           ┌──────────┐
                          └───────────│ Hetzner  │
                           updates    │ S3 nbg1  │
                           (attempts, └──────────┘
                            duration,
                            error)

Adaptive controller ticks every 10s → resizes worker pool.
Metrics aggregator maintains ring buffer → feeds /api/v1/uploads/stats.
WS events: ChunkUploadAttempt, ChunkUploaded, ChunkUploadFailed → dashboard.
```

## Error handling

- **Transient S3 error**: record, backoff via `upload_next_retry_at`, release worker. Another worker picks it up after backoff elapses.
- **Permanent S3 error (4xx)**: no retry value in re-trying auth/acl errors; after first 4xx that is not 5xx/timeout, set `upload_failed_permanently=1` immediately.
- **DB error on picker**: log, sleep 1s, retry. DB locked briefly during migrations is normal.
- **Worker panic**: top-level `spawn` catches via `JoinError`; adaptive controller respawns on next tick.
- **Migration failure**: startup aborts (`rs-core::db::run_migrations` already does this).

## Test plan

### Unit (crates/rs-endpoint)

- `pick_next_uploadable_chunk` respects `upload_next_retry_at`, `upload_failed_permanently`, oldest-first ordering.
- Backoff formula: monotonic increase, capped at 30s.
- Adaptive controller state machine: scale up only on zero errors + fast median; scale down on > 20% errors; bounded [4, 32].
- Metrics aggregator: ring buffer windowing, percentile computation.

### Integration (crates/rs-endpoint/tests/uploader_integration.rs)

Extend existing MinIO-backed test:

- Induced failure every 3rd chunk → verify requeue, eventual success, telemetry columns populated.
- 50 chunks queued → verify ≥ 10 concurrent in-flight after warm-up (adaptive scale-up).
- Simulated permanent failure (MinIO 403) → chunk marked `upload_failed_permanently=1` within N attempts, WS event emitted.
- Worker panics → pool self-heals on next adaptive tick.

### E2E (Playwright, e2e/uploads.spec.ts)

- Start stream → `/uploads` page loads → table populates → click "errors only" filter works.
- Operator dashboard strip shows live rate > 0 during stream.
- Console has zero errors/warnings during interaction.

### Microbench (not in CI)

`crates/rs-endpoint/src/bin/bench_s3_upload.rs`:

- Reads `S3_*` env vars (same as stream.lan config).
- Uploads 200 × 100 KB random blobs with configurable concurrency (arg).
- Reports chunks/s, MB/s, p50/p95 latency, error count.
- Run manually from stream.lan to confirm ≥ 20 chunks/s achievable.

### CI gate

The existing "Simulated network disconnect" E2E stays. Add assertion:

- After unblock + 60 s: pending ≤ 5 (was: not checked, only "decreased")
- Final Streaming Test: pending ≤ 5 at end of 10-min stream (was: 378 observed)

Both assertions must pass 5 runs in a row before this work is merged.

## Scope discipline

Out of scope for this PR (separate issues if needed):

- Rewriting rust-s3 — we stay on 0.35, proven sufficient.
- Per-endpoint cache bar changes — this is an uploader fix, not a visualization refactor.
- S3 upload-rate telemetry in metrics exporter (Prometheus/etc.) — not needed yet.
- Multi-region S3 failover — YAGNI.
- Changing retry backoff formula — the *blocking* is the bug, not the formula.

## File map

```
Create:
  leptos-ui/src/pages/uploads.rs
  crates/rs-endpoint/src/bin/bench_s3_upload.rs
  e2e/tests/uploads.spec.ts

Modify:
  crates/rs-core/src/db/migrations/mod.rs           (V17 registration)
  crates/rs-core/src/db/migrations/v017_upload_telemetry.sql  (new)
  crates/rs-core/src/models.rs                      (Chunk struct + upload cols, UploadStats, UploadChunkRow)
  crates/rs-core/src/db.rs                          (pick_next_uploadable_chunk, record_* fns)
  crates/rs-endpoint/src/uploader.rs                (full refactor)
  crates/rs-endpoint/src/metrics.rs                 (new module, re-exported)
  crates/rs-endpoint/tests/uploader_integration.rs  (extend)
  crates/rs-api/src/routes.rs                       (new endpoints)
  crates/rs-api/src/uploads_endpoints.rs            (new file)
  leptos-ui/src/components/operator_dashboard.rs    (inline strip)
  leptos-ui/src/router.rs                           (/uploads route)
  leptos-ui/src/api.rs                              (fetchUploadStats, fetchUploads)
  Cargo.toml                                        (version bump 0.3.53 → 0.3.54)
  src-tauri/Cargo.toml
  src-tauri/tauri.conf.json
  leptos-ui/Cargo.toml
  .github/workflows/ci.yml                          (tighten pending assertion)
```

## Acceptance checklist

- [ ] Migration V17 applied cleanly on fresh install AND upgrade from V16
- [ ] Unit tests: picker, backoff, adaptive controller, metrics — all pass
- [ ] Integration tests: induced-failure requeue, concurrency scale-up, permanent-failure — all pass
- [ ] E2E `uploads.spec.ts` passes; zero console errors
- [ ] E2E "Simulated network disconnect" passes 5 runs in a row, pending ≤ 5 at end
- [ ] E2E Streaming Test ends with pending ≤ 5
- [ ] `GET /api/v1/uploads/stats` returns live-updating values during stream
- [ ] Operator dashboard inline strip visible + updates live
- [ ] `/uploads` page renders, filters work, updates live
- [ ] Microbench ≥ 20 chunks/s from stream.lan → nbg1 (manual, recorded in PR)
- [ ] CI green (all jobs)
- [ ] Deploy to stream.lan verified via Playwright: strip visible + rate > 0 during live stream
