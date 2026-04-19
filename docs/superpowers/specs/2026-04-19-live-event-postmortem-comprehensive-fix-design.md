# 2026-04-19 Live-Event Post-Mortem — Comprehensive Fix Design

## Context

The 2026-04-19 Sunday-service live event failed. Audience on 5 of 6 endpoints saw nothing. Operator switched to OBS multistream plugin fallback.

Forensic findings (backed by SQLite `delivery_restart_log.stderr_tail` + `delivery_endpoint_status` + `C:\ProgramData\Restreamer\restreamer.log`):

1. **No persisted audit log.** `WsEvent::ActivityFeed` is emitted by backend but dashboard ignores it (`leptos-ui/src/ws.rs:318-320`, regressed in commit `60289c1` on 2026-03-29). Never persisted to DB. Zero post-mortem evidence of what happened during the 4-hour event.
2. **`StartPosition::Live` is broken** — `crates/rs-api/src/delivery_endpoints.rs:45-50` calls `get_first_sequence_number_for_event` instead of `get_latest_sequence_number_for_event`. Identical body to `Beginning`. Every mid-event endpoint add starts at chunk 1 (3-hour delay). Dashboard default is `"Live"`.
3. **ffmpeg death stderr invisible to operator.** 16 restart rows captured today in `delivery_restart_log` with `stderr_tail` containing `Error submitting a packet to the muxer: Broken pipe` (YouTube RTMP) and `TLS fatal alert … session has been invalidated` (Facebook RTMPS) — but `restart_history: []` in `/api/v1/delivery/status` response and nothing surfaced in dashboard.
4. **Aggressive RTMP(S) reconnect backoff** — `1s→2s→4s→8s→16s→32s`. YouTube and Facebook treat rapid reconnects as duplicate sessions and reject. Today wave-3 FB restarts all failed with `lifetime_secs=0`.
5. **2-hour dead zone during live event** (08:12–10:09 UTC) because operator removed all endpoints one-by-one with no warning.
6. **VPS logs only captured on instance delete** — `delivery_logs` has 0 rows for live instance 654 running 4+ hours.
7. **No per-endpoint metrics time-series** — `delivery_endpoint_status` has 1 row/alias, overwritten each poll.
8. **25,366 SQLite BUSY errors in 5h** — uploader workers thrashing same rows, burying real signal.
9. **"Start delivery" can be clicked before RTMP stable** — today triggered 2 aborted Hetzner VPS provisions at 07:00 and 07:02.
10. **CI `deploy-stream-lan` runs on every dev push** with no live-event gate — a push during live window would restart production mid-service.

---

## Goals

- Comprehensive one-shot fix in a single PR (version 0.3.66).
- Deterministic post-mortem evidence via persistent audit log + metrics time-series.
- Fail-safe against operator mis-steps (zero-endpoints, unstable-RTMP start) and CI mis-steps (deploy-during-live).
- No regression to the existing RTMP ingest path (which was rock-solid today — 7,561 chunks, 100% uploaded).

## Non-goals

- Investigating *why* YouTube/Facebook dropped their connections today (network path / remote-side). That's separate follow-up.
- Moving away from ffmpeg per-endpoint (pure-Rust RTMP push is tracked in #103).
- Rewriting the dashboard beyond the panels listed below.

---

## Architecture Overview

Two new subsystems form the observability backbone, plus eight targeted fixes:

- **Subsystem A — `audit_log`**: unified persistent event log with typed-enum write API, WebSocket real-time broadcast, REST query API, dashboard live panel, event-timeline view.
- **Subsystem B — `delivery_endpoint_metrics`**: per-endpoint time-series sampled every ~6 s, 7-day retention, endpoint-card sparkline.
- **Targeted fixes C–J**: `StartPosition::Live` correction, stderr reason classification + surfacing, remote-close-aware reconnect backoff, remove-last-endpoint guard, continuous VPS log mirror, SQLite WAL + worker dedup, RTMP-stable gate before delivery start, CI deploy gate on live event.

All ship as `restreamer-v0.3.66` in one PR.

---

## Data Model

### Migration V17 — `audit_log`

```sql
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    severity    TEXT    NOT NULL,            -- 'info' | 'warn' | 'error' | 'critical'
    source      TEXT    NOT NULL,            -- 'operator' | 'inpoint' | 'uploader' | 'delivery' | 'vps' | 'ffmpeg' | 's3' | 'system'
    event_id    INTEGER,
    instance_id INTEGER,
    endpoint    TEXT,
    action      TEXT    NOT NULL,            -- typed kind, e.g. 'endpoint_ffmpeg_died'
    detail      TEXT    NOT NULL DEFAULT '{}' -- JSON blob, action-specific fields
);
CREATE INDEX IF NOT EXISTS idx_audit_ts    ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_sev   ON audit_log(severity, ts DESC);
```

Rotation: nightly 02:00 UTC task deletes rows older than 90 days. Negligible size — a full event produces ~300 rows × ~200 bytes ≈ 60 KB.

### Migration V18 — `delivery_endpoint_metrics` + cursor column

```sql
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
CREATE INDEX IF NOT EXISTS idx_dem_event_alias ON delivery_endpoint_metrics(event_id, alias, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_dem_ts          ON delivery_endpoint_metrics(ts_ms DESC);

ALTER TABLE delivery_instances ADD COLUMN last_audit_cursor INTEGER NOT NULL DEFAULT 0;
```

Rotation: nightly 02:00 UTC task deletes rows `ts_ms < now() - 7 days`.

Both migrations are idempotent — `CREATE TABLE IF NOT EXISTS` + guarded `ALTER TABLE ADD COLUMN` pattern already used in V13 (#112 fix).

---

## Subsystem A — Audit Log

### Typed write API (`crates/rs-core/src/audit.rs`)

```rust
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity { Info, Warn, Error, Critical }

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Operator, Inpoint, Uploader, Delivery, Vps, Ffmpeg, S3, System,
}

/// Strongly-typed action. Adding a variant forces every call site to
/// consider whether to emit it — prevents string-typo silent bugs.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    // operator
    EventStarted, EventStopped,
    DeliveryStarted, DeliveryStopped,
    EndpointAdded, EndpointRemoved,
    S3Cleared, ConfigChanged,
    // inpoint
    RtmpConnected, RtmpDisconnected, RtmpHandshakeFailed,
    // delivery (host side)
    VpsCreating, VpsReady, VpsDeleted, VpsUnreachable,
    DeliveryInitSent, DeliveryInitResponse,
    // vps (mirrored from rs-delivery)
    EndpointStarted, EndpointAliveTransition,
    EndpointFfmpegDied, EndpointFfmpegRestartFailed,
    // uploader
    S3UploadFailed, S3FetchFailed,
    // system
    RestreamerStarted, MigrationsApplied,
}

pub fn record(
    tx: &mpsc::Sender<AuditRow>,
    severity: Severity,
    source: Source,
    event_id: Option<i64>,
    instance_id: Option<i64>,
    endpoint: Option<&str>,
    action: Action,
    detail: serde_json::Value,
);
```

`record()` synchronously pushes into a bounded `mpsc::channel(1024)`. On channel full, oldest `info` rows are dropped; `warn` and up are always kept.

### Writer task

One dedicated `tokio::spawn`ed task per process, started at service init:

```rust
pub async fn audit_writer_task(
    pool: SqlitePool,
    ws_tx: broadcast::Sender<WsEvent>,
    mut rx: mpsc::Receiver<AuditRow>,
);
```

Receives rows, INSERTs into `audit_log`, broadcasts `WsEvent::AuditAppended { row }`. Batches up to 32 rows per transaction for burst bursts but flushes within 100 ms of first arrival to keep live-feed lag low.

### Rate-limiting

`S3UploadFailed`, `S3FetchFailed`, and `VpsUnreachable` are emitted from tight loops. Record site wraps them in a per-error-class `LastEmittedAt` `DashMap` keyed by `(action, detail["class"])`; emits at most 1 row per minute per class.

### WebSocket event

```rust
WsEvent::AuditAppended {
    id: i64,
    ts: String,
    severity: String,
    source: String,
    event_id: Option<i64>,
    instance_id: Option<i64>,
    endpoint: Option<String>,
    action: String,
    detail: serde_json::Value,
}
```

Existing `WsEvent::ActivityFeed` is kept for back-compat (still emitted for the operator-facing endpoint-alive transitions) but is no longer the canonical source. Dashboard consumes `AuditAppended` as the primary stream.

### REST API

```
GET /api/v1/audit?event_id=X&since=ISO&severity=warn,error&source=delivery&endpoint=YT+NLW&limit=200&offset=0
```
Returns newest-first rows. All query params optional.

```
GET /api/v1/audit/{id}
```
Single row with untruncated `detail`.

### Call sites (complete list)

**operator source** (in `stream_handlers.rs`, `delivery_handlers.rs`, `delivery_endpoints.rs`, `s3_handlers.rs`, `handlers.rs`):
- `event_started`: `{ event_id, event_name }` on `POST /api/v1/stream/start-stream`
- `event_stopped`: `{ event_id, event_name, duration_secs }` on `POST /api/v1/stream/stop-stream`
- `delivery_started`: `{ event_id, instance_id, ipv4 }` on `POST /api/v1/delivery/start`
- `delivery_stopped`: `{ event_id, instance_id, reason }` on `POST /api/v1/delivery/stop`
- `endpoint_added`: `{ event_id, endpoint, start_position, resolved_start_chunk_id }`
- `endpoint_removed`: `{ event_id, endpoint, was_last_endpoint }`
- `s3_cleared`: `{ event_id, event_name, chunks_deleted }`
- `config_changed`: `{ patched_fields: [...] }`

**inpoint source** (in `rs-inpoint`, hooked to xiu events):
- `rtmp_connected`: `{ client_addr, stream_name, app_name }`
- `rtmp_disconnected`: `{ client_addr, stream_name, duration_secs }`
- `rtmp_handshake_failed`: `{ error }`

**delivery source** (in `crates/rs-api/src/delivery.rs`):
- `vps_creating`: `{ hetzner_id, server_type, datacenter }`
- `vps_ready`: `{ hetzner_id, ipv4, boot_secs }`
- `vps_deleted`: `{ hetzner_id, ipv4, reason }`
- `vps_unreachable`: `{ consecutive_failures, last_error }` (rate-limited)
- `delivery_init_sent`: `{ event_id, endpoints_count, start_chunk_id }`
- `delivery_init_response`: `{ event_id, endpoints_started, status }`

**vps source** (mirrored from rs-delivery — see "Continuous VPS log capture" below):
- `endpoint_started`: `{ alias, start_chunk_id }`
- `endpoint_alive_transition`: `{ alias, was_alive, is_alive, current_chunk_id, chunk_delay_secs }`
- `endpoint_ffmpeg_died`: `{ alias, chunk_id, lifetime_secs, reason_class, stderr_last_error_line, backoff_secs }`
- `endpoint_ffmpeg_restart_failed`: `{ alias, attempt, backoff_secs, reason_class }`

**uploader source** (in `rs-endpoint`):
- `s3_upload_failed`: `{ chunk_id, error_class, error_msg }` (rate-limited)
- `s3_fetch_failed`: `{ chunk_id, error_class, error_msg }` (rate-limited)

**system source** (in `rs-service` or startup code):
- `restreamer_started`: `{ version, config_path }`
- `migrations_applied`: `{ from_version, to_version }`

---

## Subsystem B — Metrics Time-Series

`crates/rs-api/src/lib.rs::delivery_broadcast_loop` polls every 2s today. Every 3rd tick (≈6s), INSERT one row per endpoint into `delivery_endpoint_metrics`:

```rust
sqlx::query(
    "INSERT INTO delivery_endpoint_metrics
       (ts_ms, instance_id, event_id, alias, alive, current_chunk_id,
        chunks_processed, chunk_delay_secs, bytes_processed_total,
        ffmpeg_restart_count, delivery_mode)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
).bind(now_ms).bind(instance_id).bind(event_id).bind(&m.alias)
 .bind(m.alive as i64).bind(m.current_chunk_id).bind(m.chunks_processed as i64)
 .bind(m.chunk_delay_secs).bind(m.bytes_processed_total as i64)
 .bind(m.ffmpeg_restart_count as i64).bind(m.delivery_mode.as_deref())
 .execute(&pool).await?;
```

Rotation task (in `rs-service` startup):
```rust
tokio::spawn(async move {
    loop {
        sleep_until(next_02_utc()).await;
        let cutoff = now_ms() - 7 * 86_400_000;
        sqlx::query("DELETE FROM delivery_endpoint_metrics WHERE ts_ms < ?1")
            .bind(cutoff).execute(&pool).await.ok();
        sqlx::query("DELETE FROM audit_log WHERE ts < datetime('now', '-90 days')")
            .execute(&pool).await.ok();
    }
});
```

REST API:
```
GET /api/v1/delivery/metrics?event_id=X&alias=Y&since_ms=Z&until_ms=W&limit=2000
```
Returns chronological samples. Default `since_ms = now - 1h`.

---

## Fix C — `StartPosition::Live`

`crates/rs-api/src/delivery_endpoints.rs:45-50` currently:
```rust
StartPosition::Live => {
    let first_seq = db::get_first_sequence_number_for_event(pool, event_id)
        .await?
        .unwrap_or(1);
    Ok(first_seq)
}
```

Change to:
```rust
StartPosition::Live => {
    let last_seq = db::get_latest_sequence_number_for_event(pool, event_id)
        .await?
        .unwrap_or(1);
    Ok(last_seq)
}
```

Doc-comment on `resolve_start_chunk_id` rewritten: "For `Live` returns latest sequence number; for `Beginning` returns first; for `Resume` passes through."

Unit test (new, in `delivery_endpoints.rs` `#[cfg(test)]` module):
```rust
#[tokio::test]
async fn start_position_live_returns_latest_sequence_not_first() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::create_streaming_event(&pool, "t").await.unwrap();
    // insert chunks 1..=100
    for seq in 1..=100 {
        db::insert_chunk(&pool, event_id, format!("c{seq}.bin"), 1024)
            .await.unwrap();
        // manually set sequence_number via UPDATE
    }
    let resolved = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live)
        .await.unwrap();
    assert_eq!(resolved, 100, "Live must resolve to latest sequence");
    let resolved_beg = resolve_start_chunk_id(&pool, event_id, &StartPosition::Beginning)
        .await.unwrap();
    assert_eq!(resolved_beg, 1, "Beginning must resolve to first sequence");
    assert_ne!(resolved, resolved_beg, "Live and Beginning must differ");
}
```

---

## Fix D — ffmpeg stderr reason classification + surfacing

### New module `crates/rs-delivery/src/ffmpeg_reason.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

/// Classify the last ~2 KB of ffmpeg stderr into a reason class.
/// `service_type` is one of "YT_RTMP" | "YT_HLS" | "FB" | "CUSTOM_RTMP" | …
pub fn classify(service_type: &str, stderr_tail: &str) -> ReasonClass {
    let tail = stderr_tail.rsplit('\n').take(20).collect::<Vec<_>>().join("\n");

    if tail.contains("TLS fatal alert") || tail.contains("session has been invalidated") {
        return ReasonClass::FacebookTlsInvalidated;
    }
    if tail.contains("Error submitting a packet to the muxer: Broken pipe")
        || tail.contains("IO error: Broken pipe") {
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
    if tail.contains("rs-delivery: killed") { return ReasonClass::ProcessKilled; }
    ReasonClass::Unknown
}

/// Minimum wait before next restart attempt, given reason and consecutive failure count.
pub fn reconnect_floor(class: ReasonClass, consecutive: u32) -> Duration {
    use ReasonClass::*;
    match class {
        ProcessKilled => Duration::from_secs(u64::MAX), // never restart; caller suppresses
        YoutubeRtmpClosed | FacebookTlsInvalidated | RemoteBrokenPipe => {
            let secs = 30u64.saturating_mul(2u64.saturating_pow(consecutive));
            Duration::from_secs(secs.min(300)) // cap 5 min
        }
        NetworkTimeout => Duration::from_secs(10),
        InvalidInput => Duration::from_secs(1),
        S3FetchError => Duration::from_secs(5),
        Unknown => Duration::from_secs(15),
    }
}

/// Extract the single most-useful line from the stderr tail for display.
/// Skips ffmpeg progress lines (`size=... time=... bitrate=...`) and the
/// banner; returns the last error-looking line.
pub fn pick_last_error_line(stderr_tail: &str) -> Option<String> { ... }
```

Unit tests fixture-driven: `crates/rs-delivery/tests/ffmpeg_reason_fixtures/` contains the 16 real stderr captures from today's `delivery_restart_log` (checked in), each with expected classification.

### Integration into restart loop

`crates/rs-delivery/src/endpoint_task.rs` currently uses a hardcoded exponential backoff. Replace with:
```rust
let class = ffmpeg_reason::classify(&ep.service_type, &stderr_tail);
let floor = ffmpeg_reason::reconnect_floor(class, consecutive_failures);
// also write audit:
record_audit(Severity::Error, Source::Ffmpeg, Action::EndpointFfmpegDied, json!({
    "alias": ep.alias,
    "chunk_id": last_chunk_id,
    "lifetime_secs": lifetime.as_secs(),
    "reason_class": class,
    "stderr_last_error_line": ffmpeg_reason::pick_last_error_line(&stderr_tail),
    "backoff_secs": floor.as_secs(),
}));
tokio::time::sleep(floor).await;
```

### `/api/v1/delivery/status` exposes `restart_history`

The existing response field `restart_history: []` is wired to an empty Vec. Change `poll_delivery_metrics` in `rs-api/src/delivery.rs` to read the last 10 rows from `delivery_restart_log` for this instance and populate:
```rust
let restarts: Vec<RestartEntry> = sqlx::query_as(
    "SELECT alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs
     FROM delivery_restart_log
     WHERE instance_id = ?1
     ORDER BY timestamp_ms DESC
     LIMIT 10"
).bind(instance_id).fetch_all(pool).await?;
```

### Dashboard surfacing

Endpoint card gains "Last failure" line:
```
Last failure: YouTube closed connection 2m ago (3 restarts in 10m)
```
Pulled via `WsEvent::AuditAppended` subscription (primary) or `/api/v1/audit?endpoint=...` (fallback).

---

## Fix E — Remote-close-aware reconnect backoff

Implemented by Fix D's `reconnect_floor` (see above). The delta vs current code: today's ffmpeg restart loop sleeps `1 * 2^n` seconds capped at 60 s. New policy distinguishes classes and uses 30 s minimum for remote-close events to survive YouTube/FB de-dup windows.

State tracked per-endpoint in `endpoint_task.rs`:
```rust
struct EndpointRestartState {
    consecutive_same_class: u32,
    last_class: Option<ReasonClass>,
}
```
Resets `consecutive_same_class` to 0 when reason-class changes; increments otherwise.

---

## Fix F — Remove-last-endpoint guard

### Server-side reject

`crates/rs-api/src/delivery_endpoints.rs::remove_endpoint_from_delivery` gains a pre-flight check:

```rust
// After instance lookup + is_delivery_active guard (existing):
let force = headers.get("x-force-remove")
    .and_then(|v| v.to_str().ok()) == Some("true");

if !force {
    let event = db::get_streaming_event_by_id(pool, event_id).await?;
    let endpoint_count = /* VPS-reported endpoint count from last status */;
    if event.delivering_activated && endpoint_count <= 1 {
        return Err(anyhow::anyhow!(
            "would_leave_zero_endpoints: delivery is active and removing this endpoint \
             would leave zero endpoints. Pass x-force-remove:true to override."
        ));
    }
}
```

HTTP handler maps this specific anyhow error to 409 Conflict with body `{"error": "would_leave_zero_endpoints"}`.

### Dashboard confirm modal

Leptos component `EndpointRemoveConfirmModal` patterned after `DestructiveConfirmModal` (from #72/#87). Shown when:
- user clicks "Remove endpoint" AND
- dashboard store's `delivery.endpoints.len() == 1` AND
- `pipeline_state.state != "idle"` (i.e., delivery is active)

Modal copy:
> **Leaving delivery with 0 active endpoints**
>
> Removing *{alias}* is the last endpoint. Audience will see nothing.
> Type the event name to confirm: __________
>
> [Cancel] [Remove anyway]

On confirm, re-call `DELETE /api/v1/delivery/events/{event_id}/endpoints/{alias}` with `x-force-remove: true`.

### Zero-endpoint warning banner

New component `ZeroEndpointBanner` in `operator_dashboard.rs`, shown when:
- `pipeline_state.state != "idle"` AND
- `delivery.endpoints.len() == 0`

Red, pulsing. Copy: "⚠️ Delivery is active but 0 endpoints are running. Audience sees nothing."

---

## Fix G — Continuous VPS log capture

### rs-delivery side

Module `crates/rs-delivery/src/audit_ring.rs`:
```rust
pub struct AuditRing {
    rows: Mutex<VecDeque<AuditRow>>,
    next_id: AtomicI64,
}

impl AuditRing {
    pub fn push(&self, row: AuditRow);
    pub fn since(&self, cursor: i64) -> (Vec<AuditRow>, i64 /*new cursor*/);
}
```

Keeps the last 500 rows in memory + appends to `/var/log/rs-delivery/audit.jsonl` (one JSON per line) via a `tokio::spawn`ed background writer.

All rs-delivery audit emissions (endpoint_started, endpoint_alive_transition, endpoint_ffmpeg_died, endpoint_ffmpeg_restart_failed) go through this ring.

### /api/status extension

```rust
#[derive(Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
    pub endpoint_count: usize,
    pub endpoints: Vec<EndpointStatus>,
    pub recent_audit: Vec<AuditRow>,     // NEW
    pub next_audit_cursor: i64,           // NEW
}

// Handler accepts ?since=<cursor>:
async fn get_status(Query(q): Query<StatusQuery>) -> Json<StatusResponse> {
    let (rows, cursor) = audit_ring.since(q.since.unwrap_or(0));
    ...
}
```

### Host side

`crates/rs-api/src/delivery.rs::poll_delivery_metrics` passes `?since=<cursor>` from `delivery_instances.last_audit_cursor`, and on response:
1. Inserts each returned row into host `audit_log` with `source = 'vps'`, `instance_id` filled, and the original `ts` preserved.
2. Updates `delivery_instances.last_audit_cursor = response.next_audit_cursor`.

---

## Fix H — SQLite WAL + worker dedup

### Pragmas on pool init

New `crates/rs-core/src/db/pragmas.rs`:
```rust
pub async fn apply_pragmas(pool: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA journal_mode=WAL").execute(pool).await?;
    sqlx::query("PRAGMA busy_timeout=5000").execute(pool).await?;
    sqlx::query("PRAGMA synchronous=NORMAL").execute(pool).await?;
    sqlx::query("PRAGMA foreign_keys=ON").execute(pool).await?;
    Ok(())
}
```

Called from `db::create_memory_pool()` AND `db::create_file_pool(path)` (both must apply the same pragmas). Existing call sites (`rs-service` startup, test helpers) don't change since pragmas are applied inside pool-creation helpers.

### Uploader worker dedup

Current `crates/rs-endpoint/src/uploader.rs` spawns N workers each running:
```rust
loop {
    let Some(chunk) = db::pick_next_uploadable_chunk(pool).await? else { ... };
    upload(chunk).await?;
    db::mark_chunk_sent(pool, chunk.id).await?;
}
```

When N workers all hit the same row on `SELECT … LIMIT 1`, SQLite serialises them → BUSY errors.

Replace with single **claim-coordinator** task + worker pool:

```rust
// Single task that owns picking.
async fn claim_coordinator(pool: SqlitePool, dispatch: mpsc::Sender<ChunkJob>) {
    loop {
        let batch = db::pick_next_uploadable_chunks(&pool, /* limit */ 16).await?;
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        for chunk in batch {
            db::mark_chunk_in_process(&pool, chunk.id).await?;
            dispatch.send(ChunkJob { chunk }).await?;
        }
    }
}

// Existing N workers consume from dispatch channel:
async fn worker(mut rx: mpsc::Receiver<ChunkJob>, ...) {
    while let Some(job) = rx.recv().await {
        upload(job.chunk).await?;
        db::mark_chunk_sent(...).await?;
    }
}
```

One `SELECT` per batch of 16, serialized. BUSY errors collapse to near-zero.

New DB helper `db::pick_next_uploadable_chunks(pool, limit)` returns `Vec<ChunkRecord>` via `SELECT … WHERE sent=0 AND in_process=0 AND upload_failed_permanently=0 ORDER BY upload_next_retry_at ASC, id ASC LIMIT ?1`.

---

## Fix I — RTMP-stable gate before `start_delivery`

### AppState tracking

`crates/rs-api/src/state.rs::AppState` gains:
```rust
pub rtmp_stable_since: Arc<Mutex<Option<Instant>>>,
```

Hooked to inpoint events: on `rtmp_connected` set to `Some(Instant::now())`, on `rtmp_disconnected` set to `None`.

### Gate in `start_delivery` handler

```rust
const RTMP_STABLE_REQUIRED_SECS: u64 = 15;

let stable_since = state.rtmp_stable_since.lock().await.clone();
let current_secs = stable_since.map(|t| t.elapsed().as_secs()).unwrap_or(0);
if current_secs < RTMP_STABLE_REQUIRED_SECS {
    return Err(ApiError::bad_request(json!({
        "error": "rtmp_not_stable",
        "current_secs": current_secs,
        "need_secs": RTMP_STABLE_REQUIRED_SECS,
    })));
}
```

### Dashboard surfacing

Start-delivery button disabled in `operator_dashboard.rs` when `pipeline_state.state == "idle"` or RTMP unstable; tooltip explains "Waiting for OBS stream to stabilize ({current_secs}/15s)".

---

## Fix J — CI deploy gate on active live event

New step in `.github/workflows/ci.yml` `deploy-stream-lan` job, BEFORE the actual deploy step:

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

API unreachable → fail (conservative). Commit-message override `[skip-live-check]` for emergency deploys. No GitHub Actions `env` hacks — the check is simple PowerShell against the self-hosted runner's own stream.lan endpoint.

---

## File Structure

### New files
| Path | Responsibility |
|---|---|
| `crates/rs-core/src/audit.rs` | `Severity/Source/Action` enums, `record()`, `audit_writer_task` |
| `crates/rs-core/src/db/audit.rs` | DB access for `audit_log` (insert, query, rotate) |
| `crates/rs-core/src/db/metrics.rs` | DB access for `delivery_endpoint_metrics` |
| `crates/rs-core/src/db/pragmas.rs` | WAL + busy_timeout + sync pragmas |
| `crates/rs-delivery/src/ffmpeg_reason.rs` | stderr classification + reconnect_floor + pick_last_error_line |
| `crates/rs-delivery/src/audit_ring.rs` | in-memory ring + JSONL file writer |
| `crates/rs-delivery/tests/ffmpeg_reason_fixtures/*.txt` | 16 real stderr captures from today |
| `crates/rs-api/src/audit_handlers.rs` | `GET /api/v1/audit`, `/api/v1/audit/{id}` |
| `crates/rs-api/src/metrics_handlers.rs` | `GET /api/v1/delivery/metrics` |
| `leptos-ui/src/components/audit_panel.rs` | Right-side live audit feed |
| `leptos-ui/src/components/endpoint_history.rs` | Sparkline tab per endpoint |
| `leptos-ui/src/components/endpoint_remove_confirm_modal.rs` | Remove-last-endpoint modal |
| `leptos-ui/src/components/zero_endpoint_banner.rs` | Warning banner |

### Modified files
- `crates/rs-core/src/db/migrations.rs` — V17 + V18
- `crates/rs-core/src/db/mod.rs` — call `pragmas::apply_pragmas` in both `create_*_pool`
- `crates/rs-core/src/models.rs` — add `WsEvent::AuditAppended`
- `crates/rs-api/src/lib.rs` — `delivery_broadcast_loop` writes metrics every 6 s + writes audit rows; spawn `audit_writer_task` and rotation task on serve
- `crates/rs-api/src/delivery_endpoints.rs` — Fix C (Live→latest); Fix F (remove-last guard); audit emissions
- `crates/rs-api/src/delivery.rs` — audit call sites; populate `restart_history` from DB; VPS audit cursor mirroring
- `crates/rs-api/src/delivery_handlers.rs` — audit emissions for start/stop
- `crates/rs-api/src/stream_handlers.rs` — audit emissions; no behavioural change
- `crates/rs-api/src/state.rs` — `rtmp_stable_since`; audit `mpsc::Sender`
- `crates/rs-api/src/s3_handlers.rs` — audit on clear; rate-limited error audit
- `crates/rs-api/src/router.rs` — mount audit + metrics handlers
- `crates/rs-inpoint/src/lib.rs` (or wherever RTMP events fire) — audit emissions + `rtmp_stable_since` signalling
- `crates/rs-endpoint/src/uploader.rs` — claim-coordinator pattern; audit emissions
- `crates/rs-delivery/src/endpoint_task.rs` — use `ffmpeg_reason::classify` + `reconnect_floor`; emit audit rows to `AuditRing`
- `crates/rs-delivery/src/api_handlers.rs` — `/api/status` includes `recent_audit` + `next_audit_cursor`
- `crates/rs-delivery/src/lib.rs` — spawn `audit_ring` JSONL writer
- `leptos-ui/src/ws.rs` — restore `ActivityFeed` update (reverse 60289c1 drop) + new `AuditAppended` handler + `MetricsSample` handler
- `leptos-ui/src/store.rs` — restore `activity_feed` signal + add `audit_feed` + `endpoint_metrics_history`
- `leptos-ui/src/components/operator_dashboard.rs` — mount `AuditPanel`, `ZeroEndpointBanner`, `EndpointRemoveConfirmModal`; gate start-delivery button on RTMP stable
- `.github/workflows/ci.yml` — pre-deploy live-event gate

---

## Testing

### Unit tests
- `audit::record` rate-limits error classes; fills channel without panicking
- `Severity/Source/Action` round-trip through serde
- `ffmpeg_reason::classify` — 16 fixture cases, each asserting expected `ReasonClass`
- `ffmpeg_reason::reconnect_floor` — table-driven: each class × `consecutive` ∈ {0,1,2,3,5,10}
- `ffmpeg_reason::pick_last_error_line` — skips progress/banner, returns last error-looking line
- `StartPosition::Live` returns latest sequence; `StartPosition::Beginning` returns first; `StartPosition::Resume` passes through
- `pragmas::apply_pragmas` — assert `PRAGMA journal_mode` returns `wal`
- `db::pick_next_uploadable_chunks` — returns up to `limit` rows, skips `in_process=1`, ordered by retry-time then id
- Claim-coordinator: on stress (100 chunks, 8 workers), zero BUSY errors
- Remove-endpoint guard: returns 409 when would-leave-zero + active; returns ok when `x-force-remove: true`; returns ok when not active
- Start-delivery gate: returns 400 when RTMP unstable; ok when stable ≥15s
- Migration V17 and V18 idempotency (rerun-on-same-DB test, mirrors V13 pattern)

### Integration tests (`crates/rs-api/tests/`)
- Audit roundtrip: call operator action → audit row in DB → `AuditAppended` broadcast → `GET /api/v1/audit` returns it
- Metrics: `delivery_broadcast_loop` writes rows at expected cadence; `GET /api/v1/delivery/metrics` returns them
- VPS audit cursor mirroring: mock rs-delivery returning 5 rows → host inserts 5 with `source='vps'` + cursor advances
- remove_endpoint_from_delivery 409 flow end-to-end

### Playwright E2E (`e2e/`)
- **`audit-panel.spec.ts`** — start event, add endpoint, remove endpoint; assert audit panel shows rows with correct action strings
- **`remove-last-endpoint-modal.spec.ts`** — 1 endpoint, delivery active; clicking Remove shows modal; typing event name + confirm proceeds
- **`start-delivery-rtmp-gate.spec.ts`** — click Start Delivery immediately after OBS connect → button disabled; wait 15s → enabled
- **`endpoint-history-sparkline.spec.ts`** — open endpoint history tab; assert sparkline renders with ≥2 points after 12 s of streaming
- **`zero-endpoint-banner.spec.ts`** — force delivery to have 0 endpoints via `x-force-remove` → banner visible

Every E2E spec asserts `expect(consoleMessages).toEqual([])` at the end per `browser-console-zero-errors` rule.

### Mutation testing
- No new `--exclude-re` additions
- Run mutation on PR; any surviving mutant in new code must be killed by tightening tests

---

## CI

Existing pipeline runs all of the above. Additions:
- New Playwright specs automatically picked up by existing `Frontend E2E (Playwright)` + `E2E OBS-to-YouTube Test` jobs.
- `Refuse deploy during active live event` step added to `deploy-stream-lan` job before the deploy itself.
- Version-check will pass because 0.3.65 → 0.3.66 (strictly increasing).

---

## Version and PR

Version bump 0.3.65 → 0.3.66 in:
- `Cargo.toml` workspace line 24
- `src-tauri/Cargo.toml` line 3
- `src-tauri/tauri.conf.json` line 4
- `leptos-ui/Cargo.toml` line 3

PR title: `feat: live-event post-mortem — audit log, metrics, ffmpeg reason + reconnect, RTMP/endpoint guards, SQLite WAL (#120, post-mortem 2026-04-19)`

PR closes #120 and references the post-mortem in body.

---

## Acceptance Criteria

- [ ] Today's real ffmpeg stderr produces correct `ReasonClass` in unit test
- [ ] `StartPosition::Live` resolves to latest sequence (unit + integration)
- [ ] `audit_log` persists rows from every listed call site; rows queryable via API and surfaced via WebSocket
- [ ] Dashboard audit panel visible, live-updating, severity-coded, filterable by source
- [ ] Remove-last-endpoint modal blocks silent zero-endpoint state; confirm requires event-name typing
- [ ] Zero-endpoint warning banner visible when `delivering_activated && endpoint_count==0`
- [ ] `start_delivery` rejects with 400 when RTMP stable <15 s; ok when stable
- [ ] `/api/v1/delivery/status` response `restart_history` populated from `delivery_restart_log`
- [ ] rs-delivery `/api/status` returns `recent_audit` + `next_audit_cursor`; host mirrors rows into `audit_log` with `source='vps'`
- [ ] Metrics rows written every ~6 s during running delivery; sparkline renders in endpoint card history tab
- [ ] SQLite `journal_mode=wal` verified at pool init; BUSY errors reduced ≥99% in stress test vs current behaviour
- [ ] CI `deploy-stream-lan` refuses when live event active; `[skip-live-check]` overrides
- [ ] All existing E2E + unit + mutation tests pass
- [ ] Version bumped to 0.3.66; single PR
- [ ] No new mutation-testing exclusions

---

## Risks / Non-Risks

- **Migration V17/V18 on running production** — idempotent, additive-only, no ALTER that could fail on existing rows (the one ALTER adds column with DEFAULT 0). Low risk, mirrors V13 fix pattern.
- **Audit write volume** — bounded channel + rate-limiting + batched INSERT. Stress-tested. No impact on streaming hot path.
- **WAL mode** — already a best practice; some SQLite tooling (older `sqlite3.exe`) may see stale reads without checkpoint. Not a concern for us (nothing external reads the DB live).
- **Dashboard rebuild** — restores a panel that existed pre-`60289c1`. Code path well-understood.
- **Reconnect 30 s floor** — makes stream recovery slightly slower in the `InvalidInput` case? No — that class keeps 1 s. Only remote-close classes get 30 s, which is correct behaviour.
