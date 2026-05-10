# Fast-Endpoint Zero-Reconnect — Design Spec

**Date:** 2026-05-10
**Issue:** filed before plan execution; reference the GH issue number in Task 1 of the implementation plan
**Status:** Approved for implementation
**Goal:** ZERO reconnects on fast endpoints (Kiko / Resolume control). Match OBS-direct reliability while keeping the S3-on-VPS routing path. Preserve "fast" UX (~4s end-to-end delay).

---

## 1. Problem

### 1.1 Observed behavior

In production event 9292 on streamsnv (rust-pusher path, v0.7.5), Kiko fast endpoint died **4 times in ~1 hour**, all with `upstream closed connection mid-stream: connection reset` (TCP RST from Resolume receiver). Other endpoints — non-fast with 120s buffer — had zero reconnects in the same window.

Death pattern from `disk_cache_push_sample` audit rows immediately preceding each death:

| Death | Last `chunk_supply_lag_ms` | Last `inter_chunk_gap_ms` |
|---|---|---|
| 1 | 20 800 | 2 000 |
| 2 | 46 740 | 3 800 |
| 3 | **1 981 000** | 1 826 |
| 4 | 95 590 | **32 179** |

Every death preceded by `chunk_supply_lag_ms` exploding well past the chunk-duration baseline (~2 s).

### 1.2 Mechanism

Kiko = `is_fast=true` ⇒ `delivery_delay_ms=0` ⇒ pusher reads chunk N from `disk_cache` then immediately writes to TCP, then requests N+1, etc. **Zero buffered chunks** between fetcher and wire. When `disk_cache` cannot supply N+1 within Resolume's RTMP-receiver idle tolerance, the TCP socket goes idle; Resolume sends RST; rust-pusher detects via session read-loop (`poisoned` flag → `ConnectionReset`); audit emits `endpoint_rtmp_push_died`; backoff + reconnect.

Non-fast endpoints have a 120 s buffer that absorbs the same supply hiccups invisibly.

### 1.3 What we don't yet know

The current audit instrumentation — `disk_cache_push_sample` (one row per push) and `endpoint_s3_fetch_failed` (one row per minute per error class) — gives the **symptom** (`chunk_supply_lag_ms` spike) but not the **stage**. The stall could come from any of:

- **Stage A→B**: host chunker slow to write, or S3 upload slow / 5xx-retry on host
- **Stage B→C**: chunk visible in S3 but VPS fetcher hasn't issued GET yet (poller cadence)
- **Stage C→D**: VPS S3 GET slow (Hetzner Object Storage 504 / TCP-level slowness)
- **Stage D→E**: chunk in VPS memory but pusher hasn't requested it (queue dwell)
- **Stage E→F**: pusher pacing / TCP write slow

Event 9292 audit had **zero** `endpoint_s3_fetch_failed` rows during the 33-minute pre-death-3 stall, so the rate-limiter either suppressed them OR the stall isn't S3-fetch-failure-side at all. Today's instrumentation cannot tell us which.

### 1.4 User constraints

- ZERO reconnects on fast endpoints (period — measured during operator soak / live stream)
- Keep VPS routing path (no LAN bypass for now)
- Keep "fast" UX — Kiko delay must NOT rise meaningfully (no "fast becomes slow")
- One PR (telemetry + buffer ship together)
- **Never stop retrying anywhere in the pipeline. Slow down (backoff cap) but never give up.**

---

## 2. Architecture

### 2.1 Pipeline overview

```
┌────────────────────────────── HOST (stream.lan) ──────────────────────────────┐
│  RTMP ingest → chunker → S3 uploader                                          │
│         │                       │                                             │
│         A. host_emit_ts         B. s3_upload_complete_ts                      │
│                                 │                                             │
│                                 ▼                                             │
│                    chunk_records DB row carries A, B                          │
│                    S3 object metadata headers carry A, B                      │
└──────────────────────────────────│────────────────────────────────────────────┘
                                   │ (chunk in S3)
                                   ▼
┌────────────────────────────── VPS (Hetzner rs-delivery) ──────────────────────┐
│  DiskCache fetcher pulls chunk → memory                                       │
│       │                  │                                                    │
│       C. vps_fetch_start D. vps_fetch_done                                    │
│                          │                                                    │
│                          ▼                                                    │
│           ┌──────── EndpointReader ────────┐                                  │
│           │  PrefetchQueue<K>              │ ◄── K=0 default; K=1 for fast    │
│           │   front: pusher consumes       │     (double-buffered)            │
│           │   back:  reader replenishes    │                                  │
│           └──────────────┬─────────────────┘                                  │
│                          E. pusher_request_ts                                 │
│                          ▼                                                    │
│                   RtmpPusher.push_flv                                         │
│                          F. wire_first_byte_ts                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Two units, one PR

1. **`chunk_lifecycle` module** (new, in `rs-delivery/src/chunk_lifecycle/`) — `ChunkLifecycleTimings` struct, lifecycle audit emission rules, S3 metadata header propagation, predeath ring buffer.
2. **`PrefetchQueue<K>` in EndpointReader** (new, in `rs-delivery/src/disk_cache/prefetch_queue.rs`) — bounded queue between disk_cache fetch and pusher consumption, default K=0 for non-fast (no behavior change), K=1 for fast (double-buffered = zero added delay in steady state).
3. **Never-stop retry hardening** — remove `max_attempts = 5` from `download_service.rs:188`; switch to retry-forever with exponential backoff capped at 60s; emit one audit row per minute per endpoint per error class as long as retry is active.

---

## 3. Components

### 3.1 `ChunkLifecycleTimings` (new struct, `rs-delivery/src/chunk_lifecycle/timings.rs`)

```rust
use std::time::{Duration, SystemTime};

/// Timestamps captured at each pipeline stage for one chunk.
///
/// Stages A and B are set on the host (chunker + uploader). They are
/// propagated to the VPS through:
///   1. `chunk_records` DB columns (already exists; we add 2 cols)
///   2. S3 object metadata headers (`x-amz-meta-host-emit-ts` and
///      `x-amz-meta-s3-complete-ts`) so the VPS can backfill A/B from
///      the same GET that fetches the chunk body.
///
/// Stages C–F are set on the VPS by the disk_cache fetcher, EndpointReader,
/// and RtmpPusher respectively.
#[derive(Debug, Clone)]
pub struct ChunkLifecycleTimings {
    pub sequence_number: i64,
    pub event_id: i64,
    pub endpoint_alias: String,

    // Host stages
    pub host_emit_ts: Option<SystemTime>,           // A
    pub s3_upload_complete_ts: Option<SystemTime>,  // B

    // VPS stages
    pub vps_fetch_start_ts: Option<SystemTime>,     // C
    pub vps_fetch_done_ts: Option<SystemTime>,      // D
    pub pusher_request_ts: Option<SystemTime>,      // E
    pub wire_first_byte_ts: Option<SystemTime>,     // F
}

impl ChunkLifecycleTimings {
    pub fn new(sequence_number: i64, event_id: i64, endpoint_alias: String) -> Self { /* ... */ }

    pub fn gap_a_to_b(&self) -> Duration { /* host-clock */ }
    pub fn gap_b_to_c(&self) -> Duration { /* CROSS-CLOCK — labeled in audit */ }
    pub fn gap_c_to_d(&self) -> Duration { /* vps-clock */ }
    pub fn gap_d_to_e(&self) -> Duration { /* vps-clock */ }
    pub fn gap_e_to_f(&self) -> Duration { /* vps-clock */ }

    /// Returns (label, duration) for the slowest single stage. Cross-clock
    /// stage B→C is excluded from worst-stage calculation (its duration is
    /// noise dominated by clock skew).
    pub fn worst_stage(&self) -> (&'static str, Duration);

    /// True if either A or B is None (chunk uploaded by an old host
    /// without lifecycle instrumentation). Used to label the audit row
    /// `instrumented=false`.
    pub fn is_partial(&self) -> bool;
}
```

### 3.2 `LifecycleSampler` (new, `rs-delivery/src/chunk_lifecycle/sampler.rs`)

Owns three audit emission rules. Runs per endpoint inside `EndpointReader`.

```rust
pub struct LifecycleSampler {
    sample_every_n: u64,                     // default 30 (≈ once per minute @ 2s/chunk)
    breach_threshold_ms: u64,                // default 4_000 (= 2× chunk_duration default)
    breach_rate_limit_window: Duration,      // default 5s per endpoint
    last_breach_emit: Option<Instant>,
    pushed_count: u64,
    predeath_ring: VecDeque<ChunkLifecycleTimings>, // last 5 chunks
}

impl LifecycleSampler {
    /// Called on each chunk write completion. Decides whether to emit a
    /// `disk_cache_lifecycle_sample` (steady-state) or
    /// `disk_cache_lifecycle_breach` (any single stage > threshold).
    /// Always pushes to predeath_ring.
    pub fn observe(
        &mut self,
        timings: &ChunkLifecycleTimings,
        audit_ring: &Option<Arc<AuditRing>>,
    );

    /// Called when the endpoint dies (any cause). Emits one
    /// `endpoint_lifecycle_predeath` row with last 5 chunks' full timings.
    /// Always emits (no rate-limit).
    pub fn emit_predeath(&self, audit_ring: &Option<Arc<AuditRing>>);
}
```

Three new `Action` variants in `rs-core/src/audit.rs`:
- `DiskCacheLifecycleSample` — periodic steady-state sample (severity=info)
- `DiskCacheLifecycleBreach` — single chunk where any stage exceeded threshold (severity=warn)
- `EndpointLifecyclePredeath` — last-5-chunks dump on death (severity=warn)

Per-endpoint breach rate limit: at most 1 row per 5 s (token bucket). Predeath always emits — no rate limit.

### 3.3 `PrefetchQueue<K>` (new, `rs-delivery/src/disk_cache/prefetch_queue.rs`)

Bounded async FIFO between disk_cache fetcher and pusher. K=0 → bypass entirely (zero overhead for non-fast endpoints). K≥1 → double-buffered (or deeper, if operator opts in).

```rust
pub struct PrefetchQueue {
    capacity: usize,                        // K
    inner: Mutex<VecDeque<Arc<Chunk>>>,
    not_full: Notify,                       // reader waits here when at capacity
    not_empty: Notify,                      // pusher waits here when drained
    closed: AtomicBool,
}

impl PrefetchQueue {
    pub fn new(capacity: usize) -> Arc<Self>;

    /// Reader-side: push at back. Awaits `not_full` if at capacity.
    /// Returns Err if queue closed.
    pub async fn push_back(&self, chunk: Arc<Chunk>) -> Result<(), QueueClosed>;

    /// Pusher-side: pop front. Awaits `not_empty` if drained.
    /// Returns Err if queue closed.
    pub async fn pop_front(&self) -> Result<Arc<Chunk>, QueueClosed>;

    pub fn len(&self) -> usize;             // for dashboard fill bar
    pub fn capacity(&self) -> usize;
    pub fn close(&self);
}
```

**K=0 special case:** when `capacity == 0`, `push_back` and `pop_front` rendezvous synchronously (no buffering). Used by non-fast endpoints to preserve current behavior with zero added overhead.

### 3.4 `PrefetchReader` task (new, lives inside `EndpointReader`)

Background task that drives `PrefetchQueue`:

```rust
async fn prefetch_reader_task(
    queue: Arc<PrefetchQueue>,
    fetcher: Arc<DiskCacheFetcher>,
    next_chunk_id: AtomicI64,
    audit_ring: Option<Arc<AuditRing>>,
) -> ! {
    loop {
        let id = next_chunk_id.fetch_add(1, Ordering::AcqRel);
        // RETRY FOREVER (per user rule: never give up)
        let chunk = fetch_chunk_with_infinite_retry(&fetcher, id, &audit_ring).await;
        // capture C, D timestamps; backfill A, B from S3 metadata header
        if queue.push_back(chunk).await.is_err() {
            return; // endpoint shutdown
        }
    }
}
```

`fetch_chunk_with_infinite_retry` — exponential backoff (1s → 2s → 4s → ... cap 60s) with one audit row per minute per error class while retry is active. Never returns Err — always eventually returns Ok, even if it takes hours.

### 3.5 Endpoint config (rs-core `Endpoint`)

```rust
pub struct Endpoint {
    // ... existing fields ...

    /// Number of chunks to pre-fetch ahead of the pusher. Defaults are
    /// is_fast=true → K=1 (double-buffered, ~zero added delay).
    /// is_fast=false → K=0 (current behavior, no buffering).
    /// Operator can override per endpoint (e.g. K=2 if telemetry shows
    /// repeated 1-chunk hiccups).
    #[serde(default)]
    pub prefetch_chunks: Option<u32>,
}
```

Effective K resolution at endpoint init:
1. If `prefetch_chunks` explicitly set → use it
2. Else if `is_fast == true` → K=1
3. Else → K=0

### 3.6 Dashboard surfacing (leptos-ui)

Endpoint card adds two new visualizations:

- **Prefetch fill bar** — small horizontal bar showing `queue.len() / queue.capacity()`. Hidden for K=0 endpoints. Green when full, yellow when half, red when drained.
- **Worst-stage indicator** — last lifecycle sample's worst-stage label and duration as a small badge (e.g. "C→D: 850 ms"). Tooltips show all five gaps.

### 3.7 Never-stop retry hardening (download_service.rs)

`crates/rs-delivery/src/disk_cache/download_service.rs:188` — current code:

```rust
let max_attempts = 5;
for attempt in 1..=max_attempts {
    // ...
    if attempt >= max_attempts {
        return Err(...);
    }
}
```

Replace with retry-forever:

```rust
let mut attempt: u32 = 0;
let mut last_audit_emit: Option<Instant> = None;
loop {
    attempt = attempt.saturating_add(1);
    match self.try_download(chunk_id).await {
        Ok(bytes) => return Ok(bytes),
        Err(err) => {
            // Emit one audit row per minute per error class
            // (existing S3FetchAuditLimiter handles per-class rate-limit)
            self.audit_limiter.try_emit(&self.audit_ring, alias, chunk_id,
                &err.to_string(), backoff_secs);
            // Exponential backoff: 1s → 2s → 4s ... cap 60s
            let backoff = Duration::from_secs((1u64 << attempt.min(6)).min(60));
            tokio::time::sleep(backoff).await;
            // NO max_attempts check. Loop forever until success or task cancel.
        }
    }
}
```

Cancellation comes from the endpoint task being dropped (queue closed → reader task exits).

---

## 4. Data flow + error handling

### 4.1 Steady-state happy path (one chunk)

```
1. HOST: chunker writes chunk N to local FS at host_t=0
   → set timings.host_emit_ts = host_t=0
   → write to chunk_records DB row
2. HOST: uploader PUT to S3 with x-amz-meta-host-emit-ts header
   → on 200 OK at host_t=120 ms: set timings.s3_upload_complete_ts
   → update DB row column s3_upload_complete_ts
3. VPS: PrefetchReader requests next chunk_id from DiskCacheFetcher
   → at vps_t=180 ms: set timings.vps_fetch_start_ts; issue S3 GET
   → reads x-amz-meta headers → backfills A, B onto timings struct
4. VPS: S3 GET completes at vps_t=240 ms → vps_fetch_done_ts; chunk Arc'd
5. VPS PrefetchReader: queue not full → push_back; Notify not_empty
6. VPS Pusher: pop_front (instantly, queue had 1 chunk waiting)
   → set pusher_request_ts; call push_flv
7. VPS Pusher: first TCP write succeeds → set wire_first_byte_ts
8. LifecycleSampler.observe(&timings, &audit_ring):
   - Push to predeath_ring (evict oldest if > 5)
   - If pushed_count % sample_every_n == 0 → emit lifecycle_sample
   - Else if any single stage > breach_threshold_ms AND rate-limit window
     elapsed → emit lifecycle_breach
```

### 4.2 Stall scenarios + behavior

| Scenario | Symptom in lifecycle | Pusher impact (K=1 default) | Audit |
|---|---|---|---|
| Host upload slow (S3 5xx retry) | gap A→B = 8 s | invisible (next chunk queued) | breach |
| VPS fetch slow (Hetzner 504) | gap C→D = 6 s | invisible if 1 chunk queued | breach + s3_fetch_failed |
| Prefetch drained (K too small) | E waits, gap D→E spikes | pusher stalls, eventually RST | breach |
| Pusher TCP slow | gap E→F = 5 s | downstream lag rises but no reconnect | breach |
| Death | last 5 chunks all show one stage spiking | reconnect | predeath dump |

### 4.3 Error handling

- **S3 metadata header missing** (chunk uploaded by an old host without instrumentation): A, B = None; gap math returns Duration::ZERO; row emits with `instrumented=false`. Backward-compatible during rollout.
- **Clock skew host vs VPS:** stages A, B from host clock; C–F from VPS clock. Within-host gaps and within-VPS gaps trustworthy. Cross-host gap (B→C) carries skew = noisy. Audit row labels which gaps are cross-clock; dashboard sorts by within-clock gaps for diagnosis. Worst-stage calculation excludes B→C.
- **Prefetch reader fetch fails forever (S3 outage):** existing `s3_fetch_failed` audit emits one row per minute per error class. Queue stays at last depth. Pusher drains queue. If queue drains to 0 → pop_front blocks (waits Notify forever, never times out). When S3 returns, fetcher resumes, queue refills, pusher resumes — **chunks 5..N never lost, never skipped.**
- **Prefetch capacity exhausted by upstream burst:** queue at K, reader awaits not_full → no harm (back-pressure to fetcher, fetcher idles).

### 4.4 Cancel / shutdown

Endpoint stop → drop PrefetchQueue (close()) → reader task notices QueueClosed → exits cleanly. Pusher exits via existing LocalCancel path. No goroutine/task leaks.

### 4.5 Never-stop guarantee

| Layer | Today | Per user rule |
|---|---|---|
| RTMP push fails | reconnect, 3s→6s→...→cap 300s, no attempt cap | keep ✓ |
| S3 download fails | gives up after 5 attempts | **fix:** retry forever, cap 60s, audit 1/min |
| PrefetchReader fail | (new code) | retry forever same as above |
| Pusher pop empty | (new code) | wait Notify forever, never timeout-die |
| Endpoint task exit | only on LocalCancel (operator stop) | keep ✓ |

---

## 5. Testing

### 5.1 Unit tests

- `ChunkLifecycleTimings::worst_stage` — synth 5 stage gaps; assert correct stage; assert B→C excluded
- `ChunkLifecycleTimings::gap_*` — None-safe (returns ZERO when stage missing); cross-clock labeling
- `ChunkLifecycleTimings::is_partial` — true when A or B is None
- `LifecycleSampler` — sample emits every Nth; breach emits when threshold exceeded; predeath dumps last 5
- `LifecycleSampler` rate-limiter — burst of 100 breach events → at most 12 audit rows in 60 s
- `LifecycleSampler::emit_predeath` — always emits, even immediately after another emit (no rate-limit)
- `PrefetchQueue` — push/pop FIFO order, capacity bound, blocks on full/empty, Notify wakeups
- `PrefetchQueue` K=0 → synchronous rendezvous, no buffering
- `PrefetchQueue::close` → all pending push_back/pop_front return Err
- `PrefetchReader` — fetch failure retries forever (mocked S3 client returning 503 × 100 times → still trying); audit row emitted every minute (not every retry)
- `download_service` retry loop — `max_attempts` removed; retry-forever path covered; backoff caps at 60 s; one audit row per minute per endpoint per error class

### 5.2 Integration tests (rs-delivery, async tokio)

- `prefetch_double_buffered_zero_delay` — mock S3 fetch with 50 ms latency, 100 ms write per chunk → assert pusher never waits between writes (gap E→F < 1 ms after first chunk)
- `prefetch_absorbs_one_chunk_hiccup` — mock S3: chunk 5 takes 1500 ms (vs 50 ms norm) → pusher writes 0–4 normally; gap on 5 invisible to wire; no death
- `prefetch_stall_beyond_buffer` — mock S3 outage: chunks 5+ take 30 s → pusher drains buffer, eventually waits, but session NOT killed (no death audit, lifecycle breach emits)
- `s3_outage_recovery_no_chunk_loss` — S3 dies for 90 s then returns → all chunks 5..N fetched (none skipped), pusher resumes from chunk 5 not chunk N
- `lifecycle_predeath_dump` — induce session death after 50 chunks → assert one `endpoint_lifecycle_predeath` row emitted with last 5 chunks' full timings

### 5.3 E2E (rs-delivery integration test against real local xiu)

- `local_xiu_loopback_kiko_simulated` — extend existing loopback to inject a chunk-delivery hiccup (sleep 5 s before serving chunk 30) and assert: 0 reconnects, lifecycle breach row exists for chunk 30, gap C→D ≈ 5 s

### 5.4 Mutation testing

- New modules `disk_cache::prefetch_queue` and `chunk_lifecycle::*` MUST NOT be added to `--exclude-re`
- All gap arithmetic, threshold comparisons, retry-forever loop, predeath dump triggers must survive cargo-mutants

### 5.5 Test discipline

- No mocked-internal-code shortcuts. S3 client mocked at HTTP boundary only (per `test-strictness`)
- Pusher uses real `RtmpPusher` against test xiu RTMP server (per `test-strictness`)
- All tests run in CI on every push (per project CLAUDE.md)
- No `#[ignore]`, no `assume!`, no skipped tests

---

## 6. Operator validation

After deploy to streamsnv, operator runs a normal soak (Kiko endpoint active alongside FB / YT) for at least one full live event. Success criteria:

- **Zero reconnects on Kiko** (audit query: `endpoint_rtmp_push_died` rows where endpoint='Control stream Kiko'; expect empty)
- Lifecycle audit rows present and well-formed (sample once/min, breach rows on any real hiccup, predeath if a death somehow happens)
- Kiko delay (`chunk_delay_secs` on dashboard) stays at ~4 s in steady state (unchanged from today — double-buffered fetch happens concurrently with TCP write, no added latency). Transient peaks during an absorbed supply hiccup may briefly reach ~6 s (one chunk consumed from buffer while fetcher catches up), then return to ~4 s.
- Other endpoints unchanged (FB / YT / e2e all behave identically to current production)
- No new memory leaks (VPS RSS stable)
- If reconnects DO happen, lifecycle predeath row pinpoints the stage; operator + Claude iterate on that stage

---

## 7. Out of scope (not in this PR)

- LAN bypass routing for fast endpoints (rejected during brainstorming — keep VPS path)
- Sub-chunk byte-level pre-read inside the pusher (not needed if K=1 covers chunk-grained variance)
- Auto-tuning K based on lifecycle data (operator manually adjusts via config based on telemetry; auto-tune deferred until enough data exists)
- Migrating ffmpeg-path endpoints to PrefetchQueue (only rust-pusher path benefits from this design; ffmpeg pipe semantics differ)
- Replacing `chunk_supply_lag_ms` metric (kept; lifecycle data complements rather than replaces)

---

## 8. File structure

### New files

- `crates/rs-delivery/src/chunk_lifecycle/mod.rs`
- `crates/rs-delivery/src/chunk_lifecycle/timings.rs`
- `crates/rs-delivery/src/chunk_lifecycle/sampler.rs`
- `crates/rs-delivery/src/chunk_lifecycle/audit.rs`
- `crates/rs-delivery/src/disk_cache/prefetch_queue.rs`
- `crates/rs-delivery/src/disk_cache/prefetch_reader.rs`
- Test files mirroring each above

### Modified files

- `crates/rs-core/src/audit.rs` — 3 new `Action` variants
- `crates/rs-core/src/models.rs` — `Endpoint::prefetch_chunks` field
- `crates/rs-core/src/db/mod.rs` — append incremental migration adding `chunk_records` columns `host_emit_ts INTEGER NULL`, `s3_upload_complete_ts INTEGER NULL` (millis since epoch). Plan Task verifies migration runner idempotent.
- `crates/rs-endpoint/src/uploader.rs` — set host_emit_ts and s3_upload_complete_ts; add `x-amz-meta-host-emit-ts` and `x-amz-meta-s3-complete-ts` headers to S3 PUT
- `crates/rs-cloud/src/s3.rs` — extend PUT path to accept a `metadata: HashMap<String,String>` arg passed through to the rust-s3 `put_object_with_*` call; extend GET response wrapper to surface response headers so the VPS fetcher can read the metadata back
- `crates/rs-delivery/src/disk_cache/mod.rs` — wire PrefetchQueue + PrefetchReader into EndpointReader path
- `crates/rs-delivery/src/disk_cache/download_service.rs` — remove max_attempts cap; retry forever
- `crates/rs-delivery/src/api.rs` — `EndpointStatusEntry` adds `prefetch_fill: Option<PrefetchFill>` (depth/capacity)
- `crates/rs-api/src/delivery_status.rs` + `delivery_handlers.rs` — pass through prefetch_fill + last lifecycle sample
- `leptos-ui/src/api.rs` + `store.rs` + endpoint-card component — render prefetch fill bar + worst-stage badge

### File-size cap

Each new `.rs` file MUST stay under 1000 lines (CI gate). Sampler logic, queue logic, and reader logic kept in separate files to respect this.

---

## 9. Versioning

`v0.7.5` → `v0.8.0` (architectural change: new pipeline component + audit schema + DB migration). Bump 4 files: `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`.

---

## 10. Risks

- **DB migration on production:** chunk_records new columns (`host_emit_ts`, `s3_upload_complete_ts`). Per project rule, use incremental migration (ALTER TABLE ADD COLUMN, idempotent). Existing rows: NULL. No data loss.
- **S3 metadata header propagation:** Hetzner Object Storage may strip unknown `x-amz-meta-*` headers. Pre-flight test required; if stripped, fall back to DB-row carrying A, B (slower path: VPS reads chunk_records before fetch). Test in Task 0 of plan.
- **PrefetchReader with K=1 introduces 1 chunk's worth of additional latency in worst case.** Steady-state double-buffer pattern keeps it at zero, but if pusher writes faster than fetcher fills, the pusher waits — same as today. So K=1 worst case ≡ K=0 today. K=1 best case absorbs supply hiccups invisibly.
- **`max_attempts=5` removal could mask a real bug** (e.g. permanent 404 because chunk never uploaded). Mitigation: audit row every minute makes silent black-hole impossible to miss. Operator sees activity, investigates.

---

## 11. Acceptance

PR is mergeable when:
- All unit, integration, E2E tests pass in CI
- No `#[ignore]`, no `assume!`, no `continue-on-error`
- File-size CI gate passes (every new .rs < 1000 lines)
- Mutation tests pass on new modules
- Coverage does not decrease
- One operator soak on streamsnv shows zero Kiko reconnects through one full live event
- Dashboard shows new prefetch fill bar and worst-stage badge correctly
