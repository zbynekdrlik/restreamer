# rs-delivery local-disk chunk cache — design spec

**Issue:** #174
**Status:** brainstorming → writing-plans
**Date:** 2026-05-05
**Replaces:** v0.3.98 PREFETCH_BUFFER_SIZE bump (regressed; reverted in v0.3.99)

---

## 1. Problem

`rs-delivery` VPS today fetches chunks from Hetzner S3 in a per-endpoint
producer loop and pipes them through a 10-chunk in-memory `mpsc::channel`
to a per-endpoint consumer that pushes via RTMP. Two failure modes have
been observed in production soaks:

1. **S3 transient outages** (~70 s observed 2026-05-05 event 9289): the
   10-chunk channel drains, consumer idles, upstream RTMP (YT, FB)
   closes the session via TCP idle-timeout. All endpoints reconnect
   simultaneously → cache stair-step → red dashboard for the rest of
   the event (issue #171).
2. **Concurrent S3 fetch storms** (v0.3.98 attempt at fixing #1 via
   60-chunk buffer): 6 endpoints × 60 chunks × ~3 MB pre-fill = ~1 GB
   bursting on cpx32's 1 Gbit shared NIC, blocking outbound RTMP writes,
   firing the 30 s `WRITE_TIMEOUT_SECS` cascade-style across all
   endpoints.

The 4-hour green-dashboard soak target is unreachable while the data
path makes synchronous S3 calls in the RTMP push hot path.

## 2. Solution shape

Introduce a per-event **disk-backed chunk cache** on VPS local SSD that
fully decouples upstream S3 ingress from the downstream RTMP push hot
path.

- One `DownloadService` per event; bandwidth-managed; deduplicated
  fetches across endpoints; writes to `/var/cache/rs-delivery/{event}/{seq}.bin`.
- One `EndpointReader` per endpoint, replaces today's
  `producer_task` + `consumer_task`. Reads from local disk only.
- `ChunkRegistry` provides async availability waits via
  `tokio::sync::Notify`.
- `EvictionTask` keeps disk bounded by per-endpoint window
  `[pos, pos + cache_delay_secs]`.
- `EndpointPositionRegistry` lets eviction know the live window union.

End state: 4-hour soak runs with cache fully populated within ~2 s of
each endpoint start, S3 transients absorbed (push reads from disk
regardless), NIC saturation prevented (token-bucketed S3 ingress, never
competes with RTMP outbound).

## 3. Architecture

```
                    ┌──────────────────────┐
                    │  ChunkRegistry       │
                    │  BTreeMap<id,Avail>  │
                    │  + tokio::Notify     │
                    └──────────────────────┘
                       ▲              ▲
                       │ mark         │ wait
                       │ available    │ for chunk
            ┌──────────┴──────┐    ┌──┴───────────────────────┐
            │ DownloadService │    │ EndpointReader (× N)     │
            │ - one per event │    │ - one per endpoint       │
            │ - bandwidth cap │    │ - reads disk → RTMP push │
            │ - dedup fetches │    │ - reports position       │
            └─────────────────┘    └──────────────────────────┘
                       │                       │
                       ▼                       │
                    Hetzner S3                 │
                       │                       │
                       ▼                       │
                /var/cache/rs-delivery/{event}/{seq}.bin
                                               ▲
                                               │ read
                                          (sequential, sub-ms latency)

 EvictionTask: deletes files outside any endpoint window (5 s tick)
 EndpointPositionRegistry: tracks per-endpoint chunk_id (RwLock<HashMap>)
```

## 4. Module layout

New module **`crates/rs-delivery/src/disk_cache/`** (own folder so each
file stays under the 1000-line gate):

```
disk_cache/
├── mod.rs                  // re-exports + DiskCache facade
├── registry.rs             // ChunkRegistry
├── download_service.rs     // DownloadService + token bucket
├── endpoint_reader.rs      // EndpointReader (replaces consumer_task)
├── eviction.rs             // EvictionTask
├── position_registry.rs    // EndpointPositionRegistry
└── tests/                  // integration tests
```

Public surface (re-exported from `disk_cache::mod`):

```rust
pub struct DiskCache {
    registry: Arc<ChunkRegistry>,
    download_service: Arc<DownloadService>,
    position_registry: Arc<EndpointPositionRegistry>,
    eviction_handle: tokio::task::JoinHandle<()>,
    cache_dir: PathBuf,
}

impl DiskCache {
    pub fn new(cfg: DiskCacheConfig) -> Result<Self>;
    pub fn endpoint_reader(&self, alias: &str, start_chunk_id: i64) -> EndpointReader;
    pub async fn shutdown(self);
}
```

`producer_task` and `consumer_task` in `endpoint_task.rs`: deleted
(replaced by `EndpointReader`). `endpoint_loop` keeps lifecycle but
delegates the data path to the disk cache. `s3_fetch.rs::S3Fetcher`
unchanged; used internally by `DownloadService`.

## 5. Data flow

**Per-chunk lifecycle:**

```
1. EndpointReader at chunk N:
   - reader.request_window(N, N+W)        // fire-and-forget hint
   - registry.wait_for_chunk(N).await     // blocks until on disk
   - read /var/cache/.../{N}.bin          // sub-ms local I/O
   - pusher.push_flv_bytes(&bytes)         // existing RtmpPusher
   - position_registry.set(alias, N)
   - N += 1

2. DownloadService receives request_window(N, N+W):
   - for each id in N..=N+W:
     - if registry.exists(id) || in_flight: skip
     - else: enqueue(id)
   - drain queue with bandwidth-limited workers
     - fetch from S3
     - write {tmp}/{id}.bin.part
     - rename to {tmp}/{id}.bin (atomic)
     - registry.mark_available(id) → wakes Notify waiters

3. EvictionTask (every 5 s):
   - positions = position_registry.snapshot()
   - needed = union(pos..=pos+W for pos in positions)
   - delete files where chunk_id not in needed
```

**Backpressure:** DownloadService bounded queue (200 chunks); when full,
drop oldest pending. EndpointReader's `wait_for_chunk` has 60 s timeout
→ surfaces real outages, returns `Err(StallTimeout)` for skip-ahead
logic.

**Atomicity:** files written `.part` then renamed; `mark_available`
called only after rename. Readers never see partial files.

**Concurrent reads safe:** POSIX read-only file shared by multiple
readers; no locks needed for hot path.

## 6. Eviction + window management

Per-endpoint window:

```rust
struct EndpointWindow {
    alias: String,
    current_chunk_id: i64,
    cache_window_chunks: i64,    // = cache_delay_secs / chunk_dur_secs
}

fn needed_range(w: &EndpointWindow) -> RangeInclusive<i64> {
    w.current_chunk_id ..= (w.current_chunk_id + w.cache_window_chunks)
}
```

`EndpointPositionRegistry` tracks all live endpoints:

```rust
pub struct EndpointPositionRegistry {
    inner: Arc<RwLock<HashMap<String, EndpointWindow>>>,
}
impl EndpointPositionRegistry {
    pub fn register(&self, alias: String, window_chunks: i64);
    pub fn advance(&self, alias: &str, chunk_id: i64);
    pub fn deregister(&self, alias: &str);
    pub fn snapshot(&self) -> Vec<EndpointWindow>;
    pub fn needed_chunks(&self) -> BTreeSet<i64>;
}
```

`EvictionTask` runs every `EVICTION_INTERVAL_SECS = 5`:

```rust
loop {
    tokio::time::sleep(EVICTION_INTERVAL_SECS).await;
    let needed = position_registry.needed_chunks();
    for entry in tokio::fs::read_dir(&cache_dir).await? {
        let chunk_id = parse_chunk_id(entry.file_name())?;
        if !needed.contains(&chunk_id) {
            tokio::fs::remove_file(entry.path()).await?;
            registry.mark_evicted(chunk_id);
        }
    }
}
```

**Edge cases:**
- Endpoint deregistered → its window's chunks become unreferenced →
  evicted next tick.
- Operator increases `cache_delay_secs` mid-event → re-register with new
  window size; eviction retains larger range from next tick.
- Empty registry between events → eviction clears entire cache_dir.
- All endpoints frozen on same chunk → window above each frozen position
  is fixed at W chunks → no growth.

**Disk-usage invariant:**
```
disk_used ≤ n_endpoints × W × max_chunk_size
         ≤ 6 × 60 × 5 MB
         = 1.8 GB worst case
```
Well within 80 GB SSD regardless of event length / endpoint divergence.

## 7. Error handling + resilience

**S3 fetch errors:**
- 404: `registry.mark_not_found(N)`. Reader's `wait_for_chunk(N)` returns
  `Err(NotFound)` → existing `consecutive_chunk_misses` skip-ahead +
  `chunk_gap` stall_reason logic preserved.
- 5xx / network: per-error-class backoff, audit via existing
  `emit_s3_fetch_failed` (#173).
- Continuous 503 (rate limit): bandwidth limiter cuts in-flight count;
  audit `disk_cache_download_throttled`.

**Disk write errors:**
- ENOSPC: unexpected (1.8 GB invariant); emit `Severity::Error` audit
  `disk_cache_write_failed`. DownloadService pauses, retry-with-backoff.
  EvictionTask continues; once enough freed, retry succeeds.
- EIO / corrupt: atomic rename ensures partial files never visible to
  readers. Re-fetch on next request.

**Reader stall (chunk never arrives):**
- `wait_for_chunk(N)` 60 s timeout → reader sets
  `stall_reason = "chunk_wait_timeout"` → triggers existing skip-ahead
  HEAD probing → advance + audit `chunk_gap_skipped`.

**Cache-miss race** (eviction deletes file between availability check
and read): `tokio::fs::read` returns ENOENT → reader treats as
cache-miss, re-requests download. Loops at most twice. Defense:
RwLock per chunk_id, eviction takes write-lock to delete; readers take
read-lock to read.

**RtmpPusher errors:** unchanged. Reader catches errors, applies same
restart_state logic as today's `consumer_task`.

**Process crash / restart:** cache_dir persists on local SSD across
process restarts (NOT VPS reboot). On startup, scan cache_dir → populate
registry with already-on-disk chunks. Eviction's first tick clears stale.

**VPS reboot / fresh spawn:** cache_dir empty. Cold start, ~2 s pre-fill
at LAN speed.

**Bandwidth cap config:** default `S3_INGRESS_CAP_MBIT = 200`,
configurable via env. Outbound RTMP per endpoint = 12 Mbit × 6 = 72 Mbit;
leaves ~700 Mbit on cpx32's 1 Gbit interface.

## 8. Operator visibility

**New audit events** (extend `rs_core::audit::Action`):

```rust
DiskCachePrefillStarted     // event start, first endpoint registered
DiskCachePrefillReady       // window populated, first push imminent
DiskCacheChunkEvicted       // rate-limited summary (1/min): N evicted
DiskCacheDownloadThrottled  // bandwidth cap hit; sustained S3 latency
DiskCacheStallTimeout       // wait_for_chunk timed out
DiskCacheWriteFailed        // ENOSPC / EIO
DiskCacheReaderRecovered    // post-stall, push resumed
```

Emitted via `endpoint_audit::emit_*` helpers (mirror existing
`emit_s3_fetch_failed`); rate-limited per-class via a generalized
`AuditRateLimiter`.

**New dashboard fields** in `EndpointStatusEntry` (rs-delivery)
→ `EndpointDeliveryStatus` (host) → `DeliveryEndpointEntry` (JSON):

```rust
struct DiskCacheStats {
    cached_chunks_in_window: u32,
    window_target_chunks: u32,
    cache_dir_bytes: u64,
    download_in_flight: u32,
    s3_ingress_mbps_recent: f64,
}
```

**Leptos UI** extensions:
- Cache fill bar per endpoint card
  (`cached_chunks_in_window / window_target_chunks`)
- "downloads x/N" badge alongside existing "ffmpeg xN" / "reconn xM"
- Tooltip: `cache_dir_bytes` + `s3_ingress_mbps_recent`

**Activity feed** new event types render with disk-cache icon class.

**Pacing-diagnostics panel:** per-event row showing total cache_dir
bytes + total downloads/sec. Confirms "no S3 RTT in hot path" during
soak.

## 9. Testing strategy

**Unit tests** (workspace-runnable, native):

1. **registry.rs**
   - `wait_for_chunk` blocks until `mark_available` (`tokio::test(start_paused = true)`)
   - Concurrent waiters all wake on single `mark_available`
   - `mark_evicted` causes new `wait_for_chunk` to behave as missing
   - 60 s timeout fires when chunk never arrives
2. **download_service.rs** with mock S3 client
   - Dedup: 6 concurrent `request_chunk(N)` → exactly 1 S3 GET
   - Bandwidth cap: 5 concurrent fetches × 5 MB at 200 Mbit/s ≥ 1 s
   - 404 → `mark_not_found`, no retry storm
   - 5xx → exponential backoff, audit emit (rate-limited)
3. **eviction.rs** with mock filesystem
   - Empty registry → all files evicted
   - 6 endpoints in disjoint windows → only union retained
   - Endpoint window expansion → preserved chunks survive next tick
4. **endpoint_reader.rs** with mock pusher + mock registry
   - Chunk arrives → push called with file content
   - Skip-ahead on missing → advances chunk_id, audit emitted
   - Pusher error → respects existing restart logic (parity with today's
     `consumer_task`)
5. **position_registry.rs** pure: register/advance/snapshot/needed_chunks

**Integration tests** (`crates/rs-delivery/tests/`):

1. `disk_cache_e2e.rs` — tempdir, mock S3, 1 EndpointReader for 30 chunks
2. `disk_cache_dedup.rs` — 6 readers at chunk 0, mock S3 with hit-counter
3. `disk_cache_s3_outage.rs` — 503 for 90 s mid-stream; reader keeps
   pushing from disk; resumes after
4. `disk_cache_disjoint_windows.rs` — endpoint A at chunk 100, B at 7000;
   only their windows on disk
5. `disk_cache_eviction.rs` — endpoint advances 0→1000; disk usage stays
   bounded

**Mutation testing:** `rs-delivery::disk_cache::*` included in
`cargo-mutants` config, no `--exclude-re`.

**Loopback soak test:** extends `local_xiu_loopback.rs` pattern; e2e
pusher + disk cache + xiu server, asserts RTMP push survives a 30 s
simulated S3 outage at the storage layer.

**Performance-regression gate:** `disk_cache_steady_state_throughput`
measures sustained MB/s read disk → pusher; CI fails if regresses
> 10% from baseline.

**No CI E2E changes required for v1**: existing CI E2E tests run a real
stream end-to-end and exercise the disk cache transparently.

## 10. Out of scope (follow-ups)

- Encryption at rest on `/var/cache` (chunks already public-read on S3).
- Cross-VPS cache sharing (each VPS independent for now).
- Multi-event cache co-location (per-event UUID prefix already isolates).
- Disk-cache stats persistence across VPS reboot (each spawn starts cold).
- Alternative storage (e.g. NVMe RAID, network attached) — cpx32 SSD is
  sufficient at 1.8 GB worst case.

## 11. Acceptance

- 4-hour soak with 6 endpoints (4 YT_RTMP + 2 FB_RTMPS) passes with
  zero `endpoint_rtmp_push_died` events caused by S3-side issues.
- Operator dashboard shows cache fill bars at ~100% within 15 s of each
  endpoint start.
- No NIC-saturation regression: outbound RTMP write timeouts stay at
  zero across the soak.
- All audit events from §8 fire during synthetic-fault tests as
  designed.
- Mutation testing passes with `disk_cache::*` included in scope.
- File-size gate: every `disk_cache/*.rs` file under 1000 lines.
