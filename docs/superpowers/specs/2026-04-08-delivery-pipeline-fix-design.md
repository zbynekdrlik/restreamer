# Delivery Pipeline Fix — S3 Key Redesign + Pre-fetch Buffer + CI Hardening

**Issues:** #94 (umbrella), #95, #96, #97, #98, #99

**Problem:** S3 migration from Linode to Hetzner broke YouTube streaming quality. Root cause is twofold: (1) S3 LIST on every chunk fetch adds latency, (2) the delivery pipeline is fully sequential with no pre-fetch buffer — any S3 latency spike starves ffmpeg.

**Goal:** Eliminate S3 LIST from the hot path, add a pre-fetch buffer to decouple S3 latency from ffmpeg consumption, and harden CI gates to catch quality regressions.

---

## Part 1: S3 Key Format Change (#99a)

### Current State

Chunk key format: `{event_id}/{seq}_{duration_ms}_{event_id}.bin`

Because `duration_ms` varies per chunk (I-frame alignment), the VPS cannot construct the key without knowing the duration. Every fetch does:
1. S3 LIST with prefix `{event_id}/{seq}_` to discover the full key
2. Parse `duration_ms` from the discovered key
3. S3 GET with the discovered key

Two HTTP requests per chunk. Hetzner S3 LIST is slower than Linode, causing ffmpeg starvation.

### New Design

**Key format:** `{event_id}/{seq}.bin`

Always constructable from `event_id` and `seq` number. No LIST needed.

**Duration storage:** S3 object custom metadata header `x-amz-meta-duration-ms`.

Set during upload via `bucket.add_header("x-amz-meta-duration-ms", &duration_ms.to_string())` before the PUT call. The rust-s3 crate's `Bucket::extra_headers` field and `add_header()` method support this. After the PUT, remove the header so it doesn't leak to subsequent requests (or use a cloned bucket).

**Reading duration:**

- **During delivery GET:** `ResponseData::headers()` returns `HashMap<String, String>` which includes `x-amz-meta-duration-ms`. One GET returns both data and duration — zero extra cost.
- **During buffer fill probing:** `Bucket::head_object()` returns `ResponseData` with headers but no body. Parse `x-amz-meta-duration-ms` from the HEAD response. One tiny request per probe instead of LIST + full GET.

### Upload Side Changes

**File:** `crates/rs-endpoint/src/s3.rs`

```rust
// Before (current)
pub fn chunk_key(event_identifier: &str, sequence_number: i64, duration_ms: i64) -> String {
    format!("{event_identifier}/{sequence_number}_{duration_ms}_{event_identifier}.bin")
}

// After
pub fn chunk_key(event_identifier: &str, sequence_number: i64) -> String {
    format!("{event_identifier}/{sequence_number}.bin")
}
```

Upload method sets metadata header before PUT, then clears it:

```rust
pub async fn upload_chunk(&self, local_path: &Path, event_id: &str, seq: i64, duration_ms: i64) -> Result<(), EndpointError> {
    let key = Self::chunk_key(event_id, seq);
    // Clone bucket to set per-request metadata without leaking to other calls
    let mut upload_bucket = (*self.bucket).clone();
    upload_bucket.add_header("x-amz-meta-duration-ms", &duration_ms.to_string());
    // ... upload with upload_bucket ...
}
```

All callers of `chunk_key()` must be updated to drop the `duration_ms` parameter.

### Delivery Side Changes

**File:** `crates/rs-delivery/src/s3_fetch.rs`

`fetch_chunk_with_meta()` becomes:

```rust
pub async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<ChunkData>, S3FetchError> {
    let key = format!("{}/{}.bin", self.event_identifier, chunk_id);
    match self.bucket.get_object(&key).await {
        Ok(response) if response.status_code() == 200 => {
            let duration_ms = response.headers()
                .get("x-amz-meta-duration-ms")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            Ok(Some(ChunkData {
                data: response.to_vec(),
                duration_ms,
            }))
        }
        Ok(response) if response.status_code() == 404 => Ok(None),
        // ... error handling unchanged ...
    }
}
```

New `head_chunk_duration()` using HEAD (no data download):

```rust
pub async fn head_chunk_duration(&self, chunk_id: i64) -> Result<Option<i64>, S3FetchError> {
    let key = format!("{}/{}.bin", self.event_identifier, chunk_id);
    // head_object returns (HeadObjectResult, status_code)
    // HeadObjectResult has content_length, last_modified, custom metadata, etc.
    // For custom metadata, we may need to inspect response headers directly.
    // Implementation must verify exact rust-s3 head_object return type at build time.
    match self.bucket.head_object(&key).await {
        Ok((head, code)) if code == 200 => {
            // Extract x-amz-meta-duration-ms from head object result
            // Exact field access depends on rust-s3 HeadObjectResult structure
            let duration_ms = /* parse from head metadata */ 0_i64;
            Ok(Some(duration_ms))
        }
        Ok((_, code)) if code == 404 => Ok(None),
        Ok((_, code)) => Err(S3FetchError::Fetch(format!("HEAD status {code}"))),
        Err(e) => {
            if e.to_string().contains("404") { Ok(None) }
            else { Err(S3FetchError::Fetch(e.to_string())) }
        }
    }
}
```

Note: The exact `head_object` return type and custom metadata access must be verified against the rust-s3 0.35.1 API during implementation. If `HeadObjectResult` doesn't expose custom metadata, fall back to a range GET of 0 bytes with custom headers or use `get_object` with a range request for an empty range to get headers only.

### Migration

No backward compatibility needed. Existing chunks on S3 are from test streams and can be deleted. The bucket will be clean when the new code deploys.

---

## Part 2: Producer-Consumer Pipeline (#99b)

### Current State

`endpoint_loop` in `endpoint_task.rs` is a single sequential loop:

```
loop {
    S3 GET chunk N        ← blocks 100-300ms
    FLV normalize         ← ~0ms
    write to ffmpeg stdin ← blocks until pipe accepts
    yield_now()
}
```

The only buffer between S3 and ffmpeg is the 64KB OS pipe. At 2Mbps, that's ~32ms of video. Any S3 fetch taking >32ms starves ffmpeg.

### New Design

Split into three concurrent tasks communicating through a bounded tokio channel:

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  S3 Fetcher │────▶│  Chunk Buffer    │────▶│  ffmpeg Writer  │
│  (producer) │     │  (channel, 10)   │     │  (consumer)     │
└─────────────┘     └──────────────────┘     └─────────────────┘
```

**Producer task (S3 fetcher):**
- Runs as a `tokio::spawn` task
- Maintains `next_fetch_id` counter, always tries to stay ahead of consumer
- Fetches `{evt}/{next_fetch_id}.bin` via direct GET
- Sends `ChunkData { data, duration_ms, chunk_id }` into the bounded channel
- If channel is full (10 items), `.send()` awaits — natural backpressure
- If chunk not found (404), waits 2s and retries (live edge)
- S3 error backoff logic (exponential, same as current)
- Listens for stop signal

**Buffer (bounded channel):**
- `tokio::sync::mpsc::channel(10)` — holds ~10 chunks = ~20 seconds of video
- Backpressure: producer blocks when buffer full (caught up to live edge)
- Starvation: consumer blocks when buffer empty (S3 falling behind)
- 20 seconds of resilience against S3 latency spikes or brief network outages

**Consumer task (ffmpeg writer):**
- Runs as a `tokio::spawn` task
- Pulls `ChunkData` from channel receiver
- Applies FLV normalization
- Writes to ffmpeg stdin with timeout
- Handles ffmpeg crashes, restarts, circuit breaker (same logic as current)
- Updates stats (chunk_id, bytes_processed, etc.)
- Never makes S3 calls — zero network I/O in the write path

**Buffer fill (startup):**
- Before spawning producer/consumer, buffer fill runs using HEAD requests
- Probes sequential chunks with `head_chunk_duration()` until `delivery_delay_ms` accumulated
- Same logic as current, but HEAD instead of LIST+GET (much faster)

**Drought mode:**
- If consumer sees channel closed or empty for too long (MAX_CHUNK_MISS_COUNT equivalent), it enters drought mode
- Producer detects drought via shared state, does skip-ahead probing
- On recovery, producer signals re-buffer needed

### Channel Item Type

```rust
struct PrefetchedChunk {
    chunk_id: i64,
    data: Vec<u8>,
    duration_ms: i64,
}
```

### Concurrency Model

```rust
let (tx, rx) = tokio::sync::mpsc::channel::<PrefetchedChunk>(10);

let producer = tokio::spawn(producer_task(fetcher, tx, start_chunk_id, stop_rx.clone()));
let consumer = tokio::spawn(consumer_task(rx, factory, ep_cfg, stats, stop_rx));

// Wait for both to finish
let _ = tokio::join!(producer, consumer);
```

### Stats Tracking

Both producer and consumer share `Arc<Mutex<EndpointStats>>`. Producer updates fetch-related stats. Consumer updates delivery-related stats. The existing `EndpointStats` struct works unchanged.

---

## Part 3: CI Hardening (#95, #96, #98)

### #95 — Second YouTube Health Gate

Add a health gate step after the resilience tests (OBS disconnect, network disconnect, crash recovery). By this point the stream has been running 10+ minutes. Any quality degradation from the delivery pipeline would be visible.

**Configuration:** Strict and fast — 5 retries, 10s delay (~50s max). The stream should already be healthy. If not after resilience tests, it's broken.

**Position in ci.yml:** After the crash recovery test, before deactivation/cleanup.

### #96 — bitrateLow Gate

Extend both health gates (first and second) to inspect `configuration_issues` on each active stream. If any issue contains `bitrateLow`, the gate fails — even if `health_status` is "good".

```powershell
# After confirming health_status == "good", also check:
foreach ($s in $response.streams) {
    if ($s.configuration_issues) {
        foreach ($issue in $s.configuration_issues) {
            if ($issue -match "bitrateLow") {
                throw "FAILED: YouTube reports bitrateLow in configurationIssues"
            }
        }
    }
}
```

### #97 — OBS Bitrate Verification

Already implemented in CI (lines 1487-1546). Verified: reads active OBS profile, checks `streamEncoder.json` bitrate, sets to 12000 if different, restarts OBS. Restore step at line 3333. No changes needed.

### #98 — OBS Scene Selection

Add a CI step before the YouTube E2E test that sets OBS to the "test" scene (drone flight with high-motion video). This scene exposes bitrate/quality degradation that static test sources hide.

**Implementation:** Use the OBS WebSocket protocol via PowerShell (the CI runner is on stream.lan where OBS runs). The OBS WebSocket plugin provides `SetCurrentProgramScene` request. Alternatively, use the `obs-stream-snv` MCP server's `obs_scene` tool.

**Restore:** Save the original scene name before changing, restore in cleanup.

---

## Testing Strategy

### Unit Tests
- `chunk_key()` format change — verify new format `{evt}/{seq}.bin`
- `parse_chunk_key()` in db.rs — update or remove (no longer parsing from key)
- FLV normalizer — unchanged, existing tests remain
- Producer/consumer channel — mock S3 fetcher, verify ordering and backpressure

### Integration Tests
- E2E endpoint_loop with mock fetcher — verify producer/consumer pipeline delivers chunks in order
- Verify HEAD request returns duration from metadata
- Verify GET returns both data and duration

### CI E2E
- Full pipeline: OBS → RTMP → chunks → S3 (new key format) → VPS (pipeline) → YouTube
- First health gate: health == "good", no bitrateLow
- Resilience tests (OBS disconnect, network disconnect, crash recovery)
- Second health gate: health == "good", no bitrateLow (strict, fast)

---

## Request Flow Comparison

| Operation | Before | After |
|-----------|--------|-------|
| Chunk delivery | LIST + GET (2 req, sequential) | GET only (1 req, pre-fetched) |
| Buffer fill probe | LIST + GET entire chunk (2 req, wastes bandwidth) | HEAD only (1 req, metadata only) |
| After network recovery | LIST to discover key, sequential | Direct GET, pre-fetch buffer refills |
| ffmpeg feed | Direct from S3 fetch (0 buffer) | From 10-chunk pre-fetch buffer (~20s) |
| S3 latency spike tolerance | ~32ms (OS pipe buffer) | ~20s (application buffer) |
| CI health gates | 1 gate, no bitrateLow check | 2 gates, bitrateLow fails CI |
