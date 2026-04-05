# Duration-Aware Delivery Design

**Issue:** #82 — VPS endpoints start delivering at 240s instead of 120s cache target
**Date:** 2026-04-05

## Problem

VPS endpoints start streaming ~240s after "Start Delivering" despite a 120s cache target. The root cause is in `delivery.rs:358-360`:

```rust
let delivery_delay_chunks = (delay_secs * 1000 / chunk_duration_ms) as i64;
// = (120 * 1000 / 1000) = 120 chunks
```

This assumes 1 chunk = 1 second of content. With OBS's 2-second keyframe interval, chunks are actually ~2s each. So 120 chunks = ~240s of content, not 120s.

Both the local client and VPS delivery service use chunk counts instead of actual content duration for buffer-fill decisions.

## Design Principles

1. **Local SQLite is the offline source of truth** — must work without internet
2. **S3 chunks are self-describing** — each chunk carries its metadata in its filename
3. **VPS is self-sufficient** — learns everything from S3, no dependency on local client being online
4. **Duration, not chunk count** — all buffer/cache decisions use actual content duration (milliseconds)

## Architecture

### S3 Key Format Change

**Current:** `{event}/{seq}_{event}.bin`
**New:** `{event}/{seq}_{duration_ms}_{event}.bin`

Example: `evt-123/42_2100_evt-123.bin` (sequence 42, 2100ms content duration)

Each chunk on S3 is self-describing. The filename encodes the content duration measured from RTMP timestamps (added in PR #81). This is portable across any storage backend.

### Local Side (rs-api, rs-endpoint)

**Buffer-fill wait** (`crates/rs-api/src/delivery.rs`):

Replace chunk-count comparison with actual content duration query:

```rust
// Current (broken): waits for N chunks assuming 1 chunk = 1s
let delivery_delay_chunks = (delay_secs * 1000 / chunk_duration_ms) as i64;
// ... gap >= delivery_delay_chunks

// New: waits for actual content duration of sent (uploaded to S3) chunks
let sent_duration_ms = db::get_sent_duration_ms(&pool, event_id).await?;
if sent_duration_ms >= (delay_secs * 1000) as i64 { break; }
```

Key distinction: only count `duration_ms` of chunks with `sent = 1` (uploaded to S3). Local-only chunks are not cached yet from VPS perspective.

**S3 upload** (`crates/rs-endpoint/src/uploader.rs`, `crates/rs-endpoint/src/s3.rs`):

`S3Client::chunk_key()` changes to include `duration_ms`:

```rust
// Current
pub fn chunk_key(event_identifier: &str, sequence_number: i64) -> String {
    format!("{event_identifier}/{sequence_number}_{event_identifier}.bin")
}

// New
pub fn chunk_key(event_identifier: &str, sequence_number: i64, duration_ms: i64) -> String {
    format!("{event_identifier}/{sequence_number}_{duration_ms}_{event_identifier}.bin")
}
```

The uploader reads `duration_ms` from the `ChunkRecord` (already stored in DB from PR #81) and passes it to `chunk_key()`.

**`/api/init` payload change**: Pass `delivery_delay_ms: u64` instead of `delivery_delay_chunks: i64`. The VPS uses this as the target for its own duration-based buffer fill.

### VPS Side (rs-delivery)

**New: VPS SQLite database** (`crates/rs-delivery/src/db.rs`):

```sql
CREATE TABLE IF NOT EXISTS chunks (
    sequence_number INTEGER PRIMARY KEY,
    duration_ms     INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
```

This is a local cache of chunk metadata, populated as chunks are fetched from S3. It survives within a VPS session and can be rebuilt from S3 on crash recovery.

**S3 key parsing** (`crates/rs-delivery/src/s3_fetch.rs`):

`fetch_chunk()` now returns metadata alongside data:

```rust
pub struct ChunkData {
    pub data: Vec<u8>,
    pub duration_ms: i64,
}
```

Two fetch strategies:
1. **Known key**: If VPS already knows the duration (from DB or prior LIST), construct key directly
2. **Discovery**: S3 LIST with prefix `{event}/{seq}_` to find the full key, parse duration from filename

**Buffer-fill (initial)** — replaces chunk-existence check:

```rust
// Current: checks if target_chunk exists on S3
if let Ok(Some(_)) = fetcher.fetch_chunk(target_chunk).await { break; }

// New: fetch chunks sequentially, accumulate duration
let mut total_duration_ms: i64 = 0;
let mut next_chunk = start_chunk_id;
loop {
    if let Ok(Some(chunk)) = fetcher.fetch_chunk_with_meta(next_chunk).await {
        db::insert_chunk(&pool, next_chunk, chunk.duration_ms, chunk.data.len()).await;
        total_duration_ms += chunk.duration_ms;
        // Cache chunk data for immediate delivery (avoid re-fetching)
        chunk_cache.insert(next_chunk, chunk.data);
        next_chunk += 1;
    }
    if total_duration_ms >= delivery_delay_ms as i64 { break; }
    tokio::time::sleep(Duration::from_secs(2)).await;
}
```

**Buffer-fill (re-buffer after drought)** — same duration-based logic:

```rust
// Current: waits for chunk_id + delivery_delay_chunks to exist
// New: fetch chunks, sum duration until delivery_delay_ms reached
```

**Crash recovery**: On VPS restart/init, LIST `{event}/` prefix on S3. Parse all filenames to get `(seq, duration_ms)` pairs. Populate VPS SQLite. Resume delivery from last known position.

### Protocol Change

**`/api/init` request** (`crates/rs-delivery/src/api.rs`):

```rust
// Current
pub delivery_delay_chunks: i64,

// New
pub delivery_delay_ms: u64,
```

**`/api/endpoints/add`**: Same change — uses `delivery_delay_ms` stored in AppState.

**AppState** (`crates/rs-delivery/src/main.rs`):

```rust
// Current
pub delivery_delay_chunks: RwLock<i64>,

// New
pub delivery_delay_ms: RwLock<u64>,
pub db_pool: sqlx::SqlitePool,  // VPS-side SQLite
```

### New DB Query (Local Side)

`db::get_sent_duration_ms(pool, event_id)` — returns `SUM(duration_ms)` for chunks where `sent = 1` and belonging to the given event. Similar to existing `get_cache_duration_secs` but specifically for sent-to-S3 chunks.

## Testing

### Unit Tests

1. **S3 key format**: `chunk_key("evt-123", 42, 2100)` returns `"evt-123/42_2100_evt-123.bin"`
2. **S3 key parsing**: Parse `"evt-123/42_2100_evt-123.bin"` → `(seq=42, duration_ms=2100)`
3. **`get_sent_duration_ms`**: Insert chunks with varying `sent` status, verify only sent chunks counted
4. **VPS buffer-fill**: Mock fetcher returns chunks with known durations, verify loop exits when target_ms reached (not when chunk count reached)
5. **VPS DB rebuild from S3 LIST**: Given a list of S3 keys, verify correct parsing and DB population

### E2E Tests

1. **Cache target accuracy**: Start delivering with 120s target. Measure wall-clock time until endpoints go alive. Must be within 20% of actual content duration (not 2x).
2. **Duration in S3 keys**: After chunks are uploaded, verify S3 key format includes duration field.

### CI Gate

Existing cache stability gate (PR #81) already catches the symptom. New gate: verify endpoints go alive within `target_delay_secs * 1.5` wall-clock seconds (allows for VPS boot overhead but catches the 2x bug).

## Migration

- S3 key format change is forward-only. Old chunks without duration in filename won't be found by the new VPS fetcher.
- This is acceptable: old S3 data is from past events and not reused (each event creates fresh chunks).
- No database migration needed on local side (duration_ms column already exists from PR #81).
- VPS SQLite is created fresh on each VPS instance (ephemeral servers).

## Future Foundation

This design enables:
- **Rebroadcast**: VPS has chunk duration DB, can seek to time positions in past recordings
- **Mid-stream endpoint management**: Position new endpoints at exact time offsets using duration sums
- **VPS dashboard**: Display cache/position/timeline info from VPS SQLite
- **Multi-VPS**: Each VPS independently discovers chunk metadata from S3
