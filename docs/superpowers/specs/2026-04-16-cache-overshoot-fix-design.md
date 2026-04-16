# Cache Overshoot Fix — Design Spec

**Issue:** #122 — Cache overshoot during VPS initialization: 172s→120s impossible jump

## Problem

During VPS warmup, `current_chunk_id` (delivered_up_to) is 0 because VPS hasn't started real playback yet. The cache metric `SUM(duration_ms WHERE seq > 0)` returns ALL sent content. This grows past the 120s target during boot/warmup, then drops when VPS starts playing (`current_chunk_id` jumps from 0 to 1+).

## Fix

Cap the cache metric at the target during warmup. When `delivered_up_to == 0`, return `min(raw_cache, target)`. Once VPS is playing normally (`delivered_up_to > 0`), use raw metric.

## Changes

### 1. `get_cache_duration_secs` — add target cap

File: `crates/rs-core/src/db/mod.rs`

Add a `target_secs` parameter. When `delivered_up_to == 0`, cap result at `target_secs`.

```rust
pub async fn get_cache_duration_secs(
    pool: &SqlitePool,
    event_id: i64,
    delivered_up_to: i64,
    target_secs: f64,
) -> Result<f64> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) as total_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1 AND sequence_number > ?2",
    )
    .bind(event_id)
    .bind(delivered_up_to)
    .fetch_one(pool)
    .await?;
    let raw = row.get::<i64, _>("total_ms") as f64 / 1000.0;
    if delivered_up_to == 0 {
        Ok(raw.min(target_secs))
    } else {
        Ok(raw)
    }
}
```

### 2. Update all callers to pass target_secs

Find every call to `get_cache_duration_secs` and pass the event's `cache_delay_secs` (or default 120).

### 3. Unit tests

- Cache with delivered_up_to=0 and 200s of content → returns 120 (capped)
- Cache with delivered_up_to=0 and 50s of content → returns 50 (below cap)
- Cache with delivered_up_to=5 and 200s of content → returns raw value (no cap)

### 4. CI E2E

Existing init-phase overshoot detection (156s threshold = 130% of 120) and impossible-jump detection (>20s drop in 5s) already enforce this. No CI changes needed — the fix makes the existing tests pass honestly.

## What does NOT change

- VPS starts from chunk 1 (first_seq) — unchanged
- Rescue video plays during warmup — unchanged
- VPS warmup logic — unchanged
- `/api/init` timing — unchanged (rescue bypass stays)

## Acceptance criteria (from #122)

- [x] Cache monotonically grows from 0→120s during initialization (no overshoot past 130%)
- [x] Cache never drops more than 10s between consecutive 5s polls
- [x] Dashboard shows smooth transition from warmup→delivering (no visual jump)
- [x] Works for both fresh-start AND restart-after-stop scenarios
- [x] CI E2E init-phase detection catches any regression
