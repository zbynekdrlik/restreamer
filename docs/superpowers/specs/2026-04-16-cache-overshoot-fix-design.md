# Cache Overshoot Fix — Design Spec

**Issue:** #122 — Cache overshoot during VPS initialization: 172s→120s impossible jump

## Problem

Two issues cause cache overshoot during VPS initialization:

1. **Metric overshoot**: During warmup (`delivered_up_to == 0`), cache = `SUM(all sent)` which grows unbounded past target.
2. **Content overshoot**: If VPS boot exceeds the cache target, or OBS started before Start Delivering, more content exists than the target allows. Starting from chunk 1 gives a cache far above target.

## Fix — Two layers

### Layer 1: Metric cap during warmup

`get_cache_duration_secs` caps result at `target_secs` when `delivered_up_to == 0`. Smooth 0→target growth on dashboard during boot/warmup.

### Layer 2: Live-edge start chunk

`compute_target_start_chunk` walks backwards from latest sent chunk, accumulating `duration_ms` until `>= target_ms`. Returns the sequence number that gives exactly the target cache.

- **Content ≤ target** (normal): Returns `first_seq`. VPS warmup waits for more content. Rescue video plays.
- **Content > target** (VPS boot exceeded target, or OBS started early): Returns a later chunk. VPS skips old content to achieve exact target cache.
- **Any cache target** (30s, 120s, 300s): Works correctly regardless of target size.

### Scenarios

| Scenario | Content at VPS ready | Target | start_chunk_id | Cache |
|----------|---------------------|--------|----------------|-------|
| Normal (boot < target) | 90s | 120s | chunk 1 | warmup waits 30s more → 120s |
| Boot > target | 150s | 120s | chunk ~8 | 120s from live edge |
| OBS started 3min early | 270s | 120s | chunk ~38 | 120s from live edge |
| Small cache | 90s | 30s | chunk ~16 | 30s from live edge |
| Exact match | 120s | 120s | chunk 1 | 120s |

## Changes

### `crates/rs-core/src/db/mod.rs`

1. `get_cache_duration_secs` — added `target_secs` param, caps when `delivered_up_to == 0`
2. `compute_target_start_chunk` — new function, walks backwards from latest to find target buffer start

### `crates/rs-api/src/delivery.rs`

- `poll_and_init`: replaced `get_first_sequence_number_for_event` with `compute_target_start_chunk`

### `crates/rs-api/src/lib.rs`

- Updated both `get_cache_duration_secs` callers to pass `target_secs`

### Unit tests (`crates/rs-core/src/db/tests.rs`)

- `cache_duration_capped_at_target_during_warmup` — cap, no-cap, playing modes
- `compute_target_start_chunk_returns_first_when_content_below_target` — normal warmup
- `compute_target_start_chunk_skips_old_when_content_exceeds_target` — boot > target
- `compute_target_start_chunk_exact_match` — exact content = target

## What does NOT change

- Rescue video plays during warmup — unchanged
- VPS warmup logic on VPS side — unchanged
- `/api/init` timing — unchanged (rescue bypass stays for fast VPS creation)

## Acceptance criteria

- Cache grows monotonically 0→target during initialization (no overshoot past 130%)
- Cache never drops more than 10s between consecutive 5s polls
- Works for VPS boot < target, VPS boot > target, and OBS-started-early scenarios
- Works with any cache target value (30s, 120s, 300s)
- CI E2E init-phase + impossible-jump detection catches regressions
