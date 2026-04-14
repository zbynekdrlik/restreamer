# Buffer Rescue Mode — Design Spec

**Issue:** #62
**Date:** 2026-04-12

## Problem

When OBS stops streaming (network outage, computer restart, internet issues), the delivery buffer on the VPS drains at real-time pace. Once empty, the endpoint consumer starves. When OBS resumes, chunks arrive one-at-a-time through S3, causing a "one chunk in, one chunk out" pattern that produces constant micro-glitches for viewers — worse than a single clean outage.

Additionally, during the initial cache fill (first 120s after VPS init), endpoints sit idle with nothing playing. Viewers who tune in early see nothing.

## Solution

Add a **rescue mode** to the VPS delivery pipeline. Each endpoint has four delivery states:

| State | Condition | Behavior |
|-------|-----------|----------|
| **Warmup** | Initial start, buffer < target | Loop rescue video with countdown overlay |
| **Normal** | Buffer >= 120s | Chunk → ffmpeg → endpoint (current behavior) |
| **Rescue** | Buffer empty + no new chunks for 30s | Kill chunk ffmpeg, loop rescue video with countdown |
| **Recovering** | New chunks arriving, buffer < 120s | Continue rescue video, update countdown |

State transitions:
```
Warmup → Normal:       buffer reaches 120s of content ahead
Normal → Rescue:       prefetch channel empty + producer stalled for 30s
Rescue → Recovering:   producer detects new chunks arriving on S3
Recovering → Normal:   buffer reaches 120s of content ahead
Normal → Rescue:       can re-trigger if buffer empties again
```

## Architecture

### Where Rescue Logic Lives

**VPS-side only (rs-delivery crate).** The VPS has real-time visibility into:
- Prefetch channel depth (chunks buffered in memory)
- S3 chunk availability (producer's consecutive miss count)
- Per-endpoint ffmpeg state

The orchestrator on stream.lan polls via HTTP every few seconds — too slow for mode switching. All rescue decisions happen in `endpoint_task.rs`.

### Rescue Video

A **user-provided looped video with audio**, configured per-event in the streaming event settings. Stored at a URL accessible from the VPS (S3 bucket, public HTTP, etc.).

- Configured via dashboard event settings as `rescue_video_url`
- Stored in `streaming_events` table (new column: `rescue_video_url TEXT`)
- Passed from orchestrator to VPS via the `/api/init` payload
- If null/empty: no rescue mode — current stall behavior (ffmpeg starves, viewers see buffering)

The video should be pre-encoded in a format compatible with the target endpoints (H.264 + AAC recommended). The user creates and hosts this video themselves.

### Countdown Overlay

ffmpeg's `drawtext` filter with `textfile` and `reload=1` provides seamless countdown updates without restarting ffmpeg:

1. VPS writes countdown text to `/tmp/rescue_{endpoint_alias}.txt`
2. Producer task updates this file every 5s with current estimate
3. ffmpeg re-reads the file on every frame automatically

Countdown format: `"Stream recovering ~ 2m 15s"` (warmup: `"Stream starting ~ 1m 45s"`)

The estimate is calculated from:
- **Target:** 120s of buffer (fixed, not configurable — user chose this)
- **Current:** Sum of `duration_ms` from chunks available ahead on S3 but not yet consumed
- **Rate:** Chunk arrival rate observed over the last 30s

### Rescue ffmpeg Command

```bash
ffmpeg -stream_loop -1 -re -i <rescue_video_url> \
  -vf "drawtext=textfile=/tmp/rescue_{alias}.txt:reload=1:\
       fontsize=48:fontcolor=white:x=(w-tw)/2:y=h-80:\
       borderw=2:bordercolor=black" \
  -c:v libx264 -preset ultrafast -c:a aac -b:a 128k \
  -f flv <endpoint_rtmp_url>
```

For non-RTMP endpoints (HLS PUT to YouTube), the output format and URL change accordingly — same pattern as existing `build_ffmpeg_args()` in `endpoint_task.rs`.

### Consumer Task Changes

The consumer task (`consumer_task()` in `endpoint_task.rs`) currently runs a single loop: pull chunk from channel → write to ffmpeg. This becomes a state machine:

```rust
enum DeliveryMode {
    /// Initial buffer fill or rescue recovery — playing rescue video
    Rescue { reason: RescueReason },
    /// Normal chunk delivery
    Normal,
}

enum RescueReason {
    Warmup,       // initial buffer fill
    BufferEmpty,  // buffer drained during outage
}
```

**Rescue mode loop:**
1. Spawn rescue ffmpeg (looped video + drawtext)
2. Write countdown file every 5s
3. Monitor buffer level via shared state from producer
4. When buffer >= 120s: kill rescue ffmpeg, transition to Normal

**Normal mode loop:**
1. Current behavior: pull chunk → normalize → pace → write to ffmpeg
2. New: if channel empty for 30s (no chunk received via `tokio::select!` timeout), transition to Rescue

### Producer Task Changes

The producer task needs to expose buffer state for the consumer and countdown:

- New shared state: `buffer_duration_ms: AtomicU64` — sum of `duration_ms` for chunks fetched ahead but not yet consumed
- Updated whenever a chunk is sent to the channel or when probing S3 for available chunks
- Consumer reads this to calculate countdown and decide when to exit rescue mode

### Endpoint Stats Changes

`EndpointStats` gains:
```rust
pub delivery_mode: String,         // "warmup", "normal", "rescue", "recovering"
pub rescue_eta_secs: Option<u64>,  // countdown to resumption (None when normal)
```

This propagates through the existing `/api/status` endpoint to the orchestrator and dashboard.

### Init Payload Changes

`InitRequest` gains:
```rust
pub rescue_video_url: Option<String>,  // per-event rescue video URL
```

The orchestrator reads `rescue_video_url` from the streaming event and includes it in the `/api/init` POST.

### Database Changes

One new column on `streaming_events`:
```sql
ALTER TABLE streaming_events ADD COLUMN rescue_video_url TEXT;
```

Incremental migration (V14). No data loss, nullable column with implicit NULL default.

### Dashboard Changes

**Event settings:** New field "Rescue Video URL" — text input for the video URL.

**Endpoint status cards:** Show delivery mode badge:
- "WARMUP" (blue) with countdown
- "NORMAL" (green) — no change from current
- "RESCUE" (orange/amber) with countdown
- "RECOVERING" (yellow) with countdown

### Fast Endpoints

Fast endpoints (`is_fast: true`) skip the initial buffer fill entirely — they run near-live. Rescue mode does NOT apply to fast endpoints. If a fast endpoint's buffer empties, it simply stalls (current behavior). The rescue video would add unacceptable latency to a near-live endpoint.

## What's NOT in Scope

- Automatic rescue video generation — user provides their own
- Per-endpoint different rescue videos — one per event is sufficient
- Rescue mode for fast endpoints — incompatible with near-live delivery
- Custom rescue refill threshold — fixed at 120s
- Video format validation — user is responsible for providing compatible video

## Testing Strategy

### Unit Tests (rs-delivery)
- State machine transitions: Warmup → Normal, Normal → Rescue → Recovering → Normal
- Countdown calculation from buffer duration
- Rescue skipped when `rescue_video_url` is None
- Fast endpoints never enter rescue mode

### Integration Tests
- Rescue ffmpeg spawns with correct drawtext arguments
- Countdown file is written and updated
- Mode transition kills old ffmpeg and spawns new one

### E2E (Playwright)
- Configure rescue video URL in event settings
- Dashboard shows delivery mode badges
- Verify mode transitions appear in endpoint status during simulated outage

### CI GATE
- New `/api/init` field accepted by mock-api
- Rescue video URL persisted in DB and returned by API
