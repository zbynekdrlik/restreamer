# Phase 3 Fix Attempts — What Was Tried, What Failed

**Status:** All Phase 3 attempts reverted. The drift fix is **not included in this PR**. Phase 1 instrumentation + Phase 2 root-cause evidence **are** included, and the `drift_debug` chunk-emit logging is retained so future fix attempts have production visibility.

## Attempts

### A1. Consumer throttle `-readrate 0.994`

**Theory:** Match consumer drain rate to producer rate (0.994×). Cache stable.

**Result:** YouTube's `videoIngestionStarved: Video output low` sensor fires within seconds. Applies to both HLS and RTMP endpoints. At 9 Mbps AND 12 Mbps base bitrate.

**Evidence:**
- Baseline (no throttle) OBS → Restreamer → YouTube: `health=good`
- With `-readrate 0.994`: `health=bad, videoIngestionStarved, audioBitrateLow`

**Commits tried + reverted:** 0d3c319, 14ab6f5, 79e692f, c71fbea

### A2. `-readrate 0.994 -readrate_initial_burst 10`

**Theory:** 10-second initial burst at full rate gets YouTube past its early-warmup bitrate check, then throttle kicks in for steady-state drift correction.

**Result:** Cache stable during init (no jump). YouTube flags `videoIngestionStarved` persistently after the burst ends (~5 min into test). YouTube's tolerance is tighter than the 0.6% throttle.

**Commit tried + reverted:** 14ab6f5

### A3. `-readrate 0.998` (partial correction)

**Theory:** Reduce throttle from 0.6% → 0.2% to stay within YouTube's tolerance while still partially mitigating drift.

**Result:** Still flagged. YouTube's sensor catches even 0.2% underbitrate for sustained period. Consumer-side is architecturally blocked by YouTube's ingestion sensitivity.

**Commit tried + reverted:** 79e692f

### B1. Producer rescale — FLV tag timestamps rewritten to match wall-clock span

**Theory:** If each chunk's declared tag span equals its wall-clock production span, consumer at `-re` (native rate) drains at real-time rate. No throttle needed — YouTube sees normal bitrate.

**Result:** Cache jump in CI init phase ("cache JUMPED from 91.7s to 49.8s" with 24 chunks consumed in 5 wall-seconds — 4.8x `-re` rate). Root cause unclear.

**Diagnostic data** (from `drift_debug` log added this session):
- Chunks are 2s each (keyframe-aligned to OBS `keyint_sec=2`), not 1s.
- `wall_span_ms` values are ~2000ms, matching `tag_span_ms` within 1%.
- So rescale is near-no-op — nearly matches untouched timestamps.
- YET the e2e-hls endpoint shows cache draining at 2-5x `-re` rate when rescale is applied.

**Hypothesis (unconfirmed):** HLS muxer interacts badly with rewritten FLV timestamps. The ffmpeg `-f hls -hls_time 2 -hls_segment_type mpegts` pipeline may take cues from input tag intervals and produces output segments faster than real-time when tag cadence is uniform (2000ms exactly) vs natural jitter.

**Commits tried + reverted:** 0420268 (with guards), 7e3564e (without guards), e2a8122 (refined guards)

## What's in the PR (after all reverts)

### Phase 1 — Instrumentation (kept)

- V20 migration: `chunk_records.wall_clock_written_at_ms`, `clock_skew_samples`, `ffmpeg_progress_samples`
- `rs_core::db::drift::*` helpers for inserts + derived-rate queries
- `FlvChunker` stamps `wall_clock_written_at_ms` at chunk emit
- `rs-ffmpeg` parses stderr `time=` and emits `FfmpegProgress` on bounded `mpsc::Sender`
- `rs-delivery` exposes `/clock` endpoint + `ProgressRing` in `/api/status?progress_since=<cursor>`
- `rs-api` spawns per-delivery clock-skew probe + progress-poll in `poll_and_init`
- Leptos `PacingPanel` component + `/api/v1/diagnostics/pacing` endpoint
- `drift_debug` chunk-emit trace log (`tag_span_ms`, `wall_span_ms`, `buffer_size`) — **retained for future investigation**

### Phase 2 — Evidence (kept)

- 82-min live test, 1125 producer samples + 86 clock-skew samples
- Producer rate mean = 0.994, clock skew = 9.4 ppm (negligible)
- Root cause documented: OBS stamps FLV at 1/30 s but captures at ~30.30 fps → timestamps 0.6% slow
- Evidence JSON + analysis saved at `docs/superpowers/specs/2026-04-23-phase2-evidence/`

### Phase 3 — Fix (NOT in this PR)

All attempts reverted. The drift fix requires a different approach that:
1. Does not reduce consumer bitrate (YouTube blocks).
2. Does not modify FLV tag timestamps in ways that change HLS segment timing.

Possible next approaches (for a future PR):
- **Boost OBS bitrate further** (e.g., 15-18 Mbps) so even ~1% consumer throttle stays within YouTube's tolerance. Operator decision.
- **Fix xiu ingest** to stamp frames with actual arrival wall-clock instead of propagating OBS's declared-rate timestamps. More invasive but addresses the root cause at its source.
- **Tighter OBS clock sync** — use OBS plugins or Windows time service tuning to pin OBS's internal clock to real wall-clock.
- **Raise cache target** from 120s to 180s — not a fix but a safety margin: even with 18 s/hour drift, a 4h stream stays above 80s cache. Pragmatic workaround.

## What the instrumentation tells us for next time

The `drift_debug` log (`FLV chunk emit chunk_index=N tag_span_ms=X wall_span_ms=Y buffer_size=Z`) gives per-chunk visibility. Useful for any future fix attempt:
- Compare tag_span and wall_span to confirm the drift rate in real-time.
- Detect chunk size variations (burst vs steady-state).
- Observe effects of any producer or ingest change immediately.

The `/api/v1/diagnostics/pacing` endpoint exposes all three drift time-series (producer rate, consumer rate, clock skew) to the dashboard's `PacingPanel`. Operators can see drift live without re-reading logs.
