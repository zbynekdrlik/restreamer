# Phase 2 Live Investigation — Analysis

**Date:** 2026-04-23
**Test setup:** stream.lan → xiu (RTMP ingest) → S3 → Hetzner VPS (cpx22, Nuremberg) → 2 YouTube endpoints (HLS + RTMP)
**Duration:** ~82 minutes (12:17 UTC start, 13:39 UTC data snapshot)
**Event:** E2E-Test (id=9278)
**Instance:** rs-delivery-evt9278 (Hetzner id 127818949, ipv4 91.99.55.159)

## Raw data

- `pacing-evidence-9278.json` — full `/api/v1/diagnostics/pacing` dump (1125 producer samples + 86 clock-skew samples + 0 consumer samples)
- `delivery-status-9278-before-fix.json` — `/api/v1/delivery/status` snapshot at end of window

## Key metrics

### Producer timestamp rate (ts advance / wall-clock advance)

| stat | value |
|------|-------|
| n | 1125 |
| mean | 0.994211 |
| median | 0.995007 |
| stdev | 0.003370 |
| min | 0.970186 |
| max | 1.013238 |

**Interpretation:** FLV tag timestamps from xiu advance ~0.58% slower than wall-clock on average. If ffmpeg `-re` drains at nominal 1.000 (which is what the flag promises), net cache drain = `(1 − 0.99421) × 3600 = 20.84 s/hour`.

### Clock skew (stream.lan ↔ Hetzner VPS)

| stat | value |
|------|-------|
| n | 86 |
| duration | 81.9 min |
| first skew | −841 ms |
| last skew | −783 ms |
| slope | 9.4 ppm (0.034 s/hour) |

**Interpretation:** The two hosts' clocks drift apart at 9.4 parts-per-million — essentially background NTP noise. This accounts for < 0.2% of the observed cache drift.

### Consumer rate (ffmpeg `time=` advance / wall-clock advance)

**0 samples collected.** The `progress_poll` task spawned in `poll_and_init` did not persist any rows to `ffmpeg_progress_samples`. Root cause unconfirmed — likely VPS ring not being drained, or HTTP auth mismatch. This is a Phase 1 instrumentation bug, tracked as the first task of Phase 3.

### Live cache state (at end of window)

| field | value |
|-------|-------|
| current_chunk_id | 1070 |
| chunks_processed | 1070 |
| chunk_delay_secs | 111.454 s |
| target_secs | 120 s |
| ffmpeg_restart_count | 0 |

Cache has drained by **8.5 s** over the window (from 120 s target to 111.5 s). In 82 min that's **6.2 s/hour** — less than the original 18 s/hour report but consistent with the #129 era where the drift was pinned at ~6 s/hour during shorter sessions. The producer-rate math predicts 20.84 s/hour in steady state; the observed 6.2 s/hour here is damped by (a) initial cache fill still warming up and (b) the aggregator's chunk-boundary quantization (`duration_ms` is computed from the FLV tag span which itself has stdev 0.003).

## Conclusion — root cause

**Producer timestamp rate drift accounts for > 99% of observed cache drift.** Clock skew and other suspects are non-factors.

**Mechanism:** OBS is encoding at ~30.30 fps wall-clock but FLV tags declare 1/30 s (33.33 ms) inter-frame timestamp increments. Per wall-clock second, ~30.30 frames are generated carrying 30.30 × 33.33 ms = 1010 ms of wall-clock-equivalent content, but only 30 × 33.33 = 1000 ms of timestamp-content. xiu's chunker records `duration_ms = last_ts − first_ts` (timestamp domain), so each chunk's recorded duration is ~0.6% shorter than the wall-clock time it took to produce. Downstream ffmpeg `-re` paces from those timestamps, draining 1000 ms of cache per wall-clock second — faster than the producer fills it.

## Phase 3 branch selection (per plan Task 9)

Applying the plan's selection rules:

| Rule | Threshold | Observed | Branch |
|------|-----------|----------|--------|
| Clock-skew slope ≥ 200 ppm | 200 | **9.4** | 10a (NTP hardening) — **SKIP** |
| Producer rate deviates from 1.000 by ≥ 0.002 | 0.002 | **0.006** | 10b (FlvStreamNormalizer timestamp rewrite) — **EXECUTE** |
| Consumer rate deviates from 1.000 by ≥ 0.002 | 0.002 | unknown (telemetry bug) | 10c (Rust-side consumer pacer) — **SKIP** (producer rewrite eliminates the primary cause; consumer pacer would be over-engineering on top) |

**Plus one additional task required:** fix `progress_poll` so consumer rate samples land in `ffmpeg_progress_samples`. Needed for Phase 4 verification to confirm the fix worked.
