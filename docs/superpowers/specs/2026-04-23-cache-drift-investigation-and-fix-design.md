# Cache Drift Investigation & Fix — Design

**Date:** 2026-04-23
**Issue:** [#135](https://github.com/zbynekdrlik/restreamer/issues/135) — Cache delay drifts ~18s/hour on long streams
**Target version:** 0.3.67 → 0.3.68
**Branch:** `dev` → PR → `main`

---

## Problem

Over a 3-hour continuous delivery test on 2026-04-22 (OBS 4K → stream.lan → xiu → S3 → Hetzner VPS → 4 YT_RTMP endpoints), the cache depth drifted linearly downward:

| t | cache delay |
|---|---|
| 0 | 119s |
| 30m | 109s |
| 60m | 101s |
| 90m | 91s |
| 120m | 82s |
| 150m | 72s |
| 180m | 62s |

Rate: **~18s/hour, linear, no acceleration**. At this rate a 6.5h stream would hit cache=0 and stall the delivery pipeline. This matters for long events (concerts, special services) that exceed the typical ~4h Sunday window.

The drift existed before #129 (32s ffmpeg death cycle) but was hidden — the ffmpeg restart reset the pacing anchor every 32s so drift never had time to accumulate visibly. Now that delivery survives multi-hour streams, the slow drift is exposed.

## Non-goals

- Raising the cache target as a workaround.
- Fixing anything in ffmpeg upstream.
- Changes to OBS encoding profile (that would mask a subtle bug rather than fix it).

## Root cause — unknown, requires data

Four plausible causes, each capable of producing linear drift:

1. **Clock skew** between stream.lan (producer host) and the Hetzner VPS (consumer host). NTP over public internet can easily drift 100-500ppm. 500ppm = 1.8s/hour skew per pair. If stream.lan clock runs fast and VPS clock runs slow, producer timestamps advance faster than consumer wall-clock — cache shrinks.
2. **Producer timestamp rate ≠ wall-clock rate.** If OBS stamps frames at 1/30 intervals but actually captures at 30.30 fps of wall-clock, each chunk's timestamp span differs from its wall-clock lifetime. This is what the issue body speculated about; it remains a hypothesis.
3. **ffmpeg `-re` inaccuracy.** `-re` claims to read at 1× native rate, but TCP pipe backpressure, bursty writes from the consumer task, and internal scheduling jitter can make the effective drain rate deviate from 1.000×. Bounded, but if the deviation is persistent rather than mean-zero, it causes drift.
4. **YouTube ingest back-pressure.** If YouTube applies upstream rate limits, ffmpeg stalls its output and eventually stdin backs up — which would cause irregular drops, not linear drift. Can be ruled out via data.

**Without data, any single-scalar fix (`-readrate 0.995`, hardcoded constant) is a guess that rots silently when any of the four suspects shifts — including across different Hetzner hosts whose CPU crystals drift differently.**

## Design

One PR, four phases in sequence:

### Phase 1 — Instrumentation

Add three measurement layers to the existing code so we can directly observe which of the four suspects is responsible.

#### 1a. Producer wall-clock per chunk

`chunk_records` gains one column:

```sql
ALTER TABLE chunk_records ADD COLUMN wall_clock_written_at_ms INTEGER;
```

Added via an incremental migration (idempotent, `ADD COLUMN IF NOT EXISTS` pattern — this project already uses `V4`/`V5`/`V16` numbered migrations; this is the next one).

`FlvChunker` (`crates/rs-inpoint/src/flv_chunker.rs:336`) records `SystemTime::now().duration_since(UNIX_EPOCH).as_millis()` at chunk-emit time and passes it through the `PendingChunkWrite` struct into `insert_chunk_record()` (`crates/rs-core/src/db/mod.rs:269`).

**Derived metric:** producer timestamp rate = `(last_ts − first_ts) / (wall_clock_written_at_ms − prev_chunk_wall_clock_written_at_ms)`. A value of 1.000 means wall-clock matches timestamps; anything else is drift on the producer side.

#### 1b. Consumer ffmpeg-time progress parsing

`rs-ffmpeg::FfmpegProcess` already reads stderr into a ring buffer (`STDERR_BUFFER_SIZE = 30`, `lib.rs:193`). Extend the stderr reader task to additionally parse the `time=HH:MM:SS.xx` field (ffmpeg emits it every ~0.5s during encoding) and emit a structured event:

```rust
pub struct FfmpegProgress {
    pub media_time_ms: u64,    // ffmpeg's "time=" field
    pub wall_clock_ms: u64,    // when we received this progress line
}
```

Events shipped up via the existing `vps_logs` infrastructure (#129) to `rs-api` on stream.lan, persisted as JSON lines.

**Derived metric:** consumer drain rate = `Δmedia_time_ms / Δwall_clock_ms`. 1.000 is perfect; >1.000 means `-re` drains too fast.

#### 1c. Clock-skew probe stream.lan ↔ VPS

New endpoint on `rs-delivery`:

```
GET /clock  →  { "vps_ms": <u64 SystemTime-since-UNIX-epoch in ms> }
```

Background task on stream.lan (in `rs-api::delivery_orchestrator`) runs every 30s while delivery is active:

```
local_before = now_ms()
{vps_ms} = GET http://<vps_ip>:<port>/clock
local_after = now_ms()
rtt = local_after - local_before
skew = vps_ms - (local_before + local_after) / 2     // RTT-compensated
```

Persisted to SQLite in a new `clock_skew_samples` table (`clock_skew_samples(event_id, measured_at_ms, skew_ms, rtt_ms)`), created by the same migration as 1a.

**Derived metric:** slope of `skew_ms` vs `measured_at_ms` across a streaming session. Linear slope > ~50ppm (~0.18s/hour) is suspect-level. > 500ppm (1.8s/hour) is smoking-gun territory.

#### 1d. Dashboard panel (optional but cheap)

Add a "Clock & Pacing" panel to the main dashboard that plots three time-series side-by-side during an active delivery:

- cache_delay_secs (already in existing panel — keep as-is)
- clock skew between stream.lan and VPS
- producer timestamp-vs-wall-clock rate
- consumer ffmpeg-time-vs-wall-clock rate

Operator can eyeball root cause in real time. This panel survives past the investigation — it's genuinely useful for future ops.

### Phase 2 — Live investigation

After Phase 1 lands on `dev` and deploys to stream.lan (via the existing `deploy-stream-lan` CI job), I run a ≥2h live streaming test on the real production path:

1. Start OBS on stream.lan with the standard profile (Stream_Obs, 4K@30.30).
2. Activate an event, start delivery to 4 YT_RTMP endpoints (the same setup as the 2026-04-22 test).
3. Run ≥2h while watching the dashboard + SQLite telemetry.
4. Collect and analyze the four rates:
   - cache_delay_secs slope (confirm still ~−18s/h baseline)
   - clock skew slope
   - producer rate slope (should be ~1.000)
   - consumer rate slope (should be ~1.000)
5. Identify which of the four suspects explains the observed cache_delay_secs slope. At least one must account for it quantitatively.

**I run this test in this session using the existing MCP tooling (`win-stream-snv`, Hetzner orchestration already exercised by CI). It does not require user action.** While the test runs, I continue on other work (writing the fix code behind a feature flag) so the 2h is not idle.

### Phase 3 — Targeted fix (same PR)

The fix is locked in by data, not guessed:

| What Phase 2 shows | Fix |
|---|---|
| **Clock-skew slope accounts for ≥80% of cache drift** | Harden NTP on VPS cloud-init: install chrony, configure multi-peer stratum-1 upstream (e.g., cloudflare.com NTS, time.google.com), poll interval 16-64s, `makestep 0.1 3` to correct small drift immediately at boot. Producer side (stream.lan) is Windows — confirm w32time is synced to a stratum-1 source; add check to deploy-stream-lan job. |
| **Producer timestamp rate slope ≠ 1.000** | Extend `FlvStreamNormalizer` to rewrite tag timestamps to match producer wall-clock cadence. Uses `wall_clock_written_at_ms` as the wall-clock reference; scales `Δts` across all tags in a chunk so first-to-last spans the wall-clock interval, preserving intra-chunk relative spacing. |
| **Consumer ffmpeg-time rate slope ≠ 1.000** | Reintroduce a narrow Rust-side consumer pacer in the delivery task (removed in #129 commit 3c7b8ef). This time anchor-per-chunk based on wall-clock, with explicit drift clamp so it can't cascade into anything like the 32s death cycle #129 removed. Unit-tested with synthetic chunks. |
| **Mixed causes** | Fix each cause in proportion. The PR grows but still ships atomically. |

If the data shows a cause we didn't anticipate (hypothetical: YouTube HLS PUT jitter), the fix is still landed in the same PR. Investigation is not done when the telemetry ships; it's done when the cache is stable.

### Phase 4 — Verification (same PR, same session)

Run another ≥1h live test on the fixed code. Success criterion:

- `cache_delay_secs` stays within **±5s of the target** across the full run
- `chunk_delay_secs` for each endpoint stays within **±5s of the target**
- No regressions on existing E2E tests
- Zero ffmpeg restarts (regression check — #129 fix must still hold)

If Phase 4 fails, we loop back to Phase 3 and adjust the fix. PR does not merge until Phase 4 is green.

Instrumentation code stays after verification; it's flipped to sample-every-Nth-chunk mode (default N=10) so SQLite doesn't grow unboundedly. Dashboard panel stays permanently.

## Code changes summary

| Crate | Files | Change |
|---|---|---|
| `rs-core` | `db/migrations.rs`, `db/mod.rs` | New migration adds 2 columns/tables. New field on `insert_chunk_record` signature. |
| `rs-inpoint` | `flv_chunker.rs` | Record wall-clock at chunk emit. |
| `rs-ffmpeg` | `lib.rs` | Parse stderr `time=`, emit `FfmpegProgress` events. |
| `rs-delivery` | `endpoint_task.rs`, new `clock_endpoint.rs` | Consume progress events, ship to stream.lan via existing logs channel. New `/clock` endpoint. |
| `rs-api` | `delivery_orchestrator.rs`, `delivery_status.rs` | Clock-skew probe task per active delivery. Persist progress events + skew samples. |
| `leptos-ui` | New panel component | Render the three time-series alongside the existing cache-delay panel. |
| *Fix locus (selected at Phase 3 after data)* | One of: `rs-cloud` (cloud-init), `rs-delivery/flv_normalizer.rs`, `rs-delivery/endpoint_task.rs` | Targeted fix per Phase 3 table. The plan document enumerates all three as conditional branches; the branch to execute is picked once Phase 2 data lands. |

## Testing

- **Unit tests:** existing coverage for chunk insertion, stderr parsing, migrations extends to cover the new columns. `clock_endpoint` gets unit tests for the skew calculation (mocking time).
- **Integration tests:** new test in `crates/rs-api/tests` that spins up a mock VPS `/clock` endpoint, runs the skew probe, asserts sample is recorded.
- **Playwright E2E:** new `e2e/cache-drift-panel.spec.ts` — navigates to dashboard during simulated delivery, confirms the three new time-series panels render and update.
- **Live verification:** Phase 2 + Phase 4 above. Reports captured as screenshots + SQLite exports in the PR body.
- **Post-deploy verification:** after CI deploys the fix to stream.lan, reopen the dashboard via Playwright, assert cache_delay_secs stable over ≥15 min in post-deploy window.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Phase 2 2h test costs wall-clock time | Run in background during this session; I continue on other useful work. |
| Fix behind the root cause turns out more complex than expected | Single PR still; we keep the instrumentation atomic with the fix and don't split. |
| New SQLite writes at per-chunk rate bloat the DB | Sample-every-Nth after verification; SQLite writes are cheap on stream.lan's local disk anyway. |
| `/clock` endpoint adds attack surface on VPS | Read-only, no sensitive info, VPS already exposes `/api/init`. Trivial handler. |
| Migration on a running system | Already project convention — idempotent `ADD COLUMN IF NOT EXISTS`, auto-migrate on startup. Matches existing V4/V5/V16 pattern. |
| PR grows beyond comfortable review | Spec self-imposes ~10-12 tasks; still one PR. Reviewer handled #129's larger scope; this is smaller. |

## Deliverables

- Single PR: `dev` → `main`, version 0.3.68.
- Green CI (all jobs, including `deploy-stream-lan`).
- Phase 2 + Phase 4 test artifacts attached to PR body (cache-delay plots, skew plots, producer/consumer rate plots).
- Issue #135 closed by the PR.
- Dashboard permanently gains the new pacing panel.
