# Soak gate + RTMP/cache telemetry — design spec

**Issue:** #176 (Phase 1 of three-phase 4h-green-dashboard recovery)
**Status:** brainstorming → writing-plans
**Date:** 2026-05-06
**Branch state at design time:** dev = 0.5.0, PR #170 (disk_cache) open & blocked

---

## 1. Problem

The 4-hour green-dashboard soak target is missed again. Production
event 9289 (stream.snv, dev 0.5.0, 26 min uptime at 2026-05-06T18:35Z)
shows **three coexisting regressions** that the existing CI gate let
through:

### 1.1 CI gate accepts bad health

`e2e/youtube-studio-check.spec.ts:400-416` "stream-receiving" detection
contains the regex
`/stream\s*health.*(?:excellent|good|ok|bad)/i`. The literal `bad`
inside the alternation causes the test to mark the run as receiving
when YT Studio shows "Stream health: Bad". No assertion in CI checks
the structured `health_status` field returned by
`/api/v1/youtube/status`. Result: CI green while production health =
`bad` and `configuration_issues = ["videoIngestionFasterThanRealtime"]`.

### 1.2 FB `rust_rtmp_push` chronic disconnect

Live audit log on event 9289 over the last 200 events:

| endpoint | action | error |
|---|---|---|
| FB-NewLevel | endpoint_rtmp_push_died ×100 | upstream closed connection mid-stream: unexpected end of file |
| FB-Zbynek | endpoint_rtmp_push_died ×100 | same |

`reconnect_count` reached 2840 in 26 min (~109/min/endpoint). YT
endpoints did NOT exhibit this pattern (only 2 deaths total, and those
with `error="rtmp_push_timeout"`, `backoff_ms=30000`, not the FB
error). The FB pattern is constant since the VPS booted; it is not a
warm-up artefact.

The last YT death was at 15:26Z (early in the event); steady-state for
YT is "no deaths" while FB cycles every ~1.5 s.

### 1.3 +30 s drift on every endpoint

| endpoint | chunk_delay_secs | target | drift |
|---|---|---|---|
| YT NLCH 4K | 151.3 | 120 | +31.3 |
| YT NLW 4k | 151.3 | 120 | +31.3 |
| e2e rtmp | 151.3 | 120 | +31.3 |
| Control stream Kiko | 151.3 | 120 | +31.3 |
| FB-NewLevel | 149.4 | 120 | +29.4 |
| FB-Zbynek | 151.3 | 120 | +31.3 |

YouTube live API confirms `videoIngestionFasterThanRealtime` for the
e2e rtmp stream — symptomatic of bursty supply (long pause then
catch-up dump) rather than slow supply.

### 1.4 Why this spec does NOT fix 1.2 and 1.3 directly

The root cause for 1.2 (FB upstream close) is unknown. Memory
`feedback_no_upstream_excuse` rules out "FB rotates servers". Memory
`feedback_fb_keys_persistent` rules out key expiry. Memory
`feedback_no_ffmpeg_fallback` rules out "switch FB back to ffmpeg".
The remaining candidates are: missing RTMP `Acknowledgement` message,
faster-than-realtime push triggering FB rate-limit close, or wrong
chunk-stream-id / message-stream-id. Picking one without telemetry =
guessing. The same applies to 1.3: bursty supply could come from
disk_cache prefetch concentration, from RTMP-push catch-up after
reconnect, or from S3 latency variance. We need timestamped
per-message data to tell.

This spec lands the gate that catches the regression class plus the
telemetry that distinguishes the candidates. Phase 2 (FB protocol fix)
and Phase 3 (pacing fix + 4 h soak) consume the data this spec
generates.

---

## 2. Goals

1. CI fails on every push when production-class symptoms reappear.
2. Production audit log captures enough RTMP-push and disk_cache state
   to identify root causes without re-running stream events.
3. PR #170 stays unmerged. Phase 2/3 land before any merge to main.
4. No behavior change in the data path (RTMP push, disk_cache, S3
   fetch). Telemetry is read-only or write-only.

---

## 3. Non-goals

- Fixing the FB `upstream closed connection` cause.
- Fixing the +30 s drift / videoIngestionFasterThanRealtime cause.
- Adding a 4 h soak CI job. (Phase 3 — after the fixes land.)
- Replacing or refactoring `rust_rtmp_push` or disk_cache.
- Changing the dashboard rendering of the new audit fields. The fields
  appear in the audit feed JSON; they don't need new UI.

---

## 4. Architecture

Five independent additions, each landable as one or two tasks:

```
┌──────────────────────────────────────────────────────────┐
│ Phase 1 spec                                             │
├──────────────────────────────────────────────────────────┤
│ (a) e2e/youtube-studio-check.spec.ts                     │
│     - drop "bad" from receivingPatterns regex            │
│     - add /api/v1/youtube/status assertion               │
│                                                          │
│ (b) .github/workflows/soak-mini.yml                      │
│     - workflow_dispatch + nightly cron                   │
│     - 30 min sample-driven assertion job                 │
│                                                          │
│ (c) crates/rs-delivery/src/endpoint_audit.rs             │
│     - extend emit_rtmp_push_died detail JSON             │
│     - new RtmpPushTelemetry struct kept per endpoint     │
│                                                          │
│ (d) crates/rs-delivery/src/disk_cache/endpoint_reader.rs │
│     - per-chunk push samples → DiskCachePushSample audit │
│     - Action::DiskCachePushSample new variant            │
│     - rate-limited via existing AuditRateLimiter         │
│                                                          │
│ (e) crates/rs-api/src/diag.rs                            │
│     - new module                                         │
│     - POST /api/v1/diag/dump → JSON snapshot             │
└──────────────────────────────────────────────────────────┘
```

(a)+(b) are CI / test-side only. (c)+(d)+(e) are stream.snv/VPS
runtime telemetry — additive, no behavior change.

---

## 5. Components

### 5.1 (a) Hardened YouTube studio E2E

`e2e/youtube-studio-check.spec.ts` change:

```diff
       const receivingPatterns = [
         /\d+\s*kbps/i,
         /\d+p\s+\d+\s*fps/i,
-        /stream\s*health.*(?:excellent|good|ok|bad)/i,
+        /stream\s*health.*(?:excellent|good|ok)/i,
         /Výborn/i,
         /Stav\s*streamu/i,
         /Kvalita\s*streamu/i,
       ];
```

After the existing "Preparing" check passes, fetch the structured
status from the live Restreamer API and assert health is good with
no configuration issues:

```typescript
const ytStatus = await page.evaluate(async () => {
  const res = await fetch("http://10.77.9.204:8910/api/v1/youtube/status");
  return res.json();
});
const activeStreams = (ytStatus.streams || []).filter(
  (s: any) => s.stream_status === "active",
);
expect(activeStreams.length, "no active YT stream observed").toBeGreaterThan(0);
for (const s of activeStreams) {
  expect(
    s.health_status,
    `YT health must be 'good' (got '${s.health_status}' on stream '${s.title}')`,
  ).toBe("good");
  expect(
    s.configuration_issues,
    `YT configuration_issues must be empty (got ${JSON.stringify(s.configuration_issues)} on '${s.title}')`,
  ).toEqual([]);
}
```

The fetch URL is the stream.snv LAN IP. CI runs the test on the
self-hosted runner that has LAN access — already the case for
existing E2E.

### 5.2 (b) Mini-soak workflow

New `.github/workflows/soak-mini.yml`. Runs on:

- `workflow_dispatch` (operator on demand)
- Nightly cron (`0 2 * * *` UTC = 03:00 Slovak)

Behavior:

1. Trigger receiving on stream.snv via existing API
   (`POST /api/v1/streaming-events/{id}/receiving`)
2. Trigger delivering via `POST /api/v1/streaming-events/{id}/delivering`.
3. Wait for `/api/v1/delivery/instances` to report `status=delivering`.
4. Sample loop, 30 s interval, 60 samples (= 30 min):
   - GET `/api/v1/delivery/status?event_id=<id>` → assert per-endpoint:
     - `chunk_delay_secs <= delivery_delay_secs * threshold`
       where threshold = 1.1 for YT-style aliases, 1.3 for FB-style,
       matching `leptos-ui::utils::cache_threshold_for_service`.
   - GET `/api/v1/audit?event_id=<id>&action=endpoint_rtmp_push_died&since=<start_ts>` →
     assert `count <= 50` per endpoint over the FULL window.
5. On any assertion fail: print full per-endpoint timeline (all
   samples) and the offending audit rows, then exit non-zero.
6. Always: stop event at end via `POST .../stopped`.
7. Workflow does NOT skip / continue-on-error / retry. Binary success
   or fail.

The job runs as a Bash script (no Playwright needed — pure HTTP
sampling). Implementation in `scripts/soak-mini.sh`, called from the
workflow.

The 30 min window is the smallest interval over which the +30 s drift
becomes detectable (drift accumulates over ~20-25 min). 4 h is
deferred to Phase 3.

### 5.3 (c) Extended `endpoint_rtmp_push_died` audit row

Today the row payload is:

```json
{ "backend": "rust_rtmp_push", "error": "...", "backoff_ms": 3000, "reconnect_count": N }
```

Extended payload:

```json
{
  "backend": "rust_rtmp_push",
  "error": "...",
  "backoff_ms": 3000,
  "reconnect_count": N,
  "bytes_sent_since_connect": 12345678,
  "time_since_connect_ms": 1450,
  "time_since_last_upstream_ack_ms": 1450,           // null if never_acked
  "last_rtmp_message_type_sent": "Audio",            // | "Video" | "@setDataFrame" | "Connect" | ...
  "upstream_close_first_bytes_hex": "00000000...",   // 64 bytes max
  "chunks_pushed": 2,
  "chunks_buffered_in_pipeline": 0
}
```

Implementation: a `RtmpPushTelemetry` struct held by the rust pusher
state machine, reset on each connect. Updated on every send and on
every read from the upstream socket. On disconnect, its snapshot is
passed into a new `emit_rtmp_push_died_detailed` helper.

The struct lives in `crates/rs-delivery/src/disk_cache/...` is wrong —
it's RTMP push state. Place it in
`crates/rs-delivery/src/rtmp_push_telemetry.rs`. This file does not
exist today and stays small (<200 lines).

```rust
// crates/rs-delivery/src/rtmp_push_telemetry.rs (new file)
pub struct RtmpPushTelemetry {
    connect_at: Instant,
    bytes_sent: u64,
    last_upstream_ack_at: Option<Instant>,
    last_message_type_sent: Option<&'static str>,
    chunks_pushed: u32,
}

impl RtmpPushTelemetry {
    pub fn new() -> Self { ... }
    pub fn note_send(&mut self, msg_type: &'static str, n_bytes: u64) { ... }
    pub fn note_upstream_ack(&mut self) { ... }
    pub fn note_chunk_pushed(&mut self) { ... }
    pub fn snapshot(&self, close_buf: &[u8]) -> serde_json::Value { ... }
}
```

Wired into the existing `rust_rtmp_push` send path and into the read
side that consumes upstream `Acknowledgement` messages.

The "first 64 bytes read at close" is captured by extending the
upstream-read loop: every read writes into a small ring buffer; on
close, the ring's last fill is hex-encoded into the audit detail.

### 5.4 (d) Disk-cache push samples

A new `Action::DiskCachePushSample` variant (extend the enum in
`crates/rs-core/src/audit.rs`).

`EndpointReader` (the disk_cache hot loop) tracks per-endpoint:

- `last_push_at: Option<Instant>`
- `last_chunk_id_pushed: Option<u64>`
- `last_chunk_supply_lag_ms: Option<i64>`
  (= time the chunk became available locally minus the chunk's
  expected wall-clock time, computed from `chunk_id * chunk_duration_ms`
  and the event start instant)

On every chunk push, sample emission goes through `AuditRateLimiter`
keyed by `(DiskCachePushSample, endpoint_alias)`. Limiter already emits
1/min/key, matching the spec target.

Sample payload:

```json
{
  "endpoint": "FB-NewLevel",
  "chunk_id": 703,
  "chunk_supply_lag_ms": 320,
  "inter_chunk_gap_ms": 850,
  "burst_factor": 1.18,
  "delivery_delay_secs": 120,
  "current_chunk_delay_secs": 151.3
}
```

`burst_factor = chunk_duration_ms / inter_chunk_gap_ms` — values >1.5
mean the pusher dumped the chunk faster than wall-clock. Sustained
>1.5 across 5 samples on a single endpoint = bursty supply confirmed.

### 5.5 (e) Diagnostic dump endpoint

`POST /api/v1/diag/dump` on stream.snv. Response: single JSON
document, ~1 MB typical. Body shape:

```json
{
  "generated_at": "2026-05-06T18:39:00Z",
  "version": "0.6.0",
  "event_id": 9289,
  "event": { ... full streaming_event row ... },
  "audit_60min": [ /* every audit row from last 60 min */ ],
  "endpoint_timeline": {
    "FB-NewLevel": [
      { "ts": "...", "chunk_delay_secs": 149.4, "reconnect_count": 2840, "bytes_processed_total": ... },
      ...  /* one entry per 30 s, 120 entries */
    ],
    "FB-Zbynek": [...],
    ...
  },
  "disk_cache_stats": { /* current snapshot from DiskCache facade */ },
  "s3_fetch_profile": {
    "count": 1234,
    "bytes_total": 1234567890,
    "p50_latency_ms": 45,
    "p99_latency_ms": 320,
    "fail_count_by_class": { "504": 1, "503": 0, "timeout": 2 }
  }
}
```

The endpoint runs entirely in the host (stream.snv) — VPS state is
fetched live from `/api/v1/delivery/status?event_id=<id>` and merged.
The 30 s timeline samples come from a new in-memory ring buffer
populated by the existing delivery-monitor poll loop.

Implementation lives in `crates/rs-api/src/diag.rs` (new module).
S3 fetch profile data is sourced from a per-process `S3FetchProfile`
struct kept in `rs-delivery::s3_fetch::profile` (new) and surfaced
through the VPS `/api/v1/delivery/status` response, then aggregated
host-side.

For this Phase 1 spec, `s3_fetch_profile` carries fail-count-by-class
plus byte/count totals. Per-endpoint p50/p99 are computed from a
quantile sketch maintained server-side (existing `hdr_histogram`
crate already in the workspace tree, otherwise simple bucketing).

---

## 6. Data flow

### 6.1 Telemetry write side (VPS)

```
disk_cache::EndpointReader::push_chunk
   → emit_disk_cache_push_sample()
       → AuditRateLimiter (1/min/endpoint)
           → AuditRing → /api/v1/audit (host-mirrored)

rust_rtmp_push::session_loop::on_send
   → RtmpPushTelemetry::note_send

rust_rtmp_push::session_loop::on_upstream_ack
   → RtmpPushTelemetry::note_upstream_ack

rust_rtmp_push::session_loop::on_disconnect
   → emit_rtmp_push_died_detailed(snapshot, close_first_bytes)
```

### 6.2 Telemetry read side (host)

```
POST /api/v1/diag/dump
   → diag::build_dump
       ├── reads audit DB (last 60 min)
       ├── reads endpoint_timeline ring (30 s samples)
       ├── GET VPS /api/v1/delivery/status (DiskCacheStats + S3FetchProfile)
       └── returns JSON
```

### 6.3 CI gate read side

```
soak-mini.sh
  loop 60 × {
    sleep 30
    GET /api/v1/delivery/status?event_id=ID  → per-endpoint chunk_delay_secs
    GET /api/v1/audit?action=endpoint_rtmp_push_died&since=START → cumulative count
    assert thresholds
  }
```

---

## 7. Error handling

- (a) Studio E2E: if `/api/v1/youtube/status` returns 5xx, the test
  fails (no graceful skip — that would be the same hole we just
  closed).
- (b) `soak-mini.sh`: any HTTP non-2xx during sampling = fail; any
  threshold breach = fail with full timeline dump in CI log.
- (c) RTMP telemetry: every field is `Option`-typed; emitting partial
  data is allowed when the pusher dies before the first ack.
- (d) Disk-cache sample emission failures (limiter false negatives)
  are silently dropped — telemetry is best-effort and must never
  block the data path.
- (e) Diag dump: panic-free; on partial source failure (e.g. VPS
  unreachable) the missing section is replaced with
  `{ "error": "<reason>" }` and the rest is returned with HTTP 200.
  The dump is for operator triage; partial data still beats no data.

---

## 8. Testing

### 8.1 Unit tests

- `RtmpPushTelemetry::snapshot` round-trip with known clock skew
  (mock `Instant`).
- `AuditRateLimiter` confirms `(DiskCachePushSample, alias)` keying:
  two endpoints emit independently within the same minute, single
  endpoint coalesces.
- `S3FetchProfile` quantile/bucketing math.
- `diag::build_dump` with mocked sources + a partial-failure case.

### 8.2 E2E

- `youtube-studio-check.spec.ts` change: regex no longer matches
  `Stream health: Bad`; assertion against `/api/v1/youtube/status`
  with `health_status="good"` passes against a fake-API fixture and
  fails against a fake `health_status="bad"` fixture (test added in
  `e2e/frontend.spec.ts` style with stubbed JSON).

### 8.3 Mini-soak workflow

- Manual `workflow_dispatch` run after merge to dev. Expected to
  FAIL today on current production state (proves gate works). Once
  Phase 2 + 3 land, expected to pass. CI dev push does NOT trigger
  this workflow — only manual + nightly.

### 8.4 Mutation testing

`crates/rs-delivery/src/rtmp_push_telemetry.rs` and
`crates/rs-api/src/diag.rs` MUST NOT be added to `--exclude-re`.
Mutation score budget per file: `cargo mutants --in-diff` shows zero
surviving mutants.

---

## 9. File-size and line-count constraints

CI gate: every new `.rs` file <1000 lines. Estimated:

| file | est. lines |
|---|---|
| `crates/rs-delivery/src/rtmp_push_telemetry.rs` | 220 |
| `crates/rs-api/src/diag.rs` | 380 |
| `crates/rs-delivery/src/s3_fetch/profile.rs` | 180 |
| `scripts/soak-mini.sh` | 220 |
| `.github/workflows/soak-mini.yml` | 60 |

Existing file edits (audit.rs +5 lines for new Action variant, etc.)
all stay well under 1000.

---

## 10. Acceptance criteria

1. `youtube-studio-check.spec.ts` regex no longer accepts `bad`. Test
   fetches `/api/v1/youtube/status` and asserts good health with no
   `configuration_issues`. Unit-level fixture covers both branches.
2. `.github/workflows/soak-mini.yml` exists. Manual dispatch on dev
   today FAILS loud with full per-endpoint timeline. Same workflow,
   run after Phase 2+3, PASSES.
3. Audit rows for `endpoint_rtmp_push_died` carry the seven new
   fields. Backwards-compat: dashboard / audit consumers tolerate
   missing fields (older rows pre-spec do not have them).
4. `Action::DiskCachePushSample` variant exists and is emitted at
   ~1/min/endpoint during steady-state delivery.
5. `POST /api/v1/diag/dump` returns a JSON document conforming to
   the §5.5 schema. Manually invoked on stream.snv current state
   produces the document; output attached to the Phase 2 issue
   filed at the end of Phase 1.
6. PR for this spec lands on dev with all CI green (the existing
   per-push CI does NOT include `soak-mini`). Soak-mini is opt-in.
7. PR #170 stays open and unmerged. After this PR lands, the new
   tests in (a) FAIL on dev's `youtube-studio-check` against live
   production — confirming the gate exists and is honest. (Note:
   Phase 1 PR's own CI runs against a fixture, not live YT, so its
   own CI is green.)
8. Phase 2 issue (FB rs_rtmp_push protocol fix) and Phase 3 issue
   (disk_cache pacing fix + 4 h soak) are filed before Phase 1
   completion report.

---

## 11. Versioning

dev current = 0.5.0. Phase 1 lands as 0.6.0 (new Action variant,
new endpoint, new audit fields = minor bump). Bump in first task
per `version-bumping.md`.

---

## 12. Out-of-scope (explicit)

- FB RTMP protocol root-cause fix: Phase 2.
- Disk_cache pacing fix: Phase 3.
- 4 h soak workflow: Phase 3.
- Dashboard rendering of the new audit fields beyond the existing
  audit JSON view.
- Removing the `is_fast` column drift from `Control stream Kiko` (=
  same +30 s as the rest — the `is_fast` flag is irrelevant; the
  drift is supply-side, not delivery-mode-side).
