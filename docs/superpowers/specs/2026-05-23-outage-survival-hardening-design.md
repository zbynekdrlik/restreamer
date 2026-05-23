# Outage Survival Hardening — Design Spec

**Date:** 2026-05-23
**Status:** Approved for planning
**Closes (proposed):** new tracking issues filed during planning; supersedes the unaddressed items in `project_2026_04_19_live_event_failure` memory.

---

## 1. Problem

Restreamer is built to survive an outage of **any component downstream of the protected core** (OBS + the stream.lan laptop, which runs on battery). During the live event of 2026-05-22 the power, the building internet, and the network switch all went down. The laptop survived on battery and OBS kept feeding stream.lan (OBS runs locally on stream.lan — confirmed). Only the S3 uplink was lost.

The system was supposed to: keep ingesting and buffering chunks locally, let the delivery VPS fall back to rescue video, and on restore upload the backlog and resume with **zero content lost**. Instead the operator saw a dashboard full of red error walls, could not tell a survivable outage from a fatal one, lost confidence, and recreated the event from zero — losing all Facebook streams.

### 1.1 Confirmed root cause (verified in current code)

**`crates/rs-endpoint/src/uploader.rs:84-85,471-473`** — the S3 uploader abandons a chunk permanently after `MAX_ATTEMPTS = 10` **or** `MAX_WALL_CLOCK_MS = 600_000` (10 minutes):

```rust
const MAX_ATTEMPTS: i64 = 10;
const MAX_WALL_CLOCK_MS: i64 = 600_000; // 10 min total retry budget
...
let permanent = attempt >= MAX_ATTEMPTS || wall_clock_ms >= MAX_WALL_CLOCK_MS;
if permanent {
    let _ = db::mark_upload_permanently_failed(pool, chunk.id).await;
}
```

A switch/internet outage longer than 10 minutes therefore **permanently drops** every chunk buffered during the outage. The chunk sequence gets a permanent hole the delivery VPS can never fill. Downstream endpoints break and stay broken — exactly the "recreate the event from zero" failure. The laptop battery kept the data alive; the code threw it away anyway. **This single constant defeats the entire design promise.**

### 1.2 Secondary problems (verified)

1. **Dashboard cannot distinguish survivable from fatal.** Per-endpoint status is effectively binary `alive`/dead (`leptos-ui/src/components/operator_dashboard.rs:664-689`). A transient, auto-recovering outage paints the same red as a dead endpoint, plus raw backend error strings rendered verbatim with no truncation (`operator_dashboard.rs:777-780`) and stacked badges (`stall:` + `ffmpeg xN` + `reconn xN`). A calm `delivery_mode: "recovering"` blue badge exists but the backend rarely sets it during real outages, so the operator sees a wall of red and panics.
2. **Audit log is half-blind for outage forensics.** The `audit_log` table (migration v18, `crates/rs-core/src/db/migrations.rs`), the `/api/v1/audit` API, and the UI panel all exist and work. But **7 cache events are defined in the `Action` enum and never emitted** (`DiskCachePrefillStarted`, `DiskCachePrefillReady`, `DiskCacheChunkEvicted`, `DiskCacheDownloadThrottled`, `DiskCacheStallTimeout`, `DiskCacheWriteFailed`, `DiskCacheReaderRecovered`), **rescue-video activation has zero audit coverage**, and `RtmpHandshakeFailed` is defined but never emitted. The operator cannot reconstruct what the cache and rescue path did during the outage.
3. **No local disk-pressure safety/visibility.** With permanent-drop removed, an arbitrarily long outage buffers chunks until the laptop disk fills. There is no monitoring, no audit, no UI alarm, and no controlled last-resort behavior.

### 1.3 Design decisions (locked by the operator)

- **Recovery mode = replay everything (continuity).** On restore, every buffered chunk is replayed in sequence order. Zero content is lost on the live feed. The accepted tradeoff: after a long outage the stream stays roughly `outage_duration` behind real-time for the rest of the event (strict 1× pacing, no catch-up burst — consistent with the existing strict-1× pusher). The full recording is preserved locally for VOD regardless.
- **Topology = OBS local on stream.lan.** During the outage OBS keeps feeding; only the S3 uplink is lost. A local backlog therefore always exists and replay is meaningful.

---

## 2. Goal

When any component downstream of the protected core (OBS + stream.lan) fails for any duration, the system must:

1. Keep ingesting and buffering locally with **zero chunk loss** for the full life of the laptop battery / disk.
2. Fall back to rescue video on the delivery VPS, clearly and automatically.
3. On restore, upload the backlog and **replay every chunk in order** — no holes, no recreate-from-zero.
4. Show the operator a **calm, unambiguous status**: "protected, recovering, no action needed" for survivable outages; red **only** when the operator genuinely must act.
5. Record a **complete audit timeline** that reconstructs the entire outage after the fact, without SSH.
6. Be **locked by TDD + CI + an E2E outage-simulation test** so this class of failure can never silently return.

---

## 3. Architecture

Pipeline (unchanged): `OBS → RTMP → stream.lan (FLV chunker → local disk → uploader) → S3 (Hetzner nbg1) → delivery VPS (rs-delivery: disk cache → endpoint task → RTMP/RTMPS push) → YouTube / Facebook`.

The hardening touches five pillars, each mapped to concrete components.

### Pillar 1 — Never drop a buffered chunk (continuity guarantee)

**Component: `crates/rs-endpoint/src/uploader.rs`**

- Remove the time/attempt permanent-drop for **retryable** errors. Classification (`classify_upload_error`) already exists; reuse it:
  - **Retryable forever** (capped 30s backoff): `timeout`, `conn`, `5xx` (`503`/`504`), `enospc`-on-S3-side, offline/DNS, and `other`. These are network/outage classes — the chunk stays on disk and is retried indefinitely until uploaded.
  - **Abandonable** (after a small attempt budget): only a chunk S3 rejects as structurally invalid (`400`/`403`/`404`-style permanent client errors) where retrying can never succeed. Abandoning emits a `Critical` audit row with full context. This is rare and not an outage path.
- The chunk is deleted from local disk **only after successful upload** (existing behavior at `uploader.rs:458`) — unchanged, and now the only deletion path for retryable chunks.
- New/auditable signals:
  - Reuse existing `HostInternetUnreachable` / `HostInternetRecovered` (`crates/rs-api/src/internet_probe.rs`) as the outage envelope.
  - Add an upload-backlog gauge (count of pending chunks, oldest pending age) surfaced over WS for the UI and a periodic rate-limited `info` audit while a backlog drains.

**Disk safety valve — `crates/rs-endpoint` (uploader/chunk-store) + `crates/rs-core` audit**

- A lightweight monitor samples free disk on the chunk-store volume (configurable interval, default 10s).
- Thresholds (proposed defaults, configurable): **warn at 80% used**, **critical at 90% used**.
- `LocalDiskPressure` audit (new `Action`): `Warn` at warn threshold, `Critical` at critical threshold (rate-limited 1/min per severity). UI raises a corresponding alarm.
- **Last-resort only**, at true disk-full (write actually fails): drop the **oldest unsent** chunk, emit `DiskWriteFailed` (`Critical`) naming the dropped chunk id. Never silent. For an event-length outage on a normal laptop this never triggers; it is insurance, not a normal path.
- Wire `DiskWriteFailed` (currently defined, never emitted) at every disk-write failure site in the chunk path.

### Pillar 2 — VPS replays cleanly on restore (continuity)

**Component: `crates/rs-delivery/src/endpoint_task.rs`, `rescue.rs`, `disk_cache/`**

- The producer must **wait through an arbitrarily long sequential gap** for the next chunk id (rescue plays meanwhile) and resume from the **exact next chunk** — never skip ahead to a live edge for continuity endpoints. Verify and lock: the producer's wait loop has no terminal give-up that would break replay; if a bounded wait exists it must only feed audit/telemetry, not abort.
- `DiskCacheStallTimeout` becomes an **audit-only** signal: emit it when a reader has waited past the threshold (default 60s) so the stall is visible, but it must **not** abort the reader or skip the chunk. Pair with `DiskCacheReaderRecovered` (emit when the reader successfully pushes again after a stall) to bound the outage in the timeline.
- **Rescue audit (currently zero coverage):** add `RescueActivated` (`Warn`, with the chunk position where playback stalled) and `RescueRecovered` (`Info`, with `gap_secs` = duration the rescue covered). These are new `Action`s emitted at the rescue enter/exit points in `rescue.rs` / `endpoint_task.rs`.
- Confirm the `is_fast` live-edge jump (`crates/rs-api/src/delivery_live_edge.rs`, `FastEndpointJumpedToLiveEdge`) does **not** fire for continuity endpoints during recovery — continuity and live-edge-jump are mutually exclusive per endpoint.

### Pillar 3 — Complete audit timeline for outage forensics

**Component: `crates/rs-core/src/db/audit.rs` (Action enum, already defined) + emission sites across `rs-delivery` and `rs-endpoint`**

Wire every defined-but-dead event to a real emission site:

| Action | Source | Severity | Emit when |
|---|---|---|---|
| `DiskCachePrefillStarted` | Vps | Info | first EndpointReader registers / first chunk requested for an event |
| `DiskCachePrefillReady` | Vps | Info | the cache window is first fully populated for an endpoint (push imminent) |
| `DiskCacheChunkEvicted` | Vps | Info | rate-limited 1/min summary of chunks evicted by the eviction task |
| `DiskCacheDownloadThrottled` | Vps | Warn | download bandwidth cap reached / sustained S3 latency |
| `DiskCacheStallTimeout` | Vps | Error | reader wait exceeds threshold (audit-only, see Pillar 2) |
| `DiskCacheReaderRecovered` | Vps | Info | reader pushes successfully after a stall |
| `DiskCacheWriteFailed` | Vps / Uploader | Error | disk write fails (ENOSPC/EIO) — see Pillar 1 |
| `RescueActivated` | Vps | Warn | rescue video starts (Pillar 2) |
| `RescueRecovered` | Vps | Info | rescue exits, live resumes (Pillar 2), with `gap_secs` |
| `RtmpHandshakeFailed` | Vps | Warn | RTMP/RTMPS handshake to endpoint fails (currently defined, never emitted) |

Acceptance: after a simulated outage the audit timeline, read in order, tells the full story — internet down → upload backlog grows → VPS reader stalls → rescue on → internet up → backlog drains → reader recovers → rescue off — every transition timestamped, with `gap_secs` and backlog durations.

### Pillar 4 — Operator UX: calm semaphore, not a red wall

**Backend: per-endpoint lifecycle state.** The backend computes an explicit `EndpointLifecycle` state and sends it over the existing `WsEvent::DeliveryStatus` endpoint struct (extend `WsDeliveryEndpoint`). States:

- `Live` → **green**.
- `Buffering` / `Rescue` / `Recovering` → **blue** ("protected — rescue live — N s behind — recovering automatically"; include ETA / `behind_secs` where known).
- `Attention` → **red**, set **only** for states that genuinely need the operator:
  - endpoint key / OAuth / auth rejected (e.g. RTMP `ConnectRejected` / `PublishRejected` bad-name),
  - local disk **critical**,
  - a chunk abandoned as poison (Pillar 1).

A transient network outage maps to **blue**, never red.

**Frontend: `leptos-ui/src/components/operator_dashboard.rs`**

- Render the lifecycle state directly: green / blue / red dot + label, driven by the new backend field — not by re-deriving from `alive` + `stall_reason` + counts.
- **Top-level outage banner:** when ANY endpoint is in a blue auto-recovery state, show a single calm banner at the top: *"Upstream outage detected — all endpoints protected, rescue video live, recovering automatically. No action needed."* This replaces the wall of N red cards as the dominant visual.
- **Error-string hygiene:** the card shows a short human label (mapped from the error class), not the raw backend string. The raw string stays in the audit log / `last_error` detail, available on expand — never rendered full-width verbatim. Truncate any displayed error to a fixed length.
- Keep the existing version label in the header (`leptos-ui/src/components/header.rs:44-45`).

### Pillar 5 — TDD / CI / E2E (lock it so it can't return)

- **Unit (RED-first):**
  - uploader: a network-class failure across a >10-minute simulated window keeps the chunk retryable and never marks it permanently failed; a structural-reject class is abandoned after the small budget with a `Critical` audit.
  - disk valve: threshold crossings emit `LocalDiskPressure` warn/critical; true write failure emits `DiskWriteFailed` and drops oldest-unsent only.
  - lifecycle state machine: outage inputs → `Buffering/Rescue/Recovering` (blue); only auth/disk-critical/poison → `Attention` (red).
  - audit: each newly-wired `Action` emits exactly once at its transition with the documented detail fields.
- **Integration:** mock S3 disappear → return after a window longer than the old 10-min cap → assert every chunk is retained, retried, and ultimately uploaded **in sequence order with zero drops**; assert the VPS reader resumes from the exact next chunk.
- **E2E centerpiece (new CI job, stream.lan self-hosted runner, mirrors `e2e-obs-youtube` pattern):**
  1. Real pipeline up: OBS → stream.lan → S3 → VPS → endpoint, `Live`.
  2. Simulate the outage by blocking the runner's egress to S3 (firewall rule / route block) for a window exceeding the old 10-min cap (test uses a compressed but >cap-equivalent window via injectable threshold to keep CI time bounded).
  3. Assert during outage: zero chunks dropped (DB/audit), dashboard shows the blue banner (Playwright on the live DOM), audit contains `RescueActivated`, lifecycle is blue not red.
  4. Restore egress. Assert: backlog drains, all chunks delivered in order, `RescueRecovered` + `DiskCacheReaderRecovered` in audit, endpoints back to `Live`.
  5. Browser console zero errors/warnings.
- Runs on every push to dev/main, blocks merges via the `e2e-gate` aggregator.

> Note: the E2E uses an **injectable threshold** (the upload retry/stall window is configurable) so the test proves "survives beyond the old fatal cap" without literally waiting 10+ real minutes — keeping CI bounded while still exercising the never-drop path past the previous failure point.

---

## 4. Data flow during a full outage (target behavior)

1. **Steady state:** OBS → chunks → uploaded to S3 → VPS pulls window → pushes → endpoints `Live` (green).
2. **Outage start (switch/internet down):** S3 PUTs fail. `HostInternetUnreachable` audit. Chunks keep being produced + written to local disk. Uploader retries forever at capped backoff; pending backlog grows. UI: endpoints go **blue** `Buffering`, top banner appears.
3. **VPS starves:** producer reaches the last available chunk, waits. After the stall threshold: `DiskCacheStallTimeout` audit; rescue video starts: `RescueActivated`. UI: **blue** `Rescue`.
4. **Outage persists (minutes → hours):** chunks keep buffering locally; no drops (Pillar 1). Disk valve quiet unless laptop disk actually fills (then `LocalDiskPressure` warn/critical, UI alarm, last-resort oldest-drop only at true full).
5. **Restore (internet/switch back):** `HostInternetRecovered`. Uploader drains the backlog to S3 (capped concurrency). VPS producer finds the next sequential chunk, resumes replay from the exact position; `DiskCacheReaderRecovered`. Rescue exits: `RescueRecovered` with `gap_secs`. UI: **blue** `Recovering` (shows `behind_secs`), then **green** `Live` once steady.
6. **Post-outage:** stream is ~`outage_duration` behind real-time, playing all content in order at 1×. Zero content lost. Full audit timeline reconstructs the whole episode.

---

## 5. Out of scope

- Catch-up / fast-forward burst to close the post-outage latency gap (operator explicitly chose continuity over latency recovery).
- Live-edge jump for continuity endpoints.
- Auto-recreating / tearing down the VPS on network loss (current "wait, don't tear down" behavior is correct for continuity and is preserved).
- Battery/UPS management and OBS-side resilience (the protected core is assumed alive by definition).

---

## 6. Acceptance criteria

1. A simulated S3 outage longer than the old 10-minute cap results in **zero dropped chunks**; all are delivered in sequence order after restore. (unit + integration + E2E)
2. During the outage the dashboard shows a **calm blue "protected / recovering" banner**, not a red error wall; red appears only for auth/disk-critical/poison. (Playwright E2E on live DOM)
3. The audit timeline after the outage contains, in order: internet down, rescue activated, internet recovered, reader recovered, rescue recovered (with `gap_secs`) — plus the previously-dead cache events wired and emitting. (integration + E2E)
4. The new E2E outage-simulation job runs on every push, is wired into `e2e-gate`, and is green.
5. Browser console: zero errors/warnings throughout. (E2E)
6. Version label visible on the dashboard and matching the deployed build. (E2E)

---

## 7. Handoff notes for planning

- Repo `/home/newlevel/devel/restreamer`, branch `dev`, currently `v0.19.1` (== main). **Task 0 of the plan = version bump to `v0.20.0`** across `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` (+ `Cargo.lock`).
- One cohesive feature → **one spec, one plan, one PR** (per MVP / single-feature-single-PR). The E2E outage-sim test is the linchpin task.
- TDD strict: RED before GREEN, one commit per task, visible in `git log --oneline`. This is a defect-class fix (the 10-min cap) → regression tests are mandatory and must be RED on the unfixed code first.
- Tier-2 fast-iterate is active: controller runs `cargo fmt --all --check` + `cargo check --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --no-run --workspace` between batches and before push.
- File-size cap <1000 lines per `.rs`. ASCII-only PowerShell strings in any CI YAML.
- Planning will re-verify exact file:line for every edit with `Read` before `Edit`.
- File tracking GitHub issues during planning (e.g. "outage never-drop", "outage UX semaphore", "audit completeness", "outage E2E") and reference them in commits / PR.
