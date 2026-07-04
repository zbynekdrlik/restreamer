---
name: outage-rescue
description: >
  Outage survival design, rescue mechanism behavior, and operator decisions
  for the rescue/notification UX cluster. Load when working on outage handling,
  rescue clip activation, keepalive logic, RTMP push behavior during outages,
  or the notification/overlay features (#259-#263, #73).
triggers:
  - outage
  - rescue
  - keepalive
  - stream behind
  - continuity
  - replay
  - EndpointLifecycle
  - outage notification
  - rescue overlay
---

# Outage Survival and Rescue Design

## Locked Design Decisions (DO NOT RE-ASK)

### Recovery = Replay Everything / Continuity

Network-class upload errors (timeout/5xx/conn/other) retry **FOREVER** at capped 30s backoff — never abandoned. Only structural rejects (400/403/404) abandon after a 5-attempt budget (`should_abandon_upload`).

On restore, the VPS replays the backlog in sequence order. **By design the stream stays ~outage_duration behind real-time after a long outage** (strict 1x, no catch-up burst). This is intentional: continuity over latency. Full recording preserved locally for VOD.

**RTMP push to YT/FB MUST ALWAYS be strictly 1x — never speed up or burst to catch up.** Speeding up corrupts quality and YT/FB kills the stream. Recovery is never via speed-up.

### Topology = OBS Local on stream.lan

During an outage, OBS keeps feeding; only the S3 uplink dies. A local backlog always exists.

### Lifecycle Semaphore (EndpointLifecycle)

Defined in `crates/rs-core/src/endpoint_lifecycle.rs`, computed host-side in `delivery_status.rs`:

- **Live** = GREEN
- **Buffering/Rescue/Recovering** = BLUE ("protected, recovering, no action needed")
- **Attention** = RED ONLY for auth/key-reject, disk-critical, or poison chunk

A survivable outage shows BLUE, never RED. Calm `OutageBanner` replaces the red wall. Actionable `last_error` only reds when `!alive` (stale-error guard).

### disk_critical Safety Valve

`disk_pressure.rs` monitor (warn 80%/critical 90%, alert-only, NEVER drops) sets an `AtomicBool` shared via `AppState` → `DeliveryOrchestrator.disk_critical()` → lifecycle goes Attention (red) on disk-full.

## Fast Endpoints and Rescue (Operator Decision 2026-06-14)

**Fast (is_fast) endpoints MUST escalate to the rescue clip on a fresh RTMP session during sustained outage.** This reverses the old "fast unprotected" tradeoff (root cause of #251 all-dark crash test).

**LIVE RTMP session = codec-homogeneous bytes ONLY.** A rescue clip pushed on a live session with different codec settings (wrong SPS/PPS) locks the decoder → solid GREEN video with all health metrics showing good — silent visual corruption. Keepalive is freeze-only. Codec-foreign content requires a session drop + reconnect.

**Rescue activation requires**: push rescue clip on a **fresh** RTMP session (not the existing live session).

## E2E Test Requirements

CI proof for outage survival:
- `e2e-obs-youtube` "Long outage" step blocks S3 for 5 min
- Assert: zero abandoned chunks + rescue activated + calm blue banner (Playwright) + backlog drains in order + rescue recovered
- `RescueRecovered` event fires ~120s after chunks resume (`RESCUE_REFILL_TARGET_SECS`)
- CI assertions must **POLL** for RescueRecovered, not check once

## Rescue UX Cluster — Operator Decisions (2026-06-22)

Design decisions for tickets #259-#263 and #73:

| Ticket | Feature | Decision |
|---|---|---|
| #259 | Rescue overlay language | Slovak text + live countdown timer |
| #260 | No-rescue-url warning | Show warning if no rescue_video_url configured |
| #261 | Discord alert | Implement now (top priority) |
| #263 | Dashboard banner | Implement |
| #73 | Desktop screen-edge glow overlay | RESCOPED: Tauri transparent always-on-top overlay (like iemmixer), desktop screen-edge glow |
| #262, #139 | Phone notifications | DEFERRED — needs-creds (Twilio/VAPID) |

## Known Architecture Gaps (from #251 root cause analysis)

Five failure layers discovered in 2026-06-14 crash test (event 9312 — all 7 endpoints went dark):

1. **Fast endpoints architecturally excluded from rescue** (primary — `endpoint_task.rs:430` fast branch has only `rx.recv` + 2s freeze-only keepalive, no `run_outage_rescue` arm) → #251
2. **No host crash-recovery resume** (poll_handles/endpoint_fast_cache/resume_positions in-memory, reset to empty on crash) → #252
3. **Non-fast rescue gate stuck shut on error-shaped drain** (`run_outage_rescue` gated on `!producer_active`, cleared only on Ok(None)/404 path) → #253
4. **No producer respawn** (spec Component #4a never implemented; endpoint tears down permanently on producer finish/panic) → #237
5. **Test gap = meta-cause**: rescue tests are source-grep only (`rescue_tests.rs:550-829`), never exercise the trigger; fast tests assert rescue NEVER pushed; specced e2e crash job never built (#238); cargo-mutants excludes rescue::/consumer_task/producer_task/endpoint_loop

**Fix constraint**: Cannot build/test locally (dev1 OOM) — all via CI. Crash verification needs forced kill on stream.lan (destructive → requires operator to not be in a live event).

## Outage Audit Events

Wired in PR #232 (v0.20.0) — these events must fire for correct forensic reconstruction:
- 7 previously-dead `DiskCache*` events
- `RescueActivated` (with gap_secs)
- `RescueRecovered` (fires ~120s after chunks resume)
- `RtmpHandshakeFailed`

"Working" = cache ~120s GREEN + 0 deaths + 0 crashes + 0 errors over full soak — NOT just "chunks advancing".
