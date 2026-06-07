# Self-Healing Fast Stream — Design

**Date:** 2026-06-07
**Status:** Approved (brainstorming) — ready for implementation plan
**Author:** CAWE

## Problem

The "fast" delivery endpoint restarts in a loop during production events and
forces the operator to abandon restreamer and push from OBS directly. Reported
on the last two PP-live events; worst case 39 consecutive restarts in one event.

## Root cause (proven from streampp production data, 2026-06-07)

The fast endpoint is the **only** endpoint with `is_fast = 1`
(`endpoint_configs` id 22, `KS-PP-TEST`, `YT_RTMP`, rust pusher).

`crates/rs-delivery/src/endpoint_task.rs:132`:

```rust
let effective_delay = if ep_cfg.is_fast { 0 } else { delivery_delay_ms };
```

- Normal endpoints read S3 **120 s behind live** (`delivery.delivery_delay_secs = 120`)
  — a 2-minute buffer that absorbs any upload hiccup.
- The fast endpoint reads at **delay 0 = the live edge** and re-pins to the
  newest chunk. **Zero buffer.**

S3 upload from the local machine has a long latency tail. Measured on the same
S3 bucket (`restreamer-chunks`, nbg1), same event window:

| capture→S3 lag | stream.lan | streampp |
|---|---|---|
| average | 0.5 s | 1.3 s |
| **max** | **2.2 s** | **90.8 s** |
| chunks > 5 s | 0 | 106 |
| chunks > 20 s | 0 | 7 |
| disk used | 63.6 % | **82.8 %** |

Identical S3 backend, wildly different tails. **S3 / the Hetzner region move is
fine** (stream.lan proves it). streampp's 90 s spikes track its near-full disk
(82.8 %) plus per-minute cache eviction churning local I/O.

The failure chain (each link confirmed in code + production logs):

1. Fast endpoint pins to the live edge (0 buffer).
2. A local upload spike (up to 90 s) leaves the next live-edge chunk not yet in S3.
3. VPS producer logs `Producer: no chunks found in probe range`
   (8× in today's `delivery-logs/1780828999-evt16-inst19.log`).
   `delivery_endpoint_status` for the fast endpoint shows `current_chunk_id=1410`
   vs `chunks_processed=1328` → 82 chunks never delivered.
4. Fast endpoints **skip rescue by design** (`crates/rs-delivery/src/rescue.rs`),
   so the RTMP push goes **idle** (bytes/sec → 0).
5. YouTube resets the idle connection:
   `endpoint_rtmp_push_died … "upstream closed connection mid-stream: connection reset"`,
   `chunks_pushed=30 time_since_connect_ms=84990` (≈0.35× realtime — starved).
6. Reconnect → re-pin to live edge → starve again → loop. Worst case:
   `delivery_restart_log` shows 39 restarts, every one `lifetime_secs=0`, 15 s apart.

Normal endpoints were healthy throughout (single mid-stream reset after ~1 h, or
steady 1× pacing in the VPS log). The fast path is the only fragile one, and it
failed exactly as designed under upload jitter. It shipped CI-green because no
test simulates upload jitter against a fast endpoint.

## Design principles (operator directive)

1. **Starvation must NEVER crash or end the fast stream.** A missing chunk is a
   wait, never a fatal error. The RTMP connection is torn down only on a real
   socket error — never because the producer is behind.
2. **Restreamer self-tunes the delay to a working value.** The fast endpoint's
   delay grows when starving and shrinks when healthy — chasing the lowest
   *working* latency automatically, not a hand-set constant.
3. **The viewer sees buffering-then-resume, like an OBS freeze** — never a dead
   stream. (Freeze last frame, then rescue loop for long gaps.)
4. **Push is always strictly 1×.** Recovery is never via speed-up/burst
   (corrupts quality; YouTube/FB kill sped-up streams). Grow = hold and let the
   buffer rebuild; shrink = an occasional controlled keyframe skip.

## Architecture

All new behaviour lives on the VPS delivery side (`crates/rs-delivery`), where
the producer sees chunk misses and the consumer controls the RTMP push. The
one-time live-edge jump in `rs-api` (`delivery_live_edge.rs`) is subsumed by the
adaptive controller's initial state. State is per-endpoint, in-memory on the VPS.

### Component 1 — Adaptive delay controller (`fast_delay.rs`, new)

Per fast endpoint, the producer keeps a dynamic `target_delay_chunks`.

- **Init:** floor (`FAST_DELAY_FLOOR_SECS`, default 5 s).
- **Grow (on starvation):** when the producer cannot supply the next chunk and
  the consumer buffer is at/near empty, raise the target to
  `max(target, observed_deficit + FAST_DELAY_MARGIN_SECS)`, capped at
  `FAST_DELAY_CEILING_SECS` (default 120 s = same safety as the normal stream).
  `observed_deficit` = (newest chunk available in S3) − (chunk we needed),
  expressed in seconds. The read position does not move; the consumer holds
  (keepalive) until the buffer has rebuilt to `target_delay`, then resumes at 1×.
  Net effect: the steady-state read position is now `target_delay` behind live.
- **Shrink (on sustained health):** after `FAST_HEALTHY_SHRINK_SECS` (default
  180 s) with zero misses, lower the target by one step
  (`FAST_DELAY_SHRINK_STEP_SECS`, default 5 s) toward the floor, executed as a
  single controlled skip-ahead at the next keyframe (drop a few chunks; one
  small, infrequent glitch — acceptable for a low-latency stream). Never by
  speeding up the push.
- Emits audit events (with values): `fast_delay_grown`, `fast_delay_shrank`,
  carrying `from_secs`, `to_secs`, `observed_deficit_secs`.

The controller is pure logic over (current buffer, newest-available chunk,
miss history) and is unit-testable in isolation.

### Component 2 — Never-crash keepalive (extend `rescue.rs` / consumer)

When the consumer buffer empties and the producer is behind (starvation), the
consumer enters **keepalive** instead of going idle or returning a terminal
error:

- **Freeze last frame:** cache the last AVC sequence header + last IDR keyframe
  (GOP) seen on the wire; re-emit it with advancing timestamps + silent AAC,
  reusing the existing rust pusher timestamp-continuity machinery
  (`rust_rescue_push`). Glass looks frozen, exactly like an OBS hiccup.
- **Long-gap fallback:** if the gap exceeds `FAST_KEEPALIVE_RESCUE_AFTER_SECS`
  (default 10 s), switch to the existing rescue loop FLV (fast endpoints are
  enabled for rescue as the long-gap fallback; they currently skip it).
- **Resume:** when the buffer has rebuilt to `target_delay`, resume real chunks
  seamlessly via `FlvStreamNormalizer::new()` (the existing reset path that
  guarantees monotonic timestamps across a rescue→live transition).
- **Connection lifecycle:** starvation NEVER closes the RTMP connection. Only a
  genuine socket error triggers reconnect (existing backoff). On reconnect, the
  endpoint re-enters keepalive if still starved — it can never enter a
  starvation-driven death loop.
- Emits `fast_keepalive_started` / `fast_keepalive_ended` with `mode`
  (`freeze` | `rescue`) and `gap_secs`.

### Component 3 — Reduce local jitter (`rs-endpoint` uploader / `rs-core` disk)

Secondary mitigation — shrink the jitter source so the controller rarely has to
grow:

- **Tighter local cache retention:** lower the local chunk-cache cap so the disk
  stays well below the pressure threshold, reducing the per-minute eviction
  churn that fattens the upload-latency tail. (Exact cap decided in the plan
  against current retention code.)
- **Quiet the disk-pressure log spam:** `local_disk_pressure` currently logs
  every 60 s (565 rows). Change to log only on **level transitions**
  (ok→warn→critical), per the comprehensive-logging "log state transitions"
  rule — keeps the signal, drops the noise.
- **Ops note (not code):** streampp's disk is 82.8 % full. Freeing disk space is
  an operator action (no automatic remote deletion without approval).

### Component 4 — Regression test (`rs-delivery` integration + unit)

The CI gap that let this ship green. Add:

- **Unit:** `fast_delay` controller grow/shrink law — given a miss with deficit
  D, target grows to D + margin (capped); after a healthy window, shrinks one
  step; never below floor, never above ceiling.
- **Integration:** a `ChunkFetcher` mock that injects a 30–90 s availability gap
  for a fast endpoint. Assert: (a) the pusher never returns a terminal error /
  the connection is never closed by starvation; (b) keepalive frames are emitted
  during the gap (freeze, then rescue after 10 s); (c) on chunk availability the
  stream resumes with monotonic timestamps; (d) `target_delay` grew during the
  gap and shrinks back after the healthy window.

## Data flow

```
OBS ─RTMP→ local inpoint ─chunk→ local disk ─uploader→ S3
                                                 │  (latency tail: 0.5–90 s)
                                                 ▼
VPS producer: fetch chunk_id from S3
   ├─ present → push to buffer; health++; maybe shrink target
   └─ missing → grow target; signal consumer
VPS consumer: pull from buffer ─1×→ rust pusher ─RTMP→ YouTube
   └─ buffer empty + producer behind → KEEPALIVE (freeze→rescue), connection held
                                       resume when buffer ≥ target_delay
```

## Error handling

- Missing chunk → grow + keepalive. Never fatal.
- Real socket error → existing reconnect/backoff; re-enter keepalive if starved.
- Rescue asset unavailable → fall back to freeze-frame (already in hand).
- Controller target is clamped to `[floor, ceiling]`; ceiling = normal-stream
  safety, so the worst case degrades to "as safe as the main stream," never dies.

## Constants (defaults — tunable in the plan)

| Constant | Default | Meaning |
|---|---|---|
| `FAST_DELAY_FLOOR_SECS` | 5 | Lowest fast-stream latency when healthy |
| `FAST_DELAY_CEILING_SECS` | 120 | Max delay (= normal stream) |
| `FAST_DELAY_MARGIN_SECS` | 5 | Headroom added above observed deficit on grow |
| `FAST_DELAY_SHRINK_STEP_SECS` | 5 | Step size when shrinking |
| `FAST_HEALTHY_SHRINK_SECS` | 180 | Healthy window before a shrink step |
| `FAST_KEEPALIVE_RESCUE_AFTER_SECS` | 10 | Freeze→rescue switch threshold |

## Out of scope

- Changing normal-endpoint behaviour (120 s buffer stays).
- Speed-up/burst recovery (banned — push is always 1×).
- Automatic remote disk deletion (operator action).

## Verification

- CI: new unit + integration tests above (deterministic, no real VPS).
- Post-merge soak on streampp: a full event with the fast endpoint enabled,
  asserting zero `endpoint_rtmp_push_died` from starvation, visible
  grow/shrink audit events, and a continuous stream through an induced disk-busy
  spike. "Working" = sustained soak, 0 deaths, 0 crashes (per project DoD).
