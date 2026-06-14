# Rescue Actually Works (VPS-side) — Design

**Supersedes the broken parts of** `2026-05-31-always-on-rust-rescue-design.md`. That feature was marked done but shipped with its core protection paths missing and only source-grep tests. This spec makes rescue actually fire for ALL endpoint types on a real outage, recover when the source returns, and be proven by behavioral + crash tests.

**Driving incident:** #251 — operator crashed stream.lan mid-stream, cache drained, NO endpoint switched to rescue, all 7 endpoints of event 9312 went dark. CI was green.

**Goal:** On a sustained source outage (stream.lan crash / cache drain), every enabled endpoint — fast and non-fast — switches to the rescue clip on a fresh RTMP session within ~10s, keeps a watchable preview, and resumes live delivery when the source returns. Proven by tests that actually run the trigger, and by a CI job that kills Restreamer.exe mid-stream.

**Scope:** VPS-side (`rs-delivery`) rescue correctness + recovery + tests. Host-side delivery resume on Restreamer.exe restart is OUT (tracked as #252 follow-up) — the VPS keeps endpoints alive on rescue through a stream.lan crash regardless of host state.

---

## Confirmed root cause (6-reader investigation + adversarial verify)

1. **Fast endpoints excluded from rescue** — `endpoint_task.rs:430` fast branch has only `rx.recv` + 2s freeze-only keepalive + stop; no `run_outage_rescue` arm. `keepalive_until_chunk` is freeze-only and pushes NOTHING when no codec-safe chunk was delivered → dark. (rank 1, #251)
2. **Non-fast rescue gate stuck shut on error-shaped drain** — gated on `!producer_active`, cleared only on `Ok(None)`/404; an `Err`/stall drain leaves it `true`. (#253)
3. **No producer respawn** — endpoint tears down permanently on producer finish; recovery never completes. (#237)
4. **Test gap** — R1/R2/R3 are `read_to_string`+`.contains` source greps; fast tests assert rescue NEVER pushed; crash-E2E never built (#238); lost producer/consumer coverage (#239); `cargo-mutants` excludes the whole pipeline.

`run_outage_rescue` already reconnects a FRESH RTMP session for rescue (`rescue.rs:194-232`) — pushing rescue on the live session is the #249 green-video corruption, so fresh-session is mandatory.

---

## Design

### C1 — Fast-endpoint rescue escalation (#251, operator decision)
Keep the 2s freeze-only keepalive for SHORT gaps (low-latency, no session churn). Add escalation: track contiguous starvation; once starved ≥ `RESCUE_STALL_THRESHOLD_SECS` (8s) **and** `producer_active==false`, the fast consumer calls the SAME `run_outage_rescue` non-fast endpoints use (fresh session, rescue clip). On recovery it returns to normal live delivery and re-enters the fast low-latency path. NEVER push rescue on the existing live session.

### C2 — Open the rescue gate on error-shaped drains (#253)
In the producer `Err`/stall arm (`endpoint_producer.rs:264-278`), count consecutive failures and set `producer_active=false` once they persist past a threshold (mirror the `Ok(None) >= 3` logic), so `run_outage_rescue` fires whether the drain is 404s or errors/wedged-InFlight.

### C3 — Producer respawn for recovery (#237)
In `endpoint_loop` (`endpoint_task.rs:962-972`), when the producer finishes while the consumer is still draining and stop was not signalled, RESPAWN the producer from `last_delivered_chunk_id+1` instead of breaking, so a returning source refills the buffer and rescue's 120s-active recovery can complete. Bound respawns to avoid hot-loops.

### C4 — Real behavioral tests (#239, replaces R1/R2/R3 greps)
A runtime harness in `rs-delivery`: mock `ChunkFetcher` (Ok(None) and Err variants), a recording `Pushable`, `#[tokio::test(start_paused = true)]` to fast-forward the 8s / producer-flip / 120s timers. Assert, for FAST and NON-FAST: on drain → rescue bytes actually pushed + `delivery_mode=="rescue"`; on source-return → producer respawns + live resumes + `RescueRecovered`. Delete the source-grep R1/R2/R3.

### C5 — Crash-recovery CI gate (#238)
Add `e2e-stream-lan-crash-rescue` to `ci.yml`: mid-stream kill Restreamer.exe on stream.lan via MCP, poll the VPS API for `delivery_mode==rescue` within 60s with `last_pushed_chunk_id` advancing; restart Restreamer.exe; assert `RescueRecovered` + normal delivery resumes within 180s. Serialized under the existing `stream-lan-box` concurrency group.

### C6 — Remove mutants exclusions
Once C4 covers them, drop the `--exclude-re` entries for `rescue::`/`run_rescue_loop`/`consumer_task`/`producer_task`/`endpoint_loop` so assertion-free rescue tests can never silently return.

---

## Acceptance
- New behavioral tests are RED on current `dev` (prove the bugs), GREEN after C1–C3.
- `e2e-stream-lan-crash-rescue` CI job green.
- Live verification on stream.lan (operator-approved forced crash, no live event): kill Restreamer.exe mid-stream → all endpoints (incl. fast KS-PP-TEST/Control Kiko) show rescue preview → restart → live resumes.
- `cargo-mutants` exclusions removed; mutation gate green.

## Risks
- Touches the live delivery hot path → behavioral tests + crash E2E + a soak before trusting in production.
- Codec hazard (#249): rescue MUST be a fresh session, never the live one — `run_outage_rescue` already does this; the fast path must call it, not splice rescue into keepalive.
- Cannot build/test locally (dev1 OOM) → RED/GREEN runs through CI.
