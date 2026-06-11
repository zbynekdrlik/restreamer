# Delivery Binary Version Lockstep + Trickle-Grow — Design

**Date:** 2026-06-10
**Status:** Approved (operator directive: "hardening needs be implemented")
**Author:** CAWE

## Problem — the 2026-06-10 recurrence

Event PP-live-2026-06-10 failed exactly like the pre-fix events (fast endpoint
KS-PP-TEST reset by YouTube at 18:01 + 18:07 local, operator removed it at
18:14 and switched to OBS direct) — **despite the v0.22.6 fix being installed
on streampp**. Proven from streampp's audit DB + today's VPS log
(`delivery-logs/1781112184-evt17-inst20.log`):

- The VPS log contains **zero** `keepalive` / `fast_delay` lines but 20×
  "no chunks found in probe range" → the VPS ran a **pre-fix rs-delivery**.
- The VPS downloads rs-delivery from **the client's own S3 bucket**
  (`crates/rs-api/src/delivery.rs:324`:
  `binary_url = {s3.endpoint}/{s3.bucket}/rs-delivery`).
- **CI only updates stream.lan's bucket** (`ci.yml`:
  `aws s3 cp … s3://restreamer-chunks-fsn1/rs-delivery`). streampp's nbg1
  object was last modified **May 29** — pre-fix. Nothing ever updated it.
- Conclusion: installing the Windows app does NOT update the VPS-side binary.
  The 06-07 "deploy to streampp" was incomplete, and nothing in the system
  detected or reported the version drift.

Immediate ops fix applied 2026-06-10: release `rs-delivery-0.22.6` PUT to
`nbg1…/restreamer-chunks/rs-delivery` with `x-amz-acl: public-read`
(cloud-init fetches the binary anonymously — a PUT without that ACL produces
403 and breaks VPS boot; verified live), sha256-verified byte-for-byte.

## Goals

1. **Version lockstep is enforced, never assumed.** A VPS running a different
   rs-delivery version than the client must be impossible to miss and (when
   remediable) impossible to happen.
2. **The latent trickle flaw is fixed.** The adaptive delay currently grows
   only after ~80 s of *uninterrupted* misses (`MAX_CHUNK_MISS_COUNT=40` probe
   cycle); streampp's real jitter is a trickle (chunks 5–30 s late with
   successes in between) which resets the miss counter — so the delay never
   grows and viewers see a freeze/rescue blip on every spike.

## Design

### Component 1 — rs-delivery reports its version

- `main()` logs `rs-delivery v{CARGO_PKG_VERSION}` as the FIRST log line
  (today's incident would have been one `grep` to diagnose).
- `/api/health` response gains `version: String`
  (`env!("CARGO_PKG_VERSION")`). `#[serde(default)]` on the orchestrator's
  deserialize side so old VPS binaries (no field) read as `""` = mismatch.

### Component 2 — post-boot version gate (orchestrator, rs-api)

In `start_delivery`'s health-poll loop (`delivery.rs` ~505): when health
passes, compare `health.version` to the client's own `CARGO_PKG_VERSION`
(single workspace version → identical strings when in lockstep).

- Match → audit Info `delivery_binary_version` `{vps_version, client_version}`
  and proceed.
- Mismatch (or missing field = old binary) → audit **Critical**
  `delivery_binary_version_mismatch` `{vps_version, client_version,
  binary_url}`, **abort the delivery start** (delete the VPS, return error to
  the operator's dashboard). Never silently stream on a wrong binary — a hard
  visible failure at start beats a silent broken event (exactly today's
  failure made loud).

### Component 3 — pre-create ensure (orchestrator, rs-api)

Before creating the VPS, ensure the bucket binary matches the client version
using a sidecar object `rs-delivery.version` (plain text, e.g. `0.22.6`):

1. GET sidecar. If sidecar == client version → proceed (cheap, every start).
2. Else: download the GitHub release asset for the client's own version
   (`…/releases/download/restreamer-v{ver}/rs-delivery-{ver}-linux-amd64`
   + `.sha256`), verify sha256, PUT binary to `{bucket}/rs-delivery` **with
   `x-amz-acl: public-read`**, PUT sidecar (same ACL). Audit Info
   `delivery_binary_ensured` `{version, sha256}`. Proceed.
3. No release asset for this version (dev build) AND sidecar mismatch → fail
   delivery start with explicit error (audit Critical, same action as the
   gate). On stream.lan this cannot happen: CI uploads the fresh dev binary +
   sidecar on every deploy (Component 4).

Pure decision logic (`decide_binary_action(sidecar, client_version,
asset_available) → Proceed | EnsureFromRelease | Fail`) is separated from I/O
and unit-tested.

### Component 4 — CI writes the sidecar

Extend the existing `Upload rs-delivery to S3` step in `ci.yml` to also write
`rs-delivery.version` (the workspace version of the build) next to the binary.

### Component 5 — trickle-grow (the latent flaw)

The consumer's keepalive already measures the true starvation gap
(`gap_secs` in `fast_keepalive_ended`). Feed it back to the producer's delay
controller:

- `BufferState` gains `starvation_gap_ms: AtomicU64` (max-accumulate: store
  `max(current, gap_ms)`).
- Consumer: on keepalive end, store the measured gap.
- Producer hot loop: on each successful fetch, `swap(0)` the atomic; if
  non-zero → `ctrl.on_starvation(gap_secs, now)` → emits `fast_delay_grown`.
  The existing probe-cycle grow stays (covers total outages).
- Result: the FIRST freeze of an event grows the read-delay to cover the
  observed spike; subsequent spikes of that size are absorbed silently from
  the buffer. RED test: a trickle pattern (late-but-arriving chunks, never 40
  consecutive misses) must grow the delay — the regression the soak missed.

## New audit actions

`DeliveryBinaryVersion` (Info), `DeliveryBinaryVersionMismatch` (Critical),
`DeliveryBinaryEnsured` (Info). snake_case via existing serde rename.

## Out of scope

- Presigned binary URLs (anonymous + public-read ACL kept — matches the
  working cloud-init; revisit only if the bucket must go private).
- Multi-client fleet management; each client self-ensures its own bucket.

## Verification

- Unit: decide_binary_action matrix; version-compare gate; trickle-grow RED→GREEN.
- CI E2E: existing delivery E2E now implicitly exercises the gate (CI uploads
  fresh binary+sidecar → versions match → delivery proceeds).
- Post-merge: next streampp event — audit log must show
  `delivery_binary_version` match row; if the bucket is ever stale again the
  event start FAILS LOUDLY instead of silently streaming a broken fast stream.
