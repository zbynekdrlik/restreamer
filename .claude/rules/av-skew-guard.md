---
paths:
  - "crates/rs-rtmp-push/src/skew.rs"
  - "crates/rs-rtmp-push/src/skew_tests.rs"
  - "crates/rs-rtmp-push/src/pusher.rs"
  - "crates/rs-inpoint/src/ingest_skew.rs"
---

# A/V-skew guard — the model to know before touching it

Two SEPARATE trackers reuse the same `SkewTracker` (rs-rtmp-push) measurement:

- **Ingest side** (`crates/rs-inpoint/src/ingest_skew.rs`, #354) — DIAGNOSTIC ONLY.
  It observes the chunker-stamped content-PTS at RTMP publish and gates
  `Start Delivering`. It NEVER acts on `SkewDecision::TripRecovery` (the source is
  OBS, owned by camera-box; never restart/reconnect it). Don't wire recovery here.
- **Push side** (`crates/rs-rtmp-push/src/skew.rs`, #257/#359) — the ACTUATOR. On a
  sustained skew it returns `PushError::AvSkewExceeded` → the rs-delivery consumer
  force-closes + reconnects → `push_flv_bytes`' reconnect path calls
  `skew.reset_tracks()`.

## The metric (both trackers)

`raw_skew_ms = video_max_ts − audio_max_ts` over each track's INPUT (chunker-stamped)
PTS on a SHARED origin (the origin cancels). `current_skew_ms = raw_skew − baseline`,
where the baseline is captured on the first both-tracks chunk after a reset — so a
CONSTANT offset present from the first chunk folds into the baseline and reads ~0; only
a skew that CHANGES mid-stream is visible.

**Time model (since #367): the chunker stamps BOTH tracks in the publisher's source-ts
domain (`out = src_ts − session_origin`), and the pusher maps both through ONE shared
origin + base.** Nothing is wall-clock-stamped any more (before #367 audio was xiu-ts and
video wall-clock — the split domain that caused the 2026-10-01 desync). So a constant
offset at chunk 0 is the PUBLISHER's own A/V relation, and an offset the PIPELINE makes
is caught by the ABSOLUTE `AvInvariantGuard` (no baseline), not by this relative guard.
The full model, the invariant, and the guard live in `.claude/rules/av-time-model.md` —
read it before changing any stamping, origin, or base logic.

## STEP vs DRIFT — a reconnect only fixes ONE of them (#359, the death-loop lesson)

- A **STEP** desync (a republish freezing a fixed inter-track offset) is fixed by a
  clean reconnect + symmetric re-anchor → converges in ONE reconnect. KEEP killing it.
  **Caveat since #367:** the pusher's shared origin + base make a reconnect keep the
  wire A/V relation EQUAL to the content relation, so a reconnect can no longer fix a
  STEP that is already in the content — the kill now only costs a reconnect. The
  ROZHODNUTÉ on #367 (issuecomment-5997672756) keeps `TripRecovery` unchanged for the first
  production event after #367 (one variable at a time); switching it to alert-only is
  tracked on #359. Do not change it in passing.
- A **DRIFT** (audio/video advancing at slightly different rates) is NOT fixed by a
  reconnect: `reset_tracks()` re-zeroes the baseline and the same drift re-accumulates
  past threshold and trips again → a death-loop by construction. Reconnecting a drift
  is like restarting a clock to fix its tick rate.

The push guard (#359) distinguishes them with a `GuardMode { Normal, DriftHold }` state
machine: a trip CHAINED within `SKEW_LOOP_WINDOW_MS` of the previous (a converged STEP
never re-trips) drives a bounded streak; past `SKEW_LOOP_MAX_DRIFT`/`_STEP` it enters
`DriftHold` and only the hard cap `SKEW_HOLD_MAX_MS` trips, rate-limited. Invariants when
editing it:

- **Never weaken into "never-kill".** DriftHold still kills at the hard cap; a real large
  STEP must still trip fast in Normal. The kill protects YouTube from a real desync.
- **`loop_streak` / `mode` / `last_over_threshold_ms` / `hold_since_ms` MUST persist across
  `reset_tracks()`** — the death-loop is only visible ACROSS reconnects. The per-connection
  classifier (`prev_skew_ms` / `max_abs_step_ms`) clears.
- The chain/decay is WINDOW-based, so it covers drift rates ≳ 6.7 ms/s (the observed
  signature ~20–26 ms/s). A much slower drift is a known gap needing a longer-baseline,
  jitter-immune rate estimator — a per-chunk delta can't classify it (benign jitter is
  ~100 ms/chunk >> a slow drift's ~13 ms/chunk).
- `SkewTracker` derives only `Default`, so any WRITE-ONLY field trips the dead-code lint
  (`clippy --workspace --all-targets -D warnings` is a CI gate) — read every field you add
  (e.g. into a trip log).

Tests live in `skew_tests.rs` (split out via `#[cfg(test)] #[path=...] mod tests;` for the
1000-line cap). Reproduce a divergence with synthetic per-chunk PTS + `drive_drift`; assert
a BOUNDED trip count + `mode()==DriftHold`, and separately that a genuine STEP still trips
once and recovers.
