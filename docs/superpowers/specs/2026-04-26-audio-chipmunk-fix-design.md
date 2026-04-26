# Audio Chipmunk / Glitch Fix — FLV Chunker

**Date:** 2026-04-26
**Trigger:** Live event 2026-04-26 — first event since v0.3.69 (PR #140) merged. Audio over restreamer was chipmunked / glitchy from the start; presenter aborted to fallback (non-restreamer) path.
**Affected component:** `crates/rs-inpoint/src/flv_chunker.rs`
**Severity:** Production blocker. Restreamer is unusable for live events until fixed.

---

## Problem Statement

After PR #140 merged on 2026-04-22, the next live event (2026-04-26) had unusable audio:
audio playback was distorted with a chipmunk pitch shift and glitchy artefacts (symptom C
in operator triage). Video was acceptable. The operator fell back to a non-restreamer
path mid-event.

This is the first live event since v0.3.69, so we have one data point — but symptom + code
diff together give an unambiguous root cause.

---

## Root Cause

Commit `a2800ba` ("fix(inpoint): stamp FLV tags with wall-clock at ingest (#135)") rewrote
both `write_video` and `write_audio` in `flv_chunker.rs` to ignore the xiu-forwarded RTMP
timestamp and stamp tags with `wall-clock - session_start` instead.

For **video** this is defensible: the commit message correctly identifies that OBS
declares 30 fps but captures at ~30.30 fps wall-clock, producing 994 ms of FLV tag time
per wall-clock second and causing the cache-drift described in #135.

For **audio** this is wrong:

- AAC frames have a **fixed cadence**: at 48 kHz each frame is exactly
  `1024 / 48000 = 21.333 ms` of audio. The producer cannot drift; sample rate is the clock.
- RTMP delivery is **bursty** — frames arrive at irregular wall-clock intervals
  (e.g. 18 ms / 25 ms / 19 ms / 27 ms…) even though they each represent 21.333 ms of audio.
- Stamping each AAC frame with arrival wall-clock produces PTS deltas that do not match
  the audio cadence. Downstream the decoder either:
  - resamples to fit the wrong PTS → **chipmunk pitch shift** (sounds slightly fast/slow), or
  - drops / duplicates samples to fit → **glitches, clicks, choppy playback**.

The commit message itself describes a **video-only** problem ("OBS declares timestamps at
frame_index/30fps") but the fix was applied uniformly to audio, where it has no physical
basis. Audio never had a producer-side drift problem.

---

## Approach: Audio-Only Revert

Revert the wall-clock rewrite **for audio only**. Keep the wall-clock rewrite for video
(it solves a real, measured problem in #135). Audio reverts to using the xiu-forwarded
RTMP timestamp, which is already AAC-cadence-accurate because OBS derives audio
timestamps from the sample clock.

### Code changes (`crates/rs-inpoint/src/flv_chunker.rs`)

1. Restore the `timestamp` parameter name in `write_audio` (drop the `_xiu_timestamp`
   underscore prefix).
2. In `write_audio`, replace `let ts = Self::current_session_ts(&mut inner);` with
   `let ts = timestamp;`.
3. Update the doc comment on `write_audio` to reflect that audio uses xiu RTMP timestamps
   directly because AAC has fixed-cadence frames.
4. Leave `write_video`, `current_session_ts`, `session_start_wall_clock_ms`, and
   `reset()` untouched — video keeps the wall-clock fix.

### Why not the alternatives

- **Approach 2 (sample-clock derive from AAC sequence header):** mathematically pure but
  requires an AAC parsing path, more code, and more test surface. We can layer it in
  later as a follow-up if A/V drift turns out to be measurable.
- **Approach 3 (full revert of `a2800ba`):** brings back the cache-drift bug from #135.
  We had a real problem there, the fix worked for video, only audio was wrong. No reason
  to throw away a working fix.

### A/V sync risk

After the fix, audio PTS comes from xiu (OBS sample clock) and video PTS comes from
wall-clock (system clock at chunker). Both are derived from the host system clock, so the
offset between them stays within ~1 frame over hours. If long-stream A/V drift turns out
to be measurable, layer in Approach 2 as a follow-up.

---

## Out of Scope

- Delete-button UI feedback issue (separate spec).
- Dashboard cache headers / version handshake (separate spec).
- A/V sync drift correction (only address if measured to be a problem).
- Replacing the ffmpeg subprocess with pure-Rust RTMP push (issue #103).
- Tightening or replacing the `current_session_ts` design for video.

---

## Testing Strategy

### Unit test (TDD — written first, must fail before fix)

In `crates/rs-inpoint/src/flv_chunker.rs` test module, add `audio_uses_xiu_timestamp_not_wall_clock`:

- Create a `FlvChunkSink` with a chunk duration that won't flush mid-test.
- Write a video keyframe with timestamp 0 to start the chunk.
- Sleep 100 ms.
- Call `write_audio(timestamp = 21, data = …)` with a synthetic AAC frame.
- Sleep 100 ms.
- Call `write_audio(timestamp = 42, data = …)`.
- Force a chunk flush and inspect the buffer.
- Parse the audio FLV tags and assert their stored timestamps are exactly `21` and `42`,
  NOT something near `100` and `200` (which is what wall-clock would produce).

This test fails on `main` today (PTS would be ~100 / ~200) and passes after the fix.

### Audio integrity E2E (CI, blocking)

Extend the existing streaming E2E to verify audio PTS cadence:

- ffmpeg generates a synthetic 30-second `sine` audio + `testsrc` video and pushes via RTMP
  to the local restreamer (this fixture exists in the streaming test).
- After the run, fetch one of the produced S3 chunks (or the local FLV before upload).
- Run `ffprobe -show_packets -select_streams a -of json` against the chunk.
- Assert the median delta between consecutive audio packet PTS values is within `[20, 22]`
  ms. Any value > 24 ms or < 19 ms → fail.
- Assert no audio packet has PTS gap > 50 ms (would indicate drop/burst).

Add this as a gate in the existing CI streaming job. The gate must be binary — succeed and
continue, or fail and stop the build (no informational-only steps).

### Pre-deploy verification on stream.lan

After CI deploys the new binary to stream.lan, before reporting "deploy verified":

- Push a 30-second synthetic RTMP stream from the runner to stream.lan's `1234/live` endpoint.
- Pull one of the produced chunks back via the API.
- Run the same ffprobe PTS-cadence check.
- Fail the deploy job (and any "verified" claim) if cadence is off.

This addresses the post-deploy verification gap that allowed v0.3.70 to be reported "verified"
yesterday despite the UI fix not being functionally tested.

### Live-event operator checklist (documentation)

Create `docs/operator-runbook.md` containing a pre-live checklist; the audio smoke-test is
its first entry:

> **Before going live:** push a 30-second test stream from OBS → listen on the rescue/preview
> output → confirm clean audio. Do this even after every restreamer upgrade.

Future operator-facing checks (rescue cut, delivery drift, etc.) get appended to this file
rather than to `CLAUDE.md`, which stays focused on developer/CI rules.

This is belt-and-braces against future regressions slipping through CI.

---

## Acceptance Criteria

- Unit test `audio_uses_xiu_timestamp_not_wall_clock` exists in `flv_chunker.rs` and passes.
- CI audio-cadence ffprobe gate exists in the streaming E2E job and passes.
- Pre-deploy ffprobe gate exists in the deploy step on stream.lan and passes.
- A test RTMP stream into stream.lan after deploy produces chunks whose audio plays
  cleanly (no chipmunk, no glitch) when fed to ffmpeg.
- Operator runbook includes the pre-event audio smoke-test step.

---

## File Map

| Path | Change |
|------|--------|
| `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` | Bump `0.3.70` → `0.3.71`. |
| `crates/rs-inpoint/src/flv_chunker.rs` | Revert audio-side wall-clock; add unit test asserting xiu-timestamp pass-through for audio. |
| `.github/workflows/ci.yml` | Add audio-cadence ffprobe gate to the streaming E2E job. |
| `.github/workflows/ci.yml` (deploy job) | Add pre-deploy ffprobe gate against stream.lan. |
| `docs/operator-runbook.md` | Create file with pre-live audio smoke-test step. |

---

## Issue Tracking

A GitHub issue will be filed at the start of the implementation plan (Task 0) titled
"Audio chipmunk / glitch caused by wall-clock stamping of AAC frames in flv_chunker"
referencing this spec and PR #140 / commit `a2800ba` as the regression source.
