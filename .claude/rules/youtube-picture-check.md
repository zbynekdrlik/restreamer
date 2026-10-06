---
paths:
  - "e2e/youtube-studio-check.spec.ts"
  - "e2e/lib/frame-analysis.ts"
  - "e2e/frame-analysis.unit.spec.ts"
  - "e2e/playwright-youtube.config.ts"
---

# YouTube content-level picture check (#249)

The 2026-06-11 green-video incident (a rescue clip pushed first on a live
session made YouTube lock a wrong codec config → **solid green for the whole
session**) passed *every* existing gate: bitrate/frames flowed, `liveStreams`
health stayed good, the push never died, dashboards were green. Only a human
looking at the picture saw it. This is the "local signals are not proof" lesson
applied to YouTube's **decoded output** — no health field describes the pixels.

## The two pieces

1. **`e2e/lib/frame-analysis.ts`** — pure, browser-free analysis:
   - `analyzeFrame(rgba, w, h)` → `{maxStd, distinctColors, meanLuma}` (per-channel
     stddev, count of 4-bit-quantized colours, Rec.601 luma).
   - `isFlat(a)` — FLAT iff `maxStd < 6` **AND** `distinctColors <= 4` (the AND is
     what keeps a two-tone slide — few colours but high variance — out of FLAT).
   - `allSamplesFlat(samples)` — the temporal verdict: true only when EVERY sample
     is flat (a single non-flat sample rescues it → a fade/scene-switch cannot
     false-fail; an empty set is never a vacuous pass).
   - `frameDiff(a, b, w, h)` — mean abs per-pixel diff (freeze signal).
   - Locked by **`e2e/frame-analysis.unit.spec.ts`**, wired into the ubuntu
     **`frontend-e2e`** job (`playwright-frontend.config.ts` testMatch) — runs on
     every push, deterministic, no real YouTube. This is the calibrated core.

2. **`e2e/youtube-studio-check.spec.ts`** — the live surface. It samples the
   Studio Live Control Room preview `<video>` (the only reachable YouTube-*rendered*
   picture surface — there is no video-id/HLS resolution in this repo) onto a
   32×18 canvas via in-page `getImageData` (frame only — NOT an element screenshot,
   whose control-bar/badge/timestamp pixels could mask a green field), then runs the
   analysis above over 3 samples 10 s apart. Gated by `YT_PICTURE_GATE`:
   - **`shadow`** (default): logs `maxStd`/`distinct`/`meanLuma`/`meanDiff` per sample
     and WARNs on the FLAT verdict, so CI collects calibration data without redding
     the fleet. The readiness/decode-liveness precondition still hard-fails.
   - **`enforce`**: a *sustained* flat field (all 3 samples flat) FAILS.
   - **`off`**: skip picture sampling entirely.
   - In both live modes: an **absent** preview surface is a WARNING in shadow
     (Studio DOM churn must not red the fleet pre-calibration), a FAIL in enforce; a
     `<video>` that never advances its decoded-frame counter + clock within 60 s is a
     decode-stall/freeze and **hard-fails in both modes** (the one freeze-class check
     safe to enforce now). A `getImageData` `SecurityError` (cross-origin taint) is a
     warn/fail per mode, pointing at the pngjs-fallback follow-up.

## Secret hygiene (non-negotiable)

The Live Control Room page carries the **stream-key panel**. Two facts, stated accurately:

- **The #249 picture gate itself never screenshots** — it reads pixels via in-page
  `canvas`/`getImageData` of the preview `<video>` only. It writes no image files.
- **The pre-existing debug screenshots DO capture the full page.** `youtube-studio-check.spec.ts`
  takes several `page.screenshot({ fullPage: true })` of Studio (incl. the Live Control Room)
  into a **local box directory** `SCREENSHOT_DIR` (`~/.playwright-yt-screenshots`, NOT the
  `e2e/playwright-report/` dir that `frontend-e2e` uploads) — so they are **not** currently
  uploaded as CI artifacts. `playwright-youtube.config.ts` sets `screenshot/video/trace:'off'`
  as belt-and-suspenders; note this only governs Playwright's *fixture* context, and this spec
  launches its own `launchPersistentContext`, so for THIS spec the setting is a harmless no-op
  (those defaults are already off) rather than the thing that protects the key.

**When wiring this into CI (follow-up), the stream-key panel must not leak:** ensure the CI step
does NOT upload `SCREENSHOT_DIR`, and mask/element-scope (or drop) the pre-existing full-page
Studio screenshots before an `actions/upload-artifact` ever points at them. Never print a stream
key / OAuth token to the log.

## Not yet a wired always-on gate — and why

`youtube-studio-check.spec.ts` is **not** invoked by `ci.yml` today (the
`e2e-obs-youtube-test` job re-implements health checks natively against
`/api/v1/youtube/status`). Wiring the picture gate is deferred because: (a) the
persistent Google profile is **not CI-provisioned** — the runner runs as SYSTEM, so
the profile must live where SYSTEM reads it, seeded by a one-time **headed** operator
login; (b) session expiry has no programmatic re-auth → a guaranteed eventual red on
the single shared runner; (c) the FLAT thresholds want real-run calibration first.

### How it WILL be wired (the plan)

A new step inside `e2e-obs-youtube-test`, **inside the sustained window after YouTube
health reaches "good"**:

```yaml
- name: "GATE: decoded picture is not a uniform field (#249)"
  shell: powershell
  timeout-minutes: 4
  env:
    YT_PROFILE_DIR: "C:\\path\\readable\\by\\SYSTEM\\.playwright-yt-profile"
    YT_PICTURE_GATE: "shadow"   # -> "enforce" after N runs of clean metrics
  run: |
    Set-Location "$env:GITHUB_WORKSPACE\e2e"
    npx playwright test --config playwright-youtube.config.ts
```

Run it in `shadow` for the first N cycles, read the logged `maxStd`/`distinct`
metrics from real content, then flip to `enforce`. Tracked by the follow-up issue
(profile provisioning + wiring, `Scope-gate: security-boundary`, `needs-owner-action`).
The A/V-desync content check (the #249 2026-06-19 reinforcement) is a separate,
cross-repo follow-up (needs a synchronized beep/flash test signal in the
camera-box-owned OBS scene + an audio-capture path).
