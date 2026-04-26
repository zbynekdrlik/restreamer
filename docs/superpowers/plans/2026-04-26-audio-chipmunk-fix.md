# Audio Chipmunk Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore correct audio playback over restreamer by reverting the wall-clock timestamp rewrite for AAC frames in `flv_chunker.rs`, and gate future regressions with a CI audio-cadence ffprobe assertion.

**Architecture:** Audio-only revert of commit `a2800ba`. `write_audio` goes back to using the xiu-forwarded RTMP timestamp (which is sample-clock-accurate by construction). `write_video` keeps the wall-clock fix that solved #135 cache drift. A new ffprobe step in the existing `e2e-streaming-test` job asserts audio packet PTS cadence falls within `[20, 22] ms` median delta — this gate doubles as post-deploy verification because the job runs on the `stream-lan` self-hosted runner against the freshly-deployed binary.

**Tech Stack:** Rust 1.85 (workspace), tokio, xiu RTMP, ffmpeg/ffprobe, GitHub Actions, PowerShell on stream.lan runner.

**Spec:** `docs/superpowers/specs/2026-04-26-audio-chipmunk-fix-design.md`

---

## Context Notes for the Implementer

- **Branch state:** `dev = main = 0.3.70` after PR #141. Task 2 must bump to `0.3.71` before any other change.
- **No test deletion required.** The pre-loaded plan context referred to a buggy "audio wall-clock test" — but inspecting `crates/rs-inpoint/src/flv_chunker.rs` shows that the three tests added by `a2800ba` (`write_video_first_frame_stamps_ts_zero` line 831, `session_start_resets_on_reset` line 854, `wall_clock_ts_monotonic_across_frames` line 891) are all video-side. The only existing audio test is `saves_audio_sequence_header` at line 688 which tests sequence-header storage and returns early before the timestamp logic — it stays unchanged. Task 3 adds a new test only.
- **Local checks: `cargo fmt --all --check` only.** Do NOT run `cargo build`, `cargo test`, `cargo clippy`. Compiles run on CI per `airuleset/ci-push-discipline.md`. The TDD "failing test" semantics here are: the new test asserts behavior the buggy code does NOT satisfy, and CI runs it.
- **No push.** The implementing subagent commits locally only. The orchestrator pushes after all tasks are committed (Task 7).
- **The CI streaming gate doubles as post-deploy verification.** The existing `e2e-streaming-test` job already runs on the `[self-hosted, windows, stream-lan]` runner AFTER `deploy-stream-lan`. Adding the ffprobe assertion to that job (Task 5) covers both spec requirements ("CI audio integrity gate" + "pre-deploy verification on stream.lan") in one place — the spec's two-gate description is conceptual; in production they collapse to one augmented job.

---

## File Map

| Path | Change | Owner task |
|------|--------|------------|
| (GitHub Issues) | Open new issue tracking this regression. | Task 1 |
| `Cargo.toml` (workspace), `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` | Bump `0.3.70` → `0.3.71`. | Task 2 |
| `crates/rs-inpoint/src/flv_chunker.rs` | Add new test `audio_uses_xiu_timestamp_not_wall_clock` in the existing `mod tests` block. | Task 3 |
| `crates/rs-inpoint/src/flv_chunker.rs` | In `write_audio`: rename `_xiu_timestamp` → `timestamp`, replace `let ts = Self::current_session_ts(&mut inner);` with `let ts = timestamp;`, update doc comment. | Task 4 |
| `.github/workflows/ci.yml` | New PowerShell step inside `e2e-streaming-test` job (after `Wait for final chunk upload`) that fetches a chunk file path via the API, reads the local FLV, runs ffprobe, asserts audio packet PTS cadence. | Task 5 |
| `docs/operator-runbook.md` (new) | Pre-live audio smoke-test entry. | Task 6 |
| (push + PR + monitor + verify) | Orchestrator-only. | Task 7 |

---

## Task 1: File GitHub Issue

**Files:** none (GitHub-only).

- [ ] **Step 1: Create the issue**

```bash
gh issue create \
  --title "Audio chipmunk / glitch — wall-clock stamping of AAC frames in flv_chunker (regression from PR #140 / commit a2800ba)" \
  --body "$(cat <<'EOF'
## Symptom

Live event 2026-04-26 (first event after v0.3.69 / PR #140 merged on 2026-04-22) had unusable audio over restreamer. Symptom matched triage option C: chipmunk pitch shift + glitches. Presenter aborted to fallback (non-restreamer) path mid-event.

## Root cause

Commit \`a2800ba\` ("fix(inpoint): stamp FLV tags with wall-clock at ingest") rewrote both \`write_video\` and \`write_audio\` in \`crates/rs-inpoint/src/flv_chunker.rs\` to ignore xiu-forwarded RTMP timestamps and stamp tags with wall-clock arrival time instead.

For video that is defensible (it solves the OBS 30fps-declared-vs-30.30fps-actual drift in #135). For audio it is wrong:

- AAC at 48kHz has fixed-cadence frames (1024 samples / 48000 Hz = 21.333 ms).
- RTMP delivery is bursty — frames arrive at irregular wall-clock intervals.
- Stamping each AAC frame with arrival wall-clock produces PTS deltas that don't match the audio cadence, so the decoder either resamples (chipmunk pitch shift) or drops/duplicates samples (glitch).

The original commit message describes a video-only problem; the fix was applied to audio uniformly with no physical basis.

## Fix

Audio-only revert: \`write_audio\` goes back to using the xiu-forwarded RTMP timestamp. \`write_video\` keeps wall-clock (preserves #135 fix).

## Spec / Plan

- Spec: \`docs/superpowers/specs/2026-04-26-audio-chipmunk-fix-design.md\`
- Plan: \`docs/superpowers/plans/2026-04-26-audio-chipmunk-fix.md\`
EOF
)"
```

- [ ] **Step 2: Capture the issue number**

```bash
NEW_ISSUE=$(gh issue list --limit 1 --json number --jq '.[0].number')
echo "Issue number: #$NEW_ISSUE"
```

The implementing subagent for Tasks 3-6 must read this number and substitute it for `#NN` in the commit messages below. The orchestrator is responsible for providing the captured number to each subagent dispatch.

No commit. The issue lives on GitHub; nothing changes in the repo.

---

## Task 2: Version Bump

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump root `Cargo.toml`**

Change:
```toml
version = "0.3.70"
```
To:
```toml
version = "0.3.71"
```

- [ ] **Step 2: Bump `src-tauri/Cargo.toml`**

Change:
```toml
version = "0.3.70"
```
To:
```toml
version = "0.3.71"
```

- [ ] **Step 3: Bump `src-tauri/tauri.conf.json`**

Change:
```json
"version": "0.3.70"
```
To:
```json
"version": "0.3.71"
```

- [ ] **Step 4: Bump `leptos-ui/Cargo.toml`**

Change:
```toml
version = "0.3.70"
```
To:
```toml
version = "0.3.71"
```

- [ ] **Step 5: Verify formatting**

Run:
```bash
cargo fmt --all --check
```
Expected: exit code 0, no output.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.71"
```

---

## Task 3: Failing Test — Audio Tags Carry XIU Timestamp

**Files:**
- Modify: `crates/rs-inpoint/src/flv_chunker.rs` (append a new `#[tokio::test]` inside `mod tests` at the end of the file, just before the closing `}` at line 919)

**Why this test fails on current code:** today `write_audio` calls `current_session_ts(&mut inner)` which returns `0` for the first audio frame and a wall-clock-derived delta for subsequent frames. The test below writes audio with xiu timestamps `21` and `42` after a 50 ms delay and asserts the stored FLV tag timestamps are exactly `21` and `42`. Current code produces approximately `0` (or wall-clock-derived ~50) — the assertion fails.

- [ ] **Step 1: Add the test**

Insert this `#[tokio::test]` block inside `mod tests { … }` (the block that starts around line 665 and closes at line 919 — append just before the final `}` of the module):

```rust
    /// Audio FLV tags must carry the xiu-supplied RTMP timestamp verbatim,
    /// not a wall-clock-derived value. AAC at 48 kHz has fixed-cadence frames
    /// (1024 samples / 48000 Hz = 21.333 ms). Wall-clock stamping introduces
    /// RTMP jitter into PTS, which the downstream decoder interprets as
    /// resampling cues — producing chipmunk pitch shift and glitches.
    /// Regression test for the live-event failure on 2026-04-26.
    #[tokio::test]
    async fn audio_uses_xiu_timestamp_not_wall_clock() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        // Seed sequence headers (audio + video) so the chunk machinery is ready.
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;
        let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
        sink.write_audio(0, &audio_seq).await;

        // Start the chunk with a video keyframe.
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        // Sleep long enough that wall-clock stamping would produce a clearly
        // different value than the xiu timestamp we pass in.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Write two AAC payload tags with explicit xiu timestamps.
        // Byte 0 = 0xAF (AAC + stereo + 16-bit + 44.1k indicator),
        // byte 1 = 0x01 (raw frame, NOT sequence header).
        let aac_frame = BytesMut::from(&[0xAF, 0x01, 0x12, 0x34, 0x56][..]);
        sink.write_audio(21, &aac_frame).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        sink.write_audio(42, &aac_frame).await;

        // chunk_last_ts is updated by every write_audio call. After the second
        // call it must equal 42, the xiu timestamp we just supplied — NOT
        // ~100 (wall-clock since session start) and NOT 0.
        let inner = sink.inner.lock().await;
        assert_eq!(
            inner.chunk_last_ts, 42,
            "audio FLV tag must carry xiu timestamp 42, got {}",
            inner.chunk_last_ts
        );
    }
```

- [ ] **Step 2: Verify formatting**

Run:
```bash
cargo fmt --all --check
```
Expected: exit code 0, no output.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-inpoint/src/flv_chunker.rs
git commit -m "test(inpoint): assert audio tags pass through xiu timestamp (#NN)"
```

(Replace `NN` with the issue number captured in Task 1.)

---

## Task 4: Implement Audio Revert

**Files:**
- Modify: `crates/rs-inpoint/src/flv_chunker.rs:250-285` (the `write_audio` function — exact line range as of `0c98390` on `main` and `0.3.71` head)

- [ ] **Step 1: Replace `write_audio`**

Locate the current `write_audio` block (starts at line 250 with the doc comment `/// Process an audio frame from xiu's FrameData::Audio.`):

Current:
```rust
    /// Process an audio frame from xiu's FrameData::Audio.
    ///
    /// `_xiu_timestamp` is the OBS-declared timestamp — intentionally ignored.
    /// See write_video for the reasoning.
    pub async fn write_audio(&self, _xiu_timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && (data[0] >> 4) == 0x0A && data[1] == 0x00;

        let pending = {
            let mut inner = self.inner.lock().await;

            // Always save sequence headers (even in null mode, for state tracking)
            if is_sequence_header {
                inner.audio_sequence_header = Some(data.clone());
                debug!("FLV audio sequence header saved ({} bytes)", data.len());
                return;
            }

            if inner.null_mode {
                return;
            }

            // Only write audio if a chunk has been started (by a video keyframe)
            if inner.chunk_start.is_none() {
                return;
            }

            let ts = Self::current_session_ts(&mut inner);
            inner.chunk_last_ts = ts;
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, ts, data);
            None
        };

        if let Some(pending) = pending {
            self.spawn_write(pending);
        }
    }
```

Replace with:
```rust
    /// Process an audio frame from xiu's FrameData::Audio.
    ///
    /// `timestamp` is the xiu-forwarded RTMP timestamp (in milliseconds).
    /// Audio uses xiu timestamps directly — unlike video, which is rewritten
    /// to wall-clock to fix #135 cache drift. AAC frames have fixed cadence
    /// (1024 samples / 48000 Hz = 21.333 ms), so the producer cannot drift,
    /// and wall-clock stamping introduces RTMP delivery jitter into PTS,
    /// causing decoder resampling artefacts (chipmunk pitch + glitches).
    pub async fn write_audio(&self, timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && (data[0] >> 4) == 0x0A && data[1] == 0x00;

        let pending = {
            let mut inner = self.inner.lock().await;

            // Always save sequence headers (even in null mode, for state tracking)
            if is_sequence_header {
                inner.audio_sequence_header = Some(data.clone());
                debug!("FLV audio sequence header saved ({} bytes)", data.len());
                return;
            }

            if inner.null_mode {
                return;
            }

            // Only write audio if a chunk has been started (by a video keyframe)
            if inner.chunk_start.is_none() {
                return;
            }

            let ts = timestamp;
            inner.chunk_last_ts = ts;
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, ts, data);
            None
        };

        if let Some(pending) = pending {
            self.spawn_write(pending);
        }
    }
```

The two changes are:
1. Doc comment rewritten to explain why audio uses xiu timestamps directly.
2. `let ts = Self::current_session_ts(&mut inner);` → `let ts = timestamp;`.

`write_video`, `current_session_ts`, `session_start_wall_clock_ms`, `reset()`, and all other code is **untouched**.

- [ ] **Step 2: Verify formatting**

Run:
```bash
cargo fmt --all --check
```
Expected: exit code 0, no output.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-inpoint/src/flv_chunker.rs
git commit -m "fix(inpoint): use xiu RTMP timestamp for audio frames (#NN)"
```

(Replace `NN` with the issue number captured in Task 1.)

---

## Task 5: CI Audio-Cadence ffprobe Gate

**Files:**
- Modify: `.github/workflows/ci.yml` (insert a new step inside the `e2e-streaming-test` job, after the existing `Wait for final chunk upload` step at line 1425, BEFORE the `GATE clear-s3 endpoint deletes 250+ chunks within 25s` step at line 1459)

**Why this placement works as the spec's "pre-deploy verification on stream.lan":** the `e2e-streaming-test` job runs on `runs-on: [self-hosted, windows, stream-lan]` and `needs: [deploy-stream-lan]`. So this step executes against the freshly-deployed binary, on stream.lan, with chunks produced from a 10-minute synthetic ffmpeg stream (`testsrc` video + 1 kHz `sine` audio at line 1364-1365). The step asserts the deployed binary produced audio with correct PTS cadence — exactly the spec's pre-deploy gate, no separate job needed.

**ffprobe is already installed on the stream-lan runner** in the same directory as ffmpeg, which the existing streaming step at line 1355 references as `C:\Users\newlevel\restreamer\local-client\ffmpeg.exe`. Derive the ffprobe path from that parent directory rather than hard-coding a different path.

- [ ] **Step 1: Insert the new step**

Locate the line in `.github/workflows/ci.yml` at line 1457-1458 (the end of "Wait for final chunk upload"):

```yaml
          Write-Host "=== E2E Stability Test PASSED ==="
          Write-Host "10-minute stream: $($stats.total_chunks) chunks produced and uploaded without freeze"

      - name: GATE clear-s3 endpoint deletes 250+ chunks within 25s
```

Insert this new step between those two `- name:` markers:

```yaml
          Write-Host "=== E2E Stability Test PASSED ==="
          Write-Host "10-minute stream: $($stats.total_chunks) chunks produced and uploaded without freeze"

      - name: GATE audio PTS cadence matches AAC sample clock
        shell: powershell
        timeout-minutes: 3
        run: |
          # Regression for the 2026-04-26 live-event failure: audio over
          # restreamer was chipmunked because flv_chunker stamped AAC frames
          # with wall-clock arrival time instead of xiu's RTMP timestamp.
          # AAC at 48kHz has 1024 samples per frame = 21.333 ms cadence; any
          # PTS deviation > a frame width forces the decoder to resample.
          #
          # The 10-minute streaming step pushed `sine=frequency=1000` audio
          # encoded as AAC. After the fix, audio packet PTS deltas in any
          # produced FLV chunk should be a tight cluster around 21.33 ms.
          # We assert the median delta is in [20, 22] ms and no individual
          # gap exceeds 50 ms.
          $ErrorActionPreference = 'Stop'
          # Sibling of the ffmpeg path used by the streaming step above.
          $ffmpegPath = "C:\Users\newlevel\restreamer\local-client\ffmpeg.exe"
          $ffprobe = Join-Path (Split-Path $ffmpegPath -Parent) "ffprobe.exe"
          if (-not (Test-Path $ffprobe)) {
            throw "FAILED: ffprobe not found at $ffprobe"
          }

          # Fetch a recent chunk record. The local file lives at
          # ChunkRecord.chunk_file_path (absolute path on stream.lan).
          $chunks = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/chunks?limit=5" -TimeoutSec 10
          if (-not $chunks -or $chunks.Count -eq 0) {
            throw "FAILED: GET /api/v1/chunks returned no records — earlier streaming step did not produce chunks?"
          }

          # Pick the first chunk that exists on disk. Older chunks may have
          # been pruned after S3 upload depending on retention policy.
          $chunkPath = $null
          foreach ($c in $chunks) {
            if (Test-Path $c.chunk_file_path) {
              $chunkPath = $c.chunk_file_path
              break
            }
          }
          if (-not $chunkPath) {
            throw "FAILED: none of the recent chunks exist on disk: $($chunks | ForEach-Object { $_.chunk_file_path } | Out-String)"
          }
          Write-Host "Probing chunk: $chunkPath"

          # ffprobe -show_packets emits one packet per line in JSON form.
          # We select audio only and compare consecutive pts_time values.
          $json = & $ffprobe -v error -show_packets -select_streams a -of json $chunkPath 2>&1 | Out-String
          if ($LASTEXITCODE -ne 0) {
            throw "FAILED: ffprobe exited $LASTEXITCODE on ${chunkPath}: $json"
          }

          $parsed = $json | ConvertFrom-Json
          if (-not $parsed.packets -or $parsed.packets.Count -lt 10) {
            throw "FAILED: ffprobe returned $($parsed.packets.Count) audio packets in $chunkPath, need >=10 for cadence stats"
          }

          # Compute consecutive pts_time deltas in milliseconds.
          $ptsTimes = $parsed.packets | ForEach-Object { [double]$_.pts_time }
          $deltasMs = @()
          for ($i = 1; $i -lt $ptsTimes.Count; $i++) {
            $deltasMs += [math]::Round(($ptsTimes[$i] - $ptsTimes[$i - 1]) * 1000.0, 3)
          }

          $sorted = $deltasMs | Sort-Object
          $median = $sorted[[int]([math]::Floor($sorted.Count / 2))]
          $maxGap = ($deltasMs | Measure-Object -Maximum).Maximum
          $minGap = ($deltasMs | Measure-Object -Minimum).Minimum

          Write-Host "Audio packet count:   $($parsed.packets.Count)"
          Write-Host "Audio delta median:   ${median} ms"
          Write-Host "Audio delta min:      ${minGap} ms"
          Write-Host "Audio delta max:      ${maxGap} ms"

          if ($median -lt 20.0 -or $median -gt 22.0) {
            throw "FAILED: median audio PTS delta ${median}ms outside [20, 22]ms — AAC cadence broken (chipmunk regression)"
          }
          if ($maxGap -gt 50.0) {
            throw "FAILED: max audio PTS delta ${maxGap}ms exceeds 50ms — audio drop/burst regression"
          }

          Write-Host "=== Audio PTS cadence GATE PASSED ==="

      - name: GATE clear-s3 endpoint deletes 250+ chunks within 25s
```

The gate is binary: succeed and continue, or throw and fail the job. No `continue-on-error`. Step output stays compact (a few lines per run) so CI logs remain readable.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: gate streaming E2E on audio PTS cadence (#NN)"
```

(Replace `NN` with the issue number captured in Task 1.)

---

## Task 6: Operator Runbook

**Files:**
- Create: `docs/operator-runbook.md`

- [ ] **Step 1: Create the file**

Create `docs/operator-runbook.md` with this content (verbatim, no placeholders):

```markdown
# Operator Runbook

Procedures for running live events with restreamer. Keep this file short — one-liner checks the operator follows from the top before going live, plus a small set of post-event spot checks.

## Before Going Live

- [ ] **Audio smoke-test (mandatory after every restreamer upgrade):**
  Push a 30-second test stream from OBS into restreamer (use the regular live event RTMP URL).
  Listen on the rescue / preview output. Confirm:
  - Audio plays without chipmunk / pitch shift.
  - No glitches, clicks, or dropouts.
  - Audio stays in sync with video.
  If any of these fail — **do not go live.** Escalate to engineering. The 2026-04-26 live event was lost because this check was skipped after a restreamer version upgrade and audio was unusable from the start.

- [ ] **Dashboard liveness:**
  Open the dashboard. Confirm the deployed version banner matches the version you intended to run. Confirm the inpoint shows `disconnected` (no leftover stream) and there are no error banners.

- [ ] **No active leftover events:**
  On the dashboard's Events tab, confirm no event other than the one you are about to use shows `receiving_activated` or `delivering_activated`. Deactivate any leftovers.

## During the Event

- Watch the dashboard delivery cache value. It should sit near the configured `delivery_delay_secs` (default 120s) and not drift toward 0 or balloon past 200s.
- If audio degrades mid-event, switch to the fallback path (non-restreamer) immediately. Don't try to fix restreamer mid-stream — file an issue afterwards.

## After the Event

- Stop the active event from the dashboard (don't just stop OBS — the receive/deliver flags should clear cleanly).
- If anything was off, file a GitHub issue with: timestamp, symptom, dashboard screenshot, and which restreamer version you were running (visible in the dashboard footer).
```

- [ ] **Step 2: Commit**

```bash
git add docs/operator-runbook.md
git commit -m "docs: operator runbook with pre-live audio smoke-test (#NN)"
```

(Replace `NN` with the issue number captured in Task 1.)

---

## Task 7: Push, Monitor CI, PR, Post-Deploy Verification (Orchestrator-Only)

This task is performed by the orchestrator, not a subagent. Tasks 1-6 must all be committed locally first.

- [ ] **Step 1: Final formatting check**

```bash
cargo fmt --all --check
```
Expected: exit code 0, no output. If anything is off, fix and amend (or — preferred per `commit-conventions.md` — make a follow-up commit).

- [ ] **Step 2: Push**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI to terminal state**

```bash
gh run list --branch dev --limit 3
```

Identify the run-id triggered by your push, then poll in the background:

```bash
sleep 300 && gh run view <run-id> --json status,conclusion,jobs
```

Use `Bash(... run_in_background: true)`. When it returns: if all jobs are `success`, proceed. If any failed, `gh run view <run-id> --log-failed`, fix the root cause in a follow-up commit, push, monitor again. Do not blindly rerun.

The critical jobs to watch:
- `rust-ci-gate` — must pass (covers `cargo fmt`, `cargo clippy`, `cargo test`, mutation tests)
- `deploy-stream-lan` — must pass (the new binary lands on stream.lan)
- `e2e-streaming-test` — must pass (this is where the new audio-cadence gate runs)
- `e2e-gate` — must pass

- [ ] **Step 4: Open PR `dev` → `main`**

```bash
gh pr create --base main --head dev \
  --title "fix(inpoint): audio chipmunk — revert wall-clock for AAC frames (#NN)" \
  --body "$(cat <<'EOF'
## Summary

Audio over restreamer was chipmunked / glitchy in the 2026-04-26 live event (first event after PR #140 merged 2026-04-22). Presenter aborted to fallback path.

Root cause: commit `a2800ba` rewrote both `write_video` and `write_audio` in `flv_chunker.rs` to stamp tags with wall-clock arrival time. For video that solves #135 cache drift. For audio it is wrong — AAC has fixed-cadence frames (21.333 ms at 48 kHz), so wall-clock stamping injects RTMP delivery jitter into PTS, forcing the decoder to resample (chipmunk) or drop/duplicate samples (glitch).

This PR reverts the audio side only. Video keeps wall-clock.

## Changes

- `crates/rs-inpoint/src/flv_chunker.rs::write_audio` reverts to using the xiu-forwarded RTMP timestamp.
- New unit test `audio_uses_xiu_timestamp_not_wall_clock` asserting the corrected behavior.
- New CI gate inside `e2e-streaming-test` running ffprobe against a produced FLV chunk and asserting median audio PTS delta in [20, 22] ms with no gap > 50 ms. The gate doubles as post-deploy verification (job runs on stream.lan after deploy).
- New `docs/operator-runbook.md` documenting a pre-live audio smoke-test for the operator.

## Test plan

- [x] Unit test passes on CI.
- [x] `e2e-streaming-test` audio cadence gate passes (median delta ~21.3 ms).
- [ ] Post-deploy on stream.lan: real OBS test stream → listen to delivered audio → confirm clean (no chipmunk).

Closes #NN.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Monitor PR CI to green**

```bash
gh pr view <pr-number>
gh pr checks <pr-number> --watch
```

All checks must be `success`. The audio cadence gate is the new one to watch. If it fails in a way that suggests the assertion is too tight (e.g. median is 22.5 ms instead of ≤22), do NOT widen the threshold without investigation — that's a `no-timeout-band-aids.md` violation. The xiu/OBS/AAC pipeline produces 21.33 ms; deviation indicates a real timing issue.

- [ ] **Step 6: Verify mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/<pr-number> --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `{"mergeable": true, "mergeable_state": "clean"}`. If `unstable`, `behind`, `dirty`, or `blocked` — fix per `autonomous-quality-discipline.md`. Never propose admin-merge.

- [ ] **Step 7: Post-merge — wait for main CI + auto-release**

After the user explicitly says "merge it":
- Merge via `gh pr merge <pr-number> --merge` (no squash, no rebase per `two-branch-workflow.md`).
- Monitor main `Rust CI` run to green (`sleep 300 && gh run view <run-id>` background).
- Auto-tag `restreamer-v0.3.71` is created on main merge — monitor the `Release` workflow run to green.
- Confirm GitHub Release exists with `Restreamer_0.3.71_x64-setup.exe` and `rs-delivery-0.3.71-linux-amd64`.

- [ ] **Step 8: Functional post-deploy verification on stream.lan**

Per `autonomous-verification.md`, liveness is not enough. Do the actual user workflow:

1. Confirm `Restreamer.exe` on stream.lan is `FileVersion 0.3.71` (`mcp__win-stream-snv__Shell` with `(Get-Item ...).VersionInfo`).
2. From the orchestrator, push a 30-second synthetic ffmpeg stream into stream.lan's `1234/live`:
   ```powershell
   # via mcp__win-stream-snv__Shell
   $ffmpeg = "C:\ffmpeg\ffmpeg.exe"
   & $ffmpeg -y -re -f lavfi -i "testsrc=duration=30:size=320x240:rate=30" -f lavfi -i "sine=frequency=1000:duration=30" -c:v libx264 -preset ultrafast -g 30 -c:a aac -f flv "rtmp://127.0.0.1:1234/live/post-deploy-verify"
   ```
3. Wait for chunks, fetch a chunk path via `GET /api/v1/chunks?limit=5`.
4. Run the same ffprobe assertion (median PTS delta ∈ [20, 22] ms, max gap ≤ 50 ms).
5. **Listen-test or spectrum-check the delivered audio** if any production-style endpoint is wired up: confirm pure 1 kHz sine, no pitch deviation, no clicks.
6. Report results in the completion message with concrete numbers, not just "verified".

Only after step 5 produces clean evidence is the work done.

- [ ] **Step 9: Send completion report**

Use the format mandated by `airuleset/completion-report.md`. Include the new ffprobe E2E table row. Include the live PR/Release URLs. Do NOT include any "remaining / future / TODO" section.

---

## Verification

1. CI `Rust CI / e2e-streaming-test / GATE audio PTS cadence matches AAC sample clock` passes on PR run.
2. PR is `mergeable: true` AND `mergeable_state: "clean"`.
3. After merge, `restreamer-v0.3.71` GitHub Release exists.
4. Stream.lan `Restreamer.exe` is `FileVersion 0.3.71`.
5. Post-deploy synthetic stream → ffprobe → median audio delta in `[20, 22]` ms reported with concrete numbers.
6. `docs/operator-runbook.md` exists with the pre-live smoke-test entry.
