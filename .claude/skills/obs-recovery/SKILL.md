---
name: obs-recovery
description: >
  What to do when stream OBS on stream.lan (streamsnv) is degraded, or a CI
  E2E job fails on OBS state, and when the self-hosted CI runner appears
  offline. Stream OBS is camera-box's: restreamer never restarts or
  reconfigures it (#374); it reports the problem to camera-box instead.
triggers:
  - OBS degraded
  - OBS not starting
  - OBS recovery
  - stream OBS not ready
  - CI runner offline
  - runner stalled
  - obs64
  - WebSocket not listening
  - streamsnv runner
---

# Stream OBS problems and CI runner recovery

## Stream OBS is camera-box's: do not recover it (#374)

Owner directive 2026-08-30, verbatim: "na stream nb robi sa vyvoj aj obska tak ty nic nesahaj do
obs a tak si rob vyvoj iba si zapinaj vypinaj streamovanie ale nic viac".

OBS on stream.lan is the camera-box project's development target. Restreamer (this session, CI,
scripts) may only START and STOP streaming and READ status. Restreamer never:

- kills, closes (`WM_CLOSE`/`CloseMainWindow`, `ExitOBS`), suspends or relaunches obs64;
- runs or (re)registers an OBS scheduled task (`StartOBS`, `Start OBS Studio`, `OBSStudio`);
- deletes `.sentinel`/`safe_mode`, clicks the crash dialog, or edits anything under
  `%APPDATA%\obs-studio` (service.json, streamEncoder.json, global.ini, scene collections);
- changes or "restores" the scene, profile, bitrate or stream service;
- starts or stops a recording (StopRecord included);
- reboots the box to "fix" OBS.

Why: before #374, CI force-killed and relaunched stream OBS and rewrote its settings. Four
force-kills on 2026-10-06 lost camera-box's unsaved runtime settings (their `mbc` sync went from
37 to 29 ms). The test-integrity guard `scripts/ci/verify_no_obs_mutation.py` now fails the build
when ci.yml or scripts/ contain any of the above.

## When a CI job fails with "stream OBS not ready"

`scripts/ci/obs-readiness-check.ps1` (early, read-only) and `scripts/ci/obs-stream.ps1 -Action Start`
(the only CI start, same checks in the start session) fail with
`stream OBS not ready (<what>) -- camera-box owns it, not touching it` or
`stream OBS is recording -- not touching it` when camera-box's TEST mode is not in place:

| `<what>` | Meaning |
|---|---|
| `obs64 is not running` / `N obs64 processes` | OBS down or duplicated |
| `websocket ... unreachable` | obs-websocket on 4455 not answering (often a crash dialog) |
| `program scene is '<x>', not the TEST scene 'Development'` | rig not in TEST mode (`PRO` = production) |
| `stream service is ... not the restreamer inpoint` | OBS points elsewhere (YouTube, a test URL) |
| `OBS is already streaming` | someone else's stream; never stopped by us |
| `stream OBS is recording` | a recording camera-box or the owner started |
| `camera-box holds the rig lease: <job> (<run>)` | `obs-stream.ps1 -Action Start` saw a live lease at start time |
| `StartStream refused: code ...` | OBS refused the start (usually camera-box started streaming first) |
| `the active stream is newer than ours` / `a newer stream replaced ours` | the teardown refused: the active session is not the one CI recorded. The line prints its duration and our record. Usually camera-box's stream, so leave it. If it IS ours (a Restreamer restart with no `-Action Rebaseline` after it; see `.claude/rules/stream-obs-ci.md`), fix the missing Rebaseline step |

What to do:

1. Read the failure line; do not touch OBS.
2. Tell camera-box: a comment on the restreamer ticket that hit it plus a camera-box ticket
   (cross-repo: `gh issue create -R zbynekdrlik/camera-box` from the supervisor), naming the
   exact `<what>` and the run URL.
3. Re-run the failed job (`gh run rerun <id> --failed`) once camera-box reports the rig is back in
   TEST mode, or work on something else meanwhile.

The rig lease (`scripts/ci/rig-lease-wait.ps1`, #349) still waits out a camera-box hold early in
the job and then proceeds; `obs-stream.ps1 -Action Start` then FAILS (never streams over it) if
the lease is still held and not stale. The mid-run gates restart our stream with
`-Action Republish`, which re-runs the same checks.

## Runner Offline Detection

When a CI deploy/E2E job stays "queued" with `startedAt` set, the self-hosted runner is likely
offline. Alert the user within the first poll (within 5 minutes of detecting the queue condition);
do NOT wait hours. Restarting the runner service or rebooting the box is a host-level action
and needs the owner's explicit approval at the command (`no-destructive-remote-actions.md`); it
is never a way to recover OBS.
