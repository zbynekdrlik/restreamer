---
paths:
  - ".github/workflows/*.yml"
  - "scripts/ci/**"
  - "tests/ci/**"
  - "crates/rs-api/src/obs.rs"
---

# CI and stream OBS: Start/Stop streaming + reads only (#374)

Stream OBS on stream.lan is camera-box's development target. Owner directive 2026-08-30: restreamer
may only START and STOP OBS streaming. CI never kills, relaunches, schedules, reconfigures,
"restores" or records OBS. Four CI force-kills on 2026-10-06 lost camera-box's unsaved settings.

## The pieces

- **`scripts/ci/obs-ws.ps1`**: the one obs-websocket client (dot-sourced).
  - `Invoke-ObsRequest "<Type>"` is the only request path. Allowed types: StartStream,
    StopStream, GetStreamStatus, GetRecordStatus, GetVersion, GetCurrentProgramScene,
    GetStreamServiceSettings.
  - `Test-ObsReady` is the readiness logic; `Get-RigLeaseHolder` reads the #349 lease once.
- **`scripts/ci/obs-readiness-check.ps1`**: read-only fail-fast, early in each OBS job, before
  a VPS boots. It fails `stream OBS not ready (<what>) -- camera-box owns it, not touching it`
  unless:
  - exactly one obs64 runs, and the websocket answers;
  - the program scene is `Development` (camera-box issue 1380; `PRO` is never accepted);
  - the service is `rtmp_custom` -> `rtmp://<127.0.0.1|localhost|10.77.9.204|stream.lan>:1234/live`,
    any key (`$script:ObsInpointPattern` in obs-ws.ps1);
  - OBS is neither streaming nor recording.
- **`scripts/ci/obs-stream.ps1 -Action Start|Stop|Republish|AssertNotStreaming`**: the ONLY CI
  start/stop. Workflows call it only in the canonical forms (whole `run:` line, or
  `& powershell ... -Action Republish -GapSeconds N` inside a run).
  - **Start**, in one session: lease free + readiness, then `OBS_STREAMING_STARTED_BY_CI=true` to
    GITHUB_ENV, then StartStream with its response checked. A refused start writes `=false`
    (camera-box got there first: not ours). Then it waits for active and logs the effective kbps.
  - **Stop**: StopStream, then waits until OBS reports idle. It fails loudly if OBS keeps
    streaming.
  - **Republish** (the disconnect and A/V-republish gates): Stop, marker=false, N s of dead air,
    then the full Start. If camera-box took OBS during the gap, the restart is refused and
    the marker stays false. It prints the measured dead air: stop, idle, the gap, the lease read
    and readiness make it ~2-8 s longer than `-GapSeconds`.
    A held lease, a recording, or a scene/service change at the restart now FAILS the gate
    mid-run (new with #374; before, CI restarted blindly).
  - **The rig lease is a hard gate at Start.** `rig-lease-wait.ps1` (#349) still waits up to its
    budget early in the job and then proceeds. Start then FAILS on a lease that is still held
    and not stale, instead of streaming over a live camera-box run. An unreachable lease
    endpoint is still fail-open (warning).
  - **AssertNotStreaming**: read-only. It fails when OBS streams, and only warns when OBS is
    unreachable.
- **Teardown**: `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`, exactly. Otherwise a
  readiness failure on "already streaming" would be followed by stopping camera-box's stream.
- **Stream identity.** The marker only says "this job started a stream once"; OBS outputs carry
  no session id.
  - Start records when OUR session began in `$RUNNER_TEMP/obs-streaming-started-at`
    (job-scoped, rewritten by every restart).
  - `outputDuration` is frames since the last (re)connect, and OBS zeroes it on EVERY
    automatic reconnect (libobs `obs_output_begin_data_capture`). So any step that kills,
    restarts or suspends Restreamer.exe while we stream must be followed by
    `obs-stream.ps1 -Action Rebaseline` (`if: always() && env.OBS_STREAMING_STARTED_BY_CI ==
    'true'`). That step is read-only. It waits out the reconnect, then re-records the start,
    but only for a session that began within `OBS_REBASELINE_WINDOW_S` (180 s) of the
    Restreamer.exe start time. A later session could be camera-box's, started after OBS
    gave up on ours: it is not adopted (exit 1), and the teardown then refuses it. If the
    output is gone, it writes marker=false. Residual: if ours is still reconnecting after
    90 s, it anchors at now. The guard enforces this after every
    restart chain. The YT job has one, after the crash gates. While OBS reconnects, its
    output stays active, so no second session can start meanwhile.
  - Before StopStream, Stop-OurStream refuses an active session whose `outputDuration` is under
    half the time since that record (after the first 60 s).
  - While waiting for idle, it refuses a session whose duration dropped below the
    pre-stop value: camera-box took OBS in the gap.
  - In both cases it writes the marker false and exits 1, never stopping the newer session.
- **No inline websocket client in any workflow.** `ClientWebSocket`, `ws://` and `requestType`
  are allowed only in `scripts/ci/obs-ws.ps1`. The StartStream request is allowed only in
  obs-stream.ps1 `Start-OurStream`, and StopStream only in `Stop-OurStream`.
- **`tests/ci/test_obs_stream.py`** (ci.yml job `obs-scripts-test`, windows-latest = PowerShell
  5.1, part of the Rust CI Gate) runs every action against a stdlib mock obs-websocket, a mock
  lease, a mock program-audio sampler (#379) and a fake `obs64` process, in 46 scenarios. It asserts:
  - the exit codes and the marker sequence;
  - that only allowlisted requests were sent (the list is read from the guard);
  - that StopStream is sent only by stop/republish;
  - that a stream or recording CI did not start is left as it was.
  The mock also checks the real obs-websocket auth hash, interleaves events, and delays the
  stop. Camera-box taking OBS during a republish gap is a scenario too. Locally: `OBS_TEST_PWSH=<pwsh> python3 tests/ci/test_obs_stream.py`
  (a portable pwsh tarball works; no install needed).
- **`/api/v1/obs/start-stream` is banned in CI.** It is fire-and-forget and skips readiness
  (`obs.rs` never awaits the reply).
- **A job never needs an OBS setting changed.** It runs on camera-box's TEST-mode encoder. If a gate
  truly needs another setting, that is a camera-box ticket, never a CI step.

## The guard (test-integrity "Verify CI never mutates stream OBS (#374)")

- `python3 scripts/ci/verify_no_obs_mutation.py` scans every SELF-HOSTED job in every workflow
  (run/with/env/uses/name, plus the job env) and every file under `scripts/`. A hosted job cannot
  reach OBS, so it is skipped; that is why a test-integrity grep pattern never self-matches.
- `--self-test` applies 91 known-bad mutations to a temp copy, and each must go red for its own
  reason. When you add a guard rule, add its mutation there.
- `requestType` is fail-closed. Only `requestType = "<Literal>"` (or the JSON
  `"requestType":"<Literal>"`) passes, plus `requestType = $requestType` inside
  `Invoke-ObsRequest`. Every call to it must pass an allowed literal.
- The structure of `Start-OurStream` is pinned: lease exit, readiness exit, marker true right
  before StartStream, marker false on a refused start.
- The `switch ($Action)` dispatcher is pinned verbatim (`DISPATCH`), and so is the number of
  `Start-OurStream`/`Stop-OurStream`/`Set-StartedMarker` occurrences: only the Start/Stop/Republish
  arms may call them. Changing the dispatcher means updating `DISPATCH` in the guard, on purpose.
- No other script may name obs-stream.ps1, Start-/Stop-OurStream, the marker or the started-at
  record. scripts/ci bans aliases, Get-Command, Invoke-Expression and `& $var` (so a scripts/ci
  helper cannot call a native tool through a variable; spell the exe out). Workflows may use an obs-stream
  `-Action` word or a computed `-File $x` only in the canonical forms.
- The guard also requires the `obs-scripts-test` job (windows, runs the mock test) and its
  Rust CI Gate wiring.
- Run it with `PYTHONDONTWRITEBYTECODE=1` locally. A stray `scripts/ci/__pycache__` is skipped,
  but do not commit one.

## Gotchas hit while building it

- **An unquoted step name containing ` #` is truncated by YAML** (`- name: Foo (read-only, #374)` parses as
  `Foo (read-only,`). Quote every step name that carries an issue ref.
- **PowerShell `"$var: text"` is a parse error** (scope-qualified variable). Write `"${var}: text"`.
- **A variable named `$...ObsWs` reads like the `obsws` client to the guard**: name the socket
  `$script:ObsSocket`.
- **Execute new OBS script code against the mock, never on the box.** Run
  `tests/ci/test_obs_stream.py` locally with a portable pwsh (dev1/dev2 have none installed),
  and in CI under PowerShell 5.1 on windows-latest. A parse check on the box is optional.
  A worktree lane's hooks refuse a bare `pwsh` command; run it through the Python harness
  (`OBS_TEST_PWSH=~/.local/pwsh74/pwsh python3 tests/ci/...`).

## Program-audio guard (#379): no room/FOH music on YouTube/FB

The program's only audio input is the FOH Dante feed (owner copyright rule). camera-box
classifies it at `http://dev1:8890/program-audio.json`; restreamer reads ONLY that verdict.

- **`scripts/ci/program-audio-guard.ps1`** (dot-sourced):
  - `Test-ProgramAudio` returns `$null` only for MEASUREMENT/SILENT with `age_s <= 10` and no
    FOREIGN within the last 15 s (`last_foreign_age_s`). Otherwise it returns a reason:
    FOREIGN, UNKNOWN, stale, unreachable or malformed. It fails closed.
  - Start-OurStream runs `Test-ProgramAudio -BeforeStart` right before StartStream, after
    readiness. That covers Start and every Republish. `-BeforeStart` also refuses after an
    earlier breach or with a dead watchdog in this job.
  - `Set-StartedMarker` mirrors the marker into `program-audio-stream-owned`. The watchdog
    stops only OUR stream (#374). Music while the marker is false is logged and is not a
    breach: nothing of ours is live, and the next start re-checks.
  - `Start-ProgramAudioWatchdog` starts the watchdog. `Assert-NoProgramAudioBreach` and
    `Stop-ProgramAudioWatchdog` check and end it. State is kept in `$RUNNER_TEMP`
    (`program-audio-*`).
- **The watchdog is a DETACHED `Start-Process` powershell, not `Start-Job`.** A Start-Job
  dies with its step's process.
  - On Desktop it uses ShellExecute and `-WindowStyle Hidden`, so the child inherits no
    handle; an inherited stdout pipe would hold the step open.
  - On Core it redirects to files.
  - The runner's job-end cleanup reaps it.
  - On a breach it writes the marker FIRST, then POSTs `/api/v1/obs/stop-stream`. That POST
    only QUEUES a command (`obs.rs`: 200 = queued). So the watchdog re-POSTs until
    `GET /api/v1/obs/status` reports `connected:true, streaming:false`, then writes
    `stop CONFIRMED`. It never gives up while our stream is live: a crash gate may have
    Restreamer down. It keeps its heartbeat, and the teardown ends it.
  - An unreachable, stale or malformed read is re-read once after 2 s. A FOREIGN or UNKNOWN
    verdict trips at once.
  - A dead watchdog fails the assert, and so does a hung one: a heartbeat older than
    3 x poll + 20 s.
- **The stop-stream API carve-out.** The #374 guard allows it ONCE, in
  `Invoke-ProgramAudioStop` only (`CONFINED_API`). start-stream stays banned. A new guard
  script under `scripts/` that names the hunted patterns goes into `GUARD_FILES`; the scan
  otherwise flags its own source.
- **Wiring is pinned by `scripts/ci/verify_program_audio_guard.py`** (test-integrity, with
  `--self-test`). In every job that runs `-Action Start`:
  - the watchdog step comes right after the start, under
    `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`;
  - a "Program-audio breach check" step follows every long step before the next long step and
    before the OBS stop. A long step has a timeout >= 10 min or NO `timeout-minutes` (it can
    run to the job limit); `if: always()` cleanup steps are exempt. When you add such a step
    to a streaming job, add a check after it;
  - an `if: always()` teardown after the OBS stop runs Stop, THEN Assert.

  No workflow may set a `PROGRAM_AUDIO_*` knob; they exist for the mock test only.
- **Gotchas.**
  - `ConvertFrom-Json` unwraps a one-element JSON array into its object. Check that the body
    starts with `{` first.
  - The `StartTime` that `Start-Process -PassThru` reports and the one `Get-Process` reports
    differ slightly. Compare pid identity with a 2 s tolerance on the ticks.
  - `"" -as [int]` is 0 and `"" -as [double]` is 0, not `$null`. A missing pid file then
    reads as pid 0 (the Idle process exists), and a missing heartbeat reads as one from 1970.
    Check for an empty string first.
  - Rewrite the small state files in place (`WriteAllText`), with retries on both the read
    and the write side. A rename-over fails on Windows while a reader holds the file open.
  - GitHub runs `shell: powershell` as `-command ". '<file>'"`, so an `exit 3` reaches the
    runner as exit 1. Tests that mimic a step can assert only zero vs non-zero.
