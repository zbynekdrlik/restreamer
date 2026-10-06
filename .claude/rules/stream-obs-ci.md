---
paths:
  - ".github/workflows/*.yml"
  - "scripts/ci/**"
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
  - the service is `rtmp_custom` -> `rtmp://127.0.0.1:1234/live`;
  - OBS is neither streaming nor recording.
- **`scripts/ci/obs-stream.ps1 -Action Start|Stop|AssertNotStreaming`**: the ONLY CI start/stop.
  - **Start**, in one session: lease free + readiness, then `OBS_STREAMING_STARTED_BY_CI=true` to
    GITHUB_ENV, then StartStream with its response checked. A refused start writes `=false`
    (camera-box got there first: not ours). Then it waits for active and logs the effective kbps.
  - **Stop**: StopStream, then waits until OBS reports idle. It fails loudly if OBS keeps
    streaming.
  - **AssertNotStreaming**: read-only. It fails when OBS streams, and only warns when OBS is
    unreachable.
- **Teardown**: `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`, exactly. Otherwise a
  readiness failure on "already streaming" would be followed by stopping camera-box's stream.
- **Mid-run restarts** (disconnect test, A/V republish gate) may StopStream/StartStream inline,
  but only AFTER the job's own start and with `if: success()`.
- **`/api/v1/obs/start-stream` is banned in CI.** It is fire-and-forget and skips readiness
  (`obs.rs` never awaits the reply).
- **A job never needs an OBS setting changed.** It runs on camera-box's TEST-mode encoder. If a gate
  truly needs another setting, that is a camera-box ticket, never a CI step.

## The guard (test-integrity "Verify CI never mutates stream OBS (#374)")

- `python3 scripts/ci/verify_no_obs_mutation.py` scans every SELF-HOSTED job in every workflow
  (run/with/env/uses/name, plus the job env) and every file under `scripts/`. A hosted job cannot
  reach OBS, so it is skipped; that is why a test-integrity grep pattern never self-matches.
- `--self-test` applies 49 known-bad mutations to a temp copy, and each must go red for its own
  reason. When you add a guard rule, add its mutation there.
- `requestType` is fail-closed. Only `requestType = "<Literal>"` (or the JSON
  `"requestType":"<Literal>"`) passes, plus `requestType = $requestType` inside an allowlisted
  pass-through function (`PASSTHROUGH_FUNCS`). Every call to one of those must pass an allowed
  literal.
- Run it with `PYTHONDONTWRITEBYTECODE=1` locally. A stray `scripts/ci/__pycache__` is skipped,
  but do not commit one.

## Gotchas hit while building it

- **An unquoted step name containing ` #` is truncated by YAML** (`- name: Foo (read-only, #374)` parses as
  `Foo (read-only,`). Quote every step name that carries an issue ref.
- **PowerShell `"$var: text"` is a parse error** (scope-qualified variable). Write `"${var}: text"`.
- **A variable named `$...ObsWs` reads like the `obsws` client to the guard**: name the socket
  `$script:ObsSocket`.
- **Parse-check edited PowerShell on the box, read-only.** Use `share --private` +
  `[Parser]::ParseInput` (`.claude/skills/ci-yaml-maintenance`). Never execute it there. Neither
  dev1 nor dev2 has pwsh, so CI's E2E run is the first real execution.
