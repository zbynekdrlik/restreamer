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
    the marker stays false.
  - **The rig lease is a hard gate at Start.** `rig-lease-wait.ps1` (#349) still waits up to its
    budget early in the job and then proceeds. Start then FAILS on a lease that is still held
    and not stale, instead of streaming over a live camera-box run. An unreachable lease
    endpoint is still fail-open (warning).
  - **AssertNotStreaming**: read-only. It fails when OBS streams, and only warns when OBS is
    unreachable.
- **Teardown**: `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`, exactly. Otherwise a
  readiness failure on "already streaming" would be followed by stopping camera-box's stream.
- **No inline websocket client in any workflow.** `ClientWebSocket`, `ws://` and `requestType`
  are allowed only in `scripts/ci/obs-ws.ps1`. The StartStream request is allowed only in
  obs-stream.ps1 `Start-OurStream`, and StopStream only in `Stop-OurStream`.
- **`tests/ci/test_obs_stream.py`** (ci.yml job `obs-scripts-test`, windows-latest = PowerShell
  5.1, part of the Rust CI Gate) runs every action against a stdlib mock obs-websocket, a mock
  lease and a fake `obs64` process. It asserts the exit codes, the marker sequence, and that
  only allowlisted requests were sent. Locally: `OBS_TEST_PWSH=<pwsh> python3 tests/ci/test_obs_stream.py`
  (a portable pwsh tarball works; no install needed).
- **`/api/v1/obs/start-stream` is banned in CI.** It is fire-and-forget and skips readiness
  (`obs.rs` never awaits the reply).
- **A job never needs an OBS setting changed.** It runs on camera-box's TEST-mode encoder. If a gate
  truly needs another setting, that is a camera-box ticket, never a CI step.

## The guard (test-integrity "Verify CI never mutates stream OBS (#374)")

- `python3 scripts/ci/verify_no_obs_mutation.py` scans every SELF-HOSTED job in every workflow
  (run/with/env/uses/name, plus the job env) and every file under `scripts/`. A hosted job cannot
  reach OBS, so it is skipped; that is why a test-integrity grep pattern never self-matches.
- `--self-test` applies 68 known-bad mutations to a temp copy, and each must go red for its own
  reason. When you add a guard rule, add its mutation there.
- `requestType` is fail-closed. Only `requestType = "<Literal>"` (or the JSON
  `"requestType":"<Literal>"`) passes, plus `requestType = $requestType` inside
  `Invoke-ObsRequest`. Every call to it must pass an allowed literal.
- The structure of `Start-OurStream` is pinned: lease exit, readiness exit, marker true right
  before StartStream, marker false on a refused start. Republish writes the marker false right
  after its Stop.
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
