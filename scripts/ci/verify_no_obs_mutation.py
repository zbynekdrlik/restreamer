#!/usr/bin/env python3
"""#374 guard: restreamer CI never mutates stream OBS.

Stream OBS on the stream box (10.77.9.204) is camera-box's development target.
Owner directive 2026-08-30: restreamer may only START and STOP OBS streaming --
no kill, no relaunch, no scheduled task, no scene / profile / bitrate /
stream-service change, no "restore OBS ...", no recording control (StopRecord
included, #374 scope addition). Before #374, CI force-killed and relaunched OBS
and rewrote its settings (four force-kills on 2026-10-06 lost camera-box's
unsaved runtime settings).

The shape this guard enforces:
  * workflows never talk to OBS inline. They run only the canonical
    invocations of scripts/ci/obs-readiness-check.ps1 and scripts/ci/obs-stream.ps1
    (Start / Stop / Republish / AssertNotStreaming);
  * scripts/ci/obs-ws.ps1 is the only obs-websocket client, and its one request
    function, Invoke-ObsRequest, is called with allowlisted literals only;
  * the StartStream request lives only in obs-stream.ps1 Start-OurStream (lease +
    readiness + marker=true + checked start, marker=false when refused), and
    StopStream only in Stop-OurStream;
  * per self-hosted job: Start comes first and is unconditional; Republish and Stop
    come after it; Stop runs only under `if: always() && env.OBS_STREAMING_STARTED_BY_CI
    == 'true'`; the marker is written nowhere else.
And everywhere it scans: no OBS process action (kill, suspend, launch, close,
scheduled task, the port-4455 owner, a wildcard name), no obs-studio / streamEncoder
touch, no request batch, no third-party OBS client.

Scope: every job that runs on a self-hosted runner in every `.github/workflows/*.yml`
(step run/with/env/uses/name, job env) and every file under `scripts/`. A
GitHub-hosted job cannot reach OBS, so it is not scanned; that is also why this
file's own ci.yml step and the test-integrity grep patterns never self-match (#325).
This file is skipped by path and may only run in a GitHub-hosted job (checked).
The scripts' runtime behaviour is tested separately against a mock obs-websocket:
tests/ci/test_obs_stream.py (ci.yml job obs-scripts-test).

`--self-test` copies the real tree, applies each known-bad mutation, and requires
every copy to go red for its own reason, and the unmodified copy to pass.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

WORKFLOWS = Path(".github/workflows")
CI = WORKFLOWS / "ci.yml"
SCRIPTS = Path("scripts")
SELF = Path("scripts/ci/verify_no_obs_mutation.py")
OBS_STREAM = Path("scripts/ci/obs-stream.ps1")
OBS_LIB = Path("scripts/ci/obs-ws.ps1")
MARKER = "OBS_STREAMING_STARTED_BY_CI"
PS = "powershell -NoProfile -ExecutionPolicy Bypass -File scripts/ci/"
START_RUN = PS + "obs-stream.ps1 -Action Start"
STOP_RUN = PS + "obs-stream.ps1 -Action Stop"
ASSERT_RUN = PS + "obs-stream.ps1 -Action AssertNotStreaming"
READINESS_RUN = PS + "obs-readiness-check.ps1"
REPUBLISH_LINE = re.compile(r"&\s*" + re.escape(PS + "obs-stream.ps1 -Action Republish -GapSeconds") + r" \d+")
WHOLE_RUNS = {START_RUN, STOP_RUN, ASSERT_RUN, READINESS_RUN}
TEARDOWN_IF = f"always() && env.{MARKER} == 'true'"
SUCCESS_ONLY = {"", "success()"}

READ_ONLY_AND_STREAMING = {
    "StartStream",
    "StopStream",
    "GetStreamStatus",
    "GetRecordStatus",
    "GetVersion",
    "GetCurrentProgramScene",
    "GetStreamServiceSettings",
}
PASSTHROUGH_FUNCS = {"Invoke-ObsRequest"}
# Request literal -> the one obs-stream.ps1 function allowed to send it.
CONFINED_REQUESTS = {"StartStream": "Start-OurStream", "StopStream": "Stop-OurStream"}

# "obs" as a word or a prefix (obs64, obs-studio, "obs-*", OBSStudio, Start OBS),
# plus the StartOBS task name -- but not the "obs" inside "jobs"/"blobs".
OBS_WORD = r"(?:(?<![a-z])obs|startobs)"
PROCESS_ACTION = (
    r"(?:stop-process|taskkill|\.kill\s*\(|suspend|debug-process|"
    r"invoke-cimmethod|terminate|start-process|wait-process)"
)
TASK_ACTION = r"(?:schtasks|scheduledtask)"

LINE_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"obs64\.exe", re.I), "launches or kills obs64.exe"),
    (re.compile(r"closemainwindow|exitobs", re.I), "closes OBS (CloseMainWindow / ExitOBS)"),
    (re.compile(r"obs-studio", re.I), "touches OBS's install/config tree (obs-studio)"),
    (re.compile(r"streamencoder", re.I), "touches OBS's encoder settings (streamEncoder)"),
    (
        re.compile(rf"{PROCESS_ACTION}.*{OBS_WORD}|{OBS_WORD}.*{PROCESS_ACTION}", re.I),
        "a process action (kill/suspend/launch) on OBS",
    ),
    (
        re.compile(rf"{TASK_ACTION}.*{OBS_WORD}|{OBS_WORD}.*{TASK_ACTION}", re.I),
        "an OBS scheduled task (register/run)",
    ),
    (
        re.compile(r"(?:stop-process|get-process)\b[^\n]*-name\s+[\"']?[^\s\"'|]*\*", re.I),
        "a wildcard process name next to a process action",
    ),
    (
        re.compile(r"requestbatch|\bop\b\W{0,3}\s*[=:]\s*8\b", re.I),
        "an obs-websocket request batch (op 8) bypasses the request allowlist",
    ),
    (
        re.compile(r"(?<![\w-])(?:obs-cmd|obs-cli|obsws|simpleobsws|obs_websocket)", re.I),
        "a third-party OBS client",
    ),
    (
        re.compile(r"/api/v1/obs/(?:start|stop)-stream", re.I),
        "Restreamer's /api/v1/obs/start|stop-stream skips the readiness check and the marker; "
        "use scripts/ci/obs-stream.ps1",
    ),
]

# A unit (one step, one script) that looks up obs64 or the obs-websocket port may
# not also kill, launch or close a process: `$z = Get-Process obs64` +
# `Stop-Process -Id $z.Id` has no OBS word on the kill line.
UNIT_OBS_LOOKUP = re.compile(r"obs64|\b4455\b", re.I)
UNIT_PROCESS_ACTION = re.compile(
    r"stop-process|taskkill|\.kill\s*\(|start-process|closemainwindow|schtasks|scheduledtask|suspend",
    re.I,
)
# The websocket client internals: only in obs-ws.ps1, never in a workflow.
CLIENT_INTERNALS = re.compile(r"clientwebsocket|ws://|requesttype|send-obsjson|receive-obsjson|obssocket", re.I)

RT_LITERAL = re.compile(r"""\brequestType\s*=\s*"(?P<name>\w+)"(?=\s*(?:;|\}|$))""")
RT_JSON = re.compile(r"""\\?"requestType\\?"\s*:\s*\\?"(?P<name>\w+)\\?"(?=\s*(?:,|\}|$))""")
RT_PASSTHROUGH = re.compile(r"""\brequestType\s*=\s*\$requestType(?=\s*(?:;|\}|$))""")
RT_VAR_READ = re.compile(r"""\$\{?requestType\}?(?!\s*=)""")
FUNC_DEF = re.compile(r"^[ \t]*function\s+(?P<name>[\w-]+)", re.I | re.M)

BAD_STEP_NAME = [
    re.compile(r"\brestore\b.*\bobs\b|\bobs\b.*\brestore\b", re.I),
    re.compile(r"\b(set|switch|change)\b.*\bobs\b.*\b(scene|bitrate|encoder|service|settings|profile)\b", re.I),
    re.compile(r"\b(kill|relaunch|restart|launch)\b.*\bobs\b|\bensure obs is running\b", re.I),
    re.compile(r"\bstoprecord\b", re.I),
]
HOSTED_LABEL = re.compile(r"^(ubuntu|windows|macos)-[\w.]+$")
SKIP_DIRS = {"__pycache__"}


# ------------------------------------------------------------------ helpers --


def strip_comments(text: str) -> str:
    """Drop PowerShell block comments that open a line and full-line `#` comments."""
    text = re.sub(r"(?m)^\s*<#.*?#>", "", text, flags=re.S)
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def norm_if(cond: object) -> str:
    c = str(cond if cond is not None else "")
    c = c.replace("${{", " ").replace("}}", " ")
    return re.sub(r"\s+", " ", c).strip()


def runs_on_hosted(job: dict) -> bool:
    runs_on = job.get("runs-on", "")
    labels = runs_on if isinstance(runs_on, list) else [runs_on]
    return bool(labels) and all(isinstance(l, str) and HOSTED_LABEL.match(l.strip()) for l in labels)


def function_spans(code: str) -> list[tuple[str, int, int]]:
    """(name, start, end) of each PowerShell function body, by brace matching."""
    masked = re.sub(r'"[^"\n]*"|\'[^\'\n]*\'', lambda m: " " * len(m.group(0)), code)
    spans = []
    for m in FUNC_DEF.finditer(code):
        i = masked.find("{", m.end())
        if i < 0:
            continue
        depth = 0
        for j in range(i, len(masked)):
            if masked[j] == "{":
                depth += 1
            elif masked[j] == "}":
                depth -= 1
                if depth == 0:
                    spans.append((m.group("name"), m.start(), j + 1))
                    break
    return spans


def owner_at(spans: list[tuple[str, int, int]], pos: int) -> str | None:
    inside = [(e - s, n) for n, s, e in spans if s <= pos < e]
    return min(inside)[1] if inside else None


def body_of(code: str, name: str) -> str | None:
    for n, s, e in function_spans(code):
        if n == name:
            return code[s:e]
    return None


# --------------------------------------------------------------- unit rules --


def request_errors(label: str, lines: list[str]) -> list[str]:
    errors: list[str] = []
    for line in lines:
        if not re.search(r"requesttype", line, re.I):
            continue
        rest = line
        for rx in (RT_LITERAL, RT_JSON):
            for m in rx.finditer(line):
                if m.group("name") not in READ_ONLY_AND_STREAMING:
                    errors.append(f"{label}: obs-websocket request '{m.group('name')}' is not allowed: {line.strip()}")
            rest = rx.sub("", rest)
        rest = RT_PASSTHROUGH.sub("", rest)
        rest = RT_VAR_READ.sub("", rest)
        if re.search(r"requesttype", rest, re.I):
            errors.append(f"{label}: unrecognized requestType shape (only a literal assignment is allowed): {line.strip()}")
    return errors


def passthrough_errors(label: str, code: str) -> list[str]:
    errors: list[str] = []
    spans = function_spans(code)
    for m in RT_PASSTHROUGH.finditer(code):
        owner = owner_at(spans, m.start())
        if owner not in PASSTHROUGH_FUNCS:
            errors.append(
                f"{label}: requestType forwarded by '{owner or 'top level'}', not an allowlisted "
                f"pass-through ({', '.join(sorted(PASSTHROUGH_FUNCS))})"
            )
    return errors


def call_errors(label: str, lines: list[str]) -> list[str]:
    errors: list[str] = []
    for fn in sorted(PASSTHROUGH_FUNCS):
        call = re.compile(rf"(?<![\w-]){re.escape(fn)}(?![\w-])", re.I)
        for line in lines:
            m = call.search(line)
            if not m or re.search(r"\bfunction\s", line, re.I):
                continue
            lit = re.search(r"""["']([A-Z][A-Za-z]+)["']""", line[m.end():])
            if not lit:
                errors.append(f"{label}: {fn} called without a literal request type: {line.strip()}")
            elif lit.group(1) not in READ_ONLY_AND_STREAMING:
                errors.append(f"{label}: {fn} sends '{lit.group(1)}', not allowed: {line.strip()}")
    return errors


def check_unit(label: str, text: str) -> list[str]:
    errors: list[str] = []
    code = strip_comments(text)
    lines = code.splitlines()
    for line in lines:
        for rx, why in LINE_RULES:
            if rx.search(line):
                errors.append(f"{label}: {why}: {line.strip()}")
    if UNIT_OBS_LOOKUP.search(code):
        for line in lines:
            if UNIT_PROCESS_ACTION.search(line):
                errors.append(f"{label}: looks up obs64 / port 4455 AND runs a process action: {line.strip()}")
    errors += request_errors(label, lines)
    errors += passthrough_errors(label, code)
    errors += call_errors(label, lines)
    return errors


# ----------------------------------------------------------- workflow rules --


def step_text(step: dict) -> str:
    parts = [str(step.get("run") or "")]
    for key in ("with", "env"):
        if step.get(key):
            parts.append(json.dumps(step[key], indent=1))
    return "\n".join(parts)


def invocation_errors(label: str, code: str) -> list[str]:
    errors = []
    for line in code.splitlines():
        if not re.search(r"obs-stream\.ps1|obs-readiness-check\.ps1", line, re.I):
            continue
        s = line.strip()
        if s in WHOLE_RUNS:
            if code.strip() != s:
                errors.append(f"{label}: `{s}` must be the whole run: of its step")
        elif not REPUBLISH_LINE.fullmatch(s):
            errors.append(f"{label}: not a canonical obs-stream.ps1 / obs-readiness-check.ps1 invocation: {s}")
    return errors


def check_job(wf_name: str, job_name: str, job: dict) -> list[str]:
    errors: list[str] = []
    where = f"{wf_name} {job_name}"
    if job.get("env"):
        env_text = json.dumps(job["env"], indent=1)
        errors += check_unit(f"{where} (job env)", env_text)
        if MARKER in env_text:
            errors.append(f"{where}: {MARKER} may not be set in an env: map")
    started = False
    for i, step in enumerate(job.get("steps") or []):
        name = str(step.get("name", ""))
        uses = str(step.get("uses") or "")
        label = f"{where} / {name or uses or f'step {i}'}"
        text = step_text(step)
        code = strip_comments(str(step.get("run") or ""))
        cond = norm_if(step.get("if"))
        errors += check_unit(label, text)
        errors += invocation_errors(label, code)
        for line in strip_comments(text).splitlines():
            if CLIENT_INTERNALS.search(line):
                errors.append(f"{label}: inline obs-websocket client in a workflow; use scripts/ci/obs-stream.ps1: {line.strip()}")
        if MARKER in text:
            errors.append(f"{label}: {MARKER} may only be written by {OBS_STREAM}")
        if re.search(OBS_WORD, uses, re.I):
            errors.append(f"{label}: uses an OBS action: {uses}")
        for rx in BAD_STEP_NAME:
            if rx.search(name):
                errors.append(f"{label}: step name describes an OBS mutation")
        run = code.strip()
        republish = any(REPUBLISH_LINE.fullmatch(l.strip()) for l in code.splitlines())
        if run == START_RUN:
            if started:
                errors.append(f"{label}: a second OBS Start in one job")
            started = True
            if cond not in SUCCESS_ONLY or step.get("continue-on-error"):
                errors.append(f"{label}: the OBS start step must be unconditional (no if:/continue-on-error)")
        if run == STOP_RUN or republish:
            if not started:
                errors.append(f"{label}: stops OBS streaming before this job started it (not ours to stop)")
        if run == STOP_RUN and cond != TEARDOWN_IF:
            errors.append(f"{label}: the OBS stop teardown needs exactly `if: {TEARDOWN_IF}` (got `{cond}`)")
        if republish and (cond not in SUCCESS_ONLY or step.get("continue-on-error")):
            errors.append(f"{label}: an OBS republish must run only on success (no always()/continue-on-error)")
    return errors


def check_workflows(root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted((root / WORKFLOWS).glob("*.y*ml")):
        wf = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (wf.get("jobs") or {}).items():
            hosted = runs_on_hosted(job)
            for step in job.get("steps") or []:
                if SELF.name in str(step.get("run") or "") and not hosted:
                    errors.append(f"{path.name} {job_name}: {SELF} may only run in a GitHub-hosted job")
            if not hosted:
                errors += check_job(path.name, job_name, job)
    return errors


# ------------------------------------------------------------- script rules --


def script_errors(rel: Path, text: str) -> list[str]:
    errors = check_unit(str(rel), text)
    code = strip_comments(text)
    if rel != OBS_LIB:
        for line in code.splitlines():
            if CLIENT_INTERNALS.search(line):
                errors.append(f"{rel}: obs-websocket client internals outside {OBS_LIB}: {line.strip()}")
    if rel != OBS_STREAM and (MARKER in code or "Set-StartedMarker" in code):
        errors.append(f"{rel}: {MARKER} may only be written by {OBS_STREAM}")
    spans = function_spans(code)
    for req, fn in CONFINED_REQUESTS.items():
        for m in re.finditer(rf"""["']{req}["']""", code):
            owner = owner_at(spans, m.start())
            if rel != OBS_STREAM or owner != fn:
                errors.append(f"{rel}: the {req} request is allowed only in {OBS_STREAM} {fn} (found in {owner or 'top level'})")
    return errors


START_SHAPE = [
    (r'if \(\$holder\) \{ Write-NotReady "camera-box holds the rig lease: \$holder"; exit 1 \}',
     "Start-OurStream must fail on a held rig lease"),
    (r"\$why = Test-ObsReady\s*\n\s*if \(\$why\) \{ Write-NotReady \$why; exit 1 \}",
     "Start-OurStream must run Test-ObsReady and exit on its verdict"),
    (r'Set-StartedMarker "true"\s*\n\s*\$resp = Invoke-ObsRequest "StartStream"\s*\n'
     r'\s*if \(-not \$resp\.requestStatus\.result\) \{\s*\n\s*Set-StartedMarker "false"',
     "Start-OurStream must write the marker true right before StartStream and false when it is refused"),
]


def check_obs_stream_shape(root: Path) -> list[str]:
    path = root / OBS_STREAM
    if not path.is_file():
        return [f"{OBS_STREAM} is missing"]
    code = strip_comments(path.read_text(encoding="utf-8"))
    errs = []
    start = body_of(code, "Start-OurStream")
    if start is None:
        return [f"{OBS_STREAM}: function Start-OurStream is missing"]
    ready_at = re.search(r"\$why = Test-ObsReady", start)
    start_at = start.find('Invoke-ObsRequest "StartStream"')
    if not ready_at or start_at < 0 or ready_at.start() > start_at:
        errs.append(f"{OBS_STREAM}: StartStream must come after the Test-ObsReady readiness check")
    for rx, why in START_SHAPE:
        if not re.search(rx, start):
            errs.append(f"{OBS_STREAM}: {why}")
    if not re.search(r'"Republish"\s*\{\s*\n\s*Stop-OurStream\s*\n\s*Set-StartedMarker "false"', code):
        errs.append(f"{OBS_STREAM}: Republish must write the marker false right after stopping our stream")
    return errs


def check(root: Path) -> list[str]:
    errors = check_workflows(root)
    errors += check_obs_stream_shape(root)
    if not (root / OBS_LIB).is_file():
        errors.append(f"{OBS_LIB} is missing")
    for f in sorted((root / SCRIPTS).rglob("*")):
        if not f.is_file() or f.relative_to(root) == SELF or SKIP_DIRS & set(f.parts):
            continue
        errors += script_errors(f.relative_to(root), f.read_text(encoding="utf-8", errors="replace"))
    return errors


# ---------------------------------------------------------------- self-test --

# Inserted into obs-stream.ps1 Stop-OurStream, right before its StopStream.
STREAM_ANCHOR = '$resp = Invoke-ObsRequest "StopStream"'
INSERTIONS: list[tuple[str, str]] = [
    ("Stop-Process -Name obs64 -Force", "process action"),
    ("Get-Process obs64 | Stop-Process -Force", "process action"),
    ("$z = Get-Process -Name obs64\n{i}Stop-Process -Id $z.Id -Force", "looks up obs64"),
    ("taskkill /F /IM obs64.exe", "obs64.exe"),
    ('Start-Process "C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe"', "obs64.exe"),
    ('schtasks.exe /run /tn "StartOBS"', "scheduled task"),
    ('Register-ScheduledTask -TaskName "Start OBS Studio" -Action $a', "scheduled task"),
    ("$null = (Get-Process obs64).CloseMainWindow()", "closes OBS"),
    ('Remove-Item "C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\.sentinel" -Force', "obs-studio"),
    ("$enc.bitrate = 12000; $enc | ConvertTo-Json | Set-Content $streamEncoderFile", "streamEncoder"),
    ('$r = @{ op = 6; d = @{ requestType = "StopRecord"; requestId = "x" } }', "'StopRecord' is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "StartRecord"; requestId = "x" } }', "'StartRecord' is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetCurrentProgramScene"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetStreamServiceSettings"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetCurrentProfile"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "ToggleStream"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "ExitOBS"; requestId = "x" } }', "closes OBS"),
    ("""$j = '{"op":6,"d":{"requestType":"StopRecord","requestId":"x"}}'""", "'StopRecord' is not allowed"),
    ('$t = "Stop" + "Record"; $r = @{ op = 6; d = @{ requestType = $t } }', "unrecognized requestType shape"),
    ('$d["requestType"] = "StopRecord"', "unrecognized requestType shape"),
    ('$d.Add("requestType", "SetCurrentProgramScene")', "unrecognized requestType shape"),
    ('$j = @"\n{i}{"op":6,"d":{"requestType":\n{i}"StopRecord"}}\n{i}"@', "unrecognized requestType shape"),
    ('$r = @{ d = @{ requestType = "StartStream".Replace("StartStream","StopRecord") } }', "unrecognized requestType shape"),
    ('$k = "request" + "Type"; $d = @{}; $d[$k] = "StopRecord"; Send-ObsJson @{ op = 6; d = $d }', "client internals outside"),
    ("$b = @{ op = 8; d = @{ requestId = 'b'; requests = @() } }", "request batch"),
    ("obs-cmd scene switch PRO", "third-party OBS client"),
    ('$s = "<#"; Stop-Process -Name obs64 -Force; $t = "#>"', "process action"),
    ("Stop-Process -Id (Get-NetTCPConnection -LocalPort 4455).OwningProcess -Force", "port 4455"),
    ("Get-Process -Name ob*64 | Stop-Process -Force", "wildcard process name"),
    ('$null = Invoke-ObsRequest "StopRecord"', "Invoke-ObsRequest sends 'StopRecord'"),
    ("$null = Invoke-ObsRequest $kind", "without a literal request type"),
    ('$null = Invoke-ObsRequest "StartStream"', "StartStream request is allowed only in"),
    (
        "function Set-It { param([string]$requestType) Write-Host @{ d = @{ requestType = $requestType } } }",
        "not an allowlisted pass-through",
    ),
]

YT_STOP = "      - name: Stop OBS stream\n        if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'"
FB_START = f"        run: {START_RUN}\n\n      - name: Start delivery"
ST_ASSERT = f"        run: {ASSERT_RUN}"
DISCONNECT = "      - name: OBS disconnect/reconnect resilience test\n        if: success()"
REPUB_CALL = "& " + PS + "obs-stream.ps1 -Action Republish -GapSeconds 10"
CI_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("YT teardown without the started marker", YT_STOP, "      - name: Stop OBS stream\n        if: always()", "needs exactly"),
    ("YT teardown with an || marker condition", YT_STOP,
     "      - name: Stop OBS stream\n        if: always() || env.OBS_STREAMING_STARTED_BY_CI == 'true'", "needs exactly"),
    ("YT teardown with a != marker condition", YT_STOP,
     "      - name: Stop OBS stream\n        if: always() && env.OBS_STREAMING_STARTED_BY_CI != 'true'", "needs exactly"),
    ("YT teardown with a mixed ${{ }} || condition", YT_STOP,
     "      - name: Stop OBS stream\n        if: ${{ always() }} || env.OBS_STREAMING_STARTED_BY_CI == 'true'", "needs exactly"),
    ("YT teardown on failure() only", YT_STOP, "      - name: Stop OBS stream\n        if: failure()", "needs exactly"),
    ("FB start made non-blocking (; exit 0)", FB_START, FB_START.replace("-Action Start\n", "-Action Start; exit 0\n", 1),
     "not a canonical"),
    ("FB start with continue-on-error", FB_START,
     FB_START.replace("        run:", "        continue-on-error: true\n        run:", 1), "must be unconditional"),
    ("FB start with if: false", FB_START, FB_START.replace("        run:", "        if: false\n        run:", 1),
     "must be unconditional"),
    ("e2e-streaming-test stops via positional `Stop`", ST_ASSERT, f"        run: {PS}obs-stream.ps1 Stop", "not a canonical"),
    ("e2e-streaming-test stops via -Action \"Stop\"", ST_ASSERT, f'        run: {PS}obs-stream.ps1 -Action "Stop"', "not a canonical"),
    ("e2e-streaming-test stops via -Action:Stop", ST_ASSERT, f"        run: {PS}obs-stream.ps1 -Action:Stop", "not a canonical"),
    ("e2e-streaming-test runs the canonical Stop before any start", ST_ASSERT, f"        run: {STOP_RUN}",
     "before this job started it"),
    ("YT teardown stops via -Action 'Stop'", YT_STOP + f"\n        shell: powershell\n        timeout-minutes: 2\n        run: {STOP_RUN}",
     YT_STOP + f"\n        shell: powershell\n        timeout-minutes: 2\n        run: {PS}obs-stream.ps1 -Action 'Stop'",
     "not a canonical"),
    ("e2e-streaming-test inline StopStream (the removed bug)", ST_ASSERT,
     "        run: |\n          $stop = @{ op = 6; d = @{ requestType = \"StopStream\"; requestId = \"stop-stream\" } }",
     "inline obs-websocket client"),
    ("an inline websocket client re-appears", "          Write-Host \"Republishing OBS stream (10 s outage)...\"",
     "          $ws = New-Object System.Net.WebSockets.ClientWebSocket\n          Write-Host \"Republishing OBS stream (10 s outage)...\"",
     "inline obs-websocket client"),
    ("the disconnect republish runs on always()", DISCONNECT,
     "      - name: OBS disconnect/reconnect resilience test\n        if: always()", "only on success"),
    ("a republish before the job's start", ST_ASSERT, f"        run: |\n          {REPUB_CALL}", "before this job started it"),
    ("marker forced true in a job env", '      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      # Dedicated event',
     '      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      OBS_STREAMING_STARTED_BY_CI: "true"\n      # Dedicated event',
     "may not be set in an env"),
    ("a restore step re-appears", "- name: Verify cache_delay_secs unchanged", "- name: Restore OBS scene",
     "step name describes an OBS mutation"),
    ("the fire-and-forget API start re-appears", "          $startBody = @{ event_id = $ev.id } | ConvertTo-Json",
     '          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/obs/start-stream" -Method POST -TimeoutSec 30\n'
     "          $startBody = @{ event_id = $ev.id } | ConvertTo-Json", "skips the readiness check"),
    ("an OBS kill hidden in a step's with: input", "      - name: Start delivery",
     "      - name: evil\n        uses: some/action@v1\n        with:\n          cmd: taskkill /F /IM obs64.exe\n\n      - name: Start delivery",
     "obs64.exe"),
    ("the guard moved onto the stream box", "  test-integrity:\n    name: Test integrity check\n    runs-on: ubuntu-latest",
     "  test-integrity:\n    name: Test integrity check\n    runs-on: [self-hosted, windows, stream-lan]",
     "may only run in a GitHub-hosted job"),
    ("a self-hosted runner group is not treated as hosted",
     "    runs-on: [self-hosted, windows, stream-lan]\n    timeout-minutes: 20\n    steps:",
     "    runs-on: {group: stream}\n    timeout-minutes: 20\n    steps:\n      - run: taskkill /F /IM obs64.exe", "obs64.exe"),
]
STREAM_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("Start skips the readiness check", "$why = Test-ObsReady", "$why = $null", "after the Test-ObsReady"),
    ("Start ignores the readiness verdict", "    if ($why) { Write-NotReady $why; exit 1 }\n    Set-StartedMarker", "    Set-StartedMarker",
     "exit on its verdict"),
    ("Start never writes the marker", '    Set-StartedMarker "true"\n', "", "marker true right before"),
    ("a refused start keeps the marker true", '      Set-StartedMarker "false"\n      Write-Host "::error::StartStream refused',
     '      Write-Host "::error::StartStream refused', "false when it is refused"),
    ("Start ignores a held rig lease", 'if ($holder) { Write-NotReady "camera-box holds the rig lease: $holder"; exit 1 }',
     "$null = $holder", "held rig lease"),
    ("Republish keeps the marker true across the gap", '      Stop-OurStream\n      Set-StartedMarker "false"\n',
     "      Stop-OurStream\n", "Republish must write the marker false"),
    ("AssertNotStreaming stops a foreign stream",
     '      Write-Host "::error::stream OBS is already streaming',
     '      $null = Invoke-ObsRequest "StopStream"\n      Write-Host "::error::stream OBS is already streaming',
     "StopStream request is allowed only in"),
]
EXTRA_SCRIPTS: list[tuple[str, dict[str, str], str]] = [
    ("a new script restarts OBS", {"scripts/ci/fix-obs.ps1": 'Stop-Process -Name obs64 -Force\nschtasks /run /tn "StartOBS"\n'},
     "process action"),
    ("another script writes the started marker",
     {"scripts/ci/fake.ps1": f'"{MARKER}=true" | Out-File $env:GITHUB_ENV -Append\n'}, "may only be written by"),
    ("a helper script frees the inpoint with StopStream",
     {"scripts/ci/free-inpoint.ps1": '. "$PSScriptRoot\\obs-ws.ps1"\nConnect-Obs\n$null = Invoke-ObsRequest "StopStream"\n'},
     "StopStream request is allowed only in"),
    ("a helper script starts without readiness",
     {"scripts/ci/quick-start.ps1": '. "$PSScriptRoot\\obs-ws.ps1"\nConnect-Obs\n$null = Invoke-ObsRequest "StartStream"\n'},
     "StartStream request is allowed only in"),
    ("a helper script opens its own websocket",
     {"scripts/ci/own-ws.ps1": "$w = New-Object System.Net.WebSockets.ClientWebSocket\n"}, "client internals outside"),
]


def _insert_before(text: str, anchor: str, line: str) -> str:
    pos = text.find(anchor)
    if pos < 0:
        raise AssertionError(f"self-test anchor missing: {anchor}")
    bol = text.rfind("\n", 0, pos) + 1
    indent = text[bol:pos]
    return text[:bol] + indent + line.replace("{i}", indent) + "\n" + text[bol:]


def self_test(root: Path) -> int:
    failures: list[str] = []
    real_ci = (root / CI).read_text(encoding="utf-8")
    real_stream = (root / OBS_STREAM).read_text(encoding="utf-8")
    count = 0

    def run_case(desc: str, files: dict[Path, str], expect: str | None) -> None:
        nonlocal count
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp)
            shutil.copytree(root / WORKFLOWS, t / WORKFLOWS)
            shutil.copytree(root / SCRIPTS, t / SCRIPTS)
            for rel, content in files.items():
                (t / rel).parent.mkdir(parents=True, exist_ok=True)
                (t / rel).write_text(content, encoding="utf-8")
            errs = check(t)
        if expect is None:
            if errs:
                failures.append(f"clean copy: expected PASS, got: {errs[:3]}")
            else:
                print("  ok   clean copy passes")
            return
        count += 1
        hit = [e for e in errs if expect in e]
        if hit:
            print(f"  ok   RED  {desc}: {hit[0][:150]}")
        else:
            failures.append(f"{desc}: expected an error containing {expect!r}, got {errs[:3]}")

    def replaced(text: str, old: str, new: str, desc: str) -> str | None:
        if old not in text:
            failures.append(f"{desc}: self-test anchor missing: {old[:70]!r}")
            return None
        return text.replace(old, new, 1)

    run_case("clean", {}, None)
    for line, expect in INSERTIONS:
        run_case(f"insert `{line.splitlines()[0][:60]}`", {OBS_STREAM: _insert_before(real_stream, STREAM_ANCHOR, line)}, expect)
    for desc, old, new, expect in CI_MUTATIONS:
        text = replaced(real_ci, old, new, desc)
        if text is not None:
            run_case(desc, {CI: text}, expect)
    for desc, old, new, expect in STREAM_MUTATIONS:
        text = replaced(real_stream, old, new, desc)
        if text is not None:
            run_case(desc, {OBS_STREAM: text}, expect)
    for desc, files, expect in EXTRA_SCRIPTS:
        run_case(desc, {Path(k): v for k, v in files.items()}, expect)
    if failures:
        print("SELF-TEST FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"self-test OK: all {count} regressed copies are red, the clean copy is green")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=".", help="repository root (default: .)")
    ap.add_argument("--self-test", action="store_true", help="prove the guard red on regressed copies")
    args = ap.parse_args()
    root = Path(args.root)
    if args.self_test:
        return self_test(root)
    errors = check(root)
    if errors:
        print("ERROR: CI must never mutate stream OBS (#374, owner directive 2026-08-30:")
        print("only Start/Stop streaming; camera-box owns OBS):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("OK: workflows and scripts/ only Start/Stop OBS streaming and read its status (#374).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
