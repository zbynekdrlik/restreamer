#!/usr/bin/env python3
"""#374 guard: restreamer CI never mutates stream OBS.

Stream OBS on the stream box (10.77.9.204) is camera-box's development target.
Owner directive 2026-08-30: restreamer may only START and STOP OBS streaming --
no kill, no relaunch, no scheduled task, no scene / profile / bitrate /
stream-service change, no "restore OBS ...", no recording control (StopRecord
included, #374 scope addition). Before #374, CI force-killed and relaunched OBS
and rewrote its settings (four force-kills on 2026-10-06 lost camera-box's
unsaved runtime settings).

Scope: every job that runs on a self-hosted runner in every
`.github/workflows/*.yml` (its steps' `run`, `with`, `env`, `uses` and name, plus
the job `env`), and every file under `scripts/`. A GitHub-hosted job cannot reach
OBS, so its text (e.g. a guard's grep pattern) is not scanned. The guard fails on:

  * a process action on OBS (kill, suspend, launch, clean close, scheduled task,
    the process owning port 4455, a wildcard process name);
  * any touch of OBS's install or config tree (`obs-studio`, `streamEncoder`);
  * an obs-websocket request type outside READ_ONLY_AND_STREAMING; any
    `requestType` written in another shape than a literal assignment (fail-closed);
    a request batch; a third-party OBS client;
  * a function that forwards a request type, unless it is one of PASSTHROUGH_FUNCS,
    and a call to one of those that does not pass an allowed literal;
  * a step named like an OBS set/switch/restore/kill;
  * Restreamer's fire-and-forget `/api/v1/obs/start-stream` (it skips readiness);
  * a job whose first OBS start is not exactly `obs-stream.ps1 -Action Start`
    (rig lease + readiness + a checked StartStream in one session) as an
    unconditional step;
  * a StopStream before that start, and a non-success-only StopStream whose `if:`
    is not exactly the started-by-CI condition;
  * the started-by-CI marker set anywhere but obs-stream.ps1 (an `env:` map, a run);
  * obs-stream.ps1 issuing StartStream before its readiness check / marker write.

Self-match-proof (#325): the patterns and the self-test mutations live only in
this file, which the scan skips by path; the ci.yml step that runs it carries
none of them, and runs in a GitHub-hosted job (checked here too).

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
MARKER = "OBS_STREAMING_STARTED_BY_CI"
START_RUN = "powershell -NoProfile -ExecutionPolicy Bypass -File scripts/ci/obs-stream.ps1 -Action Start"
TEARDOWN_IF = f"always() && env.{MARKER} == 'true'"

READ_ONLY_AND_STREAMING = {
    "StartStream",
    "StopStream",
    "GetStreamStatus",
    "GetRecordStatus",
    "GetVersion",
    "GetCurrentProgramScene",
    "GetStreamServiceSettings",
}
# Functions allowed to forward a `$requestType` parameter; every call to them must
# pass an allowed literal.
PASSTHROUGH_FUNCS = {"Invoke-ObsRequest", "Send-ObsRequest"}

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
        re.compile(r"/api/v1/obs/start-stream", re.I),
        "Restreamer's /api/v1/obs/start-stream is fire-and-forget and skips the readiness check; "
        "use scripts/ci/obs-stream.ps1 -Action Start",
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

# Accepted requestType shapes; everything else that mentions requestType fails.
RT_LITERAL = re.compile(r"""\brequestType\s*=\s*"(?P<name>\w+)"(?=\s*(?:;|\}|$))""")
RT_JSON = re.compile(r"""\\?"requestType\\?"\s*:\s*\\?"(?P<name>\w+)\\?"(?=\s*(?:,|\}|$))""")
RT_PASSTHROUGH = re.compile(r"""\brequestType\s*=\s*\$requestType(?=\s*(?:;|\}|$))""")
RT_VAR_READ = re.compile(r"""\$\{?requestType\}?(?!\s*=)""")
FUNC_DEF = re.compile(r"^\s*function\s+(?P<name>[\w-]+)", re.I | re.M)

BAD_STEP_NAME = [
    re.compile(r"\brestore\b.*\bobs\b|\bobs\b.*\brestore\b", re.I),
    re.compile(r"\b(set|switch|change)\b.*\bobs\b.*\b(scene|bitrate|encoder|service|settings|profile)\b", re.I),
    re.compile(r"\b(kill|relaunch|restart|launch)\b.*\bobs\b|\bensure obs is running\b", re.I),
    re.compile(r"\bstoprecord\b", re.I),
]

START_TOKENS = re.compile(r"""obs-stream\.ps1\s+-Action\s+Start\b|["']StartStream["']""")
STOP_TOKENS = re.compile(r"""obs-stream\.ps1\s+-Action\s+Stop\b|["']StopStream["']|/api/v1/obs/stop-stream""")
SUCCESS_ONLY = {"", "success()"}
HOSTED_LABEL = re.compile(r"^(ubuntu|windows|macos)-[\w.]+$")
SKIP_DIRS = {"__pycache__"}


def strip_comments(text: str) -> str:
    """Drop PowerShell block comments that open a line and full-line `#` comments."""
    text = re.sub(r"(?m)^\s*<#.*?#>", "", text, flags=re.S)
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def norm_if(cond: object) -> str:
    c = str(cond if cond is not None else "").strip()
    m = re.fullmatch(r"\$\{\{\s*(.*?)\s*\}\}", c, flags=re.S)
    if m:
        c = m.group(1)
    return re.sub(r"\s+", " ", c).strip()


def runs_on_hosted(job: dict) -> bool:
    runs_on = job.get("runs-on", "")
    labels = runs_on if isinstance(runs_on, list) else [runs_on]
    return bool(labels) and all(isinstance(l, str) and HOSTED_LABEL.match(l.strip()) for l in labels)


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
    """Every `requestType = $requestType` must sit inside an allowlisted function."""
    errors: list[str] = []
    defs = [(m.start(), m.group("name")) for m in FUNC_DEF.finditer(code)]
    for m in RT_PASSTHROUGH.finditer(code):
        owner = None
        for start, name in defs:
            if start <= m.start():
                owner = name
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
            # The request type is the first PascalCase literal after the call.
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


def step_text(step: dict) -> str:
    parts = [str(step.get("run") or "")]
    for key in ("with", "env"):
        if step.get(key):
            parts.append(json.dumps(step[key], indent=1))
    return "\n".join(parts)


def check_job(wf_name: str, job_name: str, job: dict) -> list[str]:
    errors: list[str] = []
    steps = job.get("steps") or []
    where = f"{wf_name} {job_name}"
    if job.get("env"):
        errors += check_unit(f"{where} (job env)", json.dumps(job["env"], indent=1))
        if MARKER in json.dumps(job["env"]):
            errors.append(f"{where}: {MARKER} may not be set in an env: map")
    start_idx = None
    for i, step in enumerate(steps):
        name = str(step.get("name", ""))
        uses = str(step.get("uses") or "")
        label = f"{where} / {name or uses or f'step {i}'}"
        text = step_text(step)
        errors += check_unit(label, text)
        if MARKER in text:
            errors.append(f"{label}: {MARKER} may only be written by {OBS_STREAM} (found in run/with/env)")
        if re.search(OBS_WORD, uses, re.I):
            errors.append(f"{label}: uses an OBS action: {uses}")
        for rx in BAD_STEP_NAME:
            if rx.search(name):
                errors.append(f"{label}: step name describes an OBS mutation")
        code = strip_comments(str(step.get("run") or ""))
        cond = norm_if(step.get("if"))
        if START_TOKENS.search(code) and start_idx is None:
            start_idx = i
            if str(step.get("run") or "").strip() != START_RUN:
                errors.append(f"{label}: the first OBS start must be exactly `{START_RUN}`")
            if cond not in SUCCESS_ONLY or step.get("continue-on-error"):
                errors.append(f"{label}: the OBS start step must be unconditional (no if:/continue-on-error)")
        if STOP_TOKENS.search(code):
            if start_idx is None or i <= start_idx:
                errors.append(f"{label}: stops OBS streaming before this job started it (not ours to stop)")
            elif cond not in SUCCESS_ONLY and cond != TEARDOWN_IF:
                errors.append(f"{label}: a teardown StopStream needs exactly `if: {TEARDOWN_IF}` (got `{cond}`)")
            if "obs-stream.ps1" in code and cond != TEARDOWN_IF:
                errors.append(f"{label}: the obs-stream.ps1 teardown needs exactly `if: {TEARDOWN_IF}`")
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


def check_obs_stream_order(root: Path) -> list[str]:
    """obs-stream.ps1 must check readiness and set the marker before StartStream."""
    path = root / OBS_STREAM
    if not path.is_file():
        return [f"{OBS_STREAM} is missing"]
    code = strip_comments(path.read_text(encoding="utf-8"))
    start = code.find('Invoke-ObsRequest "StartStream"')
    ready = re.search(r"=\s*Test-ObsReady\b", code)
    mark = code.find('Set-StartedMarker "true"')
    errs = []
    if start < 0:
        errs.append(f"{OBS_STREAM}: no StartStream found")
    if not ready or ready.start() > start:
        errs.append(f"{OBS_STREAM}: StartStream must come after the Test-ObsReady readiness check")
    if mark < 0 or mark > start:
        errs.append(f"{OBS_STREAM}: {MARKER}=true must be written before StartStream")
    return errs


def check(root: Path) -> list[str]:
    errors = check_workflows(root)
    errors += check_obs_stream_order(root)
    for f in sorted((root / SCRIPTS).rglob("*")):
        if not f.is_file() or f.relative_to(root) == SELF or SKIP_DIRS & set(f.parts):
            continue
        rel = f.relative_to(root)
        text = f.read_text(encoding="utf-8", errors="replace")
        errors += check_unit(str(rel), text)
        if MARKER in strip_comments(text) and rel != OBS_STREAM:
            errors.append(f"{rel}: {MARKER} may only be written by {OBS_STREAM}")
    return errors


# ---------------------------------------------------------------- self-test --

# Inserted into scripts/ci/obs-stream.ps1, right before its Stop's StopStream.
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
    ("$b = @{ op = 8; d = @{ requestId = 'b'; requests = @() } }", "request batch"),
    ("obs-cmd scene switch PRO", "third-party OBS client"),
    ('$s = "<#"; Stop-Process -Name obs64 -Force; $t = "#>"', "process action"),
    ("Stop-Process -Id (Get-NetTCPConnection -LocalPort 4455).OwningProcess -Force", "port 4455"),
    ("Get-Process -Name ob*64 | Stop-Process -Force", "wildcard process name"),
    ('$null = Invoke-ObsRequest "StopRecord"', "Invoke-ObsRequest sends 'StopRecord'"),
    ("$null = Invoke-ObsRequest $kind", "without a literal request type"),
    (
        "function Set-It { param([string]$requestType) Send-ObsJson @{ op = 6; d = @{ requestType = $requestType } } }",
        "not an allowlisted pass-through",
    ),
]

# (description, old, new, expected-reason): one-shot text replacements on ci.yml.
YT_STOP = "      - name: Stop OBS stream\n        if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'"
FB_START = (
    "        run: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/ci/obs-stream.ps1 -Action Start\n"
    "\n      - name: Start delivery"
)
ST_ASSERT = "        run: powershell -NoProfile -ExecutionPolicy Bypass -File scripts/ci/obs-stream.ps1 -Action AssertNotStreaming"
CI_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("YT teardown without the started marker", YT_STOP, "      - name: Stop OBS stream\n        if: always()", "needs exactly"),
    (
        "YT teardown with an || marker condition",
        YT_STOP,
        "      - name: Stop OBS stream\n        if: always() || env.OBS_STREAMING_STARTED_BY_CI == 'true'",
        "needs exactly",
    ),
    (
        "YT teardown with a != marker condition",
        YT_STOP,
        "      - name: Stop OBS stream\n        if: always() && env.OBS_STREAMING_STARTED_BY_CI != 'true'",
        "needs exactly",
    ),
    ("YT teardown on failure() only", YT_STOP, "      - name: Stop OBS stream\n        if: failure()", "needs exactly"),
    (
        "FB start made non-blocking (; exit 0)",
        FB_START,
        FB_START.replace("-Action Start\n", "-Action Start; exit 0\n", 1),
        "must be exactly",
    ),
    (
        "FB start with continue-on-error",
        FB_START,
        FB_START.replace("        run:", "        continue-on-error: true\n        run:", 1),
        "must be unconditional",
    ),
    (
        "FB start with if: false",
        FB_START,
        FB_START.replace("        run:", "        if: false\n        run:", 1),
        "must be unconditional",
    ),
    (
        "e2e-streaming-test stops a stream it did not start (the removed bug)",
        ST_ASSERT,
        "        run: |\n"
        '          $stop = @{ op = 6; d = @{ requestType = "StopStream"; requestId = "stop-stream" } } | ConvertTo-Json -Depth 5',
        "before this job started it",
    ),
    (
        "marker forced true in a job env",
        '      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      # Dedicated event',
        '      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      OBS_STREAMING_STARTED_BY_CI: "true"\n      # Dedicated event',
        "may not be set in an env",
    ),
    (
        "a restore step re-appears",
        "- name: Verify cache_delay_secs unchanged",
        "- name: Restore OBS scene",
        "step name describes an OBS mutation",
    ),
    (
        "the fire-and-forget API start re-appears",
        "          $startBody = @{ event_id = $ev.id } | ConvertTo-Json",
        '          Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/obs/start-stream" -Method POST -TimeoutSec 30\n'
        "          $startBody = @{ event_id = $ev.id } | ConvertTo-Json",
        "fire-and-forget",
    ),
    (
        "an OBS kill hidden in a step's with: input",
        "      - name: Start delivery",
        "      - name: evil\n        uses: some/action@v1\n        with:\n          cmd: taskkill /F /IM obs64.exe\n\n      - name: Start delivery",
        "obs64.exe",
    ),
    (
        "the guard moved onto the stream box",
        "  test-integrity:\n    name: Test integrity check\n    runs-on: ubuntu-latest",
        "  test-integrity:\n    name: Test integrity check\n    runs-on: [self-hosted, windows, stream-lan]",
        "may only run in a GitHub-hosted job",
    ),
    (
        "a self-hosted runner group is not treated as hosted",
        "    runs-on: [self-hosted, windows, stream-lan]\n    timeout-minutes: 20\n    steps:",
        "    runs-on: {group: stream}\n    timeout-minutes: 20\n    steps:\n      - run: taskkill /F /IM obs64.exe",
        "obs64.exe",
    ),
]
# (description, old, new, expected-reason): replacements on obs-stream.ps1.
STREAM_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("Start skips the readiness check", "$why = Test-ObsReady", "$why = $null", "after the Test-ObsReady"),
    ("the marker is never written", '    Set-StartedMarker "true"\n', "", "must be written before StartStream"),
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

    def run_case(desc: str, files: dict[Path, str], expect: str | None) -> None:
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
        desc = f"insert `{line.splitlines()[0][:60]}`"
        run_case(desc, {OBS_STREAM: _insert_before(real_stream, STREAM_ANCHOR, line)}, expect)
    for desc, old, new, expect in CI_MUTATIONS:
        text = replaced(real_ci, old, new, desc)
        if text is not None:
            run_case(desc, {CI: text}, expect)
    for desc, old, new, expect in STREAM_MUTATIONS:
        text = replaced(real_stream, old, new, desc)
        if text is not None:
            run_case(desc, {OBS_STREAM: text}, expect)
    run_case(
        "a new script restarts OBS",
        {SCRIPTS / "ci" / "fix-obs.ps1": 'Stop-Process -Name obs64 -Force\nschtasks /run /tn "StartOBS"\n'},
        "process action",
    )
    run_case(
        "another script writes the started marker",
        {SCRIPTS / "ci" / "fake.ps1": f'"{MARKER}=true" | Out-File $env:GITHUB_ENV -Append\n'},
        "may only be written by",
    )
    if failures:
        print("SELF-TEST FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("self-test OK: every regressed copy is red, the clean copy is green")
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
