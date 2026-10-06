#!/usr/bin/env python3
"""#374 guard: restreamer CI never mutates stream OBS.

Stream OBS on the stream box (10.77.9.204) is camera-box's development target.
Owner directive 2026-08-30: restreamer may only START and STOP OBS streaming --
no kill, no relaunch, no scheduled task, no scene / profile / bitrate /
stream-service change, no "restore OBS ...", no recording control (StopRecord
included, #374 scope addition). Before #374, CI force-killed and relaunched OBS
and rewrote its settings (four force-kills on 2026-10-06 lost camera-box's
unsaved runtime settings).

This guard scans `.github/workflows/ci.yml` (the YAML-parsed step `run:` / `uses:`
text, so YAML comments are ignored) and every file under `scripts/`, and fails on:

  * a process action on OBS (kill, suspend, launch, clean close, scheduled task);
  * any write into OBS's install or config tree (`obs-studio`, `streamEncoder`);
  * an obs-websocket request type outside READ_ONLY_AND_STREAMING, a request type
    that is not a literal (unless it is a `$requestType` parameter whose every
    caller passes an allowed literal), a request batch, a third-party OBS client;
  * a step named like an OBS set/switch/restore;
  * a job that starts OBS streaming without the read-only readiness check
    (scripts/ci/obs-readiness-check.ps1) as the step right before the start, or
    without setting OBS_STREAMING_STARTED_BY_CI before the start;
  * an always()/failure()/cancelled() teardown that stops OBS streaming without
    the OBS_STREAMING_STARTED_BY_CI condition (it would stop camera-box's stream
    after a readiness failure).

Self-match-proof (#325): the patterns and the self-test mutations live only in
this file, which the scan skips by path; the ci.yml step that runs it carries
none of them. This file is only allowed to run in a GitHub-hosted job (never on
the stream box), which the guard also checks.

`--self-test` copies the real tree, applies each known-bad mutation, and
requires every copy to go red for its own reason, and the unmodified copy to
pass.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

WORKFLOW = Path(".github/workflows/ci.yml")
SCRIPTS = Path("scripts")
SELF = Path("scripts/ci/verify_no_obs_mutation.py")
READINESS = "scripts/ci/obs-readiness-check.ps1"
MARKER = "OBS_STREAMING_STARTED_BY_CI"

READ_ONLY_AND_STREAMING = {
    "StartStream",
    "StopStream",
    "GetStreamStatus",
    "GetRecordStatus",
    "GetVersion",
    "GetCurrentProgramScene",
    "GetStreamServiceSettings",
}

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
    (re.compile(r"closemainwindow", re.I), "closes a main window (OBS clean-close path)"),
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
        re.compile(r"requestbatch|\bop\b\W{0,3}\s*[=:]\s*8\b", re.I),
        "an obs-websocket request batch (op 8) bypasses the request allowlist",
    ),
    (
        re.compile(r"(?<![\w-])(?:obs-cmd|obs-cli|obsws|simpleobsws|obs_websocket)", re.I),
        "a third-party OBS client",
    ),
]

# A unit (one step's run, one script) that looks up obs64 may not also kill,
# launch or close any process: `$z = Get-Process obs64` + `Stop-Process -Id $z.Id`
# has no OBS word on the kill line.
UNIT_OBS_LOOKUP = re.compile(r"obs64", re.I)
UNIT_PROCESS_ACTION = re.compile(
    r"stop-process|taskkill|\.kill\s*\(|start-process|closemainwindow|schtasks|scheduledtask|suspend",
    re.I,
)

REQUEST_TYPE = re.compile(r"""requestType\\?["']?\s*[=:]\s*(?P<val>\S[^;}\n]*)""")
LITERAL = re.compile(r"""^\\?["'](?P<name>[A-Za-z]+)\\?["']""")
PASSTHROUGH = "$requestType"
PASSTHROUGH_FUNC = re.compile(r"function\s+(?P<name>[\w-]+)\s*\((?P<params>[^)]*)\$requestType", re.I)

BAD_STEP_NAME = [
    re.compile(r"\brestore\b.*\bobs\b|\bobs\b.*\brestore\b", re.I),
    re.compile(r"\b(set|switch|change)\b.*\bobs\b.*\b(scene|bitrate|encoder|service|settings|profile)\b", re.I),
    re.compile(r"\b(kill|relaunch|restart|launch)\b.*\bobs\b|\bensure obs is running\b", re.I),
    re.compile(r"\bstoprecord\b", re.I),
]

START_TOKENS = re.compile(r"""["']StartStream["']|/api/v1/obs/start-stream""")
STOP_TOKENS = re.compile(r"""["']StopStream["']|/api/v1/obs/stop-stream""")
NON_SUCCESS_IF = re.compile(r"always\(\)|failure\(\)|cancelled\(\)")


def strip_comments(text: str) -> str:
    """Drop PowerShell block comments and full-line `#` comments (ps1/sh/py/run)."""
    text = re.sub(r"<#.*?#>", "", text, flags=re.S)
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def check_unit(label: str, text: str) -> list[str]:
    errors: list[str] = []
    code = strip_comments(text)
    lines = code.splitlines()
    for n, line in enumerate(lines, 1):
        for rx, why in LINE_RULES:
            if rx.search(line):
                errors.append(f"{label}: {why}: {line.strip()}")
    if UNIT_OBS_LOOKUP.search(code):
        for line in lines:
            if UNIT_PROCESS_ACTION.search(line):
                errors.append(f"{label}: looks up obs64 AND runs a process action: {line.strip()}")

    passthrough_used = False
    for line in lines:
        for m in REQUEST_TYPE.finditer(line):
            val = m.group("val").strip()
            lit = LITERAL.match(val)
            if lit:
                if lit.group("name") not in READ_ONLY_AND_STREAMING:
                    errors.append(f"{label}: obs-websocket request '{lit.group('name')}' is not allowed: {line.strip()}")
            elif val.startswith(PASSTHROUGH) and not re.match(r"\$requestType\w", val):
                passthrough_used = True
            else:
                errors.append(f"{label}: non-literal obs-websocket requestType '{val}': {line.strip()}")

    funcs = [m.group("name") for m in PASSTHROUGH_FUNC.finditer(code)]
    if passthrough_used and not funcs:
        errors.append(f"{label}: requestType = $requestType outside a function taking a $requestType parameter")
    for fn in funcs:
        call = re.compile(rf"(?<![\w-]){re.escape(fn)}(?![\w-])", re.I)
        for line in lines:
            m = call.search(line)
            if not m or re.search(r"\bfunction\s", line, re.I):
                continue
            rest = line[m.end():]
            # The request type is the first positional argument that is a
            # PascalCase literal; a call whose arguments carry no such literal
            # (a variable request type) is refused.
            lit = re.search(r"""["']([A-Z][A-Za-z]+)["']""", rest)
            if not lit:
                errors.append(f"{label}: {fn} called without a literal request type: {line.strip()}")
            elif lit.group(1) not in READ_ONLY_AND_STREAMING:
                errors.append(f"{label}: {fn} sends '{lit.group(1)}', not allowed: {line.strip()}")
    return errors


def runs_on_hosted(job: dict) -> bool:
    runs_on = job.get("runs-on", "")
    text = " ".join(runs_on) if isinstance(runs_on, list) else str(runs_on)
    return "self-hosted" not in text


def check_workflow(path: Path) -> list[str]:
    errors: list[str] = []
    wf = yaml.safe_load(path.read_text(encoding="utf-8"))
    for job_name, job in (wf.get("jobs") or {}).items():
        steps = job.get("steps") or []
        start_idx = None
        for i, step in enumerate(steps):
            name = str(step.get("name", ""))
            run = str(step.get("run") or "")
            uses = str(step.get("uses") or "")
            label = f"ci.yml {job_name} / {name or uses or f'step {i}'}"
            errors += check_unit(label, run)
            if re.search(OBS_WORD, uses, re.I):
                errors.append(f"{label}: uses an OBS action: {uses}")
            for rx in BAD_STEP_NAME:
                if rx.search(name):
                    errors.append(f"{label}: step name describes an OBS mutation")
            if SELF.name in run and not runs_on_hosted(job):
                errors.append(f"{label}: {SELF} may only run in a GitHub-hosted job")
            if start_idx is None and START_TOKENS.search(strip_comments(run)):
                start_idx = i
            cond = str(step.get("if", ""))
            if not runs_on_hosted(job) and STOP_TOKENS.search(strip_comments(run)) and NON_SUCCESS_IF.search(cond) and MARKER not in cond:
                errors.append(
                    f"{label}: a teardown StopStream needs `env.{MARKER} == 'true'` in its if: "
                    f"(else it stops a stream CI never started)"
                )
        # Only a job on the stream box can reach OBS; a hosted job's text that
        # merely mentions StartStream (a guard's grep pattern) is not a start.
        if start_idx is None or runs_on_hosted(job):
            continue
        start = steps[start_idx]
        label = f"ci.yml {job_name} / {start.get('name')}"
        prev_run = str(steps[start_idx - 1].get("run") or "") if start_idx > 0 else ""
        if READINESS not in prev_run:
            errors.append(f"{label}: the step right before the first OBS StartStream must run {READINESS}")
        run = strip_comments(str(start.get("run") or ""))
        mark = re.search(rf"{MARKER}=true.*GITHUB_ENV", run)
        first_start = START_TOKENS.search(run)
        if not mark or mark.start() > first_start.start():
            errors.append(f"{label}: must write {MARKER}=true to GITHUB_ENV before the StartStream")
    return errors


def check(root: Path) -> list[str]:
    errors = check_workflow(root / WORKFLOW)
    if not (root / SCRIPTS / "ci" / Path(READINESS).name).is_file():
        errors.append(f"{READINESS} is missing")
    for f in sorted((root / SCRIPTS).rglob("*")):
        if not f.is_file() or f.relative_to(root) == SELF:
            continue
        errors += check_unit(str(f.relative_to(root)), f.read_text(encoding="utf-8", errors="replace"))
    return errors


# ---------------------------------------------------------------- self-test --

# Inserted into the YT job's first-start step, right before the marker line.
YT_START_ANCHOR = f'"{MARKER}=true"'
INSERTIONS: list[tuple[str, str]] = [
    ("Stop-Process -Name obs64 -Force", "process action"),
    ("Get-Process obs64 | Stop-Process -Force", "process action"),
    ("$z = Get-Process -Name obs64\n{i}Stop-Process -Id $z.Id -Force", "looks up obs64 AND"),
    ("taskkill /F /IM obs64.exe", "obs64.exe"),
    ('Start-Process "C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe"', "obs64.exe"),
    ('schtasks.exe /run /tn "StartOBS"', "scheduled task"),
    ('Register-ScheduledTask -TaskName "Start OBS Studio" -Action $a', "scheduled task"),
    ("$null = (Get-Process obs64).CloseMainWindow()", "closes a main window"),
    ('Remove-Item "C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\.sentinel" -Force', "obs-studio"),
    ("$enc.bitrate = 12000; $enc | ConvertTo-Json | Set-Content $streamEncoderFile", "streamEncoder"),
    ('$r = @{ op = 6; d = @{ requestType = "StopRecord"; requestId = "x" } }', "'StopRecord' is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "StartRecord"; requestId = "x" } }', "'StartRecord' is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetCurrentProgramScene"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetStreamServiceSettings"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "SetCurrentProfile"; requestId = "x" } }', "is not allowed"),
    ('$r = @{ op = 6; d = @{ requestType = "ToggleStream"; requestId = "x" } }', "is not allowed"),
    ("""$j = '{"op":6,"d":{"requestType":"StopRecord","requestId":"x"}}'""", "'StopRecord' is not allowed"),
    ('$t = "Stop" + "Record"; $r = @{ op = 6; d = @{ requestType = $t } }', "non-literal"),
    ("$b = @{ op = 8; d = @{ requestId = 'b'; requests = @() } }", "request batch"),
    ("obs-cmd scene switch PRO", "third-party OBS client"),
]

# (description, old, new, count, expected-reason): text replacements on ci.yml.
WORKFLOW_MUTATIONS: list[tuple[str, str, str, int, str]] = [
    (
        "YT teardown StopStream without the started marker",
        f"if: always() && env.{MARKER} == 'true'",
        "if: always()",
        1,
        "needs `env.",
    ),
    (
        "a StopStream teardown on failure() without the marker",
        f"if: always() && env.{MARKER} == 'true'",
        "if: failure()",
        1,
        "needs `env.",
    ),
    (
        "marker written AFTER StartStream (YT)",
        f'"{MARKER}=true"',
        '"OTHER_MARKER=true"',
        1,
        f"must write {MARKER}=true",
    ),
    (
        "a restore step re-appears",
        "- name: Verify cache_delay_secs unchanged",
        "- name: Restore OBS scene",
        1,
        "step name describes an OBS mutation",
    ),
    (
        "the guard moved onto the stream box",
        "  test-integrity:\n    name: Test integrity check\n    runs-on: ubuntu-latest",
        "  test-integrity:\n    name: Test integrity check\n    runs-on: [self-hosted, windows, stream-lan]",
        1,
        "may only run in a GitHub-hosted job",
    ),
]

FINAL_READINESS_NAME = '- name: "Stream OBS readiness: final re-check right before StartStream (#374)"'


def _drop_step(text: str, header: str, which: int) -> str:
    """Remove the `which`-th (0-based; -1 = last) step block starting at header."""
    starts = [m.start() for m in re.finditer(re.escape(header), text)]
    if not starts:
        raise AssertionError(f"self-test anchor missing: {header}")
    s = text.rfind("\n", 0, starts[which]) + 1
    nxt = re.compile(r"^      - name:|^  \S", re.M).search(text, s + len(header))
    return text[:s] + text[nxt.start():]


def _insert_before(text: str, anchor: str, line: str, which: int = 0) -> str:
    starts = [m.start() for m in re.finditer(re.escape(anchor), text)]
    if not starts:
        raise AssertionError(f"self-test anchor missing: {anchor}")
    pos = starts[which]
    bol = text.rfind("\n", 0, pos) + 1
    indent = text[bol:pos]
    body = line.replace("{i}", indent)
    return text[:bol] + indent + body + "\n" + text[bol:]


def self_test(root: Path) -> int:
    failures: list[str] = []
    real_wf = (root / WORKFLOW).read_text(encoding="utf-8")

    def run_case(desc: str, wf_text: str | None, extra: dict[str, str], expect: str | None) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp)
            (t / WORKFLOW).parent.mkdir(parents=True)
            (t / WORKFLOW).write_text(real_wf if wf_text is None else wf_text, encoding="utf-8")
            shutil.copytree(root / SCRIPTS, t / SCRIPTS)
            for rel, content in extra.items():
                p = t / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(content, encoding="utf-8")
            errs = check(t)
        if expect is None:
            if errs:
                failures.append(f"clean copy: expected PASS, got: {errs[:3]}")
            else:
                print(f"  ok   clean copy passes")
            return
        hit = [e for e in errs if expect in e]
        if hit:
            print(f"  ok   RED  {desc}: {hit[0][:140]}")
        else:
            failures.append(f"{desc}: expected an error containing {expect!r}, got {errs[:3]}")

    run_case("clean", None, {}, None)
    for line, expect in INSERTIONS:
        run_case(f"insert `{line.splitlines()[0][:60]}`", _insert_before(real_wf, YT_START_ANCHOR, line), {}, expect)
    # The av-skew gate forwards a $requestType parameter: its callers are checked.
    for line, expect in [
        ('Send-ObsRequest $conn "StopRecord" "x"', "sends 'StopRecord'"),
        ('Send-ObsRequest $conn $kind "x"', "without a literal request type"),
    ]:
        run_case(f"insert `{line}`", _insert_before(real_wf, 'Send-ObsRequest $conn "StartStream"', line), {}, expect)
    for desc, old, new, count, expect in WORKFLOW_MUTATIONS:
        if real_wf.count(old) < count:
            failures.append(f"{desc}: self-test anchor missing: {old[:60]!r}")
            continue
        run_case(desc, real_wf.replace(old, new, count), {}, expect)
    run_case(
        "YT job loses the readiness check right before StartStream",
        _drop_step(real_wf, FINAL_READINESS_NAME, 0),
        {},
        "the step right before the first OBS StartStream",
    )
    run_case(
        "FB job loses the readiness check right before StartStream",
        _drop_step(real_wf, FINAL_READINESS_NAME, -1),
        {},
        "the step right before the first OBS StartStream",
    )
    readiness = (root / READINESS).read_text(encoding="utf-8")
    run_case(
        "readiness script sends a scene change through its read helper",
        None,
        {READINESS: readiness + '\n$null = Invoke-ObsRead "SetCurrentProgramScene"\n'},
        "sends 'SetCurrentProgramScene'",
    )
    run_case(
        "readiness script kills OBS",
        None,
        {READINESS: readiness + "\nStop-Process -Id $procs[0].Id -Force\n"},
        "looks up obs64 AND",
    )
    run_case(
        "a new script restarts OBS",
        None,
        {"scripts/ci/fix-obs.ps1": 'Stop-Process -Name obs64 -Force\nschtasks /run /tn "StartOBS"\n'},
        "process action",
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
    print("OK: ci.yml and scripts/ only Start/Stop OBS streaming and read its status (#374).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
