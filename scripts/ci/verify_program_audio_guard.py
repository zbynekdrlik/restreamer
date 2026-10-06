#!/usr/bin/env python3
"""#379 guard: every CI StartStream is gated by the program-audio check, and every
job that streams OBS runs the watchdog, its breach checks and its teardown.

Owner copyright rule: no room/FOH music on YouTube/Facebook. The runtime pieces live
in scripts/ci/program-audio-guard.ps1 (tested by tests/ci/test_program_audio_guard.py);
this file pins their WIRING, which no runtime test can see:

  * obs-stream.ps1 dot-sources the guard, and EVERY StartStream request in it is
    immediately preceded by `$why = Test-ProgramAudio -BeforeStart` (which also
    refuses after an earlier breach or a dead watchdog) + an exit on its verdict
    (only the started marker may sit between them). The #374 guard
    (verify_no_obs_mutation.py) already confines StartStream to obs-stream.ps1
    Start-OurStream and bans the start-stream API, so this covers every start site:
    `-Action Start` and every `-Action Republish`;
  * in every workflow job that runs `obs-stream.ps1 -Action Start`:
      - the very next step starts the watchdog, under
        `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`;
      - every LONG streaming step (timeout-minutes >= 10, or NO timeout-minutes: it
        may run to the job limit) is followed by a breach check before the next
        long step and before the OBS stop. Only a step with a timeout under 10
        minutes is short, whatever its `if:` says;
      - after the OBS stop, an `if: always()` teardown stops the watchdog and then
        asserts no breach;
      - no watchdog / check / teardown step is continue-on-error;
  * no workflow sets a PROGRAM_AUDIO_* knob (they exist for the mock test only);
  * the obs-scripts-test job runs the guard's mock test.

The checks parse the YAML (job/step structure), so the step that runs this file can
never self-match (#325). `--self-test` applies known-bad edits to a temp copy; each
must go red for its own reason, and the clean copy must pass.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_no_obs_mutation import norm_if, strip_comments  # noqa: E402  (the #374 guard's helpers)

WORKFLOWS = Path(".github/workflows")
CI = WORKFLOWS / "ci.yml"
SCRIPTS = Path("scripts")
OBS_STREAM = Path("scripts/ci/obs-stream.ps1")
GUARD = Path("scripts/ci/program-audio-guard.ps1")
MOCK_TEST = "python tests/ci/test_program_audio_guard.py"

PS = "powershell -NoProfile -ExecutionPolicy Bypass -File scripts/ci/"
START_RUN = PS + "obs-stream.ps1 -Action Start"
STOP_RUN = PS + "obs-stream.ps1 -Action Stop"
STARTED_IF = "always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'"
DOT_SOURCE = ". scripts/ci/program-audio-guard.ps1"
LONG_MINUTES = 10
GUARD_FUNCS = ["Test-ProgramAudio", "Start-ProgramAudioWatchdog", "Assert-NoProgramAudioBreach",
               "Stop-ProgramAudioWatchdog"]
GATED_START = re.compile(
    r'\$why = Test-ProgramAudio -BeforeStart[ \t]*\n[ \t]*if \(\$why\) \{ [^\n]*\bexit 1 \}[ \t]*\n'
    r'[ \t]*Set-StartedMarker "true"[ \t]*\n[ \t]*\$resp = Invoke-ObsRequest "StartStream"')
START_LITERAL = re.compile(r"""["']StartStream["']""")


def run_lines(step: dict) -> list[str]:
    return [l.strip() for l in strip_comments(str(step.get("run") or "")).splitlines() if l.strip()]


def is_call(step: dict, func: str) -> bool:
    lines = run_lines(step)
    return bool(lines) and lines[0] == DOT_SOURCE and func in lines[1:]


def check_obs_stream(root: Path) -> list[str]:
    path = root / OBS_STREAM
    if not path.is_file():
        return [f"{OBS_STREAM} is missing"]
    code = strip_comments(path.read_text(encoding="utf-8"))
    errs = []
    if not re.search(r'(?m)^\. "\$PSScriptRoot\\program-audio-guard\.ps1"$', code):
        errs.append(f"{OBS_STREAM}: must dot-source program-audio-guard.ps1 at top level")
    starts = len(START_LITERAL.findall(code))
    gated = len(GATED_START.findall(code))
    if starts == 0:
        errs.append(f"{OBS_STREAM}: no StartStream request found (anchor missing)")
    elif gated != starts:
        errs.append(f"{OBS_STREAM}: {starts - gated} of {starts} StartStream request(s) are not immediately preceded by "
                    "`$why = Test-ProgramAudio` + an exit on its verdict (only the started marker may sit between)")
    return errs


def check_guard_file(root: Path) -> list[str]:
    path = root / GUARD
    if not path.is_file():
        return [f"{GUARD} is missing"]
    code = strip_comments(path.read_text(encoding="utf-8"))
    return [f"{GUARD}: function {f} is missing" for f in GUARD_FUNCS
            if not re.search(rf"(?m)^function {re.escape(f)}\b", code)]


def long_step(step: dict) -> bool:
    t = step.get("timeout-minutes")
    bounded_short = isinstance(t, (int, float)) and not isinstance(t, bool) and t < LONG_MINUTES
    # A short `if: always()` cleanup step is not a streaming step; an always() step that
    # can run long is (adding always() must not buy a long step out of its check).
    return not bounded_short


def check_job(job_name: str, job: dict) -> list[str]:
    steps = job.get("steps") or []
    where = f"ci.yml {job_name}"
    starts = [i for i, s in enumerate(steps) if "\n".join(run_lines(s)) == START_RUN]
    if not starts:
        return []
    errs: list[str] = []
    start = starts[0]
    stops = [i for i, s in enumerate(steps) if "\n".join(run_lines(s)) == STOP_RUN]
    stop = stops[-1] if stops else len(steps)
    if not stops:
        errs.append(f"{where}: streams OBS but has no OBS stop step (anchor missing)")
    nxt = steps[start + 1] if start + 1 < len(steps) else {}
    if not is_call(nxt, "Start-ProgramAudioWatchdog"):
        errs.append(f"{where}: the step right after the OBS start must start the program-audio watchdog")
    elif norm_if(nxt.get("if")) != STARTED_IF:
        errs.append(f"{where}: the watchdog start needs exactly `if: {STARTED_IF}` (got `{norm_if(nxt.get('if'))}`)")
    pending: str | None = None
    for i in range(start + 1, stop):
        s = steps[i]
        name = str(s.get("name") or f"step {i}")
        if is_call(s, "Assert-NoProgramAudioBreach"):
            pending = None
            continue
        if long_step(s):
            if pending:
                errs.append(f"{where}: long streaming step `{pending}` has no program-audio breach check before `{name}`")
            pending = name
    if pending:
        errs.append(f"{where}: long streaming step `{pending}` has no program-audio breach check before the OBS stop")
    teardown = [i for i, s in enumerate(steps) if is_call(s, "Stop-ProgramAudioWatchdog")]
    if not teardown:
        errs.append(f"{where}: no teardown step stops the program-audio watchdog")
    for i in teardown:
        s = steps[i]
        lines = run_lines(s)
        if i < stop:
            errs.append(f"{where}: the watchdog teardown must come after the OBS stop step")
        if norm_if(s.get("if")) != "always()":
            errs.append(f"{where}: the watchdog teardown needs exactly `if: always()` (got `{norm_if(s.get('if'))}`)")
        if "Assert-NoProgramAudioBreach" not in lines or \
                lines.index("Assert-NoProgramAudioBreach") < lines.index("Stop-ProgramAudioWatchdog"):
            errs.append(f"{where}: the watchdog teardown must stop the watchdog, THEN assert no breach")
    for i, s in enumerate(steps):
        if any(is_call(s, f) for f in GUARD_FUNCS[1:]) and s.get("continue-on-error"):
            errs.append(f"{where}: program-audio step `{s.get('name')}` may not be continue-on-error")
    return errs


def knob_errors(wf_name: str, wf: dict) -> list[str]:
    errs = []

    def scan(label: str, obj: object) -> None:
        if obj and "PROGRAM_AUDIO_" in yaml.safe_dump(obj):
            errs.append(f"{wf_name} {label}: sets a PROGRAM_AUDIO_* knob (test-only; CI must use the real sampler)")

    scan("(workflow env)", wf.get("env"))
    for jn, job in (wf.get("jobs") or {}).items():
        scan(f"{jn} (job env)", job.get("env"))
        for s in job.get("steps") or []:
            scan(f"{jn} / {s.get('name')}", {"env": s.get("env"), "run": s.get("run"), "with": s.get("with")})
    return errs


def check_workflows(root: Path) -> list[str]:
    errs: list[str] = []
    streaming = 0
    for path in sorted((root / WORKFLOWS).glob("*.y*ml")):
        wf = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        errs += knob_errors(path.name, wf)
        for jn, job in (wf.get("jobs") or {}).items():
            job_errs = check_job(jn, job)
            if any("\n".join(run_lines(s)) == START_RUN for s in job.get("steps") or []):
                streaming += 1
            errs += job_errs
    if streaming == 0:
        errs.append("no workflow job runs obs-stream.ps1 -Action Start (anchor missing)")
    wf = yaml.safe_load((root / CI).read_text(encoding="utf-8")) or {}
    mock = (wf.get("jobs") or {}).get("obs-scripts-test") or {}
    if not any(MOCK_TEST in str(s.get("run") or "") for s in mock.get("steps") or []):
        errs.append(f"ci.yml: obs-scripts-test must run `{MOCK_TEST}`")
    return errs


def check(root: Path) -> list[str]:
    return check_obs_stream(root) + check_guard_file(root) + check_workflows(root)


# ---------------------------------------------------------------- self-test --

YT_SUSTAINED = '      - name: "GATE: Sustained YT health + endpoint chunk_delay (15 min observation)"'
CHECK_STEP = ('      - name: "Program-audio breach check (#379)"\n        shell: powershell\n        timeout-minutes: 1\n'
              '        run: |\n          . scripts/ci/program-audio-guard.ps1\n          Assert-NoProgramAudioBreach\n\n')
WD_IF = "      - name: \"Program-audio watchdog: start (#379)\"\n        if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'"
TEARDOWN = '      - name: "Program-audio guard: stop the watchdog + final breach check (#379)"\n        if: always()'
GATED = '    $why = Test-ProgramAudio -BeforeStart\n    if ($why) { Write-ProgramAudioError "not starting OBS streaming" $why; exit 1 }\n'

STREAM_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("Start drops the program-audio check", GATED, "", "not immediately preceded"),
    ("Start ignores an earlier breach (no -BeforeStart)", "    $why = Test-ProgramAudio -BeforeStart\n",
     "    $why = Test-ProgramAudio\n", "not immediately preceded"),
    ("Start ignores the verdict", '    if ($why) { Write-ProgramAudioError "not starting OBS streaming" $why; exit 1 }\n',
     "", "not immediately preceded"),
    ("the verdict only warns", "$why; exit 1 }\n    Set-StartedMarker", "$why }\n    Set-StartedMarker", "not immediately preceded"),
    ("the check moves before readiness (not right before StartStream)", GATED, "",
     "not immediately preceded"),
    ("a wait sits between the check and StartStream", '    Set-StartedMarker "true"\n',
     '    Start-Sleep -Seconds 30\n    Set-StartedMarker "true"\n', "not immediately preceded"),
    ("the guard is no longer dot-sourced", '. "$PSScriptRoot\\program-audio-guard.ps1"\n', "", "must dot-source"),
    ("a second ungated StartStream", "function Stop-OurStream {",
     'function Start-Again {\n  $resp = Invoke-ObsRequest "StartStream"\n}\n\nfunction Stop-OurStream {',
     "1 of 2 StartStream"),
]
CI_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("YT watchdog start only on success", WD_IF, WD_IF.split("\n")[0] + "\n        if: success()", "needs exactly"),
    ("watchdog start continue-on-error", WD_IF, WD_IF + "\n        continue-on-error: true", "continue-on-error"),
    ("a step between the OBS start and the watchdog", "      # #379: the program-audio watchdog, as in the YouTube job",
     "      - name: evil\n        run: echo hi\n\n      # #379: the program-audio watchdog, as in the YouTube job",
     "right after the OBS start"),
    ("the breach check after the sustained YT gate is dropped",
     CHECK_STEP + '      - name: OBS disconnect/reconnect resilience test', '      - name: OBS disconnect/reconnect resilience test',
     "has no program-audio breach check before"),
    ("the breach check after the FB soak is dropped",
     CHECK_STEP + '      - name: "GATE: FB-side', '      - name: "GATE: FB-side', "STRICT: 30-min sustained soak"),
    ("the last breach check before the YT stop is dropped",
     CHECK_STEP + '      - name: "GATE: Second YouTube health check', '      - name: "GATE: Second YouTube health check',
     "before the OBS stop"),
    ("a new 30-min streaming step without a breach check", YT_SUSTAINED,
     "      - name: evil long soak\n        timeout-minutes: 30\n        run: echo soak\n\n" + YT_SUSTAINED,
     "`evil long soak` has no program-audio breach check"),
    ("a new UNTIMED streaming step without a breach check", YT_SUSTAINED,
     "      - name: evil untimed soak\n        run: echo soak\n\n" + YT_SUSTAINED,
     "`evil untimed soak` has no program-audio breach check"),
    ("an always() cleanup step is not a streaming step", YT_SUSTAINED,
     "      - name: evil cleanup\n        if: always()\n        timeout-minutes: 2\n        run: echo cleanup\n\n"
     + YT_SUSTAINED, None),
    ("always() does not exempt a 30-min step", YT_SUSTAINED,
     "      - name: evil always soak\n        if: always()\n        timeout-minutes: 30\n        run: echo soak\n\n"
     + YT_SUSTAINED, "`evil always soak` has no program-audio breach check"),
    ("YT teardown only on success", TEARDOWN, TEARDOWN.replace("always()", "success()"), "needs exactly `if: always()`"),
    ("YT teardown asserts before stopping", "          Stop-ProgramAudioWatchdog\n          Assert-NoProgramAudioBreach",
     "          Assert-NoProgramAudioBreach\n          Stop-ProgramAudioWatchdog", "THEN assert"),
    ("YT teardown never asserts", "          Stop-ProgramAudioWatchdog\n          Assert-NoProgramAudioBreach",
     "          Stop-ProgramAudioWatchdog", "THEN assert"),
    ("YT teardown never stops the watchdog", "          Stop-ProgramAudioWatchdog\n          Assert-NoProgramAudioBreach",
     "          Assert-NoProgramAudioBreach", "no teardown step stops"),
    ("the watchdog is stopped while OBS still streams", "      - name: Stop OBS stream\n",
     '      - name: early stop\n        if: always()\n        run: |\n          . scripts/ci/program-audio-guard.ps1\n'
     "          Stop-ProgramAudioWatchdog\n          Assert-NoProgramAudioBreach\n\n      - name: Stop OBS stream\n",
     "must come after the OBS stop step"),
    ("a job points the sampler at a fake", "      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      # Dedicated event",
     "      OBS_WS_PASSWORD: ${{ secrets.OBS_WS_PASSWORD }}\n      PROGRAM_AUDIO_URL: http://127.0.0.1:1/x\n"
     "      # Dedicated event", "PROGRAM_AUDIO_* knob"),
    ("a run block relaxes the freshness limit", "          Start-ProgramAudioWatchdog",
     "          $env:PROGRAM_AUDIO_MAX_AGE_S = '3600'\n          Start-ProgramAudioWatchdog", "PROGRAM_AUDIO_* knob"),
    ("the mock test is not run", "        run: python tests/ci/test_program_audio_guard.py", "        run: echo skipped",
     "must run `python tests/ci/test_program_audio_guard.py`"),
]
GUARD_MUTATIONS: list[tuple[str, str, str, str]] = [
    ("the teardown function is gone", "function Stop-ProgramAudioWatchdog {", "function Stop-Something {",
     "Stop-ProgramAudioWatchdog is missing"),
]


def self_test(root: Path) -> int:
    failures: list[str] = []
    real = {CI: (root / CI).read_text(encoding="utf-8"), OBS_STREAM: (root / OBS_STREAM).read_text(encoding="utf-8"),
            GUARD: (root / GUARD).read_text(encoding="utf-8")}
    count = 0

    def run_case(desc: str, files: dict[Path, str], expect: str | None) -> None:
        nonlocal count
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp)
            shutil.copytree(root / WORKFLOWS, t / WORKFLOWS)
            shutil.copytree(root / SCRIPTS, t / SCRIPTS)
            for rel, content in files.items():
                (t / rel).write_text(content, encoding="utf-8")
            errs = check(t)
        if expect is None:
            if errs:
                failures.append(f"{desc}: expected PASS, got: {errs[:3]}")
            else:
                print(f"  ok   GREEN {desc}")
            return
        count += 1
        hit = [e for e in errs if expect in e]
        if hit:
            print(f"  ok   RED  {desc}: {hit[0][:150]}")
        else:
            failures.append(f"{desc}: expected an error containing {expect!r}, got {errs[:3]}")

    run_case("clean", {}, None)
    moved = real[OBS_STREAM].replace(GATED, "", 1).replace("    $why = Test-ObsReady\n", GATED + "    $why = Test-ObsReady\n", 1)
    for rel, table in ((OBS_STREAM, STREAM_MUTATIONS), (CI, CI_MUTATIONS), (GUARD, GUARD_MUTATIONS)):
        for desc, old, new, expect in table:
            if desc.startswith("the check moves before readiness"):
                text = moved
            elif old not in real[rel]:
                failures.append(f"{desc}: self-test anchor missing: {old[:70]!r}")
                continue
            else:
                text = real[rel].replace(old, new, 1)
            run_case(desc, {rel: text}, expect)
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
        print("ERROR: the program-audio guard (#379, owner copyright rule: no room/FOH music on")
        print("YouTube/Facebook) is not wired into every OBS-streaming job:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("OK: every StartStream is gated by Test-ProgramAudio; every streaming job runs the watchdog, "
          "its breach checks and its teardown (#379).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
