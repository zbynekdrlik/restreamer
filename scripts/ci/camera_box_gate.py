#!/usr/bin/env python3
"""#357: the release gate on camera-box's full-path measurement.

restreamer CI measures the pipe. Only camera-box's full-path E2E measures what YouTube
actually recorded, so every main run dispatches it and a red YouTube leg blocks the
release. Contract: camera-box issue 1404 and restreamer #357.

Subcommands:
  measure        dispatch (or attach with --run-id), wait, read the verdict.
                 Exit 0 only for a measured YouTube pass. Writes `verdict=pass|red|unknown`
                 and `run_id` to $GITHUB_OUTPUT.
  token-check    prove CAMERA_BOX_ACTIONS_TOKEN may dispatch, without starting anything.
  release-check  fail unless this commit passed E2E Gate and the measurement.

Everything that is not a proven pass fails: no verdict, a malformed verdict, a cancelled
run and an expired token all block. Only `.youtube_leg.pass` decides: `.overall_pass`
also covers the cameras, and a camera fault must not block a restreamer release.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

CAMERA_BOX = "zbynekdrlik/camera-box"
WORKFLOW = "full-path-e2e.yml"
ARTIFACT = "recording-e2e-full-path"
# camera-box's job concludes on the rig too, so a failure can still carry a real verdict.
# Anything else (cancelled, timed_out) means the measurement did not finish.
MEASURED_CONCLUSIONS = ("success", "failure")
MAX_API_FAILURES = 5
PROBE_REF = "restreamer-token-check-no-such-ref"
RELEASE_CHECKS = ("E2E Gate", "Camera-box YouTube measurement")


@dataclass
class Result:
    state: str  # pass | red | unknown
    reasons: list[str] = field(default_factory=list)


def unknown(*reasons: str) -> Result:
    return Result("unknown", list(reasons))


def evaluate(artifact_dir: Path, conclusion: str) -> Result:
    """Judge a downloaded artifact. Pure: no network."""
    if conclusion not in MEASURED_CONCLUSIONS:
        return unknown(f"camera-box run ended {conclusion}: nothing was measured")
    files = sorted(Path(artifact_dir).glob("verdict-*.json"))
    if len(files) != 1:
        return unknown(f"expected one verdict file, found {len(files)} verdict files")
    try:
        verdict = json.loads(files[0].read_text())
    except (OSError, ValueError) as e:
        return unknown(f"{files[0].name} is not valid JSON: {e}")
    if not isinstance(verdict, dict):
        return unknown(f"{files[0].name} is not a JSON object")
    leg = verdict.get("youtube_leg")
    if not isinstance(leg, dict):
        return unknown(f"{files[0].name} has no youtube_leg")
    passed = leg.get("pass")
    if passed is True:
        return Result("pass")
    reasons = [str(r) for r in leg.get("reasons") or []]
    if passed is False:
        return Result("red", reasons or ["youtube_leg.pass is false, no reasons given"])
    return unknown(f"youtube_leg.pass is {passed!r}, not a boolean")


def gh(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=False)


def workflow_run_ids() -> dict[int, str] | None:
    p = gh(
        "api",
        f"repos/{CAMERA_BOX}/actions/workflows/{WORKFLOW}/runs"
        "?event=workflow_dispatch&per_page=30",
    )
    if p.returncode != 0:
        print(f"listing camera-box runs failed: {p.stderr.strip()}")
        return None
    return {r["id"]: r["created_at"] for r in json.loads(p.stdout)["workflow_runs"]}


def dispatch_and_find(caller: str) -> tuple[int | None, str]:
    """The dispatch API returns no run id: the newest run we had not seen is the candidate."""
    before = workflow_run_ids()
    if before is None:
        return None, "could not list camera-box runs (token?)"
    p = gh(
        "workflow", "run", WORKFLOW, "-R", CAMERA_BOX, "--ref", "main",
        "-f", "measure=true", "-f", f"caller={caller}",
    )  # fmt: skip
    if p.returncode != 0:
        return None, f"dispatch refused: {p.stderr.strip()}"
    print(f"dispatched {WORKFLOW} on {CAMERA_BOX}@main, caller={caller}")
    deadline = time.monotonic() + float(os.environ.get("GATE_FIND_TIMEOUT_S", "120"))
    while time.monotonic() < deadline:
        now = workflow_run_ids()
        new = {i: t for i, t in (now or {}).items() if i not in before}
        if new:
            return max(new, key=lambda i: (new[i], i)), ""
        time.sleep(0.05 + min(float(os.environ.get("GATE_POLL_S", "30")), 5))
    return None, "the dispatched run never appeared"


def wait_for_run(run_id: int, deadline_min: float) -> tuple[str | None, str]:
    """Return (conclusion, ""), or (None, why) when the run could not be followed."""
    deadline = time.monotonic() + deadline_min * 60
    poll = float(os.environ.get("GATE_POLL_S", "30"))
    failures = 0
    last = ""
    while True:
        p = gh("api", f"repos/{CAMERA_BOX}/actions/runs/{run_id}")
        if p.returncode != 0:
            failures += 1
            print(f"GitHub API failed ({failures} in a row): {p.stderr.strip()[-300:]}")
            if failures >= MAX_API_FAILURES:
                return None, f"{failures} GitHub API calls in a row failed"
        else:
            failures = 0
            run = json.loads(p.stdout)
            if run["status"] == "completed":
                return run["conclusion"], ""
            if run["status"] != last:
                last = run["status"]
                print(f"camera-box run {run_id}: {last}")
        if time.monotonic() >= deadline:
            return None, f"camera-box run {run_id} not finished after {deadline_min} min"
        time.sleep(poll)


def run_is_ours(run_id: int, caller: str) -> tuple[bool, str]:
    """A foreign dispatch must never answer for this release: the caller is in the log."""
    p = gh("run", "view", str(run_id), "-R", CAMERA_BOX, "--log")
    if p.returncode != 0:
        return False, f"could not read the run log: {p.stderr.strip()}"
    if re.search(rf"E2E_CALLER:\s*{re.escape(caller)}\s*$", p.stdout, re.M):
        return True, ""
    return False, f"run {run_id} does not carry caller {caller}"


def read_verdict(run_id: int, caller: str, conclusion: str, out: Path) -> Result:
    ours, why = run_is_ours(run_id, caller)
    if not ours:
        return unknown(why)
    p = gh("run", "download", str(run_id), "-R", CAMERA_BOX, "-n", ARTIFACT, "-D", str(out))
    if p.returncode != 0:
        return unknown(f"artifact {ARTIFACT} missing: {p.stderr.strip()}")
    return evaluate(out, conclusion)


def write_output(**values: object) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as f:
            for k, v in values.items():
                f.write(f"{k}={v}\n")


def finish(result: Result, url: str) -> int:
    write_output(verdict=result.state)
    if result.state == "pass":
        print(f"YouTube leg PASSED: {url}")
        return 0
    for reason in result.reasons:
        print(f"  {reason}")
    print(f"::error::YouTube leg {result.state}, release blocked: {url}")
    return 1


def cmd_measure(args: argparse.Namespace) -> int:
    run_id = args.run_id
    if run_id is None:
        run_id, why = dispatch_and_find(args.caller)
        if run_id is None:
            return finish(unknown(why), CAMERA_BOX)
    write_output(run_id=run_id)
    url = f"https://github.com/{CAMERA_BOX}/actions/runs/{run_id}"
    print(f"following {url}")
    conclusion, why = wait_for_run(run_id, args.deadline_min)
    if conclusion is None:
        return finish(unknown(why), url)
    print(f"camera-box run concluded: {conclusion}")
    return finish(read_verdict(run_id, args.caller, conclusion, Path(args.out)), url)


def cmd_token_check(_: argparse.Namespace) -> int:
    """A dispatch for a ref that does not exist: only a token with write gets as far as 422."""
    p = gh(
        "api", "-X", "POST",
        f"repos/{CAMERA_BOX}/actions/workflows/{WORKFLOW}/dispatches",
        "-f", f"ref={PROBE_REF}",
    )  # fmt: skip
    if p.returncode == 0:
        print(f"::error::dispatch of ref {PROBE_REF} was accepted: it was meant to be refused")
        return 1
    if "HTTP 422" in p.stderr and "No ref found" in p.stderr:
        print("CAMERA_BOX_ACTIONS_TOKEN may dispatch camera-box workflows")
        return 0
    print(f"::error::CAMERA_BOX_ACTIONS_TOKEN cannot dispatch: {p.stderr.strip()}")
    return 1


def cmd_release_check(args: argparse.Namespace) -> int:
    failed = False
    for name in RELEASE_CHECKS:
        query = name.replace(" ", "+")
        p = gh(
            "api",
            f"repos/{args.repo}/commits/{args.sha}/check-runs?per_page=100&check_name={query}",
            "--jq", ".check_runs",
        )  # fmt: skip
        if p.returncode != 0:
            print(f"::error::reading check {name} failed: {p.stderr.strip()}")
            failed = True
            continue
        runs = sorted(json.loads(p.stdout), key=lambda r: r.get("started_at") or "")
        latest = runs[-1] if runs else None
        if not latest or latest["status"] != "completed" or latest["conclusion"] != "success":
            state = f"{latest['status']}/{latest['conclusion']}" if latest else "never ran"
            print(f"::error::{name} on {args.sha}: {state}. A release needs it green.")
            failed = True
        else:
            print(f"{name} on {args.sha}: success")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--caller", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--deadline-min", type=float, default=225)
    m.add_argument("--run-id", type=int, help="attach to a run instead of dispatching")
    m.set_defaults(fn=cmd_measure)
    sub.add_parser("token-check").set_defaults(fn=cmd_token_check)
    r = sub.add_parser("release-check")
    r.add_argument("--repo", required=True)
    r.add_argument("--sha", required=True)
    r.set_defaults(fn=cmd_release_check)
    args = parser.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
