#!/usr/bin/env python3
"""#357: pin the wiring of the camera-box release gate.

A job's `if:` and `needs:` cannot be unit-tested outside a real run, and the gate job runs
only on the main push. So the structure is parsed here: nothing can release without the
measurement, and the measurement cannot fail open.

`--self-test` applies known-bad edits to a temp copy; each must go red for its own reason.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ".github/workflows/ci.yml"
RELEASE = ".github/workflows/release.yml"
GATE = "camera-box-release-gate"
EVIDENCE = "camera-box-red-evidence"
TOKEN_JOB = "camera-box" + "-token-check"
E2E_JOBS = ("e2e-streaming-test", "e2e-obs-youtube-test", "e2e-fb-push-stream-lan")
SECRET = "secrets.CAMERA_BOX_ACTIONS_TOKEN"


def text_of(job: dict) -> str:
    return "\n".join(str(s.get("run", "")) for s in job.get("steps", []))


def check_gate(jobs: dict) -> list[str]:
    errs = []
    gate = jobs.get(GATE)
    if not gate:
        return [f"{GATE} job is missing"]
    cond = str(gate.get("if", ""))
    if "github.ref == 'refs/heads/main'" not in cond or "github.event_name == 'push'" not in cond:
        errs.append(f"{GATE} must run on the main push only")
    if not cond.startswith("always() &&"):
        errs.append(f"{GATE} needs always(), or a skipped dependency skips it silently")
    for job in E2E_JOBS:
        # Dispatching before a stream-lan job ended would collide on the rig lease.
        if job not in gate.get("needs", []):
            errs.append(f"{GATE} must need {job}")
        if f"needs.{job}.result == 'success'" not in cond:
            errs.append(f"{GATE} must require {job} to succeed")
    if gate.get("runs-on") != "ubuntu-latest":
        errs.append(f"{GATE} must not hold the box while it waits")
    if "continue-on-error" in gate or any("continue-on-error" in s for s in gate["steps"]):
        errs.append(f"{GATE} must not use continue-on-error")
    # The script's 225 min deadline has to end before the job does.
    if int(gate.get("timeout-minutes", 0)) < 226:
        errs.append(f"{GATE} timeout must exceed the script's deadline")
    measure = next((s for s in gate["steps"] if s.get("id") == "measure"), None)
    if not measure or "camera_box_gate.py measure" not in str(measure.get("run")):
        errs.append(f"{GATE} must run the measure step")
    elif SECRET not in str(measure.get("env", {}).get("GH_TOKEN")):
        errs.append(f"{GATE} must dispatch with {SECRET}")
    if "steps.measure.outputs.verdict" not in str(gate.get("outputs", {}).get("verdict")):
        errs.append(f"{GATE} must expose the verdict")
    run = text_of(gate)
    if "|| true" in run:
        errs.append(f"{GATE} must not swallow a failure")
    return errs


def check_release_path(jobs: dict, release: dict) -> list[str]:
    errs = []
    e2e = jobs["e2e-gate"]
    if GATE not in e2e.get("needs", []):
        errs.append(f"e2e-gate must need {GATE}")
    if f'needs.{GATE}.result }}}}" != "success"' not in text_of(e2e):
        errs.append(f"e2e-gate must fail unless {GATE} succeeded")
    if "e2e-gate" not in jobs["auto-release"].get("needs", []):
        errs.append("auto-release must need e2e-gate")
    validate = release["jobs"]["validate"]
    if "camera_box_gate.py release-check" not in text_of(validate):
        errs.append("release.yml validate must run release-check")
    if validate.get("permissions", {}).get("checks") != "read":
        errs.append("release.yml validate needs checks: read")
    for job in ("build-delivery",):
        needs = release["jobs"][job].get("needs", [])
        if "validate" not in ([needs] if isinstance(needs, str) else needs):
            errs.append(f"release.yml {job} must need validate")
    return errs


def check_support(jobs: dict) -> list[str]:
    errs = []
    token = jobs.get(TOKEN_JOB)
    if not token or "camera_box_gate.py token-check" not in text_of(token):
        errs.append(f"{TOKEN_JOB} must run token-check")
    if TOKEN_JOB not in jobs["rust-ci-gate"].get("needs", []):
        errs.append(f"rust-ci-gate must need {TOKEN_JOB}")
    if f"{TOKEN_JOB}:${{{{ needs.{TOKEN_JOB}.result }}}}" not in text_of(jobs["rust-ci-gate"]):
        errs.append(f"rust-ci-gate must require {TOKEN_JOB} to succeed")
    if "test_camera_box_gate.py" not in text_of(jobs["obs-scripts-test"]):
        errs.append("obs-scripts-test must run test_camera_box_gate.py")
    evidence = jobs.get(EVIDENCE, {})
    if "outputs.verdict == 'red'" not in str(evidence.get("if")):
        errs.append(f"{EVIDENCE} must run on a red verdict")
    if not str(evidence.get("if")).startswith("always()"):
        errs.append(f"{EVIDENCE} needs always(): it follows a failed job")
    return errs


def verify(root: Path) -> list[str]:
    ci = yaml.safe_load((root / CI).read_text())
    release = yaml.safe_load((root / RELEASE).read_text())
    jobs = ci["jobs"]
    return check_gate(jobs) + check_release_path(jobs, release) + check_support(jobs)


MUTATIONS = [
    (CI, "camera-box-release-gate:\n    name:", "camera-box-gate-renamed:\n    name:", "job is missing"),
    (CI, "needs: [e2e-streaming-test, e2e-obs-youtube-test, e2e-fb-push-stream-lan]\n    if: always() && github.ref",
     "needs: [e2e-streaming-test, e2e-obs-youtube-test]\n    if: always() && github.ref", "must need e2e-fb-push"),
    (CI, "github.event_name == 'push' && needs.e2e-streaming-test.result == 'success'",
     "needs.e2e-streaming-test.result == 'success'", "main push only"),
    (CI, "needs.e2e-fb-push-stream-lan.result == 'success'\n    runs-on: ubuntu-latest\n    # camera-box:",
     "needs.e2e-fb-push-stream-lan.result != 'failure'\n    runs-on: ubuntu-latest\n    # camera-box:", "to succeed"),
    (CI, "    if: always() && github.ref == 'refs/heads/main' && github.event_name == 'push' && needs.e2e-streaming-test",
     "    if: github.ref == 'refs/heads/main' && github.event_name == 'push' && needs.e2e-streaming-test", "always()"),
    (CI, "    timeout-minutes: 235", "    timeout-minutes: 120", "timeout"),
    (CI, "        id: measure\n", "        id: measure\n        continue-on-error: true\n", "continue-on-error"),
    (CI, "--out \"$RUNNER_TEMP/measurement\"", "--out \"$RUNNER_TEMP/measurement\" || true", "swallow"),
    (CI, "camera-box-release-gate]\n    runs-on: ubuntu-latest\n    steps:\n      - name: Verify all checks",
     "e2e-fb-push-stream-lan]\n    runs-on: ubuntu-latest\n    steps:\n      - name: Verify all checks", "e2e-gate must need"),
    (CI, '"${{ needs.camera-box-release-gate.result }}" != "success"', '"${{ needs.camera-box-release-gate.result }}" == "failure"', "fail unless"),
    (CI, "    needs: [e2e-gate]\n", "    needs: [rust-ci-gate]\n", "auto-release must need"),
    (CI, "        camera-box-token-check,\n", "", "rust-ci-gate must need"),
    (CI, '"camera-box-token-check:${{ needs.camera-box-token-check.result }}"; do', '"x:success"; do', "must require camera-box-token-check"),
    (CI, "run: python tests/ci/test_camera_box_gate.py", "run: echo", "test_camera_box_gate"),
    (CI, "if: always() && needs.camera-box-release-gate.outputs.verdict == 'red'",
     "if: needs.camera-box-release-gate.outputs.verdict == 'red'", "always()"),
    (RELEASE, "release-check --repo", "--help #", "release-check"),
    (RELEASE, "      checks: read\n", "", "checks: read"),
]


def self_test() -> int:
    bad = 0
    for path, old, new, want in MUTATIONS:
        with tempfile.TemporaryDirectory() as t:
            root = Path(t)
            for f in (CI, RELEASE):
                (root / f).parent.mkdir(parents=True, exist_ok=True)
                (root / f).write_text((ROOT / f).read_text())
            source = (root / path).read_text()
            if old not in source:
                print(f"FAIL: mutation no longer applies: {old[:70]!r}")
                bad += 1
                continue
            (root / path).write_text(source.replace(old, new))
            errs = verify(root)
            if not any(want in e for e in errs):
                print(f"FAIL: {old[:60]!r} -> wanted {want!r}, got {errs}")
                bad += 1
    print(f"self-test: {len(MUTATIONS)} mutations, {bad} failed")
    return 1 if bad else 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    errs = verify(ROOT)
    for e in errs:
        print(f"ERROR: {e}")
    if errs:
        return 1
    print("camera-box release gate wiring OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
