#!/usr/bin/env python3
"""#349 / #417: scripts/ci/job-owner.ps1 finds the job's Runner.Worker on Linux.

The rig-lease heartbeat and the program-audio watchdog are detached helpers that
end with their job: they record the job's Runner.Worker (pid + start ticks) and
exit once it is gone. On the Linux stream box that process is found by walking
/proc/<pid>/stat parents (Find-JobOwnerProcessLinux). The other tests name the
owner with CI_JOB_OWNER_PID, which skips the walk, so this one runs it for real:
pwsh is started under a process whose comm is `Runner.Worker` (a symlink of that
name to bash; comm is the basename of the path given to execve).

Linux only, with `pwsh` or $OBS_TEST_PWSH. Exit 0 when every case passes.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOB_OWNER = ROOT / "scripts" / "ci" / "job-owner.ps1"


def pwsh() -> str:
    exe = os.environ.get("OBS_TEST_PWSH") or shutil.which("pwsh")
    if not exe:
        raise SystemExit("FAIL: pwsh not found (set OBS_TEST_PWSH)")
    return exe


def link(tmp: Path, name: str) -> Path:
    path = tmp / name
    if not path.exists():
        path.symlink_to(shutil.which("bash") or "/bin/bash")
    return path


def run(tmp: Path, record: Path, middle: bool) -> tuple[str, int]:
    """pwsh records its job owner, started by a bash named Runner.Worker, with
    another bash in between when `middle` (the walk must go up two levels).
    Returns the output and the fake Runner.Worker's pid."""
    script = (
        f". '{JOB_OWNER}'; "
        f"Write-Host (Write-JobOwnerRecord '{record}'); "
        f"$r = Read-JobOwnerRecord '{record}'; "
        "if ($r) { Write-Host \"owner-pid=$($r.Id)\" }"
    )
    env = {k: v for k, v in os.environ.items() if k != "CI_JOB_OWNER_PID"}
    # `; true` keeps each bash as the parent (no exec of its last command).
    inner = f'"$0" -NoProfile -NonInteractive -Command "$1"; true'
    if middle:
        args = [str(link(tmp, "Runner.Worker")), "-c", f'"$0" -c \'{inner}\' "$1" "$2"; true',
                str(link(tmp, "step-shell")), pwsh(), script]
    else:
        args = [str(link(tmp, "Runner.Worker")), "-c", inner, pwsh(), script]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    out, _ = proc.communicate(timeout=120)
    return out, proc.pid


def main() -> int:
    if sys.platform != "linux":
        print("FAIL: this test covers the Linux /proc walk and must run on Linux")
        return 1
    failures = []
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for middle, what in [(False, "the parent"), (True, "the grandparent")]:
            out, worker_pid = run(tmp, tmp / f"owner-{middle}", middle)
            if f"owner-pid={worker_pid}" not in out or "bound to its job's Runner.Worker" not in out:
                failures.append(f"Runner.Worker as {what} (pid {worker_pid}): expected that pid as owner, got:\n{out}")
            else:
                print(f"ok   Runner.Worker as {what}: owner is its pid {worker_pid}")
    for f in failures:
        print(f"FAIL {f}")
    if failures:
        return 1
    print("2/2 job-owner scenarios passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
