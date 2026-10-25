#!/usr/bin/env python3
"""#349: run scripts/ci/rig-lease-wait.ps1 against a mock camera-box rig-lease endpoint.

An OBS-streaming E2E job checks the lease twice: once after checkout and once right
before StartStream. Both checks share ONE wait budget per job (main run 38530980091,
2026-10-14: the first check saw the rig free, camera-box took it a minute later, the
second check had its own 10-minute budget, gave up while camera-box still held the rig
for 30 more minutes, and Start then refused to touch OBS). So:

  * the first check records the job's deadline in GITHUB_ENV;
  * a later check waits for the holder, but never past that deadline;
  * the deadline bounds only the wait: a free rig is used at any time.

`pwsh` on Linux -- what the stream boxes run since 2026-10 (#417); Windows
PowerShell 5.1 (`powershell`) on Windows; or the interpreter named by $OBS_TEST_PWSH.
"""

from __future__ import annotations

import http.server
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ci" / "rig-lease-wait.ps1"
DEADLINE = "RIG_LEASE_DEADLINE_EPOCH"
HOLDER = {"job": "full-path", "run_url": "https://github.com/zbynekdrlik/camera-box/actions/runs/1"}
FREE = {"schema": 1, "held": False}


def held(ttl_s: int) -> dict:
    return {"schema": 1, "held": True, "stale": False, "heartbeat_age_s": 1, "ttl_s": ttl_s, "holder": HOLDER}


class MockLease:
    """Serves `held` until `free_after_s` seconds have passed, then FREE (never, when None)."""

    def __init__(self, body: dict, free_after_s: float | None) -> None:
        start = time.monotonic()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (http.server API)
                freed = free_after_s is not None and time.monotonic() - start >= free_after_s
                payload = json.dumps(FREE if freed else body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args) -> None:
                return

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/rig-lease.json"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()


@dataclass
class Case:
    name: str
    lease: dict
    expect_out: str
    max_s: float                       # the run must finish within this (a wait past the deadline fails)
    min_s: float = 0.0                 # ... and not before this (the holder was waited for)
    free_after_s: float | None = None
    env: dict = field(default_factory=dict)
    inherited_deadline_in_s: float | None = None   # a deadline an earlier check of the job recorded
    expect_recorded_in_s: float | None = None      # the deadline this run must record (None: records nothing)


CASES = [
    Case("first check, rig free: proceeds and records the job deadline", FREE, "rig FREE", 15,
         env={"RIG_LEASE_BUDGET_S": "3600"}, expect_recorded_in_s=3600),
    Case("second check, rig held: waits until the job deadline, not its own budget", held(1800),
         "wait budget exhausted", 25, min_s=3, inherited_deadline_in_s=4),
    Case("second check, holder done before the deadline: proceeds", held(1800), "rig FREE", 25, min_s=2,
         free_after_s=3, inherited_deadline_in_s=60, env={"RIG_LEASE_POLL_S": "1"}),
    Case("second check, deadline already past, rig free: proceeds", FREE, "rig FREE", 15,
         inherited_deadline_in_s=-30),
    Case("second check, deadline already past, rig held: proceeds at once", held(1800),
         "wait budget exhausted", 15, inherited_deadline_in_s=-30),
    Case("first check, rig held: waits only its own budget", held(1800), "wait budget exhausted", 25, min_s=3,
         env={"RIG_LEASE_BUDGET_S": "4"}, expect_recorded_in_s=4),
]


def interpreter() -> list[str]:
    if os.environ.get("OBS_TEST_PWSH"):
        return [os.environ["OBS_TEST_PWSH"]]
    return ["powershell"] if os.name == "nt" else ["pwsh"]


def recorded(path: Path) -> list[float]:
    out = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.lstrip("﻿").strip()
        if line.startswith(f"{DEADLINE}="):
            out.append(float(line.split("=", 1)[1]))
    return out


def run_case(case: Case) -> list[str]:
    problems: list[str] = []
    lease = MockLease(case.lease, case.free_after_s)
    try:
        with tempfile.TemporaryDirectory() as tmp_s:
            env_file = Path(tmp_s) / "github_env"
            env_file.write_text("", encoding="utf-8")
            env = {k: v for k, v in os.environ.items() if not k.startswith("RIG_LEASE_")}
            env.update({"RIG_LEASE_URL": lease.url, "RIG_LEASE_POLL_S": "1", "GITHUB_ENV": str(env_file)})
            env.update(case.env)
            if case.inherited_deadline_in_s is not None:
                env[DEADLINE] = str(int(time.time() + case.inherited_deadline_in_s))
            started = time.time()
            try:
                proc = subprocess.run(
                    [*interpreter(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(SCRIPT)],
                    env=env, capture_output=True, text=True, timeout=case.max_s)
            except subprocess.TimeoutExpired as e:
                out = (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
                return [f"still waiting after {case.max_s}s", "output:\n    " + out.strip().replace("\n", "\n    ")]
            elapsed = time.time() - started
            out = proc.stdout + proc.stderr
            if proc.returncode != 0:
                problems.append(f"exit {proc.returncode}, want 0 (the wait never fails the job)")
            if case.expect_out not in out:
                problems.append(f"output lacks {case.expect_out!r}")
            if elapsed < case.min_s:
                problems.append(f"returned after {elapsed:.1f}s, before the holder was waited for ({case.min_s}s)")
            got = recorded(env_file)
            if case.expect_recorded_in_s is None:
                if got:
                    problems.append(f"recorded {DEADLINE} again ({got}); the job's first check owns it")
            elif len(got) != 1:
                problems.append(f"recorded {DEADLINE} {len(got)} times, want once")
            elif abs(got[0] - (started + case.expect_recorded_in_s)) > 10:
                problems.append(f"recorded deadline {got[0] - started:.0f}s ahead, want {case.expect_recorded_in_s}s")
            if problems:
                problems.append("output:\n    " + out.strip().replace("\n", "\n    "))
    finally:
        lease.close()
    return problems


def main() -> int:
    failed = 0
    for case in CASES:
        problems = run_case(case)
        if problems:
            failed += 1
            print(f"FAIL {case.name}")
            for p in problems:
                print(f"  - {p}")
        else:
            print(f"ok   {case.name}")
    print(f"{len(CASES) - failed}/{len(CASES)} scenarios passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
