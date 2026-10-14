#!/usr/bin/env python3
"""#349: run scripts/ci/rig-lease-hold.ps1 against a mock of camera-box's dev1 rig-lease
write API (contract: restreamer #349 comments 6179376408 + 6179710985).

An OBS-streaming job TAKES the shared lease right before it starts OBS streaming, beats
every 30 s while it streams, and releases it in an always() step:

  * acquire 200: the lease id is kept, RIG_LEASE_HELD=true goes to GITHUB_ENV, and a
    detached heartbeat runs until Release;
  * acquire 409: retry until the job's shared deadline, then fail with the holder;
  * a heartbeat 404 (the lease was reclaimed) fails the job at Release;
  * no token or an unreachable server: fail-open (warning, lease not taken);
  * a 400/401 is our own bug and fails the job.

Windows PowerShell 5.1 (`powershell`) on Windows -- what the stream box runs;
`pwsh` elsewhere, or the interpreter named by $OBS_TEST_PWSH.
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
SCRIPT = ROOT / "scripts" / "ci" / "rig-lease-hold.ps1"
TOKEN = "test-token-349"
CAMBOX = {"repo": "camera-box", "run_id": "1", "run_url": "https://github.com/zbynekdrlik/camera-box/actions/runs/1",
          "job": "full-path", "acquired_at": "x", "expected_release_at": "y"}


class MockLeaseApi:
    """The dev1 write routes. `held_for_s`: camera-box holds the rig that long (None:
    forever, 0: free). `lose_after_beats`: the heartbeat answers 404 from that beat on.
    `status`: force every answer to this code (401 etc.)."""

    def __init__(self, held_for_s: float | None = 0, lose_after_beats: int | None = None,
                 status: int | None = None) -> None:
        self.start = time.monotonic()
        self.requests: list[tuple[str, dict, str]] = []
        self.beats = 0
        api = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 (http.server API)
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                body = json.loads(raw or b"{}")
                api.requests.append((self.path, body, self.headers.get("Authorization", "")))
                code, out = api.answer(self.path, body, self.headers.get("Authorization", ""))
                payload = json.dumps(out).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args) -> None:
                return

        self.held_for_s, self.lose_after_beats, self.status = held_for_s, lose_after_beats, status
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def answer(self, path: str, body: dict, auth: str) -> tuple[int, dict]:
        if self.status is not None:
            return self.status, {"error": "forced"}
        if auth != f"Bearer {TOKEN}":
            return 401, {"error": "unauthorized"}
        if path == "/rig-lease/acquire":
            held = self.held_for_s is None or time.monotonic() - self.start < self.held_for_s
            if held:
                return 409, {"acquired": False, "holder": CAMBOX, "heartbeat_age_s": 3}
            return 200, {"acquired": True, "lease_id": "L-1", "holder": body, "expected_release_at": "Z",
                         "reclaimed": False}
        if path == "/rig-lease/heartbeat":
            self.beats += 1
            if self.lose_after_beats is not None and self.beats >= self.lose_after_beats:
                return 404, {"error": "not_holder"}
            return 200, {"ok": True, "expected_release_at": "Z"}
        if path == "/rig-lease/release":
            return 200, {"released": True}
        return 404, {"error": "no route"}

    def paths(self) -> list[str]:
        return [p for p, _, _ in self.requests]

    def close(self) -> None:
        self.server.shutdown()


def interpreter() -> list[str]:
    if os.environ.get("OBS_TEST_PWSH"):
        return [os.environ["OBS_TEST_PWSH"]]
    return ["powershell"] if os.name == "nt" else ["pwsh"]


def run(action: str, env: dict, timeout: float = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*interpreter(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(SCRIPT), "-Action", action],
        env=env, capture_output=True, text=True, timeout=timeout)


def job_env(tmp: Path, api: MockLeaseApi | None, **extra: str) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("RIG_LEASE_")}
    (tmp / "github_env").write_text("", encoding="utf-8")
    env.update({
        "RUNNER_TEMP": str(tmp), "GITHUB_ENV": str(tmp / "github_env"),
        "GITHUB_RUN_ID": "4242", "GITHUB_JOB": "e2e-fb-push-stream-lan",
        "GITHUB_SERVER_URL": "https://github.com", "GITHUB_REPOSITORY": "zbynekdrlik/restreamer",
        "RIG_LEASE_API": api.base if api else "http://127.0.0.1:9", "RIG_LEASE_TOKEN": TOKEN,
        "RIG_LEASE_POLL_S": "1", "RIG_LEASE_BEAT_S": "1",
        "RIG_LEASE_DEADLINE_EPOCH": str(int(time.time() + 60)),
    })
    env.update(extra)
    return env


def held_flag(tmp: Path) -> bool:
    return "RIG_LEASE_HELD=true" in (tmp / "github_env").read_text(encoding="utf-8-sig")


@dataclass
class Result:
    problems: list[str] = field(default_factory=list)

    def check(self, ok: bool, what: str, proc: subprocess.CompletedProcess | None = None) -> None:
        if not ok:
            out = (proc.stdout + proc.stderr).strip().replace("\n", "\n    ") if proc else ""
            self.problems.append(what + (f"\n    output:\n    {out}" if out else ""))


def case_free_acquire_beat_release(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=0)
    try:
        env = job_env(tmp, api)
        p = run("Acquire", env)
        r.check(p.returncode == 0, f"acquire exit {p.returncode}, want 0", p)
        r.check(held_flag(tmp), "RIG_LEASE_HELD=true not written to GITHUB_ENV", p)
        acq = [b for path, b, _ in api.requests if path == "/rig-lease/acquire"]
        r.check(len(acq) == 1, f"{len(acq)} acquire requests, want 1")
        if acq:
            want = {"repo": "restreamer", "run_id": "4242", "job": "e2e-fb-push-stream-lan",
                    "run_url": "https://github.com/zbynekdrlik/restreamer/actions/runs/4242", "expected_secs": 5400}
            r.check(acq[0] == want, f"acquire body {acq[0]}, want {want}")
        r.check(all(a == f"Bearer {TOKEN}" for _, _, a in api.requests), "a request without the bearer token")
        # The detached heartbeat beats while the job streams (other steps run meanwhile).
        deadline = time.time() + 10
        while api.beats < 2 and time.time() < deadline:
            time.sleep(0.2)
        r.check(api.beats >= 2, f"{api.beats} heartbeats after the acquire, want >= 2")
        beat_body = [b for path, b, _ in api.requests if path == "/rig-lease/heartbeat"]
        r.check(bool(beat_body) and beat_body[0] == {"lease_id": "L-1"}, f"heartbeat body {beat_body[:1]}")
        p = run("Release", env)
        r.check(p.returncode == 0, f"release exit {p.returncode}, want 0", p)
        rel = [b for path, b, _ in api.requests if path == "/rig-lease/release"]
        r.check(rel == [{"lease_id": "L-1"}], f"release requests {rel}, want one with lease L-1")
        beats_at_release = api.beats
        time.sleep(2.5)
        r.check(api.beats == beats_at_release, "the heartbeat kept beating after the release")
        p = run("Release", env)
        r.check(p.returncode == 0 and "no lease held" in p.stdout, "a second release is not a no-op", p)
    finally:
        api.close()
    return r.problems


def case_waits_for_camera_box_then_acquires(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=3)
    try:
        env = job_env(tmp, api)
        t0 = time.time()
        p = run("Acquire", env)
        r.check(p.returncode == 0, f"acquire exit {p.returncode}, want 0", p)
        r.check(time.time() - t0 >= 2, "acquired before camera-box let go")
        r.check("camera-box full-path" in p.stdout, "the wait does not name the holder", p)
        r.check(held_flag(tmp), "RIG_LEASE_HELD=true not written", p)
        run("Release", env)
    finally:
        api.close()
    return r.problems


def case_budget_spent_fails_with_holder(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=None)
    try:
        env = job_env(tmp, api, RIG_LEASE_DEADLINE_EPOCH=str(int(time.time() - 30)))
        p = run("Acquire", env)
        r.check(p.returncode == 1, f"exit {p.returncode}, want 1 (the rig stays camera-box's)", p)
        r.check("camera-box full-path" in p.stdout and "budget" in p.stdout, "no holder/budget in the error", p)
        r.check(not held_flag(tmp), "RIG_LEASE_HELD written without a lease", p)
        r.check(api.paths() == ["/rig-lease/acquire"], f"requests {api.paths()}, want one acquire")
    finally:
        api.close()
    return r.problems


def case_no_token_is_fail_open(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=0)
    try:
        env = job_env(tmp, api, RIG_LEASE_TOKEN="")
        p = run("Acquire", env)
        r.check(p.returncode == 0 and "RIG_LEASE_TOKEN is not set" in p.stdout, "no token is not fail-open", p)
        r.check(api.requests == [], "a request went out without a token")
        r.check(not held_flag(tmp), "RIG_LEASE_HELD written without a lease", p)
    finally:
        api.close()
    return r.problems


def case_unreachable_is_fail_open(tmp: Path) -> list[str]:
    r = Result()
    env = job_env(tmp, None)
    p = run("Acquire", env)
    r.check(p.returncode == 0 and "unreachable" in p.stdout, "an unreachable server is not fail-open", p)
    r.check(not held_flag(tmp), "RIG_LEASE_HELD written without a lease", p)
    return r.problems


def case_unauthorized_fails(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=0, status=401)
    try:
        p = run("Acquire", job_env(tmp, api))
        r.check(p.returncode == 1 and "401" in p.stdout, "a 401 does not fail the job", p)
    finally:
        api.close()
    return r.problems


def case_lost_lease_fails_at_release(tmp: Path) -> list[str]:
    r, api = Result(), MockLeaseApi(held_for_s=0, lose_after_beats=1)
    try:
        env = job_env(tmp, api)
        p = run("Acquire", env)
        r.check(p.returncode == 0, f"acquire exit {p.returncode}", p)
        deadline = time.time() + 10
        while api.beats < 1 and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(1)
        p = run("Release", env)
        r.check(p.returncode == 1 and "lost" in p.stdout, "a reclaimed lease does not fail the job", p)
    finally:
        api.close()
    return r.problems


CASES = [
    ("free rig: acquire, heartbeat while streaming, release once", case_free_acquire_beat_release),
    ("camera-box holds the rig: wait inside the budget, then acquire", case_waits_for_camera_box_then_acquires),
    ("camera-box holds the rig past the budget: fail, naming the holder", case_budget_spent_fails_with_holder),
    ("no token: fail-open, no request", case_no_token_is_fail_open),
    ("server unreachable: fail-open", case_unreachable_is_fail_open),
    ("401: our bug, the job fails", case_unauthorized_fails),
    ("heartbeat 404 (lease reclaimed): Release fails the job", case_lost_lease_fails_at_release),
]


def main() -> int:
    failed = 0
    for name, case in CASES:
        with tempfile.TemporaryDirectory() as tmp_s:
            try:
                problems = case(Path(tmp_s))
            except subprocess.TimeoutExpired as e:
                problems = [f"timed out after {e.timeout}s"]
        if problems:
            failed += 1
            print(f"FAIL {name}")
            for p in problems:
                print(f"  - {p}")
        else:
            print(f"ok   {name}")
    print(f"{len(CASES) - failed}/{len(CASES)} scenarios passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
