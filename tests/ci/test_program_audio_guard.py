#!/usr/bin/env python3
"""#379: run scripts/ci/program-audio-guard.ps1 against a mock camera-box sampler.

The guard keeps room/FOH music off YouTube/Facebook: it reads camera-box's verdict
(GET http://dev1:8890/program-audio.json) before every StartStream and, through a
detached watchdog, every 10 s while CI streams; on a breach the watchdog stops OBS
streaming through Restreamer's API (POST /api/v1/obs/stop-stream). Both endpoints
are external services, so this test serves stdlib mocks of them and runs the REAL
PowerShell functions:

  * Test-ProgramAudio on every verdict shape: MEASUREMENT, SILENT, FOREIGN, UNKNOWN,
    a recent FOREIGN between polls, stale, unreachable, HTTP 500 and malformed bodies;
  * the watchdog: quiet while the program is clean, and on FOREIGN / an unreachable
    sampler it writes the breach marker, calls the stop endpoint and re-asks until
    Restreamer's obs status CONFIRMS OBS stopped (the POST only queues it), then
    exits; Assert-NoProgramAudioBreach then fails with the reason in the job summary;
    a dead or hung watchdog fails the assert too; music while the stream is not ours
    is never stopped (#374); -BeforeStart refuses after a breach or a dead watchdog.

Every step runs the way GitHub Actions runs `shell: powershell` (the 'stop'
prelude, `-command ". '<file>'"`), so `exit 1` inside a function behaves as in CI.

Windows PowerShell 5.1 (`powershell`) on Windows -- what the stream box runs;
`pwsh` elsewhere, or the interpreter named by $OBS_TEST_PWSH.
"""

from __future__ import annotations

import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "ci" / "program-audio-guard.ps1"


def sample(verdict: str = "MEASUREMENT", age: object = 0.6, **extra: object) -> dict:
    body = {"schema": 1, "ts_utc": "2026-10-07T10:00:00.000Z", "age_s": age, "verdict": verdict,
            "rms_dbfs": -35.8, "outside_band_pct": 13.1, "window_s": 2.0, "source": "STREAM-SNV (stream)",
            "last_foreign_ts_utc": None, "last_foreign_age_s": None}
    body.update(extra)
    return body


@dataclass
class Reply:
    status: int = 200
    body: bytes = b""


def json_reply(obj: object, status: int = 200) -> Reply:
    return Reply(status, json.dumps(obj).encode())


@dataclass
class MockState:
    sampler: Reply = field(default_factory=lambda: json_reply(sample()))
    sampler_fail_next: int = 0           # the next N reads answer HTTP 500 (a blip)
    stop_status: list[int] = field(default_factory=lambda: [200])  # per call; the last repeats
    stop_takes_effect_after: int = 1     # accepted (2xx) stops before OBS really stops
    obs_streaming: bool = True           # what Restreamer's GET /api/v1/obs/status reports
    obs_connected: bool = True
    gets: int = 0
    stops: list[float] = field(default_factory=list)
    accepted_stops: int = 0


class MockServer:
    """Serves the sampler and Restreamer's obs status (GET) + stop-stream (POST) on one port."""

    def __init__(self, state: MockState) -> None:
        self.state = state
        st = state

        class Handler(http.server.BaseHTTPRequestHandler):
            def _send(self, reply: Reply) -> None:
                self.send_response(reply.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(reply.body)))
                self.end_headers()
                self.wfile.write(reply.body)

            def do_GET(self) -> None:  # noqa: N802 (http.server API)
                if self.path == "/api/v1/obs/status":
                    self._send(json_reply({"connected": st.obs_connected, "streaming": st.obs_streaming,
                                           "recording": False, "stream_timecode": None}))
                    return
                st.gets += 1
                if st.sampler_fail_next > 0:
                    st.sampler_fail_next -= 1
                    self._send(Reply(500, b"boom"))
                    return
                self._send(st.sampler)

            def do_POST(self) -> None:  # noqa: N802
                n = int(self.headers.get("Content-Length") or 0)
                if n:
                    self.rfile.read(n)
                st.stops.append(time.time())
                code = st.stop_status[min(len(st.stops) - 1, len(st.stop_status) - 1)]
                if code < 300:
                    # Restreamer only QUEUES the command; OBS stops when it gets to it.
                    st.accepted_stops += 1
                    if st.accepted_stops >= st.stop_takes_effect_after:
                        st.obs_streaming = False
                self._send(Reply(code, b""))

            def log_message(self, *args) -> None:
                return

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = self.server.server_address[1]
        self.sampler_url = f"http://127.0.0.1:{port}/program-audio.json"
        self.stop_url = f"http://127.0.0.1:{port}/api/v1/obs/stop-stream"
        self.status_url = f"http://127.0.0.1:{port}/api/v1/obs/status"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def interpreter() -> list[str]:
    if os.environ.get("OBS_TEST_PWSH"):
        return [os.environ["OBS_TEST_PWSH"]]
    return ["powershell"] if os.name == "nt" else ["pwsh"]


class Env:
    """One job: its own RUNNER_TEMP, step summary and knobs."""

    def __init__(self, tmp: Path, sampler_url: str, stop_url: str, status_url: str) -> None:
        self.tmp = tmp
        self.summary = tmp / "step_summary.md"
        self.summary.write_text("", encoding="utf-8")
        self.env = dict(os.environ)
        self.env.update({
            "RUNNER_TEMP": str(tmp),
            "GITHUB_STEP_SUMMARY": str(self.summary),
            "PROGRAM_AUDIO_URL": sampler_url,
            "PROGRAM_AUDIO_STOP_URL": stop_url,
            "PROGRAM_AUDIO_OBS_STATUS_URL": status_url,
            "PROGRAM_AUDIO_POLL_S": "1",
        })

    def run(self, body: str, timeout: int = 90) -> subprocess.CompletedProcess:
        """A ci.yml step, the way GitHub Actions runs `shell: powershell`: the run block
        in a .ps1 with its `$ErrorActionPreference = 'stop'` prelude and LASTEXITCODE
        epilogue, invoked as `-command ". '<file>'"`. The block dot-sources the guard."""
        script = self.tmp / f"step-{time.time_ns()}.ps1"
        script.write_text(f"$ErrorActionPreference = 'stop'\n. '{GUARD}'\n{body}\n"
                          "if ((Test-Path -LiteralPath variable:\\LASTEXITCODE)) { exit $LASTEXITCODE }\n",
                          encoding="utf-8")
        return subprocess.run(interpreter() + ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Unrestricted",
                                               "-Command", f". '{script}'"],
                              capture_output=True, text=True, env=self.env, timeout=timeout)

    def path(self, name: str) -> Path:
        return self.tmp / name

    def watchdog_pid(self) -> int | None:
        p = self.path("program-audio-watchdog.pid")
        if not p.exists():
            return None
        return int(p.read_text(encoding="ascii").split()[0])

    def kill_watchdog(self) -> None:
        pid = self.watchdog_pid()
        if pid and alive(pid):
            os.kill(pid, signal.SIGTERM)


def alive(pid: int) -> bool:
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    # a zombie (exited, not yet reaped by its parent) is not alive
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


def wait_for(pred, seconds: float) -> bool:
    end = time.time() + seconds
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.2)
    return pred()


# ------------------------------------------------------- Test-ProgramAudio --

PROBE = 'if ($why = Test-ProgramAudio) { Write-Host "WHY=$why"; exit 1 }\nWrite-Host "VERDICT-OK"; exit 0'

VERDICT_CASES: list[tuple[str, Reply | None, int, str]] = [
    ("MEASUREMENT, fresh -> ok", json_reply(sample()), 0, "VERDICT-OK"),
    ("SILENT, fresh -> ok", json_reply(sample("SILENT", 3)), 0, "VERDICT-OK"),
    ("age_s exactly at the limit (10) -> ok", json_reply(sample(age=10)), 0, "VERDICT-OK"),
    ("an old FOREIGN (300 s ago) -> ok", json_reply(sample(last_foreign_age_s=300.0)), 0, "VERDICT-OK"),
    ("FOREIGN -> fail", json_reply(sample("FOREIGN")), 1, "WHY=FOREIGN: program audio is not the measurement"),
    ("UNKNOWN -> fail", json_reply(sample("UNKNOWN")), 1, "WHY=UNKNOWN:"),
    ("an unexpected verdict -> fail", json_reply(sample("POLLUTED")), 1, "WHY=POLLUTED:"),
    ("MEASUREMENT but FOREIGN 4 s ago (between polls) -> fail",
     json_reply(sample(last_foreign_age_s=4.0)), 1, "WHY=FOREIGN: foreign audio 4s ago"),
    ("stale sample (age_s 12) -> fail", json_reply(sample(age=12.5)), 1, "WHY=stale: sample is 12.5s old"),
    ("sampler unreachable -> fail closed", None, 1, "WHY=unreachable:"),
    ("HTTP 500 -> fail closed", Reply(500, b"oops"), 1, "WHY=unreachable:"),
    ("not JSON -> malformed", Reply(200, b"<html>hi</html>"), 1, "WHY=malformed: "),
    ("JSON array -> malformed", json_reply([sample()]), 1, "WHY=malformed: "),
    ("JSON string -> malformed", json_reply("MEASUREMENT"), 1, "WHY=malformed: "),
    ("schema 2 -> malformed", json_reply(dict(sample(), schema=2)), 1, "WHY=malformed: "),
    ("no verdict -> malformed", json_reply({k: v for k, v in sample().items() if k != "verdict"}), 1,
     "WHY=malformed: "),
    ("no age_s -> malformed", json_reply({k: v for k, v in sample().items() if k != "age_s"}), 1,
     "WHY=malformed: "),
    ("age_s not a number -> malformed", json_reply(sample(age="fresh")), 1, "WHY=malformed: "),
    ("age_s negative -> malformed", json_reply(sample(age=-3)), 1, "WHY=malformed: "),
    ("age_s a JSON true -> malformed", json_reply(sample(age=True)), 1, "WHY=malformed: "),
]


def verdict_case(reply: Reply | None, expect_exit: int, expect_text: str) -> list[str]:
    state = MockState(sampler=reply or Reply())
    srv = MockServer(state)
    try:
        with tempfile.TemporaryDirectory() as tmp_s:
            url = srv.sampler_url if reply is not None else f"http://127.0.0.1:{closed_port()}/program-audio.json"
            job = Env(Path(tmp_s), url, srv.stop_url, srv.status_url)
            p = job.run(PROBE)
    finally:
        srv.close()
    out = p.stdout + p.stderr
    problems = []
    if p.returncode != expect_exit:
        problems.append(f"exit {p.returncode}, expected {expect_exit}")
    if expect_text not in out:
        problems.append(f"output lacks {expect_text!r}")
    if state.stops:
        problems.append("Test-ProgramAudio called the stop endpoint (it is a read)")
    if problems:
        problems.append("output:\n    " + out.strip().replace("\n", "\n    "))
    return problems


# ---------------------------------------------------------------- watchdog --


def wd_clean_run_stays_quiet(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    reads = st.gets
    time.sleep(3.5)
    probs = []
    if st.gets < reads + 2:
        probs.append(f"watchdog polled {st.gets - reads}x in 3.5 s at a 1 s poll")
    a = job.run("Assert-NoProgramAudioBreach")
    if a.returncode != 0 or "no breach" not in a.stdout:
        probs.append(f"assert on a clean run: exit {a.returncode} {a.stdout}{a.stderr}")
    pid = job.watchdog_pid()
    s = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if s.returncode != 0 or "watchdog stopped" not in s.stdout:
        probs.append(f"teardown: exit {s.returncode} {s.stdout}{s.stderr}")
    if pid and not wait_for(lambda: not alive(pid), 5):
        probs.append("watchdog still alive after Stop-ProgramAudioWatchdog")
    if st.stops:
        probs.append("a clean run called the stop endpoint")
    return probs


def breach_checks(job: Env, st: MockState, expect: str, stops: int) -> list[str]:
    probs = []
    breach = job.path("program-audio-breach.txt")
    if not wait_for(breach.exists, 15):
        return ["no breach marker within 15 s"]
    pid = job.watchdog_pid()
    if not wait_for(lambda: "stop CONFIRMED" in breach.read_text(encoding="ascii"), 40):
        probs.append("the breach marker never recorded a CONFIRMED stop")
    if st.obs_streaming:
        probs.append("OBS (per the mock Restreamer) is still streaming")
    if pid and not wait_for(lambda: not alive(pid), 10):
        probs.append("watchdog did not exit after the breach")
    if len(st.stops) != stops:
        probs.append(f"stop endpoint called {len(st.stops)}x, expected {stops}")
    text = breach.read_text(encoding="ascii")
    if expect not in text:
        probs.append(f"breach marker lacks {expect!r}: {text!r}")
    a = job.run("Assert-NoProgramAudioBreach")
    if a.returncode != 1:
        probs.append(f"assert after a breach exited {a.returncode}, expected 1")
    if "::error::program-audio guard (#379): BREACH" not in a.stdout or expect not in a.stdout:
        probs.append(f"assert output lacks the ::error:: reason: {a.stdout}")
    if expect not in job.summary.read_text(encoding="utf-8"):
        probs.append("the breach reason is not in GITHUB_STEP_SUMMARY")
    t = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if t.returncode != 1:
        probs.append(f"teardown after a breach exited {t.returncode}, expected 1")
    return probs


def wd_foreign_stops_stream(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    if st.stops:
        return ["stopped before the music started"]
    st.sampler = json_reply(sample("FOREIGN", 0.4, rms_dbfs=-14.2))
    return breach_checks(job, st, "BREACH: FOREIGN", 1) + (
        [] if "stop CONFIRMED: Restreamer reports OBS not streaming (stop-stream attempt 1)"
        in job.path("program-audio-breach.txt").read_text(encoding="ascii")
        else ["the stop outcome is not `stop CONFIRMED ... attempt 1`"])


def wd_sampler_gone_fails_closed(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler = Reply(503, b"down")      # persistent: survives the one re-read
    return breach_checks(job, st, "BREACH: unreachable:", 1)


def wd_stop_retried_while_restreamer_down(job: Env, st: MockState, srv: MockServer) -> list[str]:
    st.stop_status = [503, 200]
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = breach_checks(job, st, "BREACH: FOREIGN", 2)
    if "(stop-stream attempt 2)" not in job.path("program-audio-breach.txt").read_text(encoding="ascii"):
        probs.append("the stop is not confirmed on the second attempt")
    return probs


def wd_queued_stop_is_reissued_until_confirmed(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # Restreamer answers 200 (queued) but OBS keeps streaming until the 3rd stop.
    st.stop_takes_effect_after = 3
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = breach_checks(job, st, "BREACH: FOREIGN", 3)
    text = job.path("program-audio-breach.txt").read_text(encoding="ascii")
    if "stop NOT confirmed yet after 1 attempt(s)" not in text or "(stop-stream attempt 3)" not in text:
        probs.append(f"the unconfirmed stop is not re-issued and recorded: {text!r}")
    return probs


def wd_not_our_stream_is_never_stopped(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # obs-stream.ps1 wrote marker=false (republish gap / refused restart / after our stop).
    job.run("Set-ProgramAudioStreamOwned $false")
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(3.5)
    probs = []
    if st.stops:
        probs.append("stopped a session that is not ours")
    if job.path("program-audio-breach.txt").exists():
        probs.append("music on a stream that is not ours counted as a breach")
    if "not our stream, nothing to stop: FOREIGN" not in job.path("program-audio-watchdog.log").read_text(encoding="ascii"):
        probs.append("the not-ours read is not logged")
    s = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if s.returncode != 0:
        probs.append(f"teardown: exit {s.returncode} {s.stdout}{s.stderr}")
    return probs


def wd_stop_ends_when_our_stream_ends(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # The stop is never confirmed (Restreamer down); then obs-stream marks our stream gone.
    st.stop_status = [503]
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    breach = job.path("program-audio-breach.txt")
    probs = []
    if not wait_for(lambda: breach.exists() and "stop NOT confirmed yet" in breach.read_text(encoding="ascii"), 20):
        return ["no unconfirmed-stop line"]
    pid = job.watchdog_pid()
    if not (pid and alive(pid)):
        probs.append("the watchdog gave up while the stop was unconfirmed")
    job.run("Set-ProgramAudioStreamOwned $false")
    if not wait_for(lambda: "not stopping a session that is not ours" in breach.read_text(encoding="ascii"), 20):
        probs.append("the stop loop did not end when our stream ended")
    a = job.run("Assert-NoProgramAudioBreach")
    if a.returncode != 1:
        probs.append("the breach no longer fails the job")
    return probs


def wd_start_refused_after_breach(job: Env, st: MockState, srv: MockServer) -> list[str]:
    job.path("program-audio-breach.txt").write_text("2026-10-07T10:00:00Z BREACH: FOREIGN: test\n", encoding="ascii")
    p = job.run(PROBE.replace("Test-ProgramAudio)", "Test-ProgramAudio -BeforeStart)"))
    if p.returncode != 1 or "WHY=earlier in this job: 2026-10-07T10:00:00Z BREACH: FOREIGN" not in p.stdout:
        return [f"-BeforeStart after a breach: exit {p.returncode} {p.stdout}{p.stderr}"]
    q = job.run(PROBE)
    if q.returncode != 0:
        return [f"plain Test-ProgramAudio must stay a pure read: exit {q.returncode} {q.stdout}"]
    return []


def wd_start_refused_with_dead_watchdog(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    pid = job.watchdog_pid()
    job.kill_watchdog()
    if pid and not wait_for(lambda: not alive(pid), 5):
        return ["could not kill the watchdog"]
    q = job.run(PROBE.replace("Test-ProgramAudio)", "Test-ProgramAudio -BeforeStart)"))
    if q.returncode != 1 or "watchdog died" not in q.stdout:
        return [f"-BeforeStart with a dead watchdog: exit {q.returncode} {q.stdout}{q.stderr}"]
    return []


def wd_blip_is_reread(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler_fail_next = 1
    time.sleep(5)
    probs = []
    if job.path("program-audio-breach.txt").exists():
        probs.append("a single HTTP 500 blip was a breach (it must be re-read once)")
    if st.stops:
        probs.append("a blip stopped the stream")
    if "re-reading once after: unreachable" not in job.path("program-audio-watchdog.log").read_text(encoding="ascii"):
        probs.append("the re-read is not logged")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_dead_watchdog_fails(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    pid = job.watchdog_pid()
    job.kill_watchdog()
    probs = []
    if pid and not wait_for(lambda: not alive(pid), 5):
        return ["could not kill the watchdog"]
    a = job.run("Assert-NoProgramAudioBreach")
    if a.returncode != 1 or "watchdog died" not in a.stdout:
        probs.append(f"assert with a dead watchdog: exit {a.returncode} {a.stdout}")
    t = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if t.returncode != 1 or "watchdog died before teardown" not in t.stdout:
        probs.append(f"teardown with a dead watchdog: exit {t.returncode} {t.stdout}")
    if st.stops:
        probs.append("a dead watchdog called the stop endpoint")
    return probs


def wd_hung_watchdog_fails(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # A live process recorded as the watchdog whose heartbeat is 5 min old.
    sleeper = subprocess.Popen(interpreter() + ["-NoProfile", "-Command", "Start-Sleep -Seconds 120"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        job.path("program-audio-watchdog.pid").write_text(str(sleeper.pid), encoding="ascii")
        job.path("program-audio-watchdog.heartbeat").write_text(str(time.time() - 300), encoding="ascii")
        a = job.run("Assert-NoProgramAudioBreach")
        if a.returncode != 1 or "watchdog hung" not in a.stdout:
            return [f"assert with a hung watchdog: exit {a.returncode} {a.stdout}{a.stderr}"]
        return []
    finally:
        sleeper.kill()
        sleeper.wait()


def wd_never_started(job: Env, st: MockState, srv: MockServer) -> list[str]:
    t = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if t.returncode != 0 or "no watchdog was started" not in t.stdout or "no breach" not in t.stdout:
        return [f"teardown with no watchdog: exit {t.returncode} {t.stdout}{t.stderr}"]
    return []


def wd_no_runner_temp(job: Env, st: MockState, srv: MockServer) -> list[str]:
    job.env.pop("RUNNER_TEMP")
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode == 0 or "RUNNER_TEMP is not set" not in p.stdout + p.stderr:
        return [f"start without RUNNER_TEMP: exit {p.returncode} {p.stdout}{p.stderr}"]
    return []


WATCHDOG_CASES = [
    ("watchdog: clean program -> polls, no stop, clean teardown", wd_clean_run_stays_quiet),
    ("watchdog: music starts (FOREIGN) -> marker, ONE stop call, exit, assert fails", wd_foreign_stops_stream),
    ("watchdog: sampler goes away -> fail closed, stop call", wd_sampler_gone_fails_closed),
    ("watchdog: stop endpoint down once (Restreamer restarting) -> retried", wd_stop_retried_while_restreamer_down),
    ("watchdog: stop queued but OBS keeps streaming -> re-issued until CONFIRMED",
     wd_queued_stop_is_reissued_until_confirmed),
    ("watchdog: music while the stream is not ours -> no stop, no breach", wd_not_our_stream_is_never_stopped),
    ("watchdog: unconfirmed stop keeps going, ends when our stream ends", wd_stop_ends_when_our_stream_ends),
    ("start gate: an earlier breach refuses the next start", wd_start_refused_after_breach),
    ("start gate: a dead watchdog refuses the next start", wd_start_refused_with_dead_watchdog),
    ("watchdog: one HTTP 500 blip -> re-read, no breach", wd_blip_is_reread),
    ("watchdog: killed -> assert and teardown fail (unguarded)", wd_dead_watchdog_fails),
    ("watchdog: stale heartbeat -> assert fails (hung)", wd_hung_watchdog_fails),
    ("watchdog: never started -> teardown is a no-op", wd_never_started),
    ("watchdog: no RUNNER_TEMP -> refuses to start", wd_no_runner_temp),
]


def watchdog_case(fn) -> list[str]:
    state = MockState()
    srv = MockServer(state)
    try:
        with tempfile.TemporaryDirectory() as tmp_s:
            job = Env(Path(tmp_s), srv.sampler_url, srv.stop_url, srv.status_url)
            try:
                probs = fn(job, state, srv)
            finally:
                job.env.setdefault("RUNNER_TEMP", tmp_s)
                job.kill_watchdog()
            if probs and job.path("program-audio-watchdog.log").exists():
                probs.append("watchdog log:\n    " + job.path("program-audio-watchdog.log").read_text(
                    encoding="ascii", errors="replace").strip().replace("\n", "\n    "))
            return probs
    finally:
        srv.close()


def main() -> int:
    failed = total = 0
    for name, reply, code, text in VERDICT_CASES:
        total += 1
        probs = verdict_case(reply, code, text)
        failed += bool(probs)
        print(f"{'FAIL' if probs else 'ok  '} Test-ProgramAudio: {name}")
        for p in probs:
            print(f"  - {p}")
    for name, fn in WATCHDOG_CASES:
        total += 1
        probs = watchdog_case(fn)
        failed += bool(probs)
        print(f"{'FAIL' if probs else 'ok  '} {name}")
        for p in probs:
            print(f"  - {p}")
    print(f"{total - failed}/{total} program-audio scenarios passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
