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
    is never stopped (#374); -BeforeStart refuses after a breach or a dead watchdog;
  * the verdict tolerance (ROZHODNUTE 2026-10-07): while streaming FOREIGN stops at
    once, while ONE UNKNOWN / stale / unreachable poll is tolerated and the second in a
    row stops; before the start any non-OK verdict refuses, after a retry window of up
    to 15 s for a startup UNKNOWN (FOREIGN refuses at once);
  * the delivery cut (ROZHODNUTE 2026-10-07): on a breach the watchdog also stops the
    CI event's delivery and deactivates it (the 120 s cache would keep sending the
    music), confirms no delivery instance of that event is left, and never touches
    an event that is not a CI-owned E2E event.

Every step runs the way GitHub Actions runs `shell: powershell` (the 'stop'
prelude, `-command ". '<file>'"`), so `exit 1` inside a function behaves as in CI.

Windows PowerShell 5.1 (`powershell`) on Windows -- what the stream box runs;
`pwsh` elsewhere, or the interpreter named by $OBS_TEST_PWSH.
"""

from __future__ import annotations

import http.server
import json
import os
import re
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
            "last_foreign_ts_utc": None, "last_foreign_age_s": None,
            # camera-box dev dfccef2f8: MEASUREMENT needs a QPSK marker chain >= 4 over 4 s
            "markers_decoded": 8, "marker_chain": 6}
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
    sampler_script: list[Reply] = field(default_factory=list)  # served one per read first
    stop_status: list[int] = field(default_factory=lambda: [200])  # per call; the last repeats
    stop_takes_effect_after: int = 1     # accepted (2xx) stops before OBS really stops
    obs_streaming: bool = True           # what Restreamer's GET /api/v1/obs/status reports
    obs_connected: bool = True
    gets: int = 0
    stops: list[float] = field(default_factory=list)
    accepted_stops: int = 0
    # Restreamer's events + delivery (the breach also cuts the CI event's delivery).
    events: list[dict] = field(default_factory=lambda: [dict(CI_EVENT), dict(CHURCH_EVENT)])
    instances: list[dict] = field(default_factory=lambda: [
        {"id": "vps-ci", "event_id": CI_EVENT["id"], "status": "delivering"},
        {"id": "vps-church", "event_id": CHURCH_EVENT["id"], "status": "delivering"}])
    delivery_stop_status: list[int] = field(default_factory=lambda: [200])  # per call; the last repeats
    delivery_stops: list[object] = field(default_factory=list)   # event_id of each POST
    deactivates: list[int] = field(default_factory=list)          # event id of each POST


CI_EVENT = {"id": 9278, "name": "E2E-Test", "receiving_activated": True, "delivering_activated": True}
CHURCH_EVENT = {"id": 5, "name": "Nedelna bohosluzba", "receiving_activated": True, "delivering_activated": True}


class MockServer:
    """Serves the sampler and a mock Restreamer API on one port: obs status + stop-stream,
    events + deactivate, delivery instances + delivery stop."""

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
                if self.path == "/api/v1/events":
                    self._send(json_reply(st.events))
                    return
                if self.path == "/api/v1/delivery/instances":
                    self._send(json_reply(st.instances))
                    return
                st.gets += 1
                if st.sampler_fail_next > 0:
                    st.sampler_fail_next -= 1
                    self._send(Reply(500, b"boom"))
                    return
                if st.sampler_script:
                    self._send(st.sampler_script.pop(0))
                    return
                self._send(st.sampler)

            def do_POST(self) -> None:  # noqa: N802
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                if self.path == "/api/v1/delivery/stop":
                    try:
                        eid = json.loads(raw or b"{}").get("event_id")
                    except ValueError:
                        eid = None
                    st.delivery_stops.append(eid)
                    code = st.delivery_stop_status[min(len(st.delivery_stops) - 1, len(st.delivery_stop_status) - 1)]
                    if code < 300:
                        st.instances = [i for i in st.instances if i["event_id"] != eid]
                    self._send(json_reply({"stopped": code < 300}, code))
                    return
                m = re.fullmatch(r"/api/v1/events/(\d+)/deactivate", self.path)
                if m:
                    st.deactivates.append(int(m.group(1)))
                    for e in st.events:
                        if e["id"] == int(m.group(1)):
                            e["receiving_activated"] = e["delivering_activated"] = False
                    self._send(json_reply({"ok": True}))
                    return
                if self.path != "/api/v1/obs/stop-stream":
                    self._send(Reply(404, b""))
                    return
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
        self.api_base = f"http://127.0.0.1:{port}"
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

    def __init__(self, tmp: Path, sampler_url: str, api_base: str) -> None:
        self.tmp = tmp
        self.summary = tmp / "step_summary.md"
        self.summary.write_text("", encoding="utf-8")
        self.env = dict(os.environ)
        self.env.update({
            "RUNNER_TEMP": str(tmp),
            "GITHUB_STEP_SUMMARY": str(self.summary),
            "PROGRAM_AUDIO_URL": sampler_url,
            "PROGRAM_AUDIO_API_BASE": api_base,
            "PROGRAM_AUDIO_POLL_S": "1",
            "PROGRAM_AUDIO_DELIVERY_BUDGET_S": "10",
            "EVENT_NAME": CI_EVENT["name"],   # the job env of e2e-obs-youtube-test
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
    ("age_s -0.5 (a clock-sync step, camera-box accepts >= -1) -> ok", json_reply(sample(age=-0.5)), 0, "VERDICT-OK"),
    ("UNKNOWN with FOREIGN 6 s ago -> FOREIGN (the latch beats UNKNOWN)",
     json_reply(sample("UNKNOWN", markers_decoded=0, marker_chain=0, last_foreign_age_s=6.0)), 1,
     "WHY=FOREIGN: foreign audio 6s ago"),
    ("a stale FOREIGN -> FOREIGN (not a tolerable stale)", json_reply(sample("FOREIGN", age=12.5)), 1,
     "WHY=FOREIGN: program audio is not the measurement signal"),
    ("MEASUREMENT with FOREIGN 25 s ago -> FOREIGN (30 s latch window)",
     json_reply(sample(last_foreign_age_s=25.0)), 1, "WHY=FOREIGN: foreign audio 25s ago"),
    ("age_s a JSON true -> malformed", json_reply(sample(age=True)), 1, "WHY=malformed: "),
    ("MEASUREMENT without marker_chain (an old sampler) -> UNKNOWN",
     json_reply({k: v for k, v in sample().items() if k != "marker_chain"}), 1,
     "WHY=UNKNOWN: MEASUREMENT without marker_chain"),
]


def verdict_case(reply: Reply | None, expect_exit: int, expect_text: str) -> list[str]:
    state = MockState(sampler=reply or Reply())
    srv = MockServer(state)
    try:
        with tempfile.TemporaryDirectory() as tmp_s:
            url = srv.sampler_url if reply is not None else f"http://127.0.0.1:{closed_port()}/program-audio.json"
            job = Env(Path(tmp_s), url, srv.api_base)
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
    time.sleep(1.5)
    if not (pid and alive(pid)):
        probs.append("the watchdog stopped watching after the confirmed stop (our stream is still ours)")
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
    if pid and not wait_for(lambda: not alive(pid), 10):
        probs.append("the teardown did not end the watchdog")
    return probs


def wd_back_on_air_is_stopped_again(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # Confirmed stop, then OBS streams again (a lost StopStream + an OBS reconnect).
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    breach = job.path("program-audio-breach.txt")
    if not wait_for(lambda: breach.exists() and "stop CONFIRMED" in breach.read_text(encoding="ascii"), 30):
        return ["no confirmed stop"]
    st.obs_streaming = True
    probs = []
    if not wait_for(lambda: "stop re-CONFIRMED" in breach.read_text(encoding="ascii"), 30):
        probs.append("a stream back on air after the confirmed stop was not stopped again")
    text = breach.read_text(encoding="ascii")
    if "OBS is streaming AGAIN after the confirmed stop" not in text:
        probs.append(f"the return to air is not recorded: {text!r}")
    if len(st.stops) != 2 or st.obs_streaming:
        probs.append(f"stops={len(st.stops)} (expected 2), still streaming={st.obs_streaming}")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_disconnected_status_is_not_a_confirmation(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # Restreamer's OBS client is disconnected: its streaming=false proves nothing.
    st.obs_connected = False
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    breach = job.path("program-audio-breach.txt")
    probs = []
    if not wait_for(lambda: breach.exists() and "stop NOT confirmed yet" in breach.read_text(encoding="ascii"), 20):
        probs.append("a disconnected status was not treated as unconfirmed")
    if "stop CONFIRMED" in breach.read_text(encoding="ascii"):
        probs.append("confirmed on a disconnected status")
    st.obs_connected = True
    if not wait_for(lambda: "stop CONFIRMED" in breach.read_text(encoding="ascii"), 30):
        probs.append("not confirmed once Restreamer's OBS client reconnected")
    if len(st.stops) < 2:
        probs.append(f"the stop was not re-issued while unconfirmed (stops={len(st.stops)})")
    job.run("Stop-ProgramAudioWatchdog")
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
    if st.stops or st.delivery_stops or st.deactivates:
        probs.append("stopped a session / cut a delivery that is not ours")
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
    if "tolerating ONE poll (the next non-OK one stops): unreachable" not in \
            job.path("program-audio-watchdog.log").read_text(encoding="ascii"):
        probs.append("the tolerated poll is not logged")
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


def wait_breach(job: Env, text: str, seconds: float) -> bool:
    breach = job.path("program-audio-breach.txt")
    return wait_for(lambda: breach.exists() and text in breach.read_text(encoding="ascii"), seconds)


def wd_foreign_cuts_ci_delivery(job: Env, st: MockState, srv: MockServer) -> list[str]:
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = []
    if not wait_breach(job, "delivery CUT CONFIRMED", 40):
        probs.append("the CI event's delivery was not cut (no `delivery CUT CONFIRMED`)")
    if st.delivery_stops != [CI_EVENT["id"]]:
        probs.append(f"delivery/stop called for {st.delivery_stops}, expected exactly [{CI_EVENT['id']}]")
    if st.deactivates != [CI_EVENT["id"]]:
        probs.append(f"deactivate called for {st.deactivates}, expected exactly [{CI_EVENT['id']}]")
    if not any(i["event_id"] == CHURCH_EVENT["id"] for i in st.instances):
        probs.append("the non-CI event's delivery instance was touched")
    text = job.path("program-audio-breach.txt").read_text(encoding="ascii")
    order = ["BREACH: FOREIGN", "stop CONFIRMED", f"delivery stop OK: event E2E-Test (id {CI_EVENT['id']})",
             f"event E2E-Test (id {CI_EVENT['id']}) deactivated", "delivery CUT CONFIRMED"]
    pos = [text.find(x) for x in order]
    if -1 in pos or pos != sorted(pos):
        probs.append(f"breach marker lacks the steps in order {order}: {text!r}")
    a = job.run("Assert-NoProgramAudioBreach")
    summary = job.summary.read_text(encoding="utf-8")
    if a.returncode != 1 or "delivery CUT CONFIRMED" not in summary or "deactivated" not in summary:
        probs.append(f"the step summary lacks the delivery cut steps: {summary!r}")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_non_ci_event_is_never_touched(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # A job whose EVENT_NAME is not a CI-owned E2E event (e.g. the church service).
    job.env["EVENT_NAME"] = CHURCH_EVENT["name"]
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = []
    if not wait_breach(job, "REFUSED", 30):
        probs.append("no REFUSED line for a non-CI event")
    time.sleep(2)
    if st.delivery_stops or st.deactivates:
        probs.append(f"touched a non-CI event: delivery_stops={st.delivery_stops} deactivates={st.deactivates}")
    if not wait_breach(job, "stop CONFIRMED", 10):
        probs.append("the OBS stop itself must still happen")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_no_event_name_is_refused(job: Env, st: MockState, srv: MockServer) -> list[str]:
    job.env.pop("EVENT_NAME")
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = []
    if not wait_breach(job, "REFUSED", 30):
        probs.append("no REFUSED line without EVENT_NAME")
    time.sleep(2)
    if st.delivery_stops or st.deactivates:
        probs.append(f"cut a delivery with no CI event name: {st.delivery_stops} {st.deactivates}")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_delivery_stop_retried(job: Env, st: MockState, srv: MockServer) -> list[str]:
    st.delivery_stop_status = [503, 200]
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = []
    if not wait_breach(job, "delivery CUT CONFIRMED", 60):
        probs.append("delivery not cut after a failed first delivery/stop")
    if st.delivery_stops != [CI_EVENT["id"], CI_EVENT["id"]]:
        probs.append(f"delivery/stop calls {st.delivery_stops}, expected two for {CI_EVENT['id']}")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_delivery_cut_even_if_obs_stop_unconfirmed(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # Restreamer's OBS client is disconnected, so the OBS stop never confirms: the
    # buffered cache must not keep going out meanwhile.
    st.obs_connected = False
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    probs = []
    if not wait_breach(job, "delivery CUT CONFIRMED", 40):
        probs.append("delivery not cut while the OBS stop is unconfirmed")
    if "CONFIRMED: Restreamer reports OBS" in job.path("program-audio-breach.txt").read_text(encoding="ascii"):
        probs.append("OBS stop confirmed on a disconnected status")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


UNKNOWN = sample("UNKNOWN", markers_decoded=0, marker_chain=0)


def wd_one_unknown_between_measurements(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # A 4 s NDI receive gap reads UNKNOWN once; it must not kill a CI run.
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler_script = [json_reply(UNKNOWN)]
    time.sleep(4.5)
    probs = []
    if st.sampler_script:
        probs.append("the scripted UNKNOWN was never read")
    if job.path("program-audio-breach.txt").exists() or st.stops:
        probs.append("ONE UNKNOWN between MEASUREMENTs stopped the stream")
    if "tolerating ONE poll (the next non-OK one stops): UNKNOWN" not in \
            job.path("program-audio-watchdog.log").read_text(encoding="ascii"):
        probs.append("the tolerated UNKNOWN is not logged")
    s = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if s.returncode != 0:
        probs.append(f"teardown: exit {s.returncode} {s.stdout}{s.stderr}")
    return probs


def wd_two_unknowns_stop(job: Env, st: MockState, srv: MockServer) -> list[str]:
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler = json_reply(UNKNOWN)       # the classifier stays blind
    probs = []
    if not wait_breach(job, "(second non-OK poll in a row; the first: UNKNOWN", 15):
        probs.append("two UNKNOWNs in a row did not stop the stream")
    if not wait_breach(job, "stop CONFIRMED", 20) or len(st.stops) < 1:
        probs.append("no confirmed OBS stop after two UNKNOWNs")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_foreign_after_measurement_is_immediate(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # ONE FOREIGN read between MEASUREMENTs: no tolerance for music.
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler_script = [json_reply(sample("FOREIGN", rms_dbfs=-14.2))]
    probs = []
    if not wait_breach(job, "BREACH: FOREIGN", 10):
        probs.append("a single FOREIGN read did not stop the stream at once")
    elif "second non-OK poll" in job.path("program-audio-breach.txt").read_text(encoding="ascii"):
        probs.append("FOREIGN was tolerated for a poll")
    if not wait_breach(job, "stop CONFIRMED", 20):
        probs.append("no confirmed OBS stop after FOREIGN")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_latched_foreign_behind_unknown(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # camera-box heard music, then went blind (UNKNOWN, latch 8 s), then MEASUREMENT with
    # the latch at 19 s: the music must not hide behind the UNKNOWN tolerance.
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    st.sampler_script = [json_reply(sample("UNKNOWN", markers_decoded=0, marker_chain=0, last_foreign_age_s=8.0)),
                         json_reply(sample(last_foreign_age_s=19.0))]
    probs = []
    if not wait_breach(job, "BREACH: FOREIGN: foreign audio 8s ago", 10):
        probs.append("a latched FOREIGN behind an UNKNOWN was tolerated / missed")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_flapping_unknowns_breach(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # ROZHODNUTE 2026-10-07 (3rd): U/OK/U/OK/U -- never two in a row, but the 3rd
    # tolerated UNKNOWN within the window is a breach (a flapping classifier fails closed).
    job.env["PROGRAM_AUDIO_FLAP_WINDOW_S"] = "8"     # 60 s in CI; polls are 1 s here
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    ok = json_reply(sample())
    st.sampler_script = [json_reply(UNKNOWN), ok, json_reply(UNKNOWN), ok, json_reply(UNKNOWN)]
    probs = []
    if not wait_breach(job, "3 tolerated non-OK polls within 8s", 15):
        probs.append("U/OK/U/OK/U within the window did not breach")
    elif "second non-OK poll in a row" in job.path("program-audio-breach.txt").read_text(encoding="ascii"):
        probs.append("breached as 2-in-a-row, not as flapping")
    if not wait_breach(job, "stop CONFIRMED", 20):
        probs.append("no confirmed OBS stop after the flapping breach")
    job.run("Stop-ProgramAudioWatchdog")
    return probs


def wd_spread_unknowns_do_not_breach(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # The same 3 UNKNOWNs, but spread wider than the window: no breach.
    job.env["PROGRAM_AUDIO_FLAP_WINDOW_S"] = "8"
    p = job.run("Start-ProgramAudioWatchdog")
    if p.returncode != 0:
        return [f"start exit {p.returncode}: {p.stdout}{p.stderr}"]
    time.sleep(1.5)
    ok = json_reply(sample())
    st.sampler_script = ([json_reply(UNKNOWN)] + [ok] * 7 + [json_reply(UNKNOWN)] + [ok] * 7
                         + [json_reply(UNKNOWN)])
    wait_for(lambda: not st.sampler_script, 40)
    time.sleep(2.5)
    probs = []
    if st.sampler_script:
        probs.append("the scripted reads were not all consumed")
    if job.path("program-audio-breach.txt").exists() or st.stops:
        probs.append("3 UNKNOWNs spread over more than the window breached")
    s2 = job.run("Stop-ProgramAudioWatchdog\nAssert-NoProgramAudioBreach")
    if s2.returncode != 0:
        probs.append(f"teardown: exit {s2.returncode} {s2.stdout}{s2.stderr}")
    return probs


START_PROBE = PROBE.replace("Test-ProgramAudio)", "Test-ProgramAudio -BeforeStart)")


def pre_start_rides_over_startup_unknown(job: Env, st: MockState, srv: MockServer) -> list[str]:
    # The first 4 s after a sampler start read UNKNOWN: the start retries over it.
    st.sampler_script = [json_reply(UNKNOWN), json_reply(UNKNOWN)]
    p = job.run(START_PROBE)
    probs = []
    if p.returncode != 0 or "VERDICT-OK" not in p.stdout:
        probs.append(f"the pre-start check did not ride over a startup UNKNOWN: exit {p.returncode} {p.stdout}")
    if st.gets != 3:
        probs.append(f"pre-start read the sampler {st.gets}x, expected 3 (UNKNOWN, UNKNOWN, MEASUREMENT)")
    return probs


def pre_start_refuses_lasting_unknown(job: Env, st: MockState, srv: MockServer) -> list[str]:
    job.env["PROGRAM_AUDIO_START_RETRY_S"] = "4"
    st.sampler = json_reply(UNKNOWN)
    t0 = time.time()
    p = job.run(START_PROBE)
    took = time.time() - t0
    probs = []
    if p.returncode != 1 or "WHY=UNKNOWN:" not in p.stdout or "still not OK after a 4s pre-start retry" not in p.stdout:
        probs.append(f"a lasting UNKNOWN was not refused after the retry window: exit {p.returncode} {p.stdout}")
    if took < 4 or st.gets < 2:
        probs.append(f"no retry window: took {took:.1f} s, {st.gets} reads")
    return probs


def pre_start_refuses_foreign_at_once(job: Env, st: MockState, srv: MockServer) -> list[str]:
    st.sampler = json_reply(sample("FOREIGN"))
    p = job.run(START_PROBE)
    probs = []
    if p.returncode != 1 or "WHY=FOREIGN:" not in p.stdout:
        probs.append(f"FOREIGN before the start was not refused: exit {p.returncode} {p.stdout}")
    if st.gets != 1:
        probs.append(f"FOREIGN before the start was retried ({st.gets} reads)")
    return probs


WATCHDOG_CASES = [
    ("watchdog: clean program -> polls, no stop, clean teardown", wd_clean_run_stays_quiet),
    ("watchdog: music starts (FOREIGN) -> marker, ONE stop call, exit, assert fails", wd_foreign_stops_stream),
    ("watchdog: sampler goes away -> fail closed, stop call", wd_sampler_gone_fails_closed),
    ("watchdog: stop endpoint down once (Restreamer restarting) -> retried", wd_stop_retried_while_restreamer_down),
    ("watchdog: stop queued but OBS keeps streaming -> re-issued until CONFIRMED",
     wd_queued_stop_is_reissued_until_confirmed),
    ("watchdog: back on air after the confirmed stop -> stopped again", wd_back_on_air_is_stopped_again),
    ("watchdog: Restreamer's OBS client disconnected -> not a confirmation", wd_disconnected_status_is_not_a_confirmation),
    ("watchdog: music while the stream is not ours -> no stop, no breach", wd_not_our_stream_is_never_stopped),
    ("tolerance: ONE UNKNOWN between two MEASUREMENTs -> no stop", wd_one_unknown_between_measurements),
    ("tolerance: two UNKNOWNs in a row -> stop", wd_two_unknowns_stop),
    ("tolerance: flapping U/OK/U/OK/U -> breach on the 3rd UNKNOWN in the window", wd_flapping_unknowns_breach),
    ("tolerance: the same 3 UNKNOWNs spread wider than the window -> no breach", wd_spread_unknowns_do_not_breach),
    ("tolerance: FOREIGN after MEASUREMENT -> immediate stop", wd_foreign_after_measurement_is_immediate),
    ("tolerance: a FOREIGN latch behind an UNKNOWN -> immediate stop", wd_latched_foreign_behind_unknown),
    ("pre-start: rides over a startup UNKNOWN within the retry window", pre_start_rides_over_startup_unknown),
    ("pre-start: a lasting UNKNOWN is refused after the retry window", pre_start_refuses_lasting_unknown),
    ("pre-start: FOREIGN is refused at once (no retry)", pre_start_refuses_foreign_at_once),
    ("delivery cut: FOREIGN -> delivery/stop + deactivate of the CI event only, CUT CONFIRMED",
     wd_foreign_cuts_ci_delivery),
    ("delivery cut: a non-CI EVENT_NAME is REFUSED, nothing of it touched", wd_non_ci_event_is_never_touched),
    ("delivery cut: no EVENT_NAME is REFUSED", wd_no_event_name_is_refused),
    ("delivery cut: delivery/stop fails once -> retried", wd_delivery_stop_retried),
    ("delivery cut: happens even while the OBS stop is unconfirmed", wd_delivery_cut_even_if_obs_stop_unconfirmed),
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
            job = Env(Path(tmp_s), srv.sampler_url, srv.api_base)
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
    # argv: optional name substrings; only the scenarios matching one of them run
    # (for proving a single scenario RED on an older guard). CI runs them all.
    only = sys.argv[1:]

    def picked(name: str) -> bool:
        return not only or any(o in name for o in only)

    failed = total = 0
    for name, reply, code, text in VERDICT_CASES:
        if not picked(name):
            continue
        total += 1
        probs = verdict_case(reply, code, text)
        failed += bool(probs)
        print(f"{'FAIL' if probs else 'ok  '} Test-ProgramAudio: {name}")
        for p in probs:
            print(f"  - {p}")
    for name, fn in WATCHDOG_CASES:
        if not picked(name):
            continue
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
