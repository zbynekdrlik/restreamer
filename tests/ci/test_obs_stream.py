#!/usr/bin/env python3
"""#374: run scripts/ci/obs-stream.ps1 and obs-readiness-check.ps1 against a mock obs-websocket.

Stream OBS is camera-box's; restreamer CI may only start/stop streaming and read
status. The lane that wrote these scripts may not touch the real OBS, so this test is
their first execution: a stdlib-only mock obs-websocket v5 server (OBS is an external
service, so a mock is the allowed shape), a mock rig-lease endpoint, and a fake
`obs64` process. Each scenario runs the real PowerShell script and asserts:

  * the exit code and the key line of its output;
  * the OBS_STREAMING_STARTED_BY_CI values it wrote to GITHUB_ENV (in order);
  * that every request the script sent is in the read-only + Start/Stop allowlist.

Windows PowerShell 5.1 (`powershell`) on Windows -- what the stream box runs;
`pwsh` elsewhere, or the interpreter named by $OBS_TEST_PWSH.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts" / "ci"
ALLOWED = {
    "StartStream",
    "StopStream",
    "GetStreamStatus",
    "GetRecordStatus",
    "GetVersion",
    "GetCurrentProgramScene",
    "GetStreamServiceSettings",
}
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MARKER = "OBS_STREAMING_STARTED_BY_CI"


@dataclass
class ObsState:
    scene: str = "Development"
    service_type: str = "rtmp_custom"
    server: str = "rtmp://127.0.0.1:1234/live"
    streaming: bool = False
    recording: bool = False
    start_ok: bool = True
    stop_takes_effect: bool = True
    omit_output_active: bool = False
    reject_identify: bool = False
    requests: list[str] = field(default_factory=list)
    output_bytes: int = 0


# ------------------------------------------------------------ mock websocket --


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("client closed")
        buf += chunk
    return buf


def _send_frame(conn: socket.socket, opcode: int, payload: bytes) -> None:
    head = bytes([0x80 | opcode])
    n = len(payload)
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack(">H", n)
    else:
        head += bytes([127]) + struct.pack(">Q", n)
    conn.sendall(head + payload)


def _recv_frame(conn: socket.socket) -> tuple[int, bytes]:
    b1, b2 = _recv_exact(conn, 2)
    opcode, masked, n = b1 & 0x0F, b2 & 0x80, b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", _recv_exact(conn, 2))[0]
    elif n == 127:
        n = struct.unpack(">Q", _recv_exact(conn, 8))[0]
    mask = _recv_exact(conn, 4) if masked else b"\0\0\0\0"
    data = bytes(c ^ mask[i % 4] for i, c in enumerate(_recv_exact(conn, n)))
    return opcode, data


def _send_json(conn: socket.socket, obj: dict) -> None:
    _send_frame(conn, 0x1, json.dumps(obj).encode())


def _respond(state: ObsState, req: dict) -> dict:
    rtype, rid = req["requestType"], req["requestId"]
    state.requests.append(rtype)
    ok: dict = {"result": True, "code": 100}
    data: dict | None = None
    if rtype == "GetVersion":
        data = {"obsVersion": "31.0.0-mock", "obsWebSocketVersion": "5.5.0"}
    elif rtype == "GetCurrentProgramScene":
        data = {"currentProgramSceneName": state.scene}
    elif rtype == "GetStreamServiceSettings":
        data = {"streamServiceType": state.service_type,
                "streamServiceSettings": {"server": state.server, "key": "secret-key"}}
    elif rtype == "GetStreamStatus":
        if state.streaming:
            state.output_bytes += 1_500_000
        data = {"outputActive": state.streaming, "outputTimecode": "00:00:01.000",
                "outputBytes": state.output_bytes}
        if state.omit_output_active:
            del data["outputActive"]
    elif rtype == "GetRecordStatus":
        data = {"outputActive": state.recording}
        if state.omit_output_active:
            del data["outputActive"]
    elif rtype == "StartStream":
        if state.streaming:
            ok = {"result": False, "code": 500, "comment": "output already running"}
        elif not state.start_ok:
            ok = {"result": False, "code": 500, "comment": "mock refuses the start"}
        else:
            state.streaming = True
    elif rtype == "StopStream":
        if not state.streaming:
            ok = {"result": False, "code": 501, "comment": "output not running"}
        elif state.stop_takes_effect:
            state.streaming = False
    else:
        ok = {"result": False, "code": 204, "comment": "mock: unknown request"}
    d = {"requestType": rtype, "requestId": rid, "requestStatus": ok}
    if data is not None:
        d["responseData"] = data
    return {"op": 7, "d": d}


def _serve_client(conn: socket.socket, state: ObsState) -> None:
    try:
        raw = b""
        while b"\r\n\r\n" not in raw:
            raw += conn.recv(4096)
        key = next(l.split(":", 1)[1].strip() for l in raw.decode().split("\r\n")
                   if l.lower().startswith("sec-websocket-key:"))
        accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                      f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").encode())
        hello: dict = {"obsWebSocketVersion": "5.5.0", "rpcVersion": 1}
        if state.reject_identify:
            hello["authentication"] = {"challenge": "c2FsdA==", "salt": "c2FsdA=="}
        _send_json(conn, {"op": 0, "d": hello})
        while True:
            opcode, data = _recv_frame(conn)
            if opcode == 0x8:
                _send_frame(conn, 0x8, data[:2])
                return
            if opcode != 0x1:
                continue
            msg = json.loads(data)
            if msg["op"] == 1:
                if state.reject_identify:
                    _send_frame(conn, 0x8, struct.pack(">H", 4009) + b"Authentication failed.")
                    return
                _send_json(conn, {"op": 2, "d": {"negotiatedRpcVersion": 1}})
            elif msg["op"] == 6:
                _send_json(conn, _respond(state, msg["d"]))
    except (ConnectionError, OSError, StopIteration):
        return
    finally:
        conn.close()


class MockObs:
    def __init__(self, state: ObsState) -> None:
        self.state = state
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=_serve_client, args=(conn, self.state), daemon=True).start()

    def close(self) -> None:
        self.sock.close()


class MockLease:
    def __init__(self, body: dict) -> None:
        payload = json.dumps(body).encode()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (http.server API)
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


def _closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------- fake obs64 --


class FakeObs64:
    """A long-running process whose process name is `obs64` (Get-Process -Name obs64)."""

    def __init__(self, tmp: Path, count: int = 1) -> None:
        self.procs: list[subprocess.Popen] = []
        if os.name == "nt":
            exe = tmp / "obs64.exe"
            shutil.copy(Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "ping.exe", exe)
            args = [str(exe), "-n", "3600", "127.0.0.1"]
        else:
            exe = tmp / "obs64"
            shutil.copy(shutil.which("sleep") or "/bin/sleep", exe)
            exe.chmod(0o755)
            args = [str(exe), "3600"]
        for _ in range(count):
            self.procs.append(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

    def close(self) -> None:
        for p in self.procs:
            p.kill()
            p.wait()


# ----------------------------------------------------------------- scenarios --


@dataclass
class Case:
    name: str
    script: str
    args: list[str]
    state: ObsState
    expect_exit: int
    expect_text: str
    expect_markers: list[str]
    obs64: int = 1
    lease: dict | None = None
    obs_down: bool = False
    forbid_requests: set[str] = field(default_factory=set)
    require_requests: set[str] = field(default_factory=set)


FREE = {"schema": 1, "held": False, "stale": False}
HELD = {"schema": 1, "held": True, "stale": False, "ttl_s": 600,
        "holder": {"job": "full-path-e2e", "run_url": "https://example.invalid/run/1"}}
START = ["-Action", "Start"]
STOP = ["-Action", "Stop"]
ASSERT = ["-Action", "AssertNotStreaming"]
REPUBLISH = ["-Action", "Republish", "-GapSeconds", "1"]
RC = "obs-readiness-check.ps1"
OS_ = "obs-stream.ps1"

CASES = [
    Case("readiness: TEST mode -> ready", RC, [], ObsState(), 0, "stream OBS ready", []),
    Case("readiness: production scene PRO", RC, [], ObsState(scene="PRO"), 1, "production scene 'PRO'", []),
    Case("readiness: another scene", RC, [], ObsState(scene="Scene 2"), 1, "not the TEST scene 'Development'", []),
    Case("readiness: no obs64", RC, [], ObsState(), 1, "obs64 is not running", [], obs64=0),
    Case("readiness: two obs64", RC, [], ObsState(), 1, "2 obs64 processes", [], obs64=2),
    Case("readiness: websocket down", RC, [], ObsState(), 1, "unreachable", [], obs_down=True),
    Case("readiness: service points at YouTube", RC, [],
         ObsState(server="rtmp://a.rtmp.youtube.com/live2"), 1, "not the restreamer inpoint", []),
    Case("readiness: already streaming", RC, [], ObsState(streaming=True), 1, "OBS is already streaming", []),
    Case("readiness: recording", RC, [], ObsState(recording=True), 1, "stream OBS is recording -- not touching it", []),
    Case("readiness: outputActive missing", RC, [], ObsState(omit_output_active=True), 1, "returned no outputActive", []),
    Case("start: ready -> started, marker true", OS_, START, ObsState(), 0, "kbps", ["true"],
         lease=FREE, require_requests={"StartStream"}),
    Case("start: refused -> marker false", OS_, START, ObsState(start_ok=False), 1, "StartStream refused", ["true", "false"],
         lease=FREE),
    Case("start: rig lease held -> no start, no marker", OS_, START, ObsState(), 1, "holds the rig lease", [],
         lease=HELD, forbid_requests={"StartStream", "GetVersion"}),
    Case("start: camera-box already streaming -> no start, no marker", OS_, START, ObsState(streaming=True), 1,
         "OBS is already streaming", [], lease=FREE, forbid_requests={"StartStream"}),
    Case("start: recording -> no start, no marker", OS_, START, ObsState(recording=True), 1,
         "stream OBS is recording", [], lease=FREE, forbid_requests={"StartStream"}),
    Case("start: lease endpoint down -> fail-open start", OS_, START, ObsState(), 0, "kbps", ["true"]),
    Case("stop: ours -> stopped", OS_, STOP, ObsState(streaming=True), 0, "OBS stream stopped", [],
         require_requests={"StopStream"}),
    Case("stop: already stopped (501) -> ok", OS_, STOP, ObsState(), 0, "OBS stream stopped", []),
    Case("stop: OBS keeps streaming -> fail loudly", OS_, STOP, ObsState(streaming=True, stop_takes_effect=False), 1,
         "still streaming 20 s after StopStream", []),
    Case("assert: not streaming -> ok", OS_, ASSERT, ObsState(), 0, "not streaming - good", [],
         forbid_requests={"StopStream", "StartStream"}),
    Case("assert: streaming -> fail, never stopped", OS_, ASSERT, ObsState(streaming=True), 1, "already streaming", [],
         forbid_requests={"StopStream"}),
    Case("assert: OBS down -> warning, proceed", OS_, ASSERT, ObsState(), 0, "not reachable", [], obs_down=True),
    Case("assert: identify rejected -> fail", OS_, ASSERT, ObsState(reject_identify=True), 1, "identify rejected", []),
    Case("republish: ours -> stop, false, start, true", OS_, REPUBLISH, ObsState(streaming=True), 0,
         "OBS streaming to the restreamer inpoint", ["false", "true"], lease=FREE,
         require_requests={"StopStream", "StartStream"}),
    Case("republish: restart refused -> marker false", OS_, REPUBLISH, ObsState(streaming=True, start_ok=False), 1,
         "StartStream refused", ["false", "true", "false"], lease=FREE),
]


def interpreter() -> list[str]:
    if os.environ.get("OBS_TEST_PWSH"):
        return [os.environ["OBS_TEST_PWSH"]]
    return ["powershell"] if os.name == "nt" else ["pwsh"]


def markers(path: Path) -> list[str]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.lstrip("\ufeff").strip()
        if line.startswith(f"{MARKER}="):
            out.append(line.split("=", 1)[1])
    return out


def run_case(case: Case) -> list[str]:
    problems: list[str] = []
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = Path(tmp_s)
        obs = None if case.obs_down else MockObs(case.state)
        lease = MockLease(case.lease) if case.lease else None
        fake = FakeObs64(tmp, case.obs64) if case.obs64 else None
        env_file = tmp / "github_env"
        env_file.write_text("", encoding="utf-8")
        env = dict(os.environ)
        env.update({
            "OBS_WS_HOST": "127.0.0.1",
            "OBS_WS_PORT": str(obs.port if obs else _closed_port()),
            "OBS_WS_PASSWORD": "mock-password",
            "RIG_LEASE_URL": lease.url if lease else f"http://127.0.0.1:{_closed_port()}/rig-lease.json",
            "GITHUB_ENV": str(env_file),
        })
        try:
            proc = subprocess.run(
                interpreter() + ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(SCRIPTS / case.script)] + case.args,
                capture_output=True, text=True, env=env, timeout=120,
            )
        finally:
            if fake:
                fake.close()
            if obs:
                obs.close()
            if lease:
                lease.close()
        out = proc.stdout + proc.stderr
        got = markers(env_file)
    if proc.returncode != case.expect_exit:
        problems.append(f"exit {proc.returncode}, expected {case.expect_exit}")
    if case.expect_text not in out:
        problems.append(f"output lacks {case.expect_text!r}")
    if got != case.expect_markers:
        problems.append(f"markers {got}, expected {case.expect_markers}")
    sent = set(case.state.requests)
    if sent - ALLOWED:
        problems.append(f"sent non-allowlisted requests {sorted(sent - ALLOWED)}")
    if sent & case.forbid_requests:
        problems.append(f"sent forbidden requests {sorted(sent & case.forbid_requests)}")
    if case.require_requests - sent:
        problems.append(f"never sent {sorted(case.require_requests - sent)}")
    if "secret-key" in out:
        problems.append("printed the stream key")
    if problems:
        problems.append("output:\n    " + out.strip().replace("\n", "\n    "))
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
            print(f"ok   {case.name} (requests: {', '.join(case.state.requests) or 'none'})")
    print(f"{len(CASES) - failed}/{len(CASES)} scenarios passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
