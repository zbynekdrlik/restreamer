#!/usr/bin/env python3
"""#357: scripts/ci/camera_box_gate.py, the release gate on camera-box's measurement.

Verdict evaluation runs on fixtures (shaped like the real artifact of camera-box run
41024885868). The GitHub side runs the real CLI against a fake `gh` on PATH, which
answers from a JSON scenario and logs its argv. Stdlib only, no network. Exit 0 when
every case passes.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ci" / "camera_box_gate.py"

sys.path.insert(0, str(SCRIPT.parent))
sys.dont_write_bytecode = True
import camera_box_gate as gate  # noqa: E402

PASS_LEG = {"schema": 1, "verdict": "PASS", "pass": True, "reasons": []}
RED_LEG = {
    "schema": 1,
    "verdict": "FAIL",
    "pass": False,
    "reasons": ["w01-CAM5: audio lag_jumps 1"],
}
FAILS: list[str] = []


def check(name: str, cond: bool, detail: object = "") -> None:
    print(f"{'ok  ' if cond else 'FAIL'} {name}{'' if cond else f': {detail}'}")
    if not cond:
        FAILS.append(name)


def art(tmp: Path, name: str, files: dict[str, object]) -> Path:
    d = tmp / name
    d.mkdir()
    for fname, body in files.items():
        (d / fname).write_text(body if isinstance(body, str) else json.dumps(body))
    return d


def evaluate_cases(tmp: Path) -> None:
    good = {"verdict-1.json": {"overall_pass": True, "youtube_leg": PASS_LEG}}
    cases = [
        ("pass", good, "success", "pass", None),
        (
            "camera fault does not block: red run, youtube leg passes",
            {"verdict-1.json": {"overall_pass": False, "youtube_leg": PASS_LEG}},
            "failure",
            "pass",
            None,
        ),
        (
            "youtube fail is red and names the reason",
            {"verdict-1.json": {"overall_pass": False, "youtube_leg": RED_LEG}},
            "failure",
            "red",
            "lag_jumps",
        ),
        (
            "green run does not excuse a red youtube leg",
            {"verdict-1.json": {"overall_pass": True, "youtube_leg": RED_LEG}},
            "success",
            "red",
            "lag_jumps",
        ),
        ("missing verdict file", {"other.json": {}}, "success", "unknown", "verdict"),
        (
            "two verdict files",
            {
                "verdict-1.json": {"youtube_leg": PASS_LEG},
                "verdict-2.json": {"youtube_leg": PASS_LEG},
            },
            "success",
            "unknown",
            "2 verdict",
        ),
        ("malformed json", {"verdict-1.json": "{not json"}, "success", "unknown", "JSON"),
        ("json is not an object", {"verdict-1.json": [1]}, "success", "unknown", "object"),
        (
            "missing youtube_leg",
            {"verdict-1.json": {"overall_pass": True}},
            "success",
            "unknown",
            "youtube_leg",
        ),
        (
            "youtube_leg null",
            {"verdict-1.json": {"overall_pass": True, "youtube_leg": None}},
            "success",
            "unknown",
            "youtube_leg",
        ),
        (
            "missing pass field",
            {"verdict-1.json": {"youtube_leg": {"reasons": []}}},
            "success",
            "unknown",
            "pass",
        ),
        (
            "pass as the string true is not a pass",
            {"verdict-1.json": {"youtube_leg": {"pass": "true"}}},
            "success",
            "unknown",
            "pass",
        ),
        (
            "pass as 1 is not a pass",
            {"verdict-1.json": {"youtube_leg": {"pass": 1}}},
            "success",
            "unknown",
            "pass",
        ),
        ("cancelled run is not measured", good, "cancelled", "unknown", "cancelled"),
        ("timed out run is not measured", good, "timed_out", "unknown", "timed_out"),
    ]
    for i, (name, files, conclusion, want, needle) in enumerate(cases):
        result = gate.evaluate(art(tmp, f"c{i}", files), conclusion)
        check(f"evaluate: {name}", result.state == want, result)
        if needle:
            check(f"evaluate: {name} says why", needle in " ".join(result.reasons), result)
    check(
        "evaluate: no artifact dir",
        gate.evaluate(tmp / "nope", "success").state == "unknown",
    )


FAKE_GH = """\
#!/usr/bin/env python3
import json, os, sys
d = os.environ["FAKE_GH_DIR"]
args = sys.argv[1:]
with open(d + "/argv.log", "a") as f:
    f.write(json.dumps(args) + "\\n")
s = json.load(open(d + "/scenario.json"))
state = d + "/calls"
n = int(open(state).read()) if os.path.exists(state) else 0
if args[:2] == ["workflow", "run"]:
    if s.get("dispatch_fails"):
        sys.stderr.write("HTTP 403: Resource not accessible\\n"); sys.exit(1)
    open(d + "/dispatched", "w").close()
    sys.exit(0)
if args[:2] == ["run", "download"]:
    if s.get("no_artifact"):
        sys.stderr.write("no artifact\\n"); sys.exit(1)
    out = args[args.index("-D") + 1]
    os.makedirs(out, exist_ok=True)
    json.dump(s["verdict"], open(out + "/verdict-77.json", "w"))
    sys.exit(0)
if args[:2] == ["run", "view"]:
    print(s.get("log", "")); sys.exit(0)
if args[0] == "api":
    path = next(a for a in args[1:] if "repos/" in a)
    if "/dispatches" in path:
        sys.stderr.write(s["probe_stderr"]); sys.exit(s.get("probe_exit", 1))
    if "/check-runs" in path:
        print(json.dumps(s["check_runs"][path.split("check_name=")[1]])); sys.exit(0)
    if "/workflows/" in path:
        ids = s["new_ids"] if os.path.exists(d + "/dispatched") else []
        print(json.dumps({"workflow_runs": [{"id": i, "created_at": t} for i, t in ids + s.get("old_ids", [])]}))
        sys.exit(0)
    open(state, "w").write(str(n + 1))
    seq = s["run_states"]
    item = seq[min(n, len(seq) - 1)]
    if item == "api-error":
        sys.stderr.write("HTTP 502\\n"); sys.exit(1)
    print(json.dumps(item)); sys.exit(0)
sys.stderr.write("unexpected gh call\\n"); sys.exit(9)
"""

DONE_OK = {"status": "completed", "conclusion": "success", "html_url": "u"}
RUNNING = {"status": "in_progress", "conclusion": None, "html_url": "u"}
OUR_LOG = "E2E_CALLER: restreamer@1.2.3\n"


def run_cli(tmp: Path, name: str, scenario: dict, *args: str):
    d = tmp / name
    d.mkdir()
    fake = d / "gh"
    fake.write_text(FAKE_GH)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    (d / "scenario.json").write_text(json.dumps(scenario))
    out = d / "gh_output"
    env = dict(
        os.environ,
        PATH=f"{d}{os.pathsep}{os.environ['PATH']}",
        FAKE_GH_DIR=str(d),
        GITHUB_OUTPUT=str(out),
        GATE_POLL_S="0",
        GATE_FIND_TIMEOUT_S="2",
    )
    p = subprocess.run(
        [sys.executable, "-B", str(SCRIPT), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        cwd=d,
    )
    outputs = dict(
        line.split("=", 1) for line in (out.read_text().splitlines() if out.exists() else [])
    )
    argv = [json.loads(x) for x in (d / "argv.log").read_text().splitlines()]
    return p, outputs, argv


def measure(tmp: Path, name: str, **scenario) -> tuple:
    base = {
        "old_ids": [[5, "2026-10-30T01:00:00Z"]],
        "new_ids": [[6, "2026-10-30T02:00:00Z"]],
        "run_states": [RUNNING, DONE_OK],
        "verdict": {"youtube_leg": PASS_LEG},
        "log": OUR_LOG,
    }
    base.update(scenario)
    return run_cli(
        tmp,
        name,
        base,
        "measure",
        "--caller",
        "restreamer@1.2.3",
        "--out",
        str(tmp / name / "art"),
        "--deadline-min",
        "1",
    )


def measure_cases(tmp: Path) -> None:
    p, o, argv = measure(tmp, "green")
    check("measure: green verdict exits 0", p.returncode == 0, p.stdout + p.stderr)
    check("measure: output pass", o.get("verdict") == "pass", o)
    check("measure: output run id", o.get("run_id") == "6", o)
    dispatch = next(a for a in argv if a[:2] == ["workflow", "run"])
    for want in ("measure=true", "caller=restreamer@1.2.3", "main", "full-path-e2e.yml"):
        check(f"measure: dispatch carries {want}", want in dispatch, dispatch)

    p, o, _ = measure(tmp, "red", verdict={"youtube_leg": RED_LEG})
    check("measure: red verdict fails", p.returncode == 1, p.returncode)
    check("measure: output red", o.get("verdict") == "red", o)
    check("measure: prints the reasons", "lag_jumps" in p.stdout, p.stdout)

    p, o, _ = measure(tmp, "two-new", new_ids=[[6, "2026-10-30T02:00:00Z"], [7, "2026-10-30T02:00:09Z"]])
    check("measure: newest new run wins", o.get("run_id") == "7", o)

    p, o, _ = measure(tmp, "foreign", log="E2E_CALLER: operator\n")
    check("measure: a run with another caller fails", p.returncode == 1, p.stdout)
    check("measure: foreign run is unknown", o.get("verdict") == "unknown", o)
    check("measure: says the caller is wrong", "caller" in p.stdout, p.stdout)

    p, o, _ = measure(tmp, "cancelled", run_states=[{"status": "completed", "conclusion": "cancelled", "html_url": "u"}])
    check("measure: superseded run fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))

    p, o, _ = measure(tmp, "noart", no_artifact=True)
    check("measure: missing artifact fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))

    p, o, _ = measure(tmp, "nodispatch", dispatch_fails=True)
    check("measure: refused dispatch fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))
    check("measure: refused dispatch shows gh's error", "403" in p.stdout + p.stderr, p.stdout)

    p, o, _ = measure(tmp, "notfound", new_ids=[])
    check("measure: run never appears fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))

    p, o, _ = measure(tmp, "blip", run_states=["api-error", RUNNING, "api-error", DONE_OK])
    check("measure: API blips are tolerated", p.returncode == 0, p.stdout + p.stderr)

    p, o, _ = measure(tmp, "deadapi", run_states=["api-error"])
    check("measure: a dead API fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))

    env_args = ["--caller", "restreamer@1.2.3", "--out", str(tmp / "dl-art"), "--deadline-min", "0"]
    p, o, _ = run_cli(
        tmp,
        "deadline",
        {"new_ids": [[6, "t"]], "run_states": [RUNNING]},
        "measure",
        *env_args,
    )
    check("measure: deadline fails", p.returncode == 1 and o.get("verdict") == "unknown", (p.returncode, o))


def token_cases(tmp: Path) -> None:
    good = {"probe_stderr": "gh: No ref found for: restreamer-token-check (HTTP 422)\n"}
    p, _, argv = run_cli(tmp, "tok-ok", good, "token-check")
    check("token-check: 422 no ref passes", p.returncode == 0, p.stdout + p.stderr)
    check("token-check: POSTs a dispatch", "POST" in argv[0], argv)
    for name, err in (
        ("forbidden", "gh: Resource not accessible by personal access token (HTTP 403)\n"),
        ("expired", "gh: Bad credentials (HTTP 401)\n"),
        ("other 422", "gh: Workflow does not have 'workflow_dispatch' trigger (HTTP 422)\n"),
        ("network", "dial tcp: timeout\n"),
    ):
        p, _, _ = run_cli(tmp, f"tok-{name.replace(' ', '')}", {"probe_stderr": err}, "token-check")
        check(f"token-check: {name} fails", p.returncode == 1, p.stdout)
    p, _, _ = run_cli(tmp, "tok-started", {"probe_stderr": "", "probe_exit": 0}, "token-check")
    check("token-check: a dispatch that was accepted fails", p.returncode == 1, p.stdout)


def release_cases(tmp: Path) -> None:
    ok = [{"status": "completed", "conclusion": "success", "started_at": "2026-10-30T02:00:00Z"}]
    bad = [{"status": "completed", "conclusion": "failure", "started_at": "2026-10-30T03:00:00Z"}]
    args = ("release-check", "--repo", "o/r", "--sha", "abc")
    both = {"E2E+Gate": ok, "Camera-box+YouTube+measurement": ok}

    def rc(name: str, runs: dict) -> int:
        return run_cli(tmp, name, {"check_runs": runs}, *args)[0].returncode

    check("release-check: both green passes", rc("r-ok", both) == 0)
    check(
        "release-check: no measurement fails",
        rc("r-none", dict(both, **{"Camera-box+YouTube+measurement": []})) == 1,
    )
    check("release-check: no e2e gate fails", rc("r-noe2e", dict(both, **{"E2E+Gate": []})) == 1)
    check(
        "release-check: red measurement fails",
        rc("r-red", dict(both, **{"Camera-box+YouTube+measurement": bad})) == 1,
    )
    check(
        "release-check: the latest attempt decides",
        rc("r-later", dict(both, **{"Camera-box+YouTube+measurement": ok + bad})) == 1,
    )
    check(
        "release-check: a later green rerun passes",
        rc(
            "r-rerun",
            dict(
                both,
                **{
                    "Camera-box+YouTube+measurement": [
                        dict(bad[0], started_at="2026-10-30T01:00:00Z"),
                        ok[0],
                    ]
                },
            ),
        )
        == 0,
    )
    running = [{"status": "in_progress", "conclusion": None, "started_at": "2026-10-30T02:00:00Z"}]
    check(
        "release-check: running measurement fails",
        rc("r-run", dict(both, **{"Camera-box+YouTube+measurement": running})) == 1,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        for name, fn in (
            ("eval", evaluate_cases),
            ("measure", measure_cases),
            ("token", token_cases),
            ("release", release_cases),
        ):
            sub = tmp / name
            sub.mkdir()
            fn(sub)
    if FAILS:
        print(textwrap.fill(f"{len(FAILS)} failed: {', '.join(FAILS)}"))
        return 1
    print("all camera-box gate cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
