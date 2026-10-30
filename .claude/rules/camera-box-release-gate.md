---
paths:
  - ".github/workflows/*.yml"
  - "scripts/ci/camera_box_gate.py"
  - "scripts/ci/verify_camera_box_gate.py"
  - "tests/ci/test_camera_box_gate.py"
---

# Camera-box release gate (#357)

Every main run waits for camera-box's full-path measurement of what YouTube recorded. A red
YouTube leg blocks the release. Contract: camera-box issue 1404, `measure=true` dispatch.

- **Job `camera-box-release-gate`** (hosted, main push only, needed by `e2e-gate`) runs after all
  three stream-lan jobs, because camera-box takes the rig lease itself. It adds ~50-65 min to a
  main run, up to 225 min when camera-box's hardware queue is busy.
- **`scripts/ci/camera_box_gate.py`**: `measure`, `token-check`, `release-check`. Only
  `.youtube_leg.pass is True` passes. `.overall_pass` also covers cameras, and a camera fault does
  not block us. A missing or malformed verdict, a cancelled run, an expired token and a run with
  another caller all fail.
- **The verdict file is `verdict-<harness id>.json`**, not the GitHub run id. There must be exactly
  one at the artifact's top level.
- **The dispatch returns no run id.** The newest workflow_dispatch run not listed before is the
  candidate, and `E2E_CALLER: restreamer@<version>` in its log proves it is ours.
- **A second dispatch on camera-box `main` cancels the first** (their workflow concurrency): the
  gate then fails as `unknown`. So does a newer pending job on their `loopback-e2e-hardware` group.
  Rerun the job once; two of the same failure are real.
- **A red verdict** keeps the evidence, because the next delivery of E2E-Test wipes the S3 prefix:
  artifact `camera-box-red-chunks` (chunks + verdict, from the gate job) and
  `camera-box-red-delivery-log` (job `camera-box-red-evidence`, on the box).
- **`camera-box-token-check`** runs on every CI run. It POSTs a dispatch for a ref that does not
  exist: 422 "No ref found" = may dispatch, nothing started; 401/403 = token lost. The secret is
  readable only by CI, so this is how its write access gets proved.
- **`release.yml` `validate`** runs `release-check`: E2E Gate and the measurement must be green on
  the tagged commit, also for a manual dispatch or a hand-pushed tag. Releases of commits older
  than the gate fail it.
- **Wiring** is pinned by `verify_camera_box_gate.py` (test-integrity, with `--self-test`); the
  script is tested by `tests/ci/test_camera_box_gate.py` with a fake `gh` (obs-scripts-test).
- **Attach by hand** to a run: `python3 scripts/ci/camera_box_gate.py measure --run-id <id>
  --caller restreamer@<version> --out <new dir>`.
