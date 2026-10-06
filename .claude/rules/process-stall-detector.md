---
paths:
  - "crates/rs-runtime/src/stall_*.rs"
  - "crates/rs-runtime/tests/stall_detector_runtime.rs"
---

# Process-stall detector (#367 part 2): invariants and gotchas

The detector exists so that the NEXT whole-process freeze names its own cause.
On 2026-10-01 a 35.7 s freeze left only a hole in `restreamer.log`.

**Where the evidence is.** On stream.lan it is
`C:\ProgramData\Restreamer\logs\stall.log` (JSON lines, rotated to
`stall.log.old` at 1 MB), plus `ProcessStall` audit rows (`/api/v1/audit?action=process_stall`).

**Two runtimes, two detectors (#368).** The RTMP ingest runs on its own
runtime (`.claude/rules/ingest-runtime.md`), and a second detector instance
probes it. Every record and every `ProcessStall` row carries `runtime`:
`main` (evidence in `logs/stall.log`) or `ingest` (evidence in
`logs/stall-ingest.log`). A whole-process freeze shows in BOTH files; a
stall in only one names the runtime that stopped polling.
- Start one with `spawn_runtime_stall_detector(runtime, ..)` /
  `start_for_runtime(runtime, handle, ..)`. `spawn_stall_detector` and
  `start_for_service` are the `main` shorthands.
- The label is added in `Detector::write` (`stall_log::with_runtime`), so the
  record builders stay label-free.

- Every process start writes a `detector_started` line, and every detector
  exit writes `detector_stopped` with its reason. No `stall_start` between the
  two (or after a still-running start) means there was no stall. It does NOT
  mean the detector was missing.
- `class` separates two kinds of stall:
  - `runtime_starved`: the OS kept running us, but tokio stopped polling.
  - `whole_process`: the detector thread itself was not scheduled.
- `stall_start.resources` vs `baseline` is the before/after view of handles,
  kernel pools and commit.

## Invariants: break one and the detector lies or dies

- **Nothing async and no `log` crate on the detector thread while a stall may
  be open.** The `log`/tracing pipeline, or a lock the stalled runtime holds,
  could block the very thread that has to record the stall. Write errors are
  parked in `write_error` and surface with the post-recovery warning and
  audit row.
- **`rs_core::audit::record` called from an OS thread needs
  `Handle::enter()`.** On a full channel it `tokio::spawn`s a retry for
  Warn+ rows, and that spawn panics without a runtime context.
- **Spawn the next probe BEFORE the evidence I/O.** The tracker stamps a probe
  as sent at the tick's `now`. A slow snapshot plus `sync_data` append done
  first would inflate the next round trip and produce a false `probe_slow`
  stall.
- **Detect runtime shutdown by a probe that FINISHED with no answer.** tokio
  cancels tasks spawned on (or owned by) a closed runtime; it does not panic.
  Read `JoinHandle::is_finished()` BEFORE the ack, or an answered probe can
  look cancelled. Never report shutdown as a stall.
- **A stall closes only on its closing-probe SEQUENCE (`close_seq`), never on
  a timestamp comparison.** For `probe_overdue` / `probe_slow` it is the probe
  that revealed the stall. For `detector_late` it is the FRESH probe issued
  after the freeze, because the probe answered just before the freeze proves
  nothing about now.
- **`detector_late` compares the detector's WHOLE silence (`now - tick_start`)
  with the 5 s threshold, not just its overshoot past the 1 s wait.** Using only
  the overshoot left a 5-6 s blind zone. Without this trigger at all, a freeze
  that starts between probes is invisible, because probes answer in
  microseconds.
- **The Windows FFI (`stall_resources_windows.rs`) only copies raw fields.**
  All arithmetic, including PERFORMANCE_INFORMATION pages × `PageSize`, lives
  in the cross-platform `ResourceSnapshot::set_*` helpers. Those helpers are
  unit-tested on Linux and mutation-tested by the ubuntu job, which excludes
  the FFI file because it cannot compile it.
- **Every exit writes `detector_stopped`** (`stop_requested`,
  `runtime_shut_down`, or `panic: …`). The thread body runs under
  `catch_unwind`, so silence in `stall.log` is never ambiguous.

## Testing

- The decision logic is the pure `StallTracker`. Drive it with explicit
  `Instant`s (see the `Sim` helper in `stall_detector_tests.rs`). Never sleep in
  unit tests.
- Integration tests run on a real runtime with scaled thresholds (50 ms probe,
  300 ms stall). `tick_late_threshold` stays at 2 s, so CI scheduler jitter
  (Windows timer resolution is ~15 ms) can never flip `runtime_starved` into
  `whole_process`.
- **300 ms is only for tests that PROVOKE a stall** (a 1.5 s block). A test
  that asserts NO stall uses the 2 s `QUIET_THRESHOLD`. On a loaded box the
  OS starves a responsive process for 300 ms+, and the detector is right to
  report it. With 300 ms on dev2 under build load,
  `responsive_runtime_reports_no_stall` failed 3 of 187 runs and
  `detector_exits_quietly_when_the_runtime_shuts_down` failed once (#367).
  These tests stay able to catch a real bug in two different ways:
  - The responsive test watches for longer than the threshold (a const
    assert pins that), so a probe that is never answered still trips it.
  - The shutdown test runs for only 200 ms. What catches a broken exit there
    is its 10 s wait for the detector to exit.
- **Block the runtime only once the first probe EXISTS.** The detector spawns
  it a moment after `detector_started` is on disk. Under load that took
  250 ms, so the block began first, and the stall measured 1250 ms for a
  1500 ms block (1 run in 200). Wait for `rt.metrics().num_alive_tasks() > 0`
  (a stable tokio API). The stall then covers the whole block. The test
  measures the block with `Instant`, the same clock the detector uses, and
  asserts that the stall is at least that long, with no slack.
- **A `current_thread` runtime is NOT driven between `block_on` calls.** Time
  spent outside `block_on`, or a `std::thread::sleep` inside one, starves it.
  That is the cheapest real "blocked runtime" stimulus.
- To prove the thread really exited after a guard drop, give the detector the
  ONLY audit sender. The channel closes exactly when the thread has dropped its
  state.

## Verifying the `#[cfg(windows)]` code from Linux

CI's `windows-latest` Test job compiles and runs it, including the
Windows-only asserts in `resource_sample_reads_real_process_and_system_memory`.

Before pushing, you can type-check it on dev2. The `x86_64-pc-windows-msvc`
std target has been installed there since 2026-10-05.

- A whole-crate `--target x86_64-pc-windows-msvc` check cannot work, because
  sqlite/openssl build scripts need an MSVC C toolchain.
- Instead, use a scratch crate that pulls only `stall_resources.rs` in through
  `#[path]`. That file depends on nothing but std, `serde_json` and
  `windows-sys`/`sysinfo`, so keep it that way.

```toml
# ~/restreamer-bc-<lane>-wincheck/Cargo.toml  (lib.rs: #[path = "<checkout>/crates/rs-runtime/src/stall_resources.rs"] pub mod resources;)
[dependencies]
serde_json = "1"
[target.'cfg(windows)'.dependencies]
windows-sys = { version = "=0.61.2", features = ["Win32_System_ProcessStatus", "Win32_System_Threading"] }
[target.'cfg(not(windows))'.dependencies]
sysinfo = { version = "=0.37.2", default-features = false, features = ["system"] }
```

Then run, on dev2:

```bash
cargo clippy --offline --target x86_64-pc-windows-msvc -- -D warnings
```

The output must show `Checking windows-sys v0.61.2`, which proves the
`cfg(windows)` branch was compiled.

## dev2 lane mechanics that cost time here

- `~/restreamer-buildcheck` had **no `target/` at all** on 2026-10-05, so
  `cp -al` from it gives no warm build. Hardlink-copy a lane dir that still
  has a warm `target/` instead.
  - `du -sh ~/restreamer-bc-*/target` shows which lane dirs still have one.
  - First check that no cargo process is running in that lane dir
    (`ps -eo args | grep restreamer-bc-<lane>`): hardlinked `target/` files can
    be rewritten in place by its owner.
- An inline `ssh … 'setsid … &'` holds the ssh session open until the build
  ends, unless every fd is redirected. The worktree command checker also
  refuses complex inline shapes. What works: put the build in a small script,
  `scp` it over, then launch it with
  `ssh -f newlevel@dev2 'setsid bash ~/<script>.sh > /tmp/<log> 2>&1 < /dev/null &'`,
  and poll its sentinel file from a local script.
