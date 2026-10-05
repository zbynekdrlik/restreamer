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

- Every process start writes a `detector_started` line. If that line is
  there and no `stall_start` follows, there was no stall. It does NOT mean the
  detector was missing.
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
- **A `detector_late` stall closes only on a probe sent at or after
  `started_at`.** The probe answered just before the freeze must not close it
  (the rule is `probe.sent_at >= open.started_at`). Without the detector-late
  trigger, a freeze that starts between probes is invisible, because probes
  answer in microseconds.

## Testing

- The decision logic is the pure `StallTracker`. Drive it with explicit
  `Instant`s (see the `Sim` helper in `stall_detector_tests.rs`). Never sleep in
  unit tests.
- Integration tests run on a real runtime with scaled thresholds (50 ms probe,
  300 ms stall). `tick_late_threshold` stays at 2 s, so CI scheduler jitter
  (Windows timer resolution is ~15 ms) can never flip `runtime_starved` into
  `whole_process`.
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
