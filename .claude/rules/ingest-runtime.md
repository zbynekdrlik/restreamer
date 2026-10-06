---
paths:
  - "crates/rs-runtime/src/ingest_runtime*.rs"
  - "crates/rs-runtime/src/ingest_priority*.rs"
  - "crates/rs-runtime/src/inpoint_service*.rs"
  - "crates/rs-runtime/src/orchestrator.rs"
  - "crates/rs-endpoint/src/disk_pressure.rs"
  - "crates/rs-inpoint/src/flv_chunker_drain_tests.rs"
---

# The dedicated ingest runtime and the Windows priorities (#368)

OBS drops frames when its RTMP send stalls for more than ~700 ms. Whatever
runtime polls the xiu session tasks decides whether that happens. On the
app-wide runtime, two ~5-7 s stalls on 2026-10-04 cost 416 frames.

## Topology

- `InpointService::start` (the orchestrator's only way to start the inpoint)
  starts an `IngestRuntime`: a `current_thread` tokio runtime driven by ONE
  `std::thread` named `restreamer-ingest`. Its blocking pool is
  `restreamer-ingest-io`.
- `run_inpoint_loop` (bind probe, restart, crash backoff, heartbeat) stays on
  the MAIN runtime. It only spawns the RTMP server onto the ingest handle.
  Everything the server spawns follows it: xiu `ServerSession`s, the
  streams hub, `MediaReceiver`, the chunker's `tokio::fs` writes.
- Chunks and events cross to the main runtime through the existing tokio
  channels, which work across runtimes. Never `Handle::current()` or
  `tokio::spawn` main-runtime work from the ingest path, and never make the
  ingest path await something only the main runtime drives. One known
  exception is left: `InpointState::mark_connected`/`mark_disconnected` await
  the `rtmp_stable_since` tokio `Mutex` that API handlers also lock. A starved
  main-runtime waiter can hold up the receiver at a session start or end.
  Frames are safe, because xiu's channels are unbounded. The type is shared
  with rs-api's `AppState` and the Tauri tray state (since #234), so changing
  it is a follow-up of its own.
- `RtmpServer::serve`'s own `flush()` runs on the ingest runtime when the
  server stops. The loop's `flush()` afterwards (main runtime) finds the
  buffer empty.
- `InpointService::stop` does three things in order.
  1. It waits for the loop.
  2. It waits up to 5 s for the chunker's background writes
     (`FlvChunkSink::wait_for_writes`). Shutting a runtime down CANCELS its
     tasks, so a chunk still being written would never be reported. No report
     means no DB row and no upload.
  3. It shuts the ingest runtime down through `spawn_blocking`, because
     joining a thread must never block an async worker.
- Dropping an `IngestRuntime` also stops it (its stop sender drops).
- `IngestRuntime::shutdown` waits at most 15 s for the thread and returns
  whether it joined. A wedged thread does not hang THAT join. But `stop()`
  first awaits the supervision loop, which awaits the server task without a
  limit, so a wedge while the server runs still holds `stop()`. The tray's
  quit calls `exit(0)` after 500 ms anyway.
- If the ingest runtime cannot start, `InpointService::start` runs the
  server on the app runtime and logs an error. The service stays up
  (#106: degraded beats dead).

## Priorities (Windows)

- The task runs at `-Priority 4` (Normal) from `install.ps1` AND from the CI
  deploy step. Both unregister and re-register the task every run.
  `install_script_defaults.rs` pins both.
- `apply_process_priority` at `ServiceCore` startup raises a process still
  below Normal (a task registered before #368, e.g. a box upgraded by the
  in-app updater) and switches EcoQoS throttling off. It NEVER raises above
  Normal: stream OBS runs BelowNormal (camera-box's domain).
- It restores the CPU class ONLY. Task priority 7 also sets memory priority
  2 and I/O priority Low (measured on stream.lan, #368
  issuecomment-6012351712). Those stay low until the task is re-registered
  with `-Priority 4` (install.ps1 or the CI deploy).
- Only the `restreamer-ingest` thread gets `THREAD_PRIORITY_HIGHEST`. It is
  raised ON that thread (`SetThreadPriority(GetCurrentThread())`), before
  its runtime is built.
- Decisions live in `ingest_priority.rs` behind `PriorityOs`, tested with a
  fake. `ingest_priority_windows.rs` holds only the kernel32 calls and is
  in `.cargo/mutants.toml` `exclude_globs`.
- Type-check the Windows branch from dev2 with a scratch crate that pulls
  only `ingest_priority.rs` in through `#[path]` (deps: `log`, `windows-sys`
  0.61.2 with `Win32_System_Threading`), then
  `cargo +1.99.0 clippy --offline --all-targets --target x86_64-pc-windows-msvc -- -D warnings`.
  The `x86_64-pc-windows-msvc` std is installed for both dev2 toolchains
  (stable and 1.99.0) since 2026-10-06.

## Blocking calls stay off async workers

`disk_pressure::VolumeSampler` runs `sysinfo`'s volume enumeration with
`spawn_blocking` and waits at most 5 s. A timed-out enumeration stays in
flight, and the next sample waits for that same one: one stuck blocking
thread, never one per tick. Do the same for any new blocking call.

## Testing

- `inpoint_service_tests.rs` drives the inpoint as the orchestrator starts
  it, with an in-process `RtmpPusher` publisher on its own thread and
  runtime. It observes the chunker through an injected `WallClock`: the
  chunker reads it at every chunk boundary, on the thread that processes the
  frames, so the clock's call log gives both WHEN and WHICH thread. With
  `CHUNK = 50 ms` and a keyframe every 33 ms there is a clock read every
  ~66 ms while frames flow.
- Starve the main runtime with `std::thread::sleep` tasks, never with CPU
  hogs. `cargo test` runs a crate's tests as threads of one process.
- These tests pick a port by bind-0-and-drop, because the inpoint binds by
  address (as in production). Never use a fixed port.
- **Prove a restart reached the NEW server.** A publisher that connects just
  before the old server stops publishes into nothing. So wait until the loop
  has TAKEN the request: `restart_tx.capacity()` is back to
  `max_capacity()`. Then count only frames processed 300 ms after that, and
  retry the publish if there are none.
- **A deterministic write in flight:** give the chunker a fixed `WallClock`,
  so the chunk file name `chunk_<ms>_<index:06>.bin` is known. Then `mkfifo`
  that path (Unix only). The write blocks until the test opens the read end
  (`stop_still_reports_a_chunk_that_is_being_written`,
  `flv_chunker_drain_tests.rs`).
- **Prove that a stop JOINED the thread**, not only dropped it: a 300 ms
  `spawn_blocking` task on the ingest handle must be finished when `stop`
  returns. `is_ingest_running()` is false either way, so it proves nothing.
- A test-only accessor in production code gets `#[cfg(test)]`. Otherwise
  clippy `-D warnings` fails on dead code (`is_ingest_running`,
  `VolumeSampler::with_timeout`).
- clippy's `too_many_arguments` already fires at 7 parameters. Fold related
  settings into a struct (`VolumeSampler` carries the probe and the interval).
