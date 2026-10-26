---
paths:
  - "crates/**/tests/**"
  - "crates/**/*_tests.rs"
---

# Concurrency / stress tests must not depend on runner speed (#382)

GitHub's Windows runners are sometimes starved: one job seeded 800 SQLite rows
in ~4.5 s instead of ~0.3 s, and other slow runs took up to 86 s. A test that
counts work inside a fixed wall-clock window (`claim for 4 s, then require
>= 100`) failed there with no logic fault (`uploader_busy_stress`, PR run
37558129876: 19 claims).

- **Stop on a COUNT, not a clock.** Run the workers until a known amount of
  work is done (all seeded rows claimed), then assert on correctness.
- **Bound it with a liveness cap that scales to the runner.** Time a serial
  baseline phase on the same runner (the seeding loop) and use
  `max(floor, k x baseline)`. A fixed cap just moves the flake. The cap trips
  only on a hang or a real crawl, and its message must differ from the other
  failures and carry their counters (BUSY hits, missing ids).
- **Wrap the whole phase in ONE `tokio::time::timeout`**, and abort the tasks
  through `abort_handle()`s collected before the handles move. A per-loop
  deadline check never fires when a single await hangs.
- **Count the stop condition on the right set.** Background writers that
  insert more rows during the run make "N claims" race past the seeded rows;
  count claims of the seeded ids only.
- **Prove the contention really happened**: count the background writer
  rounds whose writes all succeeded and assert `> 0`, so "zero errors" is
  never the result of an uncontended run.
- **Prove the rewrite with temporary injections, never committed**: a per-write
  delay (old shape fails, new passes), a never-returning await (the cap
  fails), and the old buggy implementation (the real assertion still fails).

## Waiting for ONE event (a chunk, a report): a hang bound, never a quiet window

The chunker tests waited 500 ms for a keyframe-closed chunk, and their drains
stopped at the first 500 ms with no chunk. Such a chunk is broadcast only after
a spawned disk write, and a starved Windows runner took longer than that (#382,
main run 39270735842). A drain then silently left a chunk out.

- **Expecting one event:** wait with ONE named liveness bound
  (`CHUNK_ARRIVAL_LIVENESS`, 10 s, `flv_chunker_tests.rs`). It only catches a
  hang; no assertion depends on the latency.
- **Collecting "everything so far":** first wait for the producer to be idle
  (`FlvChunkSink::wait_for_writes`, and assert 0 still pending with its own
  message), then take what is queued with `try_recv`. A "stop after N ms of
  silence" loop is a speed assertion in disguise.
- A chunk from `flush()` is already in the channel when `flush` returns
  (`write_and_notify` is awaited), so only `spawn_write` chunks were racy.
- Proof recipe (never committed): a sleep before the spawned write fails
  the old tests and passes the new ones; a `pending()` write fails the new ones
  at the bound.

## Reproducing a CI-only timing failure (#367)

- An idle loop proves little (21 green runs); build load exposes races (a concurrent
  from-scratch workspace build gave 1-3 % failures, busy-loop hogs 0/100).
- Build once (`cargo test --workspace --locked --no-run --message-format=json`), run each
  `executable` from `dirname(manifest_path)` N times under `taskset -c 0-3` (CI's 4 vCPU)
  with `</dev/null`, interleaving the RED and GREEN binaries in one loop.
