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
