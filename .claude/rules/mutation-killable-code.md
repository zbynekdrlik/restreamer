---
paths:
  - "crates/**/*_tests.rs"
  - "crates/**/tests/**/*.rs"
  - "crates/rs-inpoint/src/**"
  - "crates/rs-rtmp-push/src/**"
---

# Code and tests cargo-mutants can verify (#367 lane L1)

The PR gate mutates only the lines in the diff, and runs only the tests of
the MUTATED crate. Once it is real (integ-b3 76d9517e), any MISSED mutant
(exit 2) or TIMEOUT fails the build. What cost time on #367:

- **A crate's function tested only from another crate survives.**
  `AvInvariantGuard::latched_delta_ms` (rs-rtmp-push) was asserted only by an
  rs-inpoint test, so `-> None` was MISSED. Test it in its own crate.
- **Log-only branches cannot be killed.** `if raw_sleep_ms >= 2_000 {
  warn!(..) }`, counters that are only logged, a log-only `fn` replaced by
  `()`. Move the decision into a small pure function that returns what to
  log, and unit-test it (`pusher.rs` `tag_sleep` / `TagSleep::is_long`,
  `frame_stats::FrameStats::count`, `Session::frames_resumed`). Do not reach
  for `--exclude-re` first.
- **A `match` with a `_` arm gets "delete match arm" mutants.** An arm that
  only logs then survives. For a log-only match, list every variant with no
  wildcard (`flv_chunker_ingest.rs` step logging).
- **Moving code puts every moved line into the diff.** Old, never-tested
  branches get mutated (the 50 MB force-flush in the chunker had no test).
- **Paused-clock tests hang under a mutant that makes a task spin** (retry
  delay 0, a probe loop): the runtime never idles, so `start_paused` time
  never advances and every `sleep`/`timeout` waits forever: TIMEOUT. Start
  each such test with a real-time watchdog that aborts the test process
  after 30 s (`media_receiver_tests.rs` `watchdog()`), and bound every loop
  over a channel by a count, not only by a virtual-time timeout.
- **Equivalent mutants are not a reason to weaken the code.** If `x > 0` vs
  `x >= 0` truly changes nothing, restructure (`std::mem::take` + a tested
  helper) rather than leave a survivor.

Run it on dev2 before returning a lane: a lane-private copy of the warm
checkout (`cp -al target` once), `git diff origin/main...HEAD > pr.diff`,
the `--exclude-re`/`-e` arguments copied from the ci.yml mutation step, then
`cargo mutants --in-diff pr.diff --in-place --timeout 300 --build-timeout 600
--baseline=skip --output <dir>`; `missed.txt` and `timeout.txt` must be
empty. ~250 mutants take about 20 min uncontended.
