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
- **A test that reads state only AFTER the function returned misses a
  mid-run mutant.** `run_warmup_loop`'s `if !ep_cfg.is_fast` -> `if
  ep_cfg.is_fast` survived (#192): the fast-endpoint test read the stats after
  warmup ended, when the end had already reset the mode to "normal". Probe
  the state WHILE the loop runs (unreachable target + a probe task that sends
  the stop), as `warmup_fast_endpoint_never_shows_warmup_while_filling` does.
- **Equivalent mutants are not a reason to weaken the code.** If `x > 0` vs
  `x >= 0` truly changes nothing, restructure (`std::mem::take` + a tested
  helper) rather than leave a survivor.

More from the 109 survivors of PR #365's diff (#367, bounded-gate lane):

- **Handlers tested only from rs-service survive as `Ok(200)`.** A handler
  replaced by `Ok(Default::default())` still answers 200. Call the handler
  directly in its own crate and assert the side effect: the DB row, the
  broadcast `WsEvent`, the 404 (`rs-api/src/handlers_crud_tests.rs`).
- **`<` vs `<=` on a continuous value is an equivalent mutant at the call
  site.** An `age < ttl` with a real `Instant` never hits equality. Move the
  comparison into a tiny tested helper (`rs-api` `cache_ttl::is_fresh`,
  `fast_keepalive_escalation::next_escalation_tick`, rs-cloud
  `retry_backoff`) and assert the exact boundary there.
- **A hand-advanced `while` counter turns `+=` into an infinite loop.** That is
  a TIMEOUT, not a caught mutant. Use a stepped range
  (`(first..b).step_by(n)`, rs-endpoint `throughput.rs`).
- **An unbounded `task.await` in a paused-clock test hangs when a mutant
  stops the task from finishing.** Wrap it in `tokio::time::timeout`, so the
  mutant fails the test instead of timing out.
- **Overlapping error-class flags cannot be separated with real errors.**
  reqwest marks a refused connection both `is_connect` and `is_request`. Test
  the decision as a pure function of the flags (rs-cloud
  `transport_error_is_transient`).
- **A fixed external URL needs a test override.** Follow the
  `FB_GRAPH_API_BASE` / `YOUTUBE_API_BASE` pattern, and take paths such as
  the running exe as parameters (`find_bundled_binary(exe)`).
  - **The override is process-global.** EVERY test that sets it, and every
    test that READS a value derived from it, must take one shared lock.
    `cargo test` runs a crate's tests as threads, so an unlocked reader can
    see another test's mock URL. nextest (one process per test) hides this
    race, so the mutation gate never sees it.
  - **When the URL decides which code runs, the override is `#[cfg(test)]`
    only.** The GitHub release and its sha256 sidecar come from the same
    server, so a production override would let whoever sets the variable
    choose the VPS binary. `delivery_binary.rs` `release_base()` is the
    example; its lock is `RELEASE_ENV_LOCK`.
- **A redundant guard is an equivalent mutant.** Delete the guard rather than
  test around it: `record_bytes` checked `b > open` before a
  `finalize_up_to` that already no-ops, and `if delta > max { max = delta }`
  became `max.max(delta)`.

From the #357 av-gate lane:

- **A guard on an OPTIONAL collaborator needs a test with it present AND
  passing.** `Some(r) if !bucket_allows(r, ..)` survived as `true` because every
  test ran with the bucket `None`; one test with a roomy bucket killed it.
- **A hand-rolled constant-time compare leaves an unkillable mutant.** An
  OR-fold over XORed digest bytes survives `|` -> `^` (two differences would have
  to cancel). Compare the SHA-256 digests with `==` instead: the timing then only
  reveals matching DIGEST bytes, never a token prefix.
- **A counter grown inline in an endless loop** (`rounds + 1`) survives as
  `rounds * 1`: the loop test just finishes faster. Put the step in a pure fn
  (`next_failed_rounds`) and assert its boundaries.

Run it on dev2 before returning a lane, with the recipe in
`.claude/rules/ci-mutation-gate.md`. The excludes and levers now live in
`.cargo/mutants.toml`, so there is nothing to copy from ci.yml.
`missed.txt` and `timeout.txt` must be empty.
