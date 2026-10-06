---
paths:
  - ".github/workflows/ci.yml"
  - ".cargo/mutants.toml"
  - ".config/nextest.toml"
---

# CI mutation gate: bounded, sharded, required (#367)

The PR gate mutates only the lines in the PR diff and runs only the MUTATED
crate's tests. It is real since integ-b3 (`76d9517e`) and bounded since #367.

## Shape

- **`mutation-plan`** runs `cargo mutants --list --in-diff pr.diff` and sets
  N = ceil(count / `MUTANTS_PER_SHARD`), capped at `MAX_SHARDS` (64). A bigger
  diff fails the plan with "split this PR".
- **`mutation-testing`** is a matrix of N shards (`--shard k/N`, slice sharding)
  on github-hosted runners, with `timeout-minutes: 20` and `fail-fast: false`.
  - Each shard cold-builds its own one or two packages. The rust-cache step
    caches only the registry (`cache-targets: "false"`).
  - Every non-zero cargo-mutants exit fails the shard at once: 2 MISSED,
    3 TIMEOUT (3 wins when both happen), 4 the tests fail unmutated. There is
    no retry.
- **The Rust CI Gate requires `mutation-testing` == success on PRs.** A matrix
  job is success only if every shard is. Branch protection requires only the
  gates, so this is what makes the mutation check binding.
- **The test-integrity guard** "Verify the mutation gate" parses the workflow
  and both configs. If you change any of the above, update the guard in the
  same commit.

## Where the settings live

- **`.cargo/mutants.toml`** holds `profile = "mutants"`, `test_tool =
  "nextest"`, `test_workspace = false` and EVERY exclude. ci.yml passes no
  scope flags, so a local run tests exactly what CI tests.
- **Cargo.toml `[profile.mutants]`** is the test profile with `debug = "none"`.
- **The shard step exports `CARGO_INCREMENTAL=1`.** The toolchain and cache
  actions export 0, but each mutant rebuilds one edited crate. Measured on
  rs-api: 7.7 s per rebuild with it, 22.5 s without.

## Budget overrun = setup bug

A shard near 15 min means MUTANTS_PER_SHARD is too high, or a package suite
got slower. Lower the constant or speed the suite up. NEVER raise
`timeout-minutes`.

Measured at 4 CPUs on #367 (issuecomment-6006792689):
- package suites: rs-delivery 27.5 s, rs-api ~19 s, rs-inpoint 14 s, the rest
  under 7 s;
- cold build: ~130-215 s;
- a 20-mutant all-rs-api shard with 13 MISSED: 566 s.

A MISSED mutant pays its package's whole suite; a caught one stops at the
first failing test.

## nextest isolation trap

nextest runs every test in its own process, and runs a package's test
binaries in parallel. `cargo test` runs the binaries one after another, with
each binary's tests as threads of one process. So a test that relies on an
in-process lock or a fixed port can fail under nextest with NO mutation
applied. Every mutant of that package then reads as "caught": a fake-green
gate.

`rs-delivery::test_file_sink_e2e` (real port 1935) is such a test.
`.config/nextest.toml` runs it exclusively and first.

The shards run `--baseline=run`, so such a test now fails the shard loudly:
cargo-mutants exits 4, "the tests fail with no mutation applied". It does not
silently count every mutant as caught. This costs nothing in practice,
because the baseline build is the shard's cold build anyway (shard 19/45 took
553 s with the baseline and 574 s without). Keep it.

Before you add a test that binds a fixed port or shares a file/env across
tests, add it to that override, or make it independent. Then run
`cargo nextest run --cargo-profile mutants -p <crate>` 2-3 times; it must be
green.

## Run it locally (dev2)

```bash
git diff origin/main...HEAD > pr.diff     # on dev1; scp to the dev2 lane copy
SQLX_OFFLINE=true CARGO_INCREMENTAL=1 cargo mutants --list --in-diff pr.diff | wc -l
SQLX_OFFLINE=true CARGO_INCREMENTAL=1 cargo mutants --in-diff pr.diff --in-place \
  --baseline=run --timeout 120 --build-timeout 600 --output <dir> [--shard k/N]
```

- Install the same pinned versions CI uses (`cargo-mutants@27.1.0`,
  `cargo-nextest@0.9.146`); the release tarballs work without `cargo install`.
- `--re '<fn names>'` plus `-f 'crates/<crate>/**'` re-checks only the
  functions you just changed.
- Never run two lanes that both run rs-delivery tests at once: both bind 1935.
- Never overwrite a running bash script: bash reads it while it runs.
  Upload a new version under a new name.

`missed.txt` and `timeout.txt` must be empty. To kill a survivor, see
`.claude/rules/mutation-killable-code.md`.

## `#[cfg(windows)]` code

cargo-mutants does not evaluate `cfg`. Mutants in Windows-only code get
generated on ubuntu, never compiled, and reported as MISSED. Keep such code in
its own `*_windows.rs` file that only makes the OS calls, keep every decision
in tested cross-platform helpers, and add the file to `exclude_globs`.
Examples: `stall_resources_windows.rs`, and `rtmp_bind_windows.rs` (whose
netstat/tasklist parsing lives in `rtmp_bind.rs`).
