---
paths:
  - ".github/workflows/ci.yml"
---

# CI "Mutation Testing" job: it is FAKE-GREEN until fixed (found 2026-10-05, #367 lane L2)

A green Mutation Testing check does NOT mean the mutants were tested. Prove the
diff locally instead (recipe below).

## Why it is fake-green

Two defects combine:

1. The step runs `cargo install cargo-mutants`, which installs the latest version
   (v27.1.0 on 2026-09-02). That version rejects the step's flags:
   `error: the argument '--in-place' cannot be used with '--jobs <JOBS>'`.
   `--in-place` already implies serial runs, so `--jobs 1` must go.
2. The retry loop captures `EXIT=$?` after `if cargo mutants …; then …; fi`.
   That is the exit status of the `if` compound (0), not of cargo-mutants.
   Every attempt logs `Attempt N failed (exit 0)` and the job exits 0.
   A run with genuine surviving mutants would be masked the same way.

Evidence: PR #365, run 33670131631, job 100381819470.

Fixing the gate is cross-cutting: every lane and PR in flight starts being
mutation-tested for real. It was handed to the supervisor as a follow-up
candidate. Do not quietly re-enable it inside an unrelated lane.

## Prove a diff has zero survivors on dev2 (what the gate should do)

On dev1:

```bash
git diff origin/main...HEAD > pr.diff
scp pr.diff newlevel@dev2:~/restreamer-bc-<lane>/   # airuleset:deploy-dirty-ok
```

On dev2, in `~/restreamer-bc-<lane>`:

```bash
SQLX_OFFLINE=true cargo mutants --list --in-diff pr.diff <same --exclude-re / -e as ci.yml>   # count first
SQLX_OFFLINE=true cargo mutants --in-diff pr.diff --in-place --timeout 300 --build-timeout 600 \
  --baseline=skip --output /tmp/<lane>-mutants-out <same excludes>
```

Read `mutants.out/missed.txt`; it must be empty. Reference run: 106 mutants for
the stall detector took ~10 min on dev2 (93 caught, 13 unviable, 0 missed).

## `#[cfg(windows)]` code

cargo-mutants does not evaluate `cfg`. Mutants in Windows-only code get
generated on ubuntu, never compiled, and reported as MISSED. Keep such code in
its own file that only copies raw FFI fields, keep the arithmetic in tested
cross-platform helpers, and add the file to the step's `-e` list
(`crates/rs-runtime/src/stall_resources_windows.rs` is the example).
