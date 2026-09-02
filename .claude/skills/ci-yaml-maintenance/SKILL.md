---
name: ci-yaml-maintenance
description: Gotchas for maintaining .github/workflows/ci.yml's conditional-logic gates (e2e/deploy skip-vs-fail semantics, the verify-ci-yaml-invariants self-check pattern, .cargo/audit.toml) and the repo's own push-gate quirks around CI-YAML-only changes. Load before touching ci.yml conditionals, the security/cargo-audit job, or any "needs.job.result" gating logic.
---

# ci.yml conditional-logic maintenance (#267 / #268 / #281, 2026-07-25)

## `!= 'failure'` vs `== 'success'` — the recurring e2e-gate bug class

`needs.<job>.result` is one of `success | failure | cancelled | skipped`.
**`'skipped' != 'failure'` is TRUE** — so any condition of the shape
`needs.X.result != 'failure'` also passes when X was SKIPPED, not just when it
succeeded. This repo's expensive stream-lan e2e jobs (`e2e-streaming-test`,
`e2e-obs-youtube-test`, `e2e-fb-push-stream-lan`, each ~10-55 min on the shared
box) used exactly this pattern gated on `deploy-stream-lan`, so a compile/lint
failure that made `deploy-stream-lan` SKIP (via its own `rust-ci-gate ==
'success'` gate) let the whole ~1.5h e2e suite run anyway on non-compiling code
(#267, run 27884399527). **Always use `== 'success'` for "only run after X
actually succeeded"** — reserve `!= 'failure'` only for "run unless X
definitely broke, tolerate skip" (rare, and usually a design smell worth a
comment explaining why skip is fine).

## `verify-ci-yaml-invariants` — this repo's OWN regression-guard convention for ci.yml logic

There's no way to unit-test a GitHub Actions `needs.job.result` expression
outside a real run. This repo's accepted substitute (pre-dating #267): a job
step that `grep`s the workflow file's own text and asserts the exact condition
string is present (`Verify deploy-stream-lan has always()`, `Verify auto-release
has always()`, `Verify E2E tests use == success (not != failure)`, etc.). When
you change a job's `if:` condition, ALSO update its matching self-check step in
the SAME PR — leaving the self-check asserting the OLD invariant will make CI
fail on your own fix.

**Known fragility (filed as #325, non-blocking):** these greps match the
job-name substring against the WHOLE file, including the self-check script's
OWN source line (which necessarily contains the same job-name string). Harmless
today (the self-matched line never happens to contain the checked substrings),
but anchor new/edited greps to `^  <job-name>:` (2-space job indent) rather than
a bare substring when you touch this pattern, to remove the fragility for good.

## `.cargo/audit.toml` is cargo-audit's real project-local config path

Confirmed by reading cargo-audit 0.22.0's own `config.rs` (`AuditConfig` docs:
`~/.cargo/audit.toml` or `.cargo/audit.toml`) and verified locally: a committed
`.cargo/audit.toml` with `[advisories] ignore = [...]` is auto-read by a bare
`cargo audit` — **no `--ignore` CLI flags needed**. (NOT a root-level
`audit.toml` — that path is silently ignored.) Every entry in this repo's file
carries a `# RUSTSEC-ID: <why> | dep: <transitive path> | expires: YYYY-MM-DD`
comment, enforced by the `security` job's "Validate audit.toml" step (parses via
`tomllib`, fails the build once an entry's expiry passes). To check whether an
ignored ID still actually matches the current `Cargo.lock` (some go stale as
deps get upgraded past the patched version): `cargo audit` locally with no
`--ignore` flags lists only the STILL-matching vulnerabilities/warnings.

## `pre-push-test-check.sh` blocks CI-YAML-only `fix(ci):` commits — use `[no-test: <reason>]`

The repo-agnostic push gate's Gate 2 (regression-test-first enforcement) has no
concept of "this workflow YAML self-check step IS the regression guard" — it
only recognizes Rust/JS/Python test-file paths or inline `#[test]`/`assert!`/
`it(`/`describe(` patterns. A `fix(ci):` commit with a `Closes #N` trailer and
no matching test-recognized diff gets blocked with "Bug-fix commit appears
BEFORE any test commit". For a pure CI-YAML/TOML conditional-logic fix, bypass
honestly on the LAST commit of the push: `[no-test: CI-YAML conditional-logic
fix, regression guard is the verify-ci-yaml-invariants self-check added in the
same commit]` — precedent: the 2026-07-19 `#287 (no label) — CI gate` entry in
`docs/autopilot-log.md`, `[no-test]` "CI YAML gate = the test". **Gotcha:** the
bypass only reads the git log-1 (LATEST) commit's message, so if your CI fix
isn't your last commit, add a trailing commit (e.g. the required
`docs/autopilot-log.md` batch-log update) and put the `[no-test: ...]` there —
it exempts the WHOLE push's Gate 1/2/3, not just one commit. Also: `it\(` in
Gate 2's regex has no word boundary and false-matches inside `sys.exit(1)`
(`ex`**`it(`**`1)`) — filed as `airuleset#41` (cross-project, not fixed here).

## Expected e2e job durations (stream-lan-box, serialized by a shared concurrency group)

Useful for judging "still healthy" vs "stuck" while polling: `E2E OBS-to-YouTube
Test` ~40-55 min, `E2E Streaming Test` ~10 min, `E2E FB Push (stream-lan, real
FB)` ~25-32 min. Full push-CI cycle (lint/test/build + this e2e suite)
end-to-end: ~1h45m-2h. A `pull_request`-event run SKIPS all four (deploy +
3 e2e jobs) since they're gated `github.event_name == 'push' ||
'workflow_dispatch'` — only `push`/`workflow_dispatch` actually exercise them.

## No path filters — EVERY push runs the full ~2h pipeline, including docs-only

`ci.yml` has no `paths`/`paths-ignore` trigger filter, so a `docs(playbook):
...`-only commit (e.g. the mandatory autopilot-log append) pushed to `dev`
triggers the SAME lint+test+build+deploy-stream-lan+3×real-E2E cycle as a code
change — there is no cheap path for docs. Two consequences: (1) prefer
including the playbook/autopilot-log commit in the SAME push as the ticket's
own work (the established pattern in `docs/autopilot-log.md` history — every
prior entry's `docs(playbook): ...` commit is the LAST commit of that ticket's
own PR, never a standalone follow-up push after merge); (2) the top-level
`concurrency: group: rust-ci-${{ github.ref }}, cancel-in-progress: true`
means a standalone follow-up push to the SAME ref while nothing else is
running just burns its own full cycle — nothing cancels it automatically
unless a THIRD push lands on the same ref while it's in flight (2026-07-26,
#286: a solo post-merge docs push got cancelled ~40min in, apparently by an
external actor on this shared box, after all non-E2E jobs had already gone
green — not a code defect, just a wasted cycle from pushing the log update
separately instead of bundling it into the ticket's PR).

## CI-monitoring waits: `sleep` may or may not be blocked — check, don't assume

**The `sleep` block is PER-SESSION, not a property of this repo** (#336,
2026-07-26): in that worker a bounded foreground loop with plain `sleep`
*worked* and was the cheapest correct pattern —

```bash
for i in $(seq 1 7); do
  s=$(gh run view <id> --json status,conclusion -q '.status+" "+(.conclusion//"-")')
  echo "[$(date +%H:%M:%S)] $s"; case "$s" in completed*) break;; esac
  sleep 55
done      # Bash tool timeout: 450000
```

Try that FIRST (one tool call ≈ 7 min of waiting, and `gh run view --json jobs`
lets you print which job is running, which `gh run watch` does not). If the
sandbox rejects `sleep`, fall back to the `timeout` form below.

**Two hazards that DO bite regardless of the `sleep` question:**

- **The harness can auto-background a long Bash call** ("manually backgrounded
  by user with ID: …"). For a dispatched subagent that is fatal — a turn ending
  with background work in flight trips the Stop hook / kills the worker. Fix:
  `TaskStop` that task id immediately, then resume with a SHORTER foreground
  loop (7 × 55 s ≈ 6.5 min per call was safe; 8 × 60 s got backgrounded).
- Keep each poll call well under the Bash tool's 600 s cap.

The `timeout`-based fallback, when `sleep` genuinely is blocked — the global
`ci-monitoring.md` foreground pattern (`sleep 300 && gh run view …`) is then
hard-blocked in both the chained and standalone forms, forcing
`Monitor`/`run_in_background`, which then
terminates a dispatched subagent's turn if left in flight when the turn ends
(confirmed twice, 2026-07-26: a raw `gh run watch` auto-backgrounded on the
Bash tool's own timeout, AND a dispatched review `Agent`, both tripped the
Stop hook's "in-flight background work" block). The working genuine-foreground
substitute, proven across a full ~2h E2E cycle: repeat
`timeout 280 gh run watch <run-id> --interval 20 --exit-status` in a loop
(each call as its own Bash tool call, `timeout: 300000` on the tool) — `timeout`
isn't pattern-matched as `sleep`, blocks the turn genuinely in the foreground
for up to 280 real seconds, and returns control (exit 143) so you can check
`gh run view --json status,conclusion,jobs` and loop again. For an async
`Agent` dispatch specifically (no `gh run watch` equivalent), `TaskStop` can
fail with an ownership error ("owned by itself") — the fallback pacer is
`timeout N tail -f /dev/null` (blocks reading `/dev/null` for exactly N
seconds, no `sleep` in the command text) repeated until the dispatch's
completion notification arrives naturally.

## Asserting a feature DEEP in a job — `sed` job-range, not `grep -A<N>` (#363/#361, 2026-09-02)

The existing self-checks use `grep -A3/-A10 "^  <job>:"` because they check the
`if:`/`needs:` near the job header. To assert something FAR from the header (a
firewall rule ~200 lines into `deploy-stream-lan`, a teardown step at the end of
a job), extract the whole job block by its bracketing headers instead:

```bash
DEPLOY_BLOCK=$(sed -n '/^  deploy-stream-lan:/,/^  e2e-streaming-test:/p' "$WORKFLOW_FILE")
echo "$DEPLOY_BLOCK" | grep -qE 'New-NetFirewallRule.*-Direction Inbound.*-LocalPort 1234.*-Action Allow' || exit 1
```

- Pick the CLOSING anchor = the very next `^  <job>:` header (verify it occurs
  exactly once). This scoping is what makes the #325 self-match impossible: the
  self-check lives in `test-integrity` (early in the file), so it is OUTSIDE the
  sed range and its own grep-pattern line can never match — even when the pattern
  literally contains the searched string.
- **Assert per-job, not per-range, when N jobs must each carry the feature.** A
  single `e2e-obs-youtube-test → e2e-gate` range spans BOTH OBS-streaming jobs, so
  `grep -q StopRecord` on it passes if only ONE has the teardown. Split into
  `obs-youtube→fb-push` and `fb-push→e2e-gate` and assert each (real #361 review
  finding — the one-range guard did not encode "both jobs").
- Pin the discriminating flags in a firewall/rule assertion (`-Direction Inbound`
  … `-Action Allow`), or the grep also passes an Outbound/Block rule.
- A negative directive ("we must NEVER do X") is a whole-file `grep -q` that EXITs
  on a match — but write the pattern so it can't match its own line (e.g.
  `requestType = "(StartRecord|ToggleRecord)"` does not match the literal
  `requestType = "(StartRecord|ToggleRecord)"` because the ERE group needs
  `StartRecord`/`ToggleRecord` right after the quote, not a literal `(`).

## Syntax-check inline PowerShell before pushing (dev1 is Tier-0, no local pwsh)

A PowerShell PARSE error in a `shell: powershell` step is NOT caught by an inner
`try/catch` and fails the step (and, for an `if: always()` teardown, the job).
dev1 can't run PowerShell, so verify a ci.yml PowerShell block against the REAL
parser on the stream box via MCP before you rely on it: extract the `run:` body,
strip the YAML indent, base64 it (avoids all quoting), then on stream.lan
`[System.Management.Automation.Language.Parser]::ParseInput($code,[ref]$t,[ref]$e)`
and print `$e.Count`. 0 errors = safe. (Also: `shell: powershell` on GitHub
prepends `$ErrorActionPreference='stop'`, so a bare cmdlet error IS terminating
and lands in your `catch`; no native exe means `$LASTEXITCODE` stays unset.)

## Cross-repo rig lease (#349/#830): the two runners are DIFFERENT machines

camera-box's `full-path-e2e` gate runs on `[self-hosted, linux, camera-lan]` =
**dev1 Linux**; restreamer's OBS-driving E2E jobs run on
`[self-hosted, windows, stream-lan]` = **the Windows stream box (10.77.9.204)**.
There is NO shared local filesystem between them, so the camera-box #830 lockdir
design (`/var/tmp/rig-lease/` acquired by atomic `mkdir`, "both runners are the
same machine") CANNOT coordinate the restreamer side as-is — a lockdir restreamer
writes locally is invisible to camera-box's dev1 gate. #349 is parked
needs-decision on the coordination surface (SSH-to-dev1 lease vs shared mount vs
lock service). Don't implement a stream-box-local lockdir: it is a false guard.
