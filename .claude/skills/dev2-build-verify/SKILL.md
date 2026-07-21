---
name: dev2-build-verify
description: How to actually compile / test / clippy / run the frontend E2E for restreamer when dev1 cannot build (Tier-0 OOM). The sanctioned path is to build on dev2. Load before any RED/GREEN verification, clippy check, or E2E run.
---

# Building & verifying restreamer on dev2 (dev1 is Tier-0 / OOMs)

dev1 has 7.5 GB RAM and OOMs on any `cargo build/test/check/clippy` (`target/`
hit 23 GB). Locally on dev1 you may run ONLY `cargo fmt --all -- --check`. To
compile, test, clippy, or run the frontend E2E, use **dev2** (16 GB). This is
the sanctioned path — the Tier-0 hook blocks heavy builds; on dev2 the build IS
allowed. If a wrapping bash command's TEXT trips the hook (e.g. a commit message
mentioning `trunk build` / `cargo test`), append `# airuleset:build-ok` to that
one command.

## One-time-per-session setup

The warm checkout on dev2 is **`~/restreamer-buildcheck`** (keeps `target/`
warm — a cold build is ~40 min, a warm incremental is seconds-to-minutes). It's
a plain source copy (no `.git`) with a warm `target/`; run all the dev2 commands
below from there. (The old `~/hotfix294/repo` path is GONE — 2026-07-21; if
`restreamer-buildcheck` is ever missing, `find ~ -maxdepth 3 -name Cargo.toml
-path '*restreamer*'` on dev2 finds the current warm checkout.) Sync your dev1
working tree into it (source only; never ship `target/`):

```bash
# airuleset:deploy-dirty-ok   # build-verify sync, NOT a deploy (clean-tree hook)
rsync -a --delete \
  --exclude 'target/' --exclude '.git/' --exclude 'node_modules/' \
  --exclude 'e2e/test-results/' --exclude '*.png' \
  /home/newlevel/devel/restreamer/ \
  newlevel@dev2:/home/newlevel/restreamer-buildcheck/
```

The `# airuleset:deploy-dirty-ok` marker is REQUIRED — the clean-tree hook
blocks any rsync/scp from a dirty tree, and you sync UNCOMMITTED work here on
purpose (you verify BEFORE committing). Re-run the rsync after EVERY local edit.

## Compile / test / clippy

Always `source ~/.cargo/env` and `export SQLX_OFFLINE=true` on dev2.

- `rs-delivery`'s `producer_lag` / `endpoint_producer` / most producer logic
  lives in the **BIN** target, not the lib — a `cargo test -p rs-delivery --lib`
  runs 0 of those. Use `cargo test --bin rs-delivery`.
- Workspace: `cargo test --workspace` and `cargo clippy --workspace
  --all-targets -- -D warnings` (a real CI gate).
- **`leptos-ui` is EXCLUDED from the workspace** — test it separately:
  `cd leptos-ui && cargo test --lib`, and check it compiles for wasm with
  `cargo check --target wasm32-unknown-unknown` (installed on dev2). This wasm
  check is the ONLY thing that catches leptos type errors — dev1 can't build and
  the workspace jobs skip leptos, so ALWAYS run it after a leptos edit.
- clippy `too_many_arguments` fires at 7 params — prefer dropping a genuinely
  unused param over `#[allow]`.
- **leptos gotchas the wasm check catches (nothing else does):**
  - `leptos::Memo<T>::new` requires `T: PartialEq`. A `Memo` returning a custom
    struct (e.g. a grouped-rows vec) needs `#[derive(Clone, PartialEq)]` on that
    struct, and every field must be PartialEq too.
  - **leptos-ui is NOT fmt-gated in CI** — the CI fmt job is `cargo fmt --all`,
    which excludes leptos-ui/src-tauri (both excluded from the workspace). The
    leptos crate has long-standing rustfmt drift; do NOT `cargo fmt` the whole
    crate (rustfmt would reorder `mod` decls + reflow unrelated drifted files).
    rustfmt only the files YOU authored fresh, or leave it.
  - `trunk build --release` does NOT pass `-D warnings`; leptos dead-code
    warnings (unused pub fns/structs) are pre-existing and don't fail CI.

## Cargo.lock is often STALE (0.29.0 while sources are higher)

Past version bumps did NOT regenerate `Cargo.lock`, so its workspace crate
versions lag. After bumping the 4 version files, regenerate the lock on dev2
(`cargo metadata --offline >/dev/null`) then pull it back to dev1. The pull is a
dev2→dev1 copy, NOT a deploy — the clean-tree deploy hook blocks it; use
`# airuleset:deploy-dirty-ok` on that one `scp`.

**But a stale lock is HARMLESS — don't sweat it.** The workspace crates use
`version.workspace = true` (path deps), and `cargo metadata --offline` / a build
does NOT rewrite their versions in `Cargo.lock` — the lock's `rs-core` etc. can
stay at an old version while the build resolves the current one from Cargo.toml.
The workspace build/test/clippy jobs are NOT `--locked` (only the
`cargo install trunk … --locked` tool step is), so cargo tolerates the drift.
The green 0.29.6 AND 0.29.7 releases both shipped with a stale lock. Regen it if
you want tidiness, but a no-change `scp` is expected and NOT a problem.

## Frontend E2E (CI-equivalent) on dev2

Mirrors ci.yml's frontend-E2E job. **The mock-api server dies when the SSH
session ends** — start it and run the tests in the SAME ssh command (one
heredoc), never as separate ssh calls:

```bash
ssh newlevel@dev2 'cd ~/restreamer-buildcheck/leptos-ui && \
  export BUILD_VERSION=0.29.3-dev BUILD_TIMESTAMP="x" && trunk build --release'
ssh newlevel@dev2 'cd ~/restreamer-buildcheck/e2e && npm install >/dev/null 2>&1
  pkill -f "node mock-api.js" 2>/dev/null; sleep 1   # kill any STALE mock first
  RESTREAMER_TEST_HOOKS=1 node mock-api.js > /tmp/mockapi.log 2>&1 &
  MOCK=$!; sleep 4
  npx playwright test --config playwright-frontend.config.ts
  kill $MOCK'
```

- **ALWAYS `pkill -f "node mock-api.js"` before starting the mock.** A leftover
  mock from an earlier run keeps port 8910; your fresh `node mock-api.js` then
  dies with `EADDRINUSE` (silently, in the background `&`), and the OLD mock —
  which lacks any routes/fixtures you just added — serves the requests. Symptom:
  a brand-new `/api/v1/_test/*` route returns the SPA `index.html` (the
  catch-all `app.get("*")`), so a test doing `res.json()` throws
  `SyntaxError: Unexpected token '<', "<!DOCTYPE"…`. It looks like a code bug but
  is a stale-process env bug. Confirm with `tail /tmp/mockapi.log` (shows the
  `EADDRINUSE`) and `ss -ltn | grep :8910`.
- A new spec file is only picked up if its basename is in the config's
  `testMatch` regex (`e2e/playwright-frontend.config.ts`) — add it there.
- `testMatch` is explicit, and `cargo test` takes only ONE positional filter —
  to run several new tests, filter by a shared substring or run the whole crate.

- E2E runs against **`e2e/mock-api.js`**, NOT the real backend — the
  `/api/v1/_test/ws-broadcast` route is the MOCK's, and it relays the posted JSON
  verbatim, so a new additive endpoint field appears in the frontend with no
  mock change. Playwright browsers are already installed on dev2.
- Each spec `beforeEach` posts `/api/v1/__reset` — that's the mock's, so the
  mock MUST be up before the tests (the `sleep 4`).
