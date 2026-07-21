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
  `cargo check --target wasm32-unknown-unknown` (installed on dev2).
- clippy `too_many_arguments` fires at 7 params — prefer dropping a genuinely
  unused param over `#[allow]`.

## Cargo.lock is often STALE (0.29.0 while sources are higher)

Past version bumps did NOT regenerate `Cargo.lock`, so its workspace crate
versions lag. After bumping the 4 version files, regenerate the lock on dev2
(`cargo metadata --offline >/dev/null`) then pull it back to dev1. The pull is a
dev2→dev1 copy, NOT a deploy — the clean-tree deploy hook blocks it; use
`# airuleset:deploy-dirty-ok` on that one `scp`.

## Frontend E2E (CI-equivalent) on dev2

Mirrors ci.yml's frontend-E2E job. **The mock-api server dies when the SSH
session ends** — start it and run the tests in the SAME ssh command (one
heredoc), never as separate ssh calls:

```bash
ssh newlevel@dev2 'cd ~/restreamer-buildcheck/leptos-ui && \
  export BUILD_VERSION=0.29.3-dev BUILD_TIMESTAMP="x" && trunk build --release'
ssh newlevel@dev2 'cd ~/restreamer-buildcheck/e2e && npm install >/dev/null 2>&1
  RESTREAMER_TEST_HOOKS=1 node mock-api.js > /tmp/mockapi.log 2>&1 &
  MOCK=$!; sleep 4
  npx playwright test --config playwright-frontend.config.ts
  kill $MOCK'
```

- E2E runs against **`e2e/mock-api.js`**, NOT the real backend — the
  `/api/v1/_test/ws-broadcast` route is the MOCK's, and it relays the posted JSON
  verbatim, so a new additive endpoint field appears in the frontend with no
  mock change. Playwright browsers are already installed on dev2.
- Each spec `beforeEach` posts `/api/v1/__reset` — that's the mock's, so the
  mock MUST be up before the tests (the `sleep 4`).
