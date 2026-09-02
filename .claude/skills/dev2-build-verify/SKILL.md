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
-path '*restreamer*'` on dev2 finds the current warm checkout.) **If it's
missing entirely** (confirmed 2026-07-25 — no `target/` to keep warm, `mkdir -p
~/restreamer-buildcheck` on dev2 first, then the same rsync below bootstraps it;
the first build is the ~40 min cold one, same as any fresh checkout.) Sync your
dev1 working tree into it (source only; never ship `target/`):

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

**The `--exclude '*.png'` above breaks `trunk build` on a fresh/bootstrapped
checkout.** `leptos-ui/index.html` references `icon-192.png` / `icon-512.png`;
trunk fails with `error getting canonical path for ".../icon-192.png": No such
file or directory` if they were never synced. One-time fix (or after any change
to those specific files): `scp leptos-ui/icon-*.png
newlevel@dev2:~/restreamer-buildcheck/leptos-ui/` with the same
`# airuleset:deploy-dirty-ok` marker. Don't just drop the png exclude — it
exists to avoid re-syncing large/binary test-fixture images on every rsync.

## Compile / test / clippy

Always `source ~/.cargo/env` and `export SQLX_OFFLINE=true` on dev2.

- **Proving RED→GREEN on dev2 with no `.git` in the warm checkout** (it's a
  source copy): to show the RED test FAILS at the RED commit without a second
  checkout, on dev1 `git stash push -- <fix-files>` (reverts the working tree to
  the committed RED state, keeping the RED test), rsync, run the specific tests
  on dev2 (they FAIL — RED proof); then `git stash pop`, rsync again, run the
  full suite (PASS — GREEN proof). Keep the RED test itself out of the stash
  (commit it first) so it exists in both states. One incremental dev2 build each.

- `rs-delivery`'s `producer_lag` / `endpoint_producer` / most producer logic
  lives in the **BIN** target, not the lib — a `cargo test -p rs-delivery --lib`
  runs 0 of those. Use `cargo test --bin rs-delivery`.
- **`ld terminated with signal 7 [Bus error]` or `No space left on device`
  during a `cargo test --workspace` link = dev2 DISK is FULL, not a code bug.**
  The workspace test links MANY large test binaries (rs-service e2e, rs-api lib
  test, …) and can tip `target/` over the edge; `ld` then SIGBUSes on a failed
  mmap. `clippy --all-targets` and `cargo test --bin rs-delivery` can still pass
  (they build less), so trust those for correctness. Fix: `df -h /` (root is one
  915G disk), then free a stale build cache — `du -sh ~/*/target | sort -rh`,
  `rm -rf ~/<stale-verify-checkout>/target` (a `target/` is always regenerable —
  purge on sight). Do NOT touch `~/camera_cache` (camera-box recordings) or
  `~/devel`. Then re-run `cargo test --workspace --jobs 2`.
- Workspace: `cargo test --workspace` and `cargo clippy --workspace
  --all-targets -- -D warnings` (a real CI gate).
- **`leptos-ui` is EXCLUDED from the workspace** — test it separately:
  `cd leptos-ui && cargo test --lib`, and check it compiles for wasm with
  `cargo check --target wasm32-unknown-unknown` (installed on dev2). This wasm
  check is the ONLY thing that catches leptos type errors — dev1 can't build and
  the workspace jobs skip leptos, so ALWAYS run it after a leptos edit.
- clippy `too_many_arguments` fires at 7 params — prefer dropping a genuinely
  unused param over `#[allow]`.
- clippy `items_after_test_module` (`-D warnings` fails the build on it): a
  `#[cfg(test)] mod tests { ... }` block MUST be the LAST item in the file —
  any `pub fn`/`fn`/`struct`/etc. declared AFTER it fails clippy, even though
  `cargo fmt`/`rustc` accept the file fine. Bit twice inserting a test module
  between two functions in the same file (e.g. after function A but before
  function B still to come) — always append new test modules at the very end
  of the file, never mid-file next to the function they cover.
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

## `src-tauri` CAN be type-checked on dev2 (it just needed system libs)

`src-tauri` is excluded from the workspace, so `cargo test/clippy --workspace`
never compiles it — a change there used to be verifiable only by a ~2 h CI
round-trip. It checks fine on dev2 now; two one-time blockers were cleared
(2026-07-26, #336):

```bash
# one-time (already done on dev2):
ssh newlevel@dev2 'sudo -n apt-get install -y libgtk-3-dev libwebkit2gtk-4.1-dev \
  libsoup-3.0-dev libjavascriptcoregtk-4.1-dev'
# after EVERY rsync (the rsync excludes *.png, and generate_context! needs the icons):
# airuleset:deploy-dirty-ok
scp -q src-tauri/icons/*.png newlevel@dev2:~/restreamer-buildcheck/src-tauri/icons/
ssh newlevel@dev2 'source ~/.cargo/env; export SQLX_OFFLINE=true
  cd ~/restreamer-buildcheck/src-tauri && cargo check'
```

- Without the GTK/webkit dev libs: `The system library gdk-3.0 required by crate
  gdk-sys was not found` (a `gdk-sys` build-script failure, nothing to do with
  your code).
- Without the icons: `proc macro panicked … failed to open icon
  .../icons/32x32.png` at `tauri::generate_context!()` in `src-tauri/src/lib.rs`
  — same root cause as the `leptos-ui/icon-*.png` / trunk gotcha above (the
  rsync's `--exclude '*.png'`), different consumer.
- `cargo check --offline` does NOT work here: the `tauri` crate isn't in dev2's
  offline registry cache (`no matching package named tauri found`). Run it
  online.
- It checks the LINUX target, which is fine for platform-independent code
  (commands/state); it does NOT validate Windows-only `cfg` branches.
- Expect 3 pre-existing `dead_code` warnings (`TrayState` helpers). src-tauri is
  not under `-D warnings`.

## EVERY version bump MUST regenerate Cargo.lock — `--locked` is a CI gate now

**A stale lock is a HARD CI FAILURE, not cosmetic** (changed by #322,
commit `a969fa19`, 2026-07-25 — this section used to say the opposite and that
stale advice cost a full CI cycle on #336). Five workspace commands in ci.yml
carry `--locked`:

```
cargo clippy --workspace --all-targets --locked -- -D warnings   # Lint
cargo test --workspace --verbose --locked                        # Test
cargo test -p rs-endpoint --features testing --verbose --locked   # Test
cargo test --workspace --locked                                  # Test integrity
cargo build --release -p rs-delivery --locked                     # Build rs-delivery
```

Bumping the 4 version files changes the 11 local member versions, so the lock no
longer matches and cargo refuses to fix it:

```
error: cannot update the lock file … because --locked was passed to prevent this
```

Symptom shape: **Lint + Test + Test-integrity all fail together within ~1 min**,
`Rust CI Gate` + `E2E Gate` fail, and everything downstream (Build Tauri,
Deploy, all three E2E) is SKIPPED. Test-integrity's message is misleading —
"Expected at least 130 tests, but only 0 passed" — because its `cargo test`
never ran. If you see that trio, check the lock BEFORE reading any test code.

Regenerate it as part of the version-bump step, in the same commit if possible:

```bash
ssh newlevel@dev2 'source ~/.cargo/env; export SQLX_OFFLINE=true
  cd ~/restreamer-buildcheck && cargo metadata --offline --format-version 1 >/dev/null'
# airuleset:deploy-dirty-ok   (dev2→dev1 pull, NOT a deploy — the clean-tree hook blocks it)
scp -q newlevel@dev2:~/restreamer-buildcheck/Cargo.lock ./Cargo.lock
grep -A1 '^name = "rs-core"$' Cargo.lock     # must show the NEW version
```

The diff must be exactly the 11 local member versions (`rs-api`, `rs-cloud`,
`rs-core`, `rs-delivery`, `rs-endpoint`, `rs-ffmpeg`, `rs-inpoint`,
`rs-rtmp-push`, `rs-runtime`, `rs-service`, `rs-youtube`) and nothing else — any
transitive-dependency churn means you resolved online; redo it `--offline`.

**Then re-verify WITH `--locked`**, since a plain `cargo test`/`clippy` on dev2
silently self-heals the lock and hides exactly this failure.

`src-tauri/Cargo.lock` and `leptos-ui/Cargo.lock` are separate and ARE stale
(they carry unrelated old versions); no `--locked` command touches them, so
leave them alone.

## Frontend E2E (CI-equivalent) on dev2

**PARALLEL WORKTREE LANES SHARE ONE PORT 8910 — the frontend E2E CANNOT run on
two lanes at once without full private-port isolation (#73, cost hours).** The
mock-api hardcodes `const PORT = 8910`, AND the app itself hardcodes 8910 for the
Tauri/E2E path in THREE places — `leptos-ui/src/api/mod.rs` `compute_api_base()`
(`http://127.0.0.1:8910/api/v1`), `leptos-ui/src/ws.rs` `ws_url()`
(`ws://127.0.0.1:8910/api/v1/ws` + the `location.host` fallback), and
`e2e/tauri-mock.js` (`const MOCK_API`). The suite injects `tauri-mock.js`, so
`is_tauri()` is TRUE and the app fetches DATA + WS from the hardcoded 8910
regardless of which port SERVES the dist. So when a sibling `/autopilot` lane is
running its own mock on 8910, your run silently loads YOUR dist from your
baseURL but reads DATA from the SIBLING's mock → glow/banners "don't appear",
scenario POSTs seem ignored, `ERR_CONNECTION_REFUSED` floods the console when the
sibling mock dies, and an `ENOENT .../restreamer-bc-<OTHER>/dist/index.html` in a
Playwright error-context is the smoking gun (a sibling's dist path). Diagnose
first: `ss -ltnp | grep :8910` shows the owning pid; if it's not yours, you are
colliding — do NOT pkill it (it belongs to a sibling session).

**Full private-port isolation recipe (lane-copy only, NEVER committed — your
worktree keeps 8910 for CI):** pick a private port (e.g. 18973), then in the dev2
lane checkout `sed -i s/127.0.0.1:8910/127.0.0.1:18973/g` on
`leptos-ui/src/api/mod.rs`, `leptos-ui/src/ws.rs`, `e2e/tauri-mock.js`, and every
`*.spec.ts` you run + your verify config's `baseURL`; `sed -i "s/const PORT =
8910;/const PORT = 18973;/"` on `e2e/mock-api.js`; **rebuild `trunk build
--release`** (bakes the port into the wasm); start the mock as a transient user
unit so its lifecycle is controlled and it survives ssh drops (avoids the
webServer/EADDRINUSE orphan races): `systemd-run --user --unit=mock-<lane>
--working-directory=<lane>/e2e --setenv=RESTREAMER_TEST_HOOKS=1 /usr/bin/node
mock-api.js`; run playwright with a NO-webServer config (`ss -ltn | grep :18973`
to confirm up first); `systemctl --user stop mock-<lane>` at the end. Because the
worktree-isolation guard rejects ssh commands containing `;`/`&`/`setsid`/`bash
<script>`, a `systemd-run --user` unit + a no-webServer playwright config is the
guard-friendly way to get a controlled background mock (a plain visible `cd DIR &&
npx playwright ...` runs; the mock cannot be backgrounded with `&`).

**Faster alternative when no sibling holds 8910:** just run the canonical
single-mock recipe below on 8910. Confirm ownership with `ss -ltnp | grep :8910`
before trusting any E2E result on a shared box.

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
- **A NEW `/api/v1/status` field must ALSO be added to `e2e/tauri-mock.js`'s
  `get_status` handler, not just `mock-api.js`'s response builder (#278).** The
  whole frontend suite injects `window.__TAURI__` via `tauri-mock.js`, so
  `is_tauri()` is true and every page goes through the Tauri IPC
  `invoke("get_status")` path — which `tauri-mock.js` implements by fetching
  `mock-api.js`'s real HTTP response and then hand-composing its OWN `data`
  object field-by-field (it does NOT forward the response verbatim, unlike the
  ws-broadcast relay above). A field present in `mock-api.js`'s JSON but missing
  from `tauri-mock.js`'s hand-built object silently vanishes — a banner/store
  test for that field passes or fails in the WRONG direction with no error,
  because the frontend just sees the field as absent/default. Caught adding
  `s3_region_standard`: the "banner shows" test failed (field missing ->
  defaulted false-ish) while the "banner hidden" test passed by coincidence.
- **`playwright-frontend.config.ts` hardcodes `workers: 1` on purpose — never
  override with `--workers=N>1` for the FULL suite.** `mock-api.js` is ONE
  shared Node process holding global mutable state (`scenario`, `events`,
  `oauthGrants`, …); running >1 worker races that shared state across parallel
  tests. Confirmed 2026-07-25: `--workers=2` on the full suite produced 3
  cross-test-contamination failures (`change-key`, `oauth-authorize`,
  `frontend.spec.ts` remove-endpoint) that vanished on a `--workers=1` rerun —
  looked exactly like real app bugs, wasn't. `--workers=1` is fine (and
  necessary) when scoping to a handful of spec files for a fast iteration loop;
  just never assume a `--workers=N` full-suite run that shows failures means
  your change broke something before re-running at `workers:1`.

### `ssh … npx playwright test` drops with **exit 255** when dev2 is loaded — run E2E DETACHED + poll a sentinel

dev2 is shared: other Claude sessions leave **Playwright MCP** chrome running for
days (`ps -eo pid,etime,cmd | grep playwright-mcp`; user-data-dir
`~/.cache/ms-playwright-mcp/…`, load avg often ~10). Under that load a heavy
interactive `ssh newlevel@dev2 '… npx playwright test …'` frequently drops at
handshake — the tool returns **`exit 255` with zero stdout** (looks like the
command failed; it never ran). A trivial `ssh … 'echo alive'` still works, which
is the tell. **Do NOT pkill those MCP chrome/`playwright-mcp` processes — they
belong to other sessions**, not your `~/restreamer-buildcheck/e2e` run.

Fix: don't hold the run in the ssh session. Launch it **detached** (writes a log
+ a done-sentinel) and poll the sentinel with separate short ssh calls — this
survives connection drops. Use `--workers=1` to keep load down:

```bash
ssh newlevel@dev2 'cd ~/restreamer-buildcheck/e2e
  pkill -f "node mock-api.js" 2>/dev/null; sleep 1; rm -f /tmp/pw.log /tmp/pw.done
  RESTREAMER_TEST_HOOKS=1 nohup node mock-api.js >/tmp/mockapi.log 2>&1 & sleep 5
  ss -ltn | grep -q :8910 && echo MOCK_UP || echo MOCK_DOWN
  setsid bash -c "cd ~/restreamer-buildcheck/e2e; npx playwright test \
    --config playwright-frontend.config.ts <spec>.spec.ts --repeat-each=5 --workers=1 \
    >/tmp/pw.log 2>&1; echo EXIT=\$? >/tmp/pw.done" >/dev/null 2>&1 &
  echo LAUNCHED'
# then poll from dev1 (survives drops): loop `ssh … 'cat /tmp/pw.done'` until non-empty,
# then `ssh … 'tail -20 /tmp/pw.log'`.
```

`--repeat-each=5` is the sanctioned way to prove a FLAKE fix (a keyed `<For>`
re-render fix, a refresh-race). If even the detached-launch ssh keeps dropping,
retry it a few times spaced ~20s — it's intermittent, not a code problem.
