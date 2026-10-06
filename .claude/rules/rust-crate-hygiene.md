---
paths:
  - "crates/**/*.rs"
  - "Cargo.toml"
  - "Cargo.lock"
  - "crates/**/Cargo.toml"
  - ".github/workflows/ci.yml"
---

# Rust crate hygiene — the CI gates that bite late

## The 1000-line-per-file cap is a CI job, and it fails AFTER the expensive jobs

`File size check` fails the whole run if any `.rs` exceeds 1000 lines. It costs a
full ~2 h cycle to discover, so check before pushing:

```bash
wc -l crates/*/src/*.rs | sort -rn | head -5
```

**Splitting a file's test module is the cheapest fix, and `#[path]` keeps it a
CHILD module** — so the tests still reach the parent's private items through
`super::*` and nothing has to be made `pub` for the sake of the split:

```rust
// at the very END of the file (clippy's items_after_test_module requires it)
#[cfg(test)]
#[path = "access_unit_tests.rs"]
mod tests;
```

The moved file starts with `use super::*;` and its contents de-indented one
level. `access.rs` went 1187 → 707 and `router.rs` 1025 → 295 this way, with no
production change and no visibility change.

**Splitting a test file that is ITSELF `#[path]`-included** (e.g. the ones under
`endpoint_task_test_root.rs`): add a nested `#[path = "child.rs"] mod child;` at
the end of the parent test file and move some `#[tokio::test]` fns into `child.rs`.
The child reaches the parent's private mock backends + harness helpers via `super::`.
**Gotcha:** the child does NOT automatically inherit the parent's `use` imports, and
in particular a TRAIT whose methods the tests call must be re-imported explicitly —
`use crate::endpoint_task::ChunkFetcher;` — or `fetcher.fetch_chunk_with_meta(..)`
fails with `method not found in DiskCacheFetcher … trait ChunkFetcher … not in scope`.
`disk_cache_stall_tests.rs` was split this way (→ `disk_cache_bracket_tests.rs`, 1049→647).

Corollary worth knowing: **clippy's `items_after_test_module` (a hard
`-D warnings` error here) guarantees a `#[cfg(test)]` module is the LAST item in
every file.** That makes "truncate at the first `#[cfg(test)]`" an exact way to
isolate production code when a test needs to scan sources.

**Splitting a PRODUCTION handler file (not just its test module)** — when the
inline production code itself is the bulk (e.g. `rs-api/src/handlers.rs`), move
whole handler GROUPS into new sibling files declared as CHILD modules and
glob-re-export them, so `handlers::<name>` router registration and every call
site stay unchanged with zero visibility churn:

```rust
#[path = "handlers_events.rs"]
mod events;
pub use events::*;          // handlers::create_event still resolves
```

Two gotchas the move creates, both `-D warnings` failures if missed: (1) a moved
handler's imports leave the PARENT file with **orphaned `use`s** (moving the only
users of `EndpointConfig` / `S3Client` out of `handlers.rs` made those two
imports unused) — prune them; (2) each new sibling needs its OWN `use` header,
and a private `const` used only by the moved group (e.g. `VALID_SERVICE_TYPES`)
moves WITH it. A `#[path]`-included test sibling (`use super::*`) is unaffected
as long as the items IT touches stay in the parent. `handlers.rs` went 955 →
525 this way (#341), splitting event-lifecycle + endpoint handlers out.

## Every version bump MUST regenerate `Cargo.lock`

Five workspace commands carry `--locked` (#322). A stale lock fails Lint + Test +
Test-integrity together within ~1 min. Regenerate in the SAME commit as the bump:
`cargo update --workspace --offline` (diff must be exactly the 12 local member
versions — any transitive churn means you resolved online).

## CI floats to the newest stable: verify with CI's toolchain, not dev2's default

Every job except Coverage uses `dtolnay/rust-toolchain@stable`, so a new Rust
release can turn an untouched tree red overnight (#367, 2026-10-05: 1.99.0 added
`clippy::double_must_use`). dev2's default toolchain lags, so a green dev2 run
proves nothing about such drift. Read the version from the failing job log
(`rustc 1.99.0 (b940084d7 …)`, or the `rust-1.99.0` clippy link) and use exactly
that version on dev2, without changing dev2's default:

```bash
rustup toolchain install 1.99.0 --profile minimal -c clippy,rustfmt
cargo +1.99.0 clippy --workspace --all-targets --locked     # no -D warnings: lists EVERY crate
cargo +1.99.0 clippy --workspace --all-targets --locked -- -D warnings
```

Run the first command without `-D warnings`. With it, clippy stops at the first
crate that fails and never checks the crates that depend on it. CI showed one
`double_must_use` site; the full run found three. They came from the
`#[must_use]` that async-trait 0.1.89 adds to every async trait method.
`cargo update -p async-trait` (0.1.92) was the fix, with no `#[allow]` needed.

`cargo update -p <crate>` can also re-point UNRELATED dependency edges. In #367,
`-p rustls` moved the `windows-sys` edge of four crates from 0.52 to 0.60. Read
the whole lock diff. If you revert stray hunks by hand, prove that cargo still
accepts the lock unchanged with `cargo metadata --locked --format-version 1`.

**Coverage is the exception: it pins `dtolnay/rust-toolchain@1.98.0` and
`cargo-tarpaulin --version 0.37.5 --locked`.** tarpaulin's ptrace engine crashes
on rustc 1.99.0 code. The SAME binary passes when run natively, so this is
not our bug. Details are in the coverage job comment, and a test-integrity
guard keeps both pins. To un-pin, run the full Coverage command on dev2 with
the new stable (`RUSTUP_TOOLCHAIN=<ver> cargo-tarpaulin tarpaulin --workspace
--exclude-files "src-tauri/*" --fail-under 55 --skip-clean --timeout 300`, in
its own `CARGO_TARGET_DIR`). Then change the pin and the guard together.

## Slow crypto in tests: optimize the dependency, don't weaken the test

RSA keygen for the Access JWT tests takes minutes with an unoptimized bignum
backend in a debug build. The fix is NOT a smaller key (ring refuses to sign
below 2048) and NOT a committed fixture key (a private key in the repo is a leak
even as a fixture — #274). It is a targeted profile override in the workspace
`Cargo.toml`:

```toml
[profile.dev.package.num-bigint-dig]
opt-level = 3
[profile.test.package.num-bigint-dig]
opt-level = 3
```

Generate the key once per test binary behind a `LazyLock`. Release builds are
untouched.

## The secret scanner blocks 40+ char hex blobs on `git add`/`git commit`

`block-sensitive-staging.sh` fires on the staged DIFF, so it catches a
credential-shaped literal inside an otherwise-allowed file. Cloudflare Access
AUD tags are 64-char hex and trip it even though they are **public**
identifiers. Do not delete the value — commit with a reason, which is logged:

```bash
git commit -F msg.txt  # airuleset:secret-ok <why this literal is not a secret>
```

The marker must sit OUTSIDE any quoted string in the command, so use `-F file`
for the message rather than an inline `-m "…"`.

## cargo-mutants: an "idle = pending()" helper becomes a TIMEOUT, and a hang fails the gate

A `select!` loop branch built from a helper like
`async fn next(rx: &mut Option<Rx>) -> Option<T> { match rx { Some(r) => r.recv().await, None => pending().await } }`
is a trap: cargo-mutants mutates the helper to `-> None`, the branch is then
ALWAYS ready, the task never yields, a `current_thread` test runtime starves,
and the mutant ends as a 300 s TIMEOUT (a non-zero exit that fails the PR
gate). Gate the branch with a `select!` precondition instead
(`frame = async { .. }, if rx.is_some() =>`). Note that the branch's future is
still BUILT when the branch is disabled, so it must not `unwrap()`. Seen on
#192 (`test_file_sink.rs`).

A "stop" function whose only observable effect is releasing a port or socket
"sooner" survives as `-> ()` when the `Drop` impl also aborts the task. Pin it
with a SYNCHRONOUS rebind right after the stop returns, with no await in
between: `drop(std::net::TcpListener::bind(addr).expect(..))`.

## Never run ci.yml's test-integrity steps wholesale on dev1

Several test-integrity steps run the full workspace test suite (e.g. "Run tests and
capture output"). Extracting and running ALL of them locally on dev1 starts a full
workspace build: a Tier-0 violation, an OOM risk, and ~750 MB of `target/` within minutes
(#367, 2026-10-06). Run only the ONE step you changed (select it by its exact name), or
run the whole set on dev2.

## GitHub evaluates `${{ }}` everywhere in a `run:` block, even inside string literals

A guard script that merely COMPARES against the text of an expression such as
`fromJSON(needs.x.outputs.y)` wrapped in `${{ }}` still gets evaluated by Actions in the
job that runs it. If that job has no such output, the WHOLE workflow template fails to
load ("Error reading JToken from JsonReader", #367 run 37402222975). Split the literal
so Actions never sees the opening `${{`, e.g. in Python `"$" "{{ ... }}"` (adjacent
literals join).
