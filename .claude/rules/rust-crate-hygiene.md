---
paths:
  - "crates/**/*.rs"
  - "Cargo.toml"
  - "crates/**/Cargo.toml"
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
`cargo update --workspace --offline` (diff must be exactly the 11 local member
versions — any transitive churn means you resolved online).

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
