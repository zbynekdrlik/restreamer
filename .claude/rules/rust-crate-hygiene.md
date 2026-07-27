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

Corollary worth knowing: **clippy's `items_after_test_module` (a hard
`-D warnings` error here) guarantees a `#[cfg(test)]` module is the LAST item in
every file.** That makes "truncate at the first `#[cfg(test)]`" an exact way to
isolate production code when a test needs to scan sources.

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
