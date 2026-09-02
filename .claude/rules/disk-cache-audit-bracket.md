---
paths:
  - "crates/rs-delivery/src/disk_cache_fetcher.rs"
  - "crates/rs-delivery/src/disk_cache/**"
  - "crates/rs-delivery/src/disk_cache_stall_tests.rs"
  - "crates/rs-delivery/src/disk_cache_bracket_tests.rs"
---

# disk_cache outage-bracket invariant (stall / recovered forensics)

`DiskCacheFetcher` brackets each S3-outage window on the VPS audit timeline with a
`DiskCacheStallTimeout` (open) → `DiskCacheReaderRecovered` (close) pair, driven by the
`was_stalled` atomic. The bracket is FORENSIC — operators read it to bound an outage — so
these invariants must hold on EVERY path (#330–#335):

- **Arm `was_stalled` ONLY when a stall row is actually emitted.** `note_stall` arms inside
  the `stall_rl.allow()` branch, not before it. Arming on a rate-limited (suppressed) stall
  produces a `DiskCacheReaderRecovered` with no matching stall row (#331). The limiter is
  keyed `(action, "{alias}:{shape}")` so a bounded-attempts storm and a stall_timeout wedge
  in the same 60s window each keep their row.

- **Close the bracket (`note_recovered`) ONLY on a GENUINE clean terminal** — a successful
  file read, a refetch that actually resolved `Available`, or a top-level / refetch
  `NotFound`/`Evicted`. **NEVER on a bare registry `Available`**, which can be STALE: the slot
  reads `Available` while the local file was already evicted and `request_chunk` dedup-skips
  it, so NO fresh S3 GET was issued (the #252 resume-after-eviction race). Closing on that
  stale state emits a spurious recovered row and silently closes the bracket while S3 is still
  down (found in review of #333). This is why `note_recovered` is called at the read/refetch
  terminals, not at the top of the `Available` match arm.

- **`note_stall` has two shapes** (`StallShape`): `bounded_attempts` (~3s `MAX_FETCH_ATTEMPTS`
  cap, the `Failed` arm) and `stall_timeout` (the outer deadline). Only `stall_timeout` carries
  `timeout_secs` in its detail (#332). The bounded path reaches `note_stall` after ~3s and never
  consults `stall_timeout` — do not label it as a 60s outage.

- **Testing the bracket:** `rs_core::audit::RateLimiter` keys off `std::time::Instant`, which
  ignores tokio's paused clock — under `#[tokio::test(start_paused)]` its 60s window is real
  wall time (~µs between two rapid calls), so two same-`(alias, shape)` stall rows in one test
  are suppressed regardless of virtual time. A test asserting a SECOND stall row must use a
  different `(alias, shape)` or space the calls by real time. The stall/recovered bracket tests
  use the mock-S3-backend harness in `disk_cache_stall_tests.rs` (`real_fetcher(backend, tmp,
  alias, Option<ring>)`) — pass `Some(ring)` to observe the forensics rows.
