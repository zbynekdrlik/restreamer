# chunk_delay_secs consumers audit (2026-05-11)

Audit performed as part of #189 before changing the semantic meaning
of per-endpoint `chunk_delay_secs` from "buffer ABOVE consumer" to "lag FROM
live edge".

## Live computation site (the only one)
- `crates/rs-api/src/delivery_status.rs:256` — the per-endpoint value sent to
  the dashboard. Modified in Task 6.

## DTO struct (passive plumbing)
- `crates/rs-api/src/delivery_handlers.rs:222,265`
- `crates/rs-api/src/delivery_status.rs:120`
- `crates/rs-core/src/models.rs:243,258`
- `crates/rs-api/src/lib.rs:419`

## Historical metrics (carries new meaning going forward)
- `crates/rs-core/src/db/metrics.rs:17,42,50,61,73,116`
  Column: `delivery_endpoint_metrics.chunk_delay_secs`
  Rows written after this PR: lag-from-live-edge semantics.
  Rows written before this PR: buffer-above-consumer semantics.
  No migration; downstream consumers must not mix old + new rows for averaging.

## Diagnostic exports (re-exports historical column)
- `crates/rs-api/src/diag.rs:24,69,131,148,239,293`

## Tests & defaults (meaning-agnostic)
- `crates/rs-core/src/models.rs:481,535,606,632,653,679,695,708,709,723,735,736,750`
- `crates/rs-api/src/stream_handlers.rs:152`
- `crates/rs-api/src/lib.rs:280,408,479`
- `crates/rs-core/src/db/migration_tests.rs:106`

## Conclusion
Task 6 changes only the producer at delivery_status.rs:256. All other sites
plumb the value through unchanged. No code surface to break.
