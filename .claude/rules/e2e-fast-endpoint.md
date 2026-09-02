---
paths:
  - ".github/workflows/ci.yml"
  - "crates/rs-api/src/delivery_live_edge.rs"
  - "crates/rs-delivery/src/api.rs"
  - "leptos-ui/src/components/endpoint_tree.rs"
  - "e2e/frontend.spec.ts"
---

# E2E fast-endpoint (`is_fast`) + audit gotchas (#192)

## The per-endpoint cache label has THREE shapes — parse fast-first

`.endpoint-cache-label` (rendered in `leptos-ui/src/components/endpoint_tree.rs`)
is NOT always `"Xs / Ns cache"`:

| Endpoint | `fast_delay_target_secs` | Label |
|---|---|---|
| non-fast | (n/a) | `"90s / 120s cache"` |
| fast | None / 0 | `"2s / live cache"` |
| fast | set (floor 5) | `"30s / 30s target cache"` |

The fast producer (`rs-delivery/src/endpoint_producer.rs`) sets
`fast_delay_target_secs` after a few loop iterations, so a fast endpoint reads
`"Ns / 5s target cache"` shortly after delivery starts. A naive
`/(\d+)s\s*\/\s*(\d+)s/` parse mis-reads that as non-fast → a CI cache-bar step
reds deterministically once the producer warms up. Classify **FAST first**,
anchor the non-fast regex on `cache$`:

```js
const FAST_RE = /(\d+)s\s*\/\s*(?:live|(\d+)s\s+target)\s+cache\s*$/i;
const NONFAST_RE = /(\d+)s\s*\/\s*(\d+)s\s+cache\s*$/;   // cache$ excludes "target"
const fast    = labels.filter(l => FAST_RE.test(l));
const nonFast = labels.filter(l => !FAST_RE.test(l) && NONFAST_RE.test(l));
```

And NEVER parse `.first()` / `[0]` of `.endpoint-cache-label` — endpoint DOM
order is `endpoint_details` HashMap iteration (`rs-delivery/src/api.rs`), so a
fast label can sort first. Collect ALL labels and pick a non-fast one.

## The two fast-endpoint audit actions, and when they fire

- `fast_endpoint_jumped_to_live_edge` — HOST, `crates/rs-api/src/delivery_live_edge.rs`.
- `endpoint_start_chunk_updated` — VPS, `crates/rs-delivery/src/api.rs`, mirrored to
  the host `audit_log` with `event_id` backfilled (`delivery_audit_mirror.rs`), so
  BOTH are queryable via the host `GET /api/v1/audit?event_id=&action=&since=`
  (`#[serde(rename_all="snake_case")]`; row `endpoint` field = the alias).
- BOTH are gated on `should_jump_to_live_edge(is_fast, gap) = is_fast && gap>0`.
  `gap = MAX(sent)+1 - original_start_chunk_id`, measured over the ~1-5s between
  the pre-VPS start computation and the delivering transition. **A ZERO-GAP is
  legitimate and emits NEITHER row** — a CI assertion must tolerate `jump==0`,
  not hard-assert `== 1`.

## The E2E-Test event PERSISTS across CI runs

Its `audit_log` accumulates, so any per-run audit assertion MUST scope its query
by a `since=<ts captured before start-stream>` (pass it between steps via
`$GITHUB_ENV`) or it will count a previous run's rows.

## Resilience gates that assert on ALL endpoints must skip `is_fast`

A fast endpoint's ~5s cushion is smaller than an outage window (a 15s S3 block
exceeds `RESCUE_STALL_THRESHOLD_SECS=8`), so it legitimately drains into rescue,
stops advancing S3 chunks, and may restart its push ffmpeg — behaviour the
OBS-disconnect / A/V-republish / network-block gates were not written for. The
progression / steady-state / delay gates already `if ($ep.is_fast) { continue }`
(or `Where-Object { -not $_.is_fast }`); any NEW all-endpoints assertion added
when a fast endpoint is attached must do the same. `is_fast` lives on the host
`endpoint_details` view (not always on the VPS `.endpoints` shape — skip by alias
if unsure).
