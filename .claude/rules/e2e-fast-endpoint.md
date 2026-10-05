---
paths:
  - ".github/workflows/ci.yml"
  - "crates/rs-api/src/delivery_live_edge.rs"
  - "crates/rs-delivery/src/api.rs"
  - "crates/rs-delivery/src/test_file_sink*.rs"
  - "crates/rs-delivery/src/endpoint_rtmp_url.rs"
  - "crates/rs-delivery/src/endpoint_start.rs"
  - "crates/rs-delivery/src/endpoint_task.rs"
  - "crates/rs-delivery/src/rescue.rs"
  - "crates/rs-delivery/tests/test_file_sink_e2e.rs"
  - "leptos-ui/src/components/endpoint_tree.rs"
  - "e2e/frontend.spec.ts"
---

# E2E fast-endpoint (`is_fast`) + audit gotchas (#192)

## TEST_FILE = in-process loopback RTMP sink on the VPS; CI seeds every gate alias itself

- **TEST_FILE is a real, credential-free target.** Under the Rust pusher a
  TEST_FILE endpoint (and its rescue loop) dials `rtmp://127.0.0.1:1935/live/<key>`
  (`endpoint_rtmp_url.rs`, built from `test_file_sink::TEST_FILE_SINK_ADDR`).
  `rs-delivery` runs an in-process accept-and-discard sink there
  (`test_file_sink.rs`) while ANY endpoint in the set is TEST_FILE:
  `api::reconcile_test_file_sink(state, incoming)` runs BEFORE spawning
  (init / add / update_start pass the incoming service types, so the first
  push never races the bind) and after every removal (`&[]`). Its address +
  counters are on the VPS `/api/status` (`test_file_sink`); the CI step
  "Assert fast-endpoint audit (#192)" reads them and requires the byte
  counter to RISE (host `alive` alone is the blind spot that hid #192).
  Before #192 nothing listened, so TEST_FILE silently never delivered.
- **Never fall back to `ServiceType::TestFile`.** Now that TEST_FILE is a real
  discard sink, `parse().unwrap_or(TestFile)` would "deliver" an unknown
  service type into a black hole while looking alive. `endpoint_loop` parses
  the type ONCE (`endpoint_start::service_type_or_refuse`) and passes the
  `ServiceType` down to warmup and the consumer; an unknown type refuses to
  start: error log, `last_error` + `stall_reason = "unknown_service_type"` +
  `delivery_mode = "refused"` in the VPS status, and an
  `EndpointFfmpegRestartFailed` row with `phase: "service_type"`.
- **Every alias a strict CI gate looks up must be seeded by CI itself.** The
  OBS-to-YouTube job creates `e2e fast` (TEST_FILE, key `ci-fast`, is_fast)
  in its pin step and attaches it; `e2e rtmp` is key-synced from
  `YOUTUBE_STREAM_KEY`. Never point a strict gate at an endpoint an operator
  (or a migration — v21 deleted the old YT_HLS fixture) can remove. The
  `test-integrity` self-check "every strict-gate endpoint alias is seeded by
  CI" fails the build otherwise; extend its rules, don't bypass them.
- **The sink is xiu `ServerSession` + our own hub-event responder, NOT xiu
  `StreamsHub`.** streamhub 0.2.4 rejects a 2nd publisher on the same key
  with `Exists` (rescue pushers reuse the endpoint key) and its transceiver
  `receive_event_loop` spins at 100% CPU when the hub is dropped mid-publish.
  Pinned by `two_publishers_on_the_same_key_are_both_accepted`.
- **xiu `ServerSession` drops a publisher idle >= 2 s** (hard-coded
  `read_timeout(2s)`). The fast keepalive bridge starts at 2 s
  (`FAST_KEEPALIVE_TRIGGER_SECS`), so a >= 2 s producer gap on a TEST_FILE
  fast endpoint becomes a reconnect here (YouTube would hold the session).
  Not a sink bug — read push deaths on `e2e fast` in that light.
- **Tests that bind the REAL 1935 live in `tests/test_file_sink_e2e.rs`
  (own process).** The rs-delivery BIN unit tests assume 127.0.0.1:1935 is
  REFUSED (e.g. `rescue_endpoint_loop_tests`); a live listener there in the
  same process breaks them. In-process tests use `AppState::new_for_test()` /
  `TestFileSinkSlot::new("127.0.0.1:0")` (ephemeral). `api.rs` tests use
  `AppState::new()` = the production 1935 slot: never seed a TEST_FILE
  endpoint + reconcile there.
- **Lock order: slot -> endpoints.** `reconcile` takes the slot mutex then
  reads the endpoint map; never touch the slot while holding the endpoint
  lock (`endpoint_status` drops it first) — tokio's fair RwLock deadlocks
  behind a queued writer otherwise.

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
