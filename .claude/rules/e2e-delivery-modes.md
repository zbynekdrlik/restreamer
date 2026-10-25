---
paths:
  - ".github/workflows/ci.yml"
  - "e2e/**"
  - "crates/rs-delivery/src/endpoint_consumer_helpers.rs"
  - "crates/rs-delivery/src/producer_lag.rs"
---

# VPS `delivery_mode` in CI gates

Each endpoint's `delivery_mode` comes from the VPS: `warmup`, `normal`, `refilling`,
`rescue`, `recovering`, `ended`.

- **`ended` is a deliberate OBS stop, not an outage** (#408). The endpoint played its
  buffer out to the end the host announced, then stopped sending. Every E2E job's
  teardown OBS stop now drains into `ended`, not into rescue. A gate that counts
  `rescue_activated` after an OBS `-Action Stop`/`Republish` must expect none once
  the buffer drained; outages are simulated by S3 blocks or killing Restreamer.
  See `.claude/rules/deliberate-obs-stop.md`.

- **`refilling` is NORMAL delivery** (#296). A buffered (non-fast) endpoint whose
  cushion is below its 120 s target delivers slightly slower (0.98x) until it is
  back.
  - Before #424 EVERY buffered endpoint went `prefill_ready ->
    buffer_refill_started` (deficit ~17-23 s) within ~40 s and stayed there until
    the stream ended. The deficit was the prefetch queue: the producer's read
    pointer runs ~11 chunks ahead of the consumer. The pusher's wall-clock pacing
    also caught up the 40 ms sleep, so nothing was actually slowed.
  - Since #424 the cushion is measured at the consumer's last pushed chunk, and
    `RtmpPusher::slow_pacing` moves the pacing anchor. Expect no refill after a
    clean prefill. After a drain, expect `buffer_refill_ended` about
    deficit / 0.02 later (20 s -> ~17 min).
- **A gate that means "warmup is over" or "it is live" must accept `normal` AND
  `refilling`.** Waiting for `normal` alone is a race: it passes only when the poll
  lands before the refill starts. That race broke run 37692367442 and was fixed in
  057eb35d. The recovery check already used `-in @("normal", "refilling")`.
- Only `warmup` (not started yet) and `rescue`/`recovering` (an outage) are
  "not delivering the live stream".
- To read what the VPS did, use the host's mirrored audit rows:
  `GET /api/v1/audit?limit=N` on stream.lan, actions `disk_cache_prefill_*`,
  `buffer_refill_started/ended` (detail `deficit_secs`), `rescue_*`. The VPS itself
  is deleted at teardown.
