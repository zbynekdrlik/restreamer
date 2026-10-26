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

- **`refilling` is no longer set** (#424, owner 2026-10-26). The #296 refill
  delivered at 0.98x of real time while the cushion was below target. It never
  acted until #424 made it real, and YouTube then flagged every refill window as
  `videoIngestionStarved` (health bad, PR #425 run 40302038359). The outgoing
  stream now always runs at real time. The deficit is still measured at the
  consumer's last pushed chunk and audited as `buffer_refill_started/ended`
  (detail `deficit_secs`), as information only: nothing paces on it, and a
  drained cushion stays lower for the rest of the event.
- **A `videoIngestionStarved` on YouTube right after an outage test means
  something changed the push speed.** Check every pacing/sleep change in the
  push path first.
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
