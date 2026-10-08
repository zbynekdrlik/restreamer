---
paths:
  - ".github/workflows/ci.yml"
  - "e2e/**"
  - "crates/rs-delivery/src/endpoint_consumer_helpers.rs"
  - "crates/rs-delivery/src/producer_lag.rs"
---

# VPS `delivery_mode` in CI gates

Each endpoint's `delivery_mode` comes from the VPS: `warmup`, `normal`, `refilling`,
`rescue`, `recovering`.

- **`refilling` is NORMAL delivery** (#296). A buffered (non-fast) endpoint whose
  cushion is below its 120 s target delivers slightly slower until it is back.
  Right after the disk-cache prefill it usually goes `prefill_ready ->
  buffer_refill_started` within ~40 s and can stay there for minutes.
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
