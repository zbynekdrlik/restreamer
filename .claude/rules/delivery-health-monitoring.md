---
paths:
  - "crates/rs-api/src/delivery_status.rs"
  - "crates/rs-api/src/delivery_yt_health.rs"
  - "crates/rs-api/src/delivery_fb_health.rs"
  - "crates/rs-core/src/models.rs"
  - "crates/rs-facebook/**"
  - "leptos-ui/src/ws.rs"
  - "leptos-ui/src/store.rs"
  - "leptos-ui/src/api/mod.rs"
---

# Per-endpoint health monitoring (YT + FB ingest health)

## Architecture: attach-at-status-build + TTL cache + audit-on-change (NOT a poll loop)

Per-endpoint health (YouTube `youtube_health`, Facebook `facebook_health`) is NOT a spawned
`tokio` poll loop. It is computed **on demand inside `delivery_status.rs::build_delivery_endpoint_metrics`**,
per endpoint, via `attach_<x>_health_cached(...)`:

- an adaptive-TTL `DashMap` cache (60 s when healthy, 15 s otherwise) keyed by endpoint id — so the
  real upstream API call happens at TTL cadence, not once per WS tick;
- an audit row emitted ONLY on a value transition (`YoutubeIssueChanged` keyed on `top_issue`,
  `FacebookStatusChanged` keyed on the mapped `health`), with the healthy-cold-start row suppressed;
- errors mapped to a `…Health.error` string, NEVER propagated (a probe failure must not break the
  status loop).

To add a new provider, mirror `delivery_yt_health.rs` / `delivery_fb_health.rs` into a **self-contained
module** and add only a 2-line call site to the loop — keep `delivery_status.rs` under the 1000-line cap.

## Adding a field to `DeliveryEndpointMetrics` touches ~9 struct-literal sites — grep them ALL first

`DeliveryEndpointMetrics` (rs-core `models.rs`) does NOT derive `Default`, so a new field is a
**compile error at every literal** until added. They are scattered and easy to miss (the miss shows
up as a late clippy failure, incl. in test files):

```
grep -rn 'DeliveryEndpointMetrics {\|youtube_health: None' crates/ --include='*.rs'
```

Sites include `delivery_status.rs`, `lib.rs` (×2), `stream_handlers.rs`, `status_summary.rs`,
`yt_health_cache_tests.rs`, `delivery_status_yt_health_tests.rs`, **`rs-core/src/models_tests.rs`
(×6)**, and **`rs-api/tests/api_integration.rs`** — the last two are the ones most often missed.

The field is ALSO mirrored in the frontend in THREE places, all of which must gain it or `ws.rs`
won't compile: `store.rs` (`DeliveryEndpointState` + a mirror struct), `ws.rs`
(`WsDeliveryEndpoint` + BOTH mapping sites — the cached-load map and the WS-update map), and
`api/mod.rs` (`CachedDeliveryEndpoint`, read by the first `ws.rs` map). Run the leptos
`cargo check --target wasm32-unknown-unknown` — it is the only thing that catches these.

## Facebook Graph ingest-health specifics (rs-facebook)

- **Token goes in the `Authorization: Bearer` header, NEVER a `?access_token=` query param.**
  reqwest's `Error` `Display` appends the request URL, so a query-param token leaks into any logged
  transport error. Also `.without_url()` every reqwest error before storing it. (See the
  `transport_error_never_contains_the_token` test.)
- **Graph errors are HTTP 400 with the real meaning in `error.code`** — key error handling on the
  code, not the HTTP status: 190 = invalid/expired token, 10 / 200-299 = permission,
  4/17/32/613 = rate limit.
- **`stream_health`** (`video_bitrate`, `video_framerate`, `video_width/height`, `audio_bitrate`)
  lives under `ingest_streams{...}`, which FB wraps as `{ "data": [ … ] }`. A non-zero
  `video_bitrate` on a `LIVE`/`SCHEDULED_LIVE` object is proof FB is decoding our push.
- **The discovery approach (poll the page's currently-receiving `live_video`) does NOT correlate
  with the endpoint's persistent stream key** — a persistent key `FB-<id>-0-<rand>` is not the
  per-session `live_video` id. A second concurrent session on the same page reads a false green.
  This is the documented trade-off; the create+bind-broadcast follow-up resolves it.
- The probe is config-gated (`facebook.enabled`, ships dark). `FB_GRAPH_API_BASE` overrides the base
  URL for wiremock tests (mirrors `YOUTUBE_API_BASE`). Graph credentials are GitHub secrets — see
  the `facebook-streaming` skill; never print/commit a token value.
