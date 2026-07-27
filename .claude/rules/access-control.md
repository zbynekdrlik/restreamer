---
paths:
  - "crates/rs-api/src/access*.rs"
  - "crates/rs-api/src/router.rs"
  - "crates/rs-api/src/diag.rs"
  - "crates/rs-core/src/config_redact.rs"
---

# Access control (`access.rs`) — invariants you must not break

Added in v0.29.22 for #70/#273/#337/#339. The full architecture is on #273 and
`docs/cloudflare-tunnel-setup.md`; this file is what a session editing these
files needs in the first 30 seconds.

## The three that are load-bearing

1. **LAN is never authenticated, and the `Local` branch does ZERO network I/O.**
   That is the Sunday-morning escape hatch: Cloudflare down, JWKS unreachable or
   the building's internet out, the operator opens `http://stream.lan:8910` and
   works. Never move a fetch above the origin classification, and never make the
   Local path await anything that can block.
2. **The middleware attaches at the very END of `build_router`, AFTER
   `fallback_service`.** `Router::layer` only wraps routes registered BEFORE it,
   so attaching it where the CORS layer sits leaves the dashboard SPA completely
   ungated. `access_tests::the_spa_fallback_is_gated_too` is the guard.
3. **RFC1918 + loopback must classify as `Local`.** `scripts/soak-mini.ps1` hits
   `http://10.77.9.204:8910` and the self-hosted runner lives on the box; both
   are unauthenticated and must stay that way. Breaking this breaks CI, not just
   the operator.

## Gotchas that cost real time here

- **`jsonwebtoken`'s `set_audience` / `set_issuer` do NOT make a claim
  required.** They validate it only when PRESENT — a token omitting `aud`
  sails past the audience pin and returns `Ok`. The pin only exists because of
  `set_required_spec_claims(&["exp", "aud", "iss"])`. Do not "simplify" that
  line away. (`nbf` is deliberately NOT required: absent means "valid now".)
- **`Origin == Host` proves nothing on its own** — in a DNS-rebinding attack
  both are attacker-chosen. The Host must ALSO be an address this box answers
  to (`is_trusted_authority`). Extending `TRUSTED_HOST_SUFFIXES` is fine; adding
  a public suffix an attacker can register a label under is not.
- **A WebSocket handshake is a `GET` and is exempt from CORS**, so it needs the
  origin check explicitly or any internet page opened on the LAN can read live
  state.
- **`api.access` is immutable over the API** (`config_redact::sanitize_patch`).
  A door that can be unlocked through the door it guards is not a lock — one
  `PATCH /config` could otherwise repoint `team_domain` at an attacker's Zero
  Trust org and the app would accept their tokens after a restart. Change it by
  editing `config.json` on the box.
- **Missing `ConnectInfo` is treated as loopback.** Only unit tests using
  `oneshot` lack it; a client cannot strip a server-inserted extension. If you
  ever remove `into_make_service_with_connect_info` from a listener, the gate
  opens silently — `api_integration::the_access_gate_is_live_through_the_real_listener`
  is the only thing that would notice.

## Before you loosen CORS or add a route

- **Adding a route needs no work** — the middleware covers everything, and
  `every_declared_route_denies_an_unauthenticated_internet_request` scrapes
  `router.rs` and fails if a new one is reachable. But routes must be registered
  IN `router.rs`; `no_routes_are_registered_outside_router_rs` fails otherwise,
  because the scraper reads that one file.
- **Before tightening or loosening CORS, grep the E2E suite for
  `page.evaluate` + `fetch`, not just for API call sites.**
  `allow_origin(Any)` was silently propping up a cross-origin in-page read in
  `e2e/youtube-studio-check.spec.ts` (from a `studio.youtube.com` page). Node-side
  `page.request.get(...)` has no CORS and no mixed-content problem — prefer it.

## Verifying on the box

`api.access.mode` is the no-rebuild rollback (`log_only` = pre-#273 behaviour,
`lan_only` = refuse all remote). Every refusal logs `access: DENY … reason=…` in
`C:\ProgramData\Restreamer\restreamer.log`, and a successful startup logs
`access: cached N Access signing keys` — that line is the proof the real JWKS
parsed. Simulate a tunneled request from the box itself:

```powershell
curl.exe -s -o NUL -w "%{http_code}" -H "cf-connecting-ip: 203.0.113.7" `
  http://127.0.0.1:8910/api/v1/status      # expect 403
```
