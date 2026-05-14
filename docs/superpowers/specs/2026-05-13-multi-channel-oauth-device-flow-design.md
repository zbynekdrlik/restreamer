# Multi-Channel YouTube OAuth: Device Flow Architecture

**Date:** 2026-05-13
**Status:** Design approved (user-directed: SOTA 2026 long-term-correct, no small tweaks)
**Refs:** PR #195 (multi-channel diagnostic infra), Issue #196 (ytbb root cause — awaits this PR)

---

## Why This Exists

PR #195 shipped multi-channel YT health probe infrastructure (DB schema, per-endpoint OAuth linkage, dashboard badge, audit emission). It cannot be used to diagnose `ytbb` because no second channel can actually authorize: the web-flow handler at `crates/rs-api/src/youtube.rs:184` redirects to `http://127.0.0.1:8910/api/v1/youtube/oauth/callback`, which is not a redirect URI registered with the Google Cloud project `restreamer-489321`. Google rejects with `redirect_uri_mismatch`. The only known-working authorization method is a one-off nginx-intercept hack on the production newlevel.media server, which does not scale and does not carry a `?label=` through nginx.

This spec replaces the broken in-band web flow with **OAuth 2.0 Device Code Flow (RFC 8628)** — the canonical pattern for headless servers and multi-account authorization. Google has signaled deprecation of loopback redirects for "Web Application" OAuth client type; Device Flow is the explicit replacement and is fully supported on production-mode projects.

## Goals

- **Operator can authorize any number of YouTube channels** from any device, without modifying Google Cloud Console (after one-time client setup) and without any nginx hack.
- **Refresh tokens stored per `label`** (the existing `youtube_oauth` schema from PR #195) and survive restreamer restarts permanently (project is in Google's "Production" mode → refresh tokens never expire).
- **`channel_id` and `connected_at` populated** so operators see "bb (UCxxxx) — connected 2026-05-13 07:32" not just an opaque label.
- **Per-endpoint health probe stays within the Google Cloud project's daily quota** at 5+ channels and survives quota exhaustion without breaking the dashboard.
- **No legacy code left behind:** the broken Web flow, the single-row `upsert_youtube_oauth`, the nginx-intercept procedure documented in `MEMORY.md` are all deleted in this PR.

## Non-Goals (Filed As Follow-Ups, Not Scope Creep)

- The actual `ytbb` `videoIngestionStarved` root-cause fix. Depends on observed `top_issue` from the dashboard once `bb` is authorized. Tracked in Issue #196; will be updated with observed data and either fixed inline or split into a separate PR depending on diff size.
- RTMPS migration (`rtmp://` → `rtmps://` for YT push). Independent concern.
- Channel ownership transfer between labels. Rare manual operation; operator deletes row + re-authorizes.
- Refresh-token rotation. Google production-mode tokens are permanent; rotation is a non-problem.

## Architecture

### 1. OAuth Client Setup (One-time operator action)

Google Cloud Console → project `restreamer-489321` → APIs & Services → Credentials → "Create Credentials" → "OAuth client ID" → Application type: **"TVs and Limited Input devices"** → Name: `Restreamer Device Flow`. This produces a new pair `client_id` + `client_secret` distinct from the existing Web App credentials. The Web App credentials stay in place for any pre-existing `default`-label refresh token (no migration disruption).

Credentials added to:
- Local: `~/.restreamer-secrets/stream-lan.env`
- CI: GitHub secrets `YOUTUBE_DEVICE_CLIENT_ID` + `YOUTUBE_DEVICE_CLIENT_SECRET`
- Config schema: `[youtube.device_flow] client_id, client_secret` (with `#[serde(default)]` for backwards compat on existing configs).

### 2. Schema Delta (Migration v27 — Incremental, Idempotent)

```sql
-- channel_id and connected_at on existing youtube_oauth rows.
ALTER TABLE youtube_oauth ADD COLUMN connected_at TEXT;
-- channel_id already exists (added in v25); no-op via add_column_if_missing.

-- Pending Device Flow grants (transient state — deleted on success or expiry).
CREATE TABLE IF NOT EXISTS oauth_device_grants (
  label            TEXT PRIMARY KEY,
  device_code      TEXT NOT NULL,
  user_code        TEXT NOT NULL,
  verification_url TEXT NOT NULL,
  interval_secs    INTEGER NOT NULL,
  expires_at       TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending',  -- pending|granted|denied|expired|error
  error            TEXT,
  started_at       TEXT NOT NULL
);
```

`status` values: `pending` (polling) → `granted` (row deleted, tokens persisted) | `denied` (operator declined) | `expired` (>15 min without action) | `error` (HTTP failure, see `error` column).

### 3. Device Flow Lifecycle

#### `POST /api/v1/youtube/oauth/device-start`
Body: `{ "label": "bb" }`.
- Validates `label` matches `[a-z0-9_]{1,32}`.
- If `youtube_oauth.label` already has a refresh token → 409 Conflict with `{error: "already_authorized", label}`. Operator must delete first if re-authorizing.
- Calls `POST https://oauth2.googleapis.com/device/code` with `client_id` (device client) + `scope=https://www.googleapis.com/auth/youtube.readonly openid`.
- Persists row to `oauth_device_grants` with status=`pending`.
- Spawns a tokio task (named via `tokio::task::Builder::new().name(...)` for traceability) that polls the token endpoint.
- Returns `{ user_code, verification_url, expires_in }`.

#### `GET /api/v1/youtube/oauth/device-status?label=bb`
Returns:
- `pending` → `{ status: "pending", user_code, verification_url, expires_at }`
- `granted` → `{ status: "granted", channel_id, connected_at }` (read from `youtube_oauth`, row in `oauth_device_grants` already deleted)
- `denied` / `expired` / `error` → `{ status, error?: string }`

Idempotent: safe to poll from the UI every 3 seconds.

#### Background polling task
```text
loop {
    sleep(interval_secs)
    POST oauth2.googleapis.com/token with grant_type=device_code, device_code, client_id, client_secret
    match response {
        200 OK with access_token + refresh_token:
            // First successful liveStreams.list call attaches channel_id.
            list_streams_for_label(pool, label).await -> capture first stream's channel reference
              (fallback: id_token sub claim if no streams yet)
            upsert_oauth_by_label(... access_token, refresh_token, expires_at, channel_id, connected_at=now)
            delete row from oauth_device_grants
            emit AuditRow { action: OAuthGranted, source: Operator, detail: {label, channel_id, scopes} }
            return
        400 with error="authorization_pending": continue loop
        400 with error="slow_down": interval_secs *= 2; continue
        400 with error="access_denied": mark grant denied; emit audit; return
        400 with error="expired_token": mark grant expired; return
        anything else: mark grant error with body; return
    }
    if now() > expires_at: mark grant expired; return
}
```

Crash recovery: on restart, scan `oauth_device_grants WHERE status='pending'`; for each, either resume polling (if `expires_at` not passed) or mark `expired`.

### 4. Quota Tracker (`crates/rs-youtube/src/quota.rs`)

Per-project sliding-window limiter, single global instance via `OnceLock`. Token bucket with capacity = `daily_quota` (default 10,000), refill rate = `daily_quota / 86_400` per second (≈0.116 for default).

```rust
pub struct QuotaTracker {
    daily_quota: u32,
    state: Mutex<BucketState>,
}
impl QuotaTracker {
    pub fn acquire(&self, units: u32) -> Result<(), QuotaExhausted>;
    pub fn remaining(&self) -> u32;
}
```

Called before every `liveStreams.list` invocation (cost = 1 unit per Google's published table). On exhaustion: probe returns `YoutubeHealth { health_status: "unknown", error: Some("quota_throttled"), ... }`. No audit row (silent — operator sees it on the dashboard, no log spam).

Daily reset at 00:00 UTC.

### 5. Adaptive Cache TTL

`attach_yt_health_cached`:
- `top_issue.is_none() && health_status == "good"` → TTL 60s
- Otherwise → TTL 15s

Five healthy channels at 60s = 7,200 probes/day = 72% of budget. One degraded channel at 15s = 5,760/day. Two degraded = 11,520/day (over). The quota tracker handles overrun gracefully.

### 6. Operator Dashboard UI

New Leptos component `leptos-ui/src/components/oauth_authorize.rs`. Mounted on the operator dashboard under existing config section.

**Channels panel:** Table showing `label | channel_id | connected_at | linked_endpoints | actions`. Data from `GET /api/v1/youtube/oauths`. `actions` = "Unlink endpoints" + "Delete grant" buttons.

**Authorize new channel button:** Opens modal:
1. Label input (validated client-side `^[a-z0-9_]{1,32}$`).
2. Submit → `POST device-start` → on 200, modal body switches to:
   - Large monospace `user_code` (selectable for copy)
   - Click-to-open `verification_url` (typically `https://www.google.com/device`)
   - Status indicator (auto-polls `device-status` every 3s)
   - Spinner with "Waiting for authorization..."
3. On `granted` → modal closes, channels table refreshes.
4. On `denied`/`expired`/`error` → error message, "Try again" button (clears grant, restarts flow).

### 7. Audit

New `Action::OAuthGranted` variant:
```rust
Action::OAuthGranted  // emitted once per successful Device Flow completion
```
Detail JSON:
```json
{
  "label": "bb",
  "channel_id": "UCxxxxxxxxxxxxxxxxxxxx",
  "scopes": "https://www.googleapis.com/auth/youtube.readonly openid"
}
```
Source: `Source::Operator`.

### 8. Deletions (Same PR)

- `crates/rs-api/src/youtube.rs::youtube_oauth_start` — Web-flow handler. Replaced by Device Flow.
- `crates/rs-api/src/youtube.rs::youtube_oauth_callback` — Web-flow callback. Replaced.
- `crates/rs-api/src/youtube.rs::parse_label_from_query` — helper for deleted handlers.
- Routes `/youtube/oauth/start` + `/youtube/oauth/callback` removed from `router.rs`.
- `crates/rs-core/src/db/v2.rs::upsert_youtube_oauth` — legacy single-row. Replaced by `upsert_oauth_by_label` everywhere.
- `crates/rs-core/src/db/v2.rs::get_youtube_oauth` — legacy single-row. Replaced.
- `crates/rs-api/src/delivery_youtube.rs::check_youtube_status` — refactored to iterate over `list_oauths` and return `Vec<YouTubeStatusPerChannel>` instead of single bool. The `/api/v1/youtube/status` endpoint response shape changes accordingly (breaking API change documented in PR description).
- `youtube_oauth_seed` — now requires `label` in body (CI workflow updated in same PR to pass `label=default`).

The memory entry about the nginx-intercept hack (`MEMORY.md` → "OAuth re-auth flow") gets marked superseded with a pointer to this spec.

### 9. Tests

| Layer | Test | TDD |
|---|---|---|
| Unit | `quota::acquire` returns `Ok` under budget, `Err(QuotaExhausted)` over | RED → GREEN |
| Unit | `quota` refill semantics: 1 second of refill restores `refill_rate` units | RED → GREEN |
| Unit | Adaptive TTL: `health=good, issue=None` → 60s; `health=bad` → 15s | RED → GREEN |
| Unit | Device Flow state machine: `authorization_pending` → continue; `slow_down` → double interval; `access_denied` → terminal; `expired_token` → terminal | RED → GREEN |
| Integration | Full Device Flow happy path against wiremock'd Google: `device/code` → `token` (pending × 2) → `token` (granted) → `youtube_oauth` row populated + grant deleted + audit row | RED → GREEN |
| Integration | `OAuthGranted` audit row fires exactly once on grant | RED → GREEN |
| Integration | Crash recovery: insert pending grant, restart, polling resumes | RED → GREEN |
| Integration | Quota exhaustion: probe returns `error: "quota_throttled"`, no panic | RED → GREEN |
| Playwright | Authorize modal happy path: type label → submit → see user_code + URL → mock backend transition to `granted` → modal closes → table updated | RED → GREEN |

Every test commit precedes its GREEN counterpart in git history (per `regression-test-first.md` for any defect-class changes; new code follows `tdd-workflow.md`).

### 10. Migration & Rollback

**Forward:** Migration v27 is incremental + idempotent. Existing `default`-label refresh token continues to work (used by the `e2e rtmp` endpoint in CI). Web flow URL `/youtube/oauth/start` returns 410 Gone for one release cycle (just in case anything bookmarked).

**Rollback:** Revert PR. Migration v27 columns/table stay (harmless). Web-flow handlers come back. Operator can still use Device-Flow-granted refresh tokens because the storage schema is forward-compatible.

### 11. Quota Math (Verification)

| Scenario | Channels | TTL | Probes/min | Daily | % of 10k budget |
|---|---|---|---|---|---|
| All good | 5 | 60s | 5 | 7,200 | 72% |
| 1 degraded | 4 good + 1 bad | 60s/15s | 4 + 4 | 5,760 + 5,760 = 11,520 | 115% (quota tracker absorbs overflow) |
| All degraded | 5 | 15s | 20 | 28,800 | 288% (quota tracker drops ~66% of probes; degraded channels see less frequent updates but no crash) |
| Adding 6th channel | 6 good | 60s | 6 | 8,640 | 86% |

Budget headroom for 7 healthy channels with adaptive TTL. Beyond that, operator can increase `daily_quota` (Google honors quota-increase requests for legitimate use).

## Open Questions

None at spec time. All design unknowns resolved by user directive ("SOTA 2026, long-term correct, don't ask me silly questions").

## File-Size Budget

Estimated diff:
- `crates/rs-core/src/db/migrations.rs` (+50 LoC for v27)
- `crates/rs-core/src/db/oauth_device_grants.rs` (new, ~120 LoC)
- `crates/rs-core/src/audit.rs` (+10 LoC OAuthGranted variant)
- `crates/rs-youtube/src/device_flow.rs` (new, ~250 LoC — Google API client + state machine)
- `crates/rs-youtube/src/quota.rs` (new, ~80 LoC)
- `crates/rs-api/src/oauth_device.rs` (new, ~180 LoC — Axum handlers + background task)
- `crates/rs-api/src/youtube.rs` (-150 LoC web-flow deletions, +30 LoC for revised `/youtube/status`)
- `crates/rs-api/src/router.rs` (~5 LoC route changes)
- `crates/rs-api/src/delivery_youtube.rs` (~50 LoC rewrite)
- `crates/rs-api/src/delivery_status.rs` (~10 LoC adaptive TTL)
- `leptos-ui/src/components/oauth_authorize.rs` (new, ~250 LoC)
- `leptos-ui/style.css` (+20 LoC modal styles)
- Tests (~600 LoC across crates)
- `.github/workflows/ci.yml` (`youtube_oauth_seed` call gets `label: default` body field)

Total: ~1,800 LoC. Each new `.rs` file stays under the 1000-line cap. Splitting is by responsibility, not size.

## Out-of-Scope Discoveries (No Follow-Up Issue Yet)

None at spec time. If implementation surfaces new gaps, file as Issue #197+ before completion report.
