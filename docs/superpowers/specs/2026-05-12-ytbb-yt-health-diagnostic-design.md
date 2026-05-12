# ytbb endpoint — multi-channel YouTube health diagnostic

**Date:** 2026-05-12
**Branch:** dev (current 0.9.0 on main after #190; bump to 0.10.0)
**Status:** Spec — awaiting plan.

## Problem

Production endpoint `ytbb` (Banská Bystrica congregation YouTube channel, broadcast videoId `2bGUd8PPp4c`) consistently shows the YT Studio error:

> "YouTube neprijíma video dosť rýchlo na to, aby bol stream plynulý. Divákom sa teda bude prenos ukladať do vyrovnávacej pamäte."

This is the user-facing form of YT `configurationIssue.type == videoIngestionStarved` (and adjacent: `bitrateLow`, `gopSizeLong`).

Observations:

- Same stream key, same machine, **OBS → key works immediately**.
- All other YT_RTMP endpoints on the working channel deliver healthy via the same restreamer push path.
- Tried: new key, new key with bitrate 1440 — same failure.
- Latency setting on bb broadcast: Normal (rules out ultra-low pacing).
- Key type: reusable persistent.
- bb is on a **different YT channel** from the working YT endpoints — existing OAuth (`mine=true`) cannot see it.

Root cause is **not knowable** from the host side without YT-API visibility into the bb stream object. We have no way to read `cdn.resolution`, `cdn.frameRate`, or `healthStatus.configurationIssues[]` for the bb bound stream during a live push.

## Scope

This spec delivers **diagnostic infrastructure**:

- Multi-account YouTube OAuth support (currently single-row `YouTubeOAuth`).
- Per-endpoint OAuth linkage.
- Background health probe that surfaces `liveStreams.list` results per endpoint.
- Dashboard + audit-log surface for observed YT health.
- TDD coverage at all layers.

The actual fix for ytbb's `videoIngestionStarved` is **out of scope** — once this spec lands, the operator starts ytbb in production, the dashboard exposes YT's precise `configurationIssues[]`, and we file a follow-up issue with that data. Without it, any fix is speculation.

## Architecture

`rs-youtube` already calls `liveStreams.list(part=id,snippet,status,cdn, mine=true)` against a fixed bearer token (single-row `YouTubeOAuth { id=1 }`). Adapt for multiple OAuth grants keyed by label (`default`, `bb`, …) and link each YT endpoint to one.

Probe runs in the existing `rs-api` monitor loop (every 15 s) while an event is delivering. Bounded per-endpoint cache (15 s TTL) to keep YT Data API quota usage well below the 10k-units/day default cap.

No push behavior changes. No host RTMP code touched. Pure observability.

## Components

| Component | File(s) | Responsibility |
|---|---|---|
| `youtube_oauths` schema migration | `crates/rs-core/src/db/migrations.rs` | Incremental ALTER: add `label TEXT NOT NULL UNIQUE DEFAULT 'default'`, `channel_id TEXT`. Backfill existing row label='default'. |
| `youtube_oauth_id` on endpoints | `crates/rs-core/src/db/migrations.rs`, `models.rs` | `ALTER TABLE endpoint_configs ADD COLUMN youtube_oauth_id INTEGER NULL REFERENCES youtube_oauths(id)`. Add `pub youtube_oauth_id: Option<i64>` to `EndpointConfig`. |
| Multi-account OAuth DB ops | `crates/rs-core/src/db/youtube_oauth.rs` (new, ~200 lines) | `get_oauth_by_label`, `get_oauth_by_id`, `list_oauths`, `upsert_oauth_by_label`. |
| Label-aware probe entrypoint | `crates/rs-youtube/src/streams.rs` | New helper `list_streams_for_label(pool, label) -> Result<Vec<LiveStream>>` does refresh-if-expired then `list_live_streams`. |
| Health attach | `crates/rs-api/src/delivery_status.rs` | For each enabled YT_RTMP endpoint with `youtube_oauth_id`, look up the stream by `stream_key` substring (or `bound_stream_id` once we capture it); attach `YoutubeHealth` to `DeliveryEndpointMetrics`. |
| `YoutubeHealth` model | `crates/rs-core/src/models.rs` | `pub struct YoutubeHealth { stream_status: String, health_status: String, top_issue: Option<String>, resolution: Option<String>, frame_rate: Option<String>, age_secs: i64, error: Option<String> }`. Added as `Option<YoutubeHealth>` field on `DeliveryEndpointMetrics`. |
| Labelled OAuth flow | `crates/rs-api/src/youtube_oauth.rs` (existing) | Accept `?label=…` query param on `/youtube/oauth/start` and `/callback`. Default `default` for backward compat. |
| Audit action | `crates/rs-core/src/audit.rs` | New variant `Action::YoutubeIssueChanged { endpoint_alias, from, to }`. |
| Dashboard surface | `leptos-ui/src/components/operator_dashboard.rs` | Per-endpoint YT health badge if `youtube_health.is_some()`. Green = `good`, yellow = `bad`, grey = stale (>30 s) or `error.is_some()`. Tooltip shows `top_issue` raw + `resolution / frame_rate`. |
| Tests | various `_tests.rs`, `e2e/frontend.spec.ts` | See Testing section. |

File size: all new and modified `.rs` files stay <1000 lines (CI gate).

## Data flow

### One-time setup (per channel to monitor)

1. Operator opens `https://restreamer.newlevel.media/youtube/auth/start?label=bb`.
2. nginx callback intercept forwards to local API; tokens saved as `youtube_oauths` row with `label='bb'`.
3. Operator opens dashboard endpoint editor → ytbb row → "Link YT channel" dropdown → select `bb` → save (sets `youtube_oauth_id`).

### Runtime probe loop (every 15 s during delivery)

```
for endpoint in active_yt_endpoints_with_oauth():
    let oauth = get_oauth_by_id(endpoint.youtube_oauth_id);
    let token = refresh_if_expired(oauth);
    let streams = list_live_streams(token).await?;
    // YT exposes stream_key via cdn.ingestionInfo.streamName. Extend
    // StreamCdn with `ingestion_info: Option<IngestionInfo>` and match
    // `iinfo.stream_name == endpoint.stream_key`. If shape changes,
    // fall back to substring containment.
    let bound = streams.iter().find(|s| stream_key_matches(s, endpoint));
    match bound {
        Some(s) => record YoutubeHealth {
            stream_status: s.status.stream_status.clone(),
            health_status: s.status.health_status.as_ref()
                .map(|h| h.status.clone())
                .unwrap_or_else(|| "unknown".into()),
            top_issue: s.status.health_status.as_ref()
                .and_then(|h| h.configuration_issues.first())
                .map(|c| c.issue_type.clone()),
            resolution: s.cdn.as_ref().and_then(|c| c.resolution.clone()),
            frame_rate: s.cdn.as_ref().and_then(|c| c.frame_rate.clone()),
            age_secs: 0, error: None,
        },
        None => record YoutubeHealth {
            stream_status: "unbound".into(), health_status: "n/a".into(),
            top_issue: None, resolution: None, frame_rate: None,
            age_secs: 0, error: Some("stream_not_in_mine_list".into()),
        },
    }
```

### Dashboard render

YT endpoints with `youtube_health.is_some()` show a badge. Tooltip text:

```
Status: active / good
Issue: videoIngestionStarved
1920x1080 @ 30fps
(probed 7s ago)
```

If `error.is_some()`: red badge, tooltip shows the error label and a link/hint to fix (e.g. "Re-grant: /youtube/oauth/start?label=bb").

### Audit row

On `top_issue` value change from one issue to another (or `None ↔ Some`), emit `Action::YoutubeIssueChanged`. Bounded once per 30 s per endpoint.

### Rate-limit guardrails

`liveStreams.list` costs 1 quota unit. Worst case: 5 active YT endpoints, 15 s poll = 28 800 units/day < 10 000 default cap **wait — that's over**. Refine: probe each oauth label at most once per 15 s regardless of endpoint count (one `list` returns all of that oauth's streams). 2 labels × (86 400 / 15) = 11 520 units/day. Add pause when no event is delivering (probe only while at least one VPS instance is `delivering`). Realistic: 4 h/week of live broadcasts = ~960 units/week per label. Well under quota.

## Error handling

| Failure mode | Handling | Surface |
|---|---|---|
| OAuth refresh 400/401 | Warn-log; `youtube_health.error = Some("oauth_invalid")`; 5 min backoff before next refresh attempt. | Dashboard tooltip: "OAuth invalid — re-grant /youtube/oauth/start?label=bb". Audit once per backoff cycle. |
| OAuth refresh 403 (Google project Testing mode) | Same as above with `error = Some("oauth_app_not_production")`. | Memory note documents that `restreamer-489321` is the only Production-mode project. |
| `liveStreams.list` 429 quota exceeded | Exponential backoff per oauth (30 s → 60 s → 120 s capped). | `age_secs` grows; badge greys after 30 s. |
| Network timeout | 10 s request timeout; 1 retry; then `age_secs` increments on each loop. | Grey badge after 30 s. |
| Stream key not in `mine=true` list | `stream_status: "unbound"`. | Diagnoses key/channel mismatch from a single look at the dashboard. |
| `youtube_oauth_id` references a deleted row | `youtube_health.error = Some("oauth_missing")`; warn-log. | Red badge, tooltip: "OAuth row deleted — re-link endpoint". |
| Probe panic | `tokio::spawn` wrapped with catch; restarted after 30 s; logged. | n/a. |
| Endpoint stopped mid-loop | Final probe sees `streamStatus` `ready` / `inactive` and emits one audit row; then idles. | n/a. |

Per `comprehensive-logging.md`: every probe attempt logs `endpoint_alias, oauth_label, latency_ms, stream_status, health_status, top_issue` at DEBUG. Errors at WARN with full context.

## Testing

TDD strict: RED test commit lands before GREEN impl commit in every pair. Mutation testing covers all new helpers.

### Unit tests

| # | File | Assertion |
|---|---|---|
| 1 | `crates/rs-core/src/db/youtube_oauth_tests.rs` | `youtube_oauths` table supports multiple labels; `get_oauth_by_label('default')` and `get_oauth_by_label('bb')` both return correct tokens. |
| 2 | same | `upsert_oauth_by_label` is idempotent: re-running with same label updates existing row, does not create duplicate. |
| 3 | `crates/rs-core/src/models.rs` tests block | `EndpointConfig` JSON round-trip preserves `youtube_oauth_id` field, including `null`. |
| 4 | `crates/rs-youtube/src/streams_tests.rs` (new) | `list_streams_for_label` uses bearer matching the label's token (wiremock). |
| 5 | same | `list_streams_for_label` calls token refresh when `expires_at` is in the past and persists the new tokens. |
| 6 | `crates/rs-api/src/yt_health_extract_tests.rs` (new) | Given fixture `liveStreams.list` JSON with `videoIngestionStarved` first in `configurationIssues`, `YoutubeHealth.top_issue == Some("videoIngestionStarved")`. |
| 7 | same | When `top_issue` changes between probes, `Action::YoutubeIssueChanged` is emitted to the audit ring exactly once. |

### Integration tests (`crates/rs-api`)

| # | File | Assertion |
|---|---|---|
| 8 | `crates/rs-api/src/delivery_status_yt_health_tests.rs` (new) | Endpoint with `youtube_oauth_id` set and wiremock returning `streamStatus=active, healthStatus=good` ⇒ `DeliveryEndpointMetrics.youtube_health.is_some()` and `health_status == "good"`. |
| 9 | same | Endpoint with `youtube_oauth_id = None` ⇒ `youtube_health.is_none()`. |
| 10 | same | OAuth refresh returns 401 ⇒ `youtube_health.error == Some("oauth_invalid")`. |

### Frontend Playwright (`e2e/frontend.spec.ts`)

| # | Assertion |
|---|---|
| 11 | `POST /api/v1/_test/ws-broadcast` a `DeliveryStatus` containing `youtube_health: { health_status: "bad", top_issue: "videoIngestionStarved", ... }` ⇒ dashboard endpoint card shows red YT badge and tooltip contains `videoIngestionStarved`. Asserts zero console errors (per `browser-console-zero-errors.md`). |

### E2E for the working channel (CI)

| # | Assertion |
|---|---|
| 12 | The existing CI E2E OBS-to-YouTube test asserts `DeliveryEndpointMetrics.youtube_health.is_some()` on the e2e-test endpoint within 60 s of `deliver_start`, and `health_status == "good"`. This locks the diagnostic regression on CI even before bb infra ships in production. |

### Production verification (post-merge, manual — out of automation scope)

1. Operator runs `/youtube/oauth/start?label=bb` on production server, completes OAuth.
2. Operator links `ytbb` endpoint to the `bb` OAuth via dashboard.
3. Operator starts delivering.
4. Operator captures `top_issue` value, `cdn.resolution`, `cdn.frameRate` from dashboard.
5. Operator files follow-up issue ("ytbb root cause") with that data attached. That follow-up issue's spec produces the actual fix.

## Operator validation

- Liveness: `cargo build` passes in CI on `dev`.
- Functional, CI level: e2e-test endpoint shows YT health in metrics (test 12).
- Functional, production level: ytbb endpoint dashboard shows a YT health badge with concrete `top_issue` value within 30 s of deliver-start.

## Out of scope

- The fix for ytbb's actual root cause (waits on observed data from this spec).
- Production deploy of the fix (separate follow-up issue / spec / plan).
- Switching push from `rtmp://` to `rtmps://` (separate concern; will revisit if observed data implicates it).
- Push-side GOP / keyframe audit (same).
- Visualizing historical YT health over time (a chart / time-series surface is a follow-up).

## Risks

| Risk | Mitigation |
|---|---|
| OAuth quota exhaustion if probe loop misconfigured | Per-oauth (not per-endpoint) polling; 15 s minimum interval; pause when nothing delivering; explicit unit test asserting interval. |
| Production OAuth grant leakage | OAuth tokens stored in DB only; never logged; reuse existing pattern. |
| Probe latency adds to monitor loop | Probe runs `tokio::spawn`-detached, not in critical loop. 10 s timeout cap. |
| Adding `youtube_oauth_id` column breaks older clients reading the DB | NULL default + serde `#[serde(default)]` on `EndpointConfig` keeps existing `config.json` parsing unchanged. |
| Migration on production DB | Incremental ALTER only; safe per `database-migrations.md`. |

## Acceptance

- Version bump 0.9.0 → 0.10.0 in 4 files per project rule.
- All RED → GREEN test pairs in commit history (test commit immediately precedes impl commit).
- CI green: lint, fmt, clippy (CI-side), tests, file-size, coverage, mutation testing, frontend Playwright, e2e OBS-to-YouTube (asserts YT health for e2e-test endpoint).
- PR description references this spec.
- Post-merge: deployed to streamsnv via existing CI deploy job; v0.10.0 visible in dashboard footer.
- Operator can complete the production verification flow above and observe ytbb's `top_issue` on the dashboard. Follow-up issue then filed with captured data.
