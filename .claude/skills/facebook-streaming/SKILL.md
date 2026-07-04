---
name: facebook-streaming
description: >
  Facebook Live streaming gotchas, CI verification architecture, Graph API
  credentials, and operator procedures. Load when working on FB endpoints,
  FB CI gate, e2e-fb-push-stream-lan job, or diagnosing FB delivery failures.
triggers:
  - facebook
  - FB endpoint
  - FB Live
  - e2e-fb-push
  - FB Graph API
  - live_video
  - FB CI
---

# Facebook Live Streaming

## Critical Gotchas (DO NOT RE-LEARN)

### Persistent stream key requires a BOUND live_video

A FB **persistent stream key** (`FB-<id>-0-<rand>`) only ingests into a decodable `live_video` when a session is **bound** to it — either an active Live Producer composer in "ready to go live" state, OR a Graph-API-created `live_video` whose `stream_url` key you push to.

**Pushing to a bare persistent key with no bound session**: FB's edge accepts the TCP/RTMP connection (no close, no error), but **silently discards the bytes**. No `live_video` is created, nothing decodes, preview stays BLACK. This is an FB platform binding behavior, NOT a pusher bug.

**CI works because**: restreamer creates the `live_video` via `POST /<page>/live_videos` first, parses the key from `stream_url`, pushes to THAT key → ingest is bound → decodes. The per-run-fresh-broadcast design is correct.

### FB live BROADCAST dies after ~6 hours

FB's RTMP server kills broadcast sessions older than ~6 hours. Symptoms: `endpoint_rtmp_push_died` + error `upstream closed connection mid-stream: unexpected end of file` + `chunks_pushed=0` + `bytes_sent_since_connect=0`.

**When this happens**: FIRST hypothesis is "FB live broadcast expired (6h session limit)", NOT bitrate.

**Fix**: Operator opens Facebook Live Producer/Studio and **creates a NEW live broadcast** (the broadcast/session, not the key). The persistent stream key itself stays the same — the 6h cap applies to the broadcast session, not the key.

- Do NOT propose bitrate reduction as the fix
- Do NOT change the persistent stream key
- Other endpoints (YouTube, Kiko/Resolume) are unaffected — only FB enforces this limit

### Stream keys are persistent — never propose rotation

All FB stream keys configured in Restreamer are FB "Always Active" / persistent keys. Do NOT suggest the keys are "expired", "stale", "single-use", or that the user must rotate them.

### FB local signals do NOT prove FB acceptance

`alive`, `chunks_processed`, `bytes_processed_total`, zero deaths — these do NOT prove FB accepts the stream. FB can silently discard. Only Live Producer preview (or Graph API `stream_health` with `status=LIVE`) = proof.

### No ffmpeg fallback — ever

NEVER propose ffmpeg fallback for FB endpoints. ffmpeg pusher never worked correctly in production — that is the exact reason for the rust pusher migration.

### CI must be hands-free — no daily re-auth

NEVER propose a CI verification architecture that requires operator to re-authenticate periodically. Always use refresh-token / long-lived API paths. Persistent-browser-session approaches are BANNED — FB session cookies expire ~17h after operator setup.

## CI Verification Architecture (FINAL — DO NOT RE-ASK)

**Architecture**: Graph API + per-run live_video create + delete. No browser, no Playwright, no persistent Chrome profile, no daily re-auth.

For each CI push that runs `e2e-fb-push-stream-lan`:

1. **Create** broadcast: `POST https://graph.facebook.com/v17.0/$FB_PAGE_ID/live_videos?access_token=$FB_PAGE_ACCESS_TOKEN&status=UNPUBLISHED&title=CI-restreamer-test-$run_id` → returns `{id, stream_url}`
2. **Parse stream key**: from `stream_url` matches `rtmps://live-api-s.facebook.com:443/rtmp/FB-<id>-0-<random>` — the segment after `/rtmp/` IS the stream key string
3. **Seed restreamer** via `POST /api/v1/facebook/config/seed` with the fresh key
4. **Activate event + attach `e2e fb` endpoint + delivery_start**
5. **Watchdog** local signals (chunks_processed, bytes, zero deaths)
6. **Verify FB-side**: poll `GET https://graph.facebook.com/v17.0/<live_video_id>?access_token=$FB_PAGE_ACCESS_TOKEN&fields=status` — assert `status=LIVE` within ~2 min of delivery start
7. **Cleanup**: `DELETE https://graph.facebook.com/v17.0/<live_video_id>?access_token=$FB_PAGE_ACCESS_TOKEN`

**Definition of done** for FB PR: CI fully green + dashboard green + 4h soak green + CI FB auto-verification green. First-preview observation is 25%, not done.

## Visual Verification (when operator wants to SEE the stream)

UNPUBLISHED broadcasts are NOT viewable via the public producer URL. Pull a frame from FB's own DASH transcode instead:

```powershell
$dash = (Invoke-RestMethod -Uri "https://graph.facebook.com/v17.0/<live_video_id>?access_token=<token>&fields=ingest_streams{dash_preview_url}").ingest_streams[0].dash_preview_url
ffmpeg -y -i "$dash" -frames:v 1 -q:v 2 frame.jpg
```

`dash_preview_url` is a live MPEG-DASH manifest FB generates FROM our incoming RTMP. A frame extracted from it proves FB received and decoded the stream.

## Credentials (CI — GitHub Secrets)

**FB App**: `restreamer-fb-ci` / App display name `New Level Church`
- App ID: `355685179541516` (GitHub secret: `FB_APP_ID`)
- App Secret: `<FB App Secret — GitHub secret FB_APP_SECRET, NOT committed>` (GitHub secret: `FB_APP_SECRET`)

**FB Page**: "New level church"
- Page ID: `163104934022649` (GitHub secret: `FB_PAGE_ID`)

**Page Access Token** (GitHub secret: `FB_PAGE_ACCESS_TOKEN`):
- `expires_at: 0` — NEVER EXPIRES (verified 2026-05-19)
- Valid as long as: Zbynek remains admin of Page 163104934022649 + App 355685179541516 stays valid

**If token is compromised or revoked**:
1. Operator regenerates User Token in Graph API Explorer with same scopes
2. Re-run long-lived exchange + page-token derivation:
   ```bash
   LONG_USER=$(curl -s "https://graph.facebook.com/v17.0/oauth/access_token?grant_type=fb_exchange_token&client_id=$APP_ID&client_secret=$APP_SECRET&fb_exchange_token=$SHORT_TOKEN" | jq -r .access_token)
   PAGE_TOKEN=$(curl -s "https://graph.facebook.com/v17.0/me/accounts?access_token=$LONG_USER" | jq -r '.data[]|select(.id=="163104934022649")|.access_token')
   ```
3. `gh secret set FB_PAGE_ACCESS_TOKEN --body "$PAGE_TOKEN"`
4. Update this file with the new value + mint timestamp

## Iterating on FB Failures (Not CI)

For unknown FB failure modes, iterate via **direct MCP test against streamsnv + VPS binary swap** (5-15 min/cycle), NOT 80-min CI cycles. CI is for regression verification after the fix is confirmed working.

## E2E Cleanup

ALWAYS detach and delete test endpoints from stream.lan's shared E2E-Test event after soaks/experiments. Leftovers fail CI because the e2e gate checks ALL attached endpoints. Do NOT cancel-thrash stuck runs.
