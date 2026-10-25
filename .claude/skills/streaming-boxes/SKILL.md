---
name: streaming-boxes
description: >
  Reference for the two streaming boxes (stream.lan on Windows, stream-pp on Linux) —
  IPs, MCP names, subnets, API endpoints, soak/test recipe, dashboard quirks.
  Load when working on stream.lan or streampp deployment, CI E2E jobs, VPS
  delivery, or any task involving the production streaming infrastructure.
triggers:
  - stream.lan
  - streampp
  - win-stream-snv
  - stream-pp
  - linux-stream-pp
  - streaming box
  - soak test
  - fast endpoint
---

# Streaming Boxes Reference

Two Windows streaming boxes (separate from dev1/dev2). Both run the single unified binary `C:\Program Files\Restreamer\Restreamer.exe` (~32 MB, Tauri+embedded service+WASM) via `RestreamerGUI` scheduled task in the interactive desktop session.

- **API**: `http://127.0.0.1:8910`
- **Config/DB**: `C:\ProgramData\Restreamer\` (contains `sqlite3.exe`)
- **Update methods**: NSIS installer `Restreamer_<ver>_x64-setup.exe` (silent `/S`) OR `scripts/install.ps1` (self-elevates UAC)
- **MCP runs as Admin** in the interactive session

## Box Details

### stream.lan — Primary CI/E2E Box

| Property | Value |
|---|---|
| IP | `10.77.9.204:8910` |
| User | `newlevel` |
| MCP | `win-stream-snv` (stream.lan:8090) |
| OBS MCP | `obs-stream-snv` (stream.lan:8091) |
| S3 bucket | `restreamer-chunks-fsn1` @ `fsn1.your-objectstorage.com` (the ONLY bucket — see Object storage below) |
| Disk | ~63% full (low upload jitter, max ~2s) |
| CI role | Self-hosted runner + E2E box — CI auto-deploys here every dev/main push |
| OBS ingest | `rtmp://127.0.0.1:1234/live/obs-e2e-test` |
| ffmpeg | Present (winget) |

Dashboard reachable from dev1 (returns 200).

### stream-pp — Secondary Box (Linux since 2026-10-11, #323)

The Windows streampp PC was reinstalled to **Ubuntu 26.04** (openbox kiosk run by camera-box,
stream OBS = `strih-obs.service` user unit). Its old Windows DB (endpoints, YT/FB keys, OAuth)
was lost in the reinstall. The Windows notes (MCP `win-streampp`, user `interkom`, NSIS
upgrades, tailscale IP) are obsolete.

| Property | Value |
|---|---|
| Host | `stream-pp`, LAN `10.77.8.204` (PP LAN, gateway 10.77.8.1) |
| From dev1 | MCP `linux-stream-pp` (remoteos MCP, `http://stream-pp.lan:8092/mcp`; dev1 resolves `stream-pp.lan` to 10.76.8.204); from scripts `remoteos-call linux-stream-pp Shell '<cmd>'` / `put` / `get` (#422). ssh `newlevel@10.76.8.204` is break-glass only, for when the MCP server is down. **No tailscale on the box** (owner, 2026-10-11). |
| User | `newlevel`, passwordless `sudo -n` since 24.10.2026 (owner) |
| Restreamer | apt-installed `.deb` (`dpkg -s restreamer`), `systemctl --user` unit `restreamer.service` (GUI operator window since #393, enabled; the kiosk autostart also starts it) |
| Config / data | `~/.config/restreamer/config.json` (own `client_uuid`, obs ws `127.0.0.1:4455` without auth) / `~/.local/share/restreamer` (DB, chunks, `restreamer.log`, `logs/stall.log`) |
| Dashboard | `http://10.77.8.204:8910` on the PP LAN (`http://10.76.8.204:8910` from dev1, Playwright works) |
| OBS ingest | `rtmp://127.0.0.1:1234/live` (camera-box sets it); restreamer may only Start/Stop streaming via its API. Never put `PRO` on program/preview. |

**Upgrade:** the self-upgrade timer does it (#323). By hand: the recipe in
`.claude/rules/linux-build.md` ("Upgrading a Linux box by hand", `remoteos-call put` + `Shell`
with `sudo -n dpkg -i`). Verify: `dpkg -s restreamer`, `/api/v1/status` 200, the dashboard DOM
version, `obs/status` connected.

**Ingest-only test without a VPS:** `pp-ingest-test.sh` recipe (2026-10-11). Create or reuse the
event `PP-Ingest-Test`, `POST /events/{id}/activate`, `POST /obs/start-stream`, sample
`/api/v1/status` (inpoint state, `ingest_gaps`, skew), `POST /obs/stop-stream`, then
`/events/{id}/deactivate`. Check `/api/v1/uploads/stats` and `stall.log`.

**Full-path test PP → VPS → YouTube (passed 2026-10-13, #387):** PP event 2 `PP-FullPath-Test`
with only endpoint 14 `e2e rtmp` (the CI YouTube test stream; its key comes from SNV endpoint 26,
copied without printing it). Use a SYNTHETIC source, not PP OBS. PP OBS program is usually `PRO`
plus the live mixer audio (likely music, a YouTube ban risk), and PP has no YT-safe scene.
- Run ffmpeg ON stream-pp: `testsrc2` 1080p30 + `sine` 1 kHz at -20 dB, to
  `rtmp://127.0.0.1:1234/live/pp-fullpath-test`.
- Then: activate, `delivery/start {event_id}`, and poll `delivery/status?event_id=` plus SNV's
  `/api/v1/youtube/status` (its OAuth sees the `e2e rtmp` stream: `active`/`good`).
- Teardown: `delivery/stop`, deactivate, kill ffmpeg, `vps_orphan_count` 0.
- Expect YouTube `bitrateLow` (the stream is configured for 2160p) and boot-time
  `vps_unreachable` warns.
- Gotcha: `pkill -f pp-fullpath-test` run through a remote shell kills that shell too (exit
  255), because the pattern is in its own command line. Use `pkill -f '[p]p-fullpath-test'`.

## Object storage — ONE bucket exists: `restreamer-chunks-fsn1`

Hetzner buckets are region-bound, so the region lives in the bucket NAME. The
only bucket is `restreamer-chunks-fsn1` (endpoint `https://fsn1.your-objectstorage.com`,
region `fsn1`). The old nbg1 bucket `restreamer-chunks` was **deleted 2026-07-27**
— it had sat unused but BILLED since the 2026-06-24 fsn1 migration (#278), 94 640
objects / 332 GB. Anything that still names it is a bug (see #347, #348).

Credentials are account-wide: the same key pair in a box's `config.json` signs
against every region's endpoint, so you can list/delete in a region no box uses.
`aws.exe` is installed on stream.lan (`C:\Program Files\Amazon\AWSCLIV2\aws.exe`)
— run S3 admin work THERE so the credentials never leave the box.

**A region migration is not finished when config.json changes.** Two things live
outside the config and were both missed in 2026-06:

- **`rescue_video_url` is DB data, not code** — `streaming_events.rescue_video_url`
  and `event_templates.rescue_video_url` hold an absolute S3 URL. stream.lan's CI
  event still pointed at the nbg1 bucket a month after the migration. Grep both
  tables for the old host before deleting anything, and PATCH via
  `/api/v1/events/{id}` (never write the live DB behind the running app).
- **`scripts/install.ps1` hardcodes the defaults a fresh box boots with** — pinned
  since #348 by `crates/rs-core/tests/install_script_defaults.rs`, which parses the
  script's `s3 = @{` block against TWO oracles: `Config::default()` (the values a
  binary boots with when `config.json` is absent) and the region-DERIVED shape
  (`<bucket>` ends `-<region>`, endpoint host is `<region>.your-objectstorage.com`).
  Either alone is escapable — a literal in the test only makes script and test
  agree while both drift from the code, and matching the code alone still lets a
  copy-paste slip mirror the SAME wrong endpoint into both.

**Deleting a bucket: `aws s3 rm --recursive` is NOT enough.** It removes objects
but leaves *incomplete multipart uploads*, and `delete-bucket` then fails with
`BucketNotEmpty` even though `list-objects-v2` returns zero keys (hit 2026-07-27:
12 abandoned `delivery-logs/*.log` MPUs, oldest from 2026-05-10). Abort them
first:

```powershell
$mp = & $aws s3api list-multipart-uploads --bucket <b> --endpoint-url $ep --region <r> --output json | ConvertFrom-Json
foreach ($u in $mp.Uploads) {
  & $aws s3api abort-multipart-upload --bucket <b> --key $u.Key --upload-id $u.UploadId --endpoint-url $ep --region <r>
}
& $aws s3api delete-bucket --bucket <b> --endpoint-url $ep --region <r>
```

A full-bucket wipe is long (94 640 objects took ~44 min) — the MCP `Shell` tool
times out at 30 s, so launch it as a detached `Start-Process powershell -File`
writing to a log and poll that log, rather than waiting inline.

## Fast Endpoints (is_fast=1)

- **stream.lan**: ids 2 (Control Stream SNV), 22 (KS-PP-TEST), 30 (Control stream)
- **NOTE**: "e2e rtmp" (id 26) is NOT fast — CI's fast path is Control/KS endpoints
- **streampp**: KS-PP-TEST (id 22)

## Dashboard Update — NSIS Does NOT Update LAN Dashboard

**Critical gotcha**: NSIS installer does NOT update the LAN dashboard (`www\` next to the exe). The browser dashboard is served from `<exe_dir>\www` (ServeDir); the NSIS installer never ships it — only CI's deploy job writes it (stream.lan only).

**Full manual upgrade = NSIS install + replace `www\`**:
```bash
gh run download <run> --name restreamer-www
# Then copy the www/ folder to C:\Program Files\Restreamer\www\ on the target box
```
No app restart needed (per-request disk reads). Also: `index.html` has no cache-control → browsers heuristic-cache it for days; hard refresh needed after `www\` swap. Proper fix tracked in **#248**.

## Soak/Test Recipe

```
# Setup
POST /api/v1/events {name}
POST /api/v1/events/{id}/endpoints/{epId}   # attach endpoint
ffmpeg source to the inpoint
POST /api/v1/events/{id}/activate           # sets receiving only
POST /api/v1/delivery/start {event_id}      # creates Hetzner VPS + sets delivering

# Teardown
POST /api/v1/delivery/stop {event_id}       # deletes VPS
POST /api/v1/events/{id}/deactivate
# detach all endpoints
DELETE /api/v1/events/{id}
```

**Note**: Inducing S3-upload starvation via single-IP firewall block does NOT work — HTTPS connection-pooling keeps uploads flowing.

## OBS MCP Server (stream.lan only)

`sbroenne/mcp-server-obs` v1.0.4 installed at `C:\Tools\obs-mcp-server\`.

- **Gateway**: supergateway wraps stdio as streamableHttp on port 8091
- **Startup**: Scheduled task `ObsMcpGateway` runs at logon (user: newlevel)
- **Start script**: `C:\Tools\obs-mcp-server\start-gateway.bat`
- **MCP config**: `.mcp.json` entry `obs-stream-snv` → `http://10.77.9.204:8091/mcp`
- **Auth**: No password needed (auth_required=false in OBS WebSocket config)

**Available tools (prefix `mcp__obs-stream-snv__`):**

| Tool | Purpose |
|---|---|
| `obs_connection` | Connect, Disconnect, GetStatus, GetStats |
| `obs_recording` | Start, Stop, Pause, Resume, GetStatus, GetSettings, SetFormat, SetQuality, SetPath |
| `obs_streaming` | Start, Stop, GetStatus |
| `obs_scene` | List, GetCurrent, Set, ListSources |
| `obs_source` | AddWindowCapture, ListWindows, SetWindowCapture, Remove, SetEnabled |
| `obs_audio` | GetInputs, Mute, Unmute, GetMuteState, SetVolume, GetVolume, MuteAll, UnmuteAll |
| `obs_media` | SaveScreenshot, StartVirtualCamera, StopVirtualCamera |

**#374, owner directive 2026-08-30: stream OBS is camera-box's.** From restreamer use ONLY
`obs_streaming` (Start, Stop, GetStatus) and the read-only Get*/List calls. Never `obs_recording`
Start/Stop/Set*, `obs_scene` Set, `obs_source`, `obs_audio` Mute/SetVolume, or virtual-camera
calls: those change camera-box's rig. See `.claude/skills/obs-recovery` when OBS is not usable.

## Adding a status field to the delivery-status wire (rs-delivery → dashboard)

An endpoint status field the operator dashboard shows travels a specific
ADDITIVE chain. To add one, copy `av_skew_ms` — NOT `producer_active`
(`producer_active` DEAD-ENDS in rs-api and never reaches the frontend). Touch
points, source → UI:

1. `rs-delivery` `BufferState`/`EndpointStats` → add an accessor on
   `EndpointHandle` (mirror `producer_active()`), then the field to
   `EndpointStatusEntry` (`crates/rs-delivery/src/api.rs`) + populate it in the
   `/api/status` construction. Use `#[serde(skip_serializing_if =
   "Option::is_none")]` for optional fields so an older host tolerates absence.
2. `rs-api` `EndpointDeliveryStatus` (`crates/rs-api/src/delivery_status.rs`):
   the struct is NOT `#[derive(Deserialize)]` — it's parsed field-by-field from
   a `serde_json::Value` (`entry["name"].as_…()`). Add the field, the parse
   line, the struct-literal push, AND the map into `DeliveryEndpointMetrics`
   (the same function's `poll_delivery_metrics`) — that last step is the one
   `producer_active` skips, and is REQUIRED to reach the frontend.
3. `rs-core` `DeliveryEndpointMetrics` (`crates/rs-core/src/models.rs`): add the
   field. **No `Default` derive** — EVERY struct literal must list it. There are
   ~13 across `rs-api` (lib.rs ×2, stream_handlers, status_summary, the test
   helpers, tests/api_integration) and `rs-core` (6 in-file test literals);
   placeholders use `None`. `grep -rn "DeliveryEndpointMetrics {"` to find them.
4. `leptos-ui`: add to `WsDeliveryEndpoint` (ws.rs), `CachedDeliveryEndpoint`
   (api.rs) — both `#[derive(Deserialize)]` with `#[serde(default)]` —
   `DeliveryEndpointState` (store.rs), and BOTH hand-mapping sites in ws.rs
   (`load_initial_state` + the `DeliveryStatus` WS arm).

Per-endpoint display numbers are capped in `delivery_status.rs`
(`cap_endpoint_delay_secs`) — a fast endpoint's delay is capped at
`FAST_ENDPOINT_DELAY_CAP_SECS` (120s since #295, was 30s). If a legitimate value
"can't show above N", that cap is why.
