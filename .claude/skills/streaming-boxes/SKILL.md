---
name: streaming-boxes
description: >
  Reference for the two Windows streaming boxes (stream.lan and streampp) —
  IPs, MCP names, subnets, API endpoints, soak/test recipe, dashboard quirks.
  Load when working on stream.lan or streampp deployment, CI E2E jobs, VPS
  delivery, or any task involving the production streaming infrastructure.
triggers:
  - stream.lan
  - streampp
  - win-stream-snv
  - win-streampp
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

### streampp — Secondary Box

| Property | Value |
|---|---|
| IP | `10.77.8.204:8910` |
| User | `interkom` |
| MCP | `win-streampp` |
| S3 bucket | `restreamer-chunks-fsn1` @ fsn1 (migrated 2026-06-24 after the nbg1 incident, #278 — verify live via `/api/v1/status` s3_region_standard or config.json, don't trust this snapshot). Its DB `rescue_video_url` still points at the DELETED nbg1 bucket — #347. |
| Disk | disk_pressure="ok" as of 2026-07-12 (was ~82%/"warn" on 2026-06-07 — check live via `/api/v1/status`, don't trust either snapshot) |
| Subnet | 10.77.8.x — LAN IP `10.77.8.204` NOT reachable from dev1 (10.77.9.x) |
| Tailscale | streampp has its OWN tailscale IP (`tailscale ip -4` via `win-streampp` Shell) — reachable from dev1/Playwright even though the LAN IP isn't. Don't assume unreachable; check tailscale first. |

**Access streampp dashboard**: via `win-streampp` MCP locally, from dev2 (10.77.8.134, same LAN subnet), OR via streampp's own tailscale IP (works from anywhere on the tailnet, incl. Playwright on dev1 for DOM version verification).

No ffmpeg, OBS installed but no headless task. Version drifts independently of stream.lan (manual NSIS deploys, not CI-driven) — always read the LIVE version via MCP (`(Get-Item 'C:\Program Files\Restreamer\Restreamer.exe').VersionInfo.ProductVersion`) before assuming any note here is current; it was v0.22.6 on 2026-06-07, v0.27.0 by 2026-07-12 (upgraded outside this doc's tracking), v0.29.1 after 2026-07-12's manual upgrade.

### Manual upgrade procedure (cross-subnet, no shared filesystem)

streampp can't be `scp`'d to directly from dev1 and has no shared drive — transfer via the airuleset file-drop server over tailscale (works even though the LAN IP doesn't):

```bash
# On dev1: get the installer + the matching www bundle for the SAME commit
gh release download restreamer-vX.Y.Z -R zbynekdrlik/restreamer --pattern "*.exe" --dir .
# Find the main-branch CI run for that release's merge commit, download its www artifact:
gh run list --workflow ci.yml -b main -L 5 --json databaseId,headSha,conclusion
gh run download <run-id> -R zbynekdrlik/restreamer --name restreamer-www --dir ./www
zip -qr www.zip www   # bundle for one-shot transfer

# Host both via the file-drop server, then have streampp pull them over tailscale:
python3 ~/devel/airuleset/airuleset.py share Restreamer_X.Y.Z_x64-setup.exe
python3 ~/devel/airuleset/airuleset.py share www.zip
```

On streampp (`win-streampp` Shell), download via the printed tailscale URL, install silently, then swap `www\` (NSIS never ships it — see below):

```powershell
Invoke-WebRequest -Uri "http://<dev1-tailscale-ip>:8788/<token>/Restreamer_X.Y.Z_x64-setup.exe" -OutFile "$env:TEMP\setup.exe"
Invoke-WebRequest -Uri "http://<dev1-tailscale-ip>:8788/<token>/www.zip" -OutFile "$env:TEMP\www.zip"
& "$env:TEMP\setup.exe" /S
Start-Sleep -Seconds 15   # let the silent install finish before touching www\

Remove-Item -Recurse -Force 'C:\Program Files\Restreamer\www'
Expand-Archive -Path "$env:TEMP\www.zip" -DestinationPath "$env:TEMP\www_extract" -Force
Move-Item "$env:TEMP\www_extract\www" 'C:\Program Files\Restreamer\www'
```

**Gotcha: the NSIS silent installer STOPS the `RestreamerGUI` scheduled task and does not restart it** — after `/S` completes, the task is left in `Ready` (not `Running`) state and the API stops answering. Always follow with `Start-ScheduledTask -TaskName RestreamerGUI` and re-verify `/api/v1/status` responds before declaring the upgrade done. Streaming-event state (DB) survives the upgrade untouched (confirmed 2026-07-12: same event id/name/received_bytes before and after).

Verify the new version from the LIVE DOM (not just the exe's `ProductVersion`) via streampp's tailscale IP — the `www\` swap is a separate step from the exe upgrade and can silently fail independently.

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
  since #348 by `crates/rs-core/tests/install_script_defaults.rs`, which fails if
  they drift from `rs_core::config::STANDARD_S3_REGION`.

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

Use these OBS MCP tools instead of python obsws_python hacks via win-stream-snv Shell.

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
