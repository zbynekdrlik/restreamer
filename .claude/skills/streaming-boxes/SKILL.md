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
| S3 region | `fsn1` (`fsn1.your-objectstorage.com` → 88.198.120.64) |
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
| S3 region | `nbg1` |
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
