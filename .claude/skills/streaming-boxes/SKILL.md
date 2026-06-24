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
| Disk | ~82% full → "warn" disk_pressure (source of upload-jitter issues) |
| Subnet | 10.77.8.x — NOT reachable from dev1 (10.77.9.x) |

**Access streampp dashboard**: via `win-streampp` MCP locally OR from dev2 (10.77.8.134, same subnet).

No ffmpeg, OBS installed but no headless task. As of 2026-06-07 running v0.22.6 (manually deployed via NSIS for the fast-stream fix; leave it unless explicitly asked to update).

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
