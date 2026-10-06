---
name: stream-lan-operations
description: Operations guide for stream.lan Windows PC - MCP tools, OBS WebSocket API, Restreamer client, and full-flow testing
---

# stream.lan Operations Guide

This skill documents ALL operations for the stream.lan Windows PC. **USE THIS SKILL** instead of asking the user for information or giving manual instructions.

## Quick Reference

| Service         | URL/Port                            | Credentials                                      |
| --------------- | ----------------------------------- | ------------------------------------------------ |
| MCP Server      | `win-stream-snv` (stream.lan:8090)  | Bearer token (configured in ~/.claude.json)      |
| OBS WebSocket   | `ws://stream.lan:4455`              | password: `<OBS WS password — live value lives in OBS on stream.lan (obs-websocket settings), owned by the camera-box project; the value historically committed here was stale and is dead>` (auth not required) |
| Restreamer API  | `http://127.0.0.1:8910`             | NOT truly local-only — publicly reachable via the cloudflared tunnel (see Security note) |
| Restreamer RTMP | `rtmp://stream.lan:1234/live/{app}` | (no auth)                                        |

### SECURITY: `:8910` is exposed to the public internet via the cloudflared tunnel

`cloudflared` runs on stream.lan and tunnels `https://streamsnv.newlevel.media`
→ `http://localhost:8910`. So a request from the **public internet** arrives at
the API from `127.0.0.1` (the local cloudflared socket). Consequence: **an
`addr.ip().is_loopback()` check ALONE does NOT make an endpoint local-only** —
it lets the whole internet through. This bit `/api/v1/diag/dump` (#205: 13 KB of
internal data — VPS IPv4, Hetzner ids, event ids, s3_fetch_profile — was world-
readable despite a loopback guard).

The tunnel is **token-managed remotely**, so a cloudflared-side `path:` deny is
NOT reliably available to us — the CODE must close the exposure.

**Since v0.29.22 you no longer gate endpoints one at a time** (#70/#273/#337/
#339). `crates/rs-api/src/access.rs` is router-wide middleware in front of
everything — API, `/ws` and the dashboard SPA — so a new route is protected the
moment it is registered, and a route-coverage test fails CI if it somehow is
not. Nothing to remember, nothing to add per endpoint.

- Local (loopback / RFC1918 / Tailscale, and NO forwarded header) → allowed,
  unauthenticated, with zero network I/O. Never break this: it is what keeps
  the church LAN working on a Sunday when Cloudflare or the building's internet
  is down.
- Internet (any forwarded header, or a public peer) → needs a valid Cloudflare
  Access JWT. Layer 1 is the Access application at the edge; see
  `docs/cloudflare-tunnel-setup.md`.
- `/api/v1/_test/*` is stricter: **loopback only**, so even a signed-in remote
  operator cannot call `s3-block` and starve the delivery cache. CI satisfies
  this — every `_test` call in ci.yml goes to `127.0.0.1`.
- Mutating requests also need a same-origin `Origin` and a non-`cross-site`
  `Sec-Fetch-Site` (#339 CSRF), and CORS is same-origin only — no
  `allow_origin(Any)`.

`api.access.mode` is the no-rebuild rollback: `log_only` = pre-#273 behaviour,
`lan_only` = refuse all remote control. `api.diag_token` is GONE (deleted with
#273 — it was a shared secret living in the config file that leaked in #336 and
was rewritable via `PATCH /config`); `/diag/dump` is now an ordinary gated route.

## MCP Access

The `win-stream-snv` MCP server provides full Windows desktop control for stream.lan. It runs in the user's desktop session (Session 1+), so GUI apps started via MCP are immediately visible — no Task Scheduler workaround needed.

**Key tools:**

| Tool                                  | Purpose                     |
| ------------------------------------- | --------------------------- |
| `mcp__win-stream-snv__Shell`          | Run PowerShell/cmd commands |
| `mcp__win-stream-snv__ListProcesses`  | List running processes      |
| `mcp__win-stream-snv__KillProcess`    | Kill a process by name      |
| `mcp__win-stream-snv__ServiceList`    | List Windows services       |
| `mcp__win-stream-snv__ServiceStart`   | Start a Windows service     |
| `mcp__win-stream-snv__ServiceStop`    | Stop a Windows service      |
| `mcp__win-stream-snv__FileRead`       | Read a file                 |
| `mcp__win-stream-snv__FileWrite`      | Write a file                |
| `mcp__win-stream-snv__PortCheck`      | Check if a port is open     |
| `mcp__win-stream-snv__Snapshot`       | Take desktop screenshot     |
| `mcp__win-stream-snv__OCR`            | Extract text from screen    |
| `mcp__win-stream-snv__NetConnections` | List network connections    |

## Stream OBS: camera-box owns it (read + Start/Stop streaming only)

**Owner directive 2026-08-30, verbatim: "na stream nb robi sa vyvoj aj obska tak ty nic nesahaj
do obs a tak si rob vyvoj iba si zapinaj vypinaj streamovanie ale nic viac".** OBS on stream.lan
is the camera-box project's development target. Restreamer (this session, CI, scripts) may only:

- **start and stop streaming**: `StartStream` / `StopStream` over the websocket, or Restreamer's
  own `POST /api/v1/obs/start-stream` / `stop-stream`. Stop only a stream YOU started;
- **read status**: `GetStreamStatus`, `GetRecordStatus`, `GetVersion`,
  `GetCurrentProgramScene`, `GetStreamServiceSettings`.

Never do any of the following: kill / close / relaunch / suspend obs64, run or (re)register an
OBS scheduled task, edit anything under `%APPDATA%\obs-studio` (service.json, streamEncoder.json,
global.ini, scene collections), change the scene / profile / bitrate / stream service, "restore"
OBS settings, or start/stop a recording (StopRecord included). Scene `PRO` is the owner's production
scene and is never selected by anyone. The CI guard `scripts/ci/verify_no_obs_mutation.py`
(test-integrity, #374) fails the build on any of these in ci.yml or scripts/.

**When OBS is not usable** (not running, two obs64, websocket down, wrong scene, wrong stream
service, recording): do NOT fix it. Report it to camera-box (a camera-box ticket, or a comment on
the restreamer ticket that hit it) and wait or work on something else.

### What CI expects from camera-box's TEST mode

`scripts/ci/obs-readiness-check.ps1` runs before every CI StartStream and FAILS with
`stream OBS not ready (<what>) -- camera-box owns it, not touching it` unless:

- exactly one obs64 runs and `ws://127.0.0.1:4455` answers;
- the program scene is `Development` (camera-box issue 1380, `scripts/lib/stream-dev-scene.sh`);
- the stream service is `rtmp_custom` -> `rtmp://127.0.0.1:1234/live` (any key; the inpoint
  accepts every key under `live`);
- OBS is not streaming and not recording (`stream OBS is recording -- not touching it`).

The OBS-to-YouTube and FB jobs run on whatever encoder / bitrate camera-box's TEST mode sets. If
a gate needs an OBS setting the TEST mode lacks, it is a requirement for camera-box, never a
setting CI changes.

## Restreamer Local Client

### Service Status

```
# Check if running
mcp__win-stream-snv__ListProcesses filter="restreamer"

# Or via Shell
mcp__win-stream-snv__Shell command="Get-Process -Name Restreamer | Format-List Id,SessionId,WorkingSet64"
```

### Restreamer API (local)

```
# Get status
mcp__win-stream-snv__Shell command="Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/status' | ConvertTo-Json"

# Get chunk status
mcp__win-stream-snv__Shell command="Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks' | ConvertTo-Json -Depth 3"

# Get streaming events
mcp__win-stream-snv__Shell command="Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/streaming-events' | ConvertTo-Json -Depth 3"
```

### Restreamer Config

Location: `C:\ProgramData\Restreamer\config.json`

Note: the block below is illustrative only — the `endpoint`/region shown is the
retired Linode setup (prod migrated to Hetzner fsn1 per #38). The real live
values (bucket, endpoint, credentials) are read from
`C:\ProgramData\Restreamer\config.json` on stream.lan itself — never hardcode
them here.

```json
{
  "client_uuid": "95da874e-6b06-41e5-99db-6f47a459c48b",
  "manager_url": "https://restreamer.newlevel.media",
  "s3": {
    "bucket": "restreamer-chunks",
    "region": "eu-central-1",
    "endpoint": "https://eu-central-1.linodeobjects.com",
    "access_key_id": "<S3 access key — live value in C:\\ProgramData\\Restreamer\\config.json on stream.lan; the value historically committed here was a stale, dead Linode-era key>",
    "secret_access_key": "<S3 secret key — same as above>"
  },
  "inpoint": {
    "chunk_duration_ms": 1000,
    "rtmp_port": 1234,
    "rtmp_bind": "0.0.0.0"
  }
}
```

### Restart Restreamer

```
# Kill existing process
mcp__win-stream-snv__KillProcess name="Restreamer"

# Wait a moment, then start via Shell (MCP runs in user session, so GUI works directly)
mcp__win-stream-snv__Shell command="Start-Process 'C:\Program Files\Restreamer\Restreamer.exe'" cwd="C:\Program Files\Restreamer"

# Verify it's running
mcp__win-stream-snv__ListProcesses filter="restreamer"
```

## Visual Verification

MCP provides visual inspection capabilities that were not possible with SSH:

```
# Take a desktop screenshot to verify UI state
mcp__win-stream-snv__Snapshot

# Extract text from the screen (useful for verifying dialog content)
mcp__win-stream-snv__OCR
```

Use `Snapshot` after starting apps, changing configs, or any operation where visual confirmation is valuable.

## Full-Flow Testing Procedure

### Prerequisites Checklist

1. [ ] Restreamer running on stream.lan
2. [ ] Stream OBS in camera-box's TEST mode (see "Stream OBS: camera-box owns it" above)
3. [ ] Manager server accessible (restreamer.newlevel.media)
4. [ ] S3 credentials configured
5. [ ] Streaming event with `receiving_activated=True`
6. [ ] Delivering server running

### Step 1: Verify Restreamer is Ready

```
# Check process running
mcp__win-stream-snv__ListProcesses filter="restreamer"

# Check no existing chunks (fresh test)
mcp__win-stream-snv__Shell command="(Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks').Count"
```

### Step 2: Check stream OBS is ready (read-only)

Run the same read-only check CI runs (it never changes OBS):

```
mcp__win-stream-snv__Shell command="powershell -NoProfile -ExecutionPolicy Bypass -File <checkout>\scripts\ci\obs-readiness-check.ps1"
```

Not ready -> report it to camera-box (see above). Never switch the stream service yourself.

### Step 3: Start Streaming via OBS WebSocket API

**Use Python directly from Linux machine (PREFERRED - fully automated):**

```python
python3 -c "
import asyncio
import websockets
import json

async def start_streaming():
    uri = 'ws://stream.lan:4455'
    async with websockets.connect(uri) as ws:
        await ws.recv()  # Hello
        await ws.send(json.dumps({'op': 1, 'd': {'rpcVersion': 1}}))
        await ws.recv()  # Identified

        # Start streaming
        request = {
            'op': 6,
            'd': {
                'requestType': 'StartStream',
                'requestId': 'start-stream-1'
            }
        }
        await ws.send(json.dumps(request))
        response = await ws.recv()
        print(response)

asyncio.run(start_streaming())
"
```

**Check OBS streaming status:**

```python
python3 -c "
import asyncio
import websockets
import json

async def get_status():
    async with websockets.connect('ws://stream.lan:4455') as ws:
        await ws.recv()
        await ws.send(json.dumps({'op': 1, 'd': {'rpcVersion': 1}}))
        await ws.recv()
        await ws.send(json.dumps({'op': 6, 'd': {'requestType': 'GetStreamStatus', 'requestId': '1'}}))
        print(await ws.recv())

asyncio.run(get_status())
"
```

### Step 4: Verify Chunks Being Created

```
# Watch for new chunks
mcp__win-stream-snv__Shell command="$chunks = Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks'; Write-Host 'Total chunks:' $chunks.Count; $chunks | Select-Object -Last 3 | ForEach-Object { Write-Host 'ID:' $_.id 'Created:' $_.created_at 'Sent:' $_.sent }"
```

### Step 5: Stop streaming (after test)

Send `StopStream` (same pattern as Step 3), or `POST /api/v1/obs/stop-stream`. Nothing else:
the stream service, scene and recording stay as camera-box set them.

## Troubleshooting

### `gh run view --log-failed` returns empty on self-hosted E2E jobs

**Symptoms:** `gh run view <id> --log-failed` returns 0 bytes / no output for a failed self-hosted-runner job (E2E Streaming/OBS-YouTube/FB Push), even though the run genuinely failed.

**Fix:** fetch the specific job's log directly via the REST API instead:

```bash
gh api repos/zbynekdrlik/restreamer/actions/jobs/<jobId>/logs
```

Get `<jobId>` from `gh run view <id> --json jobs --jq '.jobs[] | select(.conclusion=="failure") | .databaseId'`.

### One-in-flight-push discipline vs required-check bookkeeping

When a second push lands while a prior push-triggered run is still active (violates the "ONE in-flight CI run" rule — see project CLAUDE.md), the correct recovery is `gh run cancel <old-run-id>`. But a **cancelled** run still records `failure` conclusions on required-check contexts (`Rust CI Gate`, `E2E Gate`, `Version check`) for jobs that had already started — and GitHub's PR merge-gate looks at the check-run history for the head SHA across ALL runs, not just the latest. A cancelled run's stale `failure` entries can block merge (`mergeStateStatus: BLOCKED`) even when a later authoritative run (e.g. a `workflow_dispatch` re-run of the same SHA) is fully green.

**Fix:** once the authoritative green run exists for the same commit, `gh run delete <cancelled-run-id>` to remove its stale failure entries from the check-run history, then the PR mergeable state clears. Document the bookkeeping on the PR (a comment explaining which run is authoritative) so the audit trail isn't lost. Do not `--admin` merge — deleting the misleading cancelled-run artifacts is the honest fix, not a bypass.

### CRITICAL: Old Python Client Blocking RTMP Port

**Symptoms:** Chunks stop being created, old timestamps, OBS connected but no new data.

**Cause:** Old Python local client at `C:\Users\newlevel\restreamer\local-client\` spawns ffmpeg that steals port 1234.

**Diagnosis:**

```
# Check what owns port 1234
mcp__win-stream-snv__PortCheck host="127.0.0.1" port=1234

# Check network connections for port 1234
mcp__win-stream-snv__NetConnections

# Check if ffmpeg is from old client
mcp__win-stream-snv__Shell command="Get-Process ffmpeg -ErrorAction SilentlyContinue | Select-Object Id, Path"
# BAD: Path = C:\Users\newlevel\restreamer\local-client\ffmpeg.exe
# GOOD: No ffmpeg, Rust service handles RTMP directly
```

**Fix:**

```
# Kill old Python client and ffmpeg
mcp__win-stream-snv__KillProcess name="ffmpeg"
mcp__win-stream-snv__Shell command="Get-Process python*, python3* -ErrorAction SilentlyContinue | Stop-Process -Force"

# Restart Restreamer to bind port 1234
mcp__win-stream-snv__KillProcess name="Restreamer"
mcp__win-stream-snv__Shell command="Start-Process 'C:\Program Files\Restreamer\Restreamer.exe'" cwd="C:\Program Files\Restreamer"

# Verify Restreamer owns port
mcp__win-stream-snv__PortCheck host="127.0.0.1" port=1234
```

### OBS Not Connecting to Restreamer RTMP

1. Check Restreamer is running: `mcp__win-stream-snv__ListProcesses filter="restreamer"`
2. Check NO ffmpeg is blocking port 1234 (see above)
3. Check firewall allows port 1234
4. Read OBS's stream service with `GetStreamServiceSettings` (read-only). If it does not point at
   `rtmp://127.0.0.1:1234/live`, report it to camera-box; never change it.

### No Chunks Being Created

1. Verify RTMP stream is actually being sent
2. **Check port 1234 is owned by Rust service, not old Python ffmpeg**
3. Check Restreamer logs: `mcp__win-stream-snv__FileRead path="C:\ProgramData\Restreamer\logs\"`
4. Verify config.json has correct settings

### Chunks Not Uploading to S3

1. Check S3 credentials in config.json
2. Check internet connectivity: `mcp__win-stream-snv__Ping host="eu-central-1.linodeobjects.com"`
3. Check manager API is accessible

## OBS Management Rules

See "Stream OBS: camera-box owns it" above: read status and Start/Stop streaming only. No kill,
no restart, no relaunch, no config edit, no recording control -- not even a "graceful" `ExitOBS`
or a crash-dialog click. Those are camera-box's (`.claude/skills/obs-ops` in the camera-box repo).

## No-CI hotfix of the delivery VPS binary (rs-delivery) — S3-object swap

The fast-endpoint / delivery logic runs in **`rs-delivery`** (Linux binary on the Hetzner VPS), NOT in the Windows `Restreamer.exe`. To hotfix it during a live event **without CI** (a CI push auto-deploys to the live stream.lan box — forbidden mid-event) and **without touching the running stream**:

1. **Fix on `dev`** (RED→GREEN + double review). Do NOT push to origin.
2. **Build the Linux binary off dev1** (dev1 is Tier-0 / OOMs): `git bundle` the commits → scp to **dev2** → `cargo build --release -p rs-delivery` there (`# airuleset:build-ok`, rustup user-local). Run `cargo clippy -p rs-delivery --all-targets -- -D warnings` + `cargo test -p rs-delivery --lib` there too.
3. **glibc MUST match the VPS**: VPS image is `ubuntu-24.04` (glibc 2.39) — see `delivery.rs` `create_server("ubuntu-24.04")`. dev2 (24.04) build → `objdump -T rs-delivery | grep GLIBC` max must be ≤ 2.39. A newer-glibc binary crashes on VPS boot.
4. **Transfer the binary to stream.lan.** The box often CANNOT reach dev1 (no tailscale on the box; LAN subnets drift to 10.77.9 vs dev1's fallback 10.77.10 — file-drop `airuleset.py share` fails box→dev1). But **dev1 CAN reach the box** (asymmetric routing) and the box runs **OpenSSH sshd on :22**. So `scp` dev1→box: `sshpass -e scp rs-delivery 'newlevel@10.77.9.204:C:/Users/newlevel/<name>'` (SSHPASS env, box login user `newlevel`). **Always re-verify sha256 on the box** — an internet blip truncated a transfer silently to 1 MB with `scp` exit 0; re-scp and re-check until the box sha matches the dev2 sha.
5. **Upload to S3 FROM the box** (creds stay local, never echoed): read `C:\ProgramData\Restreamer\config.json` `.s3` (endpoint `fsn1.your-objectstorage.com`, bucket `restreamer-chunks-fsn1`, access/secret key), set `AWS_*` env, `aws --endpoint-url <ep>` is present on the box. **Back up first**: `aws s3 cp s3://<b>/rs-delivery-<ver> s3://<b>/rs-delivery-<ver>.release-backup-preNNN`. Then overwrite the versioned key the client requests: `aws s3api put-object --key rs-delivery-<ver> --body <file> --acl public-read --content-type application/octet-stream` (public-read = cloud-init fetches anonymously).
6. **Verify via anonymous GET** (exactly cloud-init's path): `Invoke-WebRequest <ep>/<b>/rs-delivery-<ver>` → sha256 must equal the built binary.
7. **Effect + caveats**: takes effect on the NEXT `Start Delivering` (fresh VPS); the running stream + stream.lan exe are untouched. Keep the patched binary reporting the SAME version (don't bump) so the post-boot lockstep gate (`delivery_binary.rs versions_match`) passes. This DEVIATES from the immutable-versioned-object design — track it and replace with a proper CI-built `+1` release; then the backup key can be deleted. Full functional proof only comes when the next round spins a VPS.

## Important Notes

- **NEVER** give manual instructions when these automated MCP tools exist
- **NEVER** kill, restart or reconfigure stream OBS -- camera-box owns it (#374); Start/Stop streaming only
- **ALWAYS** verify current state before making changes
- **ALWAYS** backup Restreamer configs before modifying (use FileRead + FileWrite or Shell Copy-Item)
- The last chunk timestamp tells you if streaming is active
- MCP runs in user desktop session — no Task Scheduler needed for GUI apps
