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
| OBS WebSocket   | `ws://stream.lan:4455`              | password: `JhRfqdTmuifYq60y` (auth not required) |
| Restreamer API  | `http://127.0.0.1:8910`             | (local only)                                     |
| Restreamer RTMP | `rtmp://stream.lan:1234/live/{app}` | (no auth)                                        |

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

## OBS WebSocket API

OBS has WebSocket enabled at `ws://stream.lan:4455`. Use this to control OBS remotely.

### OBS Configuration Files

| File                                                                                   | Purpose                   |
| -------------------------------------------------------------------------------------- | ------------------------- |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\global.ini`                              | Global OBS settings       |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json`  | Stream destination config |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\plugin_config\obs-websocket\config.json` | WebSocket settings        |

### Change OBS Stream Destination

To switch OBS from YouTube to Restreamer (or vice versa):

**Option 1: Edit service.json directly**

```
# Backup current config
mcp__win-stream-snv__Shell command="Copy-Item 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json' 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json.bak'"

# Set to Restreamer
mcp__win-stream-snv__Shell command="@{ type = 'rtmp_custom'; settings = @{ server = 'rtmp://127.0.0.1:1234/live'; key = 'test' } } | ConvertTo-Json -Depth 5 | Set-Content 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json'"

# Restart OBS for changes to take effect (see "Starting OBS Correctly" below)
```

**Option 2: Use OBS WebSocket API (preferred - no restart needed)**

```python
# Python example using obs-websocket-py (run from Linux)
import obsws
ws = obsws.obsws("stream.lan", 4455, "JhRfqdTmuifYq60y")
ws.connect()

# Set stream settings to Restreamer
ws.call(obsws.requests.SetStreamServiceSettings(
    streamServiceType="rtmp_custom",
    streamServiceSettings={
        "server": "rtmp://127.0.0.1:1234/live",
        "key": "test"
    }
))

# Start streaming
ws.call(obsws.requests.StartStream())
```

### Current OBS Stream Config

As of last check:

- **Service**: YouTube - RTMPS
- **Server**: `rtmps://a.rtmps.youtube.com:443/live2`
- **Key**: `w7dw-etzx-je1p-bted-6fdr` (YT KS-BB 4K endpoint)

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

```json
{
  "client_uuid": "95da874e-6b06-41e5-99db-6f47a459c48b",
  "manager_url": "https://restreamer.newlevel.media",
  "s3": {
    "bucket": "restreamer-chunks",
    "region": "eu-central-1",
    "endpoint": "https://eu-central-1.linodeobjects.com",
    "access_key_id": "WKSJVWJXD0BI5Z3BOMQ5",
    "secret_access_key": "1CYGMD7LUfBA8weK5GheAi3ZJVUTjne999I53BCe"
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
2. [ ] OBS running on stream.lan
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

### Step 2: Switch OBS to Restreamer

```
# Backup OBS config
mcp__win-stream-snv__Shell command="Copy-Item 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json' 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json.youtube-backup'"

# Set to Restreamer
mcp__win-stream-snv__Shell command="@{ type = 'rtmp_custom'; settings = @{ server = 'rtmp://127.0.0.1:1234/live'; key = 'test' } } | ConvertTo-Json -Depth 5 | Set-Content 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json'"

# Restart OBS (see "Starting OBS Correctly" section below)
```

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

### Step 5: Restore OBS to YouTube (after test)

```
# Restore YouTube config
mcp__win-stream-snv__Shell command="Copy-Item 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json.youtube-backup' 'C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json' -Force"

# Restart OBS (see "Starting OBS Correctly" below)
```

## Troubleshooting

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
4. Check RTMP URL format: `rtmp://127.0.0.1:1234/live/test`

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

### NEVER Kill OBS Abruptly

**DO NOT** use `KillProcess name="obs64"` — this causes:

1. Recovery dialog on next start
2. Duplicate OBS processes
3. WebSocket server not starting
4. Broken state that requires user intervention

### Proper OBS Restart (if absolutely necessary)

```
# CORRECT: Graceful shutdown via WebSocket (from Linux)
python3 -c "
import asyncio, json, websockets
async def quit_obs():
    async with websockets.connect('ws://stream.lan:4455') as ws:
        await ws.recv()
        await ws.send(json.dumps({'op':1,'d':{'rpcVersion':1}}))
        await ws.recv()
        await ws.send(json.dumps({'op':6,'d':{'requestType':'ExitOBS','requestId':'1'}}))
        print(await ws.recv())
asyncio.run(quit_obs())
"
```

### Prefer WebSocket Over Config Changes

Instead of editing service.json and restarting OBS, use WebSocket API:

- `SetStreamServiceSettings` - change stream destination
- `StartStream` / `StopStream` - control streaming
- Changes take effect immediately, no restart needed

### Starting OBS Correctly

Since MCP runs in the user's desktop session, OBS can be started directly without Task Scheduler:

```
# Start OBS with correct working directory (CRITICAL for proper initialization)
mcp__win-stream-snv__Shell command="Start-Process 'C:\Program Files\obs-studio\bin\64bit\obs64.exe' -ArgumentList '--disable-shutdown-check'" cwd="C:\Program Files\obs-studio\bin\64bit"
```

**CRITICAL**: The working directory MUST be `C:\Program Files\obs-studio\bin\64bit`. Without this, OBS starts in a broken state (~37MB memory, no WebSocket server, error dialogs). A healthy OBS uses ~1GB+ memory.

**Verify OBS started correctly:**

```
# Check process and memory usage
mcp__win-stream-snv__Shell command="Get-Process obs64 | Format-List Id, WorkingSet64"
# WorkingSet64 should be > 100MB (healthy) not ~37MB (broken)

# Verify single OBS process
mcp__win-stream-snv__ListProcesses filter="obs64"

# Verify WebSocket is listening
mcp__win-stream-snv__PortCheck host="127.0.0.1" port=4455

# Take screenshot to visually verify OBS is running
mcp__win-stream-snv__Snapshot
```

### Recovery From Bad State (duplicate OBS processes)

If OBS is in bad state with recovery dialog:

1. **Ask the user** to manually close OBS and dismiss dialogs
2. Then start fresh using the Shell command above
3. **NEVER try to kill OBS processes remotely** — this makes things worse

### Checking OBS Health

```
# Verify single OBS process
mcp__win-stream-snv__ListProcesses filter="obs64"
# Should show exactly ONE obs64.exe process

# Verify WebSocket is listening
mcp__win-stream-snv__PortCheck host="127.0.0.1" port=4455

# Visual check
mcp__win-stream-snv__Snapshot
```

## Important Notes

- **NEVER** give manual instructions when these automated MCP tools exist
- **NEVER** kill OBS with KillProcess — use WebSocket ExitOBS or ask user
- **ALWAYS** verify current state before making changes
- **ALWAYS** backup configs before modifying (use FileRead + FileWrite or Shell Copy-Item)
- The last chunk timestamp tells you if streaming is active
- OBS requires restart after service.json changes (unless using WebSocket API)
- MCP runs in user desktop session — no Task Scheduler needed for GUI apps
