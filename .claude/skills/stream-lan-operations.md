---
name: stream-lan-operations
description: Operations guide for stream.lan Windows PC - SSH, OBS WebSocket API, Restreamer client, and full-flow testing
---

# stream.lan Operations Guide

This skill documents ALL operations for the stream.lan Windows PC. **USE THIS SKILL** instead of asking the user for information or giving manual instructions.

## Quick Reference

| Service         | URL/Port                            | Credentials                                      |
| --------------- | ----------------------------------- | ------------------------------------------------ |
| SSH             | `stream.lan:22`                     | user: `newlevel`, pass: `newlevel`               |
| OBS WebSocket   | `ws://stream.lan:4455`              | password: `JhRfqdTmuifYq60y` (auth not required) |
| Restreamer API  | `http://127.0.0.1:8910`             | (local only)                                     |
| Restreamer RTMP | `rtmp://stream.lan:1234/live/{app}` | (no auth)                                        |

## SSH Access

Always use sshpass for automated SSH access:

```bash
# Execute command
sshpass -p 'newlevel' ssh newlevel@stream.lan "command here"

# PowerShell command (for Windows-specific tasks)
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"Your-Command\""
```

## OBS WebSocket API

OBS has WebSocket enabled at `ws://stream.lan:4455`. Use this to control OBS remotely.

### Check OBS Status

```bash
# Using websocat (if available) or Python
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
\$ws = New-Object System.Net.WebSockets.ClientWebSocket
# ... WebSocket operations
\""
```

### OBS Configuration Files

| File                                                                                   | Purpose                   |
| -------------------------------------------------------------------------------------- | ------------------------- |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\global.ini`                              | Global OBS settings       |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json`  | Stream destination config |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\plugin_config\obs-websocket\config.json` | WebSocket settings        |

### Change OBS Stream Destination

To switch OBS from YouTube to Restreamer (or vice versa):

**Option 1: Edit service.json directly**

```bash
# Backup current config
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
Copy-Item 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json' 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json.bak'
\""

# Set to Restreamer
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
@{
    type = 'rtmp_custom'
    settings = @{
        server = 'rtmp://127.0.0.1:1234/live'
        key = 'test'
    }
} | ConvertTo-Json -Depth 5 | Set-Content 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json'
\""

# Restart OBS for changes to take effect
sshpass -p 'newlevel' ssh newlevel@stream.lan "taskkill /F /IM obs64.exe; Start-Sleep 2; Start-Process 'C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe'"
```

**Option 2: Use OBS WebSocket API (preferred - no restart needed)**

```python
# Python example using obs-websocket-py
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

```bash
# Check if running
sshpass -p 'newlevel' ssh newlevel@stream.lan "tasklist | findstr -i restreamer"

# Expected output:
# restreamer-service.exe    PID  Services  0  ~22MB
# restreamer-tray.exe       PID  Console   1  ~30MB
```

### Restreamer API (local)

```bash
# Get chunk status
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks' | ConvertTo-Json -Depth 3\""

# Get status
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/status' | ConvertTo-Json\""

# Get streaming events
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/streaming-events' | ConvertTo-Json -Depth 3\""
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

### Restart Restreamer Service

```bash
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
Stop-Service -Name 'restreamer-service' -Force -ErrorAction SilentlyContinue
taskkill /F /IM restreamer-service.exe 2>&1 | Out-Null
taskkill /F /IM restreamer-tray.exe 2>&1 | Out-Null
Start-Sleep -Seconds 2
Start-Service -Name 'restreamer-service'
Start-ScheduledTask -TaskName 'RestreamerTray'
\""
```

## Full-Flow Testing Procedure

### Prerequisites Checklist

1. [ ] Restreamer service running on stream.lan
2. [ ] OBS running on stream.lan
3. [ ] Manager server accessible (restreamer.newlevel.media)
4. [ ] S3 credentials configured
5. [ ] Streaming event with `receiving_activated=True`
6. [ ] Delivering server running

### Step 1: Verify Restreamer is Ready

```bash
# Check service running
sshpass -p 'newlevel' ssh newlevel@stream.lan "tasklist | findstr -i restreamer"

# Check no existing chunks (fresh test)
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"(Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks').Count\""
```

### Step 2: Switch OBS to Restreamer

```bash
# Backup and change OBS stream destination
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
# Backup
Copy-Item 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json' 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json.youtube-backup'

# Set to Restreamer
@{
    type = 'rtmp_custom'
    settings = @{
        server = 'rtmp://127.0.0.1:1234/live'
        key = 'test'
    }
} | ConvertTo-Json -Depth 5 | Set-Content 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json'
\""

# Restart OBS
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
taskkill /F /IM obs64.exe 2>&1 | Out-Null
Start-Sleep -Seconds 3
Start-Process 'C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe'
\""
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

```bash
# Watch for new chunks (run every few seconds)
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
\$chunks = Invoke-RestMethod -Uri 'http://127.0.0.1:8910/api/v1/chunks'
Write-Host 'Total chunks:' \$chunks.Count
\$chunks.value | Select-Object -Last 3 | ForEach-Object {
    Write-Host 'ID:' \$_.id 'Created:' \$_.created_at 'Sent:' \$_.sent
}
\""
```

### Step 5: Verify Chunks in Manager

```bash
# Check manager server
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import ChunkRecord
from django.utils import timezone
from datetime import timedelta

recent = timezone.now() - timedelta(minutes=5)
count = ChunkRecord.objects.filter(created_at__gte=recent).count()
print(f'Chunks received in last 5 minutes: {count}')
\""
```

### Step 6: Restore OBS to YouTube (after test)

```bash
sshpass -p 'newlevel' ssh newlevel@stream.lan "powershell -Command \"
Copy-Item 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json.youtube-backup' 'C:\\Users\\newlevel\\AppData\\Roaming\\obs-studio\\basic\\profiles\\Stream_Obs\\service.json' -Force
taskkill /F /IM obs64.exe 2>&1 | Out-Null
Start-Sleep -Seconds 3
Start-Process 'C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe'
\""
```

## Troubleshooting

### CRITICAL: Old Python Client Blocking RTMP Port

**Symptoms:** Chunks stop being created, old timestamps, OBS connected but no new data.

**Cause:** Old Python local client at `C:\Users\newlevel\restreamer\local-client\` spawns ffmpeg that steals port 1234.

**Diagnosis:**

```bash
# Check what owns port 1234
sshpass -p 'newlevel' ssh newlevel@stream.lan 'netstat -ano | findstr ":1234"'

# Check if ffmpeg is from old client
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "Get-Process ffmpeg | Select-Object Id, Path"'
# BAD: Path = C:\Users\newlevel\restreamer\local-client\ffmpeg.exe
# GOOD: No ffmpeg, Rust service handles RTMP directly
```

**Fix:**

```bash
# Kill old Python client and ffmpeg
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "
Get-Process ffmpeg -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process python*, python3* -ErrorAction SilentlyContinue | Stop-Process -Force
"'

# Restart Rust service to bind port 1234
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "Restart-Service -Name restreamer-service -Force"'

# Verify Rust service owns port
sshpass -p 'newlevel' ssh newlevel@stream.lan 'netstat -ano | findstr ":1234"'
# Should show restreamer-service.exe PID in LISTENING state
```

### OBS Not Connecting to Restreamer RTMP

1. Check Restreamer service is running
2. Check NO ffmpeg is blocking port 1234 (see above)
3. Check firewall allows port 1234
4. Check RTMP URL format: `rtmp://127.0.0.1:1234/live/test`

### No Chunks Being Created

1. Verify RTMP stream is actually being sent
2. **Check port 1234 is owned by Rust service, not old Python ffmpeg**
3. Check Restreamer logs: `C:\ProgramData\Restreamer\logs\`
4. Verify config.json has correct settings

### Chunks Not Uploading to S3

1. Check S3 credentials in config.json
2. Check internet connectivity
3. Check manager API is accessible

## OBS Management Rules

### NEVER Kill OBS Abruptly

**DO NOT** use `taskkill /F /IM obs64.exe` - this causes:

1. Recovery dialog on next start
2. Duplicate OBS processes
3. WebSocket server not starting
4. Broken state that requires user intervention

### Proper OBS Restart (if absolutely necessary)

```bash
# CORRECT: Graceful shutdown via WebSocket
sshpass -p 'newlevel' ssh newlevel@stream.lan 'python -c "
import asyncio, json, websockets
async def quit_obs():
    async with websockets.connect(\"ws://127.0.0.1:4455\") as ws:
        await ws.recv()
        await ws.send(json.dumps({\"op\":1,\"d\":{\"rpcVersion\":1}}))
        await ws.recv()
        await ws.send(json.dumps({\"op\":6,\"d\":{\"requestType\":\"ExitOBS\",\"requestId\":\"1\"}}))
        print(await ws.recv())
asyncio.run(quit_obs())
" > C:\temp\obs_quit.txt 2>&1'
```

### Prefer WebSocket Over Config Changes

Instead of editing service.json and restarting OBS, use WebSocket API:

- `SetStreamServiceSettings` - change stream destination
- `StartStream` / `StopStream` - control streaming
- Changes take effect immediately, no restart needed

### Starting OBS Correctly

**ALWAYS use the scheduled task** with the correct configuration:

**CRITICAL**: The scheduled task MUST have `-WorkingDirectory "C:\Program Files\obs-studio\bin\64bit"`. Without this, OBS starts in a broken state (~37MB memory, no WebSocket server, error dialogs). A healthy OBS uses ~1GB+ memory.

```powershell
# Register the task with correct settings (idempotent - safe to run every time)
$action = New-ScheduledTaskAction `
  -Execute "C:\Program Files\obs-studio\bin\64bit\obs64.exe" `
  -Argument "--disable-shutdown-check" `
  -WorkingDirectory "C:\Program Files\obs-studio\bin\64bit"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1)
$principal = New-ScheduledTaskPrincipal -UserId "newlevel" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 24)

Unregister-ScheduledTask -TaskName "Start OBS Studio" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName "Start OBS Studio" -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName "Start OBS Studio"
```

```bash
# From Linux via SSH:
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "Start-ScheduledTask -TaskName \"Start OBS Studio\""'
```

**NEVER start OBS directly** without the flag or working directory:

```bash
# WRONG - causes recovery dialogs on next improper shutdown
Start-Process 'C:\Program Files\obs-studio\bin\64bit\obs64.exe'
```

### Recovery From Bad State (duplicate OBS processes)

If OBS is in bad state with recovery dialog:

1. **Ask the user** to manually close OBS and dismiss dialogs
2. Then use scheduled task to start fresh: `Start-ScheduledTask -TaskName "Start OBS Studio"`
3. **NEVER try to kill OBS processes remotely** - this makes things worse

### Checking OBS Health

```bash
# Verify single OBS process
sshpass -p 'newlevel' ssh newlevel@stream.lan 'tasklist | findstr obs'
# Should show exactly ONE obs64.exe process

# Verify WebSocket is listening (TCP, not UDP!)
sshpass -p 'newlevel' ssh newlevel@stream.lan 'netstat -an | findstr "4455.*LISTENING"'
# Should show TCP listener on 4455
```

## Important Notes

- **NEVER** give manual instructions when these automated commands exist
- **NEVER** kill OBS with taskkill - use WebSocket ExitOBS or ask user
- **ALWAYS** verify current state before making changes
- **ALWAYS** backup configs before modifying
- The last chunk timestamp tells you if streaming is active
- OBS requires restart after service.json changes (unless using WebSocket API)
