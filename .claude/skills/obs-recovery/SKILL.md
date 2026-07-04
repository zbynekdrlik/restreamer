---
name: obs-recovery
description: >
  Autonomous OBS and CI runner recovery procedures for stream.lan (streamsnv).
  Load when OBS is in degraded state, CI E2E fails on OBS state, or runner
  appears offline. Covers graceful restart, machine reboot authorization,
  and OBS health verification.
triggers:
  - OBS degraded
  - OBS not starting
  - OBS recovery
  - CI runner offline
  - runner stalled
  - obs64
  - WebSocket not listening
  - streamsnv runner
---

# OBS and CI Runner Recovery

## Autonomous Recovery Authorization

When CI fails on OBS state on the streamsnv runner and operator is **away / not in a live event**, autonomously recover without pausing to ask. Operator explicitly authorized this autonomy for non-live-event windows (2026-05-16).

**First: verify NOT in a live event** — check audit log for recent EventStarted / no `delivering_activated` event within the past hour.

**Banned phrasings during autonomous sessions:**
- "OBS is in degraded state, please restart"
- "Approve graceful WM_CLOSE?"
- "Want me to reboot the runner?"
- Any AskUserQuestion that asks permission to recover a runner

## OBS Restart Procedure

### Step 1 — Graceful shutdown

```powershell
# Via MCP shell — always WM_CLOSE first, NEVER /F unless graceful fails
mcp__win-stream-snv__Shell command="taskkill /IM obs64.exe /T"
```

Wait 15-30 seconds and verify gone:
```powershell
mcp__win-stream-snv__ListProcesses filter="obs64"
```

If still running after 30s: `/F` is acceptable during operator-away autonomous sessions.

### Step 2 — Relaunch via existing scheduled task

```powershell
# Use the EXISTING scheduled task — do NOT re-register it
mcp__win-stream-snv__Shell command="Start-ScheduledTask -TaskName 'OBS Studio'"
```

**Do NOT re-register the OBS scheduled task** — it already works (do not guess parameters and rewrite it).

### Step 3 — Wait and verify health

Wait 60-90 seconds for OBS to load scene. A healthy OBS uses ~1 GB+ memory.

```powershell
# Check memory usage — must be > 500 MB (healthy), not ~37 MB (broken)
mcp__win-stream-snv__Shell command="Get-Process obs64 | Format-List Id, WorkingSet64"

# Verify single OBS process
mcp__win-stream-snv__ListProcesses filter="obs64"

# Verify WebSocket is listening
mcp__win-stream-snv__PortCheck host="127.0.0.1" port=4455

# Visual confirmation
mcp__win-stream-snv__Snapshot
```

### Step 4 — Machine reboot (last resort)

If OBS or Restreamer cannot be recovered by relaunch:

```powershell
mcp__win-stream-snv__Shell command="Restart-Computer -Force"
```

After reboot: wait ~2-3 minutes, then verify all services recovered, then rerun the failed CI job.

## Critical OBS Rules

### NEVER use `/F` (force kill) as the first move

Force-killing OBS leaves a crash-recovery dialog on next launch. The `--disable-shutdown-check` argument does NOT prevent this dialog — it still hangs OBS at ~18 MB working set.

**Exception**: `/F` is acceptable during operator-away autonomous sessions IF graceful `taskkill /IM obs64.exe /T` fails within 30 seconds.

### NEVER try to kill OBS processes to fix duplicate/bad state

If OBS is in a bad state with recovery dialogs, ask the operator to manually close OBS and dismiss dialogs, then start fresh. Killing OBS remotely when it's showing recovery dialogs makes things worse.

### Starting OBS Correctly (when needed)

OBS MUST be started with the correct working directory (CRITICAL for proper initialization):

```powershell
mcp__win-stream-snv__Shell command="Start-Process 'C:\Program Files\obs-studio\bin\64bit\obs64.exe' -ArgumentList '--disable-shutdown-check'" cwd="C:\Program Files\obs-studio\bin\64bit"
```

Without the correct working directory, OBS starts in a broken state (~37 MB memory, no WebSocket server, error dialogs).

### OBS Config Locations

| File | Purpose |
|---|---|
| `C:\Users\newlevel\AppData\Roaming\obs-studio\global.ini` | Global OBS settings |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json` | Stream destination config |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\streamEncoder.json` | Encoder/bitrate config |
| `C:\Users\newlevel\AppData\Roaming\obs-studio\plugin_config\obs-websocket\config.json` | WebSocket settings |

**OBS uses Advanced mode, Stream_Obs profile. Bitrate is in `streamEncoder.json`, NOT `basic.ini`.**

## Runner Offline Detection

When a CI deploy/E2E job stays "queued" with `startedAt` set, the runner is likely offline. Alert user within first poll (within 5 minutes of detecting the queue condition) — do NOT wait hours.

## When Operator Must Be Involved

If OBS is in a bad state with visible recovery dialogs AND it cannot be cleared remotely:
- Ask operator to manually close OBS and dismiss dialogs
- Then start fresh using the MCP shell command above
- This is the ONLY case where operator assistance is needed for OBS state
