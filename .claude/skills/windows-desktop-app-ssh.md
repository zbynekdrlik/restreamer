---
name: windows-desktop-app-ssh
description: How to correctly start Windows desktop applications via SSH in the user's desktop session with proper working directory
---

# Running Windows Desktop Apps via SSH

This skill documents the **ONLY CORRECT WAY** to start Windows GUI applications remotely via SSH. Direct process starts from SSH DO NOT WORK because SSH runs in session 0 (non-interactive).

## The Problem

```
SSH Session (Session 0, SYSTEM context)
    ↓ Start-Process "app.exe"
    ↓ App starts in Session 0 (INVISIBLE to user)
    ✗ WRONG - User cannot see or interact with app
```

## The Solution: Task Scheduler with Interactive Session

```
SSH Session
    ↓ Create/Start Scheduled Task with:
    ↓   - LogonType: Interactive
    ↓   - UserId: desktop user (e.g., "newlevel")
    ↓   - WorkingDirectory: app's folder
    ↓ Task runs in user's desktop session (Session 1+)
    ✓ CORRECT - App visible and interactive
```

## Generic Template

```powershell
# Variables - customize these
$AppPath = "C:\Path\To\app.exe"
$AppArgs = ""  # Optional arguments
$WorkingDir = "C:\Path\To"  # CRITICAL: Set to app's directory
$TaskName = "StartMyApp"
$Username = "newlevel"  # Desktop session user

# Create and run task
$action = New-ScheduledTaskAction -Execute $AppPath -Argument $AppArgs -WorkingDirectory $WorkingDir
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1)
$principal = New-ScheduledTaskPrincipal -UserId $Username -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 24)

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $TaskName
```

## OBS Studio

**CRITICAL:** OBS requires starting from its `bin\64bit` directory. Without `-WorkingDirectory`, OBS starts but doesn't initialize properly (0 CPU, no log file, no window).

### Method 1: Script File (RECOMMENDED - avoids escaping issues)

```bash
# Step 1: Create script file
sshpass -p 'newlevel' ssh newlevel@stream.lan 'echo $action = New-ScheduledTaskAction -Execute "C:\Program Files\obs-studio\bin\64bit\obs64.exe" -WorkingDirectory "C:\Program Files\obs-studio\bin\64bit" > C:\temp\start_obs.ps1 && echo $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1) >> C:\temp\start_obs.ps1 && echo $principal = New-ScheduledTaskPrincipal -UserId "newlevel" -LogonType Interactive -RunLevel Limited >> C:\temp\start_obs.ps1 && echo $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries >> C:\temp\start_obs.ps1 && echo Unregister-ScheduledTask -TaskName "OBSCorrect" -Confirm:$false -ErrorAction SilentlyContinue >> C:\temp\start_obs.ps1 && echo Register-ScheduledTask -TaskName "OBSCorrect" -Action $action -Trigger $trigger -Principal $principal -Settings $settings >> C:\temp\start_obs.ps1 && echo Start-ScheduledTask -TaskName "OBSCorrect" >> C:\temp\start_obs.ps1'

# Step 2: Execute script
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -ExecutionPolicy Bypass -File C:\temp\start_obs.ps1'

# Step 3: Verify (should show ~1GB+ memory, CPU time > 0)
sleep 10 && sshpass -p 'newlevel' ssh newlevel@stream.lan 'tasklist /FI "IMAGENAME eq obs64.exe" /V'
```

### Method 2: With Auto-Start Streaming

```bash
# Create script with --startstreaming
sshpass -p 'newlevel' ssh newlevel@stream.lan 'echo $action = New-ScheduledTaskAction -Execute "C:\Program Files\obs-studio\bin\64bit\obs64.exe" -Argument "--startstreaming" -WorkingDirectory "C:\Program Files\obs-studio\bin\64bit" > C:\temp\start_obs_stream.ps1 && echo $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1) >> C:\temp\start_obs_stream.ps1 && echo $principal = New-ScheduledTaskPrincipal -UserId "newlevel" -LogonType Interactive -RunLevel Limited >> C:\temp\start_obs_stream.ps1 && echo $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries >> C:\temp\start_obs_stream.ps1 && echo Unregister-ScheduledTask -TaskName "OBSStream" -Confirm:$false -ErrorAction SilentlyContinue >> C:\temp\start_obs_stream.ps1 && echo Register-ScheduledTask -TaskName "OBSStream" -Action $action -Trigger $trigger -Principal $principal -Settings $settings >> C:\temp\start_obs_stream.ps1 && echo Start-ScheduledTask -TaskName "OBSStream" >> C:\temp\start_obs_stream.ps1'

# Execute
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -ExecutionPolicy Bypass -File C:\temp\start_obs_stream.ps1'
```

### Verify OBS Started Correctly

```bash
# CORRECT: Memory ~1GB+, CPU > 0:00:00, creates log file
sshpass -p 'newlevel' ssh newlevel@stream.lan 'tasklist /FI "IMAGENAME eq obs64.exe" /V'
# Image Name    PID  Session#  Mem Usage   CPU Time
# obs64.exe     XXX  1         1,783,644 K 0:00:12    <- GOOD

# WRONG: Memory ~37MB, CPU 0:00:00, no new log file (missing WorkingDirectory!)
# obs64.exe     XXX  1         37,500 K    0:00:00    <- BAD
```

### Set OBS Stream Destination

```bash
# Set to Restreamer (custom RTMP)
sshpass -p 'newlevel' ssh newlevel@stream.lan 'echo {"type":"rtmp_custom","settings":{"server":"rtmp://127.0.0.1:1234/live","key":"test"}}> C:\temp\service.json && copy C:\temp\service.json "C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json" /Y'

# Restore to YouTube
sshpass -p 'newlevel' ssh newlevel@stream.lan 'copy "C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json.youtube-backup" "C:\Users\newlevel\AppData\Roaming\obs-studio\basic\profiles\Stream_Obs\service.json" /Y'
```

## Restreamer Tray App

```bash
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "
$action = New-ScheduledTaskAction -Execute \"C:\\Program Files\\Restreamer\\restreamer-tray.exe\" -WorkingDirectory \"C:\\Program Files\\Restreamer\"
$trigger = New-ScheduledTaskTrigger -AtLogon -User \"newlevel\"
$principal = New-ScheduledTaskPrincipal -UserId \"newlevel\" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0

Unregister-ScheduledTask -TaskName \"RestreamerTray\" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName \"RestreamerTray\" -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName \"RestreamerTray\"
"'
```

## Critical Settings Explained

| Setting                  | Value          | Why It Matters                             |
| ------------------------ | -------------- | ------------------------------------------ |
| `-WorkingDirectory`      | App's folder   | Many apps need this to find config/plugins |
| `-LogonType Interactive` | Required       | Makes app run in desktop session           |
| `-UserId`                | "newlevel"     | Must match logged-in desktop user          |
| `-RunLevel Limited`      | Standard perms | Avoids UAC issues                          |
| `-ExecutionTimeLimit 0`  | No timeout     | For long-running apps                      |

## Verification

After starting an app, verify it's running in the correct session:

```bash
# Check process is running in Session 1 (user session, not Session 0)
sshpass -p 'newlevel' ssh newlevel@stream.lan 'tasklist /FI "IMAGENAME eq obs64.exe" /FO LIST'

# Should show:
# Session#:         1    <- CORRECT (user's desktop)
# NOT Session#:     0    <- WRONG (system session)
```

## Common Mistakes

| Wrong                     | Right                            |
| ------------------------- | -------------------------------- |
| `Start-Process "app.exe"` | Use Scheduled Task               |
| No `-WorkingDirectory`    | Always set to app's folder       |
| `schtasks /Run`           | Use `Start-ScheduledTask` cmdlet |
| `-LogonType S4U`          | Use `-LogonType Interactive`     |
| Running as SYSTEM         | Run as desktop user              |

## Troubleshooting

### App starts but crashes immediately

- Check `-WorkingDirectory` is set correctly
- App may need plugins/configs from its folder

### App doesn't appear on desktop

- Verify user is logged in: `query user`
- Check task uses `LogonType Interactive`
- Verify `UserId` matches logged-in user

### Task succeeds but app not running

- Check `Get-ScheduledTaskInfo -TaskName "X" | Select LastTaskResult`
- Result 0 = success, other = error code
- Check app logs for startup errors

### "Access Denied" errors

- Use `-RunLevel Limited` not `Highest`
- Don't run as different user than logged-in one
