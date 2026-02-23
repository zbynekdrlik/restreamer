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

OBS requires starting from its bin directory for plugins to load correctly.

```bash
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "
$action = New-ScheduledTaskAction -Execute \"C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe\" -WorkingDirectory \"C:\\Program Files\\obs-studio\\bin\\64bit\"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1)
$principal = New-ScheduledTaskPrincipal -UserId \"newlevel\" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 24)

Unregister-ScheduledTask -TaskName \"StartOBSCorrect\" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName \"StartOBSCorrect\" -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName \"StartOBSCorrect\"
Start-Sleep -Seconds 5
tasklist /FI \"IMAGENAME eq obs64.exe\"
"'
```

### OBS with Auto-Start Streaming

```bash
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "
$action = New-ScheduledTaskAction -Execute \"C:\\Program Files\\obs-studio\\bin\\64bit\\obs64.exe\" -Argument \"--startstreaming\" -WorkingDirectory \"C:\\Program Files\\obs-studio\\bin\\64bit\"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(1)
$principal = New-ScheduledTaskPrincipal -UserId \"newlevel\" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 24)

Unregister-ScheduledTask -TaskName \"StartOBSStreaming\" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName \"StartOBSStreaming\" -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName \"StartOBSStreaming\"
"'
```

### Use Existing OBS Task (if available)

stream.lan has pre-configured tasks. Check and use them:

```bash
# List existing OBS tasks
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "Get-ScheduledTask | Where-Object { $_.TaskName -like \"*OBS*\" } | Select-Object TaskName, State"'

# Start existing task (preferred - already configured correctly)
sshpass -p 'newlevel' ssh newlevel@stream.lan 'powershell -Command "Start-ScheduledTask -TaskName \"Start OBS\""'
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
