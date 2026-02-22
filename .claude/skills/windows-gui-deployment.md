---
name: windows-gui-deployment
description: Deploy Windows GUI apps to run in user desktop sessions from CI/service context using Task Scheduler
---

# Windows GUI App Deployment via Task Scheduler

This skill documents the **MANDATORY PATTERN** for starting Windows GUI applications from CI runners or services that run as SYSTEM. Windows session isolation prevents SYSTEM processes from directly launching GUI apps in user sessions.

## The Problem

- GitHub Actions self-hosted runners run as SYSTEM (session 0)
- Windows services run in session 0 (non-interactive)
- GUI apps require user desktop session (session 1+)
- Direct process starts from session 0 are invisible to the logged-in user

## The Solution: Task Scheduler with Interactive Logon

Use Windows Task Scheduler with `LogonType Interactive` to launch GUI apps in the user's desktop session.

## Implementation

### PowerShell Script (for CI deployment)

```powershell
# Configure the executable path
$ExePath = "C:\Program Files\Restreamer\restreamer-tray.exe"
$TaskName = "RestreamerTray"
$Username = "newlevel"  # The desktop session user

# Kill existing processes
taskkill /F /IM "restreamer-tray.exe" 2>&1 | Out-Null
Start-Sleep -Seconds 1

# Create scheduled task configuration
$action = New-ScheduledTaskAction -Execute $ExePath
$trigger = New-ScheduledTaskTrigger -AtLogon -User $Username
$principal = New-ScheduledTaskPrincipal -UserId $Username -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0

# Remove existing task (suppress errors)
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# Register new task
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings

# Start the task immediately (launches app in user session)
Start-ScheduledTask -TaskName $TaskName
```

### Critical Settings Explained

| Setting              | Value             | Purpose                                    |
| -------------------- | ----------------- | ------------------------------------------ |
| `LogonType`          | `Interactive`     | Runs in desktop session, not hidden        |
| `UserId`             | `newlevel`        | The logged-in user who sees the tray icon  |
| `RunLevel`           | `Limited`         | Standard user permissions (not elevated)   |
| `AtLogon`            | `-User $Username` | Auto-start when user logs in               |
| `ExecutionTimeLimit` | `0`               | Never timeout (tray apps run indefinitely) |

### DO NOT USE

- `schtasks /Run` - Does not properly trigger interactive tasks
- `Start-Process` from SYSTEM - App runs hidden in session 0
- VBS scripts with `WScript.Shell` - Unreliable, same session issue
- `psexec -i` - Requires additional tools, permission issues

### MUST USE

- `Start-ScheduledTask -TaskName "..."` PowerShell cmdlet
- `LogonType Interactive` in task principal
- Task registered with specific user (not SYSTEM)

## Verification

After running the task, verify:

```powershell
# Check task status
Get-ScheduledTaskInfo -TaskName "RestreamerTray" | Select-Object LastTaskResult, LastRunTime

# Check if process is running in correct session
tasklist /FI "IMAGENAME eq restreamer-tray.exe" /FO LIST
# Should show "Session#:  1" or higher (NOT session 0)
```

## CI Integration Example

In GitHub Actions workflow:

```yaml
- name: Launch tray app in user session
  shell: powershell
  run: |
    $TaskName = "RestreamerTray"
    $ExePath = "C:\Program Files\Restreamer\restreamer-tray.exe"
    $Username = "newlevel"

    # Kill existing
    taskkill /F /IM "restreamer-tray.exe" 2>&1 | Out-Null
    Start-Sleep -Seconds 1

    # Configure task
    $action = New-ScheduledTaskAction -Execute $ExePath
    $trigger = New-ScheduledTaskTrigger -AtLogon -User $Username
    $principal = New-ScheduledTaskPrincipal -UserId $Username -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0

    # Register and run
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings
    Start-ScheduledTask -TaskName $TaskName

    Start-Sleep -Seconds 2
    Write-Host "Task started - tray should appear in user session"
```

## Troubleshooting

1. **Tray not visible**: Verify user is logged in with active desktop session
2. **Task fails with error 2147942402**: Executable path not found - check path exists
3. **Task runs but no icon**: Check task is configured with Interactive logon type
4. **Permission denied**: Task must be registered, not just triggered - use full Register-ScheduledTask
