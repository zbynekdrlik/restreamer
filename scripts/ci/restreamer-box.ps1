# restreamer-box.ps1 -- #417: Restreamer's process on the Linux stream.lan box.
#
# stream.lan (stream-snv) runs Ubuntu since 22.10.2026. Restreamer is the .deb
# plus the `restreamer` systemd --user unit of the kiosk user (newlevel); the
# CI runner is a system service running as that same user, with
# XDG_RUNTIME_DIR + DBUS_SESSION_BUS_ADDRESS in its .env so `systemctl --user`
# reaches the kiosk user's manager. This file replaces the Windows calls the
# E2E jobs used: schtasks /run RestreamerGUI, taskkill, Get-Process
# SessionId, NtSuspendProcess / NtResumeProcess.
#
# Dot-source it (". scripts/ci/restreamer-box.ps1"). Every function throws on
# failure.

$script:RestreamerUnit = "restreamer"

function Get-RestreamerPid {
  # MainPID of the user unit; 0 when it is not running.
  $value = (systemctl --user show -p MainPID --value $script:RestreamerUnit)
  if ($LASTEXITCODE -ne 0) { throw "systemctl --user show $script:RestreamerUnit failed ($LASTEXITCODE)" }
  return [int]$value
}

function Invoke-RestreamerUnit([string]$Verb) {
  systemctl --user $Verb $script:RestreamerUnit
  if ($LASTEXITCODE -ne 0) { throw "systemctl --user $Verb $script:RestreamerUnit failed ($LASTEXITCODE)" }
}

function Wait-RestreamerPid([int]$TimeoutSeconds = 20) {
  # The PID once the unit runs, or throw.
  for ($i = 0; $i -lt $TimeoutSeconds; $i++) {
    $restreamerPid = Get-RestreamerPid
    if ($restreamerPid -gt 0) { return $restreamerPid }
    Start-Sleep -Seconds 1
  }
  journalctl --user -u $script:RestreamerUnit -n 30 --no-pager
  throw "Restreamer did not start within ${TimeoutSeconds}s"
}

function Assert-RestreamerDesktop([int]$RestreamerPid) {
  # The operator window needs the kiosk user's display (the Windows build's
  # "not Session 0" check).
  $owner = (ps -o user= -p $RestreamerPid).Trim()
  if ($owner -ne "newlevel") { throw "Restreamer (PID $RestreamerPid) runs as '$owner', not newlevel" }
  $vars = [System.IO.File]::ReadAllText("/proc/$RestreamerPid/environ") -split "`0"
  if (-not ($vars | Where-Object { $_ -like "DISPLAY=*" -or $_ -like "WAYLAND_DISPLAY=*" })) {
    throw "Restreamer (PID $RestreamerPid) has no DISPLAY: the operator window cannot show"
  }
}

function Start-Restreamer {
  # Start it (a no-op while it runs) and return its PID.
  Invoke-RestreamerUnit "start"
  $restreamerPid = Wait-RestreamerPid
  Assert-RestreamerDesktop $restreamerPid
  return $restreamerPid
}

function Restart-Restreamer {
  # A clean restart (loads a changed config.json) and the new PID.
  Invoke-RestreamerUnit "restart"
  $restreamerPid = Wait-RestreamerPid
  Assert-RestreamerDesktop $restreamerPid
  return $restreamerPid
}

function Stop-RestreamerHard {
  # A crash: SIGKILL the whole unit (its ffmpeg children too), no clean
  # shutdown -- what taskkill /F did. The unit has Restart=always
  # (RestartSec=5), so a `stop` follows at once: it cancels the pending
  # restart and Restreamer STAYS DOWN until Start-Restreamer, as the crash
  # gates need (the VPS must exhaust its chunks). Waits until no process is
  # left and returns the PID that was killed.
  $killed = Get-RestreamerPid
  if ($killed -le 0) { throw "Restreamer is not running: nothing to crash" }
  systemctl --user kill -s KILL $script:RestreamerUnit
  if ($LASTEXITCODE -ne 0) { throw "systemctl --user kill -s KILL $script:RestreamerUnit failed ($LASTEXITCODE)" }
  Invoke-RestreamerUnit "stop"
  for ($i = 0; $i -lt 20; $i++) {
    if (-not (Test-Path "/proc/$killed") -and (Get-RestreamerPid) -eq 0) { return $killed }
    Start-Sleep -Milliseconds 500
  }
  throw "Restreamer still running 10 s after SIGKILL + stop"
}

function Suspend-Pid([int]$TargetPid) {
  # Freeze a process (what NtSuspendProcess did). Its threads stop at once;
  # sockets stay open and the kernel keeps buffering, exactly the stall the
  # #367 late-join gate needs.
  kill -STOP $TargetPid
  if ($LASTEXITCODE -ne 0) { throw "kill -STOP $TargetPid failed ($LASTEXITCODE)" }
}

function Resume-Pid([int]$TargetPid) {
  kill -CONT $TargetPid
  if ($LASTEXITCODE -ne 0) { throw "kill -CONT $TargetPid failed ($LASTEXITCODE)" }
}

function Test-PidStopped([int]$TargetPid) {
  # True while the process is frozen (state T in /proc/<pid>/stat).
  $stat = [System.IO.File]::ReadAllText("/proc/$TargetPid/stat")
  $state = ($stat.Substring($stat.LastIndexOf(")") + 2)).Split(" ")[0]
  return $state -eq "T"
}
