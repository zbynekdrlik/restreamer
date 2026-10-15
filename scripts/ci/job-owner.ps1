# job-owner.ps1 -- #349: a detached CI helper ends with the job that started it.
#
# The rig-lease heartbeat (rig-lease-hold.ps1 -Action Beat) and the program-audio
# watchdog (program-audio-guard.ps1 -RunWatchdog) are detached PowerShell processes that
# outlive their step. The runner's job-end cleanup normally kills them, but a
# force-cancel (gh api .../force-cancel) kills Runner.Worker WITHOUT that cleanup: run
# 38745691029's heartbeat beat the dev1 rig lease on until it was killed by hand, and an
# orphaned watchdog would stop any later OBS stream (the church's) as "ours".
#
# Dot-source it, then:
#
#   Write-JobOwnerRecord <path>  in the step that starts the helper: finds the job's
#                                Runner.Worker (walking up the parent chain) and records
#                                its pid + start ticks in <path>. Returns a one-line
#                                description for the step log.
#   Read-JobOwnerRecord <path>   in the helper, ONCE at its start (a later cleanup of
#                                RUNNER_TEMP must not hide its job): the record, or
#                                $null when none was written.
#   Test-JobOwnerGone $record    on every round of the helper's loop: $true once that
#                                process is gone or its pid was reused. $false without
#                                a record (nothing to compare: the helper runs as before).
#
# CI_JOB_OWNER_PID (tests only; no workflow may set it -- verify_program_audio_guard.py)
# names the owner process instead of the Runner.Worker walk.

function Find-JobOwnerProcess {
  $override = $env:CI_JOB_OWNER_PID -as [int]
  if ($override -gt 0) { return Get-Process -Id $override -ErrorAction SilentlyContinue }
  if ($PSVersionTable.PSEdition -ne "Desktop" -and -not $IsWindows) { return $null }
  $id = $PID
  for ($i = 0; $i -lt 12 -and $id -gt 0; $i++) {
    try {
      $row = Get-CimInstance -ClassName Win32_Process -Filter "ProcessId = $id" -ErrorAction Stop
    } catch {
      return $null    # a CIM failure must not fail the step that takes the lease
    }
    if ($null -eq $row) { return $null }
    if ($row.Name -eq "Runner.Worker.exe") { return Get-Process -Id $id -ErrorAction SilentlyContinue }
    $id = [int]$row.ParentProcessId
  }
  return $null
}

function Write-JobOwnerRecord([string]$path) {
  $owner = Find-JobOwnerProcess
  if ($null -eq $owner) {
    Remove-Item -LiteralPath $path -ErrorAction SilentlyContinue
    $why = "no Runner.Worker found above pid $PID -- the helper cannot tell when its job ends (a force-cancel would orphan it, #349)"
    if ($env:GITHUB_ACTIONS -eq "true") { Write-Host "::warning::[job-owner] $why" }
    return $why
  }
  [System.IO.File]::WriteAllText($path, "$($owner.Id) $($owner.StartTime.ToUniversalTime().Ticks)")
  return "bound to its job's $($owner.ProcessName) (pid $($owner.Id))"
}

function Read-JobOwnerRecord([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) { return $null }
  $parts = ([System.IO.File]::ReadAllText($path)).Trim() -split "\s+"
  # "" -as [int] / [long] is 0, and pid 0 exists on Windows (Idle): require real values.
  $ownerId = $parts[0] -as [int]
  $ticks = $null
  if ($parts.Count -gt 1) { $ticks = $parts[1] -as [long] }
  if ($null -eq $ownerId -or $ownerId -le 0 -or $null -eq $ticks -or $ticks -le 0) { return $null }
  return @{ Id = $ownerId; Ticks = $ticks }
}

function Test-JobOwnerGone($record) {
  if ($null -eq $record) { return $false }
  $proc = Get-Process -Id $record.Id -ErrorAction SilentlyContinue
  if ($null -eq $proc) { return $true }
  try {
    $ticks = $proc.StartTime.ToUniversalTime().Ticks
  } catch {
    return $false    # alive, start time unreadable: cannot prove a reused pid
  }
  # Same pid AND start time (2 s tolerance: two reads of it can differ slightly).
  return ([Math]::Abs($ticks - $record.Ticks) -gt 20000000)
}
