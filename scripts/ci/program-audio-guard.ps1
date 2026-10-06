# program-audio-guard.ps1 -- #379: never stream room/FOH music to YouTube/Facebook.
#
# Owner copyright rule: the stream OBS program's only audio input is the FOH Dante
# feed, which carries whatever plays in the room. A CI job that streams it to a
# platform must stream only the measurement signal (or silence). camera-box samples
# the program audio and classifies it; restreamer reads that verdict over HTTP
# (http://dev1:8890/program-audio.json) and never reads OBS meters itself (owner
# directive 2026-08-30: CI only starts and stops OBS streaming).
#
# Dot-source it (". scripts/ci/program-audio-guard.ps1"), then:
#
#   Test-ProgramAudio            one read of the verdict. Returns $null when the
#                                program carries MEASUREMENT or SILENT and the sample
#                                is fresh (age_s <= 10), otherwise a one-line reason:
#                                FOREIGN / UNKNOWN / stale / unreachable / malformed.
#                                FAIL-CLOSED: no verdict = no stream. obs-stream.ps1
#                                Start-OurStream runs it right before every StartStream
#                                (the initial start and every republish).
#   Start-ProgramAudioWatchdog   right after the OBS start step: launches a detached
#                                PowerShell (it must outlive the step; a Start-Job dies
#                                with the step's process) that re-reads the verdict
#                                every 10 s. On a breach it writes the breach marker
#                                FIRST, then asks Restreamer to stop OBS streaming
#                                (POST /api/v1/obs/stop-stream, retried while Restreamer
#                                restarts) and exits.
#   Assert-NoProgramAudioBreach  fails the step (::error:: + job summary) when the
#                                breach marker exists, or when the watchdog died or hung
#                                (an unguarded stream is a failure too).
#   Stop-ProgramAudioWatchdog    teardown: stops the watchdog. A watchdog that is
#                                already gone without a breach is recorded as one.
#
# All state is job-scoped in $env:RUNNER_TEMP (the same place obs-stream.ps1 keeps
# its records), so the steps of one job share it and the next job starts clean.
# The runner reaps the detached watchdog at job end even if the teardown never ran.
#
# Knobs (env, for tests; ci.yml may not set them -- verify_program_audio_guard.py):
#   PROGRAM_AUDIO_URL               default http://dev1:8890/program-audio.json
#   PROGRAM_AUDIO_MAX_AGE_S         default 10   (freshness of camera-box's sample)
#   PROGRAM_AUDIO_FOREIGN_WINDOW_S  default 15   (a FOREIGN seen between two polls)
#   PROGRAM_AUDIO_HTTP_TIMEOUT_S    default 5
#   PROGRAM_AUDIO_POLL_S            default 10
#   PROGRAM_AUDIO_STOP_URL          default Restreamer's stop-stream API on 127.0.0.1:8910
#   PROGRAM_AUDIO_STOP_BUDGET_S     default 60   (retry the stop while Restreamer restarts)
#
# The stop-stream call lives ONLY in Invoke-ProgramAudioStop: the #374 guard
# (verify_no_obs_mutation.py) bans that API everywhere else.
# Tests: tests/ci/test_program_audio_guard.py (mock sampler + mock stop endpoint).

param([switch]$RunWatchdog)

$script:ProgramAudioGuardScript = $PSCommandPath
$script:ProgramAudioOkVerdicts = @("MEASUREMENT", "SILENT")

function Get-ProgramAudioKnob([string]$name, [int]$default) {
  # -as [int] yields $null (never throws) on a blank or non-numeric override; a $null
  # or 0 -TimeoutSec is INDEFINITE in PS 5.1, so never let one through.
  $v = [Environment]::GetEnvironmentVariable($name) -as [int]
  if ($null -eq $v -or $v -le 0) { return $default }
  return $v
}

function Get-ProgramAudioUrl {
  if ($env:PROGRAM_AUDIO_URL) { return $env:PROGRAM_AUDIO_URL }
  return "http://dev1:8890/program-audio.json"
}

function Get-ProgramAudioPaths {
  if (-not $env:RUNNER_TEMP) { throw "RUNNER_TEMP is not set; the program-audio guard keeps its state there" }
  return @{
    Breach    = Join-Path $env:RUNNER_TEMP "program-audio-breach.txt"
    Pid       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.pid"
    Heartbeat = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.heartbeat"
    Log       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.log"
    Out       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.out"
    Err       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.err"
  }
}

function Get-ProgramAudioNow { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }

function Get-ProgramAudioStamp { return [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") }

# One read of camera-box's verdict. $null = OK to stream; otherwise the reason.
function Test-ProgramAudio {
  $url = Get-ProgramAudioUrl
  $maxAge = Get-ProgramAudioKnob "PROGRAM_AUDIO_MAX_AGE_S" 10
  $foreignWindow = Get-ProgramAudioKnob "PROGRAM_AUDIO_FOREIGN_WINDOW_S" 15
  $timeout = Get-ProgramAudioKnob "PROGRAM_AUDIO_HTTP_TIMEOUT_S" 5
  try {
    $resp = Invoke-WebRequest -Uri $url -Method GET -UseBasicParsing -TimeoutSec $timeout -Headers @{ "Cache-Control" = "no-cache" }
  } catch {
    return "unreachable: $url ($($_.Exception.Message))"
  }
  $text = ([string]$resp.Content).Trim()
  # Only a JSON object: ConvertFrom-Json would unwrap a one-element array.
  if (-not $text.StartsWith("{")) { return "malformed: $url did not return a JSON object" }
  try {
    $j = $text | ConvertFrom-Json
  } catch {
    return "malformed: $url did not return JSON"
  }
  if ($null -eq $j -or $j.schema -ne 1) {
    return "malformed: $url returned no schema-1 object"
  }
  $verdict = [string]$j.verdict
  $age = $j.age_s -as [double]
  if (-not $verdict -or $null -eq $j.age_s -or $null -eq $age) {
    return "malformed: $url has no verdict / numeric age_s"
  }
  $line = "verdict=$verdict age_s=$age rms_dbfs=$($j.rms_dbfs) outside_band_pct=$($j.outside_band_pct) source=$($j.source)"
  if ($age -gt $maxAge) { return "stale: sample is ${age}s old (max ${maxAge}s) -- $line" }
  if ($script:ProgramAudioOkVerdicts -notcontains $verdict) { return "${verdict}: program audio is not the measurement signal -- $line" }
  $foreignAge = $j.last_foreign_age_s -as [double]
  if ($null -ne $j.last_foreign_age_s -and $null -ne $foreignAge -and $foreignAge -le $foreignWindow) {
    return "FOREIGN: foreign audio ${foreignAge}s ago (window ${foreignWindow}s) -- $line"
  }
  Write-Host "[program-audio] OK: $line"
  return $null
}

# A transport-class failure (not a content verdict) is re-read once before it counts.
function Test-ProgramAudioTransportFailure([string]$why) {
  return ($why -like "unreachable:*" -or $why -like "stale:*" -or $why -like "malformed:*")
}

# ::error:: plus the job summary, so the reason is visible without opening the log.
function Write-ProgramAudioError([string]$context, [string]$why) {
  Write-Host "::error::program-audio guard (#379): $context -- $why"
  if ($env:GITHUB_STEP_SUMMARY) {
    $md = "### Program-audio guard (#379): $context`n`n``$why```n"
    $md | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Encoding utf8 -Append
  }
}

# The ONE call to Restreamer's stop-stream API (the #374 guard confines it here).
# Retried until it answers 2xx or the budget ends: the breach can land while a
# crash gate has Restreamer.exe down.
function Invoke-ProgramAudioStop {
  $stopUrl = $env:PROGRAM_AUDIO_STOP_URL
  if (-not $stopUrl) { $stopUrl = "http://127.0.0.1:8910/api/v1/obs/stop-stream" }
  $budget = Get-ProgramAudioKnob "PROGRAM_AUDIO_STOP_BUDGET_S" 60
  $deadline = (Get-ProgramAudioNow) + $budget
  $attempt = 0
  $last = ""
  while ($true) {
    $attempt++
    try {
      $r = Invoke-WebRequest -Uri $stopUrl -Method POST -UseBasicParsing -TimeoutSec 5 -Body ""
      return "stop-stream OK (HTTP $($r.StatusCode), attempt $attempt)"
    } catch {
      $last = $_.Exception.Message
    }
    if ((Get-ProgramAudioNow) -ge $deadline) {
      return "stop-stream FAILED after $attempt attempts / ${budget}s: $last"
    }
    Start-Sleep -Seconds ([Math]::Min(5, $budget))
  }
}

function Write-ProgramAudioLog([string]$text) {
  $p = Get-ProgramAudioPaths
  "$(Get-ProgramAudioStamp) $text" | Out-File -FilePath $p.Log -Encoding ascii -Append
}

# The detached watchdog's body (program-audio-guard.ps1 -RunWatchdog).
function Invoke-ProgramAudioWatchdogLoop {
  $ErrorActionPreference = "Stop"
  $p = Get-ProgramAudioPaths
  $poll = Get-ProgramAudioKnob "PROGRAM_AUDIO_POLL_S" 10
  Write-ProgramAudioLog "watchdog up (pid $PID, poll ${poll}s, $(Get-ProgramAudioUrl))"
  while ($true) {
    "$(Get-ProgramAudioNow)" | Out-File -FilePath $p.Heartbeat -Encoding ascii
    $why = Test-ProgramAudio
    if ($why -and (Test-ProgramAudioTransportFailure $why)) {
      Write-ProgramAudioLog "re-reading once after: $why"
      Start-Sleep -Seconds 2
      $why = Test-ProgramAudio
    }
    if ($why) {
      # Marker first: whatever happens to the stop, the job fails on it.
      "$(Get-ProgramAudioStamp) BREACH: $why" | Out-File -FilePath $p.Breach -Encoding ascii
      Write-ProgramAudioLog "BREACH: $why"
      $stopped = Invoke-ProgramAudioStop
      "$(Get-ProgramAudioStamp) $stopped" | Out-File -FilePath $p.Breach -Encoding ascii -Append
      Write-ProgramAudioLog $stopped
      return
    }
    Write-ProgramAudioLog "ok"
    Start-Sleep -Seconds $poll
  }
}

# The live watchdog process recorded in the pid file, or $null (gone / pid reused).
function Get-ProgramAudioWatchdogProcess {
  $p = Get-ProgramAudioPaths
  if (-not (Test-Path -LiteralPath $p.Pid)) { return $null }
  $rec = ((Get-Content -LiteralPath $p.Pid -Raw).Trim()) -split " ", 2
  $wdId = $rec[0] -as [int]
  if ($null -eq $wdId) { return $null }
  $proc = Get-Process -Id $wdId -ErrorAction SilentlyContinue
  if ($null -eq $proc) { return $null }
  # Same pid AND start time (within 2 s: the clock reads differ slightly) = not a reused pid.
  $ticks = $null
  if ($rec.Count -gt 1) { $ticks = $rec[1] -as [long] }
  if ($null -ne $ticks -and [Math]::Abs($proc.StartTime.ToUniversalTime().Ticks - $ticks) -gt 20000000) { return $null }
  return $proc
}

function Get-ProgramAudioHeartbeatAge {
  $p = Get-ProgramAudioPaths
  if (-not (Test-Path -LiteralPath $p.Heartbeat)) { return $null }
  $beat = (Get-Content -LiteralPath $p.Heartbeat -Raw).Trim() -as [double]
  if ($null -eq $beat) { return $null }
  return [math]::Round((Get-ProgramAudioNow) - $beat, 1)
}

function Start-ProgramAudioWatchdog {
  $p = Get-ProgramAudioPaths
  if ($null -ne (Get-ProgramAudioWatchdogProcess)) {
    Write-Host "[program-audio] watchdog already running"
    return
  }
  Remove-Item -LiteralPath $p.Heartbeat -ErrorAction SilentlyContinue
  $hostExe = (Get-Process -Id $PID).Path
  $argLine = "-NoProfile -ExecutionPolicy Bypass -File `"$($script:ProgramAudioGuardScript)`" -RunWatchdog"
  if ($PSVersionTable.PSEdition -eq "Desktop") {
    # Windows PowerShell: ShellExecute, so the child inherits no handle of this step
    # (an inherited stdout pipe would keep the step open until the watchdog exits).
    $wd = Start-Process -FilePath $hostExe -ArgumentList $argLine -WindowStyle Hidden -PassThru
  } else {
    $wd = Start-Process -FilePath $hostExe -ArgumentList $argLine -RedirectStandardOutput $p.Out -RedirectStandardError $p.Err -PassThru
  }
  "$($wd.Id) $($wd.StartTime.ToUniversalTime().Ticks)" | Out-File -FilePath $p.Pid -Encoding ascii
  for ($i = 0; $i -lt 30; $i++) {
    if (Test-Path -LiteralPath $p.Heartbeat) {
      Write-Host "[program-audio] watchdog running (pid $($wd.Id), polls $(Get-ProgramAudioUrl))"
      return
    }
    if ($wd.HasExited) { break }
    Start-Sleep -Milliseconds 500
  }
  throw "program-audio watchdog did not start (pid $($wd.Id), no heartbeat in 15 s)"
}

# $null = guarded and clean; otherwise why the job must fail.
function Get-ProgramAudioGuardFailure {
  $p = Get-ProgramAudioPaths
  if (Test-Path -LiteralPath $p.Breach) {
    return ((Get-Content -LiteralPath $p.Breach) -join " | ")
  }
  if (Test-Path -LiteralPath $p.Pid) {
    $age = Get-ProgramAudioHeartbeatAge
    if ($null -eq (Get-ProgramAudioWatchdogProcess)) {
      return "watchdog died (last heartbeat ${age}s ago) -- the stream was unguarded"
    }
    $poll = Get-ProgramAudioKnob "PROGRAM_AUDIO_POLL_S" 10
    $limit = 3 * $poll + 20
    if ($null -eq $age -or $age -gt $limit) {
      return "watchdog hung (last heartbeat ${age}s ago, limit ${limit}s) -- the stream is unguarded"
    }
  }
  return $null
}

function Assert-NoProgramAudioBreach {
  $why = Get-ProgramAudioGuardFailure
  if ($why) {
    Write-ProgramAudioError "BREACH" $why
    exit 1
  }
  Write-Host "[program-audio] no breach"
}

function Stop-ProgramAudioWatchdog {
  $p = Get-ProgramAudioPaths
  if (-not (Test-Path -LiteralPath $p.Pid)) {
    Write-Host "[program-audio] no watchdog was started in this job"
    return
  }
  $wd = Get-ProgramAudioWatchdogProcess
  if ($null -ne $wd) {
    Stop-Process -Id $wd.Id -Force
    Write-Host "[program-audio] watchdog stopped (pid $($wd.Id))"
  } elseif (-not (Test-Path -LiteralPath $p.Breach)) {
    "$(Get-ProgramAudioStamp) BREACH: watchdog died before teardown (last heartbeat $(Get-ProgramAudioHeartbeatAge)s ago) -- the stream was unguarded" |
      Out-File -FilePath $p.Breach -Encoding ascii
  }
  Remove-Item -LiteralPath $p.Pid -ErrorAction SilentlyContinue
  if (Test-Path -LiteralPath $p.Log) {
    Write-Host "[program-audio] watchdog log (last 5 lines):"
    Get-Content -LiteralPath $p.Log -Tail 5 | ForEach-Object { Write-Host "  $_" }
  }
}

if ($RunWatchdog) {
  try {
    Invoke-ProgramAudioWatchdogLoop
  } catch {
    Write-ProgramAudioLog "watchdog crashed: $($_.Exception.Message)"
    exit 1
  }
  exit 0
}
