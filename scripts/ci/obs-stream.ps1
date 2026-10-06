# obs-stream.ps1 -- #374: the only way restreamer CI starts or stops stream OBS streaming.
#
#   -Action Start              rig lease free + readiness (obs-ws.ps1 Test-ObsReady) in
#                              the SAME websocket session, then StartStream. Writes
#                              OBS_STREAMING_STARTED_BY_CI=true to GITHUB_ENV BEFORE the
#                              start (a cancelled step still lets the teardown stop
#                              ours) and =false when OBS refuses the start (camera-box
#                              started streaming first: never ours to stop).
#   -Action Stop               StopStream, then waits until OBS reports not streaming.
#                              ci.yml runs it only with
#                              `if: always() && env.OBS_STREAMING_STARTED_BY_CI == 'true'`.
#   -Action Republish          mid-run unpublish/republish of OUR stream (the disconnect
#   -GapSeconds N              and A/V-republish gates): Stop, marker=false, N s of dead
#                              air, then the full Start (lease + readiness + checked
#                              StartStream). If camera-box took OBS during the gap, the
#                              start is refused and the marker stays false.
#   -Action Rebaseline         read-only, right after every step that kills/restarts
#                              Restreamer.exe while we stream: OBS reconnects, which
#                              RESETS outputDuration. Waits out the reconnect, then
#                              re-records when our (reconnected) session began; an
#                              output that went inactive is gone: marker=false.
#   -Action AssertNotStreaming read-only: fails when OBS is streaming (into the inpoint
#                              it would keep rtmp_connected true) or rejects us; only an
#                              unreachable OBS is a warning (a down OBS streams nothing).
#
# Stream OBS is camera-box's development target; owner directive 2026-08-30: only
# Start/Stop streaming. Nothing here changes a scene, a setting or a recording.
# scripts/ci/verify_no_obs_mutation.py allows the StartStream request only in
# Start-OurStream and StopStream only in Stop-OurStream, and checks their structure;
# tests/ci/test_obs_stream.py runs every action against a mock obs-websocket.
# The guard also pins this file's `switch ($Action)` dispatcher and how often the
# Start-/Stop-OurStream / marker functions appear: change them together with
# DISPATCH / PINNED_COUNTS in the guard (.claude/rules/stream-obs-ci.md).
#
# Stream identity (OBS outputs carry no session id): Start records when OUR session
# began in $env:RUNNER_TEMP\obs-streaming-started-at (job-scoped, survives steps).
# outputDuration is frames since the last (re)connect, and OBS zeroes it on every
# automatic reconnect (libobs obs_output_begin_data_capture), so every restart of
# Restreamer.exe is followed by -Action Rebaseline. While OBS reconnects its output
# stays active, so no second session can start meanwhile. Before StopStream the
# active session counts as ours only if its duration covers half the time since
# that record; while waiting for idle, a duration that drops means a new session.
# A session that is not ours is never stopped: marker=false and exit 1.

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("Start", "Stop", "Republish", "Rebaseline", "AssertNotStreaming")]
  [string]$Action,
  [int]$GapSeconds = 10
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\obs-ws.ps1"

$Marker = "OBS_STREAMING_STARTED_BY_CI"

function Set-StartedMarker([string]$value) {
  if (-not $env:GITHUB_ENV) { throw "GITHUB_ENV is not set; cannot record $Marker" }
  "$Marker=$value" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
}

function Get-NowEpoch { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }

function Get-StartedAtFile {
  if (-not $env:RUNNER_TEMP) { throw "RUNNER_TEMP is not set; cannot record when our stream started" }
  return (Join-Path $env:RUNNER_TEMP "obs-streaming-started-at")
}

# Records when the active stream (ours, just started) began.
function Set-StartedAt($data) {
  $began = (Get-NowEpoch) - ([double]$data.outputDuration / 1000.0)
  "$began" | Out-File -FilePath (Get-StartedAtFile) -Encoding ascii
}

# Is the active output (GetStreamStatus data) the session we started (or last
# rebaselined)? Its outputDuration must cover half the time since that record
# (slack for frames OBS drops); a younger session began after ours: not ours.
# No record (the start step died between StartStream and the record) -> ours.
function Test-OurStream($data) {
  $file = Get-StartedAtFile
  if (-not (Test-Path -LiteralPath $file)) { return $true }
  $ours = (Get-Content -LiteralPath $file -Raw).Trim() -as [double]
  if ($null -eq $ours) { return $true }
  $elapsed = (Get-NowEpoch) - $ours
  $duration = [double]$data.outputDuration / 1000.0
  return -not ($elapsed -gt 60 -and $duration -lt 0.5 * $elapsed)
}

function Get-IdentityNote($data) {
  $file = Get-StartedAtFile
  $ours = if (Test-Path -LiteralPath $file) { (Get-Content -LiteralPath $file -Raw).Trim() -as [double] } else { $null }
  $since = if ($null -ne $ours) { "$([math]::Round((Get-NowEpoch) - $ours))s" } else { "unknown" }
  return "active session outputDuration=$([math]::Round([double]$data.outputDuration / 1000))s, our recorded start $since ago; " +
    "if this IS ours (a Restreamer restart without -Action Rebaseline), see .claude/skills/obs-recovery"
}

function Wait-StreamActive([bool]$want, [int]$seconds) {
  for ($i = 0; $i -lt $seconds; $i++) {
    $data = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
    if ((Get-ObsActive "GetStreamStatus" $data) -eq $want) { return $data }
    Start-Sleep -Seconds 1
  }
  return $null
}

# Opens its own session; exits 1 (with the reason) when the rig is not ours to use.
function Start-OurStream([bool]$sampleBitrate) {
  $holder = Get-RigLeaseHolder
  if ($holder) { Write-NotReady "camera-box holds the rig lease: $holder"; exit 1 }
  $why = Test-ObsProcess
  if ($why) { Write-NotReady $why; exit 1 }
  try { Connect-Obs } catch { Write-NotReady "websocket $($_.Exception.Message)"; exit 1 }
  try {
    $why = Test-ObsReady
    if ($why) { Write-NotReady $why; exit 1 }
    Set-StartedMarker "true"
    $resp = Invoke-ObsRequest "StartStream"
    if (-not $resp.requestStatus.result) {
      Set-StartedMarker "false"
      Write-Host "::error::StartStream refused: code $($resp.requestStatus.code) $($resp.requestStatus.comment) -- not ours, not touching it"
      exit 1
    }
    $active = Wait-StreamActive $true 30
    if ($null -eq $active) { throw "OBS did not report streaming within 30 s after StartStream" }
    Set-StartedAt $active
    if ($sampleBitrate) {
      # Read-only bitrate sample: the YouTube health gates run on whatever encoder
      # camera-box's TEST mode sets, so log what OBS actually sends.
      $b0 = [double]$active.outputBytes
      Start-Sleep -Seconds 10
      $later = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
      $kbps = [math]::Round((([double]$later.outputBytes - $b0) * 8 / 1000) / 10)
      Write-Host "OBS streaming to the restreamer inpoint (~$kbps kbps over 10 s, TEST-mode encoder settings)"
    } else {
      Write-Host "OBS streaming to the restreamer inpoint"
    }
  } finally {
    Close-Obs
  }
}

# Stops the stream this job started (the caller's if:/sequence guarantees that).
function Stop-OurStream {
  Connect-Obs
  try {
    $now = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
    if ((Get-ObsActive "GetStreamStatus" $now) -and -not (Test-OurStream $now)) {
      Set-StartedMarker "false"
      Write-Host "::error::the active stream is newer than ours -- not ours, not touching it ($(Get-IdentityNote $now))"
      exit 1
    }
    $resp = Invoke-ObsRequest "StopStream"
    # 501 = OutputNotRunning: already stopped, nothing left of ours.
    if (-not $resp.requestStatus.result -and $resp.requestStatus.code -ne 501) {
      throw "StopStream failed: code $($resp.requestStatus.code) $($resp.requestStatus.comment)"
    }
    $before = [double]$now.outputDuration
    for ($i = 0; $i -lt 20; $i++) {
      $data = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
      if (-not (Get-ObsActive "GetStreamStatus" $data)) {
        Write-Host "OBS stream stopped (the one this job started)"
        return
      }
      # A shorter duration than just before our stop = a new session took OBS.
      if ([double]$data.outputDuration -lt $before - 1000) {
        Set-StartedMarker "false"
        Write-Host "::error::a newer stream replaced ours while it stopped -- not ours, not touching it ($(Get-IdentityNote $data))"
        exit 1
      }
      Start-Sleep -Seconds 1
    }
    throw "OBS still streaming 20 s after StopStream"
  } finally {
    Close-Obs
  }
}

# Read-only. After Restreamer.exe was killed/restarted, OBS reconnects and its
# outputDuration restarts at 0: re-record when our session (re)began.
function Invoke-Rebaseline {
  Connect-Obs
  try {
    for ($i = 0; $i -lt 90; $i++) {
      $data = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
      if (-not (Get-ObsActive "GetStreamStatus" $data)) {
        Set-StartedMarker "false"
        Write-Host "::warning::our OBS stream is no longer active (OBS gave up reconnecting) -- nothing of ours left to stop"
        return
      }
      if (-not $data.outputReconnecting) {
        Set-StartedAt $data
        Write-Host "OBS stream re-anchored after the Restreamer restart (outputDuration $([math]::Round([double]$data.outputDuration / 1000))s)"
        return
      }
      Start-Sleep -Seconds 1
    }
    # Still reconnecting: still our output (no second session can start while it is
    # active). Anchor at now; the reconnect's own duration then only grows from here.
    Set-StartedAt @{ outputDuration = 0 }
    Write-Host "::warning::OBS still reconnecting after 90 s; anchored our stream at now"
  } finally {
    Close-Obs
  }
}

function Invoke-AssertNotStreaming {
  try { Connect-Obs } catch {
    $msg = $_.Exception.Message
    if ($msg -like "unreachable:*") {
      Write-Host "::warning::stream OBS not reachable ($msg) -- it streams nothing; proceeding"
      return
    }
    Write-NotReady "websocket $msg"
    exit 1
  }
  try {
    $data = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
    if (Get-ObsActive "GetStreamStatus" $data) {
      Write-Host "::error::stream OBS is already streaming -- camera-box owns it, not touching it (this job needs the inpoint free)"
      exit 1
    }
    Write-Host "stream OBS is not streaming - good"
  } finally {
    Close-Obs
  }
}

try {
  switch ($Action) {
    "Start" { Start-OurStream $true }
    "Stop" { Stop-OurStream }
    "Republish" {
      Stop-OurStream
      Set-StartedMarker "false"
      $deadAir = [System.Diagnostics.Stopwatch]::StartNew()
      Write-Host "Dead-air gap: ${GapSeconds}s, then the checked restart..."
      Start-Sleep -Seconds $GapSeconds
      Start-OurStream $false
      Write-Host "Measured dead air (stopped -> streaming again): $([math]::Round($deadAir.Elapsed.TotalSeconds, 1)) s"
    }
    "Rebaseline" { Invoke-Rebaseline }
    "AssertNotStreaming" { Invoke-AssertNotStreaming }
  }
} catch {
  Write-Host "::error::obs-stream $Action failed: $($_.Exception.Message)"
  exit 1
}
exit 0
