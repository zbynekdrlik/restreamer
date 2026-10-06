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
#   -Action AssertNotStreaming read-only: fails when OBS is streaming (into the inpoint
#                              it would keep rtmp_connected true); OBS unreachable is a
#                              warning (a down OBS streams nothing).
#
# Stream OBS is camera-box's development target; owner directive 2026-08-30: only
# Start/Stop streaming. Nothing here changes a scene, a setting or a recording.

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("Start", "Stop", "AssertNotStreaming")]
  [string]$Action
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\obs-ws.ps1"

$Marker = "OBS_STREAMING_STARTED_BY_CI"

function Set-StartedMarker([string]$value) {
  if (-not $env:GITHUB_ENV) { throw "GITHUB_ENV is not set; cannot record $Marker" }
  "$Marker=$value" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
}

function Wait-StreamActive([bool]$want, [int]$seconds) {
  for ($i = 0; $i -lt $seconds; $i++) {
    $data = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
    if ((Get-ObsActive "GetStreamStatus" $data) -eq $want) { return $data }
    Start-Sleep -Seconds 1
  }
  return $null
}

function Invoke-Start {
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
    # Read-only bitrate sample: the YouTube health gates run on whatever encoder
    # camera-box's TEST mode sets, so log what OBS actually sends.
    $b0 = [double]$active.outputBytes
    Start-Sleep -Seconds 10
    $later = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
    $kbps = [math]::Round((([double]$later.outputBytes - $b0) * 8 / 1000) / 10)
    Write-Host "OBS streaming to the restreamer inpoint (~$kbps kbps over 10 s, TEST-mode encoder settings)"
  } finally {
    Close-Obs
  }
}

function Invoke-Stop {
  Connect-Obs
  try {
    $resp = Invoke-ObsRequest "StopStream"
    # 501 = OutputNotRunning: already stopped, nothing left of ours.
    if (-not $resp.requestStatus.result -and $resp.requestStatus.code -ne 501) {
      throw "StopStream failed: code $($resp.requestStatus.code) $($resp.requestStatus.comment)"
    }
    if ($null -eq (Wait-StreamActive $false 20)) { throw "OBS still streaming 20 s after StopStream" }
    Write-Host "OBS stream stopped (the one this job started)"
  } finally {
    Close-Obs
  }
}

function Invoke-AssertNotStreaming {
  try { Connect-Obs } catch {
    Write-Host "::warning::stream OBS not reachable ($($_.Exception.Message)) -- it streams nothing; proceeding"
    return
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
    "Start" { Invoke-Start }
    "Stop" { Invoke-Stop }
    "AssertNotStreaming" { Invoke-AssertNotStreaming }
  }
} catch {
  Write-Host "::error::obs-stream $Action failed: $($_.Exception.Message)"
  exit 1
}
exit 0
