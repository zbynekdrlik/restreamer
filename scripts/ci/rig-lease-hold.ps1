# rig-lease-hold.ps1 -- #349: an OBS-streaming job TAKES camera-box's shared rig lease
# (dev1 rig-lease server, camera-box issue 1432) for as long as it streams, so camera-box's
# rig-busy gate sees a NAMED holder instead of an anonymous OBS stream.
#
#   -Action Acquire  right before "Start OBS stream", after rig-lease-wait.ps1. POSTs
#                    /rig-lease/acquire; on 409 (camera-box holds the rig) it retries until
#                    the job's shared deadline (RIG_LEASE_DEADLINE_EPOCH, recorded by
#                    rig-lease-wait.ps1), then fails the job with the holder. On 200 it
#                    keeps the lease id in $RUNNER_TEMP, writes RIG_LEASE_HELD=true to
#                    GITHUB_ENV and starts a detached heartbeat (-Action Beat).
#   -Action Beat     the detached heartbeat: POST /rig-lease/heartbeat every
#                    RIG_LEASE_BEAT_S (30) while the lease file exists. A 404 means the
#                    lease is gone (reclaimed after 180 s without a beat): it leaves a
#                    "lost" file that Release turns into a failed job.
#   -Action Release  in an always() step after the OBS stop: stops the heartbeat, POSTs
#                    /rig-lease/release (idempotent), and fails the job if the lease was
#                    lost while we streamed.
#
# Contract: restreamer #349 comments 6179376408 + 6179710985. Every POST carries
# `Authorization: Bearer $RIG_LEASE_TOKEN` (repo secret). The read side stays fail-open
# like rig-lease-wait.ps1: no token or an unreachable server means "lease not taken",
# with a warning, and the job proceeds. A 400/401 is OUR bug and fails the job.

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("Acquire", "Beat", "Release")]
  [string]$Action
)

$ErrorActionPreference = "Stop"

$Base = if ($env:RIG_LEASE_API) { $env:RIG_LEASE_API.TrimEnd("/") } else { "http://dev1:8890" }
$Token = $env:RIG_LEASE_TOKEN
$StateDir = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [System.IO.Path]::GetTempPath() }
$LeaseFile = Join-Path $StateDir "rig-lease-id"
$BeatPidFile = Join-Path $StateDir "rig-lease-beat.pid"
$LostFile = Join-Path $StateDir "rig-lease-lost"
$BeatLog = Join-Path $StateDir "rig-lease-beat.log"

function Get-NowEpoch { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }

# One POST. Returns @{ code; body; text }; code 0 = no HTTP answer (unreachable).
function Invoke-LeaseCall([string]$path, $body) {
  $json = $body | ConvertTo-Json -Compress
  $headers = @{ Authorization = "Bearer $Token" }
  try {
    $r = Invoke-WebRequest -Uri "$Base/rig-lease/$path" -Method POST -Body $json `
      -ContentType "application/json" -Headers $headers -TimeoutSec 10 -UseBasicParsing
    return @{ code = [int]$r.StatusCode; body = ($r.Content | ConvertFrom-Json); text = $r.Content }
  } catch {
    $resp = $_.Exception.Response
    if ($null -eq $resp) { return @{ code = 0; body = $null; text = $_.Exception.Message } }
    $text = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { "" }
    $parsed = $null
    try { $parsed = $text | ConvertFrom-Json } catch { $parsed = $null }
    return @{ code = [int]$resp.StatusCode; body = $parsed; text = $text }
  }
}

# The lease id as a plain string. Get-Content returns one with ETS notes (PSPath, ...),
# which Windows PowerShell 5.1's ConvertTo-Json serializes into an object.
function Read-LeaseId { return [System.IO.File]::ReadAllText($LeaseFile).Trim() }

function Format-Holder($h) {
  if ($null -eq $h) { return "(unknown holder)" }
  $job = if ($h.job) { $h.job } else { "(unknown job)" }
  $who = if ($h.repo) { "$($h.repo) $job" } else { $job }
  $url = if ($h.run_url) { " ($($h.run_url))" } else { "" }
  return "$who$url"
}

function Invoke-Acquire {
  if (-not $Token) {
    Write-Host "::warning::[rig-lease] RIG_LEASE_TOKEN is not set -- lease not taken (fail-open)"
    return
  }
  $hold = if ($env:RIG_LEASE_HOLD_S) { [int]$env:RIG_LEASE_HOLD_S } else { 5400 }
  $runUrl = "$($env:GITHUB_SERVER_URL)/$($env:GITHUB_REPOSITORY)/actions/runs/$($env:GITHUB_RUN_ID)"
  $body = @{
    # owner/name: the server checks the holder's run state on GitHub with it.
    repo = "$($env:GITHUB_REPOSITORY)"
    run_id = "$($env:GITHUB_RUN_ID)"
    run_url = $runUrl
    job = "$($env:GITHUB_JOB)"
    expected_secs = $hold
  }
  $deadline = $env:RIG_LEASE_DEADLINE_EPOCH -as [double]
  $poll = if ($env:RIG_LEASE_POLL_S) { [int]$env:RIG_LEASE_POLL_S } else { 30 }
  while ($true) {
    $r = Invoke-LeaseCall "acquire" $body
    switch ($r.code) {
      200 {
        $id = $r.body.lease_id
        if (-not $id) { throw "acquire answered 200 without a lease_id: $($r.text)" }
        Set-Content -Path $LeaseFile -Value $id -Encoding ascii -NoNewline
        Remove-Item -Path $LostFile -ErrorAction SilentlyContinue
        if ($env:GITHUB_ENV) { "RIG_LEASE_HELD=true" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append }
        Start-Beat
        $note = if ($r.body.reclaimed) { ", reclaimed from a stale holder" } else { "" }
        Write-Host "[rig-lease] acquired for $($body.job) until $($r.body.expected_release_at)$note"
        return
      }
      409 {
        $holder = Format-Holder $r.body.holder
        $now = Get-NowEpoch
        if ($null -eq $deadline -or $now -ge $deadline) {
          Write-Host "::error::[rig-lease] the rig is held by $holder and the job's lease budget is spent -- not starting OBS streaming"
          exit 1
        }
        $left = $deadline - $now
        Write-Host "[rig-lease] held by $holder (heartbeat $($r.body.heartbeat_age_s)s ago) -- waiting, $([math]::Round($left))s of the job's budget left"
        Start-Sleep -Seconds ([math]::Max(1, [math]::Min($poll, [math]::Ceiling($left))))
      }
      0 {
        Write-Host "::warning::[rig-lease] $Base unreachable ($($r.text)) -- lease not taken (fail-open)"
        return
      }
      default {
        Write-Host "::error::[rig-lease] acquire answered $($r.code): $($r.text)"
        exit 1
      }
    }
  }
}

# The heartbeat outlives this step: a detached copy of this script (-Action Beat). The
# runner ends it with the job at the latest; Release stops it first.
function Start-Beat {
  $exe = (Get-Process -Id $PID).Path
  $argLine = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Action Beat"
  if ($PSVersionTable.PSEdition -eq "Desktop") {
    # Windows PowerShell: ShellExecute, so the child inherits no handle of this step. An
    # inherited stdout pipe keeps the step open until the heartbeat exits (the same trap
    # as program-audio-guard.ps1's watchdog). The heartbeat writes its own log.
    $p = Start-Process -FilePath $exe -ArgumentList $argLine -WindowStyle Hidden -PassThru
  } else {
    $p = Start-Process -FilePath $exe -ArgumentList $argLine -RedirectStandardOutput "$BeatLog.out" `
      -RedirectStandardError "$BeatLog.err" -PassThru
  }
  Set-Content -Path $BeatPidFile -Value "$($p.Id) $($p.StartTime.ToUniversalTime().Ticks)" -Encoding ascii -NoNewline
}

function Write-BeatLog([string]$line) {
  Add-Content -Path $BeatLog -Value "$([DateTimeOffset]::UtcNow.ToString('HH:mm:ss')) $line" -Encoding utf8
}

function Invoke-Beat {
  $every = if ($env:RIG_LEASE_BEAT_S) { [int]$env:RIG_LEASE_BEAT_S } else { 30 }
  while (Test-Path $LeaseFile) {
    Start-Sleep -Seconds $every
    if (-not (Test-Path $LeaseFile)) { break }
    $id = Read-LeaseId
    $r = Invoke-LeaseCall "heartbeat" @{ lease_id = $id }
    if ($r.code -eq 200) { Write-BeatLog "heartbeat ok, release at $($r.body.expected_release_at)"; continue }
    if ($r.code -eq 404) {
      Set-Content -Path $LostFile -Value "heartbeat answered 404 not_holder: $($r.text)" -Encoding utf8
      Write-BeatLog "lease LOST: $($r.text)"
      return
    }
    # 409 over_hold, 5xx or unreachable: keep beating; the server reclaims only after 180 s.
    Write-BeatLog "heartbeat answered $($r.code): $($r.text)"
  }
}

# The heartbeat process this job started, or $null (also when its pid was reused).
function Get-BeatProcess {
  if (-not (Test-Path $BeatPidFile)) { return $null }
  $parts = (Get-Content -Path $BeatPidFile -Raw).Trim() -split " "
  $proc = Get-Process -Id ([int]$parts[0]) -ErrorAction SilentlyContinue
  if ($null -eq $proc) { return $null }
  if ($parts.Count -gt 1 -and "$($proc.StartTime.ToUniversalTime().Ticks)" -ne $parts[1]) { return $null }
  return $proc
}

function Invoke-Release {
  $beat = Get-BeatProcess
  if ($beat) { Stop-Process -Id $beat.Id -Force -ErrorAction SilentlyContinue }
  Remove-Item -Path $BeatPidFile -ErrorAction SilentlyContinue
  if (Test-Path $BeatLog) { Write-Host "[rig-lease] heartbeat log:"; Get-Content -Path $BeatLog | ForEach-Object { Write-Host "  $_" } }
  if (Test-Path $LeaseFile) {
    $id = Read-LeaseId
    Remove-Item -Path $LeaseFile
    $r = Invoke-LeaseCall "release" @{ lease_id = $id }
    if ($r.code -eq 200) {
      Write-Host "[rig-lease] released (released=$($r.body.released))"
    } else {
      Write-Host "::warning::[rig-lease] release answered $($r.code): $($r.text) -- the server reclaims it 180 s after the last heartbeat"
    }
  } else {
    Write-Host "[rig-lease] no lease held by this job"
  }
  if (Test-Path $LostFile) {
    $why = Get-Content -Path $LostFile -Raw
    Remove-Item -Path $LostFile
    Write-Host "::error::[rig-lease] the lease was lost while this job streamed ($why) -- camera-box may have driven the rig meanwhile"
    exit 1
  }
}

switch ($Action) {
  "Acquire" { Invoke-Acquire }
  "Beat" { Invoke-Beat }
  "Release" { Invoke-Release }
}
exit 0
