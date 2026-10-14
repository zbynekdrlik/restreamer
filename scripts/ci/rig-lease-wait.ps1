# scripts/ci/rig-lease-wait.ps1
#
# #349 / camera-box #830 + #1277 -- the RESTREAMER half of the shared cross-repo
# rig lease. camera-box's full-path-e2e gate (on dev1 Linux) and restreamer's
# OBS-driving E2E jobs (on the Windows stream box, 10.77.9.204) drive the SAME
# physical rig (strih 10.77.9.202 / stream 10.77.9.204 OBS) from DIFFERENT
# machines, so there is no shared local filesystem. camera-box exposes its
# `/var/tmp/rig-lease/` lockdir READ-ONLY over HTTP; restreamer polls it BEFORE
# starting OBS streaming and waits out an active camera-box hold.
#
# We WRITE NOTHING -- our OBS streaming IS the lease in the camera-box->restreamer
# direction (camera-box's own OBS-state busy-check sees us streaming). This script
# only READS.
#
# Semantics (agreed on restreamer#349):
#   held && !stale  -> wait, bounded budget min(ttl_s + grace, our budget), re-poll
#   held &&  stale  -> proceed (reclaimable lease -- camera-box #657 self-heal)
#   free            -> proceed
#   connection refused / timeout / non-200 / unparseable / unknown schema
#                   -> proceed AND log a warning (FAIL-OPEN: endpoint down != rig
#                      busy; camera-box's OBS-state gate still protects the reverse
#                      direction)
#   budget exhausted while still held -> proceed anyway (never deadlock CI)
#
# ONE budget per job. The first check records the job's deadline (now + budget) as
# RIG_LEASE_DEADLINE_EPOCH in GITHUB_ENV; a later check (the final re-check right
# before StartStream) waits for the holder up to that deadline, never past it, and
# never re-records it. Main run 38530980091: a separate 10-min budget on the final
# re-check gave up while camera-box held the rig for 30 more minutes, and Start then
# refused OBS. The deadline bounds only the WAIT: a free rig is used at any time.
#
# This script ALWAYS exits 0 -- it is a courtesy serialization wait, never a hard
# gate, so it can never fail the CI job.

# NOTE: intentionally NO `$ErrorActionPreference = "Stop"` at script scope -- every
# fallible call (the HTTP GET) is wrapped in its own try/catch that proceeds
# fail-open, and we must never let an unexpected terminating error fail the job.

$Url          = if ($env:RIG_LEASE_URL) { $env:RIG_LEASE_URL } else { "http://dev1:8890/rig-lease.json" }
# -as [int] yields $null (never throws) on a null/blank/non-numeric override.
$BudgetS      = $env:RIG_LEASE_BUDGET_S       -as [int]   # 60 min default (camera-box E2E ~75 min, ttl_s guides the real wait)
$GraceS       = $env:RIG_LEASE_GRACE_S        -as [int]
$PollS        = $env:RIG_LEASE_POLL_S         -as [int]
$HttpTimeoutS = $env:RIG_LEASE_HTTP_TIMEOUT_S -as [int]

# Sanitize every numeric knob: a bad override must fall back to a safe default,
# never $null -- a $null budget/ttl collapses the wait and a $null/0 -TimeoutSec
# is INDEFINITE in PS 5.1 (would hang a black-holed endpoint to the step timeout).
if ($null -eq $BudgetS      -or $BudgetS      -le 0) { $BudgetS      = 3600 }
if ($null -eq $GraceS       -or $GraceS       -lt 0) { $GraceS       = 120 }
if ($null -eq $PollS        -or $PollS        -le 0) { $PollS        = 30 }
if ($null -eq $HttpTimeoutS -or $HttpTimeoutS -le 0) { $HttpTimeoutS = 5 }

Write-Host "[rig-lease] pre-StartStream check against $Url (budget ${BudgetS}s, poll ${PollS}s, http-timeout ${HttpTimeoutS}s)"

function Get-NowEpoch { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }

# -as [double] yields $null on a missing/non-numeric value; "" yields 0.
$JobDeadline = $env:RIG_LEASE_DEADLINE_EPOCH -as [double]
if ($null -eq $JobDeadline -or $JobDeadline -le 0) {
  $JobDeadline = [Math]::Floor((Get-NowEpoch) + $BudgetS)
  if ($env:GITHUB_ENV) {
    "RIG_LEASE_DEADLINE_EPOCH=$JobDeadline" | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
  }
  Write-Host "[rig-lease] job wait budget ${BudgetS}s starts now; the job's later lease checks share it."
} else {
  $left = [Math]::Max(0, [Math]::Round($JobDeadline - (Get-NowEpoch)))
  Write-Host "[rig-lease] ${left}s left of the job's wait budget (set by its first lease check)."
}

$deadline = $null   # set on the FIRST held-and-not-stale observation (epoch seconds)
$attempt = 0

while ($true) {
  $attempt++

  $lease = $null
  try {
    # Read live every time; -Headers no-cache defeats any intermediary cache.
    $lease = Invoke-RestMethod -Uri $Url -Method GET -TimeoutSec $HttpTimeoutS -Headers @{ "Cache-Control" = "no-cache" }
  } catch {
    # ::warning:: so a BYPASSED guard is visible in the Actions run summary -- otherwise
    # the guard could stay silently vacuous (e.g. camera-box #1277 not yet deployed).
    Write-Host "::warning::[rig-lease] endpoint unreachable ($Url): $_ -- PROCEEDING (fail-open: endpoint down is not rig busy). Guard BYPASSED; confirm camera-box #1277 is deployed."
    exit 0
  }

  if ($null -eq $lease -or $lease.schema -ne 1) {
    Write-Host "::warning::[rig-lease] unparseable or unknown-schema response -- PROCEEDING (fail-open). Guard BYPASSED."
    exit 0
  }

  if (-not $lease.held) {
    Write-Host "[rig-lease] rig FREE (attempt $attempt) -- proceeding."
    exit 0
  }

  if ($lease.stale) {
    Write-Host "[rig-lease] lease HELD but STALE (heartbeat_age_s=$($lease.heartbeat_age_s)) -- proceeding (reclaimable, camera-box #657 self-heal)."
    exit 0
  }

  # held && !stale -> a genuinely LIVE camera-box holder.
  $holderUrl = if ($lease.holder -and $lease.holder.run_url) { $lease.holder.run_url } else { "(unknown)" }
  $holderJob = if ($lease.holder -and $lease.holder.job)     { $lease.holder.job }     else { "(unknown)" }

  if ($null -eq $deadline) {
    # Bound the wait to min(ttl_s + grace, our budget). A holder claiming a huge
    # ttl can never make us wait past our own budget; a null/zero ttl falls back
    # to the full budget.
    $ttl = $lease.ttl_s -as [int]   # $null on non-numeric / overflow -- never throws
    if ($null -eq $ttl -or $ttl -le 0) { $ttl = $BudgetS }
    $waitCap = [Math]::Min($ttl + $GraceS, $BudgetS)
    $deadline = [Math]::Min((Get-NowEpoch) + $waitCap, $JobDeadline)
    $waitS = [Math]::Max(0, [Math]::Round($deadline - (Get-NowEpoch)))
    Write-Host "[rig-lease] rig HELD by $holderJob ($holderUrl); will wait up to ${waitS}s (min(ttl_s+grace, budget, the job's deadline); ttl_s=$($lease.ttl_s))."
  }

  if ((Get-NowEpoch) -ge $deadline) {
    Write-Host "::warning::[rig-lease] wait budget exhausted, rig still held by $holderUrl -- PROCEEDING anyway so CI is not deadlocked by a stuck lease (bounded budget)."
    exit 0
  }

  Write-Host "[rig-lease] rig held by $holderJob ($holderUrl), not stale (heartbeat_age_s=$($lease.heartbeat_age_s), ttl_s=$($lease.ttl_s)) -- re-poll in ${PollS}s (attempt $attempt)."
  Start-Sleep -Seconds $PollS
}
