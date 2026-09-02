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
# This script ALWAYS exits 0 -- it is a courtesy serialization wait, never a hard
# gate, so it can never fail the CI job.

# NOTE: intentionally NO `$ErrorActionPreference = "Stop"` at script scope -- every
# fallible call (the HTTP GET) is wrapped in its own try/catch that proceeds
# fail-open, and we must never let an unexpected terminating error fail the job.

$Url          = if ($env:RIG_LEASE_URL)      { $env:RIG_LEASE_URL }           else { "http://10.77.9.103:8890/rig-lease.json" }
$BudgetS      = if ($env:RIG_LEASE_BUDGET_S) { [int]$env:RIG_LEASE_BUDGET_S } else { 3600 }  # 60 min hard cap (camera-box E2E ~75 min, ttl_s guides the real wait)
$GraceS       = if ($env:RIG_LEASE_GRACE_S)  { [int]$env:RIG_LEASE_GRACE_S }  else { 120 }
$PollS        = if ($env:RIG_LEASE_POLL_S)   { [int]$env:RIG_LEASE_POLL_S }   else { 30 }
$HttpTimeoutS = if ($env:RIG_LEASE_HTTP_TIMEOUT_S) { [int]$env:RIG_LEASE_HTTP_TIMEOUT_S } else { 5 }

if ($BudgetS -le 0) { $BudgetS = 3600 }
if ($PollS   -le 0) { $PollS   = 30 }

Write-Host "[rig-lease] pre-StartStream check against $Url (budget ${BudgetS}s, poll ${PollS}s, http-timeout ${HttpTimeoutS}s)"

$deadline = $null   # set on the FIRST held-and-not-stale observation
$attempt = 0

while ($true) {
  $attempt++

  $lease = $null
  try {
    # Read live every time; -Headers no-cache defeats any intermediary cache.
    $lease = Invoke-RestMethod -Uri $Url -Method GET -TimeoutSec $HttpTimeoutS -Headers @{ "Cache-Control" = "no-cache" }
  } catch {
    Write-Host "[rig-lease] endpoint unreachable ($Url): $_ -- PROCEEDING (fail-open: endpoint down != rig busy)."
    exit 0
  }

  if ($null -eq $lease -or $lease.schema -ne 1) {
    Write-Host "[rig-lease] unparseable or unknown-schema response -- PROCEEDING (fail-open)."
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
    $ttl = if ($null -ne $lease.ttl_s -and [int]$lease.ttl_s -gt 0) { [int]$lease.ttl_s } else { $BudgetS }
    $waitCap = [Math]::Min($ttl + $GraceS, $BudgetS)
    $deadline = (Get-Date).AddSeconds($waitCap)
    Write-Host "[rig-lease] rig HELD by $holderJob ($holderUrl); will wait up to ${waitCap}s (min(ttl_s+grace, budget); ttl_s=$($lease.ttl_s))."
  }

  if ((Get-Date) -ge $deadline) {
    Write-Host "[rig-lease] wait budget exhausted, rig still held by $holderUrl -- PROCEEDING anyway so CI is not deadlocked by a stuck lease (bounded budget)."
    exit 0
  }

  Write-Host "[rig-lease] rig held by $holderJob ($holderUrl), not stale (heartbeat_age_s=$($lease.heartbeat_age_s), ttl_s=$($lease.ttl_s)) -- re-poll in ${PollS}s (attempt $attempt)."
  Start-Sleep -Seconds $PollS
}
