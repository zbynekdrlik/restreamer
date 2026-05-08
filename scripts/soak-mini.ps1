# Phase 1 mini-soak (issue #176, spec section 5.2).
# 30 min sample loop asserting per-endpoint chunk_delay and FB death-rate.
# Manual run:  $env:EVENT_ID="9289"; .\scripts\soak-mini.ps1
# CI invokes via .github/workflows/soak-mini.yml on the windows-self-hosted-runner.

$ErrorActionPreference = "Stop"

$Host_ = if ($env:HOST) { $env:HOST } else { "http://10.77.9.204:8910" }
if (-not $env:EVENT_ID) { throw "EVENT_ID env var required" }
$EventId = $env:EVENT_ID
$DeliveryDelaySecs = if ($env:DELIVERY_DELAY_SECS) { [int]$env:DELIVERY_DELAY_SECS } else { 120 }
$Samples = if ($env:SAMPLES) { [int]$env:SAMPLES } else { 60 }
$IntervalSecs = if ($env:INTERVAL_SECS) { [int]$env:INTERVAL_SECS } else { 30 }
$MaxDeathsPerEndpoint = if ($env:MAX_DEATHS_PER_ENDPOINT) { [int]$env:MAX_DEATHS_PER_ENDPOINT } else { 50 }

$StartTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.000Z")
Write-Host "soak-mini start: host=$Host_ event=$EventId samples=$Samples interval=$($IntervalSecs)s start_ts=$StartTs"

function Get-ThresholdForAlias {
    param([string]$Alias)
    if ($Alias -like "FB-*") { return 1.3 }
    return 1.1
}

$Fails = New-Object System.Collections.Generic.List[string]

for ($i = 1; $i -le $Samples; $i++) {
    Start-Sleep -Seconds $IntervalSecs
    try {
        $status = Invoke-RestMethod -Uri "$Host_/api/v1/delivery/status?event_id=$EventId" -TimeoutSec 10
    } catch {
        throw "sample $i failed: GET delivery/status: $_"
    }
    Write-Host "[sample $i] endpoints=$($status.endpoint_details.Count)"

    foreach ($ep in $status.endpoint_details) {
        $aliasVal = [string]$ep.alias
        $delay = [double]$ep.chunk_delay_secs
        $threshold = Get-ThresholdForAlias -Alias $aliasVal
        $limit = $DeliveryDelaySecs * $threshold
        if ($delay -gt $limit) {
            $msg = "[sample $i] alias='$aliasVal' delay=$($delay)s exceeds threshold $($limit)s (target=$($DeliveryDelaySecs)s, mult=$threshold)"
            Write-Host "FAIL: $msg" -ForegroundColor Red
            $Fails.Add($msg)
        }
    }

    if ($Fails.Count -gt 0) {
        Write-Host "==== soak-mini FAIL detail ===="
        $Fails | ForEach-Object { Write-Host $_ }
        exit 1
    }
}

# Cumulative death-rate check at end of window.
$auditUrl = "$Host_/api/v1/audit?event_id=$EventId&action=endpoint_rtmp_push_died&since=$StartTs&limit=10000"
try {
    $audit = Invoke-RestMethod -Uri $auditUrl -TimeoutSec 30
} catch {
    throw "audit fetch failed: $_"
}

$rows = $null
if ($audit.rows) { $rows = $audit.rows }
elseif ($audit.items) { $rows = $audit.items }
else { $rows = @() }

$deathsByEndpoint = @{}
foreach ($r in $rows) {
    $ep = if ($r.endpoint) { [string]$r.endpoint } else { "<unknown>" }
    if (-not $deathsByEndpoint.ContainsKey($ep)) { $deathsByEndpoint[$ep] = 0 }
    $deathsByEndpoint[$ep] += 1
}
Write-Host "deaths_per_endpoint:"
foreach ($k in $deathsByEndpoint.Keys) {
    Write-Host "  $k -> $($deathsByEndpoint[$k])"
}

$failed = $false
foreach ($k in $deathsByEndpoint.Keys) {
    $cnt = $deathsByEndpoint[$k]
    if ($cnt -gt $MaxDeathsPerEndpoint) {
        Write-Host "FAIL: endpoint=$k had $cnt rtmp_push_died audit rows (limit $MaxDeathsPerEndpoint)" -ForegroundColor Red
        $failed = $true
    }
}

if ($failed) {
    exit 1
}

$totalMinutes = ($Samples * $IntervalSecs) / 60
Write-Host "soak-mini PASS: $Samples samples * $($IntervalSecs)s = $totalMinutes min, all endpoints under thresholds."
