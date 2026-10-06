# obs-ws.ps1 -- #374: the ONE obs-websocket client restreamer CI uses for stream OBS.
# Dot-source it:  . "$PSScriptRoot\obs-ws.ps1"
#
# Stream OBS (10.77.9.204) is camera-box's development target. Owner directive
# 2026-08-30: restreamer may only START and STOP OBS streaming and read status.
# Every request goes through Invoke-ObsRequest, and every caller passes a literal
# request type from this set (checked by scripts/ci/verify_no_obs_mutation.py):
#   StartStream, StopStream, GetStreamStatus, GetRecordStatus, GetVersion,
#   GetCurrentProgramScene, GetStreamServiceSettings
#
# Env: OBS_WS_HOST (127.0.0.1), OBS_WS_PORT (4455), OBS_WS_PASSWORD (optional),
#      OBS_TEST_SCENE (Development), RIG_LEASE_URL (http://dev1:8890/rig-lease.json).

$script:ObsSocket = $null
$script:ObsTimeoutMs = 10000
$script:ObsTestScene = if ($env:OBS_TEST_SCENE) { $env:OBS_TEST_SCENE } else { "Development" }
$script:ObsProductionScene = "PRO"
# The restreamer inpoint as OBS reaches it on this box: loopback, the box's own
# LAN IP, or its LAN name. Any key: the inpoint accepts every key under `live`.
$script:ObsInpointPattern = '^rtmp://(127\.0\.0\.1|localhost|10\.77\.9\.204|stream\.lan):1234/live/?$'

function Send-ObsJson($obj) {
  $json = $obj | ConvertTo-Json -Depth 6 -Compress
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
  $seg = [ArraySegment[byte]]::new($bytes, 0, $bytes.Length)
  $cts = New-Object System.Threading.CancellationTokenSource
  $cts.CancelAfter($script:ObsTimeoutMs)
  $script:ObsSocket.SendAsync($seg, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts.Token).GetAwaiter().GetResult()
}

function Receive-ObsJson {
  $buf = New-Object byte[] 65536
  $text = ""
  do {
    $seg = [ArraySegment[byte]]::new($buf, 0, $buf.Length)
    $cts = New-Object System.Threading.CancellationTokenSource
    $cts.CancelAfter($script:ObsTimeoutMs)
    $recv = $script:ObsSocket.ReceiveAsync($seg, $cts.Token).GetAwaiter().GetResult()
    if ($recv.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
      throw "websocket closed by OBS (status $($script:ObsSocket.CloseStatus) $($script:ObsSocket.CloseStatusDescription))"
    }
    $text += [System.Text.Encoding]::UTF8.GetString($buf, 0, $recv.Count)
  } while (-not $recv.EndOfMessage)
  return $text | ConvertFrom-Json
}

# Connects and identifies (no event subscriptions). Throws "unreachable: ..." when
# the socket cannot be opened and "identify rejected: ..." when OBS refuses us.
function Connect-Obs {
  $wsHost = if ($env:OBS_WS_HOST) { $env:OBS_WS_HOST } else { "127.0.0.1" }
  $wsPort = $env:OBS_WS_PORT -as [int]
  if (-not $wsPort) { $wsPort = 4455 }
  $script:ObsSocket = New-Object System.Net.WebSockets.ClientWebSocket
  try {
    $cts = New-Object System.Threading.CancellationTokenSource
    $cts.CancelAfter($script:ObsTimeoutMs)
    $script:ObsSocket.ConnectAsync([Uri]"ws://${wsHost}:${wsPort}", $cts.Token).GetAwaiter().GetResult()
    $hello = Receive-ObsJson
  } catch {
    throw "unreachable: ws://${wsHost}:${wsPort}: $($_.Exception.Message)"
  }
  try {
    $auth = $hello.d.authentication
    $identify = @{ rpcVersion = 1; eventSubscriptions = 0 }
    if ($auth) {
      $sha = [System.Security.Cryptography.SHA256]::Create()
      $secret = [Convert]::ToBase64String($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("$($env:OBS_WS_PASSWORD)$($auth.salt)")))
      $identify.authentication = [Convert]::ToBase64String($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("$secret$($auth.challenge)")))
    }
    Send-ObsJson @{ op = 1; d = $identify }
    $identified = Receive-ObsJson
    if ($identified.op -ne 2) { throw "op=$($identified.op)" }
  } catch {
    throw "identify rejected: $($_.Exception.Message)"
  }
}

function Close-Obs {
  if ($null -eq $script:ObsSocket) { return }
  try { $script:ObsSocket.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, "", [System.Threading.CancellationToken]::None).GetAwaiter().GetResult() } catch { }
  $script:ObsSocket = $null
}

# One request; returns the response `d` (requestStatus + responseData). Skips any
# non-matching message until the response to THIS request arrives.
function Invoke-ObsRequest([string]$requestType) {
  $id = [guid]::NewGuid().ToString()
  Send-ObsJson @{ op = 6; d = @{ requestType = $requestType; requestId = $id } }
  for ($i = 0; $i -lt 50; $i++) {
    $msg = Receive-ObsJson
    if ($msg.op -eq 7 -and $msg.d.requestId -eq $id) { return $msg.d }
  }
  throw "no response to the request within 50 messages"
}

# A read whose failure is an error; returns responseData.
function Get-ObsData([string]$what, $response) {
  if (-not $response.requestStatus.result) {
    throw "$what failed: code $($response.requestStatus.code) $($response.requestStatus.comment)"
  }
  return $response.responseData
}

# outputActive must be a real boolean: a missing field never reads as "idle".
function Get-ObsActive([string]$what, $data) {
  if ($null -eq $data -or $data.outputActive -isnot [bool]) {
    throw "$what returned no outputActive"
  }
  return $data.outputActive
}

# Read-only readiness on an open connection. Returns $null when ready, else the
# reason. A recording returns "RECORDING" (it has its own message).
function Test-ObsReady {
  $version = Get-ObsData "GetVersion" (Invoke-ObsRequest "GetVersion")
  Write-Host "OBS $($version.obsVersion), obs-websocket $($version.obsWebSocketVersion)"

  $scene = (Get-ObsData "GetCurrentProgramScene" (Invoke-ObsRequest "GetCurrentProgramScene")).currentProgramSceneName
  Write-Host "Program scene: '$scene' (expected '$($script:ObsTestScene)')"
  if ($scene -ceq $script:ObsProductionScene) { return "program scene is the production scene '$scene', not '$($script:ObsTestScene)'" }
  if ($scene -cne $script:ObsTestScene) { return "program scene is '$scene', not the TEST scene '$($script:ObsTestScene)'" }

  $svc = Get-ObsData "GetStreamServiceSettings" (Invoke-ObsRequest "GetStreamServiceSettings")
  $server = "$($svc.streamServiceSettings.server)"
  Write-Host "Stream service: type=$($svc.streamServiceType) server=$server (key not shown)"
  if ($svc.streamServiceType -ne "rtmp_custom" -or $server -notmatch $script:ObsInpointPattern) {
    return "stream service is '$($svc.streamServiceType)' -> '$server', not the restreamer inpoint rtmp://127.0.0.1:1234/live"
  }

  $stream = Get-ObsData "GetStreamStatus" (Invoke-ObsRequest "GetStreamStatus")
  if (Get-ObsActive "GetStreamStatus" $stream) { return "OBS is already streaming ($($stream.outputTimecode))" }
  $record = Get-ObsData "GetRecordStatus" (Invoke-ObsRequest "GetRecordStatus")
  if (Get-ObsActive "GetRecordStatus" $record) { return "RECORDING" }
  return $null
}

# Exactly one obs64 process, or the reason.
function Test-ObsProcess {
  $procs = @(Get-Process -Name obs64 -ErrorAction SilentlyContinue)
  if ($procs.Count -eq 0) { return "obs64 is not running" }
  if ($procs.Count -gt 1) {
    $ids = ($procs | ForEach-Object { "$($_.Id)" }) -join ","
    return "$($procs.Count) obs64 processes (PIDs $ids)"
  }
  Write-Host "obs64 running: PID $($procs[0].Id), $([int]($procs[0].WorkingSet64 / 1MB)) MB"
  return $null
}

# camera-box's rig lease (#349), read once. Returns the holder when held and not
# stale, else $null. Unreachable / unparseable -> $null with a warning (fail-open,
# the same semantics as rig-lease-wait.ps1; this one never waits).
function Get-RigLeaseHolder {
  $url = if ($env:RIG_LEASE_URL) { $env:RIG_LEASE_URL } else { "http://dev1:8890/rig-lease.json" }
  try {
    $lease = Invoke-RestMethod -Uri $url -Method GET -TimeoutSec 5 -Headers @{ "Cache-Control" = "no-cache" }
  } catch {
    Write-Host "::warning::[rig-lease] $url unreachable: $($_.Exception.Message) -- lease not checked (fail-open)"
    return $null
  }
  if ($null -eq $lease -or $lease.schema -ne 1) {
    Write-Host "::warning::[rig-lease] unparseable or unknown-schema response -- lease not checked (fail-open)"
    return $null
  }
  if (-not $lease.held -or $lease.stale) { return $null }
  $job = if ($lease.holder -and $lease.holder.job) { $lease.holder.job } else { "(unknown)" }
  $run = if ($lease.holder -and $lease.holder.run_url) { $lease.holder.run_url } else { "(unknown)" }
  return "$job ($run)"
}

function Write-NotReady([string]$what) {
  if ($what -eq "RECORDING") {
    Write-Host "::error::stream OBS is recording -- not touching it (camera-box / the owner owns recordings)"
  } else {
    Write-Host "::error::stream OBS not ready ($what) -- camera-box owns it, not touching it"
  }
}
