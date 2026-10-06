# obs-readiness-check.ps1 -- #374: READ-ONLY pre-flight of stream OBS before CI streams.
#
# OBS on the stream box (10.77.9.204) belongs to camera-box: it is their development
# target. Owner directive 2026-08-30: restreamer CI may only start and stop OBS
# STREAMING, nothing else -- no kill, no relaunch, no scene / profile / bitrate /
# stream-service change, no recording control. So instead of "fixing" OBS, CI checks
# that camera-box's TEST mode left it usable and FAILS with the reason otherwise:
#
#   - exactly one obs64 process;
#   - the obs-websocket answers (and authenticates);
#   - the program scene is the TEST scene (camera-box's `Development`, issue 1380;
#     `PRO` is the owner's production scene and is never accepted);
#   - the stream service points at the restreamer inpoint (rtmp://<this box>:1234/live);
#   - OBS is not streaming already (someone else's stream -- not ours to stop);
#   - OBS is not recording (a recording is camera-box's / the owner's).
#
# Websocket requests sent: GetVersion, GetCurrentProgramScene, GetStreamServiceSettings,
# GetStreamStatus, GetRecordStatus. All are reads; the test-integrity guard
# (scripts/ci/verify_no_obs_mutation.py) rejects any other request type here.
# The stream key is never printed (the inpoint accepts any key under `live`).
#
# Exit 0 = ready. Exit 1 = not ready, with one line naming what is wrong.
#
# Env: OBS_WS_HOST (127.0.0.1), OBS_WS_PORT (4455), OBS_WS_PASSWORD (optional),
#      OBS_TEST_SCENE (Development).

$ErrorActionPreference = "Stop"

$wsHost = if ($env:OBS_WS_HOST) { $env:OBS_WS_HOST } else { "127.0.0.1" }
$wsPort = $env:OBS_WS_PORT -as [int]
if (-not $wsPort) { $wsPort = 4455 }
$testScene = if ($env:OBS_TEST_SCENE) { $env:OBS_TEST_SCENE } else { "Development" }
$productionScene = "PRO"
# The restreamer inpoint as OBS reaches it on this box: loopback or the box's own LAN IP.
$inpointPattern = '^rtmp://(127\.0\.0\.1|localhost|10\.77\.9\.204):1234/live/?$'
$timeoutMs = 10000

function Fail-NotReady([string]$what) {
  Write-Host "::error::stream OBS not ready ($what) -- camera-box owns it, not touching it"
  exit 1
}

# --- 1. exactly one obs64 process ---------------------------------------------------
$procs = @(Get-Process -Name obs64 -ErrorAction SilentlyContinue)
if ($procs.Count -eq 0) { Fail-NotReady "obs64 is not running" }
if ($procs.Count -gt 1) {
  $ids = ($procs | ForEach-Object { "$($_.Id)" }) -join ","
  Fail-NotReady "$($procs.Count) obs64 processes (PIDs $ids)"
}
Write-Host "obs64 running: PID $($procs[0].Id), $([int]($procs[0].WorkingSet64 / 1MB)) MB"

# --- 2. websocket reachable + identified -------------------------------------------
$ws = New-Object System.Net.WebSockets.ClientWebSocket

function Send-Json($obj) {
  $json = $obj | ConvertTo-Json -Depth 6 -Compress
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
  $seg = [ArraySegment[byte]]::new($bytes, 0, $bytes.Length)
  $cts = New-Object System.Threading.CancellationTokenSource
  $cts.CancelAfter($timeoutMs)
  $ws.SendAsync($seg, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts.Token).GetAwaiter().GetResult()
}

function Receive-Json {
  $buf = New-Object byte[] 65536
  $text = ""
  do {
    $seg = [ArraySegment[byte]]::new($buf, 0, $buf.Length)
    $cts = New-Object System.Threading.CancellationTokenSource
    $cts.CancelAfter($timeoutMs)
    $recv = $ws.ReceiveAsync($seg, $cts.Token).GetAwaiter().GetResult()
    if ($recv.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
      throw "websocket closed by OBS (status $($ws.CloseStatus) $($ws.CloseStatusDescription))"
    }
    $text += [System.Text.Encoding]::UTF8.GetString($buf, 0, $recv.Count)
  } while (-not $recv.EndOfMessage)
  return $text | ConvertFrom-Json
}

# One read request; skips events (op 5) until the matching response (op 7).
function Invoke-ObsRead([string]$requestType) {
  $id = "readiness-$requestType"
  Send-Json @{ op = 6; d = @{ requestType = $requestType; requestId = $id } }
  for ($i = 0; $i -lt 50; $i++) {
    $msg = Receive-Json
    if ($msg.op -eq 7 -and $msg.d.requestId -eq $id) {
      if (-not $msg.d.requestStatus.result) {
        throw "$requestType failed: code $($msg.d.requestStatus.code) $($msg.d.requestStatus.comment)"
      }
      return $msg.d.responseData
    }
  }
  throw "${requestType}: no response within 50 messages"
}

try {
  $cts = New-Object System.Threading.CancellationTokenSource
  $cts.CancelAfter($timeoutMs)
  $ws.ConnectAsync([Uri]"ws://${wsHost}:${wsPort}", $cts.Token).GetAwaiter().GetResult()
  $hello = Receive-Json
  $auth = $hello.d.authentication
  if ($auth) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $secret = [Convert]::ToBase64String($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("$($env:OBS_WS_PASSWORD)$($auth.salt)")))
    $authHash = [Convert]::ToBase64String($sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("$secret$($auth.challenge)")))
    # eventSubscriptions 0: no events, so only responses come back.
    Send-Json @{ op = 1; d = @{ rpcVersion = 1; authentication = $authHash; eventSubscriptions = 0 } }
  } else {
    Send-Json @{ op = 1; d = @{ rpcVersion = 1; eventSubscriptions = 0 } }
  }
  $identified = Receive-Json
  if ($identified.op -ne 2) { throw "identify not accepted (op=$($identified.op))" }
} catch {
  Fail-NotReady "websocket ws://${wsHost}:${wsPort} unreachable: $($_.Exception.Message)"
}

try {
  $version = Invoke-ObsRead "GetVersion"
  Write-Host "OBS $($version.obsVersion), obs-websocket $($version.obsWebSocketVersion)"

  # --- 3. program scene = camera-box TEST scene -------------------------------------
  $scene = (Invoke-ObsRead "GetCurrentProgramScene").currentProgramSceneName
  Write-Host "Program scene: '$scene' (expected '$testScene')"
  if ($scene -ceq $productionScene) { Fail-NotReady "program scene is the production scene '$productionScene', not '$testScene'" }
  if ($scene -cne $testScene) { Fail-NotReady "program scene is '$scene', not the TEST scene '$testScene'" }

  # --- 4. stream service = restreamer inpoint ---------------------------------------
  $svc = Invoke-ObsRead "GetStreamServiceSettings"
  $server = "$($svc.streamServiceSettings.server)"
  Write-Host "Stream service: type=$($svc.streamServiceType) server=$server (key not shown)"
  if ($svc.streamServiceType -ne "rtmp_custom" -or $server -notmatch $inpointPattern) {
    Fail-NotReady "stream service is '$($svc.streamServiceType)' -> '$server', not the restreamer inpoint rtmp://127.0.0.1:1234/live"
  }

  # --- 5. not streaming, not recording ----------------------------------------------
  $stream = Invoke-ObsRead "GetStreamStatus"
  if ($stream.outputActive) { Fail-NotReady "OBS is already streaming ($($stream.outputTimecode))" }
  $record = Invoke-ObsRead "GetRecordStatus"
  if ($record.outputActive) {
    Write-Host "::error::stream OBS is recording -- not touching it (camera-box / the owner owns recordings)"
    exit 1
  }
} catch {
  Fail-NotReady "read failed: $($_.Exception.Message)"
} finally {
  try { $ws.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, "", [System.Threading.CancellationToken]::None).GetAwaiter().GetResult() } catch { }
}

Write-Host "stream OBS ready: one obs64, scene '$testScene', inpoint service, not streaming, not recording"
exit 0
