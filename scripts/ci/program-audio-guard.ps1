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
#   Test-ProgramAudio [-BeforeStart]
#                                one read of the verdict. Returns $null when the
#                                program carries MEASUREMENT or SILENT, the sample is
#                                fresh (0 <= age_s <= 10) and no FOREIGN was seen in the
#                                last 15 s; otherwise a one-line reason: FOREIGN /
#                                UNKNOWN / stale / unreachable / malformed. FAIL-CLOSED.
#                                obs-stream.ps1 Start-OurStream runs it with -BeforeStart
#                                right before every StartStream (the start and every
#                                republish): that also refuses after an earlier breach
#                                or a dead watchdog in this job.
#   Set-ProgramAudioStreamOwned  obs-stream.ps1 Set-StartedMarker mirrors its marker
#                                here: the watchdog stops only OUR stream (#374: a
#                                session that is not ours is never stopped).
#   Start-ProgramAudioWatchdog   right after the OBS start step: launches a detached
#                                PowerShell (it must outlive the step; a Start-Job dies
#                                with the step's process) that re-reads the verdict
#                                every 10 s while our stream is live. On a breach it
#                                writes the breach marker FIRST, then asks Restreamer to
#                                stop OBS streaming (POST /api/v1/obs/stop-stream) and
#                                re-asks until Restreamer's GET /api/v1/obs/status
#                                CONFIRMS OBS is not streaming (3 reads in a row; the
#                                POST only queues the command), then keeps watching and
#                                re-stops if OBS streams again. It never gives up while
#                                the stream is ours; the teardown (or the job end) ends it.
#                                It ALSO cuts the CI event's delivery (delivery stop +
#                                deactivate, then no instance left), because the 120 s
#                                cache would keep sending the music (ROZHODNUTE
#                                2026-10-07); a non-CI event is never touched.
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
# Knobs (env, for tests; no workflow may set them -- verify_program_audio_guard.py):
#   PROGRAM_AUDIO_URL               default http://dev1:8890/program-audio.json
#   PROGRAM_AUDIO_MAX_AGE_S         default 10   (freshness of camera-box's sample)
#   PROGRAM_AUDIO_FOREIGN_WINDOW_S  default 30   (camera-box's FOREIGN latch, checked first)
#   PROGRAM_AUDIO_HTTP_TIMEOUT_S    default 5
#   PROGRAM_AUDIO_POLL_S            default 10
#   PROGRAM_AUDIO_FLAP_WINDOW_S     default 60   (3 tolerated non-OK polls in it = breach)
#   PROGRAM_AUDIO_API_BASE          default http://127.0.0.1:8910 (Restreamer's API)
#   PROGRAM_AUDIO_DELIVERY_BUDGET_S default 120  (wait for the CI event's instances to go)
# The job env's EVENT_NAME names the CI event whose delivery a breach cuts; only the
# CI-owned E2E events ($script:ProgramAudioCiEvents) are ever touched.
#
# The stop-stream call lives ONLY in Invoke-ProgramAudioStop: the #374 guard
# (verify_no_obs_mutation.py) bans that API everywhere else.
# Tests: tests/ci/test_program_audio_guard.py (mock sampler + mock Restreamer API).

param([switch]$RunWatchdog)

$script:ProgramAudioGuardScript = $PSCommandPath
$script:ProgramAudioOkVerdicts = @("MEASUREMENT", "SILENT")
# The CI-owned E2E events (ci.yml EVENT_NAME of the OBS-streaming jobs; pinned by
# verify_program_audio_guard.py). A breach never cuts the delivery of any other event.
$script:ProgramAudioCiEvents = @("E2E-Test", "E2E-FB-Test")

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
    Owned     = Join-Path $env:RUNNER_TEMP "program-audio-stream-owned"
    Log       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.log"
    Out       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.out"
    Err       = Join-Path $env:RUNNER_TEMP "program-audio-watchdog.err"
  }
}

function Get-ProgramAudioNow { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }

function Get-ProgramAudioStamp { return [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") }

# A small state file, rewritten in place. Not a rename-over: on Windows that fails
# while a reader holds the file open. A concurrent read can briefly see it empty, so
# Read-ProgramAudioFile retries; a sharing violation here is retried too.
function Write-ProgramAudioFile([string]$path, [string]$text) {
  for ($i = 0; ; $i++) {
    try {
      [System.IO.File]::WriteAllText($path, $text)
      return
    } catch {
      if ($i -ge 9) { throw }
      Start-Sleep -Milliseconds 100
    }
  }
}

# The trimmed content, or "" when the file is missing (or stays empty for ~1 s).
function Read-ProgramAudioFile([string]$path) {
  for ($i = 0; $i -lt 10; $i++) {
    if (-not (Test-Path -LiteralPath $path)) { return "" }
    $text = ([string](Get-Content -LiteralPath $path -Raw -ErrorAction SilentlyContinue)).Trim()
    if ($text) { return $text }
    Start-Sleep -Milliseconds 100
  }
  return ""
}

# Test-ProgramAudio = one read. -BeforeStart (right before every StartStream) is
# strict: any non-OK verdict refuses. It refuses at once after an earlier breach / a
# dead watchdog in this job, and on FOREIGN; a tolerable verdict (a startup UNKNOWN, a
# blip) is re-read every 2 s for up to PROGRAM_AUDIO_START_RETRY_S (15) first.
function Test-ProgramAudio([switch]$BeforeStart) {
  if (-not $BeforeStart) { return Read-ProgramAudioVerdict }
  $prior = Get-ProgramAudioGuardFailure
  if ($prior) { return "earlier in this job: $prior" }
  $window = Get-ProgramAudioKnob "PROGRAM_AUDIO_START_RETRY_S" 15
  $deadline = (Get-ProgramAudioNow) + $window
  while ($true) {
    $why = Read-ProgramAudioVerdict
    if (-not $why -or -not (Test-ProgramAudioTolerable $why)) { return $why }
    if ((Get-ProgramAudioNow) -ge $deadline) { return "$why (still not OK after a ${window}s pre-start retry)" }
    Write-Host "[program-audio] not OK yet, re-reading before the start: $why"
    Start-Sleep -Seconds 2
  }
}

# One read of camera-box's verdict. $null = OK to stream; otherwise the reason.
function Read-ProgramAudioVerdict {
  $url = Get-ProgramAudioUrl
  $maxAge = Get-ProgramAudioKnob "PROGRAM_AUDIO_MAX_AGE_S" 10
  # 30 s like camera-box's reference guard: with one tolerated poll, two decisions can
  # be ~20-25 s apart, and a FOREIGN window between them must still be seen.
  $foreignWindow = Get-ProgramAudioKnob "PROGRAM_AUDIO_FOREIGN_WINDOW_S" 30
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
  # >= -1 s like camera-box: a clock-sync step can make a fresh sample read slightly negative.
  if (-not $verdict -or $null -eq $j.age_s -or $j.age_s -is [bool] -or $null -eq $age -or $age -lt -1) {
    return "malformed: $url has no verdict / numeric age_s >= -1"
  }
  $line = "verdict=$verdict age_s=$age rms_dbfs=$($j.rms_dbfs) outside_band_pct=$($j.outside_band_pct) " +
    "markers_decoded=$($j.markers_decoded) marker_chain=$($j.marker_chain) " +
    "last_foreign_age_s=$($j.last_foreign_age_s) source=$($j.source)"
  # Proof of music FIRST, before anything tolerable: a FOREIGN verdict counts even when
  # stale, and camera-box's FOREIGN latch beats an UNKNOWN / stale / MEASUREMENT read
  # (as in camera-box's reference guard). Otherwise music could hide behind the
  # one-poll tolerance and air between two decisions unseen.
  if ($verdict -ceq "FOREIGN") { return "FOREIGN: program audio is not the measurement signal -- $line" }
  $foreignAge = $j.last_foreign_age_s -as [double]
  if ($null -ne $j.last_foreign_age_s -and $j.last_foreign_age_s -isnot [bool] -and $null -ne $foreignAge -and
      $foreignAge -le $foreignWindow) {
    return "FOREIGN: foreign audio ${foreignAge}s ago (window ${foreignWindow}s) -- $line"
  }
  if ($age -gt $maxAge) { return "stale: sample is ${age}s old (max ${maxAge}s) -- $line" }
  if ($script:ProgramAudioOkVerdicts -notcontains $verdict) { return "${verdict}: program audio is not the measurement signal -- $line" }
  # camera-box (dev dfccef2f8): MEASUREMENT means a QPSK marker chain >= 4 over 4 s.
  # One without `marker_chain` comes from an old sampler and is served as UNKNOWN;
  # treat it the same here in case such a sampler is ever back.
  if ($verdict -eq "MEASUREMENT" -and $null -eq $j.marker_chain) {
    return "UNKNOWN: MEASUREMENT without marker_chain (an old sampler) -- $line"
  }
  Write-Host "[program-audio] OK: $line"
  return $null
}

# Not proof of music: UNKNOWN (camera-box's first 4 s after a sampler start or an NDI
# receive gap), a stale sample, an unreachable or malformed sampler. ROZHODNUTE
# 2026-10-07: while streaming, ONE such poll is tolerated and the SECOND in a row
# stops; FOREIGN (and any unexpected verdict) stops at once.
function Test-ProgramAudioTolerable([string]$why) {
  return ($why -clike "UNKNOWN:*" -or $why -like "unreachable:*" -or $why -like "stale:*" -or $why -like "malformed:*")
}

# ::error:: plus the job summary, so the reason is visible without opening the log.
function Write-ProgramAudioError([string]$context, [string]$why) {
  Write-Host "::error::program-audio guard (#379): $context -- $why"
  if ($env:GITHUB_STEP_SUMMARY) {
    # One bullet per breach-marker line (" | "-joined): every step of the stop and the
    # delivery cut is visible in the job summary.
    $md = "### Program-audio guard (#379): $context`n`n"
    foreach ($line in ($why -split " \| ")) { $md += "- ``$line```n" }
    $md | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Encoding utf8 -Append
  }
}

# obs-stream.ps1 Set-StartedMarker mirrors OBS_STREAMING_STARTED_BY_CI here.
function Set-ProgramAudioStreamOwned([bool]$owned) {
  $p = Get-ProgramAudioPaths
  Write-ProgramAudioFile $p.Owned ($(if ($owned) { "true" } else { "false" }))
}

# Is the live OBS stream ours? Only an explicit "false" says no: with no record the
# watchdog errs on the side of stopping (music on a platform is the worse outcome).
function Test-ProgramAudioStreamOwned {
  $p = Get-ProgramAudioPaths
  return ((Read-ProgramAudioFile $p.Owned) -ne "false")
}

function Get-ProgramAudioApiBase {
  if ($env:PROGRAM_AUDIO_API_BASE) { return $env:PROGRAM_AUDIO_API_BASE.TrimEnd("/") }
  return "http://127.0.0.1:8910"
}

# The ONE call to Restreamer's stop-stream API (the #374 guard confines it here).
# Returns "" when Restreamer accepted the command (it only QUEUES it), else the error.
function Invoke-ProgramAudioStop {
  try {
    $null = Invoke-WebRequest -Uri "$(Get-ProgramAudioApiBase)/api/v1/obs/stop-stream" -Method POST -UseBasicParsing -TimeoutSec 5 -Body ""
    return ""
  } catch {
    return $_.Exception.Message
  }
}

# Restreamer's read of OBS: $true / $false, or $null when it cannot tell (Restreamer
# down, its OBS client not connected, no answer).
function Get-ProgramAudioObsStreaming {
  try {
    $s = Invoke-RestMethod -Uri "$(Get-ProgramAudioApiBase)/api/v1/obs/status" -Method GET -TimeoutSec 5
  } catch {
    return $null
  }
  if ($null -eq $s -or $s.connected -ne $true -or $s.streaming -isnot [bool]) { return $null }
  return $s.streaming
}

# Delivery instances of one event still alive, or $null when Restreamer cannot answer.
function Get-ProgramAudioEventInstances([long]$eventId) {
  try {
    # `(...) | ForEach-Object { $_ }` flattens: a JSON array can come back as ONE object.
    $all = @((Invoke-RestMethod -Uri "$(Get-ProgramAudioApiBase)/api/v1/delivery/instances" -Method GET -TimeoutSec 10) |
      ForEach-Object { $_ })
  } catch {
    return $null
  }
  # The comma keeps an EMPTY result an empty array: a function returning @() yields $null.
  return , @($all | Where-Object { $null -ne $_ -and [string]$_.event_id -eq [string]$eventId })
}

# ROZHODNUTE 2026-10-07 (#379): stopping OBS stops NEW input only; the CI event's
# 120 s cache would keep sending the music to the platform. So the breach also stops
# the CI event's delivery and deactivates it, then confirms none of its delivery
# instances is left. ONLY a CI-owned E2E event (by the job's EVENT_NAME) is ever
# touched; anything else is REFUSED. These are the ONLY delivery-stop / deactivate
# calls in this file (verify_program_audio_guard.py confines them here).
# Returns $true when finished (cut confirmed, or refused), $false to retry later.
function Invoke-ProgramAudioDeliveryCut {
  $name = [string]$env:EVENT_NAME
  if ($script:ProgramAudioCiEvents -cnotcontains $name) {
    Add-ProgramAudioBreachLine ("delivery cut REFUSED: the job's EVENT_NAME '$name' is not a CI-owned E2E event " +
      "($($script:ProgramAudioCiEvents -join ', ')) -- not touching it")
    return $true
  }
  $base = Get-ProgramAudioApiBase
  try {
    $ev = (Invoke-RestMethod -Uri "$base/api/v1/events" -Method GET -TimeoutSec 10) | ForEach-Object { $_ } |
      Where-Object { $null -ne $_ -and $_.name -ceq $name } | Select-Object -First 1
  } catch {
    Write-ProgramAudioLog "delivery cut: events unreadable: $($_.Exception.Message)"
    return $false
  }
  if ($null -eq $ev) {
    Add-ProgramAudioBreachLine "delivery cut: no event named '$name' -- nothing of ours to cut"
    return $true
  }
  $id = $ev.id -as [long]
  if ($null -eq $id -or $id -le 0) {
    Add-ProgramAudioBreachLine "delivery cut REFUSED: event '$name' has no usable id"
    return $true
  }
  try {
    $body = @{ event_id = $id } | ConvertTo-Json -Compress
    $null = Invoke-WebRequest -Uri "$base/api/v1/delivery/stop" -Method POST -UseBasicParsing -TimeoutSec 60 -Body $body -ContentType "application/json"
  } catch {
    Write-ProgramAudioLog "delivery cut: delivery stop for event $name (id $id) failed: $($_.Exception.Message)"
    return $false
  }
  Add-ProgramAudioBreachLine "delivery stop OK: event $name (id $id)"
  try {
    $null = Invoke-WebRequest -Uri "$base/api/v1/events/$id/deactivate" -Method POST -UseBasicParsing -TimeoutSec 10 -Body ""
  } catch {
    Write-ProgramAudioLog "delivery cut: deactivating event $name (id $id) failed: $($_.Exception.Message)"
    return $false
  }
  Add-ProgramAudioBreachLine "event $name (id $id) deactivated"
  $budget = Get-ProgramAudioKnob "PROGRAM_AUDIO_DELIVERY_BUDGET_S" 120
  $deadline = (Get-ProgramAudioNow) + $budget
  $left = $null
  while ($true) {
    $left = Get-ProgramAudioEventInstances $id
    if ($null -ne $left -and $left.Count -eq 0) {
      Add-ProgramAudioBreachLine "delivery CUT CONFIRMED: no delivery instance of event $name (id $id) left"
      return $true
    }
    if ((Get-ProgramAudioNow) -ge $deadline) { break }
    Write-ProgramAudioFile (Get-ProgramAudioPaths).Heartbeat "$(Get-ProgramAudioNow)"
    Start-Sleep -Seconds 5
  }
  $n = "?"
  if ($null -ne $left) { $n = $left.Count }
  Add-ProgramAudioBreachLine "delivery cut NOT confirmed: $n instance(s) of event $name (id $id) still listed after ${budget}s -- retrying"
  return $false
}

function Write-ProgramAudioLog([string]$text) {
  $p = Get-ProgramAudioPaths
  "$(Get-ProgramAudioStamp) $text" | Out-File -FilePath $p.Log -Encoding ascii -Append
}

function Add-ProgramAudioBreachLine([string]$text) {
  $p = Get-ProgramAudioPaths
  "$(Get-ProgramAudioStamp) $text" | Out-File -FilePath $p.Breach -Encoding ascii -Append
  Write-ProgramAudioLog $text
}

# Three consecutive "not streaming" reads, 1 s apart, within ~6 s. Restreamer's flag
# can read false for a moment while OBS still sends (right after its OBS client
# connects, or while OBS reconnects), so one read is not enough.
function Test-ProgramAudioObsStopped {
  $quiet = 0
  for ($i = 0; $i -lt 6; $i++) {
    Start-Sleep -Seconds 1
    if ((Get-ProgramAudioObsStreaming) -eq $false) {
      $quiet++
      if ($quiet -ge 3) { return $true }
    } else {
      $quiet = 0
    }
  }
  return $false
}

# When our OBS stream is already over but the cut is not done (e.g. Restreamer was down
# in a crash gate): keep trying, bounded by PROGRAM_AUDIO_DELIVERY_BUDGET_S, before the
# watchdog stops watching. The breach marker records how it ended.
function Invoke-ProgramAudioDeliveryCutUntilDone {
  $p = Get-ProgramAudioPaths
  $budget = Get-ProgramAudioKnob "PROGRAM_AUDIO_DELIVERY_BUDGET_S" 120
  $deadline = (Get-ProgramAudioNow) + $budget
  while (-not (Invoke-ProgramAudioDeliveryCut)) {
    if ((Get-ProgramAudioNow) -ge $deadline) {
      Add-ProgramAudioBreachLine "delivery cut GAVE UP after ${budget}s once our stream was over -- the job's teardown stops the delivery"
      return
    }
    Write-ProgramAudioFile $p.Heartbeat "$(Get-ProgramAudioNow)"
    Start-Sleep -Seconds 5
  }
}

# After a breach: ask for the stop until Restreamer CONFIRMS OBS stopped, then KEEP
# watching while the stream is ours: a lost StopStream or an OBS reconnect can put it
# back on air, and then the stop is re-issued. Never gives up while our stream is
# ours; the heartbeat keeps proving the watchdog is alive, the teardown ends it.
# Deliberate #374 exception: until obs-stream.ps1 marks our stream over, a session
# camera-box starts meanwhile is stopped too (Restreamer's status cannot tell it from
# our reconnect, and the CI event may still be delivering to a platform).
function Invoke-ProgramAudioBreachStop {
  $p = Get-ProgramAudioPaths
  $poll = Get-ProgramAudioKnob "PROGRAM_AUDIO_POLL_S" 10
  $attempt = 0
  $confirmed = $false
  $cutDone = $false
  while ($true) {
    Write-ProgramAudioFile $p.Heartbeat "$(Get-ProgramAudioNow)"
    if (-not (Test-ProgramAudioStreamOwned)) {
      Add-ProgramAudioBreachLine "our stream already ended (marker false) -- not stopping a session that is not ours"
      # The CI event's cached delivery is still ours (by name) and still sends the music.
      if (-not $cutDone) { Invoke-ProgramAudioDeliveryCutUntilDone }
      return
    }
    $attempt++
    $err = Invoke-ProgramAudioStop
    if ($err) { Write-ProgramAudioLog "stop-stream attempt $attempt failed: $err" }
    $stopped = Test-ProgramAudioObsStopped
    if ($stopped) {
      $word = "CONFIRMED"
      if ($confirmed) { $word = "re-CONFIRMED" }
      Add-ProgramAudioBreachLine "stop ${word}: Restreamer reports OBS not streaming (stop-stream attempt $attempt)"
      $confirmed = $true
    }
    # Normally right after the confirmed OBS stop; but an unconfirmed stop must not keep
    # the cached music going out either, so it runs on every round until it is done.
    if (-not $cutDone) { $cutDone = Invoke-ProgramAudioDeliveryCut }
    if ($stopped) {
      # Watch until our stream is over; back to the stop loop if OBS streams again.
      while ($true) {
        Write-ProgramAudioFile $p.Heartbeat "$(Get-ProgramAudioNow)"
        if (-not (Test-ProgramAudioStreamOwned)) {
          if (-not $cutDone) { Invoke-ProgramAudioDeliveryCutUntilDone }
          Write-ProgramAudioLog "our stream is over (marker false) -- watch ended"
          return
        }
        if (-not $cutDone) { $cutDone = Invoke-ProgramAudioDeliveryCut }
        Start-Sleep -Seconds $poll
        if ((Get-ProgramAudioObsStreaming) -eq $true) {
          Add-ProgramAudioBreachLine "OBS is streaming AGAIN after the confirmed stop -- re-issuing the stop"
          break
        }
      }
      continue
    }
    if ($attempt -eq 1 -or $attempt % 10 -eq 0) {
      Add-ProgramAudioBreachLine "stop NOT confirmed yet after $attempt attempt(s) -- still asking"
    }
    Start-Sleep -Seconds ([Math]::Min(5, $poll))
  }
}

# The detached watchdog's body (program-audio-guard.ps1 -RunWatchdog).
function Invoke-ProgramAudioWatchdogLoop {
  $ErrorActionPreference = "Stop"
  $p = Get-ProgramAudioPaths
  $poll = Get-ProgramAudioKnob "PROGRAM_AUDIO_POLL_S" 10
  Write-ProgramAudioLog "watchdog up (pid $PID, poll ${poll}s, $(Get-ProgramAudioUrl))"
  $tolerated = $null
  # ROZHODNUTE 2026-10-07 (3rd): 3+ tolerated non-OK polls within ANY flap window
  # (60 s) are a breach even when never two in a row -- a flapping classifier fails closed.
  $flapWindow = Get-ProgramAudioKnob "PROGRAM_AUDIO_FLAP_WINDOW_S" 60
  $flaps = @()
  while ($true) {
    Write-ProgramAudioFile $p.Heartbeat "$(Get-ProgramAudioNow)"
    $why = Read-ProgramAudioVerdict
    $tolerable = [bool]($why -and (Test-ProgramAudioTolerable $why))
    if ($tolerable) {
      $now = Get-ProgramAudioNow
      $flaps = @($flaps | Where-Object { $_ -ge $now - $flapWindow }) + $now
      if ($flaps.Count -ge 3) {
        $why = "$why ($($flaps.Count) tolerated non-OK polls within ${flapWindow}s: a flapping classifier)"
        $tolerable = $false      # no further tolerance: this poll is a breach
        $tolerated = $null
      }
    }
    if ($tolerable -and $null -eq $tolerated) {
      # The first non-OK poll that is not proof of music: tolerated; the next decides.
      $tolerated = $why
      Write-ProgramAudioLog "tolerating ONE poll (the next non-OK one stops): $why"
      Start-Sleep -Seconds $poll
      continue
    }
    if ($tolerable -and $null -ne $tolerated) {
      $why = "$why (second non-OK poll in a row; the first: $tolerated)"
    }
    $tolerated = $null
    if ($why -and -not (Test-ProgramAudioStreamOwned)) {
      # Not our stream (a republish gap, a refused restart, after our stop): nothing of
      # ours is on a platform, and a session that is not ours is never stopped. The
      # next start re-checks the verdict itself.
      Write-ProgramAudioLog "not our stream, nothing to stop: $why"
    } elseif ($why) {
      # Marker first: whatever happens to the stop, the job fails on it.
      Add-ProgramAudioBreachLine "BREACH: $why"
      Invoke-ProgramAudioBreachStop
      return
    } else {
      Write-ProgramAudioLog "ok"
    }
    Start-Sleep -Seconds $poll
  }
}

# The live watchdog process recorded in the pid file, or $null (gone / pid reused).
function Get-ProgramAudioWatchdogProcess {
  $p = Get-ProgramAudioPaths
  $text = Read-ProgramAudioFile $p.Pid
  if (-not $text) { return $null }
  $rec = $text -split " ", 2
  # "" -as [int] is 0, and pid 0 exists on Windows (Idle): require a real pid.
  $wdId = $rec[0] -as [int]
  if ($null -eq $wdId -or $wdId -le 0) { return $null }
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
  $text = Read-ProgramAudioFile $p.Heartbeat
  if (-not $text) { return $null }   # "" -as [double] would be 0, i.e. "beat in 1970"
  $beat = $text -as [double]
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
  Write-ProgramAudioFile $p.Pid "$($wd.Id) $($wd.StartTime.ToUniversalTime().Ticks)"
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
