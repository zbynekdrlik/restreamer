# obs-readiness-check.ps1 -- #374: READ-ONLY pre-flight of stream OBS.
#
# Stream OBS (10.77.9.204) is camera-box's development target. Owner directive
# 2026-08-30: restreamer CI may only start and stop OBS STREAMING. Instead of
# "fixing" OBS, CI checks that camera-box's TEST mode left it usable and FAILS with
# the reason otherwise. The OBS jobs run this early (fail fast, before a VPS boots);
# `obs-stream.ps1 -Action Start` runs the same checks again in the session that
# starts the stream.
#
# Ready means:
#   - exactly one obs64 process;
#   - the obs-websocket answers and accepts us;
#   - the program scene is the TEST scene (camera-box's `Development`, issue 1380;
#     `PRO` is the owner's production scene and is never accepted);
#   - the stream service points at the restreamer inpoint (rtmp://<this box>:1234/live);
#   - OBS is not streaming already and not recording.
#
# Exit 0 = ready. Exit 1 = not ready, with one ::error:: line naming what is wrong.

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\obs-ws.ps1"

$why = Test-ObsProcess
if ($why) { Write-NotReady $why; exit 1 }

try {
  Connect-Obs
} catch {
  Write-NotReady "websocket $($_.Exception.Message)"
  exit 1
}
try {
  $why = Test-ObsReady
} catch {
  $why = "read failed: $($_.Exception.Message)"
} finally {
  Close-Obs
}
if ($why) { Write-NotReady $why; exit 1 }

Write-Host "stream OBS ready: one obs64, scene '$($script:ObsTestScene)', inpoint service, not streaming, not recording"
exit 0
