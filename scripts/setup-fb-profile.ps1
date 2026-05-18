# One-time FB Live Producer profile setup.
#
# Usage on stream.lan (via MCP win-stream-snv Shell):
#   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
#
# Launches a HEADED Chromium with the persistent profile at
# C:\Users\newlevel\.playwright-fb-profile. Operator signs into Facebook
# manually using the dedicated test-account. When the operator closes the
# browser, the Facebook session cookies are persisted to that profile
# directory and reused by the CI Playwright spec in headless mode.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -Path (Join-Path $RepoRoot "e2e")

$env:HEADED = "1"
$env:FB_BROADCAST_URL = "https://www.facebook.com/live/producer"

Write-Host "Launching HEADED Playwright with FB profile."
Write-Host "Sign into Facebook in the opened browser window using the dedicated test account."
Write-Host "After signing in, close the browser window to save the session."

# Set FB_SOAK_MINUTES = 0 to make the spec exit immediately after login.
$env:FB_SOAK_MINUTES = "0"
$env:FB_POLL_INTERVAL_MS = "1000"

npx playwright test -c playwright-facebook.config.ts
if ($LASTEXITCODE -ne 0) {
  # A non-zero exit is expected on the very first run (the spec asserts
  # health signals that won't exist until the operator is logged in and
  # streaming). The important side effect is the saved session. Print a
  # reminder and continue.
  Write-Host "Playwright exited non-zero (expected on first setup run)."
}

Write-Host "Profile saved to C:\Users\newlevel\.playwright-fb-profile"
Write-Host "Next: create the FB test broadcast in Live Producer (scheduled / unpublished)"
Write-Host "      and set FB_TEST_STREAM_KEY GitHub secret to the persistent key."
