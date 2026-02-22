# Setup GitHub Actions Self-Hosted Runner for stream.lan
# This enables auto-deployment from dev branch pushes
#
# Prerequisites:
# 1. Generate a runner token from: https://github.com/zbynekdrlik/restreamer/settings/actions/runners/new
# 2. Run this script as Administrator
#
# Usage:
#   .\setup-runner.ps1 -Token "AXXXXXXXXXXXXXXX"

param(
    [Parameter(Mandatory=$true)]
    [string]$Token,

    [string]$RunnerName = "stream-lan",
    [string]$InstallDir = "C:\actions-runner"
)

$ErrorActionPreference = "Stop"

# Self-elevate if needed
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Requesting administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Token `"$Token`" -RunnerName `"$RunnerName`" -InstallDir `"$InstallDir`"" -Verb RunAs
    exit
}

Write-Host ""
Write-Host "  GitHub Actions Runner Setup for stream.lan" -ForegroundColor Cyan
Write-Host "  ===========================================" -ForegroundColor Cyan
Write-Host ""

$GithubRepo = "zbynekdrlik/restreamer"
$RunnerVersion = "2.321.0"  # Update as needed
$RunnerZip = "actions-runner-win-x64-$RunnerVersion.zip"
$RunnerUrl = "https://github.com/actions/runner/releases/download/v$RunnerVersion/$RunnerZip"

# Create install directory
Write-Host "[1/6] Creating install directory..." -ForegroundColor Yellow
if (Test-Path $InstallDir) {
    Write-Host "  Directory exists, checking for existing runner..." -ForegroundColor Gray
    if (Test-Path "$InstallDir\.runner") {
        Write-Host "  Runner already configured. To reconfigure, run:" -ForegroundColor Yellow
        Write-Host "    cd $InstallDir && .\config.cmd remove" -ForegroundColor White
        exit 1
    }
} else {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}
Write-Host "  OK: $InstallDir" -ForegroundColor Green

# Download runner
Write-Host "[2/6] Downloading GitHub Actions Runner v$RunnerVersion..." -ForegroundColor Yellow
$zipPath = "$InstallDir\$RunnerZip"
if (-not (Test-Path $zipPath)) {
    Invoke-WebRequest -Uri $RunnerUrl -OutFile $zipPath
    Write-Host "  OK: Downloaded $RunnerZip" -ForegroundColor Green
} else {
    Write-Host "  OK: Already downloaded" -ForegroundColor Green
}

# Extract runner
Write-Host "[3/6] Extracting runner..." -ForegroundColor Yellow
if (-not (Test-Path "$InstallDir\config.cmd")) {
    Expand-Archive -Path $zipPath -DestinationPath $InstallDir -Force
    Write-Host "  OK: Extracted" -ForegroundColor Green
} else {
    Write-Host "  OK: Already extracted" -ForegroundColor Green
}

# Configure runner
Write-Host "[4/6] Configuring runner..." -ForegroundColor Yellow
Push-Location $InstallDir
try {
    & .\config.cmd --url "https://github.com/$GithubRepo" `
                   --token $Token `
                   --name $RunnerName `
                   --labels "self-hosted,windows,stream-lan" `
                   --work "_work" `
                   --runasservice `
                   --windowslogonaccount "NT AUTHORITY\SYSTEM"

    if ($LASTEXITCODE -ne 0) {
        throw "Runner configuration failed"
    }
    Write-Host "  OK: Runner configured" -ForegroundColor Green
} finally {
    Pop-Location
}

# Verify service
Write-Host "[5/6] Verifying runner service..." -ForegroundColor Yellow
Start-Sleep -Seconds 2
$svc = Get-Service -Name "actions.runner.*" -ErrorAction SilentlyContinue | Select-Object -First 1
if ($svc) {
    Write-Host "  Service: $($svc.Name)" -ForegroundColor Cyan
    Write-Host "  Status:  $($svc.Status)" -ForegroundColor $(if ($svc.Status -eq "Running") { "Green" } else { "Yellow" })

    if ($svc.Status -ne "Running") {
        Write-Host "  Starting service..." -ForegroundColor Yellow
        Start-Service -Name $svc.Name
        Start-Sleep -Seconds 2
        $svc = Get-Service -Name $svc.Name
        Write-Host "  Status:  $($svc.Status)" -ForegroundColor $(if ($svc.Status -eq "Running") { "Green" } else { "Red" })
    }
} else {
    Write-Warning "Runner service not found"
}

# Summary
Write-Host ""
Write-Host "[6/6] Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  Runner Name:   $RunnerName" -ForegroundColor Cyan
Write-Host "  Labels:        self-hosted, windows, stream-lan" -ForegroundColor Cyan
Write-Host "  Install Path:  $InstallDir" -ForegroundColor Cyan
Write-Host ""
Write-Host "  The runner will now automatically:" -ForegroundColor White
Write-Host "    - Pick up jobs from: $GithubRepo" -ForegroundColor White
Write-Host "    - Deploy dev builds to this machine" -ForegroundColor White
Write-Host ""
Write-Host "  View runner status at:" -ForegroundColor Yellow
Write-Host "    https://github.com/$GithubRepo/settings/actions/runners" -ForegroundColor Yellow
Write-Host ""
