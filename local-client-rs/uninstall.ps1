# Restreamer Uninstaller
# Usage: irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/local-client-rs/uninstall.ps1 | iex

$ErrorActionPreference = "Stop"

$InstallDir = "C:\Program Files\Restreamer"
$ConfigDir = "C:\ProgramData\Restreamer"
$AppName = "Restreamer"
$TaskName = "RestreamerGUI"

function Write-Status($msg) { Write-Host "  [*] $msg" -ForegroundColor Cyan }
function Write-Ok($msg) { Write-Host "  [+] $msg" -ForegroundColor Green }

# --- Self-elevate ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    $script = "irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/local-client-rs/uninstall.ps1 | iex"
    Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -Command `"$script`"" -Verb RunAs
    exit
}

Write-Host ""
Write-Host "  Restreamer Uninstaller" -ForegroundColor Yellow
Write-Host "  ======================" -ForegroundColor Yellow
Write-Host ""

# --- Stop app ---
Write-Status "Stopping Restreamer..."
Get-Process -Name $AppName -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Ok "Restreamer stopped"

# --- Remove scheduled task ---
Write-Status "Removing scheduled task..."
Get-ScheduledTask | Where-Object { $_.TaskName -like "*estreamer*" } | ForEach-Object {
    Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Ok "Removed task: $($_.TaskName)"
}

# --- Remove legacy Windows service if it exists ---
Write-Status "Removing legacy Windows service..."
foreach ($name in @("RestreamerService", "restreamer-service")) {
    $svc = Get-Service -Name $name -ErrorAction SilentlyContinue
    if ($svc) {
        if ($svc.Status -eq "Running") {
            Stop-Service -Name $name -Force
        }
        sc.exe delete $name | Out-Null
        Write-Ok "Removed legacy service: $name"
    }
}

# --- Remove install directory ---
Write-Status "Removing install directory..."
if (Test-Path $InstallDir) {
    Remove-Item -Path $InstallDir -Recurse -Force
    Write-Ok "Removed $InstallDir"
}

# --- Remove Tauri app via uninstaller ---
$uninstaller = "$env:LOCALAPPDATA\$AppName\uninstall.exe"
if (Test-Path $uninstaller) {
    Write-Status "Running Tauri uninstaller..."
    Start-Process -FilePath $uninstaller -ArgumentList "/S" -Wait
    Write-Ok "Tauri app uninstalled"
}

# --- Config is preserved ---
Write-Host ""
Write-Host "  Uninstall complete!" -ForegroundColor Green
Write-Host "  Config preserved at: $ConfigDir" -ForegroundColor Yellow
Write-Host "  To remove config: Remove-Item -Recurse '$ConfigDir'" -ForegroundColor Yellow
Write-Host ""
