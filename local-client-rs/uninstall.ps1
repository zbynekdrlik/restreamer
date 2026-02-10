# Restreamer Local Client Uninstaller
# Usage: irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/local-client-rs/uninstall.ps1 | iex

$ErrorActionPreference = "Stop"

$ServiceName = "restreamer-service"
$InstallDir = "C:\Program Files\Restreamer"
$ConfigDir = "C:\ProgramData\Restreamer"
$TrayAppName = "Restreamer"

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
Write-Host "  Restreamer Local Client Uninstaller" -ForegroundColor Yellow
Write-Host "  ====================================" -ForegroundColor Yellow
Write-Host ""

# --- Stop and remove service ---
Write-Status "Stopping service..."
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
    if ($svc.Status -eq "Running") {
        Stop-Service -Name $ServiceName -Force
        Write-Ok "Service stopped"
    }
    sc.exe delete $ServiceName | Out-Null
    Write-Ok "Service removed"
} else {
    Write-Ok "Service not found (already removed)"
}

# --- Kill tray app ---
Write-Status "Stopping tray app..."
Get-Process -Name $TrayAppName -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Ok "Tray app stopped"

# --- Remove service binary ---
Write-Status "Removing service binary..."
if (Test-Path $InstallDir) {
    Remove-Item -Path $InstallDir -Recurse -Force
    Write-Ok "Removed $InstallDir"
}

# --- Remove Tauri app via uninstaller ---
$uninstaller = "$env:LOCALAPPDATA\$TrayAppName\uninstall.exe"
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
