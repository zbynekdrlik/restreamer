# Restreamer Installer
# Usage: irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/scripts/install.ps1 | iex

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$InstallDir = "C:\Program Files\Restreamer"
$ConfigDir = "C:\ProgramData\Restreamer"
$ConfigFile = "$ConfigDir\config.json"
$GithubRepo = "zbynekdrlik/restreamer"
$AppName = "Restreamer"
$TaskName = "RestreamerGUI"

function Write-Status($msg) { Write-Host "  [*] $msg" -ForegroundColor Cyan }
function Write-Ok($msg) { Write-Host "  [+] $msg" -ForegroundColor Green }
function Write-Err($msg) { Write-Host "  [-] $msg" -ForegroundColor Red }

# --- Self-elevate ---
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Status "Requesting administrator privileges..."
    # Save current script to temp to avoid re-downloading (TOCTOU safety)
    $tempScript = "$env:TEMP\restreamer-install.ps1"
    $MyInvocation.MyCommand.ScriptBlock.ToString() | Set-Content -Path $tempScript -Encoding UTF8
    Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$tempScript`"" -Verb RunAs
    exit
}

Write-Host ""
Write-Host "  Restreamer Installer" -ForegroundColor Yellow
Write-Host "  ====================" -ForegroundColor Yellow
Write-Host ""

# --- Fetch latest release ---
Write-Status "Fetching latest release from GitHub..."
$releaseUrl = "https://api.github.com/repos/$GithubRepo/releases"
$releases = Invoke-RestMethod -Uri $releaseUrl -Headers @{ "User-Agent" = "Restreamer-Installer" }
$latestRelease = $releases | Where-Object { $_.tag_name -like "restreamer-v*" } | Select-Object -First 1

if (-not $latestRelease) {
    Write-Err "No release found with tag 'restreamer-v*'"
    exit 1
}

$version = $latestRelease.tag_name -replace "restreamer-v", ""
Write-Ok "Latest version: $version"

# --- Stop existing app ---
Write-Status "Stopping existing Restreamer..."
Get-Process -Name $AppName -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

# NOTE: We intentionally do NOT delete restreamer.db-wal / restreamer.db-shm.
# In WAL journaling mode these files hold transactions that have not yet been
# checkpointed into the main .db. Deleting them between a forced kill and the
# next process start permanently destroys those transactions (e.g. OAuth grants
# created seconds before redeploy). SQLite re-opens and replays the WAL safely.

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
# Clean up old service binary
Remove-Item "$InstallDir\restreamer-service.exe" -Force -ErrorAction SilentlyContinue

# --- Create directories ---
Write-Status "Creating directories..."
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
New-Item -ItemType Directory -Path "$ConfigDir\chunks" -Force | Out-Null

# --- Ensure WebView2 runtime is installed ---
Write-Status "Checking WebView2 runtime..."
$wv2Found = $false
foreach ($guid in @("{F3017226-FE2A-4295-8BEF-AE82F87EC1B0}", "{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}")) {
    $reg = Get-ItemProperty -Path "HKLM:\SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\$guid" -ErrorAction SilentlyContinue
    if ($reg -and $reg.pv) { $wv2Found = $true; Write-Ok "WebView2 already installed: version $($reg.pv)"; break }
}
if (-not $wv2Found -and (Test-Path "C:\Program Files (x86)\Microsoft\EdgeWebView\Application")) {
    $wv2Found = $true; Write-Ok "WebView2 already installed (detected from filesystem)"
}
if (-not $wv2Found) {
    Write-Status "Installing WebView2 Evergreen Runtime..."
    $bootstrapper = "$env:TEMP\MicrosoftEdgeWebview2Setup.exe"
    Invoke-WebRequest -Uri "https://go.microsoft.com/fwlink/p/?LinkId=2124703" -OutFile $bootstrapper
    Start-Process -FilePath $bootstrapper -ArgumentList "/silent /install" -Wait
    Remove-Item $bootstrapper -ErrorAction SilentlyContinue
    if (Test-Path "C:\Program Files (x86)\Microsoft\EdgeWebView\Application") {
        Write-Ok "WebView2 installed successfully"
    } else {
        Write-Err "WebView2 installation failed - dashboard may not render"
    }
}

# --- Download and run Tauri NSIS installer ---
$tauriAsset = $latestRelease.assets | Where-Object { $_.name -like "*setup*.exe" -or $_.name -like "*Setup*.exe" } | Select-Object -First 1
if ($tauriAsset) {
    Write-Status "Downloading Tauri installer..."
    $tauriInstallerPath = "$env:TEMP\Restreamer-Setup.exe"
    Invoke-WebRequest -Uri $tauriAsset.browser_download_url -OutFile $tauriInstallerPath
    Write-Ok "Downloaded: $($tauriAsset.name)"

    Write-Status "Running Tauri installer (silent)..."
    Start-Process -FilePath $tauriInstallerPath -ArgumentList "/S" -Wait
    Write-Ok "Tauri app installed"
} else {
    Write-Err "Tauri installer not found in release assets"
    exit 1
}

# --- Create/preserve config ---
if (-not (Test-Path $ConfigFile)) {
    Write-Status "Creating default config..."
    $defaultConfig = @{
        client_uuid  = [guid]::NewGuid().ToString()
        s3           = @{
            bucket            = "restreamer-chunks"
            region            = "eu-central-1"
            endpoint          = "https://fsn1.your-objectstorage.com"
            access_key_id     = ""
            secret_access_key = ""
        }
        hetzner      = @{
            api_token          = ""
            location           = "fsn1"
            default_server_type = "cx23"
            snapshot_label     = "rs-delivery"
            ssh_key_name       = "restreamer"
        }
        youtube      = @{
            client_id     = ""
            client_secret = ""
        }
        inpoint      = @{
            rtmp_port         = 1234
            chunk_duration_ms = 1000
            read_buffer_bytes = 102400
        }
        api          = @{
            port = 8910
            bind = "127.0.0.1"
        }
    }
    $defaultConfig | ConvertTo-Json -Depth 3 | Set-Content -Path $ConfigFile -Encoding UTF8
    Write-Ok "Config created at $ConfigFile"
    Write-Host ""
    Write-Host "  IMPORTANT: Edit $ConfigFile to configure:" -ForegroundColor Yellow
    Write-Host "    - S3 credentials (s3.access_key_id, s3.secret_access_key)" -ForegroundColor Yellow
    Write-Host "    - Hetzner API token (hetzner.api_token) for delivery functionality" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Ok "Existing config preserved at $ConfigFile"
}

# --- Setup scheduled task for auto-start ---
Write-Status "Setting up auto-start..."
$exePath = "$InstallDir\$AppName.exe"

# Remove old scheduled tasks
Get-ScheduledTask | Where-Object { $_.TaskName -like "*estreamer*" } | ForEach-Object {
    Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false -ErrorAction SilentlyContinue
}

# Create scheduled task for auto-start at login
$action = New-ScheduledTaskAction -Execute $exePath -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -AtLogon
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit 0

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
Write-Ok "Scheduled task registered"

# --- Start the app ---
Write-Status "Starting Restreamer via scheduled task..."
# Use schtasks.exe for cross-context reliability
schtasks.exe /run /tn $TaskName
Start-Sleep -Seconds 10

# --- Verify deployment ---
Write-Status "Verifying deployment..."

$proc = Get-Process -Name $AppName -ErrorAction SilentlyContinue
if (-not $proc) {
    Write-Err "Restreamer process not running"
    Write-Status "Restreamer will start on next login"
} else {
    Write-Ok "Process running (PID: $($proc.Id), Session: $($proc.SessionId))"

    # Check port is listening
    $listener = netstat -ano | Select-String "LISTENING" | Select-String ":8910"
    if ($listener) {
        Write-Ok "Port 8910 is listening"

        # Check API health
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/health" -TimeoutSec 5
            Write-Ok "API health check passed"
        } catch {
            Write-Err "API health check failed - ServiceCore may not have started correctly"
        }
    } else {
        Write-Err "Port 8910 not listening - ServiceCore may not have started correctly"
    }
}

Write-Host ""
Write-Host "  Installation complete! v$version" -ForegroundColor Green
Write-Host "  App:     $InstallDir\$AppName.exe" -ForegroundColor Green
Write-Host "  Config:  $ConfigFile" -ForegroundColor Green
Write-Host "  API:     http://127.0.0.1:8910/api/v1/health" -ForegroundColor Green
Write-Host ""
