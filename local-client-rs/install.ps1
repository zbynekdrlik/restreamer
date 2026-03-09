# Restreamer Local Client Installer
# Usage: irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/local-client-rs/install.ps1 | iex

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$ServiceName = "restreamer-service"
$InstallDir = "C:\Program Files\Restreamer"
$ConfigDir = "C:\ProgramData\Restreamer"
$ConfigFile = "$ConfigDir\config.json"
$GithubRepo = "zbynekdrlik/restreamer"
$TrayAppName = "Restreamer"

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
Write-Host "  Restreamer Local Client Installer" -ForegroundColor Yellow
Write-Host "  ==================================" -ForegroundColor Yellow
Write-Host ""

# --- Fetch latest release ---
Write-Status "Fetching latest release from GitHub..."
$releaseUrl = "https://api.github.com/repos/$GithubRepo/releases"
$releases = Invoke-RestMethod -Uri $releaseUrl -Headers @{ "User-Agent" = "Restreamer-Installer" }
$latestRelease = $releases | Where-Object { $_.tag_name -like "local-client-rs-v*" } | Select-Object -First 1

if (-not $latestRelease) {
    Write-Err "No release found with tag 'local-client-rs-v*'"
    exit 1
}

$version = $latestRelease.tag_name -replace "local-client-rs-v", ""
Write-Ok "Latest version: $version"

# --- Stop existing service and tray ---
Write-Status "Stopping existing service and tray app..."
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -eq "Running") {
    Stop-Service -Name $ServiceName -Force
    Write-Ok "Service stopped"
}
Get-Process -Name $TrayAppName -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

# --- Create directories ---
Write-Status "Creating directories..."
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
New-Item -ItemType Directory -Path "$ConfigDir\chunks" -Force | Out-Null

# --- Download service binary ---
$serviceAsset = $latestRelease.assets | Where-Object { $_.name -like "restreamer-service-*-windows-x64.exe" } | Select-Object -First 1
$checksumAsset = $latestRelease.assets | Where-Object { $_.name -eq "SHA256SUMS.txt" } | Select-Object -First 1
if ($serviceAsset) {
    Write-Status "Downloading service binary..."
    $servicePath = "$InstallDir\restreamer-service.exe"
    Invoke-WebRequest -Uri $serviceAsset.browser_download_url -OutFile $servicePath
    Write-Ok "Downloaded: $($serviceAsset.name)"

    # Verify checksum if available
    if ($checksumAsset) {
        Write-Status "Verifying checksum..."
        $checksums = (Invoke-WebRequest -Uri $checksumAsset.browser_download_url).Content
        $expectedHash = ($checksums -split "`n" | Where-Object { $_ -match $serviceAsset.name } | ForEach-Object { ($_ -split '\s+')[0] })
        if ($expectedHash) {
            $actualHash = (Get-FileHash -Path $servicePath -Algorithm SHA256).Hash.ToLower()
            if ($actualHash -ne $expectedHash.ToLower()) {
                Write-Err "Checksum mismatch for $($serviceAsset.name)! Expected: $expectedHash, Got: $actualHash"
                exit 1
            }
            Write-Ok "Checksum verified"
        }
    }
} else {
    Write-Err "Service binary not found in release assets"
    exit 1
}

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
            default_server_type = "cx22"
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
    Write-Host "  IMPORTANT: Edit $ConfigFile to set your S3 credentials" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Ok "Existing config preserved at $ConfigFile"
}

# --- Register Windows service ---
Write-Status "Registering Windows service..."
$existingSvc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingSvc) {
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
}
sc.exe create $ServiceName binPath= "`"$InstallDir\restreamer-service.exe`" `"$ConfigFile`"" start= auto DisplayName= "Restreamer Service" | Out-Null
sc.exe description $ServiceName "Restreamer local client service - RTMP capture, chunk upload, manager sync" | Out-Null
Write-Ok "Service registered"

# --- Start service ---
Write-Status "Starting service..."
Start-Service -Name $ServiceName
Write-Ok "Service started"

# --- Launch tray app ---
Write-Status "Launching tray app..."
$trayPath = "$env:LOCALAPPDATA\$TrayAppName\$TrayAppName.exe"
if (Test-Path $trayPath) {
    Start-Process -FilePath $trayPath
    Write-Ok "Tray app launched"
} else {
    Write-Status "Tray app not found at $trayPath — it may use a different install path"
}

Write-Host ""
Write-Host "  Installation complete! v$version" -ForegroundColor Green
Write-Host "  Service: $ServiceName (running)" -ForegroundColor Green
Write-Host "  Config:  $ConfigFile" -ForegroundColor Green
Write-Host "  API:     http://127.0.0.1:8910/api/v1/health" -ForegroundColor Green
Write-Host ""
