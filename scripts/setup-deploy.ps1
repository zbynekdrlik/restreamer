# Setup SSH for GitHub Actions deployment to stream.lan
# Run this once on stream.lan to enable auto-deployment from dev branch
#
# Usage (run as Administrator on stream.lan):
#   irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/dev/scripts/setup-deploy.ps1 | iex

$ErrorActionPreference = "Stop"

# Self-elevate if needed
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Requesting administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -Command `"irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/dev/scripts/setup-deploy.ps1 | iex`"" -Verb RunAs
    exit
}

Write-Host ""
Write-Host "  GitHub Actions SSH Deployment Setup" -ForegroundColor Cyan
Write-Host "  ====================================" -ForegroundColor Cyan
Write-Host ""

# The public key for GitHub Actions deployment
$PublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJblm4fanNHqBJZFh5naj+KYkVx2drBHxd7iLZIvIAmo github-actions-deploy@restreamer"

# Enable OpenSSH Server if not already
Write-Host "[1/4] Checking OpenSSH Server..." -ForegroundColor Yellow
$sshd = Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH.Server*'
if ($sshd.State -ne 'Installed') {
    Write-Host "  Installing OpenSSH Server..." -ForegroundColor Gray
    Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
    Write-Host "  OK: OpenSSH Server installed" -ForegroundColor Green
} else {
    Write-Host "  OK: OpenSSH Server already installed" -ForegroundColor Green
}

# Start and enable sshd service
Write-Host "[2/4] Configuring sshd service..." -ForegroundColor Yellow
Start-Service sshd -ErrorAction SilentlyContinue
Set-Service -Name sshd -StartupType Automatic
Write-Host "  OK: sshd service running and set to auto-start" -ForegroundColor Green

# Configure firewall
Write-Host "[3/4] Configuring firewall..." -ForegroundColor Yellow
$rule = Get-NetFirewallRule -Name "OpenSSH-Server-In-TCP" -ErrorAction SilentlyContinue
if (-not $rule) {
    New-NetFirewallRule -Name "OpenSSH-Server-In-TCP" -DisplayName "OpenSSH Server (sshd)" -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 | Out-Null
    Write-Host "  OK: Firewall rule created" -ForegroundColor Green
} else {
    Write-Host "  OK: Firewall rule already exists" -ForegroundColor Green
}

# Add public key to authorized_keys
Write-Host "[4/4] Adding deployment key to authorized_keys..." -ForegroundColor Yellow
$sshDir = "$env:USERPROFILE\.ssh"
$authKeys = "$sshDir\authorized_keys"

New-Item -ItemType Directory -Path $sshDir -Force | Out-Null

# Check if key already exists
$existingKeys = ""
if (Test-Path $authKeys) {
    $existingKeys = Get-Content $authKeys -Raw
}

if ($existingKeys -notlike "*github-actions-deploy@restreamer*") {
    Add-Content -Path $authKeys -Value $PublicKey
    Write-Host "  OK: Deployment key added" -ForegroundColor Green
} else {
    Write-Host "  OK: Deployment key already present" -ForegroundColor Green
}

# Set proper permissions on authorized_keys
icacls $authKeys /inheritance:r /grant "${env:USERNAME}:F" /grant "SYSTEM:F" | Out-Null

# For administrators, also need to add to administrators_authorized_keys
$adminAuthKeys = "$env:ProgramData\ssh\administrators_authorized_keys"
if ((Get-LocalGroupMember -Group "Administrators" -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "*$env:USERNAME" })) {
    Write-Host "  Adding key to administrators_authorized_keys..." -ForegroundColor Gray
    if (-not (Test-Path $adminAuthKeys) -or ((Get-Content $adminAuthKeys -Raw -ErrorAction SilentlyContinue) -notlike "*github-actions-deploy@restreamer*")) {
        Add-Content -Path $adminAuthKeys -Value $PublicKey -Force
        icacls $adminAuthKeys /inheritance:r /grant "Administrators:F" /grant "SYSTEM:F" | Out-Null
        Write-Host "  OK: Admin key added" -ForegroundColor Green
    }
}

# Create temp directory for deployments
$tmpDir = "$env:USERPROFILE\tmp"
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
Write-Host "  OK: Created $tmpDir for deployment artifacts" -ForegroundColor Green

Write-Host ""
Write-Host "  Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  GitHub Actions can now deploy to this machine via SSH." -ForegroundColor White
Write-Host "  Every push to 'dev' branch will auto-deploy here for testing." -ForegroundColor White
Write-Host ""
Write-Host "  Test SSH connection from another machine:" -ForegroundColor Yellow
Write-Host "    ssh $env:USERNAME@stream.lan whoami" -ForegroundColor Gray
Write-Host ""
