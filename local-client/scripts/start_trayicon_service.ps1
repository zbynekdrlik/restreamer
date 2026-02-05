
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Definition

$logDirectory = $PSScriptRoot + "\services_logs"
$logFile = $logDirectory + "\tray_icon.log"


Set-Location -Path (Join-Path $scriptDirectory "..\..")
# Activate the virtual environment (replace 'your_venv_name' with the actual name of your virtual environment)
. .\venv\Scripts\Activate

# Navigate to the 'server' directory
Set-Location -Path '.\local-client'


python manage.py trayicon_service > $logFile


pause