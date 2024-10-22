# Navigate to the first location
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Definition

Set-Location -Path (Join-Path $scriptDirectory "..\..")

# Activate the virtual environment (replace 'your_venv_name' with the actual name of your virtual environment)
. .\venv\Scripts\Activate

# Navigate to the 'server' directory
Set-Location -Path '.\restreamer-local-client'

# Run the Django development server
python manage.py inpoint_service

Read-Host "Press Enter to exit"

pause