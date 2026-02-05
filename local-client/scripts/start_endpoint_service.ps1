
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Definition

Set-Location -Path (Join-Path $scriptDirectory "..\..")
# Activate the virtual environment (replace 'your_venv_name' with the actual name of your virtual environment)
. .\venv\Scripts\Activate

# Navigate to the 'server' directory
Set-Location -Path '.\local_client'

# Run the Django development server
python manage.py endpoint_service

pause