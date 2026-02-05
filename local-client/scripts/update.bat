@echo off

REM Set working directory to the directory of this script
cd /d %~dp0
echo Starting update in: %cd%

REM Navigate to local-client root
cd ..\
set "LOCAL_CLIENT_DIR=%cd%"
echo Local client directory: %LOCAL_CLIENT_DIR%

REM Navigate to repo root
cd ..\
echo Repo root: %cd%

echo Pulling latest changes...
git fetch origin
git reset --hard origin/main

REM Activate the virtual environment
call venv\Scripts\activate

REM Navigate to local-client
cd /d "%LOCAL_CLIENT_DIR%"

echo Installing dependencies...
pip install -r requirements.txt

echo Applying migrations...
python manage.py migrate

rem Get the directory of the batch script
set "ScriptDir=%~dp0"

rem Navigate to the bin directory
cd /d "%ScriptDir%..\bin"

REM Stop services with a delay
nssm stop inpoint_service confirm
timeout /t 5 >nul

nssm stop endpoint_service confirm
timeout /t 5 >nul

nssm stop CeleryWorker confirm
timeout /t 5 >nul

nssm stop CeleryBeat confirm
timeout /t 5 >nul

REM Start services again
nssm start inpoint_service confirm
nssm start endpoint_service confirm
nssm start CeleryWorker confirm
nssm start CeleryBeat confirm
