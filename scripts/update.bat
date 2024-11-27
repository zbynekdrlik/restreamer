@echo off

REM Set working directory to the directory of this script
cd /d %~dp0
echo Starting script in: %cd%

REM Navigate one directory down
cd ..\
echo Navigated to: %cd%


echo Pulling latest changes...
git pull origin integration

REM Navigate two directories up
cd ..\
echo Navigated to: %cd%


REM Activate the virtual environment
call venv\Scripts\activate


REM Navigate to the restreamer-local-client directory
cd 'local_client'
echo Navigated to: %cd%

echo Installing dependencies...
pip install -r requirements.txt


echo Making migrations...
python manage.py makemigrations

echo Applying migrations...
python manage.py migrate

rem Get the directory of the batch script
set "ScriptDir=%~dp0"

rem Navigate one step up to the 'client' directory, then to the 'bin' directory
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

