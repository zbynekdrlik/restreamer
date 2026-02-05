@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo   Restreamer Local Client - Setup
echo ============================================================
echo.

REM Set the directory of the batch file as the working directory
set "ScriptDir=%~dp0"

REM Set execution policy for PowerShell scripts
powershell -Command "Start-Process cmd.exe -ArgumentList '/c call \"%ScriptDir%executionpolicy.bat\"' -Verb RunAs"

REM Navigate to local-client root (one level up from scripts/)
cd /d "%ScriptDir%.."
set "LOCAL_CLIENT_DIR=%cd%"
echo Local client directory: %LOCAL_CLIENT_DIR%

REM Check that .env exists
if not exist ".env" (
    echo.
    echo ERROR: .env file not found!
    echo Please copy .env.example to .env and fill in your credentials before running setup.
    echo.
    pause
    exit /b 1
)

REM Navigate to parent directory (repo root or wherever venv should live)
cd ..
set "REPO_ROOT=%cd%"

REM Create virtual environment
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
) else (
    echo Virtual environment already exists.
)

REM Activate the virtual environment
call venv\Scripts\activate

REM Navigate back to local-client
cd /d "%LOCAL_CLIENT_DIR%"

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt

REM Unzip ffmpeg.zip if it exists
if exist "ffmpeg.zip" (
    echo Unzipping ffmpeg.zip...
    powershell -Command "Expand-Archive -Path 'ffmpeg.zip' -DestinationPath '.' -Force"
    echo ffmpeg has been unzipped.
    del "ffmpeg.zip"
) else (
    echo ffmpeg.zip not found, skipping unzip step.
)

REM Run migrations (migrations are committed, no makemigrations needed)
echo Applying database migrations...
python manage.py migrate

REM Create superuser interactively
echo.
echo Creating admin superuser...
echo Please enter credentials for the Django admin account:
python manage.py createsuperuser

REM Create local user profile
echo.
set /p USER_UUID="Enter the client UUID from the manager server (or press Enter to skip): "
if not "!USER_UUID!"=="" (
    python manage.py create_local_user --uuid "!USER_UUID!"
) else (
    echo Skipping local user creation.
)

REM Create directory for logs
if not exist "%ScriptDir%services_logs" (
    mkdir "%ScriptDir%services_logs"
)

REM Install and start services (Redis, NSSM services) as admin
echo.
echo Installing services (requires Administrator privileges)...
powershell.exe -Command "Start-Process cmd.exe -ArgumentList '/c call \"%ScriptDir%run_services.bat\"' -Verb RunAs"

REM Start tray icon
cscript.exe "%ScriptDir%run_trayicon.vbs"

REM Copy VBS launchers to startup folder
copy "%ScriptDir%run_trayicon.vbs" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\" >nul
copy "%ScriptDir%check_update.vbs" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\" >nul
echo Startup scripts installed.

REM Open admin page and start server
echo.
echo Starting Django development server on port 8571...
start "" http://127.0.0.1:8571/admin/
python manage.py runserver 8571

pause
