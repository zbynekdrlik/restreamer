@echo off

REM Set the directory of the batch file as the working directory
set "ScriptDir=%~dp0"

REM Run the PowerShell commands as an administrator by calling the secondary script
powershell -Command "Start-Process cmd.exe -ArgumentList '/c call \"%ScriptDir%executionpolicy.bat\"' -Verb RunAs"

rem Set the working directory to the main project directory

cd ..\

REM Initialize Git repository and add origin
if not exist .git (
    echo Initializing Git repository...
    git init
    git remote add origin https://github.com/kuskryptus/restreamer-local.git
    git fetch origin
    git checkout -b development origin/development
) else (
    echo Git repository already exists. Skipping initialization.
)

cd ..\

rem Create a virtual environment
python -m venv venv

rem Activate the virtual environment
call venv\Scripts\activate

echo Current Directory: %cd%

cd restreamer-local-client

echo Current Directory: %cd%

rem Install all dependencies
pip install -r requirements.txt

rem Make migrations
python manage.py makemigrations

rem Migrate
python manage.py migrate 


echo from django.contrib.auth.models import User; User.objects.create_superuser('admin', '', 'Milostsnv123!') | python manage.py shell

start "" http://127.0.0.1:8571/admin/

python manage.py create_local_user

rem Make migrations
python manage.py makemigrations

rem Migrate
python manage.py migrate 

REM Create directory for logs
mkdir "%ScriptDir%services_logs"

REM Run a specific line as administrator
powershell.exe -Command "Start-Process cmd.exe -ArgumentList '/c call \"%ScriptDir%run_services.bat\"' -Verb RunAs"

@echo off
set "shortcutTarget=%USERPROFILE%\Desktop\PowerShell.lnk"

cscript.exe "%ScriptDir%run_trayicon.vbs"

@echo off
move "%ScriptDir%run_trayicon.vbs" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
move "%ScriptDir%check_update.vbs" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
echo File moved to startup folder

python manage.py runserver 8571 

Read-Host "Press Enter to exit"

pause


