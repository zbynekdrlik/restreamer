@echo off

echo Pulling latest changes...
git pull origin development

echo Installing dependencies...
pip install -r requirements.txt

cd ..\..\

venv\scripts\activate

python manage.py makemigrations

echo Applying migrations...
python manage.py migrate

rem Get the directory of the batch script
set "ScriptDir=%~dp0"

rem Navigate one step up to the 'client' directory, then to the 'bin' directory
cd /d "%ScriptDir%..\bin"

nssm restart inpoint_service confirm

nssm restart endpoint_service confirm

nssm restart CeleryWorker confirm

nssm restart CeleryBeat confirm

