@echo off

REM Navigate to repo root using script directory
cd /d "%~dp0..\.."

call venv\Scripts\activate

cd local-client

"%~dp0..\..\venv\Scripts\python.exe" -m celery -A nl_restreamer worker -l INFO --pool=threads
