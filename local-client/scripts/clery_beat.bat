@echo off

cd ..\..\

call venv\Scripts\activate

cd local-client

celery -A nl_restreamer beat -l debug