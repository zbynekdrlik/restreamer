@echo off

cd ..\..\

call venv\Scripts\activate

cd restreamer-local-client

celery -A nl_restreamer beat -l debug