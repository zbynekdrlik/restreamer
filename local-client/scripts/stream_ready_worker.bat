@echo off

cd ..\..\

call venv\Scripts\activate

cd local-client

celery -A nl_restreamer worker -l INFO --pool=threads