@echo off

cd ..\..\

call venv\Scripts\activate

cd restreamer-local-client

celery -A nl_restreamer worker -l INFO --pool=threads