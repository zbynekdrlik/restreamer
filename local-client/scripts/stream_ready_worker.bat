@echo off

cd ..\..\

call venv\Scripts\activate

cd local-client

python -m celery -A nl_restreamer worker -l INFO --pool=threads