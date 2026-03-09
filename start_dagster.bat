@echo off
cd /d "C:\Python_scripts"
start "Dagster Webserver" cmd /k ".venv\Scripts\dagster-webserver.exe -f dagster_pipeline.py -p 8081 -h 0.0.0.0"
start "Dagster Daemon" cmd /k ".venv\Scripts\dagster-daemon.exe run -f dagster_pipeline.py"