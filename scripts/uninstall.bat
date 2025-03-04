@echo off
REM Set the path to the NSSM binary
set NSSM_PATH=%~dp0..\bin\nssm.exe

REM List of services to stop and delete
set SERVICES=endpoint_service CeleryBeat CeleryWorker inpoint_service RedisServer

REM Loop through each service and stop and delete it
for %%s in (%SERVICES%) do (
    echo Stopping %%s...
    "%NSSM_PATH%" stop %%s
    if %errorlevel% neq 0 (
        echo Failed to stop %%s.
    ) else (
        echo %%s stopped successfully.
        echo Deleting %%s...
        "%NSSM_PATH%" remove %%s confirm
        if %errorlevel% neq 0 (
            echo Failed to delete %%s.
        ) else (
            echo %%s deleted successfully.
        )
    )
)

echo Killing PowerShell and Command Prompt processes associated with the restreamer folder...
powershell -Command "Get-WmiObject Win32_Process | Where-Object { $_.CommandLine -like '*restreamer*' -and $_.Name -ne 'explorer' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"


echo All specified services have been processed.



