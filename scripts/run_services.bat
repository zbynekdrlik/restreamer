@echo off

rem Get the directory of the batch script
set "ScriptDir=%~dp0"

rem Navigate one step up to the 'client' directory, then to the 'bin' directory
cd /d "%ScriptDir%..\bin"

rem Install and start RedisServer
nssm install RedisServer "%ScriptDir%..\bin\redis-server.exe"
if %ERRORLEVEL% neq 0 (
    echo Failed to install RedisServer service
   
)

nssm start RedisServer
if %ERRORLEVEL% neq 0 (
    echo Failed to start RedisServer service
    
)

rem Install and start inpoint_service
nssm install inpoint_service "%ScriptDir%\inpoint_service.bat"
if %ERRORLEVEL% neq 0 (
    echo Failed to install inpoint_service

)

nssm start inpoint_service
if %ERRORLEVEL% neq 0 (
    echo Failed to start inpoint_service
 
)

rem Install and start endpoint_service  
nssm install endpoint_service "%ScriptDir%\endpoint_service.bat"
if %ERRORLEVEL% neq 0 (
    echo Failed to install endpoint_service
    
)

nssm start endpoint_service
if %ERRORLEVEL% neq 0 (
    echo Failed to start endpoint_service
   
) 


nssm install CeleryWorker "%ScriptDir%..\scripts\stream_ready_worker.bat"
if %ERRORLEVEL% neq 0 (
    echo Failed to install RedisServer service
   
)

nssm start CeleryWorker
if %ERRORLEVEL% neq 0 (
    echo Failed to start RedisServer service
    
)


nssm install CeleryBeat "%ScriptDir%..\scripts\clery_beat.bat"
if %ERRORLEVEL% neq 0 (
    echo Failed to install RedisServer service
   
)

nssm start CeleryBeat
if %ERRORLEVEL% neq 0 (
    echo Failed to start RedisServer service
    
)
