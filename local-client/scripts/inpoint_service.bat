@echo off

REM Get the directory of the batch file (the script)
set ScriptDir=%~dp0

REM Set the correct path to your PowerShell executable
set PowerShellExe=C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe

REM Set the relative path to your PowerShell script
set Script2=%ScriptDir%start_inpoint_service.ps1

REM Set the relative path for the log file
set LogFile2=%ScriptDir%services_logs\inpoint_service.txt

REM Unblock the PowerShell script if it is blocked
%PowerShellExe% -Command "Unblock-File -Path '%Script2%'"

REM Start the PowerShell script and redirect output to the log file
%PowerShellExe% -File "%Script2%" > "%LogFile2%" 2>&1