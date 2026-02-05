@echo off

REM Set the execution policy to RemoteSigned at the machine scope
powershell -Command "Set-ExecutionPolicy RemoteSigned -Scope LocalMachine -Force"

REM Set the execution policy to RemoteSigned at the user scope
powershell -Command "Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force"

REM Set the execution policy to RemoteSigned at the process scope
powershell -Command "Set-ExecutionPolicy RemoteSigned -Scope Process -Force"

@echo off