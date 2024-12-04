Set objShell = CreateObject("WScript.Shell")
strScriptPath = "%USERPROFILE%\Desktop\restreamer\local_client\restreamer-local-client\scripts\start_update_service.ps1"

' Unblock the PowerShell script
strUnblockCommand = "powershell.exe -Command Unblock-File -Path """ & strScriptPath & """"
objShell.Run strUnblockCommand, 0, True

' Run the PowerShell script
strCommand = "powershell.exe -ExecutionPolicy Bypass -File """ & strScriptPath & """"
objShell.Run strCommand, 0, False

Set objShell = Nothing