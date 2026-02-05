Set objFSO = CreateObject("Scripting.FileSystemObject")
Set objShell = CreateObject("WScript.Shell")

' Resolve path relative to this script's location
strScriptDir = objFSO.GetParentFolderName(WScript.ScriptFullName)
strScriptPath = objFSO.BuildPath(strScriptDir, "start_update_service.ps1")

' Unblock the PowerShell script
strUnblockCommand = "powershell.exe -Command Unblock-File -Path """ & strScriptPath & """"
objShell.Run strUnblockCommand, 0, True

' Run the PowerShell script
strCommand = "powershell.exe -ExecutionPolicy Bypass -File """ & strScriptPath & """"
objShell.Run strCommand, 0, False

Set objShell = Nothing
Set objFSO = Nothing
