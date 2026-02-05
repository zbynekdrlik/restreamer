import subprocess
import sys
import pyuac
import os


# Action based on arguments passed from try_icon module
def handle_action(action):
    creation_flags = subprocess.CREATE_NO_WINDOW

    if action == 'restart_endpoint':
        command = r'powershell.exe -WindowStyle Hidden -Command "Restart-Service -Name endpoint_service "'
        subprocess.Popen(command, creationflags=subprocess.CREATE_NO_WINDOW)
    elif action == 'restart_inpoint':
        command = r'powershell.exe -WindowStyle Hidden -Command "Restart-Service -Name inpoint_service "'
        subprocess.Popen(command, creationflags=subprocess.CREATE_NO_WINDOW)
    elif action == 'start_endpoint':
        command = r'powershell.exe -WindowStyle Hidden -Command "Start-Service -Name endpoint_service "'
        subprocess.Popen(command, creationflags=subprocess.CREATE_NO_WINDOW)
    elif action == 'start_inpoint':
        command = r'powershell.exe -WindowStyle Hidden -Command "Start-Service -Name inpoint_service "'
        subprocess.Popen(command, creationflags=subprocess.CREATE_NO_WINDOW)


# Elevated privileges needed for this action.
if __name__ == "__main__":
    if not pyuac.isUserAdmin():
        pyuac.runAsAdmin()
    else:
        handle_action(sys.argv[1])  # Already an admin here.
