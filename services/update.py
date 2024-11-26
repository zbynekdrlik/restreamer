import ctypes
import logging
import subprocess
import threading
import time
from pathlib import Path

from PIL import Image, ImageDraw
from pystray import Icon, Menu, MenuItem

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

ICONS_DIR = BASE_DIR / "static" / "icons"
ICON_PATH = ICONS_DIR / "update_info.png"
UPDATE_SCRIPT = BASE_DIR / "scripts" / "update.bat"

# Function to check for updates
def check_updates():
    try:
        remote_commit = subprocess.check_output(
            "git ls-remote origin development | findstr /B /C:\"refs/heads/development\"",
            shell=True,
        ).decode().strip().split()[0]
        local_commit = subprocess.check_output(
            "git rev-parse development", shell=True
        ).decode().strip()
        return remote_commit != local_commit
    except subprocess.CalledProcessError:
        return False

# Function to trigger the update process
def run_update():
    if UPDATE_SCRIPT.exists():
        # Run the update.bat file with admin privileges
        try:
            ctypes.windll.shell32.ShellExecuteW(
                None, "runas", str(UPDATE_SCRIPT), None, None, 1
            )
        except Exception as e:
            log.info(f"Failed to run update script: {e}")
    else:
        log.info(f"Update script not found at {UPDATE_SCRIPT}")

# Function triggered when the "Update Available" menu item is clicked
def on_click_update(icon, item):
    icon.stop()
    run_update()

# Function to display the tray icon
def tray_icon():
    # Load the existing icon file
    icon_image = Image.open(ICON_PATH)

    # Define actions for the tray menu
    menu = Menu(
        MenuItem("Update Available", on_click_update)
    )

    # Create and display the tray icon
    icon = Icon("Updater", icon_image, menu=menu)
    icon.run()

# Background thread for monitoring updates
def monitor_updates():
    while True:
        if check_updates():
            tray_icon()
            break
        time.sleep(10)