import ctypes
import logging
import subprocess
import time
from pathlib import Path

from PIL import Image
from pystray import Icon, Menu, MenuItem

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

ICONS_DIR = BASE_DIR / "static" / "icons"
ICON_PATH = ICONS_DIR / "update_info.png"
UPDATE_SCRIPT = BASE_DIR / "scripts" / "update.bat"


# Function to check for updates
def check_updates():
    try:
        # Fetch remote commits for the 'main' branch
        log.info("Fetching remote commit...")
        remote_commit_output = (
            subprocess.check_output(
                "git ls-remote origin main",
                shell=True,
            )
            .decode()
            .strip()
        )

        if not remote_commit_output:
            log.error("No output from 'git ls-remote'. Is the branch 'main' present on the remote?")
            return False

        remote_commit = remote_commit_output.split()[0]
        log.info(f"Remote commit hash: {remote_commit}")

        # Fetch the local commit hash
        log.info("Fetching local commit...")
        local_commit_output = (
            subprocess.check_output(
                "git rev-parse main",
                shell=True,
            )
            .decode()
            .strip()
        )

        local_commit = local_commit_output
        log.info(f"Local commit hash: {local_commit}")

        # Compare commits
        is_different = remote_commit != local_commit
        log.info(f"Commits are different: {is_different}")
        return is_different

    except subprocess.CalledProcessError as e:
        # Log detailed error information
        log.error("Command failed with error:")
        log.error(f"Return code: {e.returncode}")
        log.error(f"Command: {e.cmd}")
        log.error(f"Output: {e.output.decode() if e.output else 'No output'}")
        log.error(f"Stderr: {e.stderr.decode() if e.stderr else 'No stderr'}")
        return False


# Function to trigger the update process
def run_update():
    if UPDATE_SCRIPT.exists():
        # Run the update.bat file with admin privileges
        try:
            ctypes.windll.shell32.ShellExecuteW(None, "runas", str(UPDATE_SCRIPT), None, None, 1)
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
    menu = Menu(MenuItem("Update Available", on_click_update))

    # Create and display the tray icon
    icon = Icon("Updater", icon_image, menu=menu)
    icon.run()


# Background thread for monitoring updates
def monitor_updates():
    while True:
        if check_updates():
            tray_icon()
            break
        time.sleep(10000)
