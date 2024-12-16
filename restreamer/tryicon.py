import logging
import threading
import time
import os
import sys
import subprocess
from pathlib import Path
import psutil
import pystray
from PIL import Image
from pystray import MenuItem as item
from services.utils import delete_local_chunks, get_buffer_time

BASE_DIR = Path(__file__).resolve().parent.parent
ICONS_DIR = BASE_DIR / "static" / "icons"

E_LOG_FILE_DIR = BASE_DIR / "scripts" / "services_logs" / 'endpoint_service.txt'
I_LOG_FILE_DIR = BASE_DIR / "scripts" / "services_logs" / 'inpoint_service.txt'

log = logging.getLogger(__name__)


class TrayIcon:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def open_log_file(self, service):
        if service == 'endpoint':
            if os.path.exists(E_LOG_FILE_DIR):
                # Use subprocess to open the log file
                subprocess.Popen(['notepad.exe', str(E_LOG_FILE_DIR)])
            else:
                log.info(f"Log file does not exist: {E_LOG_FILE_DIR}")
        else:
            if os.path.exists(I_LOG_FILE_DIR):
                # Use subprocess to open the log file
                subprocess.Popen(['notepad.exe', str(I_LOG_FILE_DIR)])
            else:
                log.info(f"Log file does not exist: {I_LOG_FILE_DIR}")

    @staticmethod
    def tray_icon_actions(action):
        script_path = os.path.join(BASE_DIR, 'scripts', 'service_actions.py')
        python_interpreter = sys.executable
        subprocess.Popen([python_interpreter, script_path, action])

    @staticmethod
    def update_endpoint_icon(icon_state, icon):

        if icon_state == "endpoint_active":
            image = Image.open(ICONS_DIR / 'green_e.png')
            icon.title = "Endpoint Active"
        elif icon_state == "endpoint_waiting":
            image = Image.open(ICONS_DIR / 'orange_e_icon.png')
            icon.title = "Endpoint Waiting"
        else:
            log.info("Invalid icon state")
            return

        icon.icon = image

    def monitor_endpoint_redis_queue(self, icon):
        while True:
            result = self.redis_client.brpop('endpoint_icon_status', timeout=10)  # Block and wait for new messages
            if result:
                _, message = result
                icon_state = message.decode('utf-8')
                log.info(icon_state)
                self.update_endpoint_icon(icon_state, icon)
                self.redis_client.lrem('endpoint_icon_status', 1, message)
                self.redis_client.delete('endpoint_icon_status', 'inpoint_icon_status')
                time.sleep(3)

            else:
                log.info("Inactive")
                icon.title = "Endpoint Inactive"
                icon.icon = Image.open(ICONS_DIR / "red_e.png")

            service = psutil.win_service_get('endpoint_service')
            status = service.as_dict().get('status')

            menu_items = []
        
            # Add "start" or "restart" based on the service status
            if status != 'running':
                menu_items.append(pystray.MenuItem(text="Start",
                                                action=lambda: self.tray_icon_actions('start_endpoint')))
            else:
                menu_items.append(pystray.MenuItem(text="Restart",
                                                action=lambda: self.tray_icon_actions('restart_endpoint')))
            
            menu_items.append(pystray.MenuItem(text="Log File",
                                            action=lambda: self.open_log_file('endpoint')))
            
            # Update the menu with all items
            icon.menu = pystray.Menu(*menu_items)


    def run_endpoint_icon(self):

        self.redis_client.delete('inpoint_icon_status', 'inpoint_icon_status')
        icon = pystray.Icon("endpoint")
        icon.icon = Image.open(ICONS_DIR / "red_e.png")  # Initial red icon
        endpoint_icon_thread = threading.Thread(target=self.monitor_endpoint_redis_queue, args=(icon,))
        endpoint_icon_thread.start()
        
        time.sleep(2)

        icon.run_detached()

    
    def update_inpoint_icon(self, icon_state, icon):
        if icon_state == "inpoint_active":
            image = Image.open(ICONS_DIR / "green_I.png")
            icon.title = "Inpoint Active"
        elif icon_state == "inpoint_waiting":
            image = Image.open(ICONS_DIR / "orange_I.png")
            icon.title = "Inpoint Waiting"
        else:
            log.info("Invalid icon state")
            return

        icon.icon = image

    def monitor_inpoint_redis_queue(self, icon):

        while True:
            result = self.redis_client.brpop('inpoint_icon_status', timeout=10)
            if result:
                _, message = result
                icon_state = message.decode('utf-8')
                log.info(icon_state)
                self.update_inpoint_icon(icon_state, icon)
                self.redis_client.lrem('inpoint_icon_status', 1, message)
                self.redis_client.delete('endpoint_icon_status', 'inpoint_icon_status')
                time.sleep(3)
            else:
                log.info("Inactive")
                icon.title = "Inpoint Inactive"
                icon.icon = Image.open(ICONS_DIR / "red_I.png")

            service = psutil.win_service_get('inpoint_service')
            status = service.as_dict().get('status')

            buffer_time = get_buffer_time()
            
            menu_items.append(pystray.MenuItem(text="Delete Chunks",
                                            action=lambda: delete_local_chunks()))

            menu_items = [
                item(text=f"Time in buffer: {buffer_time}", action=None)
            ]

            if status != 'running':
                menu_items.append(pystray.MenuItem(text="Start",
                                                action=lambda: self.tray_icon_actions('start_inpoint')))
            else:
                menu_items.append(pystray.MenuItem(text="Restart",
                                                action=lambda: self.tray_icon_actions('restart_inpoint')))

            # Always add the "Log File" option for inpoint
            menu_items.append(pystray.MenuItem(text="Log File",
                                            action=lambda: self.open_log_file('inpoint')))
            
            # Update the menu with all items
            icon.menu = pystray.Menu(*menu_items)

    def run_inpoint_icon(self):
        self.redis_client.delete('endpoint_icon_status', 'inpoint_icon_status')
        icon = pystray.Icon("inpoint")
        icon.icon = Image.open(ICONS_DIR / "red_I.png")
        inpoint_icon_thread = threading.Thread(target=self.monitor_inpoint_redis_queue, args=(icon,))
        inpoint_icon_thread.start()
        time.sleep(1)
        icon.run_detached()
