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

BASE_DIR = Path(__file__).resolve().parent.parent
ICONS_DIR = BASE_DIR / "static" / "icons"
log = logging.getLogger(__name__)


class TrayIcon:
    def __init__(self, redis_client):
        self.redis_client = redis_client

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

            if status != 'running':
                try:
                    icon.menu = pystray.Menu(pystray.MenuItem(text="start",
                                                              action=lambda: self.tray_icon_actions(
                                                                  'start_endpoint'),
                                                              default=True))
                except Exception as e:
                    log.info(e)
            else:
                try:
                    icon.menu = pystray.Menu(pystray.MenuItem(text="restart",
                                                              action=lambda: self.tray_icon_actions(
                                                                  'restart_endpoint'),
                                                              default=True))
                except Exception as e:
                    log.info(e)

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

            if status != 'running':
                try:
                    icon.menu = pystray.Menu(pystray.MenuItem(text="start",
                                                              action=lambda: self.tray_icon_actions('start_inpoint'),
                                                              default=True))
                except Exception as e:
                    log.info(e)
            else:
                try:
                    icon.menu = pystray.Menu(pystray.MenuItem(text="restart",
                                                              action=lambda: self.tray_icon_actions('restart_inpoint'),
                                                              default=True))
                except Exception as e:
                    log.info(e)

    def run_inpoint_icon(self):
        self.redis_client.delete('endpoint_icon_status', 'inpoint_icon_status')
        icon = pystray.Icon("inpoint")
        icon.icon = Image.open(ICONS_DIR / "red_I.png")
        inpoint_icon_thread = threading.Thread(target=self.monitor_inpoint_redis_queue, args=(icon,))
        inpoint_icon_thread.start()
        time.sleep(1)
        icon.run_detached()
