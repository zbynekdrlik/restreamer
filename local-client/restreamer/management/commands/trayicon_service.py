import logging
from pathlib import Path

import redis
from django.core.management.base import BaseCommand
from restreamer.tryicon import TrayIcon

BASE_DIR = Path(__file__).resolve().parent
log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run try-icon service'
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    def handle(self, *args, **options):

        try:
            try_icon = TrayIcon(self.redis_client)
            try_icon.run_endpoint_icon()
            try_icon.run_inpoint_icon()

        except Exception as e:
            log.exception(e)
