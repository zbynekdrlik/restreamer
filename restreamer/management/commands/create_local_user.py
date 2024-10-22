import logging
import os
from django.core.management.base import BaseCommand
from restreamer.models import ClientProfile
from django.conf import settings



log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Create new stream manager'

    def handle(self, *args, **options):
        base_dir = settings.BASE_DIR.parent
        conf_dir = os.path.dirname(base_dir)
        log.info(conf_dir)
        conf_file = os.path.join(conf_dir, 'config.txt')
        with open(conf_file, 'r') as f:  # Change 'w' to 'r' for read mode
            line = f.readline().strip()
            user_id = line.split(" ")[1]

        ClientProfile.objects.create(user_id=user_id)

        os.remove(conf_file)

    