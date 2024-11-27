import logging
from django.core.management.base import BaseCommand
from services.update import monitor_updates

log = logging.getLogger(__name__)

#update 5
class Command(BaseCommand):
    help = 'Run endpoint service'

    def handle(self, *args, **options):
        try:
            monitor_updates()
            log.info('successfuly called monitor updates')
        except Exception as e:
            log.exception(F"Error with update function {e}")

        