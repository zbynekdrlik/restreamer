import logging
import time

from django.core.management.base import BaseCommand
from restreamer.endpoints import restr_manager

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run endpoint service"

    def handle(self, *args, **options):
        try:
            while True:
                buff_string = ""
                for n in restr_manager.endpoint_list:
                    buff_string += f"{n.alias}: {n.buff_size.value / 1024 / 1024:.2f}MB (id:{n.chunk_record_id.value})|"

                log.debug(buff_string)

                time.sleep(10)
                restr_manager.manage_endpoints()
                log.debug("Endpoint on")

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            # time.sleep(15)
            # log.info('End of life')
            # in_point.control_queue.put('Terminate')
            # in_point.join()
