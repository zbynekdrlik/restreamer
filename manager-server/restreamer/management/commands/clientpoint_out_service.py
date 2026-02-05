import logging
import time

from django.core.management.base import BaseCommand
from restreamer.client_out_point import ClientOutPoint

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run client_point service"

    def handle(self, *args, **options):
        client_out_point = ClientOutPoint("NL_RTMP")

        try:
            while True:
                client_out_point.run()
                # buff_string = f'Transferred from {Kristian}: {client_out_point.buff_size.value / 1024 / 1024:.2f}MB ' \
                #               f'(id:{client_out_point.chunk_record_id.value}) '
                #
                # log.info(buff_string)
                time.sleep(1)

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            # in_point.control_queue.put('Terminate')
            # client_out_point.join()
