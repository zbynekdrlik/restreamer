import logging
import time

from django.core.management.base import BaseCommand
from restreamer.client_in_point import ClientInPoint
from restreamer.models import StreamingEvent

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run client_point service"

    def handle(self, *args, **options):
        streaming_event = StreamingEvent.objects.using("client_db").get(active=True)
        client_in_point = ClientInPoint("NL_RTMP", "9998", streaming_event)

        try:
            while True:
                client_in_point.run()
                # buff_string = f'Transferred from {kristian}: {client_in_point.buff_size.value / 1024 / 1024:.2f}MB ' \
                #               f'(id:{client_in_point.chunk_record_id.value}) '
                #
                # log.info(buff_string)
                time.sleep(1)

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            # in_point.control_queue.put('Terminate')
            # client_in_point.join()
