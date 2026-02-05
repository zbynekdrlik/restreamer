import logging
import time

import redis
from django.core.management.base import BaseCommand

from restreamer.inpoint import InPoint
from restreamer.models import StreamingEvent

log = logging.getLogger(__name__)
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)


class Command(BaseCommand):
    help = "Run restreamer service"

    def handle(self, *args, **options):
        while True:
            while True:
                streaming_event = StreamingEvent.objects.last()
                if not streaming_event.receiving_activated:
                    redis_client.rpush("inpoint_icon_status", "inpoint_waiting")
                    log.info("Waiting until it is active")
                    log.debug("Press start button")
                    time.sleep(10)
                    break

                in_point = InPoint("VMIX/OBS", "1234", streaming_event)
                in_point.start()

                try:
                    while True:
                        buff_string = (
                            f"Transferred from {in_point.name}: {in_point.buff_size / 1024 / 1024:.2f}MB "
                            f"(id:{in_point.chunk_record_id}) "
                        )

                        log.info(buff_string)
                        streaming_event.refresh_from_db()
                        if not streaming_event.receiving_activated:
                            log.info("Shutting down")
                            break
                        redis_client.rpush("inpoint_icon_status", "inpoint_active")
                        time.sleep(1)

                except KeyboardInterrupt:
                    log.info("Ctrl-C detected, terminating!")
                    in_point.join()
                    return
