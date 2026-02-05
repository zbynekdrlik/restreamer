import logging
import time

from django.core.management.base import BaseCommand
from restreamer.inpoint import InPoint
from restreamer.models import StreamingEvent

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run restreamer service"

    def handle(self, *args, **options):
        while True:
            while True:
                try:
                    streaming_event = StreamingEvent.objects.get(receiving_activated=True)
                    break
                except StreamingEvent.DoesNotExist:
                    log.info("Waiting until it is active")
                    log.debug("Press start buttom")
                    time.sleep(10)

            print("Ide to")

            in_point = InPoint("NL_Rist", "9998", streaming_event)
            in_point.start()

            try:
                while True:
                    buff_string = (
                        f"Transferred from {in_point.name}: {in_point.buff_size.value / 1024 / 1024:.2f}MB "
                        f"(id:{in_point.chunk_record_id.value}) "
                    )

                    log.info(buff_string)
                    time.sleep(1)
                    # try:
                    #     streaming_event = StreamingEvent.objects.get(id=streaming_event.id)
                    # except:
                    #     pass
                    if not in_point.control_queue.empty():
                        message = in_point.control_queue.get_nowait()
                        if message == "inpoint_terminated":
                            # if not streaming_event.receiving_activated:
                            log.info("Stop receiving is pressed")
                            time.sleep(5)
                            from django.db import connection

                            connection.close()

                            break  # Ak je tlačidlo "stop receiving" stlačené, prerušíme slučku

            except KeyboardInterrupt:
                log.info("Ctrl-C detected, terminating!")
                # in_point.control_queue.put('Terminate')
                in_point.join()
                return
