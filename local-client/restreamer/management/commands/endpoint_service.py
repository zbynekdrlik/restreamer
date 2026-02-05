import logging
import time
import redis

from django.conf import settings
from linode_api4 import Instance, LinodeClient, objects
from django.core.management.base import BaseCommand
from restreamer.local_endpoint import ChunkSender
from restreamer.models import ChunkRecord, StreamingEvent

log = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


class Command(BaseCommand):
    help = 'Run endpoint service'

    def handle(self, *args, **options):
        while True:
            streaming_event_id = StreamingEvent.objects.last()
            ChunkRecord.objects.filter(in_process=True).update(in_process=False)  # Chunks are redy to be send again.

            if streaming_event_id and not streaming_event_id.delivering_activated:
                redis_client.rpush('endpoint_icon_status', 'endpoint_waiting')
                log.info("Waiting until it is active")
                log.debug("Press start button")
                time.sleep(5)
                continue

            while True:
                try:
                    while True:
                        first_chunk = ChunkRecord.objects.filter(streaming_event=streaming_event_id).first()
                        if streaming_event_id and not streaming_event_id.delivering_activated:
                            redis_client.rpush('endpoint_icon_status', 'endpoint_waiting')
                            log.info("Shutting down")
                            break
                        if first_chunk is not None:
                            start_endpoint = ChunkSender(streaming_event_id)
                            start_endpoint.sending_chunks()
                                                        
                        else:
                            redis_client.rpush('endpoint_icon_status', 'endpoint_waiting')
                            log.info('No available chunks for this streaming event.')
                            log.info("Waiting for new chunks")
                            break
                    if streaming_event_id and not streaming_event_id.delivering_activated:
                        log.info("Shutting down")
                        break

                    time.sleep(5)
                    continue

                except KeyboardInterrupt:
                    log.info('Ctrl-C detected, terminating!')
                    log.info('End of chunk transmission.')
                    time.sleep(1)
                    raise

                
                

